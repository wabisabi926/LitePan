"""天翼云盘 PC 扫码登录。"""

import asyncio
import base64
import io
import re
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional

import aiohttp

from .api import Cloud189API, Cloud189ApiHelper


QR_CODE_TIMEOUT_SEC = 300
QR_STATE_TTL_SEC = 360


@dataclass
class Cloud189QrState:
    state_id: str
    created_at: float
    session: Optional[aiohttp.ClientSession]
    lt: str
    req_id: str
    param_id: str
    captcha_token: str
    qr_url: str
    encryuuid: str
    status: str = "waiting"
    message: str = ""
    refresh_token: str = ""


class Cloud189QrLoginManager:
    def __init__(self):
        self._states: Dict[str, Cloud189QrState] = {}
        self._lock = asyncio.Lock()

    def _headers(self) -> Dict[str, str]:
        return dict(Cloud189API.HEADERS)

    async def _gc(self):
        now = time.time()
        expired_ids = [
            sid for sid, state in self._states.items()
            if now - state.created_at > QR_STATE_TTL_SEC
        ]
        for sid in expired_ids:
            state = self._states.pop(sid, None)
            if state and state.session and not state.session.closed:
                await state.session.close()

    async def _build_qr_image(self, qr_url: str) -> str:
        try:
            import qrcode
        except ImportError as e:
            raise RuntimeError(f"缺少依赖 qrcode，请执行 pip install 'qrcode[pil]'：{e}")

        def _make() -> bytes:
            img = qrcode.make(qr_url, box_size=8, border=2)
            buf = io.BytesIO()
            img.save(buf, format="PNG")
            return buf.getvalue()

        png_bytes = await asyncio.get_running_loop().run_in_executor(None, _make)
        return "data:image/png;base64," + base64.b64encode(png_bytes).decode()

    def _extract_base_params(self, html: str) -> Dict[str, str]:
        patterns = {
            "captcha_token": r"'captchaToken'\s+value='(.+?)'",
            "lt": r'lt\s*=\s*"(.+?)"',
            "param_id": r'paramId\s*=\s*"(.+?)"',
            "req_id": r'reqId\s*=\s*"(.+?)"',
        }
        result: Dict[str, str] = {}
        for key, pattern in patterns.items():
            match = re.search(pattern, html)
            if not match:
                raise RuntimeError(f"解析天翼登录参数失败：缺少 {key}")
            result[key] = match.group(1)
        return result

    async def _init_base_params(self, session: aiohttp.ClientSession) -> Dict[str, str]:
        params = {
            "appId": Cloud189API.APP_ID,
            "clientType": Cloud189API.CLIENT_TYPE,
            "returnURL": Cloud189API.RETURN_URL,
            "timeStamp": str(int(time.time() * 1000)),
        }
        async with session.get(
            f"{Cloud189API.WEB_URL}/api/portal/unifyLoginForPC.action",
            params=params,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            html = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"初始化登录参数失败 HTTP {resp.status}")
        return self._extract_base_params(html)

    async def start(self) -> Dict[str, Any]:
        async with self._lock:
            await self._gc()

        session = aiohttp.ClientSession(headers=self._headers())
        state_id = uuid.uuid4().hex
        try:
            base_params = await self._init_base_params(session)
            async with session.post(
                f"{Cloud189API.AUTH_URL}/api/logbox/oauth2/getUUID.do",
                data={"appId": Cloud189API.APP_ID},
                headers={"Accept": "application/json;charset=UTF-8"},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                data = await resp.json(content_type=None)
                if resp.status != 200:
                    raise RuntimeError(f"获取二维码失败 HTTP {resp.status}")

            qr_url = data.get("uuid") or ""
            encryuuid = data.get("encryuuid") or ""
            if not qr_url or not encryuuid:
                raise RuntimeError("获取二维码失败：响应缺少 uuid/encryuuid")

            qr_image_base64 = await self._build_qr_image(qr_url)
            state = Cloud189QrState(
                state_id=state_id,
                created_at=time.time(),
                session=session,
                lt=base_params["lt"],
                req_id=base_params["req_id"],
                param_id=base_params["param_id"],
                captcha_token=base_params["captcha_token"],
                qr_url=qr_url,
                encryuuid=encryuuid,
            )
            async with self._lock:
                self._states[state_id] = state

            return {
                "state_id": state_id,
                "qr_image_base64": qr_image_base64,
                "qr_url": qr_url,
                "expires_in": QR_CODE_TIMEOUT_SEC,
            }
        except Exception:
            await session.close()
            raise

    async def poll(self, state_id: str) -> Dict[str, Any]:
        state = self._states.get(state_id)
        if not state:
            return {"status": "expired", "message": "扫码会话不存在或已被回收，请重新获取二维码"}

        if time.time() - state.created_at > QR_CODE_TIMEOUT_SEC and state.status == "waiting":
            state.status = "expired"
            state.message = "二维码已过期，请重新获取"
            await self._cleanup_session(state)
            return {"status": "expired", "message": state.message}

        if state.status == "success":
            return {"status": "success", "config": {"refresh_token": state.refresh_token}}
        if state.status in {"failed", "expired"}:
            return {"status": state.status, "message": state.message}

        session = state.session
        if session is None or session.closed:
            return {"status": "expired", "message": "扫码会话已关闭"}

        now = time.time()
        form = {
            "appId": Cloud189API.APP_ID,
            "clientType": Cloud189API.CLIENT_TYPE,
            "returnUrl": Cloud189API.RETURN_URL,
            "paramId": state.param_id,
            "uuid": state.qr_url,
            "encryuuid": state.encryuuid,
            "date": time.strftime("%Y-%m-%d%H:%M:%S.", time.localtime(now)) + f"{int((now % 1) * 1000):03d}",
            "timeStamp": str(int(now * 1000)),
            "cb_SaveName": "0",
            "isOauth2": "true",
            "state": "",
        }

        try:
            async with session.post(
                f"{Cloud189API.AUTH_URL}/api/logbox/oauth2/qrcodeLoginState.do",
                data=form,
                headers={
                    "Referer": Cloud189API.AUTH_URL,
                    "Reqid": state.req_id,
                    "lt": state.lt,
                    "Accept": "application/json;charset=UTF-8",
                },
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                data = await resp.json(content_type=None)
        except Exception as e:
            return {"status": "waiting", "message": f"轮询异常：{e}"}

        status = data.get("status")
        if status == 0:
            redirect_url = data.get("redirectUrl")
            if not redirect_url:
                state.status = "failed"
                state.message = "扫码成功但未返回授权地址"
                await self._cleanup_session(state)
                return {"status": "failed", "message": state.message}
            return await self._finalize_login(state, redirect_url)

        if status == -11001:
            state.status = "expired"
            state.message = "二维码已过期，请重新获取"
            await self._cleanup_session(state)
            return {"status": "expired", "message": state.message}

        if status in {-106, -11002}:
            return {"status": "waiting", "message": "请扫码并在手机上确认登录"}

        message = data.get("msg") or data.get("message") or f"扫码登录失败，状态码 {status}"
        state.status = "failed"
        state.message = message
        await self._cleanup_session(state)
        return {"status": "failed", "message": message}

    async def _finalize_login(self, state: Cloud189QrState, redirect_url: str) -> Dict[str, Any]:
        session = state.session
        params = Cloud189ApiHelper.client_suffix()
        params["redirectURL"] = redirect_url
        try:
            async with session.post(
                f"{Cloud189API.API_URL}/getSessionForPC.action",
                params=params,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                data = await resp.json(content_type=None)
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}")
        except Exception as e:
            state.status = "failed"
            state.message = f"换取会话失败：{e}"
            await self._cleanup_session(state)
            return {"status": "failed", "message": state.message}

        if int(data.get("res_code") or 0) != 0:
            state.status = "failed"
            state.message = data.get("res_message") or "换取会话失败"
            await self._cleanup_session(state)
            return {"status": "failed", "message": state.message}

        refresh_token = data.get("refreshToken") or ""
        if not refresh_token:
            state.status = "failed"
            state.message = "登录完成但未收到 refreshToken"
            await self._cleanup_session(state)
            return {"status": "failed", "message": state.message}

        state.refresh_token = refresh_token
        state.status = "success"
        await self._cleanup_session(state)
        return {"status": "success", "config": {"refresh_token": refresh_token}}

    async def _cleanup_session(self, state: Cloud189QrState):
        if state.session and not state.session.closed:
            try:
                await state.session.close()
            except Exception:
                pass
        state.session = None


qr_login_manager = Cloud189QrLoginManager()
