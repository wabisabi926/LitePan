"""夸克 CAS 扫码登录，进程内状态。"""

import asyncio
import base64
import io
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional

import aiohttp

from .api import QuarkAPI, QuarkConstants

QR_LOGIN_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
)

CLIENT_ID = "532"
QR_BASE_URL = "https://su.quark.cn/4_eMHBJ"
CAS_GET_TOKEN = "https://uop.quark.cn/cas/ajax/getTokenForQrcodeLogin"
CAS_GET_TICKET = "https://uop.quark.cn/cas/ajax/getServiceTicketByQrcodeToken"
PAN_ACCOUNT_INFO = "https://pan.quark.cn/account/info"

QR_CODE_TIMEOUT_SEC = 300
QR_STATE_TTL_SEC = 360

CAS_STATUS_OK = 2000000
CAS_STATUS_FAIL = {50004002, 50004003, 50004004}


@dataclass
class QrLoginState:
    state_id: str
    token: str
    qr_url: str
    created_at: float
    session: Optional[aiohttp.ClientSession] = None
    cookie: str = ""
    status: str = "waiting"
    message: str = ""


class QuarkQrLoginManager:
    def __init__(self):
        self._states: Dict[str, QrLoginState] = {}
        self._lock = asyncio.Lock()

    def _base_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": QR_LOGIN_UA,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Referer": "https://pan.quark.cn/",
        }

    def _cookie_jar_to_header_string(self, jar) -> str:
        grouped: Dict[str, list] = {}
        for morsel in jar:
            key = morsel.key
            if not key:
                continue
            if key in QuarkConstants.COOKIE_IGNORE_KEYS or key.startswith("_ga"):
                continue
            dom = (morsel["domain"] or "").lower()
            if dom and "quark" not in dom:
                continue
            score = len(dom)
            if "drive-pc" in dom:
                score += 80
            elif "pan.quark" in dom:
                score += 40
            if dom.startswith("."):
                score += 5
            grouped.setdefault(key, []).append((morsel.value, score))
        parts: list[str] = []
        for key in sorted(grouped.keys()):
            best_val, _ = max(grouped[key], key=lambda x: x[1])
            parts.append(f"{key}={best_val}")
        return "; ".join(parts)

    async def _bootstrap_quark_web_pc_session(self, session: aiohttp.ClientSession) -> None:
        try:
            async with session.get(
                "https://pan.quark.cn/",
                headers={
                    "User-Agent": QR_LOGIN_UA,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "zh-CN,zh;q=0.9",
                    "Upgrade-Insecure-Requests": "1",
                },
                allow_redirects=True,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                await resp.read()
        except Exception:
            pass

        list_url = f"{QuarkAPI.BASE_URL}{QuarkAPI.ENDPOINTS['file_list']}"
        params = {
            **QuarkConstants.DEFAULT_PARAMS,
            "pdir_fid": "0",
            "_page": 1,
            "_size": 1,
            "_fetch_total": 1,
        }
        try:
            async with session.get(
                list_url,
                params=params,
                headers={
                    "User-Agent": QuarkConstants.QUARK_USER_AGENT,
                    "Referer": "https://pan.quark.cn/",
                    "Origin": "https://pan.quark.cn",
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Language": "zh-CN,zh;q=0.9",
                },
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                await resp.read()
        except Exception:
            pass

    async def _gc(self):
        now = time.time()
        expired_ids = [
            sid for sid, st in self._states.items()
            if now - st.created_at > QR_STATE_TTL_SEC
        ]
        for sid in expired_ids:
            state = self._states.pop(sid, None)
            if state and state.session and not state.session.closed:
                try:
                    await state.session.close()
                except Exception:
                    pass

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

    async def start(self) -> Dict[str, Any]:
        async with self._lock:
            await self._gc()

        session = aiohttp.ClientSession(headers=self._base_headers())
        state_id = uuid.uuid4().hex

        try:
            params = {
                "client_id": CLIENT_ID,
                "v": "1.2",
                "request_id": str(uuid.uuid4()),
            }
            async with session.get(CAS_GET_TOKEN, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"获取二维码 token HTTP {resp.status}")
                data = await resp.json(content_type=None)

            if data.get("status") != CAS_STATUS_OK:
                raise RuntimeError(f"获取二维码 token 失败：{data.get('message') or data.get('status')}")
            token = (data.get("data") or {}).get("members", {}).get("token")
            if not token:
                raise RuntimeError("获取二维码 token 失败：响应里没有 token 字段")

            qr_url = (
                f"{QR_BASE_URL}?token={token}&client_id={CLIENT_ID}&ssb=weblogin"
                f"&uc_param_str=&uc_biz_str=S%3Acustom%7COPT%3ASAREA%400"
                f"%7COPT%3AIMMERSIVE%401%7COPT%3ABACK_BTN_STYLE%400"
            )
            qr_image_base64 = await self._build_qr_image(qr_url)

            state = QrLoginState(
                state_id=state_id,
                token=token,
                qr_url=qr_url,
                created_at=time.time(),
                session=session,
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
            return {"status": "success", "cookie": state.cookie}
        if state.status == "failed":
            return {"status": "failed", "message": state.message or "登录失败"}
        if state.status == "expired":
            return {"status": "expired", "message": state.message or "扫码已过期"}

        session = state.session
        if session is None or session.closed:
            state.status = "expired"
            return {"status": "expired", "message": "扫码会话已关闭"}

        try:
            params = {
                "client_id": CLIENT_ID,
                "v": "1.2",
                "token": state.token,
                "request_id": str(uuid.uuid4()),
            }
            async with session.get(CAS_GET_TICKET, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return {"status": "waiting"}
                data = await resp.json(content_type=None)
        except Exception as e:
            return {"status": "waiting", "message": f"轮询异常：{e}"}

        status_code = data.get("status")
        message = data.get("message", "")
        service_ticket = (data.get("data") or {}).get("members", {}).get("service_ticket")

        if status_code == CAS_STATUS_OK and service_ticket:
            return await self._finalize_login(state, service_ticket)

        if status_code in CAS_STATUS_FAIL:
            state.status = "failed"
            state.message = message or "登录失败"
            await self._cleanup_session(state)
            return {"status": "failed", "message": state.message}

        return {"status": "waiting"}

    async def _finalize_login(self, state: QrLoginState, service_ticket: str) -> Dict[str, Any]:
        session = state.session
        try:
            async with session.get(
                PAN_ACCOUNT_INFO,
                params={"st": service_ticket, "lw": "scan"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                await resp.read()
                if resp.status != 200:
                    state.status = "failed"
                    state.message = f"获取登录 cookie 失败，HTTP {resp.status}"
                    await self._cleanup_session(state)
                    return {"status": "failed", "message": state.message}
        except Exception as e:
            state.status = "failed"
            state.message = f"获取登录 cookie 异常：{e}"
            await self._cleanup_session(state)
            return {"status": "failed", "message": state.message}

        await self._bootstrap_quark_web_pc_session(session)

        cookie_str = self._cookie_jar_to_header_string(session.cookie_jar)
        if not cookie_str.strip():
            state.status = "failed"
            state.message = "登录完成但未收到 cookie，请重试"
            await self._cleanup_session(state)
            return {"status": "failed", "message": state.message}
        state.cookie = cookie_str
        state.status = "success"
        await self._cleanup_session(state)
        return {"status": "success", "cookie": cookie_str}

    async def _cleanup_session(self, state: QrLoginState):
        if state.session and not state.session.closed:
            try:
                await state.session.close()
            except Exception:
                pass
        state.session = None


qr_login_manager = QuarkQrLoginManager()
