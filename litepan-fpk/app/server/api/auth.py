from fastapi import APIRouter, Request, Form
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
from core.session_manager import session_manager
from core.security import check_password_hash, assess_admin_credential_state, generate_password_hash, is_password_hash
from core.error_handler import raise_unauthorized
from api.deps import is_public_index_enabled

router = APIRouter()

def get_admin_credentials():
    from config import config_manager
    username = config_manager.get('admin_username') or "admin"
    password = config_manager.get('admin_password') or "admin"
    return username, password


def get_admin_credential_state():
    username, password = get_admin_credentials()
    return assess_admin_credential_state(username, password)

class AuthResponse(BaseModel):
    success: bool
    data: Optional[dict] = None
    message: str = ""


def _now_ts() -> int:
    import time

    return int(time.time())


def _get_temp_password_state() -> dict:
    from config import config_manager

    hashed = config_manager.get("admin_temp_password_hash") or ""
    expires_at = int(config_manager.get("admin_temp_password_expires_at") or 0)
    last_reset_at = int(config_manager.get("admin_temp_password_last_reset_at") or 0)
    now = _now_ts()
    valid = bool(hashed) and expires_at > now
    return {
        "hash": str(hashed),
        "expires_at": expires_at,
        "last_reset_at": last_reset_at,
        "valid": valid,
    }


_reset_ip_cooldown: dict = {}


@router.post("/login")
async def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    remember: str = Form("")
):
    stored_username, stored_password = get_admin_credentials()
    credential_state = assess_admin_credential_state(stored_username, stored_password)

    temp_state = _get_temp_password_state()

    password_match = False
    if is_password_hash(stored_password):
        password_match = check_password_hash(stored_password, password)
    else:
        password_match = (password == stored_password)

    temp_password_match = False
    if not password_match and username == stored_username and temp_state["valid"] and temp_state["hash"]:
        temp_password_match = check_password_hash(temp_state["hash"], password)

    if username == stored_username and (password_match or temp_password_match):
        must_change_password = bool(credential_state["must_change_password"] or temp_password_match)
        password_change_reason = (
            "temporary_password" if temp_password_match else credential_state["password_change_reason"]
        )
        response_data = {
            "success": True,
            "data": {
                "username": username,
                "is_admin": True,
                "must_change_password": must_change_password,
                "password_change_reason": password_change_reason,
            },
            "message": "登录成功"
        }
        
        response = JSONResponse(content=response_data)

        session_manager.create_session(
            response=response,
            user_data={
                'is_admin': True,
                'username': username,
                'must_change_password': must_change_password,
                'password_change_reason': password_change_reason,
            },
            remember=(remember == "1"),
            request=request
        )
        
        return response
    else:
        raise_unauthorized("用户名或密码错误")

@router.post("/logout")
async def logout(request: Request):
    response_data = {"success": True, "message": "登出成功"}
    response = JSONResponse(content=response_data)
    session_manager.clear_session(response)
    return response

@router.get("/status")
async def get_auth_status(request: Request) -> AuthResponse:
    session_data = session_manager.get_session(request)
    is_admin = session_data.get('is_admin', False)
    public_index_enabled = await is_public_index_enabled()
    if is_admin and bool(session_data.get("must_change_password")):
        credential_state = {
            "must_change_password": True,
            "password_change_reason": str(session_data.get("password_change_reason") or "temporary_password"),
        }
    else:
        credential_state = get_admin_credential_state() if is_admin else {
            "must_change_password": False,
            "password_change_reason": "",
        }
    
    return AuthResponse(
        success=True,
        data={
            "is_admin": is_admin,
            "username": session_data.get('username') if is_admin else None,
            "public_index_enabled": public_index_enabled,
            "must_change_password": credential_state["must_change_password"],
            "password_change_reason": credential_state["password_change_reason"],
        },
        message="获取认证状态成功"
    )

@router.post("/reset-password")
async def reset_password(request: Request):
    """生成临时密码写入配置（哈希 + 过期时间），不修改原管理员密码。"""
    import secrets
    import string
    import time
    from config import config_manager
    from core.log_manager import get_writer, LogModule

    now = int(time.time())
    cooldown_seconds = 60
    ttl_seconds = 600

    ip = ""
    try:
        ip = (request.client.host if request.client else "") or ""
    except Exception:
        ip = ""

    if ip:
        last_ip = int(_reset_ip_cooldown.get(ip) or 0)
        if now - last_ip < cooldown_seconds:
            return JSONResponse(content={
                "success": False,
                "message": "操作过于频繁，请稍后再试。"
            })

    temp_state = _get_temp_password_state()
    if temp_state["valid"]:
        remaining = max(0, temp_state["expires_at"] - now)
        return JSONResponse(content={
            "success": True,
            "message": "临时密码仍在有效期内，请查看容器日志获取临时密码。",
            "data": {
                "expires_at": temp_state["expires_at"],
                "remaining_seconds": remaining,
                "ttl_seconds": ttl_seconds,
            }
        })

    last_reset_at = int(temp_state["last_reset_at"] or 0)
    if now - last_reset_at < cooldown_seconds:
        return JSONResponse(content={
            "success": False,
            "message": "操作过于频繁，请稍后再试。"
        })

    alphabet = string.ascii_letters + string.digits
    temp_password = ''.join(secrets.choice(alphabet) for _ in range(12))
    temp_password_hash = generate_password_hash(temp_password)
    expires_at = now + ttl_seconds

    await config_manager.set_async("admin_temp_password_hash", temp_password_hash)
    await config_manager.set_async("admin_temp_password_expires_at", expires_at)
    await config_manager.set_async("admin_temp_password_last_reset_at", now)

    if ip:
        _reset_ip_cooldown[ip] = now

    get_writer(LogModule.SYSTEM).warning("管理员临时密码已生成，请尽快登录并修改密码。")
    print(
        f"\n[重置密码] 临时管理员密码: {temp_password} (有效期 {ttl_seconds//60} 分钟，过期后失效；原密码仍可用；使用临时密码登录后需修改密码)\n"
    )

    return JSONResponse(content={
        "success": True,
        "message": "已生成临时密码，请查看容器控制台日志获取临时密码。",
        "data": {
            "expires_at": expires_at,
            "remaining_seconds": ttl_seconds,
            "ttl_seconds": ttl_seconds,
        }
    })
