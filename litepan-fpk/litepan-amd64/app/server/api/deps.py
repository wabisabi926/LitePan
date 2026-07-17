"""API 公共依赖。"""

from typing import Optional

from fastapi import Request

from core.error_handler import raise_forbidden, raise_unauthorized
from core.security import get_allowed_cors_origins, is_request_origin_allowed, assess_admin_credential_state
from core.session_manager import session_manager
from core.utils import normalize_bool
from config import config_manager


PASSWORD_CHANGE_EXEMPT_PATHS = {
    "/api/admin/system-config",
    "/api/admin/update-credentials",
}


async def is_public_index_enabled() -> bool:
    value = await config_manager.get_async("public_index_enabled")
    return normalize_bool(value, True)


def get_admin_session_if_any(request: Request) -> Optional[dict]:
    session_data = session_manager.get_session(request)
    if session_data.get("is_admin"):
        return session_data
    return None


async def require_public_index_access(request: Request) -> Optional[dict]:
    admin_session = get_admin_session_if_any(request)
    if admin_session:
        return admin_session

    if await is_public_index_enabled():
        return None

    raise_unauthorized("当前站点未开放匿名文件列表访问，请先登录管理员账号")


async def require_admin_auth(request: Request):
    session_data = session_manager.get_session(request)
    if not session_data.get("is_admin"):
        raise_unauthorized("需要管理员权限")

    if request.method.upper() in {"POST", "PUT", "PATCH", "DELETE"}:
        allowed_origins = get_allowed_cors_origins()
        if not is_request_origin_allowed(request, allowed_origins=allowed_origins):
            raise_forbidden("请求来源不受信任，请从受信任的后台页面重新登录后再试")

    stored_username = config_manager.get("admin_username") or "admin"
    stored_password = config_manager.get("admin_password") or "admin"
    credential_state = assess_admin_credential_state(stored_username, stored_password)
    if session_data.get("must_change_password") and request.url.path not in PASSWORD_CHANGE_EXEMPT_PATHS:
        raise_forbidden("当前会话使用临时密码登录，请先到系统设置修改管理员密码")

    if credential_state["must_change_password"] and request.url.path not in PASSWORD_CHANGE_EXEMPT_PATHS:
        raise_forbidden("检测到管理员仍在使用默认或旧版明文密码，请先到系统设置修改密码")

    return session_data
