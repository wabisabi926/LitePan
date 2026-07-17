"""全局错误处理器：FastAPI 异常处理器 + 技术错误到中文提示的映射。"""

import traceback
from typing import Dict, Any
from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from datetime import datetime

from .exceptions import LitePanError, AuthenticationError, AuthorizationError, ResourceNotFoundError
from .response import APIResponse


NETWORK_ERROR_MESSAGES = {
    "connection timeout": "网络连接超时，请检查网络连接或稍后重试",
    "read timeout": "数据读取超时，请检查网络连接",
    "connection refused": "连接被拒绝，服务器可能暂时不可用",
    "connection reset": "连接被重置，请稍后重试",
    "name or service not known": "域名解析失败，请检查网络连接",
    "no route to host": "网络不可达，请检查网络连接",
    "network is unreachable": "网络不可达，请检查网络设置",
    "ssl certificate verify failed": "证书验证失败，请检查网络安全设置",
    "ssl": "SSL连接错误，请检查网络安全设置",
    "certificate": "证书验证失败，请检查网络安全设置",
    "proxy": "代理连接失败，请检查代理设置",
    "getaddrinfo failed": "域名解析失败，请检查网络连接",
}

AUTH_ERROR_MESSAGES = {
    "authentication failed": "认证失败，请检查账号配置",
    "invalid credentials": "账号或密码错误，请重新配置",
    "token expired": "访问令牌已过期，正在自动刷新",
    "invalid token": "访问令牌无效，请重新配置账号",
    "unauthorized": "未授权访问，请检查账号权限",
    "access denied": "访问被拒绝，请检查账号权限",
    "cookie expired": "Cookie已过期，正在自动刷新",
    "invalid cookie": "Cookie无效，请重新配置账号",
    "login required": "需要重新登录，请检查账号配置",
    "invalid session": "会话无效，请重新登录",
    "cookie认证失败": "Cookie认证失败，请检查cookie是否有效或已过期",
}

RATE_LIMIT_ERROR_MESSAGES = {
    "rate limit exceeded": "请求过于频繁，请稍后重试",
    "too many requests": "请求次数过多，请稍后重试",
    "quota exceeded": "配额已用完，请稍后重试",
    "api limit": "API调用次数限制，请稍后重试",
    "throttled": "请求被限流，请稍后重试",
    "frequency limit": "访问频率限制，请稍后重试",
}

FILE_ERROR_MESSAGES = {
    "file not found": "文件不存在或已被删除",
    "folder not found": "文件夹不存在或已被删除",
    "directory not found": "目录不存在或已被删除",
    "permission denied": "权限不足，无法执行此操作",
    "file already exists": "文件已存在，请选择其他名称",
    "folder already exists": "文件夹已存在，请选择其他名称",
    "invalid file name": "文件名包含非法字符",
    "file too large": "文件过大，超出限制",
    "storage full": "存储空间不足",
    "invalid path": "路径无效或包含非法字符",
    "operation not supported": "当前操作不被支持",
}

DRIVER_ERROR_MESSAGES = {
    "driver not found": "驱动不存在或未正确安装",
    "driver initialization failed": "驱动初始化失败，请检查配置",
    "unsupported operation": "当前驱动不支持此操作",
    "configuration error": "驱动配置错误，请检查设置",
    "service unavailable": "服务暂时不可用，请稍后重试",
    "driver error": "驱动运行错误，请检查配置",
}

def get_friendly_error_message(error_message: str) -> str:
    """把原生技术错误转成中文前端提示；匹配不到才回退到“操作失败：原文(截断)”。"""
    if not error_message:
        return "操作失败，请稍后重试"

    error_lower = error_message.lower()

    for key, message in NETWORK_ERROR_MESSAGES.items():
        if key in error_lower:
            return message

    for key, message in AUTH_ERROR_MESSAGES.items():
        if key in error_lower:
            return message

    for key, message in RATE_LIMIT_ERROR_MESSAGES.items():
        if key in error_lower:
            return message

    for key, message in FILE_ERROR_MESSAGES.items():
        if key in error_lower:
            return message

    for key, message in DRIVER_ERROR_MESSAGES.items():
        if key in error_lower:
            return message

    if "404" in error_lower:
        return "请求的资源不存在"
    elif "403" in error_lower:
        return "权限不足，无法访问此资源"
    elif "500" in error_lower:
        return "服务器内部错误，请稍后重试"
    elif "502" in error_lower:
        return "网关错误，服务暂时不可用"
    elif "503" in error_lower:
        return "服务暂时不可用，请稍后重试"
    elif "504" in error_lower:
        return "网关超时，请稍后重试"

    if "timeout" in error_lower:
        return "操作超时，请检查网络连接或稍后重试"
    elif "connection" in error_lower:
        return "网络连接异常，请检查网络设置"
    elif ("json" in error_lower and ("decode" in error_lower or "parse" in error_lower)) or "invalid json" in error_lower:
        return "服务器响应格式错误，请稍后重试"
    elif "encoding" in error_lower:
        return "数据编码错误，请稍后重试"

    # 配置验证类原文本身已经是中文提示，直接透传，不要再包一层“操作失败：”
    if "配置验证失败" in error_message or "验证失败" in error_message:
        if len(error_message) > 100:
            return f"{error_message[:100]}..."
        else:
            return error_message

    if error_message.startswith("操作失败"):
        if len(error_message) > 100:
            return f"{error_message[:100]}..."
        else:
            return error_message
    else:
        if len(error_message) > 100:
            return f"操作失败：{error_message[:100]}..."
        else:
            return f"操作失败：{error_message}"


class ErrorHandler:
    def __init__(self, debug: bool = False):
        self.debug = debug

    def handle_litepan_error(self, error: LitePanError) -> JSONResponse:
        status_code = self._get_status_code_from_error_type(error.error_type)

        friendly_message = get_friendly_error_message(error.message)

        if error.error_type == "CONNECTION_TEST_ERROR":
            # 连接测试错误的 message 本身已经是“添加失败...”的文案，不要再过 friendly 再包一层
            response_data = {
                "success": False,
                "message": error.message,
                "timestamp": datetime.now().isoformat()
            }
        else:
            response_data = APIResponse.from_exception(error)
            response_data["message"] = friendly_message

        if self.debug:
            response_data["traceback"] = traceback.format_exc()

        return JSONResponse(
            status_code=status_code,
            content=response_data
        )

    def handle_http_exception(self, error: HTTPException) -> JSONResponse:
        friendly_message = get_friendly_error_message(str(error.detail))

        response_data = APIResponse.error(
            message=friendly_message,
            error_type="HTTP_ERROR"
        )

        return JSONResponse(
            status_code=error.status_code,
            content=response_data
        )

    def handle_validation_error(self, error: RequestValidationError) -> JSONResponse:
        validation_errors = {}

        for err in error.errors():
            field = ".".join(str(loc) for loc in err["loc"])
            validation_errors[field] = err["msg"]

        response_data = APIResponse.validation_error(validation_errors)

        return JSONResponse(
            status_code=422,
            content=response_data
        )

    def handle_generic_exception(self, error: Exception) -> JSONResponse:
        friendly_message = get_friendly_error_message(str(error))
        # 非 debug 模式下不把技术错误回给前端
        if not self.debug and friendly_message.startswith("操作失败："):
            friendly_message = "服务器内部错误，请稍后重试"

        response_data = APIResponse.error(
            message=friendly_message,
            error_type="INTERNAL_ERROR"
        )

        if self.debug:
            response_data["traceback"] = traceback.format_exc()

        return JSONResponse(
            status_code=500,
            content=response_data
        )

    def _get_status_code_from_error_type(self, error_type: str) -> int:
        status_map = {
            "AUTH_ERROR": 401,
            "AUTHORIZATION_ERROR": 403,
            "VALIDATION_ERROR": 422,
            "NOT_FOUND": 404,
            "RATE_LIMIT_ERROR": 429,
            "CONFIG_ERROR": 400,
            "DRIVER_CONFIG_ERROR": 400,
            "CONNECTION_TEST_ERROR": 400,
            "API_ERROR": 502,
            "NETWORK_ERROR": 502,
            "TIMEOUT_ERROR": 504,
            "QUOTA_EXCEEDED_ERROR": 429,
            "DATABASE_ERROR": 500,
            "CACHE_ERROR": 500,
            "FILE_OPERATION_ERROR": 500,
        }
        
        return status_map.get(error_type, 500)


error_handler = ErrorHandler()


def setup_error_handlers(app):
    @app.exception_handler(LitePanError)
    async def litepan_error_handler(request: Request, exc: LitePanError):
        return error_handler.handle_litepan_error(exc)

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        return error_handler.handle_http_exception(exc)

    @app.exception_handler(StarletteHTTPException)
    async def starlette_http_exception_handler(request: Request, exc: StarletteHTTPException):
        http_exc = HTTPException(status_code=exc.status_code, detail=exc.detail)
        return error_handler.handle_http_exception(http_exc)

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        return error_handler.handle_validation_error(exc)

    @app.exception_handler(Exception)
    async def generic_exception_handler(request: Request, exc: Exception):
        return error_handler.handle_generic_exception(exc)


def set_debug_mode(debug: bool):
    global error_handler
    error_handler.debug = debug


def raise_api_error(message: str, operation: str = "unknown", status_code: int = 500):
    from .exceptions import APIError
    friendly_message = get_friendly_error_message(message)
    raise APIError(operation, friendly_message, status_code)


def raise_validation_error(field: str, message: str, value: Any = None):
    from .exceptions import ValidationError
    raise ValidationError(field, message, value)


def raise_not_found(resource: str = "资源"):
    raise ResourceNotFoundError(f"{resource}不存在")


def raise_unauthorized(message: str = "认证失败"):
    raise AuthenticationError(message)


def raise_forbidden(message: str = "权限不足"):
    raise AuthorizationError(message)


def get_user_friendly_connection_error(driver_name: str, technical_error: str) -> str:
    """连接测试失败时按驱动类型给更具体的中文提示，未匹配到走通用文案。"""
    if not technical_error:
        return "添加失败，请检查网络连接和认证信息"

    error_lower = technical_error.lower()

    if "115" in driver_name.lower() or "oneonefive" in driver_name.lower():
        if "access_token" in error_lower and "格式错误" in error_lower:
            return "添加失败，请检查访问令牌是否正确"
        if "refresh_token" in error_lower and "格式错误" in error_lower:
            return "添加失败，请检查刷新令牌是否正确"
        if "401" in error_lower or "unauthorized" in error_lower:
            return "添加失败，请检查访问令牌是否有效"
        if "403" in error_lower or "forbidden" in error_lower:
            return "添加失败，请检查令牌权限是否足够"
        if "api返回非json内容" in error_lower:
            return "添加失败，请检查认证信息是否正确"

    elif "123" in driver_name.lower():
        if (
            "需要安全验证" in technical_error
            or "请进行验证" in technical_error
            or "安全风险" in technical_error
            or ("验证" in technical_error and "123云盘" in technical_error)
        ):
            return (
                "添加失败：123云盘要求进行安全验证。"
                "请在 LitePan 同一网络环境下打开 123 网页版登录一次或修改密码后重试。"
            )
        if "username" in error_lower or "password" in error_lower:
            return "添加失败，请检查用户名和密码是否正确"
        if "client_id" in error_lower or "client_secret" in error_lower:
            return "添加失败，请检查Client ID和Client Secret是否正确"
        if "401" in error_lower or "unauthorized" in error_lower:
            return "添加失败，请检查认证信息是否有效"
        if "api返回非json内容" in error_lower:
            return "添加失败，请检查认证信息是否正确"

    elif "quark" in driver_name.lower():
        if "cookie" in error_lower:
            return "添加失败，请检查Cookie是否完整且有效"
        if "401" in error_lower or "unauthorized" in error_lower:
            return "添加失败，请检查Cookie是否已过期"
        if "api返回非json内容" in error_lower:
            return "添加失败，请检查Cookie格式是否正确"

    elif "aliyun" in driver_name.lower():
        if "access_token" in error_lower:
            return "添加失败，请检查访问令牌是否正确"
        if "refresh_token" in error_lower:
            return "添加失败，请检查刷新令牌是否正确"
        if "401" in error_lower or "unauthorized" in error_lower:
            return "添加失败，请检查认证信息是否有效"

    elif "baidu" in driver_name.lower():
        if "access_token" in error_lower:
            return "添加失败，请检查访问令牌是否正确"
        if "401" in error_lower or "unauthorized" in error_lower:
            return "添加失败，请检查认证信息是否有效"

    elif "local" in driver_name.lower():
        if "路径不存在" in technical_error or "no such" in error_lower:
            return f"添加失败：{technical_error}。请确认路径已通过 docker-compose volume mount 挂载到容器内"
        if "不是目录" in technical_error or "not a directory" in error_lower:
            return f"添加失败：{technical_error}（必须填写一个目录路径，不是文件路径）"
        if "不可读" in technical_error or "permission" in error_lower:
            return f"添加失败：{technical_error}。请检查容器内该目录的读取权限"
        if technical_error:
            return f"添加失败：{technical_error}"
        return "添加失败：本地路径不可用，请检查路径是否存在并可读"

    if "401" in error_lower or "unauthorized" in error_lower:
        return "添加失败，请检查认证信息是否有效"
    if "403" in error_lower or "forbidden" in error_lower:
        return "添加失败，请检查账号权限是否足够"
    if "timeout" in error_lower or "连接超时" in error_lower:
        return "添加失败，请检查网络连接是否正常"
    if "network" in error_lower or "网络" in error_lower:
        return "添加失败，请检查网络连接是否正常"
    if "api返回非json内容" in error_lower:
        return "添加失败，请检查认证信息是否正确"

    return "添加失败，请检查认证信息和网络连接"


def raise_connection_test_error(driver_name: str, technical_error: str):
    from .exceptions import ConnectionTestError
    user_friendly_message = get_user_friendly_connection_error(driver_name, technical_error)
    raise ConnectionTestError(driver_name, technical_error, user_friendly_message)