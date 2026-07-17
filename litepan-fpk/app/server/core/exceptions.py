"""LitePan 自定义异常类。"""

from typing import Dict, Any, Optional


class LitePanError(Exception):
    def __init__(self, error_type: str, message: str, details: Optional[Dict[str, Any]] = None):
        self.error_type = error_type
        self.message = message
        self.details = details or {}
        super().__init__(f"[{error_type}] {message}")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "error_type": self.error_type,
            "message": self.message,
            "details": self.details
        }


class ConfigError(LitePanError):
    def __init__(self, message: str, config_key: Optional[str] = None):
        details = {"config_key": config_key} if config_key else {}
        super().__init__("CONFIG_ERROR", message, details)


class DriverError(LitePanError):
    def __init__(self, driver_name: str, message: str, operation: Optional[str] = None):
        details = {
            "driver_name": driver_name,
            "operation": operation
        }
        super().__init__("DRIVER_ERROR", message, details)


class DriverConfigError(DriverError):
    def __init__(self, driver_name: str, field: str, message: str):
        super().__init__(
            driver_name,
            f"配置字段 '{field}' 错误: {message}",
            "config_validation"
        )
        self.details["field"] = field


class APIError(LitePanError):
    def __init__(self, operation: str, message: str, status_code: int = 500, response_data: Optional[Dict] = None):
        details = {
            "operation": operation,
            "status_code": status_code,
            "response_data": response_data
        }
        super().__init__("API_ERROR", message, details)
        self.status_code = status_code


class ValidationError(LitePanError):
    def __init__(self, field: str, message: str, value: Any = None):
        details = {
            "field": field,
            "value": value
        }
        super().__init__("VALIDATION_ERROR", f"字段 '{field}' 验证失败: {message}", details)


class AuthenticationError(LitePanError):
    def __init__(self, message: str = "认证失败"):
        super().__init__("AUTH_ERROR", message)


class AuthorizationError(LitePanError):
    def __init__(self, message: str = "权限不足"):
        super().__init__("AUTHORIZATION_ERROR", message)


class ResourceNotFoundError(LitePanError):
    def __init__(self, message: str = "资源不存在"):
        super().__init__("RESOURCE_NOT_FOUND", message)


class CacheError(LitePanError):
    def __init__(self, operation: str, message: str):
        details = {"operation": operation}
        super().__init__("CACHE_ERROR", message, details)


class DatabaseError(LitePanError):
    def __init__(self, operation: str, message: str, table: Optional[str] = None):
        details = {
            "operation": operation,
            "table": table
        }
        super().__init__("DATABASE_ERROR", message, details)


class FileOperationError(LitePanError):
    def __init__(self, operation: str, file_path: str, message: str):
        details = {
            "operation": operation,
            "file_path": file_path
        }
        super().__init__("FILE_OPERATION_ERROR", message, details)


class NetworkError(LitePanError):
    def __init__(self, message: str, url: Optional[str] = None, status_code: Optional[int] = None):
        details = {
            "url": url,
            "status_code": status_code
        }
        super().__init__("NETWORK_ERROR", message, details)


class TimeoutError(LitePanError):
    def __init__(self, operation: str, timeout_seconds: int):
        details = {
            "operation": operation,
            "timeout_seconds": timeout_seconds
        }
        super().__init__("TIMEOUT_ERROR", f"操作 '{operation}' 超时 ({timeout_seconds}秒)", details)


class RateLimitError(LitePanError):
    def __init__(self, message: str = "请求频率过高", retry_after: Optional[int] = None):
        details = {"retry_after": retry_after} if retry_after else {}
        super().__init__("RATE_LIMIT_ERROR", message, details)


class QuotaExceededError(LitePanError):
    def __init__(self, resource: str, current: int, limit: int):
        details = {
            "resource": resource,
            "current": current,
            "limit": limit
        }
        super().__init__(
            "QUOTA_EXCEEDED_ERROR",
            f"资源 '{resource}' 超出限制: {current}/{limit}",
            details
        )


class ConnectionTestError(LitePanError):
    def __init__(self, driver_name: str, technical_error: str, user_friendly_message: str):
        details = {
            "driver_name": driver_name,
            "technical_error": technical_error,
            "user_friendly_message": user_friendly_message
        }
        super().__init__("CONNECTION_TEST_ERROR", user_friendly_message, details)