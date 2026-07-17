"""标准化 API 响应工具。"""

from typing import Any, Dict, Optional, Union
from datetime import datetime
from .exceptions import LitePanError


class APIResponse:
    @staticmethod
    def success(data: Any = None, message: str = "操作成功") -> Dict[str, Any]:
        response = {
            "success": True,
            "message": message,
            "timestamp": datetime.now().isoformat()
        }

        if data is not None:
            response["data"] = data

        return response

    @staticmethod
    def error(
        message: str = "操作失败",
        error_type: str = "UNKNOWN_ERROR",
        details: Optional[Dict[str, Any]] = None,
        status_code: int = 500
    ) -> Dict[str, Any]:
        response = {
            "success": False,
            "message": message,
            "error_type": error_type,
            "timestamp": datetime.now().isoformat()
        }

        if details:
            response["details"] = details

        return response

    @staticmethod
    def from_exception(exception: Union[LitePanError, Exception]) -> Dict[str, Any]:
        if isinstance(exception, LitePanError):
            return APIResponse.error(
                message=exception.message,
                error_type=exception.error_type,
                details=exception.details
            )
        else:
            return APIResponse.error(
                message=str(exception),
                error_type="SYSTEM_ERROR"
            )

    @staticmethod
    def validation_error(errors: Dict[str, str]) -> Dict[str, Any]:
        return APIResponse.error(
            message="数据验证失败",
            error_type="VALIDATION_ERROR",
            details={"validation_errors": errors}
        )

    @staticmethod
    def not_found(resource: str = "资源") -> Dict[str, Any]:
        return APIResponse.error(
            message=f"{resource}不存在",
            error_type="NOT_FOUND",
            details={"resource": resource}
        )

    @staticmethod
    def unauthorized(message: str = "认证失败") -> Dict[str, Any]:
        return APIResponse.error(
            message=message,
            error_type="UNAUTHORIZED"
        )

    @staticmethod
    def forbidden(message: str = "权限不足") -> Dict[str, Any]:
        return APIResponse.error(
            message=message,
            error_type="FORBIDDEN"
        )

    @staticmethod
    def rate_limit(retry_after: Optional[int] = None) -> Dict[str, Any]:
        details = {"retry_after": retry_after} if retry_after else None
        return APIResponse.error(
            message="请求频率过高，请稍后重试",
            error_type="RATE_LIMIT",
            details=details
        )

    @staticmethod
    def paginated(
        data: list,
        page: int,
        page_size: int,
        total: int,
        message: str = "获取成功"
    ) -> Dict[str, Any]:
        total_pages = (total + page_size - 1) // page_size

        return APIResponse.success(
            data={
                "items": data,
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total": total,
                    "total_pages": total_pages,
                    "has_next": page < total_pages,
                    "has_prev": page > 1
                }
            },
            message=message
        )