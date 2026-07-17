"""API 统一响应辅助函数。"""

from core.response import APIResponse


def success_response(data=None, message: str = "操作成功") -> dict:
    return APIResponse.success(data=data, message=message)


def error_response(message: str, data=None) -> dict:
    response = APIResponse.error(message=message)
    if data is not None:
        response["data"] = data
    return response
