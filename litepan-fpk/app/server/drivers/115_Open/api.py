"""115 开放 API 端点、字段映射、响应路径集中表。"""

from typing import Dict, Any


class OneOneFiveAPI:
    BASE_URL = "https://proapi.115.com"
    REFRESH_URL = "https://passportapi.115.com/open/refreshToken"
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

    ENDPOINTS = {
        "file_list": "/open/ufile/files",
        "file_info": "/open/folder/get_info", 
        "download": "/open/ufile/downurl",
        "create_folder": "/open/folder/add",
        "upload_init": "/open/upload/init",
        "upload_resume": "/open/upload/resume",
        "upload_get_token": "/open/upload/get_token",
        "rename": "/open/ufile/update",
        "move": "/open/ufile/move",
        "copy": "/open/ufile/copy",
        "delete": "/open/ufile/delete",
        "recycle_list": "/open/rb/list",
        "delete_permanently": "/open/rb/del"
    }
    
    HEADERS = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive"
    }
    
    # 只列出跟内部命名不一致的字段；file_ids / recycle_ids 在 map_params 里会被 join 成逗号串
    FIELD_MAPPING = {
        "file_list": {
            "parent_id": "cid",
            "limit": "limit",
            "offset": "offset"
        },
        "file_info": {
            "file_id": "file_id"
        },
        "create_folder": {
            "parent_id": "pid",
            "name": "file_name"
        },
        "rename": {
            "file_id": "file_id",
            "new_name": "file_name"
        },
        "move": {
            "file_ids": "file_ids",
            "target_parent_id": "to_cid"
        },
        "delete": {
            "file_ids": "file_ids",
            "parent_id": "parent_id"
        },
        "recycle_list": {
            "limit": "limit",
            "offset": "offset"
        },
        "delete_permanently": {
            "recycle_ids": "tid"
        },
        "download": {
            "file_id": "file_id",
            "pick_code": "pick_code"
        }
    }

    RESPONSE_PATHS = {
        "file_list": {
            "files": "data",
            "count": "count"
        },
        "file_info": {
            "file": "data"
        },
        "download": {
            "download_url": "data.url"
        },
        "create_folder": {
            "folder_id": "data.cid",
            "folder_name": "data.file_name"
        },
        "recycle_list": {
            "files": "data",
            "count": "count"
        }
    }


class OneOneFiveConstants:
    FILE_TYPE_FILE = 0
    FILE_TYPE_FOLDER = 1

    SUCCESS_STATE = True

    ERROR_CODE_ACCESS_LIMIT = 406
    ERROR_CODE_AUTH_FAILED = 401

    MAX_FILE_LIST_LIMIT = 1000
    DEFAULT_REQUEST_TIMEOUT = 30

    TOKEN_REFRESH_ADVANCE = 1800
    DEFAULT_TOKEN_EXPIRES = 7200


class OneOneFiveApiHelper:
    @staticmethod
    def map_params(params: Dict[str, Any], operation: str) -> Dict[str, Any]:
        mapping = OneOneFiveAPI.FIELD_MAPPING.get(operation, {})
        if not mapping:
            return params

        result = {}
        for key, value in params.items():
            mapped_key = mapping.get(key, key)

            # file_ids / recycle_ids：115 要求逗号串
            if key in ["file_ids", "recycle_ids"] and isinstance(value, list):
                result[mapped_key] = ','.join(value)
            else:
                result[mapped_key] = value

        return result

    @staticmethod
    def extract_data(response: Dict[str, Any], operation: str) -> Dict[str, Any]:
        paths = OneOneFiveAPI.RESPONSE_PATHS.get(operation, {})
        if not paths:
            return response

        result = {}
        for key, path in paths.items():
            result[key] = OneOneFiveApiHelper._get_value(response, path)

        return result

    @staticmethod
    def _get_value(data: Dict[str, Any], path: str) -> Any:
        current = data
        for key in path.split('.'):
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        return current

    @staticmethod
    def check_success(response: Dict[str, Any]) -> tuple[bool, str]:
        """115 成功态：state=True/1/'true'；其余按错误码翻译成中文。"""
        state = response.get("state", True)

        if state == True or state == 1 or state == "true":
            return True, ""

        code = response.get("code", 0)
        message = response.get("message", "Unknown error")

        if code == OneOneFiveConstants.ERROR_CODE_ACCESS_LIMIT:
            message = f"115 API访问限制: {message}"
        elif code == OneOneFiveConstants.ERROR_CODE_AUTH_FAILED:
            message = "Token认证失败，请检查access_token是否有效或已过期"
        elif str(code).startswith('401'):
            # 115 认证相关错误码都是 401xxx
            message = f"Token认证失败: {message}"

        return False, message

    @staticmethod
    def build_headers(access_token: str = None, endpoint: str = None) -> Dict[str, str]:
        headers = OneOneFiveAPI.HEADERS.copy()

        if access_token:
            headers["Authorization"] = f"Bearer {access_token}"

        if endpoint == OneOneFiveAPI.ENDPOINTS["download"]:
            headers["User-Agent"] = OneOneFiveAPI.USER_AGENT

        return headers

    @staticmethod
    def get_form_data_endpoints() -> list:
        return [
            OneOneFiveAPI.ENDPOINTS["create_folder"],
            OneOneFiveAPI.ENDPOINTS["upload_init"],
            OneOneFiveAPI.ENDPOINTS["upload_resume"],
            OneOneFiveAPI.ENDPOINTS["rename"],
            OneOneFiveAPI.ENDPOINTS["move"],
            OneOneFiveAPI.ENDPOINTS["copy"],
            OneOneFiveAPI.ENDPOINTS["delete"],
            OneOneFiveAPI.ENDPOINTS["delete_permanently"],
            OneOneFiveAPI.ENDPOINTS["download"]
        ]

    @staticmethod
    def build_file_list_params(parent_id: str = "0", limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        params = {
            "parent_id": parent_id,
            "limit": limit,
            "offset": offset
        }
        mapped_params = OneOneFiveApiHelper.map_params(params, "file_list")
        mapped_params["show_dir"] = "1"
        return mapped_params
