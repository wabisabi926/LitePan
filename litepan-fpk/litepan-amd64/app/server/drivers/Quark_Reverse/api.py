"""夸克网盘 API 端点、字段映射与辅助方法。"""

from typing import Dict, Any


class QuarkAPI:
    BASE_URL = "https://drive.quark.cn/1/clouddrive"
    REFERER = "https://pan.quark.cn"
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) quark-cloud-drive/2.5.20 Chrome/100.0.4896.160 Electron/18.3.5.4-b478491100 Safari/537.36 Channel/pckk_other_ch"

    ENDPOINTS = {
        "file_list": "/file/sort",
        "download": "/file/download",
        "create_folder": "/file",
        "upload_pre": "/file/upload/pre",
        "update_hash": "/file/update/hash",
        "upload_auth": "/file/upload/auth",
        "upload_finish": "/file/upload/finish",
        "rename": "/file/rename",
        "move": "/file/move",
        "trash": "/file/delete",              # 移入回收站
        "recycle_list": "/file/recycle/list",  # 回收站列表
        "delete": "/file/recycle/remove",      # 永久删除
        "copy": "/file/copy",
        "task": "/task",
    }

    HEADERS = {
        "User-Agent": USER_AGENT,
        "Referer": REFERER,
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*"
    }

    FIELD_MAPPING = {
        "file_list": {
            "parent_id": "pdir_fid",
            "page": "_page",
            "size": "_size",
            "fetch_total": "_fetch_total"
        },
        "download": {
            "file_ids": "fids"
        },
        "create_folder": {
            "parent_id": "pdir_fid",
            "name": "file_name"
        },
        "rename": {
            "file_id": "fid",
            "new_name": "file_name"
        },
        "move": {
            "file_ids": "fids",
            "target_parent_id": "pdir_fid"
        },
        "trash": {
            "file_ids": "filelist",
            "action_type": "action_type"
        },
        "recycle_list": {
            "page": "_page",
            "size": "_size"
        },
        "delete": {
            "recycle_ids": "record_list",
            "select_mode": "select_mode"
        },
        "copy": {
            "file_ids": "filelist",
            "target_parent_id": "to_pdir_fid"
        },
    }
    
    RESPONSE_PATHS = {
        "file_list": {
            "files": "data.list",
            "total": "metadata._total"
        },
        "download": {
            "download_url": "data.list.0.download_url"
        },
        "create_folder": {
            "folder_id": "data.fid"
        },
        "recycle_list": {
            "files": "data.list"
        },
        "copy": {
            "task_id": "data.task_id"
        },
        "task": {
            "task_status": "data.status",
            "task_id": "data.task_id"
        }
    }


class QuarkConstants:
    FILE_TYPE_FILE = 1
    FILE_TYPE_FOLDER = 0

    SUCCESS_CODE = 0

    MAX_FILE_LIST_LIMIT = 200
    DEFAULT_REQUEST_TIMEOUT = 30

    DEFAULT_PARAMS = {
        "pr": "ucpro",
        "fr": "pc"
    }

    OPERATION_PARAMS: Dict[str, Dict[str, str]] = {}

    QUARK_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) quark-cloud-drive/2.5.20 Chrome/100.0.4896.160 Electron/18.3.5.4-b478491100 Safari/537.36 Channel/pckk_other_ch"

    COOKIE_ATTR_KEYS = frozenset({
        "path", "domain", "expires", "max-age",
        "httponly", "secure", "samesite", "priority",
        "partitioned", "sameparty",
    })

    COOKIE_IGNORE_KEYS = frozenset({
        "_gid", "_ga", "_ga_*",
        "isg", "l",
    })


class QuarkApiHelper:
    @staticmethod
    def map_params(params: Dict[str, Any], operation: str) -> Dict[str, Any]:
        mapping = QuarkAPI.FIELD_MAPPING.get(operation, {})
        mapped_params = {}

        for key, value in params.items():
            mapped_key = mapping.get(key, key)
            mapped_params[mapped_key] = value

        return mapped_params

    @staticmethod
    def extract_data(response: Dict[str, Any], operation: str) -> Dict[str, Any]:
        paths = QuarkAPI.RESPONSE_PATHS.get(operation, {})
        extracted = {}

        for key, path in paths.items():
            value = QuarkApiHelper._get_value(response, path)
            if value is not None:
                extracted[key] = value

        return extracted

    @staticmethod
    def _get_value(data: Dict[str, Any], path: str) -> Any:
        keys = path.split('.')
        current = data

        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None

        return current

    @staticmethod
    def check_success(response: Dict[str, Any]) -> tuple[bool, str]:
        status = response.get('status', 0)
        code = response.get('code', 0)
        message = response.get('message', '未知错误')

        if status >= 400 or code != QuarkConstants.SUCCESS_CODE:
            return False, message

        return True, ""

    @staticmethod
    def build_headers(cookie: str = None) -> Dict[str, str]:
        headers = QuarkAPI.HEADERS.copy()
        if cookie:
            headers['Cookie'] = cookie
        return headers
