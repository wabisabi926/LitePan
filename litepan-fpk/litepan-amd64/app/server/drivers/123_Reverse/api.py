"""123 云盘API 端点、字段映射、签名辅助。"""

import math
import random
import time
import zlib
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlencode, urlparse


class Pan123ReverseAPI:
    DYDOMAIN_URL = "https://yun.123pan.cn/api/dydomain"
    DEFAULT_API_BASE = "https://api.123pan.cn/b/api"
    WEB_ORIGIN = "https://yun.123pan.cn"

    ENDPOINT_PATHS = {
        "login": "user/sign_in",
        "logout": "user/logout",
        "user_info": "user/info",
        "file_list": "file/list/new",
        "file_info": "file/info",
        "download_info": "file/download_info",
        "create_folder": "file/upload_request",
        "upload_request": "file/upload_request",
        "s3_auth": "file/s3_upload_object/auth",
        "s3_presigned_urls": "file/s3_repare_upload_parts_batch",
        "upload_complete": "file/upload_complete",
        "upload_complete_v2": "file/upload_complete/v2",
        "rename": "file/rename",
        "move": "file/mod_pid",
        "trash": "file/trash",
        "delete": "file/delete",
        "copy": "restful/goapi/v1/file/copy/async",
        "copy_task": "restful/goapi/v1/file/copy/task",
    }

    HEADERS = {
        "origin": WEB_ORIGIN,
        "referer": f"{WEB_ORIGIN}/",
        "user-agent": "Dart/2.19(dart:io)-openlist",
        "platform": "web",
        "App-Version": "3",
        "content-type": "application/json;charset=UTF-8",
    }

    FIELD_MAPPING = {
        "login": {
            "username": "passport",
            "email": "mail",
        },
        "file_list": {
            "parent_id": "parentFileId",
            "page": "Page",
            "limit": "limit",
            "next": "next",
            "orderBy": "orderBy",
            "orderDirection": "orderDirection",
            "trashed": "trashed",
            "SearchData": "SearchData",
            "OnlyLookAbnormalFile": "OnlyLookAbnormalFile",
            "event": "event",
            "operateType": "operateType",
            "inDirectSpace": "inDirectSpace",
            "fileCategory": "fileCategory",
            "isSearchOrder": "isSearchOrder",
        },
        "download_info": {
            "file_id": "fileId",
            "file_name": "fileName",
            "s3_key_flag": "s3keyFlag",
        },
        "create_folder": {
            "parent_id": "parentFileId",
            "folder_name": "fileName",
        },
        "rename": {
            "file_id": "fileId",
            "new_name": "fileName",
        },
        "move": {
            "file_ids": "fileIdList",
            "target_parent_id": "parentFileId",
        },
        "trash": {
            "file_ids": "fileTrashInfoList",
        },
        "delete": {
            "file_ids": "fileTrashInfoList",
        },
    }

    RESPONSE_PATHS = {
        "login": {
            "token": "data.token",
            "code": "code",
            "message": "message",
        },
        "user_info": {
            "user_id": "data.UID",
            "username": "data.Nickname",
            "space_used": "data.SpaceUsed",
            "space_total": "data.SpacePermanent",
        },
        "file_list": {
            "files": "data.InfoList",
            "next": "data.Next",
            "total": "data.Total",
        },
        "file_info": {
            "file_data": "data",
        },
        "download_info": {
            "download_url": "data.DownloadUrl",
        },
        "upload_request": {
            "access_key_id": "data.AccessKeyId",
            "bucket": "data.Bucket",
            "key": "data.Key",
            "secret_access_key": "data.SecretAccessKey",
            "session_token": "data.SessionToken",
            "file_id": "data.FileId",
            "reuse": "data.Reuse",
            "end_point": "data.EndPoint",
            "storage_node": "data.StorageNode",
            "upload_id": "data.UploadId",
        },
        "s3_auth": {
            "presigned_urls": "data.presignedUrls",
        },
        "s3_presigned_urls": {
            "presigned_urls": "data.presignedUrls",
        },
        "upload_complete": {},
        "upload_complete_v2": {},
        "create_folder": {},
        "rename": {},
        "move": {},
        "trash": {},
        "delete": {},
        "copy": {
            "task_id": "data.taskId",
        },
        "copy_task": {
            "status": "data.status",
            "error_code": "data.errorCode",
            "reason": "data.reason",
            "task_id": "data.taskId",
        },
    }


class Pan123ReverseConstants:
    FILE_TYPE_FILE = 0
    FILE_TYPE_FOLDER = 1

    SUCCESS_CODE = 0
    LOGIN_SUCCESS_CODE = 200

    OS_TYPE = "web"
    VERSION = "3"

    MAX_FILE_LIST_LIMIT = 100
    DEFAULT_REQUEST_TIMEOUT = 30

    VERIFICATION_CODES = {11000, 5002, 5104, 5107, 5300, 5205}
    TRACELESS_OK_CODES = {0, 100, 200, None}

    SIGN_TABLE = [
        "a", "d", "e", "f", "g", "h", "l", "m", "y", "i",
        "j", "n", "o", "p", "k", "q", "r", "s", "t", "u",
        "b", "c", "v", "w", "s", "z",
    ]


class Pan123ReverseApiHelper:
    _api_base = Pan123ReverseAPI.DEFAULT_API_BASE

    @classmethod
    def configure(cls, api_base: str) -> None:
        cls._api_base = api_base.rstrip("/")

    @classmethod
    def get_api_base(cls) -> str:
        return cls._api_base

    @classmethod
    def get_endpoint(cls, operation: str) -> str:
        path = Pan123ReverseAPI.ENDPOINT_PATHS[operation]
        return f"{cls._api_base}/{path}"

    @classmethod
    def resolve_api_base_from_dydomain(cls, payload: Dict[str, Any]) -> str:
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            return Pan123ReverseAPI.DEFAULT_API_BASE

        cpp_domain = str(data.get("cppClientDomain") or "").strip()
        if cpp_domain:
            host = cpp_domain if cpp_domain.startswith("http") else f"https://{cpp_domain}"
            return f"{host.rstrip('/')}/b/api"

        share_domains = data.get("shareDomains") or []
        for domain in ("www.123pan.cn", "www.123pan.com"):
            if domain in share_domains:
                return f"https://{domain}/b/api"

        return Pan123ReverseAPI.DEFAULT_API_BASE

    @staticmethod
    def is_verification_error(message: str, code: Any = None) -> bool:
        if code in Pan123ReverseConstants.VERIFICATION_CODES:
            return True
        text = str(message or "")
        keywords = ("验证", "安全风险", "短信", "微信", "请进行", "captcha", "verify")
        return any(keyword in text for keyword in keywords)

    @staticmethod
    def format_auth_error(message: str, code: Any = None) -> str:
        if Pan123ReverseApiHelper.is_verification_error(message, code):
            return f"123云盘需要安全验证: {message}"
        return f"123云盘登录失败: {message}"

    @staticmethod
    def map_params(params: Dict[str, Any], operation: str) -> Dict[str, Any]:
        mapping = Pan123ReverseAPI.FIELD_MAPPING.get(operation, {})
        mapped_params: Dict[str, Any] = {}

        for key, value in params.items():
            mapped_key = mapping.get(key, key)
            mapped_params[mapped_key] = value

        if operation == "file_list":
            mapped_params.update(
                {
                    "driveId": "0",
                    "next": "0",
                    "orderBy": "update_time",
                    "orderDirection": "desc",
                    "trashed": "false",
                    "SearchData": "",
                    "OnlyLookAbnormalFile": "0",
                    "event": "homeListFile",
                    "operateType": "1",
                    "inDirectSpace": "false",
                    "fileCategory": "0",
                    "isSearchOrder": "false",
                }
            )
        elif operation == "create_folder":
            mapped_params.update(
                {
                    "driveId": 0,
                    "etag": "",
                    "size": 0,
                    "type": 1,
                }
            )
        elif operation == "rename":
            mapped_params.update({"driveId": 0})
        elif operation == "move":
            file_ids = params.get("file_ids", [])
            if isinstance(file_ids, list):
                mapped_params["fileIdList"] = [{"FileId": int(file_id)} for file_id in file_ids]
        elif operation == "trash":
            file_ids = params.get("file_ids", [])
            if isinstance(file_ids, list):
                mapped_params["fileTrashInfoList"] = [
                    {
                        "fileId": int(file_id),
                        "fileName": "",
                        "size": 0,
                        "type": 0,
                    }
                    for file_id in file_ids
                ]
                mapped_params["driveId"] = 0
                mapped_params["operation"] = True
        elif operation == "delete":
            file_ids = params.get("file_ids", [])
            if isinstance(file_ids, list):
                mapped_params["fileIdList"] = [{"fileId": int(file_id)} for file_id in file_ids]
                mapped_params["event"] = "recycleDelete"
                mapped_params["operatePlace"] = 1
                mapped_params["RequestSource"] = None

        return mapped_params

    @staticmethod
    def extract_data(response: Dict[str, Any], operation: str) -> Optional[Dict[str, Any]]:
        paths = Pan123ReverseAPI.RESPONSE_PATHS.get(operation)
        if paths is None:
            return None

        extracted: Dict[str, Any] = {}
        for key, path in paths.items():
            value = Pan123ReverseApiHelper._get_value(response, path)
            if value is not None:
                extracted[key] = value

        if operation == "upload_request":
            data = response.get("data", {})
            info = data.get("Info") if isinstance(data, dict) else None
            if isinstance(info, dict):
                info_file_id = info.get("FileId")
                if info_file_id not in (None, "", 0, "0"):
                    extracted["file_id"] = info_file_id
                extracted["info"] = info
        return extracted

    @staticmethod
    def _get_value(data: Dict[str, Any], path: str) -> Any:
        keys = path.split(".")
        current: Any = data
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        return current

    @staticmethod
    def _check_login_traceless(response: Dict[str, Any]) -> tuple[bool, str]:
        data_block = response.get("data")
        if not isinstance(data_block, dict):
            return True, ""

        traceless = data_block.get("traceless")
        if not isinstance(traceless, dict):
            return True, ""

        traceless_code = traceless.get("code")
        if traceless_code in Pan123ReverseConstants.TRACELESS_OK_CODES:
            return True, ""

        message = response.get("message") or "请完成安全验证"
        return False, Pan123ReverseApiHelper.format_auth_error(str(message), traceless_code)

    @staticmethod
    def check_success(response: Dict[str, Any], operation: str = "default") -> tuple[bool, str]:
        code = response.get("code")
        message = response.get("message", "未知错误")

        if operation == "login":
            if code == Pan123ReverseConstants.LOGIN_SUCCESS_CODE:
                return Pan123ReverseApiHelper._check_login_traceless(response)
            if Pan123ReverseApiHelper.is_verification_error(str(message), code):
                return False, Pan123ReverseApiHelper.format_auth_error(str(message), code)
        elif operation in {"trash", "delete"}:
            if code == Pan123ReverseConstants.SUCCESS_CODE or ("已删除" in str(message)):
                return True, ""
        else:
            if code == Pan123ReverseConstants.SUCCESS_CODE:
                return True, ""

        if Pan123ReverseApiHelper.is_verification_error(str(message), code):
            return False, Pan123ReverseApiHelper.format_auth_error(str(message), code)

        return False, str(message)

    @staticmethod
    def build_headers(
        access_token: Optional[str] = None,
        login_uuid: Optional[str] = None,
    ) -> Dict[str, str]:
        headers = Pan123ReverseAPI.HEADERS.copy()
        if login_uuid:
            headers["LoginUuid"] = login_uuid
        if access_token:
            headers["authorization"] = f"Bearer {access_token}"
        return headers

    @staticmethod
    def sign_path(path: str) -> tuple[str, str]:
        random_num = str(int(math.floor(random.random() * 1e7)))
        now = time.time() + 8 * 3600
        timestamp = str(int(now))

        now_str = time.strftime("%Y%m%d%H%M", time.gmtime(now))
        time_chars = [Pan123ReverseConstants.SIGN_TABLE[int(char)] for char in now_str]
        time_sign = str(zlib.crc32("".join(time_chars).encode()) & 0xFFFFFFFF)

        data = (
            f"{timestamp}|{random_num}|{path}|"
            f"{Pan123ReverseConstants.OS_TYPE}|{Pan123ReverseConstants.VERSION}|{time_sign}"
        )
        data_sign = str(zlib.crc32(data.encode()) & 0xFFFFFFFF)
        return time_sign, f"{timestamp}-{random_num}-{data_sign}"

    @staticmethod
    def get_signed_url(raw_url: str) -> str:
        parsed = urlparse(raw_url)
        time_sign, signature = Pan123ReverseApiHelper.sign_path(parsed.path)
        query_params = parse_qs(parsed.query)
        query_params[time_sign] = [signature]
        new_query = urlencode(query_params, doseq=True)
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{new_query}"
