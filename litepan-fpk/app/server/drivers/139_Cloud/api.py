"""移动云盘 API 端点、签名计算与请求辅助。"""

import hashlib
import secrets
import string
import time
import base64
from typing import Dict, Any, Optional, Tuple
from urllib.parse import quote


class Cloud139API:
    ROUTE_POLICY_URL = "https://user-njs.yun.139.com/user/route/qryRoutePolicy"
    TOKEN_REFRESH_URL = "https://aas.caiyun.feixin.10086.cn/tellin/authTokenRefresh.do"

    MCLOUD_VERSION = "7.14.0"
    MCLOUD_CLIENT = "10701"
    MCLOUD_CHANNEL = "1000101"
    MCLOUD_CHANNEL_SRC = "10000034"
    DEVICE_INFO = "||9|7.14.0|chrome|120.0.0.0|||windows 10||zh-CN|||"
    CLIENT_INFO = "||9|7.14.0|chrome|120.0.0.0|||windows 10||zh-CN|||dW5kZWZpbmVk||"

    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    HEADERS = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
    }

    ENDPOINTS = {
        "file_list": "/file/list",
        "get_download_url": "/file/getDownloadUrl",
        "create_folder": "/file/create",
        "delete_file": "/recyclebin/batchTrash",
        "permanent_delete": "/file/batchDelete",
        "rename_file": "/file/update",
        "move_file": "/file/batchMove",
        "copy_file": "/file/batchCopy",
        "upload_create": "/file/create",
        "upload_complete": "/file/complete",
    }

    AUTH_ERROR_CODES = {"9000", "9008", "9100", "100002"}


class Cloud139ApiHelper:

    @staticmethod
    def _gen_rand_str(length: int = 16) -> str:
        chars = string.ascii_letters + string.digits
        return "".join(secrets.choice(chars) for _ in range(length))

    @staticmethod
    def _encode_uri_component(s: str) -> str:
        return quote(s, safe="")

    @staticmethod
    def calc_sign(body: str, ts: str, rand_str: str) -> str:
        encoded = Cloud139ApiHelper._encode_uri_component(body)
        chars = sorted(encoded)
        sorted_str = "".join(chars)
        body_base64 = base64.b64encode(sorted_str.encode("utf-8")).decode("utf-8")
        hash1 = hashlib.md5(body_base64.encode()).hexdigest()
        hash2 = hashlib.md5(f"{ts}:{rand_str}".encode()).hexdigest()
        combined = f"{hash1}{hash2}"
        return hashlib.md5(combined.encode()).hexdigest().upper()

    @staticmethod
    def build_base_headers() -> Dict[str, str]:
        return {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json;charset=UTF-8",
            "mcloud-channel": Cloud139API.MCLOUD_CHANNEL,
            "mcloud-client": Cloud139API.MCLOUD_CLIENT,
            "mcloud-version": Cloud139API.MCLOUD_VERSION,
            "Origin": "https://yun.139.com",
            "Referer": "https://yun.139.com/w/",
            "x-DeviceInfo": Cloud139API.DEVICE_INFO,
            "x-huawei-channelSrc": Cloud139API.MCLOUD_CHANNEL_SRC,
            "x-inner-ntwk": "2",
            "x-m4c-caller": "PC",
            "x-m4c-src": "10002",
            "Inner-Hcy-Router-Https": "1",
            "User-Agent": Cloud139API.USER_AGENT,
        }

    @staticmethod
    def build_common_headers() -> Dict[str, str]:
        headers = Cloud139ApiHelper.build_base_headers()
        headers.update({
            "Caller": "web",
            "CMS-DEVICE": "default",
            "mcloud-route": "001",
            "x-yun-api-version": "v1",
            "x-yun-app-channel": Cloud139API.MCLOUD_CHANNEL_SRC,
            "x-yun-channel-source": Cloud139API.MCLOUD_CHANNEL_SRC,
            "x-yun-client-info": Cloud139API.CLIENT_INFO,
            "x-yun-module-type": "100",
        })
        return headers

    @staticmethod
    def build_signed_headers(
        authorization: str, ts: str, rand_str: str, sign: str, svc_type: str = "1"
    ) -> Dict[str, str]:
        headers = Cloud139ApiHelper.build_common_headers()
        headers["x-yun-svc-type"] = svc_type
        headers["x-SvcType"] = svc_type
        headers["Authorization"] = f"Basic {authorization}"
        headers["mcloud-sign"] = f"{ts},{rand_str},{sign}"
        return headers

    @staticmethod
    def build_route_headers(
        authorization: str, ts: str, rand_str: str, sign: str
    ) -> Dict[str, str]:
        headers = Cloud139ApiHelper.build_base_headers()
        headers["x-SvcType"] = "1"
        headers["Authorization"] = f"Basic {authorization}"
        headers["mcloud-sign"] = f"{ts},{rand_str},{sign}"
        return headers

    @staticmethod
    def parse_token(authorization: str) -> Tuple[str, str, int]:
        try:
            decoded = base64.b64decode(authorization).decode("utf-8")
        except Exception:
            raise ValueError("Authorization 令牌格式错误，无法解析")
        parts = decoded.split(":")
        if len(parts) < 3:
            raise ValueError("Authorization 令牌格式不完整")
        account = parts[1]
        token_info = ":".join(parts[2:])
        token_parts = token_info.split("|")
        if len(token_parts) < 4:
            raise ValueError("Token 信息不完整")
        expire_time = int(token_parts[3])
        return account, token_info, expire_time

    @staticmethod
    def get_account(authorization: str) -> str:
        account, _, _ = Cloud139ApiHelper.parse_token(authorization)
        return account

    @staticmethod
    def is_token_expired(authorization: str) -> bool:
        try:
            _, _, expire_time = Cloud139ApiHelper.parse_token(authorization)
            now_ms = int(time.time() * 1000)
            return expire_time - now_ms < 15 * 24 * 60 * 60 * 1000
        except Exception:
            return True
