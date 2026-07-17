"""天翼云盘 PC 接口常量与签名工具。"""

import hashlib
import hmac
import random
import re
import uuid
from email.utils import formatdate
from typing import Dict


class Cloud189API:
    WEB_URL = "https://cloud.189.cn"
    AUTH_URL = "https://open.e.189.cn"
    API_URL = "https://api.cloud.189.cn"
    UPLOAD_URL = "https://upload.cloud.189.cn"

    APP_ID = "8025431004"
    CLIENT_TYPE = "10020"
    ACCOUNT_TYPE = "02"
    VERSION = "6.2"
    PC = "TELEPC"
    CHANNEL_ID = "web_cloud.189.cn"
    RETURN_URL = "https://m.cloud.189.cn/zhuanti/2020/loginErrorPc/index.html"

    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )

    HEADERS = {
        "Accept": "application/json;charset=UTF-8",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "User-Agent": USER_AGENT,
        "Referer": WEB_URL,
    }


class Cloud189ApiHelper:
    @staticmethod
    def client_suffix() -> Dict[str, str]:
        return {
            "clientType": Cloud189API.PC,
            "version": Cloud189API.VERSION,
            "channelId": Cloud189API.CHANNEL_ID,
            "rand": f"{random.randrange(100000)}_{random.randrange(10000000000)}",
        }

    @staticmethod
    def http_date() -> str:
        return formatdate(usegmt=True)

    @staticmethod
    def request_path(full_url: str) -> str:
        match = re.search(r"://[^/]+((/[^/\s?#]+)*)", full_url)
        return match.group(1) if match else "/"

    @classmethod
    def signature(cls, session_secret: str, session_key: str, method: str, full_url: str, date: str, params: str = "") -> str:
        request_uri = cls.request_path(full_url)
        text = f"SessionKey={session_key}&Operate={method.upper()}&RequestURI={request_uri}&Date={date}"
        if params:
            text += f"&params={params}"
        digest = hmac.new(session_secret.encode(), text.encode(), hashlib.sha1).hexdigest()
        return digest.upper()

    @classmethod
    def signature_headers(cls, session_key: str, session_secret: str, method: str, full_url: str, params: str = "") -> Dict[str, str]:
        date = cls.http_date()
        return {
            "Date": date,
            "SessionKey": session_key,
            "X-Request-ID": str(uuid.uuid4()),
            "Signature": cls.signature(session_secret, session_key, method, full_url, date, params),
        }
