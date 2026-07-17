"""STRM 播放链接签名与路径编码工具。"""

import base64
import hashlib
import hmac
import urllib.parse

from config import Settings


def build_strm_play_path(account_id: int, file_id: str) -> str:
    encoded_file_id = urllib.parse.quote(str(file_id).lstrip("/"), safe="/")
    return f"/api/strm/play/{int(account_id)}/{encoded_file_id}"


def encode_strm_file_key(file_id: str) -> str:
    raw = str(file_id or "").encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_strm_file_key(file_key: str) -> str:
    text = str(file_key or "").strip()
    if not text:
        return ""
    padding = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode((text + padding).encode("ascii")).decode("utf-8")


def build_strm_v2_base_path(account_id: int, file_id: str, token: str) -> str:
    file_key = encode_strm_file_key(file_id)
    token_segment = urllib.parse.quote(str(token or "").strip(), safe="")
    return f"/api/strm/v2/play/{int(account_id)}/{file_key}/t/{token_segment}"


def build_strm_v2_play_path(account_id: int, file_id: str, token: str, signature_enabled: bool = False) -> str:
    base_path = build_strm_v2_base_path(account_id, file_id, token)
    if signature_enabled:
        return f"{base_path}/s/{sign_strm_path(base_path)}"
    return base_path


def sign_strm_path(path: str) -> str:
    secret = str(Settings.SECRET_KEY or "litepan-strm").encode("utf-8")
    digest = hmac.new(secret, str(path).encode("utf-8"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).decode("utf-8").rstrip("=")


def verify_strm_signature(path: str, signature: str) -> bool:
    expected = sign_strm_path(path)
    return hmac.compare_digest(expected, str(signature or "").strip())
