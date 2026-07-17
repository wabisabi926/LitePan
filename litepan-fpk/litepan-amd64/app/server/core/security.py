import os
import time
import json
import base64
import hmac
import hashlib
import binascii
from urllib.parse import urlparse

PASSWORD_HASH_PREFIXES = ('pbkdf2:', 'scrypt:')
DEFAULT_DEV_CORS_ORIGINS = (
    "http://127.0.0.1:5211",
    "http://localhost:5211",
    "http://127.0.0.1:5212",
    "http://localhost:5212",
    "http://[::1]:5211",
    "http://[::1]:5212",
)

# 密码哈希格式：pbkdf2:sha256:iterations$salt$hash，兼容 Werkzeug
def generate_password_hash(password: str, method: str = 'pbkdf2:sha256', salt_length: int = 16) -> str:
    if method != 'pbkdf2:sha256':
        method = 'pbkdf2:sha256'
    
    iterations = 600000
    salt = os.urandom(salt_length).hex()
    
    rv = hashlib.pbkdf2_hmac(
        'sha256',
        password.encode('utf-8'),
        salt.encode('utf-8'),
        iterations
    )
    
    hash_hex = binascii.hexlify(rv).decode('utf-8')
    return f"{method}:{iterations}${salt}${hash_hex}"

def check_password_hash(pwhash: str, password: str) -> bool:
    if pwhash.count('$') < 2:
        return False
    
    method, salt, hashval = pwhash.split('$', 2)
    
    if method.startswith('pbkdf2:'):
        parts = method.split(':')
        if len(parts) == 3:
            algo, iterations = parts[1], int(parts[2])
        else:
            algo, iterations = parts[1], 600000
            
        try:
            rv = hashlib.pbkdf2_hmac(
                algo, 
                password.encode('utf-8'), 
                salt.encode('utf-8'), 
                iterations
            )
            return hmac.compare_digest(binascii.hexlify(rv).decode('utf-8'), hashval)
        except ValueError:
            return False
            
    elif method.startswith('scrypt:'):
        parts = method.split(':')
        try:
            n, r, p = int(parts[1]), int(parts[2]), int(parts[3])
            rv = hashlib.scrypt(
                password.encode('utf-8'), 
                salt=salt.encode('utf-8'), 
                n=n, r=r, p=p
            )
            return hmac.compare_digest(binascii.hexlify(rv).decode('utf-8'), hashval)
        except (ValueError, AttributeError):
            return False
            
    return False


class BadSignature(Exception):
    pass

class SignatureExpired(BadSignature):
    pass


def is_password_hash(value: str) -> bool:
    text = str(value or "").strip()
    return text.startswith(PASSWORD_HASH_PREFIXES)


def assess_admin_credential_state(username: str, stored_password: str) -> dict:
    """判断当前管理员凭据状态：默认 admin/admin 或历史明文密码会被要求强制改密。"""
    normalized_username = str(username or "").strip()
    normalized_password = str(stored_password or "").strip()

    default_credentials = normalized_username == "admin" and normalized_password == "admin"
    legacy_plaintext_password = bool(normalized_password) and not is_password_hash(normalized_password)

    if default_credentials:
        reason = "default_credentials"
    elif legacy_plaintext_password:
        reason = "legacy_plaintext_password"
    else:
        reason = ""

    return {
        "is_default_credentials": default_credentials,
        "is_legacy_plaintext_password": legacy_plaintext_password,
        "must_change_password": default_credentials or legacy_plaintext_password,
        "password_change_reason": reason,
    }


def _normalize_origin(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""

    parsed = urlparse(text)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}"
    return text.rstrip("/").lower()


def parse_cors_origins(origins_value: str | None) -> list[str]:
    """解析 LITEPAN_CORS_ORIGINS 白名单，分隔符兼容 , ; 和换行。"""
    raw_value = str(origins_value or "").strip()
    if not raw_value:
        return []

    for separator in (";", "\n"):
        raw_value = raw_value.replace(separator, ",")

    result = []
    seen = set()
    for item in raw_value.split(","):
        normalized = _normalize_origin(item)
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def get_allowed_cors_origins() -> list[str]:
    configured = parse_cors_origins(os.getenv("LITEPAN_CORS_ORIGINS"))
    if configured:
        return configured
    return list(DEFAULT_DEV_CORS_ORIGINS)


def get_request_base_url(request) -> str:
    # 反代场景优先信任 X-Forwarded-*，保证 http/https 判断和白名单匹配一致
    forwarded_proto = (request.headers.get("x-forwarded-proto") or "").split(",")[0].strip()
    forwarded_host = (request.headers.get("x-forwarded-host") or "").split(",")[0].strip()
    host = forwarded_host or (request.headers.get("host") or "").strip()
    scheme = forwarded_proto or request.url.scheme
    if host:
        return f"{scheme}://{host}".rstrip("/").lower()
    return str(request.base_url).rstrip("/").lower()


def get_request_origin(request) -> str:
    origin = _normalize_origin(request.headers.get("origin"))
    if origin:
        return origin
    referer = _normalize_origin(request.headers.get("referer"))
    return referer


def is_request_origin_allowed(request, allowed_origins: list[str] | None = None) -> bool:
    """基于 Origin/Referer 做的 CSRF 边界：同源直接过，否则要命中白名单。"""
    request_origin = get_request_origin(request)
    if not request_origin:
        return True

    if request_origin == _normalize_origin(get_request_base_url(request)):
        return True

    normalized_allowed = {_normalize_origin(item) for item in (allowed_origins or get_allowed_cors_origins()) if item}
    return request_origin in normalized_allowed

class URLSafeTimedSerializer:
    def __init__(self, secret_key: str):
        self.secret_key = secret_key.encode('utf-8')

    def _b64encode(self, data: bytes) -> str:
        return base64.urlsafe_b64encode(data).decode('utf-8').rstrip('=')

    def _b64decode(self, data: str) -> bytes:
        data += '=' * (-len(data) % 4)
        return base64.urlsafe_b64decode(data)

    def dumps(self, obj) -> str:
        payload = {'d': obj, 't': int(time.time())}
        payload_json = json.dumps(payload).encode('utf-8')
        encoded_payload = self._b64encode(payload_json)

        sig = hmac.new(self.secret_key, encoded_payload.encode('utf-8'), hashlib.sha256).digest()
        encoded_sig = self._b64encode(sig)

        return f"{encoded_payload}.{encoded_sig}"

    def loads(self, s: str, max_age: int = None):
        try:
            if '.' not in s:
                raise ValueError("Invalid token format")

            encoded_payload, encoded_sig = s.rsplit('.', 1)

            expected_sig = hmac.new(self.secret_key, encoded_payload.encode('utf-8'), hashlib.sha256).digest()
            expected_encoded_sig = self._b64encode(expected_sig)

            if not hmac.compare_digest(encoded_sig, expected_encoded_sig):
                raise ValueError("Signature mismatch")

            payload_json = self._b64decode(encoded_payload)
            payload = json.loads(payload_json)

            if max_age is not None:
                if int(time.time()) - payload['t'] > max_age:
                    raise ValueError("Signature expired")

            return payload['d']
        except ValueError as e:
            if str(e) == "Signature expired":
                raise SignatureExpired("Signature expired")
            raise BadSignature(str(e))
        except Exception as e:
            raise BadSignature(str(e))
