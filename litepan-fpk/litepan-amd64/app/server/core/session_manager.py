"""基于 itsdangerous 的 session cookie 管理。"""

import json
from datetime import datetime, timedelta
from core.security import URLSafeTimedSerializer, BadSignature, SignatureExpired
from fastapi import Request, Response


class SessionManager:
    def __init__(self, secret_key: str = None):
        if secret_key is None:
            from config import settings
            secret_key = settings.SECRET_KEY
        
        self.serializer = URLSafeTimedSerializer(secret_key)
        self._session_timeout_cache = None
        self._cache_timestamp = None

    def _is_dev_mode(self) -> bool:
        try:
            from config import settings
            debug_value = getattr(settings, 'DEBUG', None)
            if debug_value is None:
                return True
            return bool(debug_value)
        except Exception:
            return True

    def _should_use_secure_cookie(self, request: Request = None) -> bool:
        try:
            if request is not None:
                forwarded_proto = request.headers.get('x-forwarded-proto', '')
                scheme = forwarded_proto.split(',')[0].strip().lower() if forwarded_proto else request.url.scheme.lower()
                host = request.headers.get('host', '').lower()

                if scheme == 'https':
                    return True

                if scheme == 'http':
                    return False

                if host.startswith('localhost') or host.startswith('127.0.0.1'):
                    return False
        except Exception:
            pass

        return not self._is_dev_mode()

    def create_session(self, response: Response, user_data: dict, remember: bool = False, request: Request = None) -> None:
        session_data = {
            **user_data,
            'created_at': datetime.now().isoformat(),
            'remember': remember
        }
        
        serialized_data = self.serializer.dumps(json.dumps(session_data))

        # remember=True -> 30 天持久 cookie；否则浏览器关闭即失效
        if remember:
            max_age = 2592000
        else:
            max_age = None
        
        response.set_cookie(
            key='admin_session',
            value=serialized_data,
            max_age=max_age,
            httponly=True,
            secure=self._should_use_secure_cookie(request),
            samesite='lax'
        )
    
    def get_session(self, request: Request) -> dict:
        session_cookie = request.cookies.get('admin_session')
        if not session_cookie:
            return {}

        try:
            json_data = self.serializer.loads(session_cookie, max_age=2592000)
            session_data = json.loads(json_data)

            # 非 remember 的会话按 session_timeout 判过期，不能只看 cookie max_age
            if not session_data.get('remember'):
                created_at = datetime.fromisoformat(session_data['created_at'])
                session_timeout = self._get_session_timeout()
                if datetime.now() - created_at > timedelta(seconds=session_timeout):
                    return {}

            return session_data

        except (BadSignature, SignatureExpired, json.JSONDecodeError):
            return {}

    def clear_session(self, response: Response) -> None:
        response.delete_cookie('admin_session')

    def _get_session_timeout(self) -> int:
        # 5 分钟内的配置读取直接走内存缓存
        if (self._session_timeout_cache is not None and
            self._cache_timestamp is not None and
            datetime.now() - self._cache_timestamp < timedelta(minutes=5)):
            return self._session_timeout_cache

        try:
            from config import config_manager, settings
            timeout = config_manager.get('session_timeout')
            if timeout is None:
                timeout = settings.SESSION_TIMEOUT
            self._session_timeout_cache = timeout
            self._cache_timestamp = datetime.now()
            return timeout
        except Exception:
            from config import settings
            return settings.SESSION_TIMEOUT

    def refresh_session_timeout_cache(self) -> None:
        self._session_timeout_cache = None
        self._cache_timestamp = None


session_manager = SessionManager()
