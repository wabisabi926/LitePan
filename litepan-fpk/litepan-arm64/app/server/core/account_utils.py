"""账号与配置公共工具。"""

from typing import Any, Dict, Optional

from database.db import db
from core.error_handler import raise_api_error, raise_not_found


RUNTIME_CONFIG_FIELDS = {
    "last_refresh_time",
    "auth_status",
    "refresh_attempts",
    "status",
    "error_message",
    "last_tested",
}

# 已废弃的配置字段，加载时自动剔除，避免旧数据库残留值传入驱动构造函数
DEPRECATED_CONFIG_FIELDS = {
    "max_retry_attempts",
}

# 别名保留：认证相关模块还在用 AUTH_RUNTIME_FIELDS 这个名字
AUTH_RUNTIME_FIELDS = RUNTIME_CONFIG_FIELDS


def filter_runtime_config(config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    config = config or {}
    return {key: value for key, value in config.items()
            if key not in RUNTIME_CONFIG_FIELDS and key not in DEPRECATED_CONFIG_FIELDS}


async def get_account_or_404(account_id: int) -> Dict[str, Any]:
    account = await db.get_account(account_id)
    if not account:
        raise_not_found("账号")
    return account


def ensure_account_active(account: Dict[str, Any], error_type: str = "account_check") -> None:
    if not account.get("is_active", True):
        raise_api_error("账号未启用", error_type, 400)
