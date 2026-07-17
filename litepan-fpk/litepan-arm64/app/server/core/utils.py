"""通用工具函数。"""

from typing import Any


def normalize_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in ("0", "false", "no", "off", ""):
        return False
    if text in ("1", "true", "yes", "on"):
        return True
    return default
