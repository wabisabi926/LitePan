"""日志查询与清理 API。"""

from fastapi import APIRouter, Query, Depends
from api.deps import require_admin_auth
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime
import json
from core.error_handler import raise_api_error

from core.log_manager import LogLevel, get_log_manager

router = APIRouter(prefix="/api/logs", tags=["日志管理"])

LOG_MODULE_GROUPS = {
    "system": {
        "name": "系统",
        "color": "#2196F3",
        "modules": ["system", "config", "database"],
    },
    "driver": {
        "name": "驱动",
        "color": "#4CAF50",
        "modules": ["driver", "driver_system", "auth"],
    },
    "file": {
        "name": "文件",
        "color": "#009688",
        "modules": ["file_op", "webdav"],
    },
    "cache": {
        "name": "缓存",
        "color": "#795548",
        "modules": ["cache"],
    },
    "interface": {
        "name": "接口",
        "color": "#FF9800",
        "modules": ["api", "web"],
    },
}

def _get_module_group(module_value: str) -> Dict[str, Any]:
    for group_value, group in LOG_MODULE_GROUPS.items():
        if module_value in group["modules"]:
            return {
                "value": group_value,
                "name": group["name"],
                "color": group["color"],
            }
    return {
        "value": module_value,
        "name": module_value,
        "color": "#64748B",
    }


def _normalize_log_level(value: Any) -> int:
    if value is None:
        return int(LogLevel.INFO)

    try:
        return int(value)
    except (TypeError, ValueError):
        pass

    text = str(value).strip()
    if not text:
        return int(LogLevel.INFO)

    try:
        return int(LogLevel.from_string(text))
    except Exception:
        return int(LogLevel.INFO)


def _normalize_log_details(value: Any) -> Optional[Dict[str, Any]]:
    if isinstance(value, dict):
        return value
    if value in (None, ""):
        return None
    return {"value": value}


def _build_log_entry(entry: Dict[str, Any], index: int) -> "LogEntry":
    level_value = _normalize_log_level(entry.get('level'))
    try:
        log_level = LogLevel(level_value)
    except ValueError:
        log_level = LogLevel.INFO

    module_group = _get_module_group(str(entry.get('module') or 'system'))
    return LogEntry(
        id=index,
        timestamp=str(entry.get('timestamp') or ''),
        level=level_value,
        level_name=log_level.to_string(),
        level_emoji=log_level.to_emoji(),
        module=module_group['value'],
        module_name=module_group['name'],
        module_color=module_group['color'],
        message=str(entry.get('message') or ''),
        details=_normalize_log_details(entry.get('details')),
        account_id=str(entry.get('account_id')) if entry.get('account_id') is not None else None,
        driver_name=str(entry.get('driver_name')) if entry.get('driver_name') is not None else None
    )


def _matches_filters(
    entry: Dict[str, Any],
    level: Optional[int],
    module: Optional[str],
    start_time: Optional[str],
    end_time: Optional[str],
    keyword: Optional[str],
) -> bool:
    if level is not None and _normalize_log_level(entry.get("level")) != int(level):
        return False

    if module:
        grouped_modules = LOG_MODULE_GROUPS.get(module, {}).get("modules")
        entry_module = entry.get("module")
        if grouped_modules:
            if entry_module not in grouped_modules:
                return False
        elif entry_module != module:
            return False

    timestamp = entry.get("timestamp")
    if start_time and timestamp and timestamp < start_time:
        return False
    if end_time and timestamp and timestamp > end_time:
        return False

    if keyword:
        search_text = " ".join([
            str(entry.get("message", "")),
            json.dumps(entry.get("details"), ensure_ascii=False) if entry.get("details") else "",
            str(entry.get("account_id", "")),
            str(entry.get("driver_name", "")),
        ]).lower()
        if keyword.lower() not in search_text:
            return False

    return True



class LogEntry(BaseModel):
    id: int
    timestamp: str
    level: int
    level_name: str
    level_emoji: str
    module: str
    module_name: str
    module_color: str
    message: str
    details: Optional[Dict[str, Any]] = None
    account_id: Optional[str] = None
    driver_name: Optional[str] = None


class LogQueryParams(BaseModel):
    level: Optional[int] = None
    module: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    keyword: Optional[str] = None
    limit: int = 100
    offset: int = 0


@router.get("/", response_model=List[LogEntry])
async def get_logs(
    level: Optional[int] = Query(None, description="日志级别过滤"),
    module: Optional[str] = Query(None, description="模块过滤"),
    start_time: Optional[str] = Query(None, description="开始时间 (ISO格式)"),
    end_time: Optional[str] = Query(None, description="结束时间 (ISO格式)"),
    keyword: Optional[str] = Query(None, description="关键词搜索"),
    limit: int = Query(100, description="返回条数限制"),
    offset: int = Query(0, description="偏移量"),
    session_data: dict = Depends(require_admin_auth)
):
    log_manager = get_log_manager()
    if not log_manager:
        return []

    try:
        logs = log_manager.storage.read_all_logs()
        filtered = [
            entry for entry in logs
            if _matches_filters(entry, level, module, start_time, end_time, keyword)
        ]
        sliced = filtered[offset: offset + limit]
        return [_build_log_entry(entry, offset + index + 1) for index, entry in enumerate(sliced)]
    except Exception as e:
        raise_api_error(f"查询日志失败: {str(e)}", "query_logs")


@router.get("/stats")
async def get_log_stats(session_data: dict = Depends(require_admin_auth)):
    log_manager = get_log_manager()
    if not log_manager:
        return {
            "total": 0,
            "by_level": {},
            "by_module": {},
            "recent_errors": 0,
            "recent_errors_total": 0,
        }

    # 仪表盘"错误已读"机制：用户在仪表盘点击"已读"会写入 dashboard_errors_ack_at，
    # 这里读取该时间戳并传给 storage，让 recent_errors 自动剔除已确认部分。
    ack_at = None
    try:
        from config import config_manager
        ack_iso = await config_manager.get_async('dashboard_errors_ack_at')
        if ack_iso:
            ack_at = datetime.fromisoformat(str(ack_iso))
    except Exception:
        ack_at = None

    try:
        return log_manager.storage.get_stats(ack_at=ack_at)
    except Exception as e:
        raise_api_error(f"获取统计失败: {str(e)}", "get_log_stats")


@router.get("/levels")
async def get_log_levels(session_data: dict = Depends(require_admin_auth)):
    return [
        {
            "value": level.value,
            "name": level.to_string(),
            "emoji": level.to_emoji()
        }
        for level in LogLevel
    ]


@router.get("/modules")
async def get_log_modules(session_data: dict = Depends(require_admin_auth)):
    return [
        {
            "value": group_value,
            "name": group["name"],
            "color": group["color"]
        }
        for group_value, group in LOG_MODULE_GROUPS.items()
    ]


@router.delete("/cleanup")
async def cleanup_old_logs(days: int = Query(30, description="保留天数"), session_data: dict = Depends(require_admin_auth)):
    log_manager = get_log_manager()
    if not log_manager:
        return {"message": "日志系统未初始化", "deleted": 0}

    try:
        deleted_count = await log_manager.storage.cleanup_old_logs(days)
        if int(days) <= 0:
            return {
                "message": "已清空所有日志文件",
                "deleted": deleted_count
            }
        return {
            "message": f"成功清理保留期外日志文件",
            "deleted": deleted_count
        }
    except Exception as e:
        raise_api_error(f"清理日志失败: {str(e)}", "cleanup_logs")


@router.delete("/filtered")
async def delete_filtered_logs(
    level: Optional[int] = Query(None, description="日志级别过滤"),
    module: Optional[str] = Query(None, description="模块过滤"),
    start_time: Optional[str] = Query(None, description="开始时间 (ISO格式)"),
    end_time: Optional[str] = Query(None, description="结束时间 (ISO格式)"),
    keyword: Optional[str] = Query(None, description="关键词搜索"),
    session_data: dict = Depends(require_admin_auth)
):
    log_manager = get_log_manager()
    if not log_manager:
        return {"message": "日志系统未初始化", "deleted": 0}

    try:
        deleted_count = await log_manager.storage.delete_matching_logs(
            lambda entry: _matches_filters(entry, level, module, start_time, end_time, keyword)
        )
        return {
            "message": f"已清理匹配条件的日志 {deleted_count} 条",
            "deleted": deleted_count
        }
    except Exception as e:
        raise_api_error(f"清理筛选日志失败: {str(e)}", "cleanup_filtered_logs")
