"""前台只读接口：不经过 admin 鉴权，只给前台页面用。"""

from fastapi import APIRouter, Request

from database.db import db
from core.dependency_container import get_hit_tracker
from core.response import APIResponse
from api.deps import require_public_index_access
from core.registry import get_all_driver_info

router = APIRouter()


def _serialize_public_account(account: dict) -> dict:
    config = account.get("config", {})
    driver_type = account["driver_type"]
    driver_info = get_all_driver_info().get(driver_type, {}) or {}
    return {
        "id": account["id"],
        "name": account["name"],
        "driver_type": driver_type,
        "driver_card_name": driver_info.get("card_name") or _get_fallback_driver_card_name(driver_type),
        "driver_card_color": driver_info.get("card_color") or _get_fallback_driver_card_color(driver_type),
        "driver_card_logo": driver_info.get("card_logo"),
        "is_active": account.get("is_active", True),
        "is_default": account.get("is_default", False),
        "sort_order": account.get("sort_order", 0),
        "status": config.get("status", "unknown"),
        "enabled": account.get("is_active", True),
        "config": {
            "root_folder_id": config.get("root_folder_id", "0"),
            "status": config.get("status", "unknown"),
            "delete_mode": config.get("delete_mode", "trash"),
        },
    }


def _get_fallback_driver_card_name(driver_type: str) -> str:
    names = {
        "pan123": "123",
        "123_reverse": "123",
        "115": "115",
        "115_open": "115",
        "quark": "夸克",
        "quark_reverse": "夸克",
        "baidu": "百度",
        "baidu_open": "百度",
        "189_cloud": "天翼",
        "139_cloud": "移动",
        "guangya": "光鸭",
        "webdav": "DAV",
    }
    return names.get(driver_type, (driver_type or "盘")[:2].upper())


def _get_fallback_driver_card_color(driver_type: str) -> str:
    colors = {
        "pan123": "#4C74DF",
        "123_reverse": "#1890ff",
        "115": "#FF6B35",
        "115_open": "#22A7F0",
        "quark": "#1890FF",
        "quark_reverse": "#7B68EE",
        "baidu": "#2932E1",
        "baidu_open": "#FF4C94",
        "189_cloud": "#FEC52C",
        "139_cloud": "#0391FF",
        "guangya": "#FF7A1A",
        "webdav": "#0d9488",
    }
    return colors.get(driver_type, "#6366f1")


@router.get("/accounts")
async def get_public_accounts(request: Request):
    try:
        await require_public_index_access(request)
        accounts = await db.list_accounts(include_inactive=False)
        public_accounts = [_serialize_public_account(account) for account in accounts]
        return APIResponse.success(
            data=public_accounts,
            message=f"成功获取 {len(public_accounts)} 个公开账号"
        )
    except Exception as e:
        if hasattr(e, "error_type"):
            raise
        return APIResponse.error(message=f"获取公开账号列表失败: {str(e)}")


@router.get("/system-config")
async def get_public_system_config(request: Request):
    try:
        await require_public_index_access(request)
        from config import config_manager

        account_switch_mode = await config_manager.get_async("index_account_switch_mode") or "dropdown"
        if account_switch_mode not in {"dropdown", "floating"}:
            account_switch_mode = "dropdown"
        theme = await config_manager.get_async("theme") or "light"
        if theme not in {"light", "dark", "auto"}:
            theme = "light"

        return APIResponse.success(
            data={
                "index_account_switch_mode": account_switch_mode,
                "theme": theme,
            },
            message="成功获取前台配置"
        )
    except Exception as e:
        if hasattr(e, "error_type"):
            raise
        return APIResponse.error(message=f"获取前台配置失败: {str(e)}")


@router.get("/cache/hit-rate")
async def get_public_cache_hit_rate(request: Request):
    try:
        await require_public_index_access(request)
        hit_tracker = get_hit_tracker()
        if not hit_tracker:
            return APIResponse.success(
                data={"hit_rate": 0},
                message="缓存命中率追踪器未初始化"
            )

        global_stats = await hit_tracker.get_global_stats()
        return APIResponse.success(
            data={"hit_rate": global_stats.get("hit_rate", 0)},
            message="成功获取缓存命中率"
        )
    except Exception as e:
        if hasattr(e, "error_type"):
            raise
        return APIResponse.error(message=f"获取缓存命中率失败: {str(e)}")
