from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from api.deps import require_admin_auth
from core.error_handler import raise_api_error
from core.plugin_system import plugin_manager


router = APIRouter(prefix="/api/plugins", tags=["插件"])


class PluginTogglePayload(BaseModel):
    enabled: bool


class PluginConfigPayload(BaseModel):
    config: Dict[str, Any]


class PluginActionPayload(BaseModel):
    payload: Dict[str, Any] = Field(default_factory=dict)


class PluginSearchPayload(BaseModel):
    keyword: str
    page: int = 1
    plugin_id: Optional[str] = None


class PluginSyncPayload(BaseModel):
    force: bool = True


class PluginSearchJobPayload(BaseModel):
    plugin_id: Optional[str] = None
    keyword: str
    page: int = 1


@router.get("")
async def list_plugins(_session_data: dict = Depends(require_admin_auth)):
    return {"success": True, "data": plugin_manager.list_plugins()}


@router.post("/rescan")
async def rescan_plugins(_session_data: dict = Depends(require_admin_auth)):
    try:
        plugins = await plugin_manager.scan_plugins()
        return {"success": True, "data": plugins, "message": "插件扫描完成"}
    except Exception as e:
        raise_api_error(f"扫描插件失败: {e}", "rescan_plugins")


@router.get("/{plugin_id}/ui/{asset_path:path}")
async def get_plugin_ui_asset(plugin_id: str, asset_path: str, _session_data: dict = Depends(require_admin_auth)):
    try:
        file_path = plugin_manager.get_plugin_ui_file(plugin_id, asset_path)
        return FileResponse(
            str(file_path),
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0",
            }
        )
    except KeyError as e:
        raise_api_error(str(e), "get_plugin_ui_asset", 404)
    except FileNotFoundError as e:
        raise_api_error(str(e), "get_plugin_ui_asset", 404)
    except Exception as e:
        raise_api_error(f"获取插件前台资源失败: {e}", "get_plugin_ui_asset")


@router.post("/{plugin_id}/toggle")
async def toggle_plugin(plugin_id: str, payload: PluginTogglePayload, _session_data: dict = Depends(require_admin_auth)):
    try:
        plugin = await plugin_manager.set_plugin_enabled(plugin_id, payload.enabled)
        return {"success": True, "data": plugin, "message": "插件状态已更新"}
    except KeyError as e:
        raise_api_error(str(e), "toggle_plugin", 404)
    except Exception as e:
        raise_api_error(f"更新插件状态失败: {e}", "toggle_plugin")


@router.put("/{plugin_id}/config")
async def update_plugin_config(plugin_id: str, payload: PluginConfigPayload, _session_data: dict = Depends(require_admin_auth)):
    try:
        plugin = await plugin_manager.update_plugin_config(plugin_id, payload.config)
        return {"success": True, "data": plugin, "message": "插件配置已更新"}
    except KeyError as e:
        raise_api_error(str(e), "update_plugin_config", 404)
    except Exception as e:
        raise_api_error(f"更新插件配置失败: {e}", "update_plugin_config")


@router.post("/{plugin_id}/actions/{action}")
async def execute_plugin_action(plugin_id: str, action: str, payload: PluginActionPayload, _session_data: dict = Depends(require_admin_auth)):
    try:
        result = await plugin_manager.execute_plugin_action(plugin_id, action, payload.payload)
        return {"success": True, "data": result}
    except KeyError as e:
        raise_api_error(str(e), "execute_plugin_action", 404)
    except Exception as e:
        raise_api_error(f"执行插件动作失败: {e}", "execute_plugin_action")


@router.post("/search")
async def search_resources(payload: PluginSearchPayload, _session_data: dict = Depends(require_admin_auth)):
    plugin_id = payload.plugin_id or "resource_search"
    try:
        result = await plugin_manager.execute_search(plugin_id, payload.keyword, payload.page)
        return {"success": True, "data": result}
    except KeyError as e:
        raise_api_error(str(e), "search_resources", 404)
    except Exception as e:
        raise_api_error(f"资源搜索失败: {e}", "search_resources")


@router.post("/search-jobs")
async def start_search_job(payload: PluginSearchJobPayload, _session_data: dict = Depends(require_admin_auth)):
    plugin_id = payload.plugin_id or "resource_search"
    try:
        result = await plugin_manager.start_search_job(plugin_id, payload.keyword, payload.page)
        return {"success": True, "data": result}
    except KeyError as e:
        raise_api_error(str(e), "start_search_job", 404)
    except Exception as e:
        raise_api_error(f"启动搜索失败: {e}", "start_search_job")


@router.get("/search-jobs/{job_id}")
async def get_search_job(job_id: str, _session_data: dict = Depends(require_admin_auth)):
    try:
        result = plugin_manager.get_search_job(job_id)
        return {"success": True, "data": result}
    except KeyError as e:
        raise_api_error(str(e), "get_search_job", 404)
    except Exception as e:
        raise_api_error(f"获取搜索任务失败: {e}", "get_search_job")


@router.post("/search-jobs/{job_id}/cancel")
async def cancel_search_job(job_id: str, _session_data: dict = Depends(require_admin_auth)):
    try:
        result = await plugin_manager.cancel_search_job(job_id)
        return {"success": True, "data": result, "message": "搜索任务已停止"}
    except KeyError as e:
        raise_api_error(str(e), "cancel_search_job", 404)
    except Exception as e:
        raise_api_error(f"停止搜索任务失败: {e}", "cancel_search_job")


@router.post("/{plugin_id}/test-connection")
async def test_plugin_connection(plugin_id: str, _session_data: dict = Depends(require_admin_auth)):
    try:
        result = await plugin_manager.test_plugin_connection(plugin_id)
        return {"success": True, "data": result, "message": "连通性测试完成"}
    except KeyError as e:
        raise_api_error(str(e), "test_plugin_connection", 404)
    except Exception as e:
        raise_api_error(f"连通性测试失败: {e}", "test_plugin_connection")


@router.post("/{plugin_id}/sync")
async def sync_plugin_index(plugin_id: str, payload: PluginSyncPayload, _session_data: dict = Depends(require_admin_auth)):
    try:
        result = await plugin_manager.sync_plugin_index(plugin_id, force=payload.force)
        return {"success": True, "data": result, "message": "同步完成"}
    except KeyError as e:
        raise_api_error(str(e), "sync_plugin_index", 404)
    except Exception as e:
        raise_api_error(f"同步资源失败: {e}", "sync_plugin_index")
