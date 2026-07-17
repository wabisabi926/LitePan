import json
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from api.deps import require_admin_auth
from api.responses import success_response, error_response
from config import config_manager
from database.db import db
from core.driver_service import get_account_driver_instance
from core.operation_wrapper import current_account_id

from mediaorganize import (
    rules,
    plan_task,
    apply_task,
    run_task,
    get_plan,
    update_plan_action,
    delete_plan_action,
    delete_plan_actions,
    get_logs,
    get_progress,
    request_stop,
    is_running,
    search_tmdb_async,
    build_proxy_url,
)


router = APIRouter()

_MO_SETTING_KEYS = {
    "mo_proxy_enabled": "proxy_enabled",
    "mo_proxy_url": "proxy_url",
    "mo_proxy_username": "proxy_username",
    "mo_proxy_password": "proxy_password",
    "mo_tmdb_api_key": "tmdb_api_key",
    "mo_tmdb_language": "tmdb_language",
    "mo_api_request_interval_ms": "api_request_interval_ms",
    "mo_ffprobe_request_interval_ms": "ffprobe_request_interval_ms",
    "mo_tmdb_request_interval_ms": "tmdb_request_interval_ms",
    "mo_ffprobe_concurrency": "ffprobe_concurrency",
    "mo_ffprobe_timeout_seconds": "ffprobe_timeout_seconds",
    "mo_min_confidence_threshold": "min_confidence_threshold",
    "mo_file_extensions": "file_extensions",
    "mo_metadata_extensions": "metadata_extensions",
    "mo_media_tag_order": "media_tag_order",
    "mo_align_media_tags": "align_media_tags",
    "mo_max_works_per_run": "max_works_per_run",
    "mo_overwrite_existing": "overwrite_existing",
}


async def _get_organize_settings_dict() -> dict:
    result = {}
    for key, field in _MO_SETTING_KEYS.items():
        result[field] = await config_manager.get_async(key)
    return result


async def _save_organize_settings_dict(updates: dict) -> None:
    field_to_key = {v: k for k, v in _MO_SETTING_KEYS.items()}
    failed = []
    for field, value in updates.items():
        config_key = field_to_key.get(field)
        if not config_key:
            continue
        try:
            await config_manager.set_async(config_key, value)
        except Exception as e:
            failed.append(f"{config_key}: {e}")
    if failed:
        raise Exception("以下配置项写入失败: " + "; ".join(failed))


def _default_config_dict() -> dict:
    return {
        "task_name": "",
        "account_id": "",
        "target_directory": "",
        "target_directory_id": "",
        "file_extensions": rules.DEFAULT_MEDIA_EXTENSIONS,
        "metadata_extensions": rules.DEFAULT_METADATA_EXTENSIONS,
        "action_type": "move",
        "target_root": "",
        "target_root_id": "",
        "media_type": "auto",
        "rename_marker": "",
        "movie_template": "{title} ({year}) {{tmdb-{tmdb_id}}} [{video_info}]",
        "episode_template": "{title} ({year}) {{tmdb-{tmdb_id}}} S{season:02d}E{episode:02d} [{video_info}]",
        "season_folder_template": "Season {season:02d}",
        "use_ffprobe": False,
        "use_tmdb": True,
        "overwrite_existing": False,
        "recursive": True,
    }


def _normalize_config(config: dict) -> dict:
    base = _default_config_dict()
    if config:
        for key in base.keys():
            if key in config:
                base[key] = config[key]
    return base


class TaskCreate(BaseModel):
    task_name: str
    account_id: str
    target_directory: str = ""
    target_directory_id: str = ""
    action_type: str = "move"
    target_root: str = ""
    target_root_id: str = ""
    media_type: str = "auto"
    rename_marker: str = ""
    use_ffprobe: bool = False
    use_tmdb: bool = True
    overwrite_existing: bool = False
    recursive: bool = True


class TaskUpdate(BaseModel):
    task_name: Optional[str] = None
    account_id: Optional[str] = None
    target_directory: Optional[str] = None
    target_directory_id: Optional[str] = None
    action_type: Optional[str] = None
    target_root: Optional[str] = None
    target_root_id: Optional[str] = None
    media_type: Optional[str] = None
    rename_marker: Optional[str] = None
    use_ffprobe: Optional[bool] = None
    use_tmdb: Optional[bool] = None
    overwrite_existing: Optional[bool] = None
    recursive: Optional[bool] = None


class PlanActionUpdate(BaseModel):
    target_name: str


class PlanActionsDelete(BaseModel):
    action_ids: List[str]


class SettingsUpdate(BaseModel):
    api_request_interval_ms: Optional[int] = None
    ffprobe_request_interval_ms: Optional[int] = None
    tmdb_request_interval_ms: Optional[int] = None
    proxy_enabled: Optional[bool] = None
    proxy_url: Optional[str] = None
    proxy_username: Optional[str] = None
    proxy_password: Optional[str] = None
    tmdb_api_key: Optional[str] = None
    tmdb_language: Optional[str] = None
    ffprobe_concurrency: Optional[int] = None
    ffprobe_timeout_seconds: Optional[int] = None
    min_confidence_threshold: Optional[float] = None
    file_extensions: Optional[str] = None
    metadata_extensions: Optional[str] = None
    media_tag_order: Optional[str] = None
    align_media_tags: Optional[bool] = None
    max_works_per_run: Optional[int] = None
    overwrite_existing: Optional[bool] = None


def _task_to_response(row: dict) -> dict:
    try:
        config = json.loads(row.get("config", "{}") or "{}")
    except Exception:
        config = {}
    try:
        last_run_result = json.loads(row.get("last_run_result") or "null") if row.get("last_run_result") else None
    except Exception:
        last_run_result = None
    return {
        "id": row["id"],
        "task_name": row["task_name"],
        "account_id": row["account_id"],
        "config": config,
        "status": row.get("status", "idle"),
        "last_run_at": row.get("last_run_at"),
        "last_run_result": last_run_result,
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }


@router.get("/tasks")
async def list_tasks(session_data: dict = Depends(require_admin_auth)):
    tasks = await db.get_media_organize_tasks()
    return success_response(data=[_task_to_response(t) for t in tasks], message="获取列表成功")


@router.post("/tasks")
async def create_task(payload: TaskCreate, session_data: dict = Depends(require_admin_auth)):
    if not payload.task_name or not payload.task_name.strip():
        return error_response(message="任务名称不能为空")
    if not payload.account_id:
        return error_response(message="请选择网盘账号")
    if payload.action_type == "move" and not (payload.target_root or "").strip():
        return error_response(message="move 模式下目标根目录不能为空")
    if payload.action_type == "rename" and not (payload.rename_marker or "").strip():
        return error_response(message="原地重命名必须设置标识：tmdb / 自定义 / off（不写入文件名，靠规范结构判断跳过）")

    config = _normalize_config({
        "task_name": payload.task_name.strip(),
        "account_id": payload.account_id,
        "target_directory": payload.target_directory or "",
        "target_directory_id": payload.target_directory_id or "",
        "action_type": payload.action_type,
        "target_root": payload.target_root or "",
        "target_root_id": payload.target_root_id or "",
        "media_type": payload.media_type,
        "rename_marker": payload.rename_marker or "",
        "use_ffprobe": payload.use_ffprobe,
        "use_tmdb": payload.use_tmdb,
        "overwrite_existing": payload.overwrite_existing,
        "recursive": payload.recursive,
    })

    try:
        task_id = await db.create_media_organize_task(
            task_name=payload.task_name.strip(),
            account_id=payload.account_id,
            config=config,
        )
        return success_response(data={"id": task_id}, message="任务创建成功")
    except Exception as e:
        return error_response(message=f"创建失败: {str(e)}")


@router.put("/tasks/{task_id}")
async def update_task(task_id: str, payload: TaskUpdate, session_data: dict = Depends(require_admin_auth)):
    existing = await db.get_media_organize_task(task_id)
    if not existing:
        return error_response(message="任务不存在")

    try:
        existing_config = json.loads(existing.get("config", "{}") or "{}")
    except Exception:
        existing_config = {}

    updates = {}
    if payload.task_name is not None:
        updates["task_name"] = payload.task_name.strip()
    if payload.account_id is not None:
        updates["account_id"] = payload.account_id

    field_map = {
        "target_directory": payload.target_directory,
        "target_directory_id": payload.target_directory_id,
        "action_type": payload.action_type,
        "target_root": payload.target_root,
        "target_root_id": payload.target_root_id,
        "media_type": payload.media_type,
        "rename_marker": payload.rename_marker,
        "use_ffprobe": payload.use_ffprobe,
        "use_tmdb": payload.use_tmdb,
        "overwrite_existing": payload.overwrite_existing,
        "recursive": payload.recursive,
    }
    for k, v in field_map.items():
        if v is not None:
            existing_config[k] = v

    if (existing_config.get("action_type") or "").lower() == "rename" \
            and not (str(existing_config.get("rename_marker") or "").strip()):
        return error_response(message="原地重命名必须设置标识：tmdb / 自定义 / off（不写入文件名，靠规范结构判断跳过）")
    if (existing_config.get("action_type") or "").lower() == "move" \
            and not (str(existing_config.get("target_root") or "").strip()):
        return error_response(message="move 模式下目标根目录不能为空")

    updates["config"] = _normalize_config(existing_config)

    try:
        await db.update_media_organize_task(task_id, **updates)
        return success_response(message="任务更新成功")
    except Exception as e:
        return error_response(message=f"更新失败: {str(e)}")


@router.delete("/tasks/{task_id}")
async def delete_task(task_id: str, session_data: dict = Depends(require_admin_auth)):
    existing = await db.get_media_organize_task(task_id)
    if not existing:
        return error_response(message="任务不存在")
    if existing.get("status") in ("running", "planning", "stopping"):
        request_stop(task_id)
        try:
            await db.update_media_organize_task(task_id, status="stopping")
        except Exception:
            pass
        return success_response(
            data={"stopping": True},
            message="任务正在执行，已请求停止；停止完成后可再次删除",
        )
    try:
        from mediaorganize.manager import _delete_plan
        await _delete_plan(task_id)
        await db.delete_media_organize_task(task_id)
        return success_response(message="任务删除成功")
    except Exception as e:
        return error_response(message=f"删除失败: {str(e)}")


@router.delete("/tasks/{task_id}/plan")
async def delete_task_plan(task_id: str, session_data: dict = Depends(require_admin_auth)):
    try:
        from mediaorganize.manager import _delete_plan
        await _delete_plan(task_id)
        return success_response(message="计划已清空")
    except Exception as e:
        return error_response(message=str(e))


@router.put("/tasks/{task_id}/plan/actions/{action_id}")
async def update_plan_action_api(
    task_id: str,
    action_id: str,
    payload: PlanActionUpdate,
    session_data: dict = Depends(require_admin_auth),
):
    try:
        result = await update_plan_action(task_id, action_id, {"target_name": payload.target_name})
        return success_response(data=result, message="动作已更新")
    except Exception as e:
        return error_response(message=str(e))


@router.delete("/tasks/{task_id}/plan/actions/{action_id}")
async def delete_plan_action_api(
    task_id: str,
    action_id: str,
    session_data: dict = Depends(require_admin_auth),
):
    try:
        result = await delete_plan_action(task_id, action_id)
        return success_response(data=result, message="动作已删除")
    except Exception as e:
        return error_response(message=str(e))


@router.post("/tasks/{task_id}/plan/actions/batch-delete")
async def delete_plan_actions_api(
    task_id: str,
    payload: PlanActionsDelete,
    session_data: dict = Depends(require_admin_auth),
):
    try:
        result = await delete_plan_actions(task_id, payload.action_ids)
        return success_response(data=result, message="动作已删除")
    except Exception as e:
        return error_response(message=str(e))


@router.get("/tasks/{task_id}/progress")
async def get_task_progress(task_id: str, session_data: dict = Depends(require_admin_auth)):
    return success_response(data=get_progress(task_id), message="获取成功")


@router.get("/settings")
async def get_settings(session_data: dict = Depends(require_admin_auth)):
    settings = await _get_organize_settings_dict()
    return success_response(data=settings, message="获取成功")


@router.put("/settings")
async def update_settings(payload: SettingsUpdate, session_data: dict = Depends(require_admin_auth)):
    updates = {}
    for field in _MO_SETTING_KEYS.values():
        v = getattr(payload, field, None)
        if v is not None:
            updates[field] = v
    try:
        await _save_organize_settings_dict(updates)
        return success_response(message="设置保存成功")
    except Exception as e:
        return error_response(message=f"保存失败: {str(e)}")


@router.get("/guess-file")
async def guess_file(account_id: str, file_id: str, session_data: dict = Depends(require_admin_auth)):
    try:
        driver = await get_account_driver_instance(int(account_id))
        current_account_id.set(str(account_id))
        info = await driver.file_info(file_id)
        if not info:
            return error_response(message="文件不存在")
        return success_response(
            data={"file_name": info.name, "parsed": rules.parse_filename_strict(info.name)},
            message="解析成功",
        )
    except Exception as e:
        return error_response(message=f"解析失败: {str(e)}")


class TmdbTestPayload(BaseModel):
    tmdb_api_key: Optional[str] = None
    tmdb_language: Optional[str] = None
    proxy_enabled: Optional[bool] = None
    proxy_url: Optional[str] = None
    proxy_username: Optional[str] = None
    proxy_password: Optional[str] = None


@router.post("/test-tmdb")
async def test_tmdb_connection(
    payload: Optional[TmdbTestPayload] = None,
    session_data: dict = Depends(require_admin_auth),
):
    saved = await _get_organize_settings_dict()
    payload_dict = payload.dict(exclude_none=True) if payload else {}
    merged = {**saved, **payload_dict}
    api_key = (merged.get("tmdb_api_key") or "").strip()
    if not api_key:
        return error_response(message="请先填写 TMDB API Key 再测试")
    language = merged.get("tmdb_language") or "zh-CN"
    proxy_url = build_proxy_url(merged)
    try:
        from mediaorganize import validate_tmdb_connection
        ok = await validate_tmdb_connection(api_key, language, proxy_url)
        if ok:
            return success_response(
                data={"ok": True, "language": language, "proxy_used": bool(proxy_url)},
                message="TMDB 连通正常（测试用的是当前编辑值，未保存）",
            )
        return error_response(message="TMDB 不可达，请检查 API Key、网络或代理配置")
    except Exception as e:
        return error_response(message=f"TMDB 连通测试异常: {e}")


@router.get("/search-tmdb")
async def search_tmdb_api(
    query: str,
    year: Optional[int] = None,
    language: str = "zh-CN",
    media_type: str = "auto",
    session_data: dict = Depends(require_admin_auth),
):
    settings = await _get_organize_settings_dict()
    api_key = settings.get("tmdb_api_key") or ""
    proxy_url = build_proxy_url(settings)
    results = await search_tmdb_async(query, year, language, api_key, proxy_url, media_type)
    return success_response(data=results, message="搜索完成")


@router.post("/tasks/{task_id}/run")
async def run(task_id: str, session_data: dict = Depends(require_admin_auth)):
    try:
        result = await run_task(task_id)
        return success_response(data=result, message="任务已开始执行")
    except Exception as e:
        return error_response(message=str(e))


@router.post("/tasks/{task_id}/plan")
async def plan(task_id: str, session_data: dict = Depends(require_admin_auth)):
    try:
        result = await plan_task(task_id)
        return success_response(data=result, message="计划生成完成")
    except Exception as e:
        return error_response(message=str(e))


@router.get("/tasks/{task_id}/plan")
async def get_task_plan(task_id: str, session_data: dict = Depends(require_admin_auth)):
    try:
        plan_dict = await get_plan(task_id)
        return success_response(data=plan_dict, message="获取计划成功")
    except Exception as e:
        return error_response(message=str(e))


@router.post("/tasks/{task_id}/apply")
async def apply(task_id: str, session_data: dict = Depends(require_admin_auth)):
    try:
        result = await apply_task(task_id)
        return success_response(data=result, message="计划已执行")
    except Exception as e:
        return error_response(message=str(e))


@router.post("/tasks/{task_id}/stop")
async def stop_task(task_id: str, session_data: dict = Depends(require_admin_auth)):
    task = await db.get_media_organize_task(task_id)
    if not task:
        return error_response(message="任务不存在")
    if task.get("status") not in ("running", "planning", "stopping"):
        return success_response(data={"stopping": False}, message="任务未在执行")
    request_stop(task_id)
    try:
        await db.update_media_organize_task(task_id, status="stopping")
    except Exception:
        pass
    return success_response(data={"stopping": True}, message="已请求停止")


@router.get("/tasks/{task_id}/logs")
async def get_task_logs(task_id: str, session_data: dict = Depends(require_admin_auth)):
    task = await db.get_media_organize_task(task_id)
    if not task:
        return error_response(message="任务不存在")
    return success_response(
        data={
            "logs": get_logs(task_id),
            "status": task.get("status", "idle"),
            "last_run_result": json.loads(task.get("last_run_result") or "null") if task.get("last_run_result") else None,
        },
        message="获取成功",
    )




