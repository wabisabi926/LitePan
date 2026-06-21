from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import ipaddress
import re
from urllib.parse import urlparse

from api.deps import require_admin_auth
from api.responses import success_response as _success_response, error_response as _error_response
from config import config_manager
from core.strm_sync_manager import strm_sync_manager
from core.utils import normalize_bool
from database.db import db


router = APIRouter()

DEFAULT_METADATA_EXTENSIONS = "srt;ass;ssa;sub;nfo;jpg;jpeg;png;webp"
DEFAULT_MEDIA_EXTENSIONS = "mp4;mkv;avi;mov;wmv;flv;ts;m2ts;mpg;mpeg;webm;m4v;iso;rmvb;mp3;flac;aac;wav;m4a"
STRM_CONFLICT_POLICIES = {"size_desc", "size_asc", "name_asc"}
STRM_LEGACY_CONFLICT_POLICIES = {"quality_then_size"}
STRM_ISO_PLAY_MODES = {"proxy", "follow"}
STRM_LINK_FORMATS = {"v1", "v2"}
STRM_TASK_NAME_WIDTH_LIMIT = 20


def _text_display_width(value: str) -> int:
    total = 0
    for ch in str(value or ""):
        total += 2 if ord(ch) > 127 else 1
    return total


def _validate_task_name(name: str) -> Optional[str]:
    if not name:
        return "任务名称不能为空"
    if _text_display_width(name) > STRM_TASK_NAME_WIDTH_LIMIT:
        return "任务名称最多10个中文或20个英文字符"
    return None


def _extract_request_base_url(request: Request) -> str:
    forwarded_proto = (request.headers.get("x-forwarded-proto") or "").strip()
    forwarded_host = (request.headers.get("x-forwarded-host") or "").strip()
    host = forwarded_host or (request.headers.get("host") or "").strip()
    scheme = forwarded_proto or request.url.scheme
    if host:
        return f"{scheme}://{host}".rstrip("/")
    return str(request.base_url).rstrip("/")


def _is_loopback_base_url(base_url: str) -> bool:
    try:
        parsed = urlparse(base_url)
        hostname = (parsed.hostname or "").strip().lower()
        if not hostname:
            return True
        if hostname in {"localhost", "::1"}:
            return True
        return ipaddress.ip_address(hostname).is_loopback
    except ValueError:
        return False
    except Exception:
        return True


async def _resolve_strm_base_url(request: Request) -> str:
    configured = str((await config_manager.get_async("strm_base_url")) or "").strip().rstrip("/")
    candidate = _extract_request_base_url(request)
    if configured and not _is_loopback_base_url(configured):
        return configured
    if candidate and not _is_loopback_base_url(candidate):
        if configured != candidate:
            await config_manager.set_async("strm_base_url", candidate)
        return candidate
    return configured


async def _get_default_strm_settings() -> Dict[str, int]:
    default_scan_interval = await config_manager.get_async("strm_default_scan_interval")
    if default_scan_interval is None:
        default_scan_interval = 60
        await config_manager.set_async("strm_default_scan_interval", int(default_scan_interval))
    task_concurrency = await config_manager.get_async("strm_task_concurrency")
    if task_concurrency is None:
        task_concurrency = 3
        await config_manager.set_async("strm_task_concurrency", int(task_concurrency))
    return {
        "scan_interval": int(default_scan_interval),
        "task_concurrency": max(1, min(int(task_concurrency), 10)),
    }


async def _get_strm_number_setting(key: str, default: int, min_value: int, max_value: int) -> int:
    value = await config_manager.get_async(key)
    if value is None:
        value = default
        await config_manager.set_async(key, int(value))
    try:
        number = int(value)
    except Exception:
        number = default
    return max(min_value, min(number, max_value))


async def _get_strm_text_setting(key: str, default: str) -> str:
    value = await config_manager.get_async(key)
    if value is None:
        value = default
        await config_manager.set_async(key, value)
    return str(value or "")


async def _get_strm_settings_data(request: Request) -> Dict[str, Any]:
    token = await config_manager.get_async("strm_token")
    if not token:
        import secrets
        token = secrets.token_urlsafe(32)
        await config_manager.set_async("strm_token", token)

    base_url = await _resolve_strm_base_url(request)
    defaults = await _get_default_strm_settings()
    signature_enabled = normalize_bool(
        await config_manager.get_async("strm_signature_enabled"),
        False,
    )
    conflict_policy = await _get_strm_text_setting("strm_conflict_policy", "size_desc")
    if conflict_policy in STRM_LEGACY_CONFLICT_POLICIES:
        conflict_policy = "size_desc"
        await config_manager.set_async("strm_conflict_policy", conflict_policy)
    elif conflict_policy not in STRM_CONFLICT_POLICIES:
        conflict_policy = "size_desc"
    link_format = await _get_strm_text_setting("strm_link_format", "v1")
    if link_format not in STRM_LINK_FORMATS:
        link_format = "v1"

    return {
        "strm_base_url": base_url,
        "strm_token": token,
        "strm_link_format": link_format,
        "strm_default_scan_interval": defaults["scan_interval"],
        "strm_task_concurrency": defaults["task_concurrency"],
        "strm_signature_enabled": signature_enabled,
        "strm_default_extensions": await _get_strm_text_setting("strm_default_extensions", DEFAULT_MEDIA_EXTENSIONS),
        "strm_min_file_size_mb": await _get_strm_number_setting("strm_min_file_size_mb", 0, 0, 10240),
        "strm_metadata_extensions": await _get_strm_text_setting("strm_metadata_extensions", DEFAULT_METADATA_EXTENSIONS),
        "strm_metadata_max_size_mb": await _get_strm_number_setting("strm_metadata_max_size_mb", 10, 1, 1024),
        "strm_metadata_parent_enabled": normalize_bool(
            await config_manager.get_async("strm_metadata_parent_enabled"),
            True,
        ),
        "strm_conflict_policy": conflict_policy,
        "strm_iso_play_mode": await _get_strm_text_setting("strm_iso_play_mode", "follow"),
    }


class StrmSettingsUpdate(BaseModel):
    strm_base_url: Optional[str] = None
    regenerate_token: Optional[bool] = None
    apply_token_to_existing_strm: Optional[bool] = None
    strm_link_format: Optional[str] = None
    apply_link_format_to_existing_strm: Optional[bool] = None
    strm_default_scan_interval: Optional[int] = None
    strm_task_concurrency: Optional[int] = None
    strm_signature_enabled: Optional[bool] = None
    strm_default_extensions: Optional[str] = None
    strm_min_file_size_mb: Optional[int] = None
    strm_metadata_extensions: Optional[str] = None
    strm_metadata_max_size_mb: Optional[int] = None
    strm_metadata_parent_enabled: Optional[bool] = None
    strm_conflict_policy: Optional[str] = None
    strm_iso_play_mode: Optional[str] = None


@router.get("/settings")
async def get_strm_settings(request: Request, session_data: dict = Depends(require_admin_auth)):
    return _success_response(
        data=await _get_strm_settings_data(request),
        message="获取STRM设置成功",
    )


@router.post("/settings")
async def update_strm_settings(payload: StrmSettingsUpdate, request: Request, session_data: dict = Depends(require_admin_auth)):
    try:
        token_replace_result = None
        link_format_replace_result = None

        if payload.strm_base_url is not None:
            value = (payload.strm_base_url or "").strip()
            if value:
                if not re.match(r"^https?://\S+$", value):
                    return _error_response(message="STRM播放地址基址格式不正确，示例：https://litepan.top")
                await config_manager.set_async("strm_base_url", value.rstrip("/"))
            else:
                await config_manager.set_async("strm_base_url", "")

        if normalize_bool(payload.regenerate_token, False):
            import secrets
            old_token = str((await config_manager.get_async("strm_token")) or "").strip()
            new_token = secrets.token_urlsafe(32)
            await config_manager.set_async("strm_token", new_token)
            if normalize_bool(payload.apply_token_to_existing_strm, False):
                token_replace_result = await strm_sync_manager.replace_strm_token(old_token, new_token)

        if payload.strm_default_scan_interval is not None:
            value = int(payload.strm_default_scan_interval)
            if value < 1 or value > 1440:
                return _error_response(message="STRM默认扫描间隔必须是 1-1440 分钟")
            await config_manager.set_async("strm_default_scan_interval", value)

        if payload.strm_task_concurrency is not None:
            value = int(payload.strm_task_concurrency)
            if value < 1 or value > 10:
                return _error_response(message="STRM任务并发数必须是 1-10")
            await config_manager.set_async("strm_task_concurrency", value)

        if payload.strm_signature_enabled is not None:
            await config_manager.set_async("strm_signature_enabled", bool(payload.strm_signature_enabled))

        if payload.strm_default_extensions is not None:
            await config_manager.set_async("strm_default_extensions", str(payload.strm_default_extensions or "").strip())

        if payload.strm_min_file_size_mb is not None:
            value = int(payload.strm_min_file_size_mb)
            if value < 0 or value > 10240:
                return _error_response(message="小文件过滤阈值必须是 0-10240 MB")
            await config_manager.set_async("strm_min_file_size_mb", value)

        if payload.strm_metadata_extensions is not None:
            await config_manager.set_async("strm_metadata_extensions", str(payload.strm_metadata_extensions or "").strip())

        if payload.strm_metadata_max_size_mb is not None:
            value = int(payload.strm_metadata_max_size_mb)
            if value < 1 or value > 1024:
                return _error_response(message="元数据大小上限必须是 1-1024 MB")
            await config_manager.set_async("strm_metadata_max_size_mb", value)

        if payload.strm_metadata_parent_enabled is not None:
            await config_manager.set_async("strm_metadata_parent_enabled", bool(payload.strm_metadata_parent_enabled))

        if payload.strm_conflict_policy is not None:
            value = str(payload.strm_conflict_policy or "size_desc")
            if value in STRM_LEGACY_CONFLICT_POLICIES:
                value = "size_desc"
            if value not in STRM_CONFLICT_POLICIES:
                return _error_response(message="STRM同名冲突策略不支持")
            await config_manager.set_async("strm_conflict_policy", value)

        if payload.strm_iso_play_mode is not None:
            value = str(payload.strm_iso_play_mode or "follow")
            if value not in STRM_ISO_PLAY_MODES:
                return _error_response(message="ISO播放规则不支持")
            await config_manager.set_async("strm_iso_play_mode", value)

        if payload.strm_link_format is not None:
            value = str(payload.strm_link_format or "v1").strip().lower()
            if value not in STRM_LINK_FORMATS:
                return _error_response(message="STRM链接格式不支持")
            await config_manager.set_async("strm_link_format", value)
            if normalize_bool(payload.apply_link_format_to_existing_strm, False):
                link_format_replace_result = await strm_sync_manager.replace_strm_link_format(value)

        data = await _get_strm_settings_data(request)
        if token_replace_result is not None:
            data["strm_token_replace_result"] = token_replace_result
        if link_format_replace_result is not None:
            data["strm_link_format_replace_result"] = link_format_replace_result
        return _success_response(data=data, message="更新STRM设置成功")
    except Exception as e:
        return _error_response(message=f"更新STRM设置失败: {str(e)}")


class StrmTaskCreate(BaseModel):
    name: str
    account_id: int
    parent_id: str
    path: str
    scan_mode: str = "incremental_update"
    api_interval: int = 200
    extensions: str = ""
    exclude_dir_keywords: str = ""
    exclude_file_keywords: str = ""
    sync_metadata: bool = False
    branch_check_enabled: bool = False
    time_window_enabled: bool = False
    time_start: str = "00:00"
    time_end: str = "00:00"
    schedule_mode: str = "window"


class StrmTaskUpdate(BaseModel):
    name: Optional[str] = None
    account_id: Optional[int] = None
    parent_id: Optional[str] = None
    path: Optional[str] = None
    scan_mode: Optional[str] = None
    api_interval: Optional[int] = None
    extensions: Optional[str] = None
    exclude_dir_keywords: Optional[str] = None
    exclude_file_keywords: Optional[str] = None
    sync_metadata: Optional[bool] = None
    branch_check_enabled: Optional[bool] = None
    status: Optional[str] = None
    time_window_enabled: Optional[bool] = None
    time_start: Optional[str] = None
    time_end: Optional[str] = None
    schedule_mode: Optional[str] = None


_ALLOWED_SCAN_MODES = {"incremental_missing", "incremental_update", "full_sync"}
_ALLOWED_SCHEDULE_MODES = {"window", "daily"}
_ALLOWED_RUN_MODES = {"auto", "full", "branch"}
_ALLOWED_BRANCH_TYPES = {"base", "temporary"}


class StrmBranchCreate(BaseModel):
    parent_id: str
    path: str
    branch_type: str = "temporary"
    recursive: bool = True
    retention_days: Optional[int] = None  # None→临时分支默认30天；0→永久（不设 expires_at）


class StrmBranchUpdate(BaseModel):
    parent_id: Optional[str] = None
    path: Optional[str] = None
    branch_type: Optional[str] = None
    recursive: Optional[bool] = None
    retention_days: Optional[int] = None
    status: Optional[str] = None


class StrmCurrentDirectoryGeneratePayload(BaseModel):
    account_id: int
    path: str
    items: List[Dict[str, Any]] = Field(default_factory=list)


def _normalize_path(path: str) -> str:
    value = "/" + str(path or "").strip("/")
    return "/" if value == "/" else value.rstrip("/")


def _build_branch_relative_path(task_path: str, branch_path: str) -> Optional[str]:
    task_norm = _normalize_path(task_path)
    branch_norm = _normalize_path(branch_path)
    if branch_norm == task_norm:
        return ""
    prefix = task_norm.rstrip("/") + "/"
    if branch_norm.startswith(prefix):
        return branch_norm[len(prefix):].strip("/")
    return None


def _parse_datetime(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _branch_expiry(retention_days: int, base_time: Optional[datetime] = None) -> Optional[str]:
    days = max(0, min(int(retention_days or 0), 3650))
    if days <= 0:
        return None
    return ((base_time or datetime.now()) + timedelta(days=days)).isoformat()


@router.get("/tasks")
async def list_tasks(session_data: dict = Depends(require_admin_auth)):
    tasks = await db.get_strm_sync_tasks()
    accounts = await db.list_accounts(include_inactive=True)
    account_name_map = {int(a["id"]): str(a["name"]) for a in accounts}
    running_task_ids = strm_sync_manager.get_running_task_ids()
    queued_task_ids = strm_sync_manager.get_queued_task_ids()

    for t in tasks:
        branches = await db.get_strm_sync_branches(int(t["id"]))
        t["account_name"] = account_name_map.get(int(t["account_id"]), "未知账号")
        t["branch_count"] = len(branches)
        t["is_scanning"] = int(t["id"]) in running_task_ids
        t["is_queued"] = int(t["id"]) in queued_task_ids
        # 注入内存中的实时进度
        mem_task = strm_sync_manager._tasks.get(int(t["id"]))
        if mem_task:
            t["scanned_dirs"] = mem_task.scanned_dirs
            t["scanned_files"] = mem_task.scanned_files
            t["started_at"] = mem_task.started_at.isoformat() if mem_task.started_at else None
            if int(t["id"]) in running_task_ids and mem_task.started_at:
                t["current_duration_ms"] = max(int((datetime.now() - mem_task.started_at).total_seconds() * 1000), 0)
            else:
                t["current_duration_ms"] = int(t.get("last_duration_ms") or 0)
        else:
            t["scanned_dirs"] = 0
            t["scanned_files"] = 0
            t["started_at"] = None
            t["current_duration_ms"] = int(t.get("last_duration_ms") or 0)
    response = _success_response(data=tasks, message="获取STRM任务列表成功")
    response["startup_remaining"] = strm_sync_manager.startup_remaining
    return response


@router.post("/tasks")
async def create_task(payload: StrmTaskCreate, session_data: dict = Depends(require_admin_auth)):
    try:
        name = payload.name.strip()
        name_error = _validate_task_name(name)
        if name_error:
            return _error_response(message=name_error)

        tasks = await db.get_strm_sync_tasks()
        if any(str(t.get("name", "")).strip() == name for t in tasks):
            return _error_response(message="任务名称已存在")

        if any(int(t.get("account_id")) == int(payload.account_id) and str(t.get("parent_id")) == str(payload.parent_id) for t in tasks):
            return _error_response(message="同一账号下已存在相同目录的任务")

        if payload.scan_mode not in _ALLOWED_SCAN_MODES:
            return _error_response(message="扫描方式不支持")

        api_interval = max(0, min(int(payload.api_interval or 0), 5000))
        schedule_mode = payload.schedule_mode if payload.schedule_mode in _ALLOWED_SCHEDULE_MODES else "window"

        defaults = await _get_default_strm_settings()

        task_id = await db.create_strm_sync_task(
            name=name,
            account_id=payload.account_id,
            parent_id=str(payload.parent_id),
            path=str(payload.path),
            recursive=True,
            scan_interval=defaults["scan_interval"],
            scan_mode=payload.scan_mode,
            concurrency=3,
            extensions=str(payload.extensions or ""),
            exclude_dir_keywords=str(payload.exclude_dir_keywords or ""),
            exclude_file_keywords=str(payload.exclude_file_keywords or ""),
            sync_metadata=bool(payload.sync_metadata),
            api_interval=api_interval,
            branch_check_enabled=bool(payload.branch_check_enabled),
            status="running",
            time_window_enabled=bool(payload.time_window_enabled),
            time_start=str(payload.time_start or "00:00"),
            time_end=str(payload.time_end or "00:00"),
            schedule_mode=schedule_mode,
        )
        await strm_sync_manager.refresh_tasks_from_db()
        # 每日定时任务按设定时间执行，创建时不立即跑；其余维持原有“建好即跑一次”行为
        if schedule_mode != "daily":
            await strm_sync_manager.run_task_now(task_id)
        return _success_response(data={"id": task_id}, message="创建STRM任务成功")
    except Exception as e:
        return _error_response(message=f"创建STRM任务失败: {str(e)}")


@router.put("/tasks/{task_id}")
async def update_task(task_id: int, payload: StrmTaskUpdate, session_data: dict = Depends(require_admin_auth)):
    try:
        task = await db.get_strm_sync_task(task_id)
        if not task:
            return _error_response(message="任务不存在")

        updates: Dict[str, Any] = {}
        if payload.name is not None:
            name = payload.name.strip()
            name_error = _validate_task_name(name)
            if name_error:
                return _error_response(message=name_error)
            tasks = await db.get_strm_sync_tasks()
            if any(int(t["id"]) != task_id and str(t.get("name", "")).strip() == name for t in tasks):
                return _error_response(message="任务名称已存在")
            updates["name"] = name

        if payload.account_id is not None:
            updates["account_id"] = int(payload.account_id)
        if payload.parent_id is not None:
            updates["parent_id"] = str(payload.parent_id)
        if payload.path is not None:
            updates["path"] = str(payload.path)

        effective_account_id = int(updates.get("account_id") or task.get("account_id"))
        effective_parent_id = str(updates.get("parent_id") or task.get("parent_id"))
        tasks = await db.get_strm_sync_tasks()
        if any(int(t.get("id")) != task_id and int(t.get("account_id")) == effective_account_id and str(t.get("parent_id")) == effective_parent_id for t in tasks):
            return _error_response(message="同一账号下已存在相同目录的任务")

        if payload.scan_mode is not None:
            if payload.scan_mode not in _ALLOWED_SCAN_MODES:
                return _error_response(message="扫描方式不支持")
            updates["scan_mode"] = payload.scan_mode

        if payload.extensions is not None:
            updates["extensions"] = str(payload.extensions or "")
        if payload.exclude_dir_keywords is not None:
            updates["exclude_dir_keywords"] = str(payload.exclude_dir_keywords or "")
        if payload.exclude_file_keywords is not None:
            updates["exclude_file_keywords"] = str(payload.exclude_file_keywords or "")
        if payload.sync_metadata is not None:
            updates["sync_metadata"] = bool(payload.sync_metadata)
        if payload.branch_check_enabled is not None:
            updates["branch_check_enabled"] = bool(payload.branch_check_enabled)

        if payload.api_interval is not None:
            updates["api_interval"] = max(0, min(int(payload.api_interval), 5000))

        if payload.status is not None:
            if payload.status not in ("running", "paused"):
                return _error_response(message="任务状态不支持")
            updates["status"] = payload.status

        if payload.time_window_enabled is not None:
            updates["time_window_enabled"] = bool(payload.time_window_enabled)
        if payload.time_start is not None:
            updates["time_start"] = str(payload.time_start or "00:00")
        if payload.time_end is not None:
            updates["time_end"] = str(payload.time_end or "00:00")
        if payload.schedule_mode is not None:
            updates["schedule_mode"] = payload.schedule_mode if payload.schedule_mode in _ALLOWED_SCHEDULE_MODES else "window"

        ok = await db.update_strm_sync_task(task_id, **updates)
        if not ok:
            return _error_response(message="更新失败")

        await strm_sync_manager.refresh_tasks_from_db()
        return _success_response(message="更新STRM任务成功")
    except Exception as e:
        return _error_response(message=f"更新STRM任务失败: {str(e)}")


@router.delete("/tasks/{task_id}")
async def delete_task(task_id: int, delete_strm_files: bool = False, session_data: dict = Depends(require_admin_auth)):
    try:
        task = await db.get_strm_sync_task(task_id)
        if not task:
            return _error_response(message="任务不存在")

        if delete_strm_files:
            await strm_sync_manager.delete_task_output(str(task.get("name") or ""))

        ok = await db.delete_strm_sync_task(task_id)
        if not ok:
            return _error_response(message="删除失败")

        await strm_sync_manager.refresh_tasks_from_db()
        return _success_response(message="删除STRM任务成功")
    except Exception as e:
        return _error_response(message=f"删除STRM任务失败: {str(e)}")


@router.post("/tasks/{task_id}/toggle")
async def toggle_task(task_id: int, session_data: dict = Depends(require_admin_auth)):
    try:
        ok = await db.toggle_strm_sync_task_status(task_id)
        if not ok:
            return _error_response(message="切换失败")
        await strm_sync_manager.refresh_tasks_from_db()
        return _success_response(message="切换任务状态成功")
    except Exception as e:
        return _error_response(message=f"切换任务状态失败: {str(e)}")


@router.post("/tasks/{task_id}/run")
async def run_task(task_id: int, mode: str = "auto", session_data: dict = Depends(require_admin_auth)):
    run_mode = str(mode or "auto")
    if run_mode not in _ALLOWED_RUN_MODES:
        return _error_response(message="执行方式不支持")
    runtime_state = await strm_sync_manager.run_task_now(task_id, run_mode=run_mode)
    if runtime_state == "missing":
        return _error_response(message="任务不存在")
    message = "已触发执行"
    if runtime_state in {"queued", "already_queued"}:
        message = "任务已加入队列"
    return _success_response(data={"runtime_state": runtime_state}, message=message)


@router.get("/tasks/{task_id}/branches")
async def list_task_branches(task_id: int, session_data: dict = Depends(require_admin_auth)):
    task = await db.get_strm_sync_task(task_id)
    if not task:
        return _error_response(message="任务不存在")
    await db.delete_expired_strm_sync_branches(task_id)
    branches = await db.get_strm_sync_branches(task_id)
    return _success_response(data=branches, message="获取分支列表成功")


@router.post("/tasks/{task_id}/branches")
async def create_task_branch(task_id: int, payload: StrmBranchCreate, session_data: dict = Depends(require_admin_auth)):
    try:
        task = await db.get_strm_sync_task(task_id)
        if not task:
            return _error_response(message="任务不存在")
        branch_path = _normalize_path(payload.path)
        relative_path = _build_branch_relative_path(str(task.get("path") or ""), branch_path)
        if relative_path is None:
            return _error_response(message="分支目录必须位于任务目录之下")
        existing_branches = await db.get_strm_sync_branches(task_id)
        if any(str(branch.get("parent_id")) == str(payload.parent_id) or _normalize_path(str(branch.get("path") or "")) == branch_path for branch in existing_branches):
            return _error_response(message="分支已存在")
        branch_type = str(payload.branch_type or "temporary")
        if branch_type not in _ALLOWED_BRANCH_TYPES:
            return _error_response(message="分支类型不支持")
        if branch_type == "base":
            retention_days = 0
            recursive = False
        else:
            rd_raw = int(payload.retention_days if payload.retention_days is not None else 30)
            if rd_raw <= 0:
                retention_days = 0
            else:
                retention_days = min(rd_raw, 3650)
            recursive = bool(payload.recursive)
        branch_id = await db.create_strm_sync_branch(
            task_id=task_id,
            account_id=int(task["account_id"]),
            parent_id=str(payload.parent_id),
            path=branch_path,
            relative_path=relative_path,
            recursive=recursive,
            retention_days=retention_days,
            expires_at=_branch_expiry(retention_days),
            branch_type=branch_type,
            source="manual",
            status="running",
        )
        return _success_response(data={"id": branch_id}, message="分支添加成功")
    except Exception as e:
        return _error_response(message=f"分支添加失败: {str(e)}")


@router.put("/tasks/{task_id}/branches/{branch_id}")
async def update_task_branch(task_id: int, branch_id: int, payload: StrmBranchUpdate, session_data: dict = Depends(require_admin_auth)):
    try:
        task = await db.get_strm_sync_task(task_id)
        branch = await db.get_strm_sync_branch(branch_id)
        if not task or not branch or int(branch.get("task_id")) != task_id:
            return _error_response(message="分支不存在")
        updates: Dict[str, Any] = {}
        if payload.path is not None:
            branch_path = _normalize_path(payload.path)
            relative_path = _build_branch_relative_path(str(task.get("path") or ""), branch_path)
            if relative_path is None:
                return _error_response(message="分支目录必须位于任务目录之下")
            updates["path"] = branch_path
            updates["relative_path"] = relative_path
        if payload.parent_id is not None:
            updates["parent_id"] = str(payload.parent_id)
        if payload.recursive is not None:
            updates["recursive"] = bool(payload.recursive)
        if payload.branch_type is not None:
            branch_type = str(payload.branch_type or "temporary")
            if branch_type not in _ALLOWED_BRANCH_TYPES:
                return _error_response(message="分支类型不支持")
            updates["branch_type"] = branch_type
            if branch_type == "base":
                updates["recursive"] = False
                updates["retention_days"] = 0
                updates["expires_at"] = None
        if payload.retention_days is not None:
            effective_type = str(updates.get("branch_type") or branch.get("branch_type") or "temporary")
            if effective_type == "base":
                retention_days = 0
                updates["retention_days"] = retention_days
                updates["expires_at"] = None
            else:
                rd_raw = int(payload.retention_days)
                retention_days = 0 if rd_raw <= 0 else min(rd_raw, 3650)
                updates["retention_days"] = retention_days
                created_at = _parse_datetime(branch.get("created_at"))
                updates["expires_at"] = _branch_expiry(retention_days, created_at)
        if payload.status is not None:
            if payload.status not in {"running", "paused"}:
                return _error_response(message="分支状态不支持")
            updates["status"] = payload.status
        if not updates:
            return _success_response(message="分支未变化")
        ok = await db.update_strm_sync_branch(branch_id, **updates)
        if not ok:
            return _error_response(message="分支更新失败")
        return _success_response(message="分支更新成功")
    except Exception as e:
        return _error_response(message=f"分支更新失败: {str(e)}")


@router.delete("/tasks/{task_id}/branches/{branch_id}")
async def delete_task_branch(task_id: int, branch_id: int, session_data: dict = Depends(require_admin_auth)):
    branch = await db.get_strm_sync_branch(branch_id)
    if not branch or int(branch.get("task_id")) != task_id:
        return _error_response(message="分支不存在")
    ok = await db.delete_strm_sync_branch(branch_id)
    if not ok:
        return _error_response(message="分支删除失败")
    return _success_response(message="分支删除成功")


@router.post("/generate-current-directory")
async def generate_current_directory_strm(payload: StrmCurrentDirectoryGeneratePayload, session_data: dict = Depends(require_admin_auth)):
    try:
        result = await strm_sync_manager.generate_current_directory_strm(
            account_id=int(payload.account_id),
            current_path=str(payload.path or "/"),
            items=payload.items or [],
        )
        if int(result.get("matched_task_id") or 0) <= 0:
            return _error_response(message="当前目录不在任何 STRM 任务范围内", data=result)
        return _success_response(data=result, message="当前目录 STRM 生成完成")
    except Exception as e:
        return _error_response(message=f"当前目录 STRM 生成失败: {str(e)}")


@router.post("/tasks/{task_id}/force-stop")
async def force_stop_task(task_id: int, session_data: dict = Depends(require_admin_auth)):
    try:
        success = await strm_sync_manager.force_stop_task(task_id)
        if success:
            return _success_response(message="任务已强制停止，下次调度不受影响")
        else:
            return _success_response(message="任务未在执行中")
    except Exception as e:
        return _error_response(message=f"强制停止失败: {str(e)}")

@router.post("/tasks/run-all")
async def run_all_tasks(session_data: dict = Depends(require_admin_auth)):
    tasks = await db.get_strm_sync_tasks()
    count = 0
    for t in tasks:
        if str(t.get("status")) != "running":
            continue
        runtime_state = await strm_sync_manager.run_task_now(int(t["id"]))
        if runtime_state != "missing":
            count += 1
    return _success_response(data={"count": count}, message="已触发全部执行")


class ReplaceDomainPayload(BaseModel):
    new_base_url: str


@router.post("/replace-domain")
async def replace_domain(payload: ReplaceDomainPayload, session_data: dict = Depends(require_admin_auth)):
    try:
        value = (payload.new_base_url or "").strip()
        if not value:
            return _error_response(message="新域名不能为空")
        if not re.match(r"^https?://\S+$", value):
            return _error_response(message="新域名格式不正确，示例：https://litepan.top")
        result = await strm_sync_manager.replace_strm_domain(value)
        return _success_response(data=result, message="批量替换完成")
    except Exception as e:
        return _error_response(message=f"批量替换失败: {str(e)}")
