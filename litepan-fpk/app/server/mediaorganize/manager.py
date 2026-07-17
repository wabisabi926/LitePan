import asyncio
import json
import os
import tempfile
import uuid
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from database.db import db
from core.driver_base import _extra_api_delay
from core.driver_service import get_account_driver_instance
from core.operation_wrapper import current_account_id
from core.dependency_container import get_cache_cleaner

from mediaorganize import rules
from mediaorganize.planner import Plan, Planner
from mediaorganize.executor import Executor


_PLAN_DIR = Path("data") / "media_organize_plans"


def _plan_file_path(task_id: str) -> Path:
    return _PLAN_DIR / f"{task_id}.json"


_LOG_LIMIT = 800
_task_logs: Dict[str, deque] = {}
_stop_requests: set = set()
_running_tasks: Dict[str, asyncio.Task] = {}
_task_progress: Dict[str, Dict[str, Any]] = {}


class TaskAborted(Exception):
    pass


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def append_log(task_id: str, message: str) -> None:
    if not task_id:
        return
    bucket = _task_logs.setdefault(task_id, deque(maxlen=_LOG_LIMIT))
    bucket.append({"time": datetime.now().strftime("%H:%M:%S"), "message": str(message)})


def get_logs(task_id: str) -> List[Dict[str, Any]]:
    return list(_task_logs.get(task_id, []))


def clear_logs(task_id: str) -> None:
    _task_logs[task_id] = deque(maxlen=_LOG_LIMIT)


def request_stop(task_id: str) -> None:
    _stop_requests.add(task_id)
    append_log(task_id, "[MediaOrganize] 已请求停止，当前操作完成后退出")


def is_stop_requested(task_id: str) -> bool:
    return task_id in _stop_requests


def discard_stop(task_id: str) -> None:
    _stop_requests.discard(task_id)


def is_running(task_id: str) -> bool:
    running = _running_tasks.get(task_id)
    return bool(running and not running.done())


def update_progress(task_id: str, info: Dict[str, Any]) -> None:
    if not task_id:
        return
    current = _task_progress.setdefault(task_id, {})
    current.update(info or {})
    current["updated_at"] = datetime.now().strftime("%H:%M:%S")


def get_progress(task_id: str) -> Dict[str, Any]:
    return dict(_task_progress.get(task_id, {}))


def reset_progress(task_id: str) -> None:
    _task_progress.pop(task_id, None)


def _make_check_stop(task_id: str):
    def _check():
        if task_id in _stop_requests:
            raise TaskAborted()
    return _check


def _make_log_fn(task_id: str):
    def _log(message: str):
        append_log(task_id, message)
        try:
            print(message)
        except Exception:
            pass
    return _log


def _load_task_config(task_row: Dict[str, Any]) -> Dict[str, Any]:
    try:
        return json.loads(task_row.get("config", "{}") or "{}")
    except Exception:
        return {}


async def _load_settings_dict() -> Dict[str, Any]:
    from config import config_manager
    keys = {
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
    out: Dict[str, Any] = {}
    for key, field_name in keys.items():
        out[field_name] = await config_manager.get_async(key)
    return out


async def _build_plan_for_task(task_id: str, task_row: Dict[str, Any], settings: Dict[str, Any]) -> Plan:
    cfg = _load_task_config(task_row)
    if not cfg.get("account_id"):
        raise Exception("任务未配置账号")
    action_type = (cfg.get("action_type") or "move").lower()
    if action_type == "rename" and not str(cfg.get("rename_marker") or "").strip():
        raise Exception("原地重命名必须设置标识：tmdb / 自定义 / off（不写入文件名），请先在任务配置中填写「整理标识」")
    if action_type == "move" and not str(cfg.get("target_root") or "").strip():
        raise Exception("move 模式下目标根目录不能为空，请先在任务配置中填写")
    driver = await get_account_driver_instance(int(cfg["account_id"]))
    current_account_id.set(str(cfg["account_id"]))

    log_fn = _make_log_fn(task_id)
    check_stop = _make_check_stop(task_id)

    def _progress(info: Dict[str, Any]):
        update_progress(task_id, info)

    planner = Planner(driver, cfg, settings, task_id, log_fn, check_stop, progress_fn=_progress)
    plan = await planner.build()
    plan.diagnostics["account_id"] = str(cfg["account_id"])
    return plan


async def _save_plan(task_id: str, plan: Plan) -> None:
    def _write():
        _PLAN_DIR.mkdir(parents=True, exist_ok=True)
        path = _plan_file_path(task_id)
        tmp = tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", delete=False, dir=str(_PLAN_DIR), suffix=".tmp"
        )
        try:
            json.dump(plan.to_dict(), tmp, ensure_ascii=False)
            tmp_path = tmp.name
        finally:
            tmp.close()
        os.replace(tmp_path, path)
    await asyncio.to_thread(_write)


async def _load_plan(task_id: str) -> Optional[Plan]:
    path = _plan_file_path(task_id)

    def _read():
        if not path.is_file():
            return None
        try:
            with open(path, "r", encoding="utf-8") as fp:
                return json.load(fp)
        except Exception:
            return None

    data = await asyncio.to_thread(_read)
    if not data:
        return None
    return Plan.from_dict(data)


async def _delete_plan(task_id: str) -> None:
    def _rm():
        try:
            _plan_file_path(task_id).unlink()
        except FileNotFoundError:
            pass
        except Exception:
            pass
    await asyncio.to_thread(_rm)


def _summarize(plan: Plan, aborted: bool) -> Dict[str, Any]:
    relocates = [a for a in plan.actions if a.kind == "relocate"]
    total = len(relocates) + len(plan.skipped)
    renamed = sum(
        1 for a in relocates
        if a.status == "done" and str(a.source_parent_id) == str(a.target_parent_id)
    )
    moved = sum(
        1 for a in relocates
        if a.status == "done" and str(a.source_parent_id) != str(a.target_parent_id)
    )
    skipped = sum(1 for a in relocates if a.status == "skipped") + len(plan.skipped)
    failed = sum(1 for a in plan.actions if a.status == "failed")
    return {
        "total": total,
        "renamed": renamed,
        "moved": moved,
        "skipped": skipped,
        "failed": failed,
        "stopped": aborted,
    }


async def plan_task(task_id: str) -> Dict[str, Any]:
    task = await db.get_media_organize_task(task_id)
    if not task:
        raise Exception("任务不存在")
    if is_running(task_id):
        raise Exception("任务正在执行中")

    discard_stop(task_id)
    clear_logs(task_id)
    reset_progress(task_id)
    append_log(task_id, "[MediaOrganize] 生成计划开始")
    settings = await _load_settings_dict()
    api_extra_delay = int(settings.get("api_request_interval_ms") or 300)
    delay_token = _extra_api_delay.set(api_extra_delay)
    await db.update_media_organize_task(task_id, status="planning")
    try:
        plan = await _build_plan_for_task(task_id, task, settings)
        await _save_plan(task_id, plan)
        append_log(task_id, f"[MediaOrganize] 计划生成完成: {len(plan.actions)} 个动作, 跳过 {len(plan.skipped)} 个")
        update_progress(task_id, {"stage": "done", "actions": len(plan.actions), "skipped": len(plan.skipped)})
        return {"plan": plan.to_dict(), "summary": {"actions": len(plan.actions), "skipped": len(plan.skipped)}}
    finally:
        _extra_api_delay.reset(delay_token)
        await db.update_media_organize_task(task_id, status="idle")


async def _apply_plan_runner(task_id: str, plan: Plan, cfg: Dict[str, Any]):
    settings = await _load_settings_dict()
    api_extra_delay = int(settings.get("api_request_interval_ms") or 300)
    delay_token = _extra_api_delay.set(api_extra_delay)

    aborted = False
    try:
        driver = await get_account_driver_instance(int(cfg["account_id"]))
        current_account_id.set(str(cfg["account_id"]))

        log_fn = _make_log_fn(task_id)
        check_stop = _make_check_stop(task_id)
        cache_cleaner = get_cache_cleaner()

        executor = Executor(
            driver=driver,
            plan=plan,
            task_id=task_id,
            log_fn=log_fn,
            check_stop=check_stop,
            cache_cleaner=cache_cleaner,
            overwrite_existing=bool(settings.get("overwrite_existing", False)),
        )
        await db.update_media_organize_task(task_id, status="running")
        try:
            await executor.apply()
        except TaskAborted:
            aborted = True
            append_log(task_id, "[MediaOrganize] 收到停止请求，已停止执行")
    finally:
        _extra_api_delay.reset(delay_token)
        discard_stop(task_id)

    summary = _summarize(plan, aborted)
    await _delete_plan(task_id)
    await db.update_media_organize_task(
        task_id,
        status="idle",
        last_run_at=_now(),
        last_run_result=summary,
    )
    append_log(task_id, f"[MediaOrganize] 任务完成: {summary}")


async def apply_task(task_id: str) -> Dict[str, Any]:
    task = await db.get_media_organize_task(task_id)
    if not task:
        raise Exception("任务不存在")
    if is_running(task_id):
        raise Exception("任务正在执行中")

    cfg = _load_task_config(task)
    if not cfg.get("account_id"):
        raise Exception("任务未配置账号")

    plan = await _load_plan(task_id)
    if not plan:
        raise Exception("当前没有可执行的计划，请先生成计划")

    discard_stop(task_id)
    append_log(task_id, "[MediaOrganize] 开始执行计划")

    async def _runner():
        try:
            await _apply_plan_runner(task_id, plan, cfg)
        except Exception as e:
            append_log(task_id, f"[MediaOrganize] 任务异常: {e}")
            try:
                await db.update_media_organize_task(task_id, status="idle", last_run_at=_now())
            except Exception:
                pass
        finally:
            _running_tasks.pop(task_id, None)

    runner_task = asyncio.create_task(_runner())
    _running_tasks[task_id] = runner_task
    return {"task_id": task_id, "submitted": True}


async def run_task(task_id: str) -> Dict[str, Any]:
    task = await db.get_media_organize_task(task_id)
    if not task:
        raise Exception("任务不存在")
    if is_running(task_id):
        raise Exception("任务正在执行中")

    discard_stop(task_id)
    clear_logs(task_id)
    append_log(task_id, "[MediaOrganize] 任务已提交，开始生成计划")

    async def _runner():
        try:
            settings = await _load_settings_dict()
            api_extra_delay = int(settings.get("api_request_interval_ms") or 300)
            delay_token = _extra_api_delay.set(api_extra_delay)
            try:
                await db.update_media_organize_task(task_id, status="planning")
                plan = await _build_plan_for_task(task_id, task, settings)
                await _save_plan(task_id, plan)
                append_log(task_id, f"[MediaOrganize] 计划已生成: {len(plan.actions)} 个动作, 跳过 {len(plan.skipped)} 个")
                cfg = _load_task_config(task)
                await _apply_plan_runner(task_id, plan, cfg)
            except TaskAborted:
                append_log(task_id, "[MediaOrganize] 任务已停止")
                await db.update_media_organize_task(task_id, status="idle", last_run_at=_now())
            except Exception as e:
                append_log(task_id, f"[MediaOrganize] 任务异常: {e}")
                await db.update_media_organize_task(task_id, status="idle", last_run_at=_now())
            finally:
                _extra_api_delay.reset(delay_token)
        finally:
            discard_stop(task_id)
            _running_tasks.pop(task_id, None)

    runner_task = asyncio.create_task(_runner())
    _running_tasks[task_id] = runner_task
    return {"task_id": task_id, "submitted": True}


async def get_plan(task_id: str) -> Optional[Dict[str, Any]]:
    plan = await _load_plan(task_id)
    return plan.to_dict() if plan else None


async def update_plan_action(task_id: str, action_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
    plan = await _load_plan(task_id)
    if not plan:
        raise Exception("当前没有可编辑的计划")
    target = None
    for action in plan.actions:
        if action.id == action_id:
            target = action
            break
    if not target:
        raise Exception("找不到对应的动作")
    if target.kind != "relocate":
        raise Exception("仅支持编辑整理动作")
    if target.status in ("done", "failed"):
        raise Exception("此动作已执行，无法编辑")
    new_target_name = (updates.get("target_name") or "").strip()
    if not new_target_name:
        raise Exception("目标名不能为空")
    if rules.sanitize_filename(new_target_name) != new_target_name:
        raise Exception("目标名包含非法字符")
    if new_target_name == target.target_name:
        return {"action": target.to_dict(), "changed": False}
    target.target_name = new_target_name
    target.reason = f"{target.reason} | 手动调整"
    target.metadata["edited"] = True
    await _save_plan(task_id, plan)
    return {"action": target.to_dict(), "changed": True}


async def delete_plan_action(task_id: str, action_id: str) -> Dict[str, Any]:
    plan = await _load_plan(task_id)
    if not plan:
        raise Exception("当前没有可编辑的计划")
    target = None
    for action in plan.actions:
        if action.id == action_id:
            target = action
            break
    if not target:
        raise Exception("找不到对应的动作")
    if target.kind != "relocate":
        raise Exception("仅支持删除整理动作（保留依赖结构）")
    if target.status == "done":
        raise Exception("此动作已执行，无法删除")
    plan.actions = [a for a in plan.actions if a.id != action_id]
    await _save_plan(task_id, plan)
    return {"removed": action_id}


async def delete_plan_actions(task_id: str, action_ids: List[str]) -> Dict[str, Any]:
    """批量从计划中移除整理动作：单次读写，规则同 delete_plan_action（仅 relocate 且未执行）。"""
    plan = await _load_plan(task_id)
    if not plan:
        raise Exception("当前没有可编辑的计划")
    wanted = set(action_ids or [])
    if not wanted:
        return {"removed": [], "skipped": []}
    removable = {
        a.id for a in plan.actions
        if a.id in wanted and a.kind == "relocate" and a.status != "done"
    }
    skipped = [aid for aid in wanted if aid not in removable]
    if removable:
        plan.actions = [a for a in plan.actions if a.id not in removable]
        await _save_plan(task_id, plan)
    return {"removed": list(removable), "skipped": skipped}
