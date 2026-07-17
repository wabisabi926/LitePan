"""跨盘秒传管理接口。"""

import asyncio
import json
from typing import List, Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from api.deps import require_admin_auth
from api.responses import error_response as _error_response, success_response as _success_response
from cross_transfer import build_routes, execute_stream, probe_stream, scan_source
from cross_transfer.relay_task_manager import relay_task_manager
from core.log_manager import LogModule, get_writer

router = APIRouter(prefix="/api/cross-transfer", tags=["跨盘秒传"])


def _log():
    return get_writer(LogModule.API)


class ScanRequest(BaseModel):
    source_account_id: int
    source_parent_id: str
    method: str
    source_display_path: str = ""


class ProbeFile(BaseModel):
    source_file_id: Optional[str] = ""
    rel_path: Optional[str] = ""
    name: str
    size: int = 0
    hash: str = ""


class ProbeRequest(BaseModel):
    source_account_id: int
    target_account_id: int
    target_parent_id: str
    method: str
    files: List[ProbeFile]


class TransferFile(BaseModel):
    source_file_id: Optional[str] = ""
    rel_path: Optional[str] = ""
    rel_dir: Optional[str] = ""
    name: str
    size: int = 0
    hash: str


class ExecuteRequest(BaseModel):
    source_account_id: int
    source_account_name: str = ""
    source_driver_type: str = ""
    target_account_id: int
    target_account_name: str = ""
    target_driver_type: str = ""
    target_parent_id: str
    target_display_path: str = ""
    method: str
    files: List[TransferFile]
    conflict: str = "rename"
    fallback: bool = False


class RelayTaskBatchDeleteRequest(BaseModel):
    task_ids: List[str]


@router.get("/routes")
async def get_routes(session_data: dict = Depends(require_admin_auth)):
    try:
        routes = build_routes()
        return _success_response(data=routes, message=f"可用线路 {len(routes)} 条")
    except Exception as exc:
        _log().error(f"获取跨盘秒传线路失败: {exc}")
        return _error_response(message=f"获取线路失败: {exc}", data=[])


@router.post("/scan")
async def scan(req: ScanRequest, session_data: dict = Depends(require_admin_auth)):
    try:
        result = await scan_source(
            source_account_id=req.source_account_id,
            source_parent_id=req.source_parent_id,
            method_id=req.method,
            source_display_path=req.source_display_path,
        )
        return _success_response(data=result, message=f"已扫描 {result['total']} 个文件")
    except Exception as exc:
        _log().error(f"跨盘秒传扫描失败: {exc}")
        return _error_response(message=f"扫描失败: {exc}")


@router.post("/probe")
async def probe(req: ProbeRequest, session_data: dict = Depends(require_admin_auth)):
    async def generate():
        try:
            async for msg in probe_stream(
                source_account_id=req.source_account_id,
                target_account_id=req.target_account_id,
                target_parent_id=req.target_parent_id,
                method_id=req.method,
                files=[f.model_dump() for f in req.files],
            ):
                yield json.dumps(msg, ensure_ascii=False) + "\n"
        except Exception as exc:
            _log().error(f"跨盘秒传试探失败: {exc}")
            yield json.dumps({"event": "error", "message": str(exc)}, ensure_ascii=False) + "\n"

    return StreamingResponse(generate(), media_type="application/x-ndjson; charset=utf-8")


@router.post("/execute")
async def execute(req: ExecuteRequest, session_data: dict = Depends(require_admin_auth)):
    async def generate():
        try:
            async for msg in execute_stream(
                source_account_id=req.source_account_id,
                source_account_name=req.source_account_name,
                source_driver_type=req.source_driver_type,
                target_account_id=req.target_account_id,
                target_account_name=req.target_account_name,
                target_driver_type=req.target_driver_type,
                target_parent_id=req.target_parent_id,
                target_display_path=req.target_display_path,
                method_id=req.method,
                files=[f.model_dump() for f in req.files],
                conflict=req.conflict,
                fallback=req.fallback,
            ):
                yield json.dumps(msg, ensure_ascii=False) + "\n"
        except Exception as exc:
            _log().error(f"跨盘秒传执行失败: {exc}")
            yield json.dumps({"event": "error", "message": str(exc)}, ensure_ascii=False) + "\n"

    return StreamingResponse(generate(), media_type="application/x-ndjson; charset=utf-8")


@router.get("/relay/tasks")
async def list_relay_tasks(session_data: dict = Depends(require_admin_auth)):
    try:
        tasks = await relay_task_manager.list_tasks()
        return _success_response(data=tasks, message="获取跨盘任务成功")
    except Exception as exc:
        return _error_response(message=f"获取跨盘任务失败: {exc}", data=[])


@router.get("/relay/tasks/stream")
async def stream_relay_tasks(request: Request, session_data: dict = Depends(require_admin_auth)):
    async def event_stream():
        queue = await relay_task_manager.subscribe_task_stream()
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield f"event: tasks\ndata: {payload}\n\n"
                except asyncio.TimeoutError:
                    yield "event: ping\ndata: {}\n\n"
        finally:
            await relay_task_manager.unsubscribe_task_stream(queue)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/relay/tasks/batch-delete")
async def batch_delete_relay_tasks(
    req: RelayTaskBatchDeleteRequest,
    session_data: dict = Depends(require_admin_auth),
):
    try:
        removed = await relay_task_manager.delete_tasks(req.task_ids or [])
        return _success_response(data={"removed": removed}, message=f"已删除 {removed} 个跨盘任务")
    except Exception as exc:
        return _error_response(message=f"删除跨盘任务失败: {exc}")
