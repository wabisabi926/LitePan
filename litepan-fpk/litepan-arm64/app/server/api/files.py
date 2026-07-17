from fastapi import APIRouter, Depends, Form, Body, Request, UploadFile, File
from fastapi.responses import StreamingResponse, RedirectResponse, JSONResponse, Response
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
import os
import uuid
import aiohttp
import urllib.parse
from core.log_manager import get_writer, LogModule


class _LazyLogWriter:
    def __init__(self, module: LogModule):
        self.module = module

    def _write(self, method: str, message: str, **kwargs):
        try:
            writer = get_writer(self.module)
            getattr(writer, method)(message, **kwargs)
        except Exception:
            print(f"{self.module.value.upper()} | {message}")

    def warning(self, message: str, **kwargs):
        self._write("warning", message, **kwargs)

    def error(self, message: str, **kwargs):
        self._write("error", message, **kwargs)


system_log = _LazyLogWriter(LogModule.SYSTEM)
api_logger = _LazyLogWriter(LogModule.WEB)
from core.driver_service import (
    build_upstream_download_headers,
    get_account_driver,
    get_account_driver_instance,
    get_effective_download_mode,
    resolve_download,
)
from core.range_proxy import (
    build_head_response,
    build_proxy_file_info,
    build_proxy_file_info_from_download,
    guess_content_type,
    is_stream_preview_type,
    serve_range_proxy,
)
from core.operation_wrapper import current_account_id, auto_refresh_cache
from core.upload_task_manager import upload_task_manager
from core.error_handler import raise_api_error
from core.response import APIResponse
from database.db import db
import asyncio
from core.dependency_container import get_cache_cleaner
from cache import get_global_cache
from cache.cache_keys import CacheKeyGenerator
from api.deps import require_admin_auth, require_public_index_access
from api.responses import error_response as _error_response, success_response as _success_response

router = APIRouter()


class DeleteRequest(BaseModel):
    account_id: int
    file_ids: List[str]
    parent_id: Optional[str] = None


class RefreshRequest(BaseModel):
    account_id: int
    parent_id: str


class FolderSizesRequest(BaseModel):
    account_id: int
    file_ids: List[str]
    parent_id: Optional[str] = None
    fetch_missing: bool = True


class MoveRequest(BaseModel):
    account_id: int
    file_ids: List[str]
    target_parent_id: str
    source_parent_id: Optional[str] = None  # 传源目录是为了让装饰器能精准清旧目录缓存

class CopyRequest(BaseModel):
    account_id: int
    file_ids: List[str]
    target_parent_id: str
    source_parent_id: Optional[str] = None

class UploadTaskBatchDeleteRequest(BaseModel):
    task_ids: List[str]
    delete_uploaded_file: bool = False


def _serialize_file_item(file) -> dict:
    return {
        "id": file.id,
        "name": file.name,
        "path": file.path,
        "size": file.size,
        "is_dir": file.is_dir,
        "modified": file.modified.isoformat() if file.modified else None,
        "created": file.created.isoformat() if file.created else None,
        "extra": file.extra
    }


def _serialize_file_list(files) -> List[dict]:
    return [_serialize_file_item(file) for file in files]


async def _get_driver_with_context(account_id: int):
    """取驱动同时把账号 ID 写进 contextvar，让 operation_wrapper 能识别出是哪个账号做的操作。"""
    driver = await get_account_driver_instance(account_id)
    current_account_id.set(str(account_id))
    return driver


async def _clear_move_related_cache(
    account_id: int,
    file_ids: List[str],
    source_parent_id: Optional[str],
    target_parent_id: Optional[str],
    result_data: Optional[dict] = None
):

    cache_cleaner = get_cache_cleaner()
    if not cache_cleaner:
        return

    result_data = result_data or {}
    account_key = str(account_id)
    parent_ids = set()

    for parent_id in result_data.get("source_parent_ids") or []:
        if parent_id not in (None, ""):
            parent_ids.add(str(parent_id))

    for parent_id in (
        source_parent_id,
        result_data.get("target_parent_id"),
        target_parent_id,
    ):
        if parent_id not in (None, ""):
            parent_ids.add(str(parent_id))

    try:
        for parent_id in parent_ids:
            await cache_cleaner._clear_directory_cache(account_key, parent_id)

        cache_manager = getattr(cache_cleaner, "cache_manager", None)
        if cache_manager:
            for file_id in file_ids:
                await cache_manager.delete(CacheKeyGenerator.file_info_key(account_key, str(file_id)))
            await cache_manager.clear_by_prefix(CacheKeyGenerator.path_mapping_prefix(account_key))
            await cache_manager.clear_by_prefix(CacheKeyGenerator.webdav_metadata_prefix(account_key))

        await cache_cleaner._clear_webdav_cache(account_key)
    except Exception as e:
        api_logger.warning(f"移动后缓存兜底清理失败: {e}")


async def _save_upload_to_temp(upload_file: UploadFile) -> str:
    os.makedirs("data/upload_tasks", exist_ok=True)
    suffix = os.path.splitext(upload_file.filename or "")[1]
    temp_path = os.path.join("data", "upload_tasks", f"{uuid.uuid4().hex}{suffix}")

    try:
        with open(temp_path, "wb") as temp_file:
            while True:
                chunk = await upload_file.read(1024 * 1024)
                if not chunk:
                    break
                temp_file.write(chunk)
        return temp_path
    except Exception:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise
    finally:
        try:
            await upload_file.close()
        except Exception as e:
            system_log.warning(f"上传文件句柄关闭异常: {e}")


@router.get("/list")
async def list_files(
    request: Request,
    account_id: int,
    path: str = "0",
    force_refresh: bool = False
) -> dict:
    """列目录。缓存由驱动层 @with_file_list_cache 统一管，这里只管拿驱动 + 序列化。"""
    try:
        await require_public_index_access(request)
        driver = await get_account_driver(account_id)

        # force_refresh 要把三类缓存都清了：目录列表 / 路径映射 / WebDAV metadata
        if force_refresh:
            cache_cleaner = get_cache_cleaner()
            if cache_cleaner:
                await cache_cleaner._clear_directory_cache(str(account_id), path)
                try:
                    await cache_cleaner.cache_manager.clear_by_prefix(
                        CacheKeyGenerator.path_mapping_prefix(str(account_id))
                    )
                except Exception as e:
                    system_log.warning(f"清理路径映射缓存失败: {e}")
                try:
                    await cache_cleaner.cache_manager.clear_by_prefix(
                        CacheKeyGenerator.webdav_metadata_prefix(str(account_id))
                    )
                except Exception as e:
                    system_log.warning(f"清理WebDAV元数据缓存失败: {e}")

        files = await driver.list_files(path)
        file_list = _serialize_file_list(files)

        return _success_response(
            data=file_list,
            message=f"成功获取 {len(file_list)} 个文件"
        )
            
    except Exception as e:
        if hasattr(e, 'error_type'):
            raise
        api_logger.error(f"获取文件列表失败: {e}")
        return _error_response(
            message=f"获取文件列表失败: {str(e)}",
            data=[]
        )


@router.post("/folder-sizes")
async def get_folder_sizes(
    request: Request,
    body: FolderSizesRequest,
) -> dict:
    """按需补全目录占用：fetch_missing=False 仅读缓存；True 时对缺失项调驱动（115 进入文件夹时用）。"""
    try:
        await require_public_index_access(request)
        file_ids = [str(file_id).strip() for file_id in (body.file_ids or []) if str(file_id).strip()]
        if not file_ids:
            return _success_response(data={}, message="成功")

        parent_id = str(body.parent_id or "").strip()
        cache_manager = get_global_cache()
        cached_sizes: Dict[str, Dict[str, Any]] = {}
        if parent_id and cache_manager:
            cached = await cache_manager.get_dir_folder_sizes_cache(str(body.account_id), parent_id)
            if isinstance(cached, dict):
                cached_sizes = cached

        missing_ids = [file_id for file_id in file_ids if file_id not in cached_sizes]
        if missing_ids and body.fetch_missing:
            driver = await get_account_driver(body.account_id)
            fetched: Dict[str, Dict[str, Any]] = {}
            if hasattr(driver, "fetch_folder_sizes"):
                fetched = await driver.fetch_folder_sizes(missing_ids)
            else:
                for file_id in missing_ids:
                    try:
                        info = await driver.file_info(file_id)
                    except Exception:
                        continue
                    if info and info.is_dir and int(info.size or 0) > 0:
                        entry: Dict[str, Any] = {"size": int(info.size)}
                        if info.modified:
                            entry["modified"] = info.modified.isoformat()
                        fetched[file_id] = entry

            if fetched:
                cached_sizes = {**cached_sizes, **fetched}
                if parent_id and cache_manager:
                    await cache_manager.set_dir_folder_sizes_cache(
                        str(body.account_id),
                        parent_id,
                        cached_sizes,
                    )

        data = {
            file_id: cached_sizes[file_id]
            for file_id in file_ids
            if file_id in cached_sizes
        }

        return _success_response(
            data=data,
            message=f"成功获取 {len(data)} 个目录大小"
        )
    except Exception as e:
        if hasattr(e, 'error_type'):
            raise
        api_logger.error(f"获取目录大小失败: {e}")
        return _error_response(message=f"获取目录大小失败: {str(e)}", data={})

@router.post("/create-folder")
async def create_folder(
    account_id: int = Form(...),
    path: str = Form(...),
    name: str = Form(...),
    _session_data: dict = Depends(require_admin_auth)
) -> dict:
    try:
        driver = await _get_driver_with_context(account_id)
        result = await driver.create_folder(path, name)

        if result.success:
            return _success_response(data=result.data, message=result.message)
        else:
            return _error_response(message=result.message)
            
    except Exception as e:
        if hasattr(e, "error_type"):
            raise
        return _error_response(message=f"创建文件夹失败: {str(e)}")

@router.post("/upload")
async def upload_file(
    account_id: int = Form(...),
    path: str = Form(...),
    file: UploadFile = File(...),
    conflict_policy: str = Form("overwrite"),
    _session_data: dict = Depends(require_admin_auth)
) -> dict:
    try:
        driver = await _get_driver_with_context(account_id)
        if not hasattr(driver, "upload_file") or not callable(getattr(driver, "upload_file")):
            return _error_response(message="当前驱动暂不支持上传文件")
        result = await driver.upload_file(file, path, conflict_policy=conflict_policy)

        if result.success:
            return _success_response(data=result.data, message=result.message)
        return _error_response(message=result.message)
    except Exception as e:
        if hasattr(e, "error_type"):
            raise
        return _error_response(message=f"上传文件失败: {str(e)}")

@router.post("/upload-task")
async def create_upload_task(
    account_id: int = Form(...),
    path: str = Form(...),
    file: UploadFile = File(...),
    conflict_policy: str = Form("overwrite"),
    client_task_id: Optional[str] = Form(None),
    display_name: Optional[str] = Form(None),
    target_display_path: Optional[str] = Form(None),
    _session_data: dict = Depends(require_admin_auth)
) -> dict:
    try:
        driver = await get_account_driver_instance(account_id)
        if not hasattr(driver, "upload_local_file") or not callable(getattr(driver, "upload_local_file")):
            return _error_response(message="当前驱动暂不支持后台上传任务")

        account = await db.get_account(account_id)
        if not account:
            return _error_response(message="账号不存在")

        temp_path = await _save_upload_to_temp(file)
        total_bytes = os.path.getsize(temp_path)
        if total_bytes <= 0:
            try:
                os.remove(temp_path)
            except OSError:
                pass
            return _error_response(message="暂不支持上传空文件")

        task = await upload_task_manager.create_upload_task(
            client_task_id=client_task_id or "",
            account_id=account_id,
            account_name=account.get("name", ""),
            driver_type=account.get("driver_type", ""),
            file_name=file.filename or os.path.basename(temp_path),
            target_path=path,
            target_display_path=str(target_display_path or "").strip(),
            local_path=temp_path,
            total_bytes=total_bytes,
            conflict_policy=conflict_policy,
            display_name=str(display_name or "").strip(),
        )
        return _success_response(data=task, message="上传任务已创建")
    except Exception as e:
        if hasattr(e, "error_type"):
            raise
        return _error_response(message=f"创建上传任务失败: {str(e)}")

@router.get("/upload/tasks")
async def list_upload_tasks(
    account_id: Optional[int] = None,
    _session_data: dict = Depends(require_admin_auth)
) -> dict:
    try:
        tasks = await upload_task_manager.list_tasks(account_id=account_id)
        return _success_response(data=tasks, message="获取上传任务成功")
    except Exception as e:
        return _error_response(message=f"获取上传任务失败: {str(e)}", data=[])

@router.get("/upload/tasks/stream")
async def stream_upload_tasks(
    request: Request,
    _session_data: dict = Depends(require_admin_auth)
):
    async def event_stream():
        queue = await upload_task_manager.subscribe_task_stream()
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
            await upload_task_manager.unsubscribe_task_stream(queue)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )

@router.get("/upload/tasks/{task_id}")
async def get_upload_task(
    task_id: str,
    _session_data: dict = Depends(require_admin_auth)
) -> dict:
    try:
        task = await upload_task_manager.get_task(task_id)
        if not task:
            return _error_response(message="上传任务不存在")
        return _success_response(data=task, message="获取上传任务成功")
    except Exception as e:
        return _error_response(message=f"获取上传任务失败: {str(e)}")

@router.post("/upload/tasks/{task_id}/pause")
async def pause_upload_task(
    task_id: str,
    _session_data: dict = Depends(require_admin_auth)
) -> dict:
    try:
        task = await upload_task_manager.pause_task(task_id)
        if not task:
            return _error_response(message="上传任务不存在")
        return _success_response(data=task, message="上传任务已暂停")
    except Exception as e:
        return _error_response(message=f"暂停上传任务失败: {str(e)}")


@router.post("/upload/tasks/{task_id}/resume")
async def resume_upload_task(
    task_id: str,
    _session_data: dict = Depends(require_admin_auth)
) -> dict:
    try:
        task = await upload_task_manager.resume_task(task_id)
        if not task:
            return _error_response(message="上传任务不存在")
        return _success_response(data=task, message="上传任务已继续")
    except Exception as e:
        return _error_response(message=f"继续上传任务失败: {str(e)}")


@router.delete("/upload/tasks/{task_id}")
async def delete_upload_task(
    task_id: str,
    delete_uploaded_file: bool = False,
    _session_data: dict = Depends(require_admin_auth)
) -> dict:
    try:
        deleted = await upload_task_manager.delete_task(task_id, delete_uploaded_file=delete_uploaded_file)
        if not deleted:
            return _error_response(message="上传任务不存在")
        return _success_response(message="上传任务已删除")
    except Exception as e:
        return _error_response(message=f"删除上传任务失败: {str(e)}")


@router.post("/upload/tasks/batch-delete")
async def batch_delete_upload_tasks(
    request: UploadTaskBatchDeleteRequest,
    _session_data: dict = Depends(require_admin_auth)
) -> dict:
    try:
        result = await upload_task_manager.batch_delete_tasks(
            request.task_ids,
            delete_uploaded_file=request.delete_uploaded_file,
        )
        return _success_response(data=result, message="批量删除上传任务成功")
    except Exception as e:
        return _error_response(message=f"批量删除上传任务失败: {str(e)}")


@router.delete("/delete")
async def delete_files(
    request: DeleteRequest,
    _session_data: dict = Depends(require_admin_auth)
) -> dict:
    try:
        driver = await _get_driver_with_context(request.account_id)

        if len(request.file_ids) == 1:
            result = await driver.delete_file(request.file_ids[0])
        else:
            result = await driver.batch_delete_file(request.file_ids)
        
        if result.success:
            return _success_response(data=result.data, message=result.message)
        else:
            return _error_response(message=result.message)
        
    except Exception as e:
        if hasattr(e, "error_type"):
            raise
        return _error_response(message=f"删除失败: {str(e)}")

@router.put("/rename")
async def rename_file(
    account_id: int = Body(...),
    old_path: str = Body(...),
    new_name: str = Body(...),
    _session_data: dict = Depends(require_admin_auth)
) -> dict:
    try:
        driver = await _get_driver_with_context(account_id)
        result = await driver.rename_file(old_path, new_name)

        if result.success:
            return _success_response(data=result.data, message=result.message)
        else:
            return _error_response(message=result.message)

    except Exception as e:
        if hasattr(e, "error_type"):
            raise
        return _error_response(message=f"重命名文件失败: {str(e)}")


@router.post("/refresh")
@auto_refresh_cache()
async def refresh_directory(
    request: RefreshRequest
) -> dict:
    """强制刷新目录：@auto_refresh_cache 负责清缓存，驱动层装饰器负责回写。"""
    try:
        driver = await get_account_driver(request.account_id)
        files = await driver.list_files(request.parent_id)
        file_list = _serialize_file_list(files)

        return _success_response(
            data=file_list,
            message=f"强制刷新完成，获取到 {len(file_list)} 个文件"
        )

    except Exception as e:
        if hasattr(e, "error_type"):
            raise
        api_logger.error(f"刷新失败: {str(e)}")
        return _error_response(message=f"刷新失败: {str(e)}", data=[])

@router.post("/move")
async def move_files(
    request: MoveRequest,
    _session_data: dict = Depends(require_admin_auth)
) -> dict:
    try:
        driver = await _get_driver_with_context(request.account_id)
        result = await driver.move_file(request.file_ids, request.target_parent_id)

        # 补齐源/目标目录信息，并用请求上下文做一次通用缓存兜底清理
        if result.success:
            if not result.data:
                result.data = {}
            if 'source_parent_ids' not in result.data or not result.data['source_parent_ids']:
                if request.source_parent_id:
                    result.data['source_parent_ids'] = [request.source_parent_id]
            if 'target_parent_id' not in result.data:
                result.data['target_parent_id'] = request.target_parent_id
            await _clear_move_related_cache(
                request.account_id,
                request.file_ids,
                request.source_parent_id,
                request.target_parent_id,
                result.data
            )
        
        if result.success:
            return _success_response(data=result.data, message=result.message)
        else:
            return _error_response(message=result.message)
        
    except Exception as e:
        if hasattr(e, "error_type"):
            raise
        return _error_response(message=f"移动失败: {str(e)}")


@router.post("/copy")
async def copy_files(
    request: CopyRequest,
    _session_data: dict = Depends(require_admin_auth)
) -> dict:
    try:
        driver = await _get_driver_with_context(request.account_id)
        result = await driver.copy_file(request.file_ids, request.target_parent_id, source_parent_id=request.source_parent_id)

        if result.success:
            if not result.data:
                result.data = {}
            if 'source_parent_ids' not in result.data or not result.data['source_parent_ids']:
                if request.source_parent_id:
                    result.data['source_parent_ids'] = [request.source_parent_id]
            if 'target_parent_id' not in result.data:
                result.data['target_parent_id'] = request.target_parent_id
            await _clear_move_related_cache(
                request.account_id,
                request.file_ids,
                request.source_parent_id,
                request.target_parent_id,
                result.data
            )

        if result.success:
            return _success_response(data=result.data, message=result.message)
        else:
            return _error_response(message=result.message, data=result.data)

    except Exception as e:
        if hasattr(e, "error_type"):
            raise
        return _error_response(message=f"复制失败: {str(e)}")


@router.get("/preview-text/{account_id}/{file_id:path}")
async def preview_text_file(
    account_id: int,
    file_id: str,
    request: Request,
    user_agent: Optional[str] = None,
    max_bytes: int = 262144,
    _session_data: dict = Depends(require_admin_auth)
):
    try:
        if not user_agent:
            user_agent = request.headers.get('User-Agent') or ''
        max_bytes = max(1024, min(int(max_bytes or 262144), 524288))
        driver = await _get_driver_with_context(account_id)

        download = await resolve_download(driver, file_id, user_agent)
        if not download.download_url:
            raise_api_error("获取预览内容失败", "preview_text", 404)

        timeout = aiohttp.ClientTimeout(total=60, connect=15, sock_read=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            headers = await build_upstream_download_headers(
                driver,
                file_id,
                user_agent,
                range_header=f"bytes=0-{max_bytes - 1}",
                prefer_identity=True,
                download=download,
            )
            headers.pop("Cache-Control", None)
            async with session.get(download.download_url, headers=headers) as response:
                if response.status == 403:
                    raise Exception("预览链接返回403错误，可能是链接已失效或无权限访问")
                response.raise_for_status()
                content = await response.content.read(max_bytes + 1)

        truncated = len(content) > max_bytes or (download.file_size and download.file_size > max_bytes)
        content = content[:max_bytes]
        for encoding in ("utf-8", "utf-8-sig", "gb18030", "latin-1"):
            try:
                text = content.decode(encoding)
                break
            except UnicodeDecodeError:
                continue

        return _success_response(
            data={
                "content": text,
                "truncated": truncated,
                "size": download.file_size,
                "preview_bytes": len(content)
            },
            message="获取文本预览成功"
        )
    except Exception as e:
        if hasattr(e, "error_type"):
            raise
        return JSONResponse(
            status_code=500,
            content=APIResponse.error(message=f"文本预览失败: {str(e)}")
        )


@router.api_route("/download/{account_id}/{file_id:path}", methods=["GET", "HEAD"])
async def download_file(
    account_id: int,
    file_id: str,
    request: Request,
    user_agent: Optional[str] = None,
    file_name: Optional[str] = None,
    preview: bool = False,
    _session_data: dict = Depends(require_admin_auth)
):
    """文件下载入口。
    - redirect 模式（115/123 等）：直接 302。
    - proxy 模式：走统一的 serve_range_proxy（内部分片，绕单连接限速；
      对外回真实 Content-Range，让 NDM/IDM 正常多线程 + 续传）。
    - HEAD：直接由 serve_range_proxy 走 build_head_response，不打上游。
    """
    try:
        if not user_agent:
            user_agent = request.headers.get('User-Agent') or ''
        driver = await _get_driver_with_context(account_id)

        download = await resolve_download(driver, file_id, user_agent)
        download_url = download.download_url
        download_name = download.file_name
        name_hint = (file_name or "").strip()
        if name_hint:
            download_name = name_hint
        if not download_url:
            raise_api_error("获取下载链接失败", "download", 404)

        download_mode = get_effective_download_mode(driver, download)
        preview_content_type = guess_content_type(download_name) if preview else ""
        force_proxy_preview = preview and preview_content_type == "application/pdf"
        if download_mode == "redirect" and not force_proxy_preview:
            if request.method == "HEAD":
                return Response(status_code=302, headers={"Location": download_url})
            return RedirectResponse(url=download_url, status_code=302)

        # proxy 模式 / PDF 预览模式：构造 FileItem，走通用 Range 代理。
        # PDF 预览需要 LitePan 控制 inline 响应，避免部分上游直链强制下载。
        file_info, proxy_url, _size = build_proxy_file_info_from_download(file_id, download)
        if file_info is None:
            file_info = build_proxy_file_info(
                file_id,
                file_name=download_name or f"file_{file_id}",
                file_size=int(download.file_size or 0),
                template=download.file_info,
            )

        encoded_filename = urllib.parse.quote(download_name or f"file_{file_id}", safe="")

        if preview and is_stream_preview_type(download_name):
            disposition = "inline"
        else:
            disposition = "inline" if preview else "attachment"
        content_disposition = f"{disposition}; filename*=UTF-8''{encoded_filename}"

        return await serve_range_proxy(
            driver=driver,
            file_id=file_id,
            file_info=file_info,
            request=request,
            initial_url=proxy_url or download_url,
            content_disposition=content_disposition,
            user_agent_override=user_agent,
            upstream_headers_override=download.headers,
        )

    except Exception as e:
        if hasattr(e, "error_type"):
            raise
        return JSONResponse(
            status_code=500,
            content=APIResponse.error(message=f"下载失败: {str(e)}")
        )
