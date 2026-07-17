"""本地存储驱动的内部下载 endpoint。
"""

from __future__ import annotations

import asyncio
import mimetypes
import os
from ipaddress import ip_address
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import FileResponse, Response, StreamingResponse

from api.deps import require_admin_auth
from core.log_manager import LogModule, get_writer
from drivers.LocalFs.driver import LocalFsDriver, verify_local_fs_signature

router = APIRouter(prefix="/api/local-fs", tags=["LocalFs"])

admin_router = APIRouter(prefix="/api/admin/local-fs", tags=["LocalFs管理"])


def _logger():
    return get_writer(LogModule.API)


def _client_is_loopback(request: Request) -> bool:
    """只允许 LitePan 自身（webdav range_proxy、files、emby 等）回环调用。"""
    client = request.client
    if not client or not client.host:
        return False
    try:
        ip = ip_address(client.host)
    except ValueError:
        return False
    return ip.is_loopback


async def _resolve_driver(account_id: str) -> Optional[LocalFsDriver]:
    try:
        from core.driver_service import get_account_driver_instance
        driver = await get_account_driver_instance(int(account_id))
        if isinstance(driver, LocalFsDriver):
            return driver
    except Exception as e:
        _logger().warning(f"LocalFs 解析 driver 失败: {account_id} {e}")
    return None


def _stream_with_range(path: Path, range_header: str | None):
    """简单的 Range 流式响应；FileResponse 不直接支持自定义 Range，所以自己实现。"""
    file_size = path.stat().st_size
    content_type = mimetypes.guess_type(str(path))[0] or "application/octet-stream"

    if not range_header:
        return FileResponse(
            path,
            media_type=content_type,
            headers={
                "Accept-Ranges": "bytes",
                "Content-Length": str(file_size),
            },
        )

    if file_size <= 0:
        return Response(
            status_code=200,
            headers={
                "Accept-Ranges": "bytes",
                "Content-Length": "0",
                "Content-Type": content_type,
            },
        )

    try:
        units, rng = range_header.strip().split("=", 1)
        if units.lower() != "bytes":
            raise ValueError
        start_s, end_s = rng.split("-", 1)
        if start_s == "" and end_s == "":
            raise ValueError
        if start_s == "":
            suffix = int(end_s)
            if suffix <= 0:
                raise ValueError
            start = max(0, file_size - suffix)
            end = file_size - 1
        else:
            start = int(start_s)
            end = int(end_s) if end_s else file_size - 1
            if end >= file_size:
                end = file_size - 1
            if start < 0 or start > end:
                raise ValueError
    except Exception:
        return Response(
            status_code=416,
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    length = end - start + 1

    async def _gen():
        chunk_size = 1024 * 1024
        f = await asyncio.to_thread(open, str(path), "rb")
        try:
            await asyncio.to_thread(f.seek, start)
            remaining = length
            while remaining > 0:
                chunk = await asyncio.to_thread(f.read, min(chunk_size, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                yield chunk
        finally:
            await asyncio.to_thread(f.close)

    return StreamingResponse(
        _gen(),
        status_code=206,
        media_type=content_type,
        headers={
            "Accept-Ranges": "bytes",
            "Content-Length": str(length),
            "Content-Range": f"bytes {start}-{end}/{file_size}",
        },
    )


@router.api_route("/raw/{account_id}/{rel_path:path}", methods=["GET", "HEAD"])
async def local_fs_raw(account_id: str, rel_path: str, request: Request):
    if not _client_is_loopback(request):
        return Response(status_code=403, content="Forbidden: loopback only")

    try:
        expires_at = int(request.query_params.get("expires") or 0)
        signature = (request.query_params.get("sig") or "").strip()
    except ValueError:
        return Response(status_code=400, content="bad expires")

    if not verify_local_fs_signature(account_id, rel_path, expires_at, signature):
        return Response(status_code=403, content="bad or expired signature")

    driver = await _resolve_driver(account_id)
    if not driver:
        return Response(status_code=404, content="account not found")

    try:
        target = driver._safe_resolve(rel_path)
    except ValueError as e:
        return Response(status_code=403, content=f"path rejected: {e}")
    if not target.exists() or not target.is_file():
        return Response(status_code=404, content="file not found")

    if request.method == "HEAD":
        file_size = target.stat().st_size
        content_type = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        return Response(
            status_code=200,
            headers={
                "Accept-Ranges": "bytes",
                "Content-Length": str(file_size),
                "Content-Type": content_type,
            },
        )

    range_header = request.headers.get("Range")
    return _stream_with_range(target, range_header)


# ============== Admin: 容器内目录浏览（用于添加/编辑 LocalFs 账号时的目录选择）==============

_BROWSE_DEFAULT_ROOTS = ["/app/strm", "/app", "/data", "/mnt", "/media", "/"]


def _browse_default_path() -> str:
    """启动浏览时的初始路径：选第一个存在的预设根。"""
    for candidate in _BROWSE_DEFAULT_ROOTS:
        try:
            if os.path.isdir(candidate):
                return candidate
        except OSError:
            continue
    return "/"


@admin_router.get("/browse")
async def browse_local_directory(
    path: str = "",
    session_data: dict = Depends(require_admin_auth),
):
    """列出容器内某个目录下的子目录，供前端做目录选择器使用。

    安全说明：endpoint 受 admin 鉴权保护；目录列表本身不暴露文件内容，
    且只列子目录（不列文件名），泄露面有限。
    """
    raw = (path or "").strip()
    if not raw:
        raw = _browse_default_path()
    if not raw.startswith("/"):
        return {
            "success": False,
            "message": "请使用绝对路径（以 / 开头）",
            "data": None,
        }

    try:
        target = Path(raw).resolve(strict=False)
    except Exception as e:
        return {"success": False, "message": f"路径解析失败: {e}", "data": None}

    if not target.exists():
        return {
            "success": False,
            "message": f"目录不存在: {target}",
            "data": {
                "path": str(target),
                "parent": str(target.parent) if str(target) != "/" else None,
                "dirs": [],
                "exists": False,
            },
        }
    if not target.is_dir():
        return {
            "success": False,
            "message": f"该路径不是目录: {target}",
            "data": None,
        }

    dirs = []
    try:
        with os.scandir(target) as it:
            for entry in it:
                if not entry.is_dir(follow_symlinks=False):
                    continue
                if entry.name.startswith("."):
                    continue
                dirs.append({"name": entry.name, "path": str(Path(entry.path))})
    except PermissionError:
        return {
            "success": False,
            "message": f"无读取权限: {target}",
            "data": None,
        }
    except Exception as e:
        return {"success": False, "message": f"读取失败: {e}", "data": None}

    dirs.sort(key=lambda x: x["name"].lower())
    parent = str(target.parent) if str(target) != "/" else None

    return {
        "success": True,
        "message": "ok",
        "data": {
            "path": str(target),
            "parent": parent,
            "dirs": dirs,
            "exists": True,
            "writable": os.access(target, os.W_OK),
        },
    }


@admin_router.post("/ensure-dir")
async def ensure_local_directory(
    payload: dict,
    session_data: dict = Depends(require_admin_auth),
):
    """在容器内新建一个目录（添加 LocalFs 账号时可能要现场建一个）。"""
    raw_parent = str(payload.get("parent") or "").strip()
    name = str(payload.get("name") or "").strip()
    if not raw_parent.startswith("/"):
        return {"success": False, "message": "父目录必须是绝对路径"}
    if not name or "/" in name or name in (".", ".."):
        return {"success": False, "message": "目录名无效"}

    try:
        parent = Path(raw_parent).resolve(strict=False)
        target = (parent / name).resolve(strict=False)
        target.relative_to(parent)
    except Exception as e:
        return {"success": False, "message": f"路径无效: {e}"}

    if not parent.is_dir():
        return {"success": False, "message": "父目录不存在"}
    if target.exists():
        return {"success": False, "message": f"目录已存在: {target.name}"}

    try:
        target.mkdir(parents=False, exist_ok=False)
    except PermissionError:
        return {"success": False, "message": f"无写入权限: {parent}"}
    except Exception as e:
        return {"success": False, "message": f"创建失败: {e}"}

    return {
        "success": True,
        "message": "目录已创建",
        "data": {"path": str(target)},
    }
