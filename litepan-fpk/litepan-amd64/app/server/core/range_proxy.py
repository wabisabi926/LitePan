"""通用 HTTP Range 代理。
供浏览器、WebDAV、STRM、文件下载这些需要本地代理的场景共用。
"""

import asyncio
import mimetypes
from dataclasses import replace
from datetime import datetime
from email.utils import format_datetime, parsedate_to_datetime
from typing import Optional, Tuple
from urllib.parse import quote

import aiohttp
from fastapi import Request
from fastapi.responses import Response, StreamingResponse

from core.base import FileItem
from core.driver_service import (
    build_upstream_download_headers,
    get_driver_proxy_part_size,
    resolve_download,
)



_proxy_session: Optional[aiohttp.ClientSession] = None


def get_range_proxy_session() -> aiohttp.ClientSession:
    global _proxy_session
    if _proxy_session is None or _proxy_session.closed:
        timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=300)
        connector = aiohttp.TCPConnector(limit=1000, limit_per_host=100, keepalive_timeout=60)
        cookie_jar = aiohttp.DummyCookieJar()
        _proxy_session = aiohttp.ClientSession(timeout=timeout, connector=connector, cookie_jar=cookie_jar)
    return _proxy_session


async def close_range_proxy_session() -> None:
    global _proxy_session
    session = _proxy_session
    _proxy_session = None
    if session and not session.closed:
        await session.close()


# ---- MIME / Content-Type ----

_FALLBACK_MIME_TYPES = {
    "mp4": "video/mp4",
    "m4v": "video/x-m4v",
    "iso": "application/x-iso9660-image",
    "mkv": "video/x-matroska",
    "ts": "video/mp2t",
    "m2ts": "video/mp2t",
    "mts": "video/mp2t",
    "flv": "video/x-flv",
    "webm": "video/webm",
    "mov": "video/quicktime",
    "avi": "video/x-msvideo",
    "wmv": "video/x-ms-wmv",
    "rmvb": "video/vnd.rn-realmedia-vbr",
    "rm": "application/vnd.rn-realmedia",
    "3gp": "video/3gpp",
    "3g2": "video/3gpp2",
    "asf": "video/x-ms-asf",
    "vob": "video/dvd",
    "f4v": "video/mp4",
    "mpd": "application/dash+xml",
    "m3u8": "application/vnd.apple.mpegurl",
    "mp3": "audio/mpeg",
    "aac": "audio/aac",
    "flac": "audio/flac",
    "wav": "audio/wav",
    "ogg": "audio/ogg",
    "oga": "audio/ogg",
    "opus": "audio/ogg",
    "m4a": "audio/mp4",
    "wma": "audio/x-ms-wma",
    "ape": "audio/x-ape",
    "alac": "audio/alac",
}


def guess_content_type(file_name: str) -> str:
    name = file_name or ""
    mime_type, _ = mimetypes.guess_type(name)
    if mime_type:
        return mime_type
    if "." in name:
        ext = name.rsplit(".", 1)[-1].strip().lower()
        if ext in _FALLBACK_MIME_TYPES:
            return _FALLBACK_MIME_TYPES[ext]
    return "application/octet-stream"


def is_stream_preview_type(file_name: str) -> bool:
    content_type = guess_content_type(file_name)
    return content_type.startswith("video/") or content_type.startswith("audio/")



def build_stable_etag(file_info: FileItem) -> str:
    modified_part = ""
    if file_info.modified:
        try:
            modified_part = f"-{int(file_info.modified.timestamp())}"
        except Exception:
            modified_part = ""
    safe_id = quote(str(file_info.id), safe="")
    return f"\"{safe_id}-{file_info.size}{modified_part}\""


def build_proxy_file_info(
    file_id: str,
    *,
    file_name: str,
    file_size: int,
    template: Optional[FileItem] = None,
) -> FileItem:
    modified = getattr(template, "modified", None) if template else None
    created = getattr(template, "created", None) if template else None
    name = file_name or (getattr(template, "name", None) if template else None) or f"file_{file_id}"
    return FileItem(
        id=file_id,
        name=name,
        path=getattr(template, "path", "") if template else "",
        size=int(file_size or 0),
        is_dir=False,
        modified=modified,
        created=created,
    )


def build_proxy_file_info_from_download(file_id: str, download) -> Tuple[Optional[FileItem], str, int]:
    raw_file_info = getattr(download, "file_info", None)
    file_size = int(getattr(download, "file_size", 0) or 0)
    raw_size = int(getattr(raw_file_info, "size", 0) or 0) if raw_file_info else 0

    if raw_file_info and not raw_file_info.is_dir and raw_size > 0:
        return raw_file_info, download.download_url, raw_size

    effective_size = file_size if file_size > 0 else raw_size
    if effective_size <= 0:
        return raw_file_info, download.download_url, 0

    file_info = build_proxy_file_info(
        file_id,
        file_name=getattr(download, "file_name", "") or f"file_{file_id}",
        file_size=effective_size,
        template=raw_file_info,
    )
    return file_info, download.download_url, effective_size


# ---- Range 解析 / 条件请求 ----

def parse_single_range(range_header: str, total_size: int) -> Tuple[int, int]:
    if total_size <= 0:
        raise ValueError("empty")
    header_value = (range_header or "").strip()
    if not header_value.startswith("bytes="):
        raise ValueError("unit")
    ranges_str = header_value[len("bytes="):].strip()
    parts = [part.strip() for part in ranges_str.split(",") if part.strip()]
    if len(parts) != 1:
        raise ValueError("multiple")
    part = parts[0]
    if "-" not in part:
        raise ValueError("format")
    start_str, end_str = part.split("-", 1)
    if start_str == "":
        suffix_length = int(end_str) if end_str else 0
        if suffix_length <= 0:
            raise ValueError("suffix")
        if suffix_length >= total_size:
            return 0, total_size - 1
        return total_size - suffix_length, total_size - 1
    start = int(start_str)
    if start < 0 or start >= total_size:
        raise ValueError("start")
    if end_str == "":
        return start, total_size - 1
    end = int(end_str)
    if end < start:
        raise ValueError("order")
    if end >= total_size:
        end = total_size - 1
    return start, end


def evaluate_if_range(request: Request, etag: str, last_modified: Optional[datetime]) -> bool:
    value = (request.headers.get("If-Range") or "").strip()
    if not value:
        return True
    if value.startswith('"'):
        return value == etag
    try:
        if_range_dt = parsedate_to_datetime(value)
        if if_range_dt.tzinfo:
            if_range_dt = if_range_dt.astimezone(tz=None).replace(tzinfo=None)
        if last_modified and last_modified.replace(microsecond=0) <= if_range_dt.replace(microsecond=0):
            return True
        return False
    except Exception:
        return False


def match_if_none_match(request: Request, etag: str) -> bool:
    value = (request.headers.get("If-None-Match") or "").strip()
    if not value:
        return False
    if value == "*":
        return True
    candidates = [item.strip() for item in value.split(",") if item.strip()]
    return etag in candidates


def match_if_modified_since(request: Request, last_modified: Optional[datetime]) -> bool:
    value = (request.headers.get("If-Modified-Since") or "").strip()
    if not value or not last_modified:
        return False
    try:
        since = parsedate_to_datetime(value)
        if since.tzinfo:
            since = since.astimezone(tz=None).replace(tzinfo=None)
        return last_modified.replace(microsecond=0) <= since.replace(microsecond=0)
    except Exception:
        return False


def format_http_datetime(value: Optional[datetime]) -> Optional[str]:
    if not value:
        return None
    try:
        return format_datetime(value, usegmt=True)
    except Exception:
        return None


DEFAULT_PROXY_PART_SIZE = 8 * 1024 * 1024


def max_slice_size(file_size: Optional[int] = None) -> int:
    return DEFAULT_PROXY_PART_SIZE


def resolve_chunk_cap(driver) -> int:
    declared = get_driver_proxy_part_size(driver)
    return declared if declared > 0 else DEFAULT_PROXY_PART_SIZE


def stream_chunk_size(file_size: Optional[int]) -> int:
    if not file_size:
        return 262144
    if file_size > 42949672960:
        return 2097152
    if file_size > 10737418240:
        return 1048576
    if file_size > 2147483648:
        return 524288
    return 262144

_CLIENT_FORWARD_HEADERS = (
    "origin",
    "accept-language",
    "sec-fetch-site",
    "sec-fetch-mode",
    "sec-fetch-dest",
    "sec-ch-ua",
    "sec-ch-ua-mobile",
    "sec-ch-ua-platform",
)


async def open_upstream_get(
    *,
    driver,
    file_id: str,
    user_agent: str,
    initial_url: str,
    file_info: Optional[FileItem],
    range_header: Optional[str],
    client_headers: Optional[dict] = None,
    upstream_headers_override: Optional[dict] = None,
) -> aiohttp.ClientResponse:
    session = get_range_proxy_session()
    current_url = initial_url
    current_headers_override = upstream_headers_override
    resp: Optional[aiohttp.ClientResponse] = None
    for attempt in range(2):
        headers = await build_upstream_download_headers(
            driver,
            file_id,
            user_agent,
            range_header=range_header,
            prefer_identity=True,
            headers_override=current_headers_override,
        )
        if client_headers:
            for key in _CLIENT_FORWARD_HEADERS:
                value = client_headers.get(key, "")
                if value:
                    headers.setdefault(key, value)
        headers.pop("Cache-Control", None)
        resp = await session.get(current_url, headers=headers)
        if resp.status in {401, 403} and attempt == 0:
            resp.release()
            refreshed = await resolve_download(driver, file_id, user_agent, file_info=file_info)
            if refreshed.download_url:
                current_url = refreshed.download_url
                current_headers_override = refreshed.headers
            continue
        return resp
    assert resp is not None
    return resp


def _base_headers(
    file_info: FileItem,
    content_type: str,
    etag: str,
    content_disposition: str = "",
) -> dict:
    headers = {
        "Content-Type": content_type,
        "Accept-Ranges": "bytes",
        "ETag": etag,
    }
    last_modified = format_http_datetime(file_info.modified)
    if last_modified:
        headers["Last-Modified"] = last_modified
    if content_disposition:
        headers["Content-Disposition"] = content_disposition
    return headers


async def _stream_chunked_range(
    *,
    driver,
    file_id: str,
    user_agent: str,
    initial_url: str,
    file_info: Optional[FileItem],
    range_start: int,
    range_end: int,
    chunk_cap: int,
    client_headers: Optional[dict] = None,
    upstream_headers_override: Optional[dict] = None,
):

    cur = range_start
    while cur <= range_end:
        slice_end = min(cur + chunk_cap - 1, range_end)
        upstream_range = f"bytes={cur}-{slice_end}"
        resp = await open_upstream_get(
            driver=driver,
            file_id=file_id,
            user_agent=user_agent,
            initial_url=initial_url,
            file_info=file_info,
            range_header=upstream_range,
            client_headers=client_headers,
            upstream_headers_override=upstream_headers_override,
        )
        if resp.status >= 400:
            try:
                resp.release()
            except Exception:
                pass
            return
        try:
            async for chunk in resp.content.iter_chunked(1048576):
                yield chunk
        except (asyncio.CancelledError, ConnectionResetError, BrokenPipeError):
            try:
                resp.release()
            except Exception:
                pass
            return
        finally:
            try:
                resp.release()
            except Exception:
                pass
        cur = slice_end + 1


# ---- HEAD ----

def _parse_total_from_content_range(value: str) -> int:

    v = (value or "").strip()
    if "/" not in v:
        return 0
    tail = v.split("/")[-1].strip()
    if tail == "*":
        return 0
    try:
        return int(tail)
    except ValueError:
        return 0


async def _probe_file_size_via_range0(
    *,
    driver,
    file_id: str,
    user_agent: str,
    initial_url: str,
    file_info: FileItem,
    client_headers: Optional[dict] = None,
    upstream_headers_override: Optional[dict] = None,
) -> int:

    resp = await open_upstream_get(
        driver=driver,
        file_id=file_id,
        user_agent=user_agent,
        initial_url=initial_url,
        file_info=file_info,
        range_header="bytes=0-0",
        client_headers=client_headers,
        upstream_headers_override=upstream_headers_override,
    )
    try:
        if resp.status == 206:
            return _parse_total_from_content_range(resp.headers.get("Content-Range", ""))
    finally:
        try:
            resp.release()
        except Exception:
            pass
    return 0


async def _serve_forwarded_upstream_range(
    *,
    driver,
    file_id: str,
    file_info: FileItem,
    etag: str,
    user_agent: str,
    range_header: str,
    initial_url: str,
    content_type: str,
    content_disposition: str,
    client_headers: Optional[dict] = None,
    upstream_headers_override: Optional[dict] = None,
) -> Response:
    """总大小未知时把客户端 Range 交给 CDN；返回 206 则透传（ISO 等依赖随机 Range）。"""
    rh = (range_header or "").strip()
    if not rh.lower().startswith("bytes="):
        return Response(status_code=400, content="Bad Range unit")
    ranges_blob = rh[6:].strip()
    if "," in ranges_blob:
        return Response(status_code=416, content="Multiple ranges not supported")

    upstream = await open_upstream_get(
        driver=driver,
        file_id=file_id,
        user_agent=user_agent,
        initial_url=initial_url,
        file_info=file_info,
        range_header=rh,
        client_headers=client_headers,
        upstream_headers_override=upstream_headers_override,
    )

    st = upstream.status
    cr = (upstream.headers.get("Content-Range") or "").strip()
    cl = (upstream.headers.get("Content-Length") or "").strip()

    chunk_size = stream_chunk_size(0)

    def base_hdrs() -> dict:
        h = _base_headers(file_info, content_type, etag, content_disposition)
        if cr:
            h["Content-Range"] = cr
        if cl:
            h["Content-Length"] = cl
        return h

    if st == 206:
        async def gen206():
            try:
                async for chunk in upstream.content.iter_chunked(chunk_size):
                    yield chunk
            finally:
                try:
                    upstream.release()
                except Exception:
                    pass

        return StreamingResponse(
            gen206(), status_code=206, headers=base_hdrs(), media_type=content_type
        )

    # 少数 CDN 对 Range 仍回 200，但带 Content-Range（视作分片成功）
    if st == 200 and cr and _parse_total_from_content_range(cr) > 0:
        async def gen200partial():
            try:
                async for chunk in upstream.content.iter_chunked(chunk_size):
                    yield chunk
            finally:
                try:
                    upstream.release()
                except Exception:
                    pass

        hdrs = base_hdrs()
        hdrs["Content-Range"] = cr
        if cl:
            hdrs["Content-Length"] = cl
        return StreamingResponse(
            gen200partial(), status_code=206, headers=hdrs, media_type=content_type
        )

    try:
        upstream.release()
    except Exception:
        pass
    return Response(
        status_code=502,
        content="Upstream did not return partial content for range request",
    )


def build_head_response(
    file_info: FileItem,
    *,
    content_type: Optional[str] = None,
    content_disposition: str = "",
) -> Response:
    """HEAD 响应：只回元数据 + Accept-Ranges，让 NDM/IDM 探测到「能续传」。"""
    total_size = int(file_info.size or 0)
    ct = content_type or guess_content_type(file_info.name or "")
    etag = build_stable_etag(file_info)
    headers = _base_headers(file_info, ct, etag, content_disposition)
    if total_size > 0:
        headers["Content-Length"] = str(total_size)
    return Response(status_code=200, headers=headers, media_type=ct)


# ---- 主入口：serve_range_proxy ----

async def serve_range_proxy(
    *,
    driver,
    file_id: str,
    file_info: FileItem,
    request: Request,
    initial_url: str,
    content_disposition: str = "",
    user_agent_override: Optional[str] = None,
    upstream_headers_override: Optional[dict] = None,
) -> Response:

    if file_info.is_dir:
        return Response(status_code=400, content="Cannot stream a directory")

    total_size = int(file_info.size or 0)
    etag = build_stable_etag(file_info)
    content_type = guess_content_type(file_info.name or "")
    user_agent = user_agent_override if user_agent_override is not None else request.headers.get("User-Agent", "")

    client_hdrs = dict(request.headers)


    if request.method == "HEAD":
        effective_info = file_info
        if int(file_info.size or 0) <= 0:
            probed = await _probe_file_size_via_range0(
                driver=driver,
                file_id=file_id,
                user_agent=user_agent,
                initial_url=initial_url,
                file_info=file_info,
                client_headers=client_hdrs,
                upstream_headers_override=upstream_headers_override,
            )
            if probed > 0:
                effective_info = replace(file_info, size=probed)
        return build_head_response(
            effective_info, content_type=content_type, content_disposition=content_disposition
        )

    raw_range_header = request.headers.get("Range")

    # 304 / If-None-Match / If-Modified-Since
    if not raw_range_header and (
        match_if_none_match(request, etag) or match_if_modified_since(request, file_info.modified)
    ):
        headers = {"ETag": etag}
        last_modified = format_http_datetime(file_info.modified)
        if last_modified:
            headers["Last-Modified"] = last_modified
        if content_disposition:
            headers["Content-Disposition"] = content_disposition
        return Response(status_code=304, headers=headers)

    # If-Range 不匹配 → 当作无 Range 处理
    if raw_range_header and not evaluate_if_range(request, etag, file_info.modified):
        raw_range_header = None

    if raw_range_header and total_size > 0:
        return await _serve_range(
            driver=driver,
            file_id=file_id,
            file_info=file_info,
            etag=etag,
            user_agent=user_agent,
            range_header=raw_range_header,
            total_size=total_size,
            initial_url=initial_url,
            content_type=content_type,
            content_disposition=content_disposition,
            client_headers=client_hdrs,
            upstream_headers_override=upstream_headers_override,
        )

    if raw_range_header and total_size <= 0:
        return await _serve_forwarded_upstream_range(
            driver=driver,
            file_id=file_id,
            file_info=file_info,
            etag=etag,
            user_agent=user_agent,
            range_header=raw_range_header,
            initial_url=initial_url,
            content_type=content_type,
            content_disposition=content_disposition,
            client_headers=client_hdrs,
            upstream_headers_override=upstream_headers_override,
        )

    return await _serve_full(
        driver=driver,
        file_id=file_id,
        file_info=file_info,
        etag=etag,
        user_agent=user_agent,
        total_size=total_size,
        initial_url=initial_url,
        content_type=content_type,
        content_disposition=content_disposition,
        client_headers=client_hdrs,
        upstream_headers_override=upstream_headers_override,
    )


async def _serve_range(
    *,
    driver,
    file_id: str,
    file_info: FileItem,
    etag: str,
    user_agent: str,
    range_header: str,
    total_size: int,
    initial_url: str,
    content_type: str,
    content_disposition: str,
    client_headers: Optional[dict] = None,
    upstream_headers_override: Optional[dict] = None,
) -> Response:
    try:
        start, end = parse_single_range(range_header, total_size)
    except Exception:
        headers = {"Content-Range": f"bytes */{total_size}"} if total_size > 0 else {}
        return Response(status_code=416, content="Invalid range request", headers=headers)

    chunk_cap = resolve_chunk_cap(driver)

    headers = _base_headers(file_info, content_type, etag, content_disposition)
    headers["Content-Range"] = f"bytes {start}-{end}/{total_size}"
    headers["Content-Length"] = str(end - start + 1)

    return StreamingResponse(
        _stream_chunked_range(
            driver=driver,
            file_id=file_id,
            user_agent=user_agent,
            initial_url=initial_url,
            file_info=file_info,
            range_start=start,
            range_end=end,
            chunk_cap=chunk_cap,
            client_headers=client_headers,
            upstream_headers_override=upstream_headers_override,
        ),
        status_code=206,
        headers=headers,
        media_type=content_type,
    )


async def _serve_full(
    *,
    driver,
    file_id: str,
    file_info: FileItem,
    etag: str,
    user_agent: str,
    total_size: int,
    initial_url: str,
    content_type: str,
    content_disposition: str,
    client_headers: Optional[dict] = None,
    upstream_headers_override: Optional[dict] = None,
) -> Response:
    headers = _base_headers(file_info, content_type, etag, content_disposition)


    if total_size <= 0:
        upstream = await open_upstream_get(
            driver=driver,
            file_id=file_id,
            user_agent=user_agent,
            initial_url=initial_url,
            file_info=file_info,
            range_header=None,
            client_headers=client_headers,
            upstream_headers_override=upstream_headers_override,
        )
        if upstream.status >= 400:
            status = upstream.status
            upstream.release()
            return Response(status_code=status, content=f"Upstream returned {status}")

        chunk_size = stream_chunk_size(total_size)

        async def stream_unknown_size():
            try:
                async for chunk in upstream.content.iter_chunked(chunk_size):
                    yield chunk
            except (asyncio.CancelledError, ConnectionResetError, BrokenPipeError):
                return
            finally:
                try:
                    upstream.release()
                except Exception:
                    pass

        upstream_length = (upstream.headers.get("Content-Length") or "").strip()
        if upstream_length.isdigit():
            headers["Content-Length"] = upstream_length
        return StreamingResponse(stream_unknown_size(), status_code=200, headers=headers, media_type=content_type)

    # 已知总大小：分片拉，对外 Content-Length=total
    chunk_cap = resolve_chunk_cap(driver)
    headers["Content-Length"] = str(total_size)
    return StreamingResponse(
        _stream_chunked_range(
            driver=driver,
            file_id=file_id,
            user_agent=user_agent,
            initial_url=initial_url,
            file_info=file_info,
            range_start=0,
            range_end=total_size - 1,
            chunk_cap=chunk_cap,
            client_headers=client_headers,
            upstream_headers_override=upstream_headers_override,
        ),
        status_code=200,
        headers=headers,
        media_type=content_type,
    )


# ---- 兼容老调用点（api/strm.py 旧版本曾 import） ----

async def assemble_chunked_proxy(
    *,
    driver,
    file_id: str,
    user_agent: str,
    download_url: str,
    download_size: int,
    client_headers: Optional[dict] = None,
    chunk_cap: Optional[int] = None,
):
    """保留旧接口：等价于 _stream_chunked_range(0, download_size-1)。"""
    if download_size <= 0:
        async def _empty():
            if False:
                yield b""
        return _empty()
    cap = chunk_cap or max_slice_size(download_size)
    return _stream_chunked_range(
        driver=driver,
        file_id=file_id,
        user_agent=user_agent,
        initial_url=download_url,
        file_info=None,
        range_start=0,
        range_end=download_size - 1,
        chunk_cap=cap,
        client_headers=client_headers,
    )
