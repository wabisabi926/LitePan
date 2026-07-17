import asyncio
import os
import time
from typing import Awaitable, Callable, List, Optional, Tuple

from core.driver_service import get_account_driver_instance, resolve_download
from core.log_manager import LogModule, get_writer
from core.range_proxy import _stream_chunked_range, build_proxy_file_info, open_upstream_get, resolve_chunk_cap

_DOWNLOAD_CONCURRENCY = 8
_MIN_PARALLEL_BYTES = 2 * 1024 * 1024
_MIN_PART_BYTES = 4 * 1024 * 1024
_PROGRESS_EMIT_INTERVAL = 0.25


def _log():
    return get_writer(LogModule.API)


ProgressCallback = Callable[[int, int, str, float], Awaitable[None]]


def _supports_parallel_download(driver) -> bool:
    checker = getattr(driver, "supports_parallel_range_download", None)
    if callable(checker):
        return bool(checker())
    return True


def _parse_content_range_total(value: str) -> int:
    text = (value or "").strip()
    if "/" not in text:
        return 0
    tail = text.split("/")[-1].strip()
    if tail == "*":
        return 0
    try:
        return int(tail)
    except ValueError:
        return 0


def plan_download_segments(total_bytes: int, *, concurrency: int = _DOWNLOAD_CONCURRENCY) -> List[Tuple[int, int]]:
    total = max(int(total_bytes or 0), 0)
    if total <= 0:
        return [(0, 0)]
    if total < _MIN_PARALLEL_BYTES:
        return [(0, total - 1)]

    max_parts = min(max(1, concurrency), max(1, total // _MIN_PART_BYTES))
    if max_parts <= 1:
        return [(0, total - 1)]

    part_size = (total + max_parts - 1) // max_parts
    segments: List[Tuple[int, int]] = []
    start = 0
    while start < total:
        end = min(start + part_size - 1, total - 1)
        segments.append((start, end))
        start = end + 1
    return segments


class _DownloadProgress:
    def __init__(
        self,
        *,
        expected: int,
        message: str,
        progress_callback: Optional[ProgressCallback],
    ):
        self.expected = max(int(expected or 0), 0)
        self.message = message
        self.progress_callback = progress_callback
        self.downloaded = 0
        self._lock = asyncio.Lock()
        self._last_emit = 0.0
        self._since_emit = 0

    async def add(self, nbytes: int) -> None:
        if nbytes <= 0:
            return
        async with self._lock:
            self.downloaded += nbytes
            self._since_emit += nbytes
            if not self.progress_callback:
                return
            now = time.monotonic()
            done = self.expected > 0 and self.downloaded >= self.expected
            if self._last_emit and now - self._last_emit < _PROGRESS_EMIT_INTERVAL and not done:
                return
            elapsed = max(now - self._last_emit, 0.001) if self._last_emit else 1.0
            speed = self._since_emit / elapsed if self._last_emit else 0.0
            self._since_emit = 0
            self._last_emit = now
            await self.progress_callback(self.downloaded, self.expected, self.message, speed)

    async def finish(self) -> None:
        if self.progress_callback:
            await self.progress_callback(self.downloaded, self.expected or self.downloaded, "源盘下载完成", 0.0)


async def _probe_range_download(
    *,
    driver,
    source_file_id: str,
    download_url: str,
    headers_override: Optional[dict],
    expected: int,
) -> Tuple[bool, int]:
    resp = await open_upstream_get(
        driver=driver,
        file_id=source_file_id,
        user_agent="",
        initial_url=download_url,
        file_info=None,
        range_header="bytes=0-0",
        upstream_headers_override=headers_override,
    )
    try:
        if resp.status == 206:
            total = _parse_content_range_total(resp.headers.get("Content-Range", ""))
            return True, total or expected
        if resp.status == 200:
            accept_ranges = (resp.headers.get("Accept-Ranges") or "").lower()
            if "bytes" in accept_ranges:
                content_length = int(resp.headers.get("Content-Length") or 0)
                return True, content_length or expected
        return False, expected
    finally:
        resp.release()


async def _download_single_stream(
    *,
    driver,
    source_file_id: str,
    download_url: str,
    headers_override: Optional[dict],
    local_path: str,
    expected: int,
    progress: _DownloadProgress,
) -> int:
    resp = await open_upstream_get(
        driver=driver,
        file_id=source_file_id,
        user_agent="",
        initial_url=download_url,
        file_info=None,
        range_header=None,
        upstream_headers_override=headers_override,
    )
    try:
        if resp.status >= 400:
            body = await resp.text(errors="ignore")
            raise RuntimeError(f"源盘下载失败 HTTP {resp.status}: {body[:200]}")

        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as fp:
            async for chunk in resp.content.iter_chunked(1024 * 256):
                if not chunk:
                    continue
                fp.write(chunk)
                await progress.add(len(chunk))
        return progress.downloaded
    finally:
        resp.release()


async def _download_range_part(
    *,
    driver,
    source_file_id: str,
    download_url: str,
    headers_override: Optional[dict],
    start: int,
    end: int,
    fp,
    io_lock: asyncio.Lock,
    progress: _DownloadProgress,
) -> int:
    range_header = f"bytes={start}-{end}"
    expected_len = end - start + 1
    written = 0

    resp = await open_upstream_get(
        driver=driver,
        file_id=source_file_id,
        user_agent="",
        initial_url=download_url,
        file_info=None,
        range_header=range_header,
        upstream_headers_override=headers_override,
    )
    try:
        if resp.status not in (200, 206):
            body = await resp.text(errors="ignore")
            raise RuntimeError(f"源盘分片下载失败 HTTP {resp.status}: {body[:200]}")

        # 流式写盘：分片可达数 GB，整段读入内存会在大文件并发时打爆内存并导致进度长时间停在 0%
        pos = start
        async for chunk in resp.content.iter_chunked(1024 * 256):
            if not chunk:
                continue
            async with io_lock:
                fp.seek(pos)
                fp.write(chunk)
            pos += len(chunk)
            written += len(chunk)
            await progress.add(len(chunk))
    finally:
        resp.release()

    if written != expected_len:
        raise RuntimeError(
            f"分片下载不完整 bytes={start}-{end} 期望 {expected_len} 实际 {written}"
        )
    return written


async def _download_via_range_proxy(
    *,
    driver,
    source_file_id: str,
    download_url: str,
    headers_override: Optional[dict],
    local_path: str,
    expected: int,
    progress: _DownloadProgress,
    file_info=None,
) -> int:
    """与前台 proxy 下载同源：复用 range_proxy._stream_chunked_range 顺序分片拉流。"""
    if expected <= 0:
        return await _download_single_stream(
            driver=driver,
            source_file_id=source_file_id,
            download_url=download_url,
            headers_override=headers_override,
            local_path=local_path,
            expected=expected,
            progress=progress,
        )

    info = file_info
    if info is None:
        try:
            info = await driver.file_info(source_file_id)
        except Exception:
            info = None
    if info is None:
        info = build_proxy_file_info(
            source_file_id,
            file_name=f"file_{source_file_id}",
            file_size=expected,
        )

    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, "wb") as fp:
        async for chunk in _stream_chunked_range(
            driver=driver,
            file_id=source_file_id,
            user_agent="",
            initial_url=download_url,
            file_info=info,
            range_start=0,
            range_end=expected - 1,
            chunk_cap=resolve_chunk_cap(driver),
            upstream_headers_override=headers_override,
        ):
            if not chunk:
                continue
            fp.write(chunk)
            await progress.add(len(chunk))
    return progress.downloaded


async def _download_parallel_ranges(
    *,
    driver,
    source_file_id: str,
    download_url: str,
    headers_override: Optional[dict],
    local_path: str,
    expected: int,
    progress: _DownloadProgress,
) -> int:
    segments = plan_download_segments(expected)
    if len(segments) <= 1:
        return await _download_single_stream(
            driver=driver,
            source_file_id=source_file_id,
            download_url=download_url,
            headers_override=headers_override,
            local_path=local_path,
            expected=expected,
            progress=progress,
        )

    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    io_lock = asyncio.Lock()
    with open(local_path, "wb") as fp:
        if expected > 0:
            fp.truncate(expected)

        results = await asyncio.gather(*[
            _download_range_part(
                driver=driver,
                source_file_id=source_file_id,
                download_url=download_url,
                headers_override=headers_override,
                start=start,
                end=end,
                fp=fp,
                io_lock=io_lock,
                progress=progress,
            )
            for start, end in segments
        ])

    downloaded = sum(results)
    if expected and downloaded < expected:
        _log().warning(f"中继并行下载体积不足: 期望 {expected} 实际 {downloaded}")
    return downloaded


async def download_source_to_file(
    *,
    source_account_id: int,
    source_file_id: str,
    local_path: str,
    total_bytes: int,
    progress_callback: Optional[ProgressCallback] = None,
) -> int:
    driver = await get_account_driver_instance(source_account_id)
    download = await resolve_download(driver, source_file_id, "")
    if not download.download_url:
        raise RuntimeError("获取源盘下载链接失败")

    expected = int(total_bytes or download.file_size or 0)
    progress = _DownloadProgress(
        expected=expected,
        message="正在从源盘下载",
        progress_callback=progress_callback,
    )

    supports_range, probed_total = await _probe_range_download(
        driver=driver,
        source_file_id=source_file_id,
        download_url=download.download_url,
        headers_override=download.headers,
        expected=expected,
    )
    if probed_total > 0:
        expected = probed_total
        progress.expected = expected

    can_parallel = (
        supports_range
        and expected >= _MIN_PARALLEL_BYTES
        and _supports_parallel_download(driver)
    )

    try:
        if can_parallel:
            downloaded = await _download_parallel_ranges(
                driver=driver,
                source_file_id=source_file_id,
                download_url=download.download_url,
                headers_override=download.headers,
                local_path=local_path,
                expected=expected,
                progress=progress,
            )
        elif supports_range:
            downloaded = await _download_via_range_proxy(
                driver=driver,
                source_file_id=source_file_id,
                download_url=download.download_url,
                headers_override=download.headers,
                local_path=local_path,
                expected=expected,
                progress=progress,
                file_info=download.file_info,
            )
        else:
            downloaded = await _download_single_stream(
                driver=driver,
                source_file_id=source_file_id,
                download_url=download.download_url,
                headers_override=download.headers,
                local_path=local_path,
                expected=expected,
                progress=progress,
            )
    except Exception as error:
        if can_parallel or supports_range:
            _log().warning(f"中继下载失败，回退顺序分片: {error}")
            progress.downloaded = 0
            progress._since_emit = 0
            progress._last_emit = 0.0
            downloaded = await _download_via_range_proxy(
                driver=driver,
                source_file_id=source_file_id,
                download_url=download.download_url,
                headers_override=download.headers,
                local_path=local_path,
                expected=expected,
                progress=progress,
                file_info=download.file_info,
            )
        else:
            raise

    await progress.finish()
    return downloaded


async def upload_temp_to_target(
    *,
    target_account_id: int,
    target_parent_id: str,
    local_path: str,
    file_name: str,
    file_size: int,
    conflict_policy: str,
    progress_callback: Optional[ProgressCallback] = None,
) -> dict:
    driver = await get_account_driver_instance(target_account_id)

    async def upload_progress(uploaded: int, total: int, message: str):
        if not progress_callback:
            return
        await progress_callback(uploaded, total, message or "正在上传到目标盘")

    result = await driver.upload_local_file(
        local_path,
        file_name,
        target_parent_id,
        progress_callback=upload_progress,
        conflict_policy=conflict_policy,
    )
    if not result.success:
        raise RuntimeError(result.message or "上传到目标盘失败")
    return result.data or {}
