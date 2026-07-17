"""
STRM 播放入口。
"""

import secrets
import urllib.parse
from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse, Response

from config import config_manager
from core.driver_service import (
    get_account_driver_instance,
    get_effective_download_mode,
    resolve_download,
)
from core.error_handler import raise_api_error
from core.operation_wrapper import current_account_id
from core.range_proxy import (
    build_head_response,
    build_proxy_file_info_from_download,
    guess_content_type,
    serve_range_proxy,
)
from core.strm_security import (
    build_strm_play_path,
    build_strm_v2_base_path,
    decode_strm_file_key,
    verify_strm_signature,
)
from core.utils import normalize_bool


router = APIRouter()


# ---- 认证 / 配置 ----

async def _get_strm_token() -> str:
    value = await config_manager.get_async("strm_token")
    token = str(value or "").strip()
    if token:
        return token
    token = secrets.token_urlsafe(32)
    await config_manager.set_async("strm_token", token)
    return token


async def _is_strm_signature_enabled() -> bool:
    value = await config_manager.get_async("strm_signature_enabled")
    return normalize_bool(value, False)


async def _require_strm_token(request: Request) -> Optional[Response]:
    token = (request.query_params.get("token") or "").strip()
    expected = await _get_strm_token()
    if not token or token != expected:
        return Response(status_code=401, content="Unauthorized")
    return None


async def _require_strm_access(request: Request, account_id: int, file_id: str) -> Optional[Response]:
    unauthorized = await _require_strm_token(request)
    if unauthorized:
        return unauthorized

    if not await _is_strm_signature_enabled():
        return None

    signature = (request.query_params.get("sign") or "").strip()
    play_path = build_strm_play_path(account_id, file_id)
    if not signature or not verify_strm_signature(play_path, signature):
        return Response(status_code=401, content="Invalid signature")
    return None


async def _require_strm_v2_access(
    request: Request,
    account_id: int,
    file_id: str,
    token: str,
    signature: str = "",
) -> Optional[Response]:
    expected = await _get_strm_token()
    if not token or token != expected:
        return Response(status_code=401, content="Unauthorized")

    if not await _is_strm_signature_enabled():
        return None

    play_path = build_strm_v2_base_path(account_id, file_id, token)
    if not signature or not verify_strm_signature(play_path, signature):
        return Response(status_code=401, content="Invalid signature")
    return None


# ---- ISO 播放策略 ----

async def _get_iso_play_mode() -> str:
    value = str(await config_manager.get_async("strm_iso_play_mode") or "follow").strip().lower()
    return value if value in {"proxy", "follow"} else "follow"


def _file_extension(file_name: str) -> str:
    name = str(file_name or "").strip()
    if "." not in name:
        return ""
    return name.rsplit(".", 1)[-1].strip().lower()


def _looks_like_iso_filename(file_name: str) -> bool:
    return _file_extension(file_name) == "iso"


def _download_looks_like_iso(download) -> bool:
    """与网盘原始文件名一致地识别 ISO；避免仅 file_name 无后缀时误走 302 直链。"""
    if _looks_like_iso_filename(getattr(download, "file_name", "") or ""):
        return True
    fi = getattr(download, "file_info", None)
    if fi is None:
        return False
    if _looks_like_iso_filename(getattr(fi, "name", "") or ""):
        return True
    path = str(getattr(fi, "path", "") or "").strip()
    if path:
        tail = path.rstrip("/").rsplit("/", 1)[-1]
        if _looks_like_iso_filename(tail):
            return True
    return False


# ---- 错误兜底 ----

def _handle_play_error(e: Exception) -> Response:
    if hasattr(e, "error_type"):
        raise e
    import traceback
    from core.log_manager import get_writer, LogModule
    try:
        get_writer(LogModule.WEB).error(f"STRM播放失败: {e}\n{traceback.format_exc()}")
    except Exception:
        print(f"STRM播放失败: {e}")
    raise_api_error(f"播放失败: {str(e)}", "play", 500)


# ---- 路由 ----

@router.api_route("/play/{account_id}/{file_id:path}", methods=["GET", "HEAD"])
async def play_by_file_id(account_id: int, file_id: str, request: Request):
    unauthorized = await _require_strm_access(request, account_id, str(file_id))
    if unauthorized:
        return unauthorized
    return await _play_resolved_file(account_id, file_id, request)


@router.api_route("/v2/play/{account_id}/{file_key}/t/{token}", methods=["GET", "HEAD"])
@router.api_route("/v2/play/{account_id}/{file_key}/t/{token}/s/{signature}", methods=["GET", "HEAD"])
async def play_v2_by_file_key(
    account_id: int,
    file_key: str,
    token: str,
    request: Request,
    signature: str = "",
):
    try:
        file_id = decode_strm_file_key(file_key)
    except Exception:
        return Response(status_code=404, content="Not found")

    unauthorized = await _require_strm_v2_access(request, account_id, file_id, token, signature)
    if unauthorized:
        return unauthorized
    return await _play_resolved_file(account_id, file_id, request)


async def _play_resolved_file(account_id: int, file_id: str, request: Request):
    if not str(file_id or "").strip():
        return Response(status_code=404, content="Not found")

    user_agent = request.headers.get("User-Agent", "")
    driver = await get_account_driver_instance(account_id)
    current_account_id.set(str(account_id))

    try:
        download = await resolve_download(driver, file_id, user_agent)
    except Exception as e:
        return _handle_play_error(e)
    if not download.download_url:
        raise_api_error("获取下载链接失败", "download", 404)
    if download.file_info and download.file_info.is_dir:
        return Response(status_code=400, content="Cannot play a directory")

    download_mode = get_effective_download_mode(driver, download)

    if download_mode == "redirect":
        is_iso = _download_looks_like_iso(download)
        iso_mode = await _get_iso_play_mode()
        if not (is_iso and iso_mode == "proxy"):
            if request.method == "HEAD":
                return Response(status_code=302, headers={"Location": download.download_url})
            return RedirectResponse(url=download.download_url, status_code=302)

        file_info, download_url, _size = build_proxy_file_info_from_download(file_id, download)
        return await _serve_via_proxy(
            driver=driver,
            file_id=str(file_id),
            request=request,
            file_info=file_info,
            download=download,
            download_url=download_url,
        )

    is_iso_proxy_branch = _download_looks_like_iso(download)
    iso_mode = await _get_iso_play_mode()
    if is_iso_proxy_branch and iso_mode == "follow":
        if request.method == "HEAD":
            return Response(status_code=302, headers={"Location": download.download_url})
        return RedirectResponse(url=download.download_url, status_code=302)

    file_info, download_url, _size = build_proxy_file_info_from_download(file_id, download)

    return await _serve_via_proxy(
        driver=driver,
        file_id=str(file_id),
        request=request,
        file_info=file_info,
        download=download,
        download_url=download_url,
    )


async def _serve_via_proxy(
    *,
    driver,
    file_id: str,
    request: Request,
    file_info,
    download,
    download_url: str,
):
    """统一通过 serve_range_proxy 走分片 Range 代理。"""
    if not download_url:
        raise_api_error("获取下载链接失败", "download", 404)

    # build_proxy_file_info_from_download 在 size 不可知时可能返回 None
    if file_info is None:
        from core.range_proxy import build_proxy_file_info as _build_fi
        file_info = _build_fi(
            file_id,
            file_name=download.file_name or f"file_{file_id}",
            file_size=int(download.file_size or 0),
            template=download.file_info,
        )

    download_name = (
        getattr(file_info, "name", None) or download.file_name or f"file_{file_id}"
    ).strip() or f"file_{file_id}"
    try:
        encoded_filename = urllib.parse.quote(download_name, safe="")
    except Exception:
        encoded_filename = f"file_{file_id}"

    # 视频/音频默认 inline；其它 STRM 命中类型一般也是流媒体，但保留通用兜底为 inline
    content_type = guess_content_type(download_name)
    disposition = "inline" if content_type.startswith(("video/", "audio/")) else "inline"
    content_disposition = f"{disposition}; filename*=UTF-8''{encoded_filename}"

    # HEAD：直接由 build_head_response 返回元数据，连一次上游探测都不需要
    if request.method == "HEAD" and int(getattr(file_info, "size", 0) or 0) > 0:
        return build_head_response(
            file_info,
            content_type=content_type,
            content_disposition=content_disposition,
        )

    try:
        return await serve_range_proxy(
            driver=driver,
            file_id=file_id,
            file_info=file_info,
            request=request,
            initial_url=download_url,
            content_disposition=content_disposition,
            upstream_headers_override=download.headers,
        )
    except Exception as e:
        return _handle_play_error(e)
