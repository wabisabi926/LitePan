from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse, Response, StreamingResponse
from pydantic import BaseModel
from starlette.requests import ClientDisconnect
from typing import Any, Dict, Optional
from urllib.parse import unquote, urlencode, urljoin, urlparse
import aiohttp
import asyncio
import json
import os
import re

from api.deps import require_admin_auth
from api.responses import success_response as _success_response, error_response as _error_response
from core.range_proxy import get_range_proxy_session
from core.emby_proxy_server import emby_proxy_server_manager
from core.driver_service import get_account_driver_instance
from core.strm_security import decode_strm_file_key
from database.db import db


admin_router = APIRouter()
proxy_router = APIRouter()

HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
}


class EmbyProxyCreate(BaseModel):
    name: str
    emby_url: str
    api_key: str
    proxy_port: int


class EmbyProxyUpdate(BaseModel):
    name: Optional[str] = None
    emby_url: Optional[str] = None
    api_key: Optional[str] = None
    proxy_port: Optional[int] = None
    status: Optional[str] = None


def _normalize_emby_url(value: str) -> str:
    url = str(value or "").strip().rstrip("/")
    if not re.match(r"^https?://\S+$", url):
        raise ValueError("Emby地址格式不正确，示例：http://192.168.1.10:8096")
    return url


def _validate_proxy_port(value: int) -> int:
    port = int(value)
    if port < 1 or port > 65535:
        raise ValueError("反代端口必须是 1-65535")
    return port


def _get_query_value(request: Request, key: str) -> str:
    key_lower = key.lower()
    for current_key, value in request.query_params.multi_items():
        if current_key.lower() == key_lower:
            return str(value or "")
    return ""


def _strip_media_source_prefix(value: str) -> str:
    return str(value or "").replace("mediasource_", "", 1)


def _extract_video_item_id(path: str) -> str:
    match = re.search(r"(?i)(?:^|/)Videos/([^/]+)/(?:stream|original)(?:\.\w+)?$", path or "")
    return match.group(1) if match else ""


def _is_video_stream_path(path: str) -> bool:
    return bool(re.match(r"(?i)^(/?emby)?/?Videos/[^/]+/(stream|original)(\.\w+)?$", path or ""))


def _is_playback_info_path(path: str) -> bool:
    return bool(re.match(r"(?i)^(/?emby)?/?Items/[^/]+/PlaybackInfo$", path or ""))


def _is_base_html_player_path(path: str) -> bool:
    return bool(re.match(r"(?i)^/?web/modules/htmlvideoplayer/basehtmlplayer\.js$", path or ""))


def _public_proxy_base(request: Request, proxy_id: int, proxy_port: int) -> str:
    forwarded_proto = (request.headers.get("x-forwarded-proto") or "").strip()
    forwarded_host = (request.headers.get("x-forwarded-host") or "").strip()
    host = forwarded_host or (request.headers.get("host") or "").strip()
    scheme = forwarded_proto or request.url.scheme
    if host:
        parsed_host = urlparse(f"//{host}")
        hostname = parsed_host.hostname or host.split(":")[0]
        if proxy_port:
            if ":" in hostname and not hostname.startswith("["):
                host = f"[{hostname}]:{int(proxy_port)}"
            else:
                host = f"{hostname}:{int(proxy_port)}"
        base = f"{scheme}://{host}".rstrip("/")
        if str(request.url.path or "").startswith(f"/emby-proxy/{proxy_id}"):
            return f"{base}/emby-proxy/{proxy_id}"
        return base
    base = str(request.base_url).rstrip("/")
    if str(request.url.path or "").startswith(f"/emby-proxy/{proxy_id}"):
        return f"{base}/emby-proxy/{proxy_id}"
    return base


def _target_url(config: Dict[str, Any], full_path: str, query_string: bytes = b"") -> str:
    base = str(config["emby_url"]).rstrip("/") + "/"
    path = str(full_path or "").lstrip("/")
    target = urljoin(base, path)
    if query_string:
        target = f"{target}?{query_string.decode('utf-8', errors='ignore')}"
    return target


def _proxied_video_url(request: Request, config: Dict[str, Any], proxy_id: int, item_id: str, media_source_id: str) -> str:
    query = dict(request.query_params)
    query["MediaSourceId"] = media_source_id
    query.setdefault("static", "true")
    if "api_key" not in {key.lower(): value for key, value in query.items()}:
        query["api_key"] = str(config.get("api_key") or "")
    base = _public_proxy_base(request, proxy_id, int(config.get("proxy_port") or 0))
    return f"{base}/Videos/{item_id}/stream?{urlencode(query)}"


def _proxied_video_path(request: Request, config: Dict[str, Any], proxy_id: int, item_id: str, media_source_id: str) -> str:
    proxied_url = _proxied_video_url(request, config, proxy_id, item_id, media_source_id)
    parsed = urlparse(proxied_url)
    path = parsed.path or f"/Videos/{item_id}/stream"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    return path


def _normalize_media_url(candidate: str, request: Request, config: Dict[str, Any], proxy_id: int) -> str:
    value = str(candidate or "").strip()
    if not value:
        return ""
    if value.startswith("http://") or value.startswith("https://"):
        return value
    if value.startswith("/"):
        base = _public_proxy_base(request, proxy_id, int(config.get("proxy_port") or 0))
        return base.rstrip("/") + value
    return value


def _litepan_strm_path(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.startswith("http://") or text.startswith("https://"):
        return urlparse(text).path
    return text.split("?", 1)[0]


def _is_litepan_strm_url(value: str) -> bool:
    path = _litepan_strm_path(value).lower()
    if not path:
        return False
    return path.startswith("/api/strm/play/") or path.startswith("/api/strm/v2/play/")


def _parse_litepan_strm_url(value: str) -> Optional[Dict[str, Any]]:
    path = _litepan_strm_path(value)
    if not path:
        return None
    # v2 链接：/api/strm/v2/play/{account_id}/{file_key}/t/{token}[/s/{signature}]
    v2_match = re.match(r"^/api/strm/v2/play/(\d+)/([^/]+)/t/", path)
    if v2_match:
        try:
            file_id = decode_strm_file_key(v2_match.group(2))
        except Exception:
            return None
        if not file_id:
            return None
        return {
            "account_id": int(v2_match.group(1)),
            "file_id": file_id,
        }
    match = re.match(r"^/api/strm/play/(\d+)/(.*)$", path)
    if not match:
        return None
    return {
        "account_id": int(match.group(1)),
        "file_id": unquote(match.group(2) or ""),
    }


async def _infer_litepan_media_info(litepan_url: str) -> Dict[str, Any]:
    parsed = _parse_litepan_strm_url(litepan_url)
    if not parsed:
        return {}
    try:
        driver = await get_account_driver_instance(int(parsed["account_id"]))
        if not hasattr(driver, "file_info"):
            return {}
        file_info = await driver.file_info(str(parsed["file_id"]))
        if not file_info:
            return {}
        name = str(getattr(file_info, "name", "") or "").strip()
        ext = os.path.splitext(name)[1].lstrip(".").lower()
        result = {
            "file_name": name,
            "size": int(getattr(file_info, "size", 0) or 0),
        }
        if ext:
            result["container"] = ext
        return result
    except Exception:
        return {}


def _extract_litepan_strm_candidate(media_source: Dict[str, Any], request: Request, config: Dict[str, Any], proxy_id: int) -> str:
    for key in ("Path", "DirectStreamUrl", "DirectStreamURL", "TranscodingUrl"):
        value = _normalize_media_url(str(media_source.get(key) or ""), request, config, proxy_id)
        if _is_litepan_strm_url(value):
            return value
    return ""


def _find_media_source_by_id(item: Optional[Dict[str, Any]], media_source_id: str) -> Optional[Dict[str, Any]]:
    if not item:
        return None
    target_id = _strip_media_source_prefix(media_source_id)
    for media_source in item.get("MediaSources") or []:
        current_id = _strip_media_source_prefix(str(media_source.get("Id") or media_source.get("ID") or ""))
        if not current_id or current_id == target_id:
            return media_source
    return None


async def _fetch_emby_item(config: Dict[str, Any], item_id: str) -> Optional[Dict[str, Any]]:
    item_id = _strip_media_source_prefix(item_id)
    if not item_id:
        return None
    params = {
        "Ids": item_id,
        "Limit": "1",
        "Fields": "Path,MediaSources",
        "Recursive": "true",
        "api_key": str(config.get("api_key") or ""),
    }
    base = str(config["emby_url"]).rstrip("/")
    # 不同版本 / 不同反代配置的 Emby 对内部 Items 接口的可用前缀不一致：
    # 部分版本同时接受 `/Items` 与 `/emby/Items`，部分版本只接受其中一种。
    # 这里两种都试，先匹配显式 `/emby/` 前缀；都失败时返回 None，让上层兜底。
    base_lower = base.lower()
    if base_lower.endswith("/emby"):
        candidates = [f"{base}/Items"]
    else:
        candidates = [f"{base}/emby/Items", f"{base}/Items"]
    query = urlencode(params)
    session = get_range_proxy_session()
    for endpoint in candidates:
        url = f"{endpoint}?{query}"
        try:
            async with session.get(url) as resp:
                if resp.status >= 400:
                    continue
                payload = await resp.json(content_type=None)
        except Exception:
            continue
        items = payload.get("Items") or []
        if items:
            return items[0]
    return None


async def _redirect_strm_stream(proxy_id: int, config: Dict[str, Any], full_path: str, request: Request):
    if request.method.upper() == "HEAD":
        return await _proxy_request(config, full_path, request)

    media_source_id = _get_query_value(request, "mediasourceid")
    if not media_source_id:
        return await _proxy_request(config, full_path, request)

    item_id = _extract_video_item_id(full_path)
    item = await _fetch_emby_item(config, item_id) or await _fetch_emby_item(config, media_source_id)
    if not item:
        return await _proxy_request(config, full_path, request)

    media_source_id_without_prefix = _strip_media_source_prefix(media_source_id)
    for media_source in item.get("MediaSources") or []:
        current_id = _strip_media_source_prefix(str(media_source.get("Id") or media_source.get("ID") or ""))
        if current_id and current_id != media_source_id_without_prefix:
            continue

        redirect_url = _extract_litepan_strm_candidate(media_source, request, config, proxy_id)
        if redirect_url:
            return RedirectResponse(url=redirect_url, status_code=302)

    item_path = _normalize_media_url(str(item.get("Path") or ""), request, config, proxy_id)
    if _is_litepan_strm_url(item_path):
        return RedirectResponse(url=item_path, status_code=302)

    return await _proxy_request(config, full_path, request)


async def _modify_playback_info(proxy_id: int, config: Dict[str, Any], full_path: str, request: Request):
    upstream = await _request_upstream(config, full_path, request, force_identity=True)
    try:
        body = await upstream.read()
        if upstream.status >= 400:
            return Response(content=body, status_code=upstream.status, headers=_response_headers(upstream))

        payload = json.loads(body.decode("utf-8"))
        item_id_match = re.search(r"(?i)(?:^|/)Items/([^/]+)/PlaybackInfo$", full_path or "")
        item_id = item_id_match.group(1) if item_id_match else ""
        item = await _fetch_emby_item(config, item_id)
        changed = False

        for media_source in payload.get("MediaSources") or []:
            media_source_id = str(media_source.get("Id") or media_source.get("ID") or "")
            if not media_source_id:
                continue
            item_media_source = _find_media_source_by_id(item, media_source_id)
            litepan_url = (
                _extract_litepan_strm_candidate(media_source, request, config, proxy_id)
                or _extract_litepan_strm_candidate(item_media_source or {}, request, config, proxy_id)
            )
            item_path = _normalize_media_url(str((item or {}).get("Path") or ""), request, config, proxy_id)
            if not litepan_url and _is_litepan_strm_url(item_path):
                litepan_url = item_path
            if not litepan_url:
                continue

            direct_stream_path = _proxied_video_path(
                request,
                config,
                proxy_id,
                item_id or _strip_media_source_prefix(media_source_id),
                media_source_id,
            )
            media_info = await _infer_litepan_media_info(litepan_url)
            media_source["SupportsDirectPlay"] = True
            media_source["SupportsDirectStream"] = True
            media_source["SupportsTranscoding"] = False
            media_source["Protocol"] = "Http"
            media_source["DirectStreamUrl"] = direct_stream_path
            for key in (
                "TranscodingUrl",
                "TranscodingSubProtocol",
                "TranscodingContainer",
                "TranscodingLiveStartIndex",
                "TrancodeLiveStartIndex",
            ):
                media_source.pop(key, None)
            if media_info.get("container"):
                media_source["Container"] = media_info["container"]
            if media_info.get("size"):
                media_source["Size"] = media_info["size"]
            if media_info.get("file_name"):
                media_source["Name"] = media_info["file_name"]
            changed = True

        headers = _response_headers(upstream)
        if changed:
            content = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json; charset=utf-8"
            headers["Content-Length"] = str(len(content))
            headers.pop("Content-Encoding", None)
            return Response(content=content, status_code=upstream.status, headers=headers)
        return Response(content=body, status_code=upstream.status, headers=headers)
    finally:
        upstream.close()


async def _modify_base_html_player(config: Dict[str, Any], full_path: str, request: Request):
    upstream = await _request_upstream(config, full_path, request, force_identity=True)
    try:
        body = await upstream.read()
        cross_origin_guard = (
            b";try{(function(){var s=Element.prototype.setAttribute;"
            b"Element.prototype.setAttribute=function(n,v){"
            b"if(this&&this.tagName&&/^(VIDEO|AUDIO)$/i.test(this.tagName)&&String(n).toLowerCase()==='crossorigin')return;"
            b"return s.call(this,n,v)};"
            b"try{Object.defineProperty(HTMLMediaElement.prototype,'crossOrigin',{get:function(){return null},set:function(){return null},configurable:true})}catch(e){}"
            b"})()}catch(e){};"
        )
        body = body.replace(
            b'mediaSource.IsRemote&&"DirectPlay"===playMethod?null:"anonymous"',
            b"null",
        )
        body = re.sub(
            rb'mediaSource\.IsRemote\s*&&\s*(?:"DirectPlay"\s*===\s*playMethod|playMethod\s*===\s*"DirectPlay")\s*\?\s*null\s*:\s*"anonymous"',
            b"null",
            body,
        )
        if b"HTMLMediaElement.prototype,'crossOrigin'" not in body:
            body = cross_origin_guard + body
        headers = _response_headers(upstream)
        headers["Content-Length"] = str(len(body))
        headers["Cache-Control"] = "no-store"
        headers.pop("Content-Encoding", None)
        return Response(content=body, status_code=upstream.status, headers=headers)
    finally:
        upstream.close()


async def _request_upstream(config: Dict[str, Any], full_path: str, request: Request, force_identity: bool = False) -> aiohttp.ClientResponse:
    session = get_range_proxy_session()
    body = None
    if request.method.upper() in {"POST", "PUT", "PATCH"}:
        body = await request.body()
    headers = {
        key: value
        for key, value in request.headers.items()
        if key.lower() not in HOP_BY_HOP_HEADERS
    }
    if force_identity:
        headers["Accept-Encoding"] = "identity"
    target = _target_url(config, full_path, request.scope.get("query_string", b""))
    return await session.request(
        request.method,
        target,
        headers=headers,
        data=body if body else None,
        allow_redirects=False,
        auto_decompress=False,
    )


def _response_headers(resp: aiohttp.ClientResponse) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    for key, value in resp.headers.items():
        if key.lower() not in HOP_BY_HOP_HEADERS:
            headers[key] = value
    return headers


def _rewrite_location(config: Dict[str, Any], request: Request, proxy_id: int, location: str) -> str:
    value = str(location or "")
    emby_url = str(config.get("emby_url") or "").rstrip("/")
    if value.startswith(emby_url):
        base = _public_proxy_base(request, proxy_id, int(config.get("proxy_port") or 0))
        return base.rstrip("/") + value[len(emby_url):]
    return value


async def _proxy_request(config: Dict[str, Any], full_path: str, request: Request):
    try:
        upstream = await _request_upstream(config, full_path, request)
    except ClientDisconnect:
        return Response(status_code=499, content="")
    headers = _response_headers(upstream)
    if "Location" in headers:
        headers["Location"] = _rewrite_location(config, request, int(config["id"]), headers["Location"])

    async def stream_content():
        try:
            async for chunk in upstream.content.iter_chunked(1024 * 128):
                yield chunk
        except (asyncio.CancelledError, ConnectionResetError, BrokenPipeError):
            return
        finally:
            upstream.close()

    return StreamingResponse(
        stream_content(),
        status_code=upstream.status,
        headers=headers,
        media_type=headers.get("Content-Type"),
    )


def _public_config(config: Dict[str, Any], request: Optional[Request] = None) -> Dict[str, Any]:
    data = dict(config)
    if request is not None:
        data["proxy_url"] = _public_proxy_base(request, int(data["id"]), int(data.get("proxy_port") or 0))
    return data


async def _get_enabled_config(proxy_id: int) -> Optional[Dict[str, Any]]:
    config = await db.get_emby_proxy_config(proxy_id)
    if not config or str(config.get("status") or "running") != "running":
        return None
    return config


@admin_router.get("/emby-proxies")
async def list_emby_proxies(request: Request, session_data: dict = Depends(require_admin_auth)):
    configs = await db.get_emby_proxy_configs()
    return _success_response(
        data=[_public_config(config, request) for config in configs],
        message="获取Emby反代列表成功",
    )


@admin_router.post("/emby-proxies")
async def create_emby_proxy(payload: EmbyProxyCreate, request: Request, session_data: dict = Depends(require_admin_auth)):
    try:
        name = str(payload.name or "").strip()
        if not name:
            return _error_response(message="反代名称不能为空")
        emby_url = _normalize_emby_url(payload.emby_url)
        api_key = str(payload.api_key or "").strip()
        if not api_key:
            return _error_response(message="Emby API Key不能为空")
        proxy_port = _validate_proxy_port(payload.proxy_port)
        config_id = await db.create_emby_proxy_config(name, emby_url, api_key, proxy_port)
        await emby_proxy_server_manager.sync_proxy(config_id)
        config = await db.get_emby_proxy_config(config_id)
        return _success_response(data=_public_config(config, request), message="创建Emby反代成功")
    except Exception as e:
        return _error_response(message=f"创建Emby反代失败: {str(e)}")


@admin_router.put("/emby-proxies/{proxy_id}")
async def update_emby_proxy(proxy_id: int, payload: EmbyProxyUpdate, request: Request, session_data: dict = Depends(require_admin_auth)):
    try:
        config = await db.get_emby_proxy_config(proxy_id)
        if not config:
            return _error_response(message="Emby反代不存在")
        updates: Dict[str, Any] = {}
        if payload.name is not None:
            name = str(payload.name or "").strip()
            if not name:
                return _error_response(message="反代名称不能为空")
            updates["name"] = name
        if payload.emby_url is not None:
            updates["emby_url"] = _normalize_emby_url(payload.emby_url)
        if payload.api_key is not None:
            api_key = str(payload.api_key or "").strip()
            if not api_key:
                return _error_response(message="Emby API Key不能为空")
            updates["api_key"] = api_key
        if payload.proxy_port is not None:
            updates["proxy_port"] = _validate_proxy_port(payload.proxy_port)
        if payload.status is not None:
            status = str(payload.status or "").strip()
            if status not in {"running", "paused"}:
                return _error_response(message="反代状态不支持")
            updates["status"] = status
        await db.update_emby_proxy_config(proxy_id, **updates)
        await emby_proxy_server_manager.sync_proxy(proxy_id)
        config = await db.get_emby_proxy_config(proxy_id)
        return _success_response(data=_public_config(config, request), message="更新Emby反代成功")
    except Exception as e:
        return _error_response(message=f"更新Emby反代失败: {str(e)}")


@admin_router.delete("/emby-proxies/{proxy_id}")
async def delete_emby_proxy(proxy_id: int, session_data: dict = Depends(require_admin_auth)):
    await emby_proxy_server_manager.delete_proxy(proxy_id)
    deleted = await db.delete_emby_proxy_config(proxy_id)
    if not deleted:
        return _error_response(message="Emby反代不存在")
    return _success_response(message="删除Emby反代成功")


@admin_router.post("/emby-proxies/{proxy_id}/toggle")
async def toggle_emby_proxy(proxy_id: int, request: Request, session_data: dict = Depends(require_admin_auth)):
    config = await db.get_emby_proxy_config(proxy_id)
    if not config:
        return _error_response(message="Emby反代不存在")
    status = "paused" if str(config.get("status") or "running") == "running" else "running"
    await db.update_emby_proxy_config(proxy_id, status=status)
    await emby_proxy_server_manager.sync_proxy(proxy_id)
    config = await db.get_emby_proxy_config(proxy_id)
    return _success_response(data=_public_config(config, request), message="Emby反代状态已切换")


@admin_router.post("/emby-proxies/{proxy_id}/test")
async def test_emby_proxy(proxy_id: int, session_data: dict = Depends(require_admin_auth)):
    config = await db.get_emby_proxy_config(proxy_id)
    if not config:
        return _error_response(message="Emby反代不存在")
    url = str(config["emby_url"]).rstrip("/") + "/System/Info?" + urlencode({"api_key": str(config.get("api_key") or "")})
    try:
        session = get_range_proxy_session()
        async with session.get(url) as resp:
            if resp.status >= 400:
                message = f"Emby返回状态码 {resp.status}"
                await db.update_emby_proxy_config(proxy_id, last_error=message)
                return _error_response(message=message)
            await db.update_emby_proxy_config(proxy_id, last_error=None)
            return _success_response(message="Emby连接测试通过")
    except Exception as e:
        message = f"Emby连接测试失败: {str(e)}"
        await db.update_emby_proxy_config(proxy_id, last_error=message)
        return _error_response(message=message)


@proxy_router.api_route("/{proxy_id}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
@proxy_router.api_route("/{proxy_id}/{full_path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
async def emby_reverse_proxy(proxy_id: int, request: Request, full_path: str = ""):
    return await handle_emby_proxy_request(proxy_id, request, full_path)


async def handle_emby_proxy_request(proxy_id: int, request: Request, full_path: str = ""):
    config = await _get_enabled_config(proxy_id)
    if not config:
        return Response(status_code=404, content="Emby proxy not found")

    if _is_video_stream_path(full_path):
        return await _redirect_strm_stream(proxy_id, config, full_path, request)
    if _is_playback_info_path(full_path) and request.method.upper() in {"GET", "POST"}:
        return await _modify_playback_info(proxy_id, config, full_path, request)
    if _is_base_html_player_path(full_path) and request.method.upper() == "GET":
        return await _modify_base_html_player(config, full_path, request)
    return await _proxy_request(config, full_path, request)
