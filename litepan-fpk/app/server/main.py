"""LitePan 主入口：装配 FastAPI、路由、WebDAV 与静态资源。"""

import os
import ipaddress
from fastapi import FastAPI, Request
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

from pathlib import Path

from config import APP_NAME, APP_VERSION
from core.log_manager import get_writer, LogModule
from core.lifespan import lifespan, silence_uvicorn_invalid_http_warnings
from core.security import get_allowed_cors_origins


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
        from urllib.parse import urlparse

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


async def _auto_capture_strm_base_url(request: Request):
    auto_capture_enabled = os.getenv("LITEPAN_AUTO_CAPTURE_STRM_BASE_URL", "false").lower() in ("1", "true", "yes", "on")
    if not auto_capture_enabled:
        return

    candidate_base = _extract_request_base_url(request)
    if not candidate_base or _is_loopback_base_url(candidate_base):
        return

    from config import config_manager
    configured = str(await config_manager.get_async("strm_base_url") or "").strip().rstrip("/")
    if configured and not _is_loopback_base_url(configured):
        return
    if configured == candidate_base:
        return
    await config_manager.set_async("strm_base_url", candidate_base)


app = FastAPI(
    title=APP_NAME,
    description="轻量级网盘挂载系统 - 扁平化架构，真Vue3前端",
    version=APP_VERSION,
    lifespan=lifespan
)

from core.error_handler import setup_error_handlers, set_debug_mode
setup_error_handlers(app)
set_debug_mode(debug=os.getenv("LITEPAN_DEBUG", "false").lower() in ("true", "1", "yes"))

app.add_middleware(
    CORSMiddleware,
    allow_origins=get_allowed_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def add_charset_middleware(request: Request, call_next):
    try:
        await _auto_capture_strm_base_url(request)
    except Exception as e:
        try:
            get_writer(LogModule.SYSTEM).debug(f"自动捕获 STRM Base URL 失败: {e}")
        except Exception:
            pass
    response = await call_next(request)
    if response.headers.get("content-type") == "application/json":
        response.headers["content-type"] = "application/json; charset=utf-8"
    return response

static_dir = Path("web/static")
if static_dir.exists():
    app.mount("/assets", StaticFiles(directory=str(static_dir / "assets")), name="assets")
else:
    try:
        system_log = get_writer(LogModule.SYSTEM)
        system_log.warning(f"静态文件目录不存在: {static_dir}")
        system_log.warning("请运行 'npm run build' 构建前端")
    except RuntimeError:
        print(f"WARN 静态文件目录不存在: {static_dir}")
        print("请运行 'npm run build' 构建前端")


def build_frontend_entry_response(index_file: Path) -> FileResponse:
    response = FileResponse(str(index_file))
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

from api.admin import router as admin_router
from api.files import router as files_router
from api.auth import router as auth_router
from api.logs import router as logs_router
from api.oauth import router as oauth_router
from api.cache_retention import router as cache_retention_router
from api.public import router as public_router
from api.strm import router as strm_router
from api.strm_admin import router as strm_admin_router
from api.emby_proxy import admin_router as emby_proxy_admin_router, proxy_router as emby_proxy_router
from api.plugins import router as plugins_router
from api.notifications import router as notifications_router
from api.media_organize import router as media_organize_router
from api.local_fs import router as local_fs_router, admin_router as local_fs_admin_router
from api.cross_transfer import router as cross_transfer_router

app.include_router(admin_router, prefix="/api/admin", tags=["管理"])
app.include_router(notifications_router, prefix="/api/admin", tags=["通知"])
app.include_router(public_router, prefix="/api/public", tags=["公开"])
app.include_router(files_router, prefix="/api/files", tags=["文件"])
app.include_router(auth_router, prefix="/api/auth", tags=["认证"])
app.include_router(logs_router)
app.include_router(oauth_router, tags=["OAuth"])
app.include_router(cache_retention_router)
app.include_router(strm_router, prefix="/api/strm", tags=["STRM"])
app.include_router(strm_admin_router, prefix="/api/admin/strm", tags=["STRM管理"])
app.include_router(emby_proxy_admin_router, prefix="/api/admin/strm", tags=["Emby反代管理"])
app.include_router(media_organize_router, prefix="/api/admin/media-organize", tags=["媒体整理"])
app.include_router(emby_proxy_router, prefix="/emby-proxy", tags=["Emby反代"])
app.include_router(plugins_router)
app.include_router(local_fs_router)
app.include_router(local_fs_admin_router)
app.include_router(cross_transfer_router)

@app.get("/")
async def serve_frontend():
    index_file = static_dir / "index.html"
    if index_file.exists():
        return build_frontend_entry_response(index_file)
    else:
        return {"error": "前端未构建", "message": "请运行 npm run build"}

@app.get("/admin")
async def serve_admin():
    index_file = static_dir / "index.html"
    if index_file.exists():
        return build_frontend_entry_response(index_file)
    else:
        return {"error": "前端未构建", "message": "请运行 npm run build"}

@app.get("/login")
async def serve_login():
    index_file = static_dir / "index.html"
    if index_file.exists():
        return build_frontend_entry_response(index_file)
    else:
        return {"error": "前端未构建", "message": "请运行 npm run build"}

@app.get("/api")
async def api_info():
    return {
        "name": APP_NAME,
        "version": APP_VERSION,
        "description": "轻量级网盘挂载系统",
        "architecture": "扁平化架构 + 真Vue3",
        "api_docs": "/docs",
        "status": "运行中"
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy", "version": APP_VERSION}


@app.api_route("/dav", methods=["GET", "HEAD", "PUT", "DELETE", "MKCOL", "MOVE", "COPY", "PROPFIND", "OPTIONS"])
async def webdav_root_handler(request: Request):
    try:
        from webdav import get_webdav_server
        webdav_server = await get_webdav_server()
        if webdav_server is None:
            return Response(
                status_code=503,
                content="WebDAV service is disabled",
                headers={'Content-Type': 'text/plain'}
            )
        return await webdav_server.handle_request(request, "")
    except Exception as e:
        print(f"WebDAV根路径处理异常: {e}")
        import traceback
        traceback.print_exc()
        return Response(
            status_code=500,
            content=f"WebDAV Error: {str(e)}",
            headers={'Content-Type': 'text/plain'}
        )

@app.api_route("/dav/{path:path}", methods=["GET", "HEAD", "PUT", "DELETE", "MKCOL", "MOVE", "COPY", "PROPFIND", "OPTIONS"])
async def webdav_handler(request: Request, path: str = ""):
    try:
        from webdav import get_webdav_server
        webdav_server = await get_webdav_server()
        if webdav_server is None:
            return Response(
                status_code=503,
                content="WebDAV service is disabled",
                headers={'Content-Type': 'text/plain'}
            )
        return await webdav_server.handle_request(request, path)
    except Exception as e:
        print(f"WebDAV处理异常: {e}")
        import traceback
        traceback.print_exc()
        return Response(
            status_code=500,
            content=f"WebDAV Error: {str(e)}",
            headers={'Content-Type': 'text/plain'}
        )

@app.get("/{full_path:path}")
async def serve_frontend(full_path: str):
    static_file = static_dir / full_path
    if static_file.exists() and static_file.is_file():
        return FileResponse(str(static_file))

    index_file = static_dir / "index.html"
    if index_file.exists():
        return build_frontend_entry_response(index_file)
    else:
        return JSONResponse({
            "message": f"{APP_NAME} 后端服务运行正常",
            "status": "前端未构建",
            "instruction": "请运行 'npm run build' 构建前端",
            "api_docs": "/docs",
            "available_apis": [
                "/api/admin - 管理接口",
                "/api/files - 文件操作",
                "/api/auth - 认证接口"
            ]
        })


def _bind_listen_socket_dual_stack_ipv6_wildcard(port: int, backlog: int):

    import socket as _sock

    lsock = _sock.socket(_sock.AF_INET6, _sock.SOCK_STREAM)
    lsock.setsockopt(_sock.SOL_SOCKET, _sock.SO_REUSEADDR, 1)
    try:
        lsock.setsockopt(_sock.IPPROTO_IPV6, _sock.IPV6_V6ONLY, 0)
    except OSError:
        pass
    lsock.bind(("::", port))
    lsock.listen(backlog)
    lsock.setblocking(False)
    return lsock


_UVICORN_LISTEN_BACKLOG = 2048


if __name__ == "__main__":
    import sys
    import uvicorn

    silence_uvicorn_invalid_http_warnings()

    os.environ['UVICORN_RELOAD'] = 'false'

    _gs_raw = os.getenv("LITEPAN_GRACEFUL_SHUTDOWN", "3").strip()
    try:
        _graceful = max(1, int(_gs_raw))
    except ValueError:
        _graceful = 30

    _web_host_override = os.getenv("LITEPAN_WEB_HOST", "").strip()
    _port = int(os.getenv("LITEPAN_PORT", "5211"))

    import time
    current_time = time.strftime('%H:%M:%S')

    try:
        if _web_host_override == "::":
            from uvicorn import Config, Server

            try:
                _lsock = _bind_listen_socket_dual_stack_ipv6_wildcard(_port, _UVICORN_LISTEN_BACKLOG)
            except OSError as e:
                print(
                    "ERROR [::] 监听失败: "
                    + str(e)
                    + " — 可先设 LITEPAN_WEB_HOST=0.0.0.0 仅用 IPv4，或检查本机 IPv6。"
                )
                sys.exit(1)
            print(f"[{current_time}] INFO 系统 | 启动 {APP_NAME} 服务器 ([::]:{_port}，IPv4+IPv6 双栈)")
            config = Config(
                "main:app",
                host="::",
                port=_port,
                reload=False,
                access_log=False,
                log_level="error",
                loop="asyncio",
                timeout_graceful_shutdown=_graceful,
            )
            Server(config=config).run(sockets=[_lsock])
        elif not _web_host_override:
            from uvicorn import Config, Server

            try:
                _lsock = _bind_listen_socket_dual_stack_ipv6_wildcard(_port, _UVICORN_LISTEN_BACKLOG)
            except OSError as e:
                print(
                    f"[{current_time}] WARN  [::] listen 不可用（{e}），自动回退 0.0.0.0:{_port}（仅用 IPv4）"
                )
                uvicorn.run(
                    "main:app",
                    host="0.0.0.0",
                    port=_port,
                    reload=False,
                    access_log=False,
                    log_level="error",
                    loop="asyncio",
                    timeout_graceful_shutdown=_graceful,
                )
            else:
                print(f"[{current_time}] INFO 系统 | 启动 {APP_NAME} 服务器 ([::]:{_port}，IPv4+IPv6 双栈自动检测)")
                config = Config(
                    "main:app",
                    host="::",
                    port=_port,
                    reload=False,
                    access_log=False,
                    log_level="error",
                    loop="asyncio",
                    timeout_graceful_shutdown=_graceful,
                )
                Server(config=config).run(sockets=[_lsock])
        else:
            print(f"[{current_time}] INFO 系统 | 启动 {APP_NAME} 服务器 ({_web_host_override}:{_port})")
            uvicorn.run(
                "main:app",
                host=_web_host_override,
                port=_port,
                reload=False,
                access_log=False,
                log_level="error",
                loop="asyncio",
                timeout_graceful_shutdown=_graceful,
            )
    except KeyboardInterrupt:
        print("\n服务已停止")
    except Exception as e:
        print(f"ERROR 服务启动失败: {e}")
        sys.exit(1)
