import asyncio
import socket
from typing import Dict, List, Optional

import uvicorn
from fastapi import FastAPI, Request

from core.log_manager import get_writer, LogModule
from database.db import db


class EmbyProxyServerManager:
    def __init__(self):
        self._servers: Dict[int, uvicorn.Server] = {}
        self._tasks: Dict[int, asyncio.Task] = {}
        self._ports: Dict[int, int] = {}
        self._sockets: Dict[int, List[socket.socket]] = {}
        self._check_tasks: Dict[int, asyncio.Task] = {}
        self._lock = asyncio.Lock()

    def _log(self, level: str, message: str):
        try:
            logger = get_writer(LogModule.SYSTEM)
            getattr(logger, level)(message)
        except Exception:
            print(message)

    def _build_app(self, proxy_id: int) -> FastAPI:
        app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

        @app.api_route("/", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
        @app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
        async def dedicated_emby_proxy(request: Request, full_path: str = ""):
            from api.emby_proxy import handle_emby_proxy_request

            return await handle_emby_proxy_request(proxy_id, request, full_path)

        return app

    def _close_sockets(self, sockets: Optional[List[socket.socket]]):
        if not sockets:
            return
        for sock in sockets:
            try:
                sock.close()
            except Exception:
                pass

    def _open_listen_sockets(self, port: int) -> List[socket.socket]:
        sockets: List[socket.socket] = []
        errors: List[str] = []

        def bind_socket(family: int, address, label: str) -> None:
            sock = socket.socket(family, socket.SOCK_STREAM)
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                if family == socket.AF_INET6 and hasattr(socket, "IPV6_V6ONLY"):
                    # 显式拆成 IPv6 + IPv4 两个 socket，避免不同系统的双栈默认值不一致。
                    sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
                sock.bind(address)
                sock.listen(2048)
                sock.setblocking(False)
                sockets.append(sock)
            except OSError:
                sock.close()
                raise

        try:
            bind_socket(socket.AF_INET6, ("::", port), "IPv6")
        except OSError as exc:
            errors.append(f"IPv6监听失败: {exc}")

        try:
            bind_socket(socket.AF_INET, ("0.0.0.0", port), "IPv4")
        except OSError as exc:
            errors.append(f"IPv4监听失败: {exc}")

        if not sockets:
            raise OSError("; ".join(errors) or f"端口 {port} 监听失败")

        for message in errors:
            self._log("warning", f"Emby反代端口 {port} {message}")
        return sockets

    async def initialize(self):
        configs = await db.get_emby_proxy_configs()
        for config in configs:
            if str(config.get("status") or "running") == "running":
                await self.start_proxy(config)

    async def _check_started_later(self, proxy_id: int, port: int, task: asyncio.Task):
        try:
            await asyncio.sleep(0.2)
            if self._tasks.get(proxy_id) is not task:
                return
            if task.done():
                error = task.exception()
                message = f"Emby反代端口 {port} 监听失败"
                if error:
                    message = f"{message}: {error}"
                await db.update_emby_proxy_config(proxy_id, last_error=message)
                self._log("error", message)
                self._servers.pop(proxy_id, None)
                self._tasks.pop(proxy_id, None)
                self._ports.pop(proxy_id, None)
                self._close_sockets(self._sockets.pop(proxy_id, None))
                return
            await db.update_emby_proxy_config(proxy_id, last_error=None)
            self._log("info", f"Emby反代已监听端口 {port}，配置ID: {proxy_id}")
        except asyncio.CancelledError:
            return
        except Exception as e:
            self._log("error", f"Emby反代启动检查异常: {e}")
        finally:
            if self._check_tasks.get(proxy_id) is asyncio.current_task():
                self._check_tasks.pop(proxy_id, None)

    async def start_proxy(self, config: dict):
        proxy_id = int(config["id"])
        port = int(config.get("proxy_port") or 0)
        if port <= 0:
            return

        await self.stop_proxy(proxy_id)

        try:
            sockets = self._open_listen_sockets(port)
        except OSError as exc:
            message = f"Emby反代端口 {port} 监听失败: {exc}"
            await db.update_emby_proxy_config(proxy_id, last_error=message)
            self._log("error", message)
            return

        app = self._build_app(proxy_id)
        uvicorn_config = uvicorn.Config(
            app,
            host=None,
            port=port,
            log_level="warning",
            access_log=False,
            lifespan="off",
            timeout_graceful_shutdown=3,
        )
        server = uvicorn.Server(uvicorn_config)
        server.install_signal_handlers = lambda: None
        task = asyncio.create_task(server.serve(sockets=sockets))
        self._servers[proxy_id] = server
        self._tasks[proxy_id] = task
        self._ports[proxy_id] = port
        self._sockets[proxy_id] = sockets

        check_task = asyncio.create_task(self._check_started_later(proxy_id, port, task))
        self._check_tasks[proxy_id] = check_task

    async def stop_proxy(self, proxy_id: int):
        server: Optional[uvicorn.Server] = self._servers.pop(proxy_id, None)
        task: Optional[asyncio.Task] = self._tasks.pop(proxy_id, None)
        check_task: Optional[asyncio.Task] = self._check_tasks.pop(proxy_id, None)
        sockets = self._sockets.pop(proxy_id, None)
        port = self._ports.pop(proxy_id, None)
        if check_task:
            check_task.cancel()
            try:
                await check_task
            except asyncio.CancelledError:
                pass
        if not server:
            self._close_sockets(sockets)
            return

        server.should_exit = True
        if task:
            try:
                await asyncio.wait_for(task, timeout=3)
            except asyncio.TimeoutError:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            except Exception:
                pass
        self._close_sockets(sockets)
        if port:
            self._log("info", f"Emby反代已停止监听端口 {port}，配置ID: {proxy_id}")

    async def sync_proxy(self, proxy_id: int):
        async with self._lock:
            config = await db.get_emby_proxy_config(proxy_id)
            await self.stop_proxy(proxy_id)
            if config and str(config.get("status") or "running") == "running":
                await self.start_proxy(config)

    async def delete_proxy(self, proxy_id: int):
        async with self._lock:
            await self.stop_proxy(proxy_id)

    async def shutdown(self):
        async with self._lock:
            proxy_ids = list(self._servers.keys())
            await asyncio.gather(
                *(self.stop_proxy(proxy_id) for proxy_id in proxy_ids),
                return_exceptions=True,
            )


emby_proxy_server_manager = EmbyProxyServerManager()
