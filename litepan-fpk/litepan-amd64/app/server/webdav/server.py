"""FastAPI 版 WebDAV 服务器：统一走驱动层 + 全局缓存 + 代理/重定向两种下载模式。"""

import xml.etree.ElementTree as ET
import base64
import time
import asyncio
import os
import uuid
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import PurePosixPath
from typing import List, Tuple, Dict, Optional, Any
from urllib.parse import unquote, quote
from html import escape as html_escape
from fastapi import Request, Response, HTTPException

# 注册 DAV 命名空间前缀，避免 ElementTree 自动生成 ns0:/ns1: 导致部分客户端（PotPlayer 等）无法解析
ET.register_namespace('D', 'DAV:')

from database.db import db
from core.driver_service import (
    get_account_driver,
    get_effective_download_mode,
    resolve_download,
)
from core.range_proxy import build_proxy_file_info_from_download, serve_range_proxy
from core.base import FileItem
from cache import get_global_cache, get_cache_cleaner
from core.log_manager import get_writer, LogModule
from config import config_manager
from core.operation_wrapper import current_account_id
from .utils import parse_webdav_path, generate_propfind_response, generate_directory_html, get_mime_type, WEBDAV_TIME_FORMAT, WEBDAV_ISO_TIME_FORMAT, ensure_utc

def _parse_db_timestamp(value: str) -> datetime:
    """将 SQLite TIMESTAMP 字符串转为 UTC datetime，解析失败回退到当前时间。"""
    if not value:
        return datetime.now(timezone.utc)
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.now(timezone.utc)
from core.utils import normalize_bool


def get_webdav_logger():
    # 延迟拿 logger，模块 import 期 log 系统可能还没 ready
    try:
        return get_writer(LogModule.WEBDAV)
    except RuntimeError:
        class FallbackLogger:
            def debug(self, msg, **kwargs): pass
            def info(self, msg, **kwargs): pass
            def warning(self, msg, **kwargs): print(f"[WebDAV] {msg}")
            def error(self, msg, **kwargs): print(f"[WebDAV ERROR] {msg}")
        return FallbackLogger()

webdav_logger = get_webdav_logger()

def get_cache_logger():
    try:
        return get_writer(LogModule.CACHE)
    except RuntimeError:
        class FallbackLogger:
            def debug(self, msg, **kwargs): pass
            def info(self, msg, **kwargs): pass
            def warning(self, msg, **kwargs): print(f"[CACHE] {msg}")
            def error(self, msg, **kwargs): print(f"[CACHE ERROR] {msg}")
        return FallbackLogger()

cache_logger = get_cache_logger()

NS = {
    'D': 'DAV:',
    'xmlns:D': 'DAV:'
}


class FastAPIWebDAVServer:
    def __init__(self, check_auth_func=None):
        self.check_auth = check_auth_func
        self.cache_manager = get_global_cache()
        self.cache_cleaner = get_cache_cleaner()

    def _is_macos_metadata_path(self, path: str) -> bool:
        parts = [unquote(part) for part in (path or "").split("/") if part]
        if not parts:
            return False

        name = parts[-1]
        ignored_dirs = {".TemporaryItems", ".Trashes", ".Spotlight-V100", ".fseventsd"}
        return (
            name == ".DS_Store"
            or name.startswith("._")
            or any(part in ignored_dirs for part in parts)
        )

    async def _handle_macos_metadata_request(self, request: Request, method: str) -> Response:
        if method == "PUT":
            try:
                await request.body()
            except Exception:
                pass
            return Response(status_code=201)
        if method in {"DELETE", "MOVE", "COPY"}:
            return Response(status_code=204)
        if method == "MKCOL":
            return Response(status_code=201)
        return Response(status_code=404)

    def _normalize_cache_path(self, path: str) -> str:
        normalized = "/" + (path or "").strip("/")
        return "/" if normalized == "/" else normalized.rstrip("/")

    def _format_http_datetime(self, value: Optional[datetime]) -> str:
        if value:
            return ensure_utc(value).strftime(WEBDAV_TIME_FORMAT)
        return datetime.now(timezone.utc).strftime(WEBDAV_TIME_FORMAT)

    def _build_etag(self, file_info: FileItem) -> str:
        modified_part = ""
        if file_info.modified:
            try:
                modified_part = f"-{int(file_info.modified.timestamp())}"
            except Exception:
                modified_part = ""
        # WebDAV 驱动的 id 可能含中文，HTTP 头按 latin-1 编码，这里必须 percent-encode
        safe_id = quote(str(file_info.id), safe="")
        return f"\"{safe_id}-{file_info.size}{modified_part}\""

    def _parse_single_range(self, range_header: str, total_size: int) -> Tuple[int, int]:
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
        if start < 0:
            raise ValueError("start")
        if start >= total_size:
            raise ValueError("unsat")
        if end_str == "":
            return start, total_size - 1
        end = int(end_str)
        if end < start:
            raise ValueError("order")
        if end >= total_size:
            end = total_size - 1
        return start, end

    def _match_if_none_match(self, request: Request, etag: str) -> bool:
        value = (request.headers.get("If-None-Match") or "").strip()
        if not value:
            return False
        if value == "*":
            return True
        candidates = [item.strip() for item in value.split(",") if item.strip()]
        return etag in candidates

    def _match_if_modified_since(self, request: Request, last_modified: Optional[datetime]) -> bool:
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

    def _build_not_modified_response(self, etag: str, last_modified: Optional[datetime]) -> Response:
        headers = {"ETag": etag}
        if last_modified:
            headers["Last-Modified"] = self._format_http_datetime(last_modified)
        return Response(status_code=304, headers=headers)

    async def _is_webdav_metadata_cache_enabled(self) -> bool:
        try:
            value = await config_manager.get_async("webdav_cache_enabled")
            return normalize_bool(value, True)
        except Exception:
            return True

    async def _is_webdav_path_cache_enabled(self) -> bool:
        try:
            value = await config_manager.get_async("webdav_cache_enabled")
            return normalize_bool(value, True)
        except Exception:
            return True

    async def handle_request(self, request: Request, path: str = "") -> Response:
        try:
            if not await self._check_authentication(request):
                return Response(
                    status_code=401,
                    headers={'WWW-Authenticate': 'Basic realm="LitePan v3.1 WebDAV"'}
                )

            method = request.method.upper()
            path = '/' + path.strip('/')

            handlers = {
                'OPTIONS': self._handle_options,
                'PROPFIND': self._handle_propfind,
                'GET': self._handle_get,
                'HEAD': self._handle_head,
                'PUT': self._handle_put,
                'DELETE': self._handle_delete,
                'MKCOL': self._handle_mkcol,
                'MOVE': self._handle_move,
                'COPY': self._handle_copy,
            }

            handler = handlers.get(method)
            if not handler:
                return Response(
                    status_code=405,
                    headers={'Allow': ', '.join(handlers.keys())}
                )

            if method != "OPTIONS" and self._is_macos_metadata_path(path):
                return await self._handle_macos_metadata_request(request, method)

            try:
                return await handler(request, path)
            except (ConnectionResetError, BrokenPipeError, OSError, asyncio.CancelledError) as e:
                webdav_logger.debug(f"WebDAV客户端连接断开: {type(e).__name__}: {e}")
                return Response(status_code=499, content="Client Disconnected")
            except Exception as e:
                webdav_logger.error(f"WebDAV处理错误: {e}", exc_info=True)
                return Response(status_code=500, content=str(e))
        except Exception as e:
            webdav_logger.error(f"WebDAV请求处理异常: {e}", exc_info=True)
            return Response(status_code=500, content=f"Internal Server Error: {str(e)}")

    async def _check_authentication(self, request: Request) -> bool:
        """WebDAV Basic Auth：直接复用管理员用户名/密码，密码必须已哈希。"""
        auth_header = request.headers.get('Authorization', '')
        if not auth_header.startswith('Basic '):
            return False

        try:
            credentials = base64.b64decode(auth_header[6:]).decode('utf-8')
            username, password = credentials.split(':', 1)

            admin_username = config_manager.get('admin_username')
            admin_password = config_manager.get('admin_password')

            # 兼容历史把值存成带引号 JSON 字符串的情况
            if isinstance(admin_username, str) and admin_username.startswith('"') and admin_username.endswith('"'):
                admin_username = admin_username[1:-1]
            if isinstance(admin_password, str) and admin_password.startswith('"') and admin_password.endswith('"'):
                admin_password = admin_password[1:-1]

            if not admin_username or not admin_password:
                return False

            if username != admin_username:
                webdav_logger.warning(f"WebDAV认证失败: 用户名不匹配 {username}")
                return False

            password_match = False

            webdav_logger.debug(f"WebDAV认证尝试: 用户名={username}")

            try:
                if admin_password.startswith('scrypt:') or admin_password.startswith('pbkdf2:'):
                    from core.security import check_password_hash
                    password_match = check_password_hash(admin_password, password)
                    if password_match:
                        webdav_logger.debug("标准哈希密码匹配成功")
                else:
                    # 明文密码直接拒绝，避免和旧配置产生安全误判
                    webdav_logger.warning("管理员密码未哈希化，出于安全原因拒绝访问")
            except Exception as hash_error:
                webdav_logger.debug(f"哈希密码验证失败: {hash_error}")

            if password_match:
                webdav_logger.debug(f"WebDAV认证成功: 用户={username}")
            else:
                webdav_logger.warning(f"WebDAV认证失败: 用户={username}, 密码错误")

            return password_match

        except Exception as e:
            webdav_logger.error(f"WebDAV认证失败: {e}")
            return False

    async def _handle_options(self, request: Request, path: str) -> Response:
        return Response(
            status_code=200,
            headers={
                'Allow': 'OPTIONS, PROPFIND, GET, HEAD, PUT, DELETE, MKCOL, MOVE, COPY',
                'DAV': '1, 2',
                'MS-Author-Via': 'DAV'
            }
        )

    async def _handle_propfind(self, request: Request, path: str) -> Response:
        try:
            depth = request.headers.get('Depth', 'infinity')
            account_name, file_path = parse_webdav_path(path)

            if not account_name:
                resource = await self._get_resource(path)
                if not resource:
                    return Response(status_code=404, content="Resource not found")

                multistatus = ET.Element('{DAV:}multistatus')
                await self._add_response_element(multistatus, path, resource)

                if resource['is_collection'] and depth != '0':
                    children = await self._list_children(path)
                    for child_name, child_resource in children:
                        child_path = path.rstrip('/') + '/' + child_name
                        await self._add_response_element(multistatus, child_path, child_resource)

                xml_str = ET.tostring(multistatus, encoding='unicode', xml_declaration=True)
                return Response(
                    content=xml_str,
                    status_code=207,
                    headers={'Content-Type': 'application/xml; charset=utf-8'}
                )

            context = await self._get_account_context_by_name(account_name)
            if not context:
                return Response(status_code=404, content="Account not found")

            account_id = context["account_id"]
            cache_key_path = f"PROPFIND|{path}|depth={depth}"
            if self.cache_manager and await self._is_webdav_metadata_cache_enabled():
                cached_xml = await self.cache_manager.get_webdav_metadata_cache(account_id, cache_key_path)
                if isinstance(cached_xml, str) and cached_xml:
                    return Response(
                        content=cached_xml.encode("utf-8"),
                        status_code=207,
                        headers={'Content-Type': 'application/xml; charset=utf-8'}
                    )

            driver = context["driver"]
            account_info = {
                "id": account_id,
                "name": context.get("name") or account_name,
                "driver_type": context.get("driver_type"),
                "config": context.get("config"),
            }

            if file_path in ("/", ""):
                file_info = FileItem(
                    id="0",
                    name=account_name,
                    path="/",
                    size=0,
                    is_dir=True,
                    modified=datetime.now(timezone.utc),
                    created=datetime.now(timezone.utc),
                )
                resource = {
                    "is_collection": True,
                    "file_info": file_info,
                    "driver": driver,
                    "account": account_info,
                }
                children_parent_id = "0"
            else:
                file_info = await self._resolve_path_to_file(driver, file_path, account_id=account_id)
                if not file_info:
                    return Response(status_code=404, content="Resource not found")
                resource = {
                    "is_collection": file_info.is_dir,
                    "file_info": file_info,
                    "driver": driver,
                    "account": account_info,
                }
                children_parent_id = str(file_info.id)

            multistatus = ET.Element('{DAV:}multistatus')
            await self._add_response_element(multistatus, path, resource)

            if resource["is_collection"] and depth != "0":
                files = await driver.list_files(children_parent_id)
                for child in files:
                    child_path = path.rstrip('/') + '/' + child.name
                    await self._add_response_element(
                        multistatus,
                        child_path,
                        {
                            "is_collection": child.is_dir,
                            "file_info": child,
                            "driver": driver,
                            "account": account_info,
                        },
                    )

            xml_str = ET.tostring(multistatus, encoding='unicode', xml_declaration=True)
            if self.cache_manager and await self._is_webdav_metadata_cache_enabled():
                await self.cache_manager.set_webdav_metadata_cache(account_id, cache_key_path, xml_str)

            return Response(
                content=xml_str,
                status_code=207,
                headers={'Content-Type': 'application/xml; charset=utf-8'}
            )

        except Exception as e:
            webdav_logger.error(f"PROPFIND处理错误: {e}", exc_info=True)
            return Response(status_code=500, content="Internal Server Error")

    async def _handle_root_propfind(self, path: str, depth: str) -> Response:
        try:
            from database.db import get_db
            db_conn = await get_db()

            if not db_conn:
                webdav_logger.error("数据库连接不可用")
                return Response(status_code=503, content="Database not available")

            try:
                async with db_conn.execute("SELECT id, name, driver_type, created_at FROM cloud_accounts WHERE is_active = 1") as cursor:
                    accounts = await cursor.fetchall()
            except Exception as db_error:
                webdav_logger.error(f"查询账号失败: {db_error}")
                return Response(status_code=503, content="Database query failed")

            if not accounts:
                return Response(status_code=404, content="No accounts available")

            root_info = FileItem(
                id="root",
                name="",
                path="/",
                size=0,
                is_dir=True,
                modified=datetime.now(timezone.utc),
                created=datetime.now(timezone.utc)
            )

            webdav_logger.debug(f"PROPFIND根路径处理: depth={depth}, 账号数量={len(accounts)}")
            if depth == '0':
                webdav_logger.debug("使用Depth=0响应")
                xml_response = generate_propfind_response(root_info, path)
            else:
                webdav_logger.debug("使用Depth=1响应，包含账号列表")
                xml_response = self._generate_root_propfind_response(root_info, path, accounts)

            return Response(
                content=xml_response,
                media_type="application/xml",
                headers={
                    'Content-Type': 'application/xml; charset=utf-8',
                    'DAV': '1, 2'
                }
            )
            
        except Exception as e:
            webdav_logger.error(f"根路径PROPFIND处理错误: {e}", exc_info=True)
            return Response(status_code=500, content="Internal Server Error")

    def _build_webdav_xml_element(self, parent, path: str, file_info: Optional[FileItem], is_collection: bool = False):
        import xml.etree.ElementTree as ET
        from urllib.parse import quote
        from datetime import datetime
        import mimetypes

        response = ET.SubElement(parent, '{DAV:}response')

        href = ET.SubElement(response, '{DAV:}href')
        href.text = '/dav' + quote(path.encode('utf-8'))
        if is_collection and not href.text.endswith('/'):
            href.text += '/'

        propstat = ET.SubElement(response, '{DAV:}propstat')
        prop = ET.SubElement(propstat, '{DAV:}prop')

        resourcetype = ET.SubElement(prop, '{DAV:}resourcetype')
        if is_collection:
            ET.SubElement(resourcetype, '{DAV:}collection')
            folder_size = int(file_info.size or 0) if file_info else 0
            ET.SubElement(prop, '{DAV:}getcontentlength').text = str(folder_size if folder_size > 0 else 0)
        elif file_info:
            ET.SubElement(prop, '{DAV:}getcontentlength').text = str(file_info.size)

        if file_info:
            displayname = ET.SubElement(prop, '{DAV:}displayname')
            displayname.text = file_info.name

        getlastmodified = ET.SubElement(prop, '{DAV:}getlastmodified')
        if file_info and file_info.modified:
            getlastmodified.text = file_info.modified.strftime(WEBDAV_TIME_FORMAT)
        else:
            getlastmodified.text = datetime.now(timezone.utc).strftime(WEBDAV_TIME_FORMAT)

        creationdate = ET.SubElement(prop, '{DAV:}creationdate')
        if file_info and file_info.created:
            creationdate.text = file_info.created.strftime(WEBDAV_ISO_TIME_FORMAT)
        else:
            creationdate.text = datetime.now(timezone.utc).strftime(WEBDAV_ISO_TIME_FORMAT)

        getcontenttype = ET.SubElement(prop, '{DAV:}getcontenttype')
        if is_collection:
            getcontenttype.text = 'httpd/unix-directory'
        elif file_info:
            mime_type, _ = mimetypes.guess_type(file_info.name)
            getcontenttype.text = mime_type or 'application/octet-stream'

        status = ET.SubElement(propstat, '{DAV:}status')
        status.text = 'HTTP/1.1 200 OK'

    def _generate_root_propfind_response(self, root_info: FileItem, path: str, accounts) -> str:
        import xml.etree.ElementTree as ET

        root = ET.Element('{DAV:}multistatus')

        self._build_webdav_xml_element(root, path, root_info, is_collection=True)

        for account_id, account_name, driver_type, created_at in accounts:
            account_path = f'{path.rstrip("/")}/{account_name}/'
            account_info = FileItem(
                id=str(account_id),
                name=account_name,
                path=account_path,
                size=0,
                is_dir=True,
                modified=_parse_db_timestamp(created_at),
                created=_parse_db_timestamp(created_at)
            )
            self._build_webdav_xml_element(root, account_path, account_info, is_collection=True)
        
        return ET.tostring(root, encoding='unicode', xml_declaration=True)

    def _generate_directory_propfind_response(self, dir_info: FileItem, path: str, files) -> str:
        import xml.etree.ElementTree as ET

        root = ET.Element('{DAV:}multistatus')

        self._build_webdav_xml_element(root, path, dir_info, is_collection=True)

        for file in files:
            file_path = f'{path.rstrip("/")}/{file.name}'
            if file.is_dir:
                file_path += '/'
            self._build_webdav_xml_element(root, file_path, file, is_collection=file.is_dir)

        return ET.tostring(root, encoding='unicode', xml_declaration=True)

    async def _get_resource(self, path: str) -> Optional[Dict]:
        try:
            account_name, file_path = parse_webdav_path(path)

            if not account_name:
                from database.db import get_db
                db_conn = await get_db()

                if not db_conn:
                    webdav_logger.error("数据库连接不可用")
                    return None

                try:
                    async with db_conn.execute("SELECT id, name, driver_type, created_at FROM cloud_accounts WHERE is_active = 1") as cursor:
                        accounts = await cursor.fetchall()
                except Exception as db_error:
                    webdav_logger.error(f"查询账号失败: {db_error}")
                    return None

                result = {
                    'is_collection': True,
                    'file_info': None,
                    'driver': None,
                    'account': None,
                    'accounts': accounts
                }
                return result

            driver = await self._get_driver_by_name(account_name)
            if not driver:
                return None

            from database.db import get_db
            db_conn = await get_db()

            if not db_conn:
                webdav_logger.error("数据库连接不可用")
                return None

            try:
                async with db_conn.execute(
                    "SELECT id, name, driver_type, config, created_at FROM cloud_accounts WHERE name = ? AND is_active = 1",
                    (account_name,)
                ) as cursor:
                    account = await cursor.fetchone()
            except Exception as db_error:
                webdav_logger.error(f"查询账号失败: {db_error}")
                return None

            if not account:
                return None

            account_id, name, driver_type, config, created_at = account

            if file_path == "/" or file_path == "":
                account_time = _parse_db_timestamp(created_at)
                result = {
                    'is_collection': True,
                    'file_info': FileItem(
                        id="0",
                        name=account_name,
                        path="/",
                        size=0,
                        is_dir=True,
                        modified=account_time,
                        created=account_time
                    ),
                    'driver': driver,
                    'account': {
                        'id': account_id,
                        'name': name,
                        'driver_type': driver_type,
                        'config': config
                    },
                }
                return result

            file_info = await self._resolve_path_to_file(driver, file_path)
            
            if file_info:
                result = {
                    'is_collection': file_info.is_dir,
                    'file_info': file_info,
                    'driver': driver,
                    'account': {
                        'id': account_id,
                        'name': name,
                        'driver_type': driver_type,
                        'config': config
                    },
                }
                return result

            return None
                
        except Exception as e:
            webdav_logger.error(f"获取资源信息失败 {path}: {e}")
        
        return None

    async def _list_children(self, path: str) -> List[Tuple[str, Dict]]:
        resource = await self._get_resource(path)
        if not resource:
            webdav_logger.error(f"无法获取资源信息: {path}")
            return []

        if not resource.get('is_collection', False):
            return []

        children = []

        if not path.strip('/'):
            # 根目录返回所有账号作为子资源
            accounts = resource.get('accounts', [])
            for account_id, account_name, driver_type, created_at in accounts:
                children.append((account_name, {
                    'is_collection': True,
                    'file_info': FileItem(
                        id="0",
                        name=account_name,
                        path="/",
                        size=0,
                        is_dir=True,
                        modified=_parse_db_timestamp(created_at),
                        created=_parse_db_timestamp(created_at)
                    ),
                    'driver': None,
                    'account': {
                        'id': account_id,
                        'name': account_name,
                        'driver_type': driver_type
                    },
                }))
        else:
            driver = resource['driver']
            file_id = resource['file_info'].id if resource['file_info'] else '0'

            files = await driver.list_files(file_id)
            for file_item in files:
                children.append((file_item.name, {
                    'is_collection': file_item.is_dir,
                    'file_info': file_item,
                    'driver': driver,
                    'account': resource['account'],
                }))

        return children

    async def _generate_directory_listing(self, path: str, resource: Dict) -> str:
        try:
            if not path.strip('/'):
                accounts = resource.get('accounts', [])
                html = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>LitePan WebDAV - 根目录</title>
                    <meta charset="utf-8">
                    <style>
                        body {{ font-family: Arial, sans-serif; margin: 20px; }}
                        .header {{ background: #f0f0f0; padding: 10px; margin-bottom: 20px; }}
                        .account-list {{ list-style: none; padding: 0; }}
                        .account-item {{ 
                            padding: 10px; 
                            margin: 5px 0; 
                            background: #fff; 
                            border: 1px solid #ddd; 
                            border-radius: 5px;
                        }}
                        .account-link {{ 
                            text-decoration: none; 
                            color: #333; 
                            font-weight: bold; 
                            font-size: 16px;
                        }}
                        .account-link:hover {{ color: #007bff; }}
                    </style>
                </head>
                <body>
                    <div class="header">
                        <h1>LitePan WebDAV 根目录</h1>
                        <p>可用的网盘账号：</p>
                    </div>
                    <ul class="account-list">
                """
                
                for account_id, account_name, driver_type, created_at in accounts:
                    html += f"""
                        <li class="account-item">
                            <a href="/dav/{account_name}/" class="account-link">📁 {account_name}</a>
                        </li>
                    """
                
                html += """
                    </ul>
                </body>
                </html>
                """
                return html
            else:
                driver = resource['driver']
                file_id = resource['file_info'].id if resource['file_info'] else '0'

                files = await driver.list_files(file_id)

                html = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>LitePan WebDAV - {html_escape(path)}</title>
                    <meta charset="utf-8">
                    <style>
                        body {{ font-family: Arial, sans-serif; margin: 20px; }}
                        .header {{ background: #f0f0f0; padding: 10px; margin-bottom: 20px; }}
                        .file-list {{ list-style: none; padding: 0; }}
                        .file-item {{ 
                            padding: 10px; 
                            margin: 5px 0; 
                            background: #fff; 
                            border: 1px solid #ddd; 
                            border-radius: 5px;
                            display: flex;
                            justify-content: space-between;
                            align-items: center;
                        }}
                        .file-link {{ 
                            text-decoration: none; 
                            color: #333; 
                            font-weight: bold;
                        }}
                        .file-link:hover {{ color: #007bff; }}
                        .file-size {{ color: #666; font-size: 12px; }}
                        .folder-icon {{ color: #ffc107; }}
                        .file-icon {{ color: #6c757d; }}
                    </style>
                </head>
                <body>
                    <div class="header">
                        <h1>📁 {html_escape(path)}</h1>
                        <p>文件列表：</p>
                    </div>
                    <ul class="file-list">
                """
                
                for file_item in files:
                    safe_name = html_escape(file_item.name)
                    quoted_name = quote(file_item.name)
                    if file_item.is_dir:
                        icon = "📁"
                        link = f"/dav{path.rstrip('/')}/{quoted_name}/"
                        size = ""
                    else:
                        icon = "📄"
                        link = f"/dav{path.rstrip('/')}/{quoted_name}"
                        size = f" ({file_item.size} bytes)" if file_item.size else ""
                    
                    html += f"""
                        <li class="file-item">
                            <a href="{link}" class="file-link">{icon} {safe_name}</a>
                            <span class="file-size">{size}</span>
                        </li>
                    """
                
                html += """
                    </ul>
                </body>
                </html>
                """
                return html
                
        except Exception as e:
            webdav_logger.error(f"生成目录列表失败: {e}")
            return f"<html><body><h1>错误</h1><p>生成目录列表失败: {html_escape(str(e))}</p></body></html>"

    async def _add_response_element(self, parent: ET.Element, path: str, resource: Dict):
        self._build_webdav_xml_element(
            parent,
            path,
            resource.get('file_info'),
            is_collection=resource.get('is_collection', False)
        )

    async def _get_driver_by_name(self, account_name: str):
        context = await self._get_account_context_by_name(account_name)
        if not context:
            return None
        return context['driver']

    async def _get_account_context_by_name(self, account_name: str) -> Optional[Dict[str, Any]]:
        try:
            from database.db import get_db
            db_conn = await get_db()

            if not db_conn:
                webdav_logger.error("数据库连接不可用")
                return None

            async with db_conn.execute(
                "SELECT id, name, driver_type, config, created_at FROM cloud_accounts WHERE name = ? AND is_active = 1", 
                (account_name,)
            ) as cursor:
                account = await cursor.fetchone()
            
            if not account:
                return None

            account_id, name, driver_type, config, created_at = account

            from core.driver_service import get_account_driver
            driver = await get_account_driver(account_id, db_conn)
            
            return {
                'account_id': str(account_id),
                'name': name,
                'driver_type': driver_type,
                'config': config,
                'driver': driver,
                'created_at': created_at,
            }
            
        except Exception as e:
            webdav_logger.error(f"根据账号名称获取驱动失败: {e}")
            return None

    async def _handle_root_listing(self, path: str) -> Response:
        try:
            from database.db import get_db
            db_conn = await get_db()
            
            if not db_conn:
                webdav_logger.error("数据库连接不可用")
                return Response(status_code=503, content="Database not available")
            
            # 获取所有启用的账号
            try:
                async with db_conn.execute("SELECT id, name, driver_type, created_at FROM cloud_accounts WHERE is_active = 1") as cursor:
                    accounts = await cursor.fetchall()
            except Exception as db_error:
                webdav_logger.error(f"查询账号失败: {db_error}")
                return Response(status_code=503, content="Database query failed")
            
            if not accounts:
                return Response(status_code=404, content="No accounts available")
            
            # 创建根目录的FileItem
            root_info = FileItem(
                id="root",
                name="",
                path="/",
                size=0,
                is_dir=True,
                modified=datetime.now(timezone.utc),
                created=datetime.now(timezone.utc)
            )
            
            # 生成PROPFIND响应，包含所有账号
            xml_response = self._generate_root_propfind_response(root_info, path, accounts)
            
            return Response(
                content=xml_response,
                media_type="application/xml",
                headers={
                    'Content-Type': 'application/xml; charset=utf-8',
                    'DAV': '1, 2'
                }
            )
            
        except Exception as e:
            webdav_logger.error(f"根路径处理错误: {e}", exc_info=True)
            return Response(status_code=500, content="Internal Server Error")

    async def _handle_root_get(self, path: str) -> Response:
        try:
            from database.db import get_db
            db_conn = await get_db()
            
            if not db_conn:
                webdav_logger.error("数据库连接不可用")
                return Response(status_code=503, content="Database not available")
            
            # 获取所有启用的账号
            try:
                async with db_conn.execute("SELECT id, name, driver_type, created_at FROM cloud_accounts WHERE is_active = 1") as cursor:
                    accounts = await cursor.fetchall()
            except Exception as db_error:
                webdav_logger.error(f"查询账号失败: {db_error}")
                return Response(status_code=503, content="Database query failed")
            
            if not accounts:
                return Response(status_code=404, content="No accounts available")
            
            # 生成HTML目录列表，显示所有账号作为文件夹
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Directory listing for /</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    .file-list {{ border-collapse: collapse; width: 100%; }}
                    .file-list th, .file-list td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    .file-list th {{ background-color: #f2f2f2; }}
                    .directory {{ color: #0066cc; }}
                    .file {{ color: #333; }}
                    .size {{ text-align: right; }}
                </style>
            </head>
            <body>
                <h1>Directory listing for /</h1>
                <table class="file-list">
                    <tr>
                        <th>Name</th>
                        <th>Size</th>
                        <th>Modified</th>
                    </tr>
            """
            
            # 添加所有账号作为目录
            for account_id, account_name, driver_type, created_at in accounts:
                html_content += f"""
                    <tr>
                        <td><a href="/dav/{account_name}/" class="directory">📁 {account_name}</a></td>
                        <td class="size">-</td>
                        <td>-</td>
                    </tr>
                """
            
            html_content += """
                </table>
            </body>
            </html>
            """
            
            return Response(
                content=html_content,
                media_type="text/html",
                headers={
                    'Content-Type': 'text/html; charset=utf-8'
                }
            )
            
        except Exception as e:
            webdav_logger.error(f"根路径GET处理错误: {e}", exc_info=True)
            return Response(status_code=500, content="Internal Server Error")


    async def _handle_get(self, request: Request, path: str) -> Response:
        try:
            resource = await self._get_resource(path)
            if not resource:
                return Response(status_code=404, content="Resource not found")

            if resource['is_collection']:
                html_content = await self._generate_directory_listing(path, resource)
                return Response(
                    content=html_content,
                    media_type="text/html",
                    headers={
                        'Cache-Control': 'public, max-age=1800',
                    }
                )
            else:
                file_info = resource['file_info']
                account = resource.get('account', {})
                account_id = account['id']

                return await self._handle_file_download(resource['driver'], file_info, request)

        except Exception as e:
            webdav_logger.error(f"GET处理错误: {e}", exc_info=True)
            return Response(status_code=500, content=f"Internal Server Error: {str(e)}")

    async def _handle_directory_listing(self, driver, parent_id: str, webdav_path: str) -> Response:
        try:
            webdav_logger.debug(f"处理目录列表: parent_id={parent_id}, webdav_path={webdav_path}")

            files = await driver.list_files(parent_id)

            webdav_logger.debug(f"获取到 {len(files)} 个文件/文件夹")

            html_content = generate_directory_html(files, webdav_path)
            
            return Response(
                content=html_content,
                media_type="text/html",
                headers={
                    'Content-Type': 'text/html; charset=utf-8'
                }
            )
        except Exception as e:
            webdav_logger.error(f"目录列表生成失败: {e}", exc_info=True)
            return Response(status_code=500, content=f"Directory listing failed: {str(e)}")

    async def _handle_file_download(self, driver, file_info: FileItem, request: Request) -> Response:
        try:
            etag = self._build_etag(file_info)
            last_modified = file_info.modified
            range_header = request.headers.get('Range')

            if not range_header:
                if self._match_if_none_match(request, etag) or self._match_if_modified_since(request, last_modified):
                    return self._build_not_modified_response(etag, last_modified)

            client_user_agent = request.headers.get('User-Agent', '')

            download = await resolve_download(
                driver,
                file_info.id,
                client_user_agent,
                file_info=file_info,
            )
            download_url = download.download_url
            
            if not download_url:
                return Response(status_code=500, content="Failed to get download URL")

            download_mode = get_effective_download_mode(driver, download)

            if range_header:
                if_range = (request.headers.get("If-Range") or "").strip()
                if if_range:
                    if if_range.startswith('"') and if_range != etag:
                        range_header = None
                    elif not if_range.startswith('"'):
                        try:
                            if_range_dt = parsedate_to_datetime(if_range)
                            if if_range_dt.tzinfo:
                                if_range_dt = if_range_dt.astimezone(tz=None).replace(tzinfo=None)
                            if last_modified and last_modified.replace(microsecond=0) > if_range_dt.replace(microsecond=0):
                                range_header = None
                        except Exception:
                            range_header = None

            if download_mode == 'redirect':
                if range_header:
                    return await self._handle_redirect_range_request(download_url, file_info, range_header)
                else:
                    return Response(
                        status_code=302,
                        headers={
                            'Location': download_url,
                            'Cache-Control': 'public, max-age=3600',
                            'ETag': etag,
                            'Last-Modified': self._format_http_datetime(last_modified),
                        }
                    )
            else:
                proxy_file_info, proxy_download_url, _ = build_proxy_file_info_from_download(file_info.id, download)
                if not proxy_file_info:
                    proxy_file_info = file_info
                return await serve_range_proxy(
                    driver=driver,
                    file_id=file_info.id,
                    file_info=proxy_file_info,
                    request=request,
                    initial_url=proxy_download_url or download_url,
                    upstream_headers_override=download.headers,
                )

        except Exception as e:
            webdav_logger.error(f"文件下载失败: {e}", exc_info=True)
            return Response(status_code=500, content="Download failed")

    async def _handle_head(self, request: Request, path: str) -> Response:
        try:
            account_name, file_path = parse_webdav_path(path)

            if not account_name:
                headers = {
                    'Content-Type': 'text/html; charset=utf-8',
                    'DAV': '1, 2'
                }
                return Response(status_code=200, headers=headers)

            context = await self._get_account_context_by_name(account_name)
            if not context:
                return Response(status_code=404, content="Account not found")
            driver = context["driver"]
            account_id = context["account_id"]

            if file_path in ("", "/"):
                file_info = FileItem(
                    id="0",
                    name=account_name,
                    path="/",
                    size=0,
                    is_dir=True,
                    modified=datetime.now(timezone.utc),
                    created=datetime.now(timezone.utc),
                )
            else:
                file_info = await self._resolve_path_to_file(driver, file_path, account_id=account_id, log_missing=False)
                if not file_info:
                    file_info = await driver.file_info(file_path)
            if not file_info:
                return Response(status_code=404, content="File not found")

            etag = self._build_etag(file_info)
            last_modified = file_info.modified
            if self._match_if_none_match(request, etag) or self._match_if_modified_since(request, last_modified):
                return self._build_not_modified_response(etag, last_modified)

            headers = {
                'Content-Length': str(file_info.size),
                'Last-Modified': self._format_http_datetime(last_modified),
                'ETag': etag,
                'Accept-Ranges': 'bytes'
            }
            
            if file_info.is_dir:
                headers['Content-Type'] = 'httpd/unix-directory'
            else:
                headers['Content-Type'] = get_mime_type(file_info.name)

            return Response(status_code=200, headers=headers)

        except Exception as e:
            webdav_logger.error(f"HEAD处理错误: {e}", exc_info=True)
            return Response(status_code=500, content="Internal Server Error")

    async def _handle_redirect_range_request(self, url: str, file_info: FileItem, range_header: str) -> Response:
        try:
            start, end = self._parse_single_range(range_header, int(file_info.size or 0))

            return Response(
                status_code=302,
                headers={
                    'Location': url,
                    'Accept-Ranges': 'bytes',
                    'Content-Range': f'bytes {start}-{end}/{file_info.size}',
                }
            )

        except Exception as e:
            webdav_logger.error(f"Range请求处理失败: {e}")
            total = int(file_info.size or 0)
            headers = {}
            if total > 0:
                headers["Content-Range"] = f"bytes */{total}"
            return Response(status_code=416, content="Invalid range request", headers=headers)

    async def _handle_put(self, request: Request, path: str) -> Response:
        """PUT：请求体先落临时文件再交给驱动上传，避免全部驻留内存。"""
        temp_path = ""
        try:
            account_name, file_path = parse_webdav_path(path)
            if not account_name:
                return Response(status_code=403, content="Cannot upload to WebDAV root")
            if file_path in ('', '/'):
                return Response(status_code=403, content="Cannot upload to account root directory")

            context = await self._get_account_context_by_name(account_name)
            if not context:
                return Response(status_code=404, content="Account not found")

            driver = context['driver']
            account_id = context['account_id']
            if not hasattr(driver, 'upload_local_file'):
                return Response(status_code=501, content="Upload is not supported by this driver")

            normalized_path = file_path.strip('/')
            path_parts = [part for part in normalized_path.split('/') if part]
            if not path_parts:
                return Response(status_code=400, content="Invalid file path")

            file_name = path_parts[-1]
            parent_path = "/".join(path_parts[:-1])
            parent_id = "0"

            if parent_path:
                parent_info = await self._resolve_path_to_file(driver, parent_path, account_id=account_id)
                if not parent_info:
                    return Response(status_code=409, content="Parent collection does not exist")
                if not parent_info.is_dir:
                    return Response(status_code=409, content="Parent path is not a collection")
                parent_id = str(parent_info.id)

            existing = await self._resolve_path_to_file(driver, file_path, account_id=account_id, log_missing=False)
            if existing and existing.is_dir:
                return Response(status_code=405, content="Cannot overwrite a collection with PUT")

            temp_path = await self._save_webdav_request_to_tempfile(request, file_name)

            token = current_account_id.set(account_id)
            try:
                result = await driver.upload_local_file(
                    temp_path,
                    file_name,
                    parent_id,
                    conflict_policy="overwrite",
                )
            finally:
                current_account_id.reset(token)

            if not result or not result.success:
                message = result.message if result else "Upload failed"
                webdav_logger.warning(f"WebDAV上传失败: {path} - {message}")
                return Response(status_code=409, content=message)

            if self.cache_cleaner:
                try:
                    await self.cache_cleaner.on_file_created(
                        account_id=account_id,
                        parent_id=parent_id,
                        file_name=file_name,
                    )
                except Exception as e:
                    webdav_logger.warning(f"清除上传缓存失败: {e}")

            return Response(status_code=204 if existing else 201)
        except Exception as e:
            webdav_logger.error(f"PUT处理错误: {e}", exc_info=True)
            return Response(status_code=500, content="Internal Server Error")
        finally:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass

    async def _handle_delete(self, request: Request, path: str) -> Response:
        try:
            account_name, file_path = parse_webdav_path(path)
            if not account_name:
                return Response(status_code=403, content="Cannot delete WebDAV root")
            if file_path in ('', '/'):
                return Response(status_code=403, content="Cannot delete account root directory")

            context = await self._get_account_context_by_name(account_name)
            if not context:
                return Response(status_code=404, content="Account not found")

            driver = context['driver']
            account_id = context['account_id']
            file_info = await self._resolve_path_to_file(driver, file_path, account_id=account_id)
            if not file_info:
                return Response(status_code=404, content="File not found")

            parent_id = None
            normalized_path = file_path.strip('/')
            path_parts = [part for part in normalized_path.split('/') if part]
            if len(path_parts) > 1:
                parent_path = "/".join(path_parts[:-1])
                parent_info = await self._resolve_path_to_file(driver, parent_path, account_id=account_id, log_missing=False)
                if parent_info and parent_info.is_dir:
                    parent_id = str(parent_info.id)
            if not parent_id:
                parent_id = "0"

            token = current_account_id.set(account_id)
            try:
                result = await driver.delete_file(file_info.id)
            finally:
                current_account_id.reset(token)

            if not result or not result.success:
                message = result.message if result else "Delete operation failed"
                webdav_logger.warning(f"WebDAV删除失败: {path} - {message}")
                return Response(status_code=409, content=message)

            if self.cache_cleaner:
                try:
                    await self.cache_cleaner.on_file_deleted(
                        account_id=account_id,
                        file_id=str(file_info.id),
                        parent_id=parent_id,
                    )
                except Exception as e:
                    webdav_logger.warning(f"清除删除缓存失败: {e}")

            return Response(status_code=204)
        except Exception as e:
            webdav_logger.error(f"DELETE处理错误: {e}", exc_info=True)
            return Response(status_code=500, content="Internal Server Error")

    async def _handle_mkcol(self, request: Request, path: str) -> Response:
        # rclone / 某些客户端会尝试创建以 . 开头的隐藏元数据目录，直接返回 201 避免报错
        if path.startswith('/.') or '.ADSPOWER_GLOBAL' in path:
            return Response(status_code=201, content="Directory created")

        try:
            account_name, file_path = parse_webdav_path(path)
            if not account_name:
                return Response(status_code=403, content="Cannot create directory at WebDAV root")
            if file_path in ('', '/'):
                return Response(status_code=403, content="Cannot create account root directory")

            context = await self._get_account_context_by_name(account_name)
            if not context:
                return Response(status_code=404, content="Account not found")

            driver = context['driver']
            account_id = context['account_id']
            normalized_path = file_path.strip('/')
            path_parts = [part for part in normalized_path.split('/') if part]
            if not path_parts:
                return Response(status_code=400, content="Invalid directory path")

            folder_name = path_parts[-1]
            parent_path = "/".join(path_parts[:-1])
            parent_id = "0"

            existing = await self._resolve_path_to_file(driver, file_path, account_id=account_id)
            if existing:
                return Response(status_code=405, content="Resource already exists")

            if parent_path:
                parent_info = await self._resolve_path_to_file(driver, parent_path, account_id=account_id)
                if not parent_info:
                    return Response(status_code=409, content="Parent collection does not exist")
                if not parent_info.is_dir:
                    return Response(status_code=409, content="Parent path is not a collection")
                parent_id = str(parent_info.id)

            token = current_account_id.set(account_id)
            try:
                result = await driver.create_folder(parent_id, folder_name)
            finally:
                current_account_id.reset(token)

            if not result or not result.success:
                message = result.message if result else "Create collection failed"
                webdav_logger.warning(f"WebDAV创建目录失败: {path} - {message}")
                return Response(status_code=409, content=message)

            if self.cache_cleaner:
                try:
                    await self.cache_cleaner.on_file_created(
                        account_id=account_id,
                        parent_id=parent_id,
                        file_name=folder_name,
                    )
                except Exception as e:
                    webdav_logger.warning(f"清除创建目录缓存失败: {e}")

            return Response(status_code=201, content="Collection created")
        except Exception as e:
            webdav_logger.error(f"MKCOL处理错误: {e}", exc_info=True)
            return Response(status_code=500, content="Internal Server Error")

    async def _handle_move(self, request: Request, path: str) -> Response:
        """MOVE：只支持同账号内的重命名/移动，跨账号直接 501。"""
        try:
            source_account_name, source_file_path = parse_webdav_path(path)
            if not source_account_name:
                return Response(status_code=403, content="Cannot move WebDAV root")
            if source_file_path in ('', '/'):
                return Response(status_code=403, content="Cannot move account root directory")

            destination = request.headers.get('Destination', '').strip()
            if not destination:
                return Response(status_code=400, content="Missing Destination header")

            overwrite = request.headers.get('Overwrite', 'T').strip().upper() != 'F'
            destination_path = unquote(destination)
            if '://' in destination_path:
                destination_path = destination_path.split('://', 1)[1]
                destination_path = destination_path[destination_path.find('/'):] if '/' in destination_path else '/'

            target_account_name, target_file_path = parse_webdav_path(destination_path)
            if not target_account_name:
                return Response(status_code=400, content="Invalid Destination path")
            if target_account_name != source_account_name:
                return Response(status_code=501, content="Cross-account MOVE is not supported")
            if target_file_path in ('', '/'):
                return Response(status_code=403, content="Cannot move to account root directory")

            context = await self._get_account_context_by_name(source_account_name)
            if not context:
                return Response(status_code=404, content="Account not found")

            driver = context['driver']
            account_id = context['account_id']
            source_info = await self._resolve_path_to_file(driver, source_file_path, account_id=account_id)
            if not source_info:
                return Response(status_code=404, content="Source not found")

            target_parts = [part for part in target_file_path.strip('/').split('/') if part]
            if not target_parts:
                return Response(status_code=400, content="Invalid Destination path")

            target_name = target_parts[-1]
            target_parent_path = "/".join(target_parts[:-1])
            target_parent_id = "0"
            if target_parent_path:
                target_parent_info = await self._resolve_path_to_file(driver, target_parent_path, account_id=account_id)
                if not target_parent_info:
                    return Response(status_code=409, content="Destination parent does not exist")
                if not target_parent_info.is_dir:
                    return Response(status_code=409, content="Destination parent is not a collection")
                target_parent_id = str(target_parent_info.id)

            source_parent_path = "/".join([part for part in source_file_path.strip('/').split('/')[:-1] if part])
            source_parent_id = "0"
            if source_parent_path:
                source_parent_info = await self._resolve_path_to_file(driver, source_parent_path, account_id=account_id)
                if not source_parent_info or not source_parent_info.is_dir:
                    return Response(status_code=409, content="Source parent collection does not exist")
                source_parent_id = str(source_parent_info.id)
            existing_target = await self._resolve_path_to_file(driver, target_file_path, account_id=account_id, log_missing=False)
            if existing_target:
                if str(existing_target.id) == str(source_info.id):
                    return Response(status_code=204)
                if not overwrite:
                    return Response(status_code=412, content="Destination already exists")
                delete_token = current_account_id.set(account_id)
                try:
                    delete_result = await driver.delete_file(existing_target.id)
                finally:
                    current_account_id.reset(delete_token)
                if not delete_result or not delete_result.success:
                    message = delete_result.message if delete_result else "Failed to overwrite destination"
                    return Response(status_code=409, content=message)

            token = current_account_id.set(account_id)
            try:
                if source_parent_id == target_parent_id:
                    if source_info.name == target_name:
                        return Response(status_code=204)
                    result = await driver.rename_file(source_info.id, target_name)
                else:
                    move_result = await driver.move_file([source_info.id], target_parent_id)
                    if not move_result or not move_result.success:
                        message = move_result.message if move_result else "Move operation failed"
                        webdav_logger.warning(f"WebDAV移动失败: {path} -> {destination_path} - {message}")
                        return Response(status_code=409, content=message)
                    if source_info.name != target_name:
                        result = await driver.rename_file(source_info.id, target_name)
                    else:
                        result = move_result
            finally:
                current_account_id.reset(token)

            if not result or not result.success:
                message = result.message if result else "Move operation failed"
                webdav_logger.warning(f"WebDAV MOVE失败: {path} -> {destination_path} - {message}")
                return Response(status_code=409, content=message)

            await self._cleanup_webdav_move_cache(
                account_id=account_id,
                file_id=str(source_info.id),
                source_parent_id=source_parent_id,
                target_parent_id=target_parent_id,
                source_name=source_info.name,
                target_name=target_name,
            )

            return Response(status_code=201)
        except Exception as e:
            webdav_logger.error(f"MOVE处理错误: {e}", exc_info=True)
            return Response(status_code=500, content="Internal Server Error")

    async def _handle_copy(self, request: Request, path: str) -> Response:
        # 大多数云盘驱动没有复制能力，统一返回 501
        return Response(status_code=501, content="Copy operation not supported")

    async def _cleanup_webdav_move_cache(
        self,
        account_id: str,
        file_id: str,
        source_parent_id: str,
        target_parent_id: str,
        source_name: str,
        target_name: str,
    ) -> None:
        if not self.cache_cleaner:
            return

        if source_parent_id == target_parent_id:
            await self.cache_cleaner.on_file_renamed(
                account_id=account_id,
                parent_id=source_parent_id,
                file_id=file_id,
                old_name=source_name,
                new_name=target_name,
            )
            return

        await self.cache_cleaner.on_file_moved(
            account_id=account_id,
            file_id=file_id,
            old_parent_id=source_parent_id,
            new_parent_id=target_parent_id,
        )

        if source_name != target_name:
            await self.cache_cleaner.on_file_renamed(
                account_id=account_id,
                parent_id=target_parent_id,
                file_id=file_id,
                old_name=source_name,
                new_name=target_name,
            )

    async def _save_webdav_request_to_tempfile(self, request: Request, file_name: str) -> str:
        upload_dir = os.path.join("data", "upload_tasks")
        os.makedirs(upload_dir, exist_ok=True)

        safe_name = os.path.basename(file_name) or "upload.bin"
        temp_name = f"webdav_{uuid.uuid4().hex}_{safe_name}"
        temp_path = os.path.join(upload_dir, temp_name)

        with open(temp_path, "wb") as handle:
            async for chunk in request.stream():
                if chunk:
                    handle.write(chunk)

        return temp_path

    async def _clear_account_path_mapping_cache(self, account_id: str) -> None:
        if not account_id or not self.cache_manager:
            return
        from cache.cache_keys import CacheKeyGenerator
        await self.cache_manager.clear_by_prefix(CacheKeyGenerator.path_mapping_prefix(account_id))

    async def _resolve_path_to_file(
        self,
        driver,
        file_path: str,
        account_id: Optional[str] = None,
        log_missing: bool = True,
        allow_path_cache_retry: bool = True,
    ) -> Optional[FileItem]:
        """路径 → FileItem；命中缓存但被判定陈旧（解析不到）时清缓存重试一次。"""
        try:
            if file_path == "/" or file_path == "":
                return None

            normalized_file_path = "/" + file_path.strip("/")
            normalized_cache_path = self._normalize_cache_path(normalized_file_path)
            path_cache_enabled = False
            if account_id and self.cache_manager:
                path_cache_enabled = await self._is_webdav_path_cache_enabled()
                if path_cache_enabled:
                    cached_item = await self.cache_manager.get_path_mapping_cache(account_id, normalized_cache_path)
                    if isinstance(cached_item, FileItem):
                        return cached_item

            async def retry_without_path_cache(reason: str) -> Optional[FileItem]:
                if not (allow_path_cache_retry and path_cache_enabled and account_id and self.cache_manager):
                    return None
                try:
                    webdav_logger.debug(f"WebDAV路径解析命中旧缓存，已清理路径缓存并重试: {reason} -> {normalized_cache_path}")
                    await self._clear_account_path_mapping_cache(str(account_id))
                except Exception as clear_error:
                    webdav_logger.warning(f"清理路径映射缓存失败，无法执行自动重试: {clear_error}")
                    return None
                return await self._resolve_path_to_file(
                    driver,
                    file_path,
                    account_id=account_id,
                    log_missing=log_missing,
                    allow_path_cache_retry=False,
                )

            path_parts = [p for p in normalized_file_path.split('/') if p]
            if not path_parts:
                return None

            current_id = "0"

            resolved_parts: List[str] = []
            for part in path_parts:
                files = await driver.list_files(current_id)

                found = None
                for file_item in files:
                    if file_item.name == part:
                        found = file_item
                        break

                if not found:
                    if log_missing:
                        webdav_logger.debug(f"路径解析失败: 在 {current_id} 中未找到 {part}")
                    retried_item = await retry_without_path_cache(f"目录 {current_id} 中未找到 {part}")
                    if retried_item is not None:
                        return retried_item
                    return None
                
                current_id = found.id
                resolved_parts.append(part)
                if path_cache_enabled and account_id and self.cache_manager:
                    try:
                        resolved_path = "/" + "/".join(resolved_parts)
                        await self.cache_manager.set_path_mapping_cache(account_id, resolved_path, found)
                    except Exception as e:
                        webdav_logger.warning(f"设置路径映射缓存失败: {e}")

                if part == path_parts[-1]:
                    if path_cache_enabled and account_id and self.cache_manager:
                        try:
                            await self.cache_manager.set_path_mapping_cache(account_id, normalized_cache_path, found)
                        except Exception as e:
                            webdav_logger.warning(f"设置最终路径映射缓存失败: {e}")
                    return found

                if not found.is_dir:
                    if log_missing:
                        webdav_logger.debug(f"路径解析失败: {part} 不是文件夹，但还有更多路径部分")
                    retried_item = await retry_without_path_cache(f"{part} 不是文件夹")
                    if retried_item is not None:
                        return retried_item
                    return None
            
            return None
            
        except Exception as e:
            webdav_logger.error(f"路径解析失败: {e}")
            retried_item = await retry_without_path_cache(f"路径解析异常: {e}")
            if retried_item is not None:
                return retried_item
            return None


webdav_server = None


async def get_webdav_server() -> Optional[FastAPIWebDAVServer]:
    """按需拿 WebDAV 服务器实例；开关关闭时会顺带把缓存一起清掉。"""
    global webdav_server

    try:
        webdav_enabled = config_manager.get('webdav_enabled')
        if not webdav_enabled:
            if webdav_server is not None:
                webdav_logger.info("WebDAV服务已禁用，清除服务器实例和缓存")
                webdav_server = None
                await clear_webdav_cache()
            return None
    except Exception as e:
        webdav_logger.error(f"检查WebDAV配置失败: {e}")
        return None

    if webdav_server is None:
        webdav_server = FastAPIWebDAVServer()

    return webdav_server


async def reset_webdav_server():
    global webdav_server
    webdav_server = None
    webdav_logger.info("WebDAV服务器实例已重置")


async def _clear_all_webdav_related_cache() -> int:
    cache_manager = get_global_cache()
    if not cache_manager:
        return 0

    from cache.cache_types import CacheConstants

    cleared_count = 0
    cleared_count += await cache_manager.clear_by_prefix(
        CacheConstants.KEY_PREFIX_PATH + CacheConstants.KEY_SEPARATOR
    )
    cleared_count += await cache_manager.clear_by_prefix(
        CacheConstants.KEY_PREFIX_WEBDAV + CacheConstants.KEY_SEPARATOR
    )
    return int(cleared_count)


async def clear_webdav_cache():
    try:
        cleared_count = await _clear_all_webdav_related_cache()
        webdav_logger.info(f"WebDAV缓存已清除，累计清理 {cleared_count} 项")
    except Exception as e:
        webdav_logger.error(f"清除WebDAV缓存失败: {e}")
