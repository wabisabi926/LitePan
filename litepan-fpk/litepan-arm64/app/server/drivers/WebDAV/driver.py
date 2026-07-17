"""WebDAV 驱动：PROPFIND 列目录 + MKCOL/DELETE/MOVE/PUT 写操作 + 直链/代理下载 + 流式上传。"""

from __future__ import annotations

import asyncio
import base64
import os
import random
import ssl
import tempfile
import unicodedata
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple
from urllib.parse import quote, unquote, urljoin, urlparse
import xml.etree.ElementTree as ET

import aiohttp
from aiohttp import BasicAuth
from aiohttp.payload import AsyncIterablePayload
from fastapi import UploadFile

from core.base import DriverInfo, FileItem, OperationResult
from core.driver_base import BaseDriver
from core.log_manager import LogModule, get_writer
from core.operation_wrapper import (
    auto_cleanup_cache,
    with_file_info_cache,
    with_file_list_cache,
)

from .config import WebdavConfig

DAV = "DAV:"


_URL_PATH_SAFE = "-._~!$&'()*+,;=:@"

# 幂等方法：遇到 429/5xx/网络异常可以重试
_IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "PROPFIND", "OPTIONS"})


def _dav(tag: str) -> str:
    return f"{{{DAV}}}{tag}"


def _local_tag(tag: str) -> str:
    if tag.startswith("{"):
        return tag.split("}", 1)[-1]
    return tag


def _nfc(value: str) -> str:
    """对名字做 Unicode NFC 归一化，规避 macOS / 某些上游返回 NFD 的差异。"""
    if not value:
        return value or ""
    try:
        return unicodedata.normalize("NFC", value)
    except Exception:
        return value


def _safe_unquote(value: str) -> str:
    """反解 href：兼容 `+` 表示空格、`%2520` 这种二次编码。"""
    if not value:
        return value or ""
    raw = value
    raw = raw.replace("+", " ")
    decoded = unquote(raw)
    if "%" in decoded and decoded != value:
        try:
            again = unquote(decoded)
            if again != decoded:
                decoded = again
        except Exception:
            pass
    return _nfc(decoded)


class _SizedAsyncIterablePayload(AsyncIterablePayload):
    def __init__(self, value, *, size: int, **kwargs) -> None:
        super().__init__(value, **kwargs)
        self._size = int(size)


class WebdavDriver(BaseDriver):
    _UPLOAD_CHUNK_SIZE: int = 4 * 1024 * 1024
    _PROGRESS_MIN_INTERVAL: float = 0.2

    def __init__(self, config: WebdavConfig):
        super().__init__(config)
        self.config: WebdavConfig = config
        self._log = get_writer(LogModule.DRIVER)
        self._session: Optional[aiohttp.ClientSession] = None
        self._root_collection_url: str = ""
        self._account_root_path: str = ""
        # 并发闸门按账号一份；init() 创建 session 时一并初始化，便于在 close() 中清掉
        self._concurrency_sem: Optional[asyncio.Semaphore] = None
        # redirect 模式：当某个 file_id 跟随到匿名公网直链时，缓存它的有效期；
        # get_download_headers 据此判断要不要清 Authorization
        self._anon_redirect_until: Dict[str, float] = {}

    @classmethod
    def get_info(cls) -> DriverInfo:
        return DriverInfo(
            name="webdav",
            display_name="WebDAV",
            version="0.1.0",
            capabilities=[
                "list",
                "info",
                "download",
                "create_folder",
                "delete",
                "batch_delete",
                "rename",
                "move",
                "copy",
                "upload",
            ],
            description="将远端 WebDAV 挂载为存储（浏览、下载、上传、目录与改名/移动/删除）",
            author="LitePan",
        )

    async def init(self) -> None:
        self._root_collection_url = self._build_root_collection_url(self.config)
        parsed = urlparse(self._root_collection_url)
        path = _safe_unquote(parsed.path or "/")
        self._account_root_path = path.rstrip("/") or "/"
        # 同账号下用一个并发闸门，避免播放器探测/STRM 扫描时同时把上游打爆
        max_conc = int(getattr(self.config, "max_concurrency", 4) or 4)
        if max_conc < 1:
            max_conc = 1
        self._concurrency_sem = asyncio.Semaphore(max_conc)
        self._log.debug(
            f"WebDAV 初始化: root={self._root_collection_url} max_conc={max_conc}",
            driver_name="webdav",
        )

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
        self._concurrency_sem = None
        self._log.debug("WebDAV 驱动已关闭", driver_name="webdav")

    async def test_connection(self) -> OperationResult:
        try:
            await self._propfind_raw(self._root_collection_url, depth="0")
            return OperationResult(success=True, message="连接成功")
        except Exception as e:
            msg = str(e)
            self._log.error(f"WebDAV 连接测试失败: {msg}", driver_name="webdav")
            return OperationResult(success=False, message=f"连接失败: {msg}")

    @staticmethod
    def _build_root_collection_url(cfg: WebdavConfig) -> str:
        raw = (cfg.base_url or "").strip().rstrip("/")
        root_piece = (cfg.root_path or "").strip().strip("/")
        if root_piece:
            joined = urljoin(raw + "/", root_piece + "/")
        else:
            joined = raw + "/"
        if not joined.endswith("/"):
            joined += "/"
        return joined

    def _normalize_parent_rel(self, parent_id: Optional[str]) -> str:
        if parent_id is None:
            return ""
        p = str(parent_id).strip()
        if p in ("0", "/", ""):
            return ""
        return p.strip("/")

    @staticmethod
    def _encode_rel_segments(rel: str) -> str:
        parts = [p for p in (rel or "").split("/") if p]
        return "/".join(quote(p, safe=_URL_PATH_SAFE) for p in parts)

    def _collection_url_for_parent(self, parent_rel: str) -> str:
        if not parent_rel:
            return self._root_collection_url
        base = self._root_collection_url.rstrip("/")
        enc = self._encode_rel_segments(parent_rel)
        return f"{base}/{enc}/"

    def _resource_url_for_rel(self, rel: str, *, is_dir: bool) -> str:
        rel = (rel or "").strip().strip("/")
        base = self._root_collection_url.rstrip("/")
        if not rel:
            url = self._root_collection_url
        else:
            enc = self._encode_rel_segments(rel)
            url = f"{base}/{enc}"
        if is_dir and not url.endswith("/"):
            url += "/"
        return url

    def _href_to_path(self, href: str) -> str:
        href = (href or "").strip()
        if href.startswith("http://") or href.startswith("https://"):
            return _safe_unquote(urlparse(href).path or "/")
        if href.startswith("/"):
            return _safe_unquote(href)
        return _safe_unquote(urlparse(urljoin(self._root_collection_url, href)).path or "/")

    def _rel_path_from_account_root(self, path: str) -> str:
        p = _nfc((path or "").rstrip("/"))
        root = _nfc(self._account_root_path.rstrip("/"))
        if root == "/" or root == "":
            rel = p.lstrip("/")
        else:
            if not p.startswith(root):
                p2 = "/" + p.lstrip("/")
                if not p2.startswith(root):
                    return ""
                p = p2
            rel = p[len(root) :].lstrip("/")
        return rel.strip("/")

    def _is_direct_child(self, parent_rel: str, child_rel: str) -> bool:
        parent_rel = _nfc((parent_rel or "").strip().strip("/"))
        child_rel = _nfc((child_rel or "").strip().strip("/"))
        if not child_rel:
            return False
        if not parent_rel:
            return "/" not in child_rel
        if child_rel == parent_rel:
            return False
        if not (child_rel.startswith(parent_rel + "/")):
            return False
        suffix = child_rel[len(parent_rel) + 1 :]
        return suffix != "" and "/" not in suffix

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            if self.config.verify_ssl:
                ssl_param: Any = None
            else:
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                ssl_param = ctx
            # 加上连接池上限，避免长时间持有连接导致后续 PROPFIND 起不来
            connector = aiohttp.TCPConnector(
                ssl=ssl_param,
                limit=32,
                limit_per_host=16,
                keepalive_timeout=30,
                enable_cleanup_closed=True,
            )
            # 这里给 session 一个保底的总超时（按上传超时来），具体每次请求会再传 timeout 覆盖
            base_total = int(self.config.timeout_seconds or 60)
            timeout = aiohttp.ClientTimeout(total=base_total, sock_connect=15)
            self._session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        return self._session

    def _propfind_timeout(self) -> aiohttp.ClientTimeout:

        seconds = int(getattr(self.config, "timeout_seconds", 60) or 60)
        return aiohttp.ClientTimeout(total=seconds, sock_connect=10, sock_read=seconds)

    def _read_timeout(self) -> aiohttp.ClientTimeout:
        return aiohttp.ClientTimeout(total=None, sock_connect=15, sock_read=60)

    def _upload_timeout(self) -> aiohttp.ClientTimeout:
        return aiohttp.ClientTimeout(total=None, sock_connect=15, sock_read=120)

    async def _acquire_concurrency(self) -> None:
        sem = self._concurrency_sem
        if sem is not None:
            await sem.acquire()

    def _release_concurrency(self) -> None:
        sem = self._concurrency_sem
        if sem is not None:
            try:
                sem.release()
            except ValueError:
                pass

    @staticmethod
    def _retry_after_seconds(resp: aiohttp.ClientResponse) -> float:
        raw = resp.headers.get("Retry-After") if resp is not None else None
        if not raw:
            return 0.0
        raw = raw.strip()
        if raw.isdigit():
            try:
                return max(0.0, float(int(raw)))
            except ValueError:
                return 0.0
        try:
            target = parsedate_to_datetime(raw)
            now = datetime.utcnow().replace(tzinfo=target.tzinfo) if target.tzinfo else datetime.utcnow()
            delta = (target - now).total_seconds()
            return max(0.0, float(delta))
        except (TypeError, ValueError, OverflowError):
            return 0.0

    def _basic_auth(self) -> Optional[BasicAuth]:
        u = (self.config.username or "").strip()
        p = (self.config.password or "").strip()
        if not u:
            return None
        return BasicAuth(u, p)

    def _authorization_header_dict(self) -> Dict[str, str]:
        u = (self.config.username or "").strip()
        if not u:
            return {}
        p = (self.config.password or "").strip()
        token = base64.b64encode(f"{u}:{p}".encode("utf-8")).decode("ascii")
        return {"Authorization": f"Basic {token}"}

    @staticmethod
    def _parent_rel_of(rel: str) -> str:
        rel = (rel or "").strip().strip("/")
        if not rel or "/" not in rel:
            return ""
        return rel.rsplit("/", 1)[0]

    @staticmethod
    def _to_api_parent_id(parent_rel: str) -> str:
        return "0" if not (parent_rel or "").strip().strip("/") else parent_rel.strip().strip("/")

    async def _maybe_operation_delay(self) -> None:
        delay_ms = int(getattr(self.config, "operation_delay", 0) or 0)
        if delay_ms > 0:
            await asyncio.sleep(delay_ms / 1000.0)

    async def _request_once(
        self,
        method: str,
        url: str,
        *,
        headers: Optional[Dict[str, str]],
        data: Optional[Any],
        timeout: aiohttp.ClientTimeout,
        allow_redirects: bool,
        read_body: bool = True,
    ) -> Tuple[int, str, float]:
        session = await self._get_session()
        auth = self._basic_auth()
        async with session.request(
            method,
            url,
            headers=headers or None,
            data=data,
            auth=auth,
            allow_redirects=allow_redirects,
            timeout=timeout,
        ) as resp:
            body_text = await resp.text() if read_body else ""
            retry_after = self._retry_after_seconds(resp)
            return resp.status, body_text, retry_after

    async def _request_with_retry(
        self,
        method: str,
        url: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        data: Optional[Any] = None,
        timeout: Optional[aiohttp.ClientTimeout] = None,
        allow_redirects: bool = True,
        ok_status: Tuple[int, ...] = (200, 201, 204, 207),
        read_body: bool = True,
    ) -> Tuple[int, str]:
        method_upper = method.upper()
        max_retries = int(getattr(self.config, "max_retries", 2) or 0)
        retriable = method_upper in _IDEMPOTENT_METHODS

        last_exc: Optional[BaseException] = None
        last_status: Optional[int] = None
        last_body: str = ""
        attempt = 0
        while True:
            attempt += 1
            await self.wait_for_request_interval()
            await self._acquire_concurrency()
            try:
                status, body, retry_after = await self._request_once(
                    method_upper,
                    url,
                    headers=headers,
                    data=data,
                    timeout=timeout or aiohttp.ClientTimeout(total=int(self.config.timeout_seconds or 60)),
                    allow_redirects=allow_redirects,
                    read_body=read_body,
                )
                last_status, last_body = status, body
                if status in ok_status:
                    return status, body
                # 鉴权失败立即抛
                if status == 401:
                    raise PermissionError("WebDAV 返回 401，请检查用户名与密码")
                if status == 403:
                    raise PermissionError(f"WebDAV 返回 403: {(body or '')[:400]}")
                # 仅对幂等请求重试 429/5xx；总尝试次数 = 1 + max_retries
                if retriable and (status == 429 or 500 <= status <= 599) and attempt <= max_retries:
                    sleep = retry_after if retry_after > 0 else min(2.0 ** (attempt - 1) * 0.5, 5.0)
                    sleep += random.uniform(0, 0.3)
                    self._log.debug(
                        f"WebDAV {method_upper} {url} 收到 {status}，{sleep:.2f}s 后重试（第 {attempt}/{max_retries + 1} 次）",
                        driver_name="webdav",
                    )
                    await asyncio.sleep(sleep)
                    continue
                snippet = (body or "")[:400]
                raise RuntimeError(f"WebDAV {method_upper} 失败 HTTP {status}: {snippet}")
            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                last_exc = e
                if retriable and attempt <= max_retries:
                    sleep = min(2.0 ** (attempt - 1) * 0.5, 5.0) + random.uniform(0, 0.3)
                    self._log.debug(
                        f"WebDAV {method_upper} {url} 网络异常 {type(e).__name__}: {e}，{sleep:.2f}s 后重试",
                        driver_name="webdav",
                    )
                    await asyncio.sleep(sleep)
                    continue
                raise
            finally:
                self._release_concurrency()
        # 理论上到不了这里
        if last_exc:
            raise last_exc
        raise RuntimeError(f"WebDAV {method_upper} 失败 HTTP {last_status}: {(last_body or '')[:400]}")

    async def _simple_request(
        self,
        method: str,
        url: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        data: Optional[Any] = None,
        ok_status: tuple = (200, 201, 204),
        allow_redirects: bool = False,
        timeout: Optional[aiohttp.ClientTimeout] = None,
    ) -> None:
        await self._request_with_retry(
            method,
            url,
            headers=headers,
            data=data,
            timeout=timeout or self._propfind_timeout(),
            allow_redirects=allow_redirects,
            ok_status=tuple(ok_status),
        )

    async def _resource_exists(self, rel: str, *, is_dir: bool = False) -> bool:
        try:
            await self._propfind_raw(self._resource_url_for_rel(rel, is_dir=is_dir), depth="0")
            return True
        except PermissionError:
            # 401/403 不代表不存在，需要把错误透出去
            raise
        except Exception:
            pass
        try:
            await self._propfind_raw(self._resource_url_for_rel(rel, is_dir=not is_dir), depth="0")
            return True
        except Exception:
            return False

    async def _follow_redirect_url(self, url: str) -> Optional[str]:
        await self.wait_for_request_interval()
        session = await self._get_session()
        auth = self._basic_auth()

        final_url: Optional[str] = None
        try:
            await self._acquire_concurrency()
            try:
                async with session.request(
                    "HEAD",
                    url,
                    auth=auth,
                    allow_redirects=True,
                    timeout=self._read_timeout(),
                ) as resp:
                    if 200 <= resp.status < 400 and str(resp.url) != url:
                        final_url = str(resp.url)
            except Exception:
                pass
            if not final_url:
                try:
                    async with session.request(
                        "GET",
                        url,
                        auth=auth,
                        allow_redirects=True,
                        timeout=self._read_timeout(),
                        headers={"Range": "bytes=0-0"},
                    ) as resp:
                        if 200 <= resp.status < 400 and str(resp.url) != url:
                            final_url = str(resp.url)
                except Exception:
                    pass
        finally:
            self._release_concurrency()

        if not final_url:
            return None

        try:
            await self._acquire_concurrency()
            try:
                async with session.request(
                    "GET",
                    final_url,
                    auth=None,
                    allow_redirects=True,
                    timeout=self._read_timeout(),
                    headers={"Range": "bytes=0-0"},
                ) as resp:
                    if 200 <= resp.status < 400:
                        return final_url
            except Exception:
                return None
        finally:
            self._release_concurrency()
        return None

    async def _propfind_raw(self, url: str, *, depth: str) -> str:
        headers = {
            "Depth": depth,
            "Content-Type": "application/xml; charset=utf-8",
        }
        body = (
            '<?xml version="1.0" encoding="utf-8"?>'
            '<propfind xmlns="DAV:"><prop>'
            "<resourcetype/>"
            "<getcontentlength/>"
            "<getlastmodified/>"
            "<displayname/>"
            "</prop></propfind>"
        )
        try:
            _, text = await self._request_with_retry(
                "PROPFIND",
                url,
                headers=headers,
                data=body.encode("utf-8"),
                timeout=self._propfind_timeout(),
                allow_redirects=True,
                ok_status=(200, 207),
            )
            return text
        except PermissionError:
            raise
        except RuntimeError as e:
            msg = str(e)
            if "HTTP " in msg and "失败" in msg:
                raise
            raise RuntimeError(f"WebDAV PROPFIND 失败: {msg}") from e

    def _parse_propfind_response(
        self,
        xml_text: str,
        *,
        parent_rel: str,
        collection_url: str,
    ) -> List[FileItem]:
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            raise RuntimeError(f"无法解析 WebDAV 响应 XML: {e}") from e

        collection_path = self._href_to_path(collection_url).rstrip("/")
        items: List[FileItem] = []

        for response in root.iter():
            if _local_tag(response.tag) != "response":
                continue
            href_el = None
            prop_el = None
            for child in list(response):
                ln = _local_tag(child.tag)
                if ln == "href":
                    href_el = child
                elif ln == "propstat":
                    st = child.find(_dav("status"))
                    st_text = (st.text or "").upper() if st is not None else ""
                    if "200" in st_text or "OK" in st_text:
                        p = child.find(_dav("prop"))
                        if p is not None:
                            prop_el = p
                            break
            if href_el is None or prop_el is None:
                continue
            href_raw = (href_el.text or "").strip()
            path = self._href_to_path(href_raw).rstrip("/")
            if collection_path and (path == collection_path or path.rstrip("/") == collection_path):
                continue
            rel = self._rel_path_from_account_root(path)
            if rel == (parent_rel or "").strip().strip("/"):
                continue
            if not self._is_direct_child(parent_rel, rel):
                continue

            is_dir = False
            rt = prop_el.find(_dav("resourcetype"))
            if rt is not None and rt.find(_dav("collection")) is not None:
                is_dir = True

            display = prop_el.find(_dav("displayname"))
            name = _nfc((display.text or "").strip()) if display is not None else ""
            if not name:
                name = rel.split("/")[-1] or rel or "unknown"

            size = 0
            cl = prop_el.find(_dav("getcontentlength"))
            if cl is not None and (cl.text or "").strip().isdigit():
                size = int(cl.text.strip())

            modified: Optional[datetime] = None
            lm = prop_el.find(_dav("getlastmodified"))
            if lm is not None and (lm.text or "").strip():
                try:
                    modified = parsedate_to_datetime(lm.text.strip())
                except (TypeError, ValueError, OverflowError):
                    modified = None

            parent_api = self._to_api_parent_id(parent_rel)
            items.append(
                FileItem(
                    id=rel,
                    name=name,
                    path=f"/{rel}" if rel else "/",
                    size=0 if is_dir else size,
                    is_dir=is_dir,
                    modified=modified,
                    extra={"webdav_href": href_raw, "parent_id": parent_api},
                )
            )

        items.sort(key=lambda x: (not x.is_dir, x.name.lower()))
        return items

    def _parse_single_resource(self, xml_text: str, rel: str) -> Optional[FileItem]:
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            return None
        for response in root.iter():
            if _local_tag(response.tag) != "response":
                continue
            prop_el = None
            href_el = None
            for child in list(response):
                ln = _local_tag(child.tag)
                if ln == "href":
                    href_el = child
                elif ln == "propstat":
                    st = child.find(_dav("status"))
                    st_text = (st.text or "").upper() if st is not None else ""
                    if "200" in st_text or "OK" in st_text:
                        p = child.find(_dav("prop"))
                        if p is not None:
                            prop_el = p
                            break
            if href_el is None or prop_el is None:
                continue
            href_raw = (href_el.text or "").strip()
            path = self._href_to_path(href_raw)
            item_rel = self._rel_path_from_account_root(path.rstrip("/"))
            if _nfc(item_rel) != _nfc(rel.strip().strip("/")):
                continue

            is_dir = False
            rt = prop_el.find(_dav("resourcetype"))
            if rt is not None and rt.find(_dav("collection")) is not None:
                is_dir = True

            display = prop_el.find(_dav("displayname"))
            name = _nfc((display.text or "").strip()) if display is not None else ""
            if not name:
                name = rel.split("/")[-1] or rel

            size = 0
            cl = prop_el.find(_dav("getcontentlength"))
            if cl is not None and (cl.text or "").strip().isdigit():
                size = int(cl.text.strip())

            modified = None
            lm = prop_el.find(_dav("getlastmodified"))
            if lm is not None and (lm.text or "").strip():
                try:
                    modified = parsedate_to_datetime(lm.text.strip())
                except (TypeError, ValueError, OverflowError):
                    modified = None

            parent_api = self._to_api_parent_id(self._parent_rel_of(item_rel))
            return FileItem(
                id=item_rel,
                name=name,
                path=f"/{item_rel}" if item_rel else "/",
                size=0 if is_dir else size,
                is_dir=is_dir,
                modified=modified,
                extra={"webdav_href": href_raw, "parent_id": parent_api},
            )
        return None

    @with_file_list_cache
    async def list_files(self, parent_id: str = "0") -> List[FileItem]:
        parent_rel = self._normalize_parent_rel(parent_id)
        url = self._collection_url_for_parent(parent_rel)
        xml_text = await self._propfind_raw(url, depth="1")
        return self._parse_propfind_response(xml_text, parent_rel=parent_rel, collection_url=url)

    @with_file_info_cache
    async def file_info(self, file_id: str) -> Optional[FileItem]:
        rel = self._normalize_parent_rel(file_id)
        if rel == "":
            return None
        try:
            xml_file = await self._propfind_raw(
                self._resource_url_for_rel(rel, is_dir=False), depth="0"
            )
            hit = self._parse_single_resource(xml_file, rel)
            if hit:
                return hit
        except PermissionError:
            raise
        except Exception:
            pass
        try:
            xml_try_dir = await self._propfind_raw(
                self._resource_url_for_rel(rel, is_dir=True), depth="0"
            )
            return self._parse_single_resource(xml_try_dir, rel)
        except PermissionError:
            raise
        except Exception as e:
            self._log.debug(f"WebDAV file_info 失败: {rel} {e}", driver_name="webdav")
            return None

    @staticmethod
    def _validate_entry_name(name: str) -> Optional[str]:
        n = (name or "").strip()
        if not n:
            return None
        if "/" in n or "\\" in n or n in (".", ".."):
            return None
        return n

    def _is_virtual_root_id(self, file_id: Optional[str]) -> bool:
        p = (file_id or "").strip()
        return p in ("", "0", "/")

    @auto_cleanup_cache("create_folder")
    async def create_folder(self, parent_id: str, name: str) -> OperationResult:
        try:
            folder_name = self._validate_entry_name(name)
            if not folder_name:
                return OperationResult(success=False, message="文件夹名称无效")
            parent_rel = self._normalize_parent_rel(parent_id)
            child_rel = f"{parent_rel}/{folder_name}" if parent_rel else folder_name
            url = self._resource_url_for_rel(child_rel, is_dir=True)
            try:
                await self._simple_request("MKCOL", url, ok_status=(200, 201, 204))
            except RuntimeError as e:
                msg = str(e)
                if "HTTP 405" in msg or "HTTP 409" in msg:
                    return OperationResult(
                        success=False,
                        message=f"无法创建文件夹（可能已存在）: {folder_name}",
                    )
                raise
            await self._maybe_operation_delay()
            return OperationResult(
                success=True,
                message=f"文件夹 '{folder_name}' 创建成功",
                data={
                    "folder_id": child_rel,
                    "parent_path": self._to_api_parent_id(parent_rel),
                    "folder_name": folder_name,
                },
            )
        except Exception as e:
            self._log.error(f"WebDAV 新建文件夹失败: {e}", driver_name="webdav")
            return OperationResult(success=False, message=f"新建文件夹失败: {str(e)}")

    @auto_cleanup_cache("delete_file")
    async def delete_file(self, file_id: str) -> OperationResult:
        return await self._delete_files([file_id])

    @auto_cleanup_cache("batch_delete_file")
    async def batch_delete_file(self, file_ids: List[str]) -> OperationResult:
        if not file_ids:
            return OperationResult(success=True, message="没有文件需要删除")
        return await self._delete_files(file_ids)

    async def _delete_files(self, file_ids: List[str]) -> OperationResult:
        try:
            parent_ids: set[str] = set()
            prepared: List[tuple[str, bool]] = []
            for fid in file_ids:
                if self._is_virtual_root_id(fid):
                    return OperationResult(success=False, message="不能删除存储根目录")
                info = await self.file_info(fid)
                if not info:
                    return OperationResult(success=False, message=f"找不到文件: {fid}")
                if info.extra and info.extra.get("parent_id") is not None:
                    parent_ids.add(str(info.extra["parent_id"]))
                prepared.append((fid, info.is_dir))
            for fid, is_dir in prepared:
                url = self._resource_url_for_rel(fid, is_dir=is_dir)
                await self._simple_request("DELETE", url, ok_status=(200, 204))
            await self._maybe_operation_delay()
            return OperationResult(
                success=True,
                message=f"已删除 {len(file_ids)} 项",
                data={
                    "deleted_count": len(file_ids),
                    "file_ids": file_ids,
                    "parent_ids": list(parent_ids),
                },
            )
        except Exception as e:
            self._log.error(f"WebDAV 删除失败: {e}", driver_name="webdav")
            return OperationResult(success=False, message=f"删除失败: {str(e)}")

    @auto_cleanup_cache("rename_file")
    async def rename_file(self, file_id: str, new_name: str) -> OperationResult:
        try:
            if self._is_virtual_root_id(file_id):
                return OperationResult(success=False, message="不能重命名根目录")
            new_base = self._validate_entry_name(new_name)
            if not new_base:
                return OperationResult(success=False, message="新名称无效")
            info = await self.file_info(file_id)
            if not info:
                return OperationResult(success=False, message="找不到要重命名的文件")
            old_name = info.name
            parent_rel = self._parent_rel_of(file_id)
            dest_rel = f"{parent_rel}/{new_base}" if parent_rel else new_base
            if dest_rel.strip("/") == file_id.strip("/"):
                return OperationResult(
                    success=True,
                    message="名称未变化",
                    data={
                        "file_id": file_id,
                        "new_name": new_base,
                        "old_name": old_name,
                        "parent_id": self._to_api_parent_id(parent_rel),
                    },
                )
            src_url = self._resource_url_for_rel(file_id, is_dir=info.is_dir)
            dest_url = self._resource_url_for_rel(dest_rel, is_dir=info.is_dir)
            await self._simple_request(
                "MOVE",
                src_url,
                headers={"Destination": dest_url, "Overwrite": "T"},
                ok_status=(200, 201, 204),
            )
            await self._maybe_operation_delay()
            return OperationResult(
                success=True,
                message=f"已重命名为「{new_base}」",
                data={
                    "file_id": dest_rel,
                    "new_name": new_base,
                    "old_name": old_name,
                    "parent_id": self._to_api_parent_id(parent_rel),
                },
            )
        except Exception as e:
            self._log.error(f"WebDAV 重命名失败: {e}", driver_name="webdav")
            return OperationResult(success=False, message=f"重命名失败: {str(e)}")

    @auto_cleanup_cache("move_file")
    async def move_file(self, file_ids: List[str], target_parent_id: str) -> OperationResult:
        try:
            if not file_ids:
                return OperationResult(success=True, message="没有文件需要移动")
            target_rel = self._normalize_parent_rel(target_parent_id)
            source_parents: set[str] = set()
            for fid in file_ids:
                if self._is_virtual_root_id(fid):
                    return OperationResult(success=False, message="不能移动根目录")
                info = await self.file_info(fid)
                if not info:
                    return OperationResult(success=False, message=f"找不到文件: {fid}")
                if info.extra and info.extra.get("parent_id") is not None:
                    source_parents.add(str(info.extra.get("parent_id")))
                base = fid.split("/")[-1] if "/" in fid else fid
                dest_rel = f"{target_rel}/{base}" if target_rel else base
                if dest_rel.strip("/") == fid.strip("/"):
                    continue
                src_url = self._resource_url_for_rel(fid, is_dir=info.is_dir)
                dest_url = self._resource_url_for_rel(dest_rel, is_dir=info.is_dir)
                await self._simple_request(
                    "MOVE",
                    src_url,
                    headers={"Destination": dest_url, "Overwrite": "T"},
                    ok_status=(200, 201, 204),
                )
            await self._maybe_operation_delay()
            return OperationResult(
                success=True,
                message=f"已移动 {len(file_ids)} 项",
                data={
                    "moved_count": len(file_ids),
                    "file_ids": file_ids,
                    "target_parent_id": self._to_api_parent_id(target_rel),
                    "source_parent_ids": list(source_parents),
                },
            )
        except Exception as e:
            self._log.error(f"WebDAV 移动失败: {e}", driver_name="webdav")
            return OperationResult(success=False, message=f"移动失败: {str(e)}")

    @auto_cleanup_cache("copy_file")
    async def copy_file(self, file_ids: List[str], target_parent_id: str, source_parent_id: str = None) -> OperationResult:
        try:
            if not file_ids:
                return OperationResult(success=True, message="没有文件需要复制")
            target_rel = self._normalize_parent_rel(target_parent_id)
            for fid in file_ids:
                if self._is_virtual_root_id(fid):
                    return OperationResult(success=False, message="不能复制根目录")
                info = await self.file_info(fid)
                if not info:
                    return OperationResult(success=False, message=f"找不到文件: {fid}")
                base = fid.split("/")[-1] if "/" in fid else fid
                dest_rel = f"{target_rel}/{base}" if target_rel else base
                if dest_rel.strip("/") == fid.strip("/"):
                    return OperationResult(
                        success=False,
                        message="WebDAV 不支持复制到同一目录（路径冲突）",
                        data={"warning": True},
                    )
                src_url = self._resource_url_for_rel(fid, is_dir=info.is_dir)
                dest_url = self._resource_url_for_rel(dest_rel, is_dir=info.is_dir)
                await self._simple_request(
                    "COPY",
                    src_url,
                    headers={"Destination": dest_url, "Overwrite": "T"},
                    ok_status=(200, 201, 204),
                )
            await self._maybe_operation_delay()
            return OperationResult(
                success=True,
                message=f"已复制 {len(file_ids)} 项",
                data={
                    "copied_count": len(file_ids),
                    "file_ids": file_ids,
                    "target_parent_id": self._to_api_parent_id(target_rel),
                    "source_parent_ids": [source_parent_id] if source_parent_id else [],
                },
            )
        except Exception as e:
            self._log.error(f"WebDAV 复制失败: {e}", driver_name="webdav")
            return OperationResult(success=False, message=f"复制失败: {str(e)}")

    async def batch_copy_file(self, file_ids: List[str], target_parent_id: str) -> OperationResult:
        return await self.copy_file(file_ids, target_parent_id)

    async def get_download_url(self, file_id: str, user_agent: str = "") -> str:
        rel = self._normalize_parent_rel(file_id)
        if not rel:
            raise ValueError("无效的下载路径")
        url = self._resource_url_for_rel(rel, is_dir=False)
        if self.config.download_mode != "redirect":
            self._anon_redirect_until.pop(file_id, None)
            return url
        # redirect 模式：尝试跟随 302 到无需鉴权的最终直链；拿不到时自动降级为 proxy
        final = await self._follow_redirect_url(url)
        if final:
            # 缓存 60s 内，get_download_headers 不带 Authorization；之后过期重测
            self._anon_redirect_until[file_id] = asyncio.get_event_loop().time() + 60.0
            return final
        if self._basic_auth() is not None:
            self._log.warning(
                "WebDAV redirect 模式未拿到匿名直链，本次按 proxy 模式回源；"
                "若长期如此，建议把账号下载策略改为 proxy",
                driver_name="webdav",
            )
        self._anon_redirect_until.pop(file_id, None)
        return url

    async def get_download_headers(self, file_id: str, user_agent: str = "") -> Dict[str, str]:
        # redirect 模式拿到匿名直链时不能带 Authorization 头（对象存储会拒绝），统一返回空头
        expires_at = self._anon_redirect_until.get(file_id)
        if expires_at and expires_at > asyncio.get_event_loop().time():
            return {}
        if expires_at:
            self._anon_redirect_until.pop(file_id, None)
        return self._authorization_header_dict()

    async def get_download_info(self, file_id: str, user_agent: str = "") -> Dict[str, Any]:
        info = await self.file_info(file_id)
        if not info or info.is_dir:
            raise ValueError("只能下载文件")
        upstream_url = self._resource_url_for_rel(self._normalize_parent_rel(file_id), is_dir=False)
        url = await self.get_download_url(file_id, user_agent)
        configured_mode = (self.config.download_mode or "proxy").strip() or "proxy"
        auth_headers = self._authorization_header_dict()

        if configured_mode == "redirect" and url and url != upstream_url:
            headers = {}
            effective_mode = "redirect"
        elif configured_mode == "redirect" and auth_headers:
            headers = auth_headers
            effective_mode = "proxy"
        else:
            headers = auth_headers
            effective_mode = configured_mode

        return {
            "download_url": url,
            "file_name": info.name or file_id.split("/")[-1],
            "size": int(info.size or 0),
            "headers": headers,
            "effective_mode": effective_mode,
        }

    async def _save_upload_to_tempfile(self, upload_file: UploadFile) -> str:
        suffix = os.path.splitext(upload_file.filename or "")[1]
        fd, temp_path = tempfile.mkstemp(prefix="litepan_webdav_", suffix=suffix)
        os.close(fd)
        try:
            with open(temp_path, "wb") as temp_fp:
                while True:
                    chunk = await upload_file.read(1024 * 1024)
                    if not chunk:
                        break
                    temp_fp.write(chunk)
            return temp_path
        except Exception:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
            raise

    async def upload_file(
        self,
        upload_file: UploadFile,
        parent_path: str = "0",
        conflict_policy: str = "overwrite",
    ) -> OperationResult:
        temp_path = ""
        try:
            temp_path = await self._save_upload_to_tempfile(upload_file)
            return await self.upload_local_file(
                temp_path,
                upload_file.filename or "",
                parent_path,
                conflict_policy=conflict_policy,
            )
        finally:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
            try:
                await upload_file.close()
            except Exception:
                pass

    async def upload_local_file_with_resume(
        self,
        local_path: str,
        file_name: str,
        parent_path: str = "0",
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        conflict_policy: str = "overwrite",
        resume_state: Optional[Dict[str, Any]] = None,
        state_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
    ) -> OperationResult:
        return await self.upload_local_file(
            local_path,
            file_name,
            parent_path,
            progress_callback=progress_callback,
            conflict_policy=conflict_policy,
            resume_state=resume_state,
            state_callback=state_callback,
        )

    async def _iter_file_chunks(
        self,
        local_path: str,
        *,
        file_size: int,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        message: str = "正在上传",
    ):
        chunk_size = max(1, int(self._UPLOAD_CHUNK_SIZE))
        uploaded = 0
        last_emit_at = 0.0
        loop = asyncio.get_running_loop()

        def _open_file():
            return open(local_path, "rb")

        def _read_chunk(fp) -> bytes:
            return fp.read(chunk_size)

        def _close_file(fp) -> None:
            try:
                fp.close()
            except Exception:
                pass

        fp = await asyncio.to_thread(_open_file)
        try:
            while True:
                chunk = await asyncio.to_thread(_read_chunk, fp)
                if not chunk:
                    break
                yield chunk
                uploaded += len(chunk)
                if progress_callback is None:
                    continue
                now = loop.time()
                reached_end = uploaded >= file_size
                if reached_end or (now - last_emit_at) >= self._PROGRESS_MIN_INTERVAL:
                    last_emit_at = now
                    try:
                        await progress_callback(uploaded, file_size, message)
                    except Exception as emit_err:
                        self._log.debug(
                            f"WebDAV 进度回调异常（忽略）: {emit_err}",
                            driver_name="webdav",
                        )
        finally:
            await asyncio.to_thread(_close_file, fp)

    @auto_cleanup_cache("upload_file")
    async def upload_local_file(
        self,
        local_path: str,
        file_name: str,
        parent_path: str = "0",
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        conflict_policy: str = "overwrite",
        resume_state: Optional[Dict[str, Any]] = None,
        state_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
    ) -> OperationResult:
        _ = (resume_state, state_callback)

        async def _notify_progress(uploaded: int, total: int, message: str) -> None:
            if progress_callback:
                try:
                    await progress_callback(uploaded, total, message)
                except Exception as emit_err:
                    self._log.debug(
                        f"WebDAV 进度回调异常（忽略）: {emit_err}",
                        driver_name="webdav",
                    )

        try:
            target_name = self._validate_entry_name(os.path.basename((file_name or "").strip()))
            if not target_name:
                return OperationResult(success=False, message="上传文件名无效")
            if not local_path or not os.path.exists(local_path):
                return OperationResult(success=False, message="待上传文件不存在")

            parent_rel = self._normalize_parent_rel(parent_path)
            child_rel = f"{parent_rel}/{target_name}" if parent_rel else target_name
            parent_api = self._to_api_parent_id(parent_rel)

            if conflict_policy == "skip" and await self._resource_exists(child_rel, is_dir=False):
                await _notify_progress(0, 0, "已跳过")
                return OperationResult(
                    success=True,
                    message=f"文件「{target_name}」已存在，已跳过",
                    data={
                        "skipped": True,
                        "file_name": target_name,
                        "parent_id": parent_api,
                    },
                )

            if conflict_policy != "overwrite" and await self._resource_exists(child_rel, is_dir=False):
                return OperationResult(
                    success=False,
                    message=f"目标已存在且策略为 {conflict_policy}，无法上传",
                )

            url = self._resource_url_for_rel(child_rel, is_dir=False)
            file_size = os.path.getsize(local_path)

            await _notify_progress(0, file_size, "正在上传")

            if file_size == 0:
                await self._simple_request(
                    "PUT",
                    url,
                    headers={"Content-Length": "0"},
                    data=b"",
                    ok_status=(200, 201, 204),
                    timeout=self._upload_timeout(),
                )
            else:
                chunk_iter = self._iter_file_chunks(
                    local_path,
                    file_size=file_size,
                    progress_callback=progress_callback,
                )
                payload = _SizedAsyncIterablePayload(
                    chunk_iter,
                    size=file_size,
                    content_type="application/octet-stream",
                )
                await self._simple_request(
                    "PUT",
                    url,
                    data=payload,
                    ok_status=(200, 201, 204),
                    timeout=self._upload_timeout(),
                )

            await self._maybe_operation_delay()
            await _notify_progress(file_size, file_size, "上传成功")
            return OperationResult(
                success=True,
                message=f"文件「{target_name}」上传成功",
                data={
                    "file_name": target_name,
                    "parent_id": parent_api,
                    "file_id": child_rel,
                    "size": file_size,
                },
            )
        except Exception as e:
            self._log.error(f"WebDAV 上传失败: {e}", driver_name="webdav")
            return OperationResult(success=False, message=f"上传失败: {str(e)}")
