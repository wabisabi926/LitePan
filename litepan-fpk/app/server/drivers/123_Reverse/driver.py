"""123 云盘驱动业务方法"""

import asyncio
import hashlib
import os
import secrets
import tempfile
import aiohttp
from typing import Awaitable, Callable, Dict, Any, List, Optional
from datetime import datetime
from fastapi import UploadFile
from core.base import FileItem, OperationResult, DriverInfo
from core.driver_base import BaseDriver
from core.operation_wrapper import (
    auto_cleanup_cache, 
    with_file_list_cache, 
    with_file_info_cache, 
    with_performance_tracking,
    with_auth_retry,
    clear_operation_cache
)
from .config import Pan123ReverseConfig
from .models import Pan123ReverseFile
from .api import Pan123ReverseAPI, Pan123ReverseApiHelper, Pan123ReverseConstants


class Pan123ReverseDriver(BaseDriver):
    def __init__(self, config: Pan123ReverseConfig):
        super().__init__(config)
        self.username = config.username
        self.password = config.password
        self.access_token = config.access_token
        self._session: Optional[aiohttp.ClientSession] = None

    @classmethod
    def get_info(cls) -> DriverInfo:
        return DriverInfo(
            name="123云盘",
            display_name="123云盘",
            version="3.1.0",
            capabilities=["list", "info", "download", "create_folder", "delete", "batch_delete", "rename", "move", "copy", "upload"],
            description="123云盘Username+Password认证方式",
            author="LitePan"
        )

    async def init(self) -> None:
        await self._ensure_runtime_config()
        if not self.access_token and self.config.is_username_auth():
            await self._authenticate()

        self._log.debug("123云盘驱动初始化完成", driver_name="123_reverse")

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._log.debug("123云盘驱动已关闭", driver_name="123_reverse")

    async def test_connection(self) -> OperationResult:
        try:
            self._log.debug("开始测试 123云盘 连接", driver_name="123_reverse")
            await self._ensure_runtime_config()

            if self.config.is_username_auth() and not self.access_token:
                await self._authenticate()

            result = await self._api_request("user_info")

            if not result:
                return OperationResult(success=False, message="连接测试失败: user_info返回空结果")

            self._log.debug("123云盘 连接测试成功", driver_name="123_reverse")
            return OperationResult(success=True, message="连接测试成功")

        except Exception as e:
            error_msg = f"连接测试失败: {str(e)}"
            self._log.error(f"123云盘 {error_msg}", driver_name="123_reverse")
            return OperationResult(success=False, message=error_msg)

    async def _authenticate(self) -> bool:
        try:
            result = await self._login()
            if result:
                self.access_token = result.get("token")
                self.config.access_token = result.get("token")

                return True
            else:
                self._log.error("123云盘认证失败", driver_name="123_reverse")
                return False
        except Exception as e:
            if Pan123ReverseApiHelper.is_verification_error(str(e)):
                raise
            self._log.debug(f"123云盘认证异常: {str(e)}", driver_name="123_reverse")
            return False

    async def _ensure_runtime_config(self) -> None:
        if not self.config.login_uuid:
            self.config.login_uuid = secrets.token_hex(16)

        if self.config.api_base_url:
            Pan123ReverseApiHelper.configure(self.config.api_base_url)
            return

        session = await self._get_session()
        async with session.get(Pan123ReverseAPI.DYDOMAIN_URL) as response:
            if response.status != 200:
                text = await response.text()
                raise Exception(f"123云盘获取动态域名失败 ({response.status}): {text}")
            payload = await response.json()
            if payload.get("code") not in (0, 200):
                raise Exception(f"123云盘获取动态域名失败: {payload.get('message', '未知错误')}")
            api_base = Pan123ReverseApiHelper.resolve_api_base_from_dydomain(payload)
            self.config.api_base_url = api_base
            Pan123ReverseApiHelper.configure(api_base)
            self._log.debug(f"123云盘动态API: {api_base}", driver_name="123_reverse")

    def _build_headers(self, access_token: Optional[str] = None) -> Dict[str, str]:
        token = self.access_token if access_token is None else access_token
        return Pan123ReverseApiHelper.build_headers(token, self.config.login_uuid)

    async def refresh_auth(self) -> bool:
        success = await self._authenticate()
        if success:
            self._log.info("✅ 123云盘认证刷新成功", driver_name="123_reverse")
        return success

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers=self._build_headers())
        return self._session

    async def _apply_operation_delay(self) -> None:
        await self.wait_for_request_interval()
    
    async def _api_request(self, operation: str, method: str = "GET", **kwargs) -> Optional[Dict[str, Any]]:
        try:
            await self._ensure_runtime_config()
            session = await self._get_session()

            endpoint = Pan123ReverseApiHelper.get_endpoint(operation)
            headers = self._build_headers()
            api_url = Pan123ReverseApiHelper.get_signed_url(endpoint)

            if 'params' in kwargs:
                kwargs['params'] = Pan123ReverseApiHelper.map_params(kwargs['params'], operation)
            if 'json' in kwargs:
                kwargs['json'] = Pan123ReverseApiHelper.map_params(kwargs['json'], operation)
            
            await self._apply_operation_delay()
            async with session.request(method, api_url, headers=headers, **kwargs) as response:
                if response.status != 200:
                    text = await response.text()
                    if response.status in [401, 403]:
                        if self.is_connectivity_test():
                            raise Exception(f"123云盘API错误 ({response.status}): {text}")
                        auth_success = await self._handle_auth_error(f"API错误 ({response.status})")
                        if auth_success:
                            headers = self._build_headers()
                            await self._apply_operation_delay()
                            async with session.request(method, api_url, headers=headers, **kwargs) as retry_response:
                                if retry_response.status != 200:
                                    retry_text = await retry_response.text()
                                    raise Exception(f"123云盘API错误 ({retry_response.status}): {retry_text}")
                                data = await retry_response.json()
                                success, error_msg = Pan123ReverseApiHelper.check_success(data, operation)
                                if not success:
                                    raise Exception(f"123云盘API业务错误: {error_msg}")
                                return Pan123ReverseApiHelper.extract_data(data, operation)
                        else:
                            raise Exception("认证刷新失败")
                
                data = await response.json()
                success, error_msg = Pan123ReverseApiHelper.check_success(data, operation)
                if not success:
                    if "token" in error_msg.lower() or "auth" in error_msg.lower() or "unauthorized" in error_msg.lower():
                        if self.is_connectivity_test():
                            raise Exception(f"123云盘API错误: {error_msg}")
                        self._log.warning(f"🔐 检测到认证错误，触发被动刷新: {error_msg}", driver_name="123_reverse")
                        auth_success = await self._handle_auth_error(f"API错误 (JSON): {error_msg}")
                        if auth_success:
                            self._log.info("✅ 被动刷新成功，重新尝试请求", driver_name="123_reverse")
                            headers = self._build_headers()
                            await self._apply_operation_delay()
                            async with self._session.request(method, api_url, headers=headers, **kwargs) as retry_response:
                                if retry_response.status != 200:
                                    retry_text = await retry_response.text()
                                    raise Exception(f"123云盘API错误 ({retry_response.status}): {retry_text}")
                                
                                retry_data = await retry_response.json()
                                retry_success, retry_error_msg = Pan123ReverseApiHelper.check_success(retry_data, operation)
                                if not retry_success:
                                    raise Exception(f"123云盘API错误: {retry_error_msg}")
                                return Pan123ReverseApiHelper.extract_data(retry_data, operation)
                    raise Exception(f"123云盘API错误: {error_msg}")
                return Pan123ReverseApiHelper.extract_data(data, operation)
                
        except Exception as e:
            self._log.error(f"API请求异常: {str(e)}", driver_name="123_reverse")
            raise
    
    async def _handle_auth_error(self, error_msg: str):
        """驱动内被动刷新：交给 auth_manager 统一处理，失败再回退。"""
        try:
            if self.is_connectivity_test():
                return False
            self._log.warning(f"触发被动刷新: {error_msg}", driver_name="123_reverse")
            if hasattr(self, '_account_id'):
                from core.auth_manager import handle_auth_error
                success = await handle_auth_error(self._account_id)
                if success:
                    self._log.info("✅ 被动刷新成功", driver_name="123_reverse")
                else:
                    self._log.error("❌ 被动刷新失败", driver_name="123_reverse")
                return success
            else:
                self._log.error("❌ 无法获取账号ID，跳过被动刷新", driver_name="123_reverse")
                return False
        except Exception as e:
            self._log.error(f"❌ 被动刷新异常: {e}", driver_name="123_reverse")
            return False
    
    async def _login(self) -> Optional[Dict[str, Any]]:
        # 123 旧接口按账号类型切换字段：邮箱 -> mail + type=2；手机号 -> passport
        if '@' in self.config.username:
            body = {
                "mail": self.config.username,
                "password": self.config.password,
                "type": 2
            }
        else:
            body = {
                "passport": self.config.username,
                "password": self.config.password,
                "remember": True
            }

        try:
            await self._ensure_runtime_config()
            session = await self._get_session()
            endpoint = Pan123ReverseApiHelper.get_endpoint("login")
            headers = self._build_headers()
            api_url = Pan123ReverseApiHelper.get_signed_url(endpoint)
            
            await self._apply_operation_delay()
            async with session.post(api_url, headers=headers, json=body) as response:
                if response.status != 200:
                    text = await response.text()
                    self._log.error(f"登录HTTP错误: {response.status} - {text}", driver_name="123_reverse")
                    raise Exception(f"123云盘登录API错误 ({response.status}): {text}")
                
                data = await response.json()

                success, error_msg = Pan123ReverseApiHelper.check_success(data, "login")
                if not success:
                    raise Exception(f"123云盘登录失败: {error_msg}")

                result = Pan123ReverseApiHelper.extract_data(data, "login")
                self._log.debug(f"提取的登录数据: {result}", driver_name="123_reverse")
                return result

        except Exception as e:
            self._log.debug(f"登录异常: {str(e)}", driver_name="123_reverse")
            raise

    @with_file_list_cache
    async def list_files(self, parent_id: str = "0") -> List[FileItem]:
        files = []
        page = 1

        while True:
            response = await self._api_request("file_list", "GET", params={
                "parent_id": parent_id,
                "page": str(page),
                "limit": "100",
            })
            
            if not response:
                self._log.error("获取文件列表失败", driver_name="123_reverse")
                break

            file_list = response.get("files", [])
            next_page = response.get("next", "-1")

            for file_data in file_list:
                pan123_file = Pan123ReverseFile.from_dict(file_data)
                file_item = pan123_file.to_file_item(parent_id)
                files.append(file_item)

            if next_page == "-1" or len(file_list) == 0:
                break

            page += 1
            if page > 100:
                self._log.warning("文件列表页数过多，停止获取", driver_name="123_reverse")
                break
        
        return files
    
    @with_file_info_cache
    async def file_info(self, file_id: str) -> Optional[FileItem]:
        """先调 file/info，接口不稳时回退到扫描常用目录。"""
        try:
            self._log.debug(f"开始获取文件 {file_id} 的信息", driver_name="123_reverse")

            file_item = await self._get_file_info_direct(file_id)
            if file_item:
                return file_item

            self._log.debug(f"file/info接口失败，尝试通过文件列表搜索文件 {file_id}", driver_name="123_reverse")
            return await self._search_file_in_lists(file_id)

        except Exception as e:
            self._log.error(f"获取文件信息失败: {str(e)}", driver_name="123_reverse")
            return None

    async def _get_file_info_direct(self, file_id: str) -> Optional[FileItem]:
        try:
            file_info_params = {
                "fileIdList": [{"fileId": int(file_id)}]
            }

            session = await self._get_session()
            endpoint = Pan123ReverseApiHelper.get_endpoint("file_info")
            headers = self._build_headers()
            api_url = Pan123ReverseApiHelper.get_signed_url(endpoint)
            mapped_params = file_info_params
            
            self._log.debug(f"file/info请求URL: {api_url}", driver_name="123_reverse")
            self._log.debug(f"file/info请求参数: {mapped_params}", driver_name="123_reverse")
            
            await self._apply_operation_delay()
            async with session.post(api_url, headers=headers, json=mapped_params) as response:
                if response.status != 200:
                    return None
                
                data = await response.json()
                self._log.debug(f"file/info响应: {data}", driver_name="123_reverse")

                success, error_msg = Pan123ReverseApiHelper.check_success(data, "file_info")
                if not success:
                    return None
            
            info_list = data.get("infoList", [])
            if not info_list and 'data' in data and isinstance(data['data'], dict):
                info_list = data['data'].get("infoList", [])
            
            if not info_list or info_list is None:
                return None

            file_data = info_list[0]
            
            parent_id = file_data.get("ParentFileId", "0")
            
            pan123_file = Pan123ReverseFile.from_dict(file_data)
            file_item = pan123_file.to_file_item(str(parent_id))
            
            self._log.debug(f"通过file/info成功获取文件信息: {file_item.name}", driver_name="123_reverse")
            return file_item

        except Exception as e:
            self._log.debug(f"file/info接口调用失败: {str(e)}", driver_name="123_reverse")
            return None

    async def _search_file_in_lists(self, file_id: str) -> Optional[FileItem]:
        try:
            files = await self.list_files("0")
            if files:
                for file in files:
                    if str(file.id) == file_id:
                        return file
            
            common_parents = ["0", "1", "2", "3", "4", "5"]
            for parent_id in common_parents:
                files = await self.list_files(parent_id)
                if files:
                    for file in files:
                        if str(file.id) == file_id:
                            return file
            
            self._log.error(f"在所有目录中未找到文件ID: {file_id}", driver_name="123_reverse")
            return None

        except Exception as e:
            self._log.error(f"搜索文件失败: {str(e)}", driver_name="123_reverse")
            return None
    
    async def get_download_url(self, file_id: str, user_agent: str = None) -> str:
        try:
            self._log.debug(f"开始获取文件下载链接: {file_id}", driver_name="123_reverse")
            
            if not self.access_token:
                self._log.warning("缺少认证token，尝试重新认证", driver_name="123_reverse")
                await self._authenticate()
                if not self.access_token:
                    raise Exception("认证失败，无法获取下载链接")
            
            file_info = await self.file_info(file_id)
            if not file_info:
                raise Exception(f"无法获取文件信息: {file_id}")
            
            self._log.debug(f"文件信息extra字段: {file_info.extra}", driver_name="123_reverse")
            self._log.debug(f"s3_key_flag值: {file_info.extra.get('s3_key_flag', '')}", driver_name="123_reverse")
            self._log.debug(f"etag值: {file_info.extra.get('etag', '')}", driver_name="123_reverse")
            
            s3key_flag = file_info.extra.get('s3_key_flag', '')
            if not s3key_flag:
                self._log.error(f"文件 {file_id} 的s3keyFlag为空，无法下载", driver_name="123_reverse")
                raise Exception(f"文件 {file_id} 缺少s3keyFlag信息，无法下载")
            
            download_params = {
                "driveId": 0,
                "etag": file_info.extra.get('etag', ''),
                "fileId": int(file_id),
                "s3keyFlag": s3key_flag,
                "type": 0,
                "fileName": file_info.name,
                "size": file_info.size
            }
            
            import time
            import random
            timestamp = int(time.time())
            random_suffix = f"{timestamp}-{random.randint(1000000, 9999999)}-{random.randint(1000000000, 9999999999)}"
            url_param = f"?{random.randint(1000000000, 9999999999)}={random_suffix}"
            
            endpoint = Pan123ReverseApiHelper.get_endpoint("download_info")
            api_url = Pan123ReverseApiHelper.get_signed_url(endpoint) + url_param
            
            self._log.debug(f"下载API URL: {api_url}", driver_name="123_reverse")
            self._log.debug(f"下载参数: {download_params}", driver_name="123_reverse")
            
            session = await self._get_session()
            headers = self._build_headers()
            
            headers.update({
                'Content-Type': 'application/json;charset=UTF-8',
                'Referer': 'https://yun.123pan.cn/',
                'Origin': 'https://yun.123pan.cn',
                'User-Agent': user_agent or 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
            })
            
            await self._apply_operation_delay()
            async with session.post(api_url, headers=headers, json=download_params) as response:
                self._log.debug(f"下载请求响应状态: {response.status}", driver_name="123_reverse")
                
                if response.status != 200:
                    text = await response.text()
                    self._log.error(f"下载请求失败 ({response.status}): {text}", driver_name="123_reverse")
                    
                    if response.status in [401, 403] or "token" in text.lower() or "auth" in text.lower():
                        self._log.warning("检测到认证错误，尝试重新认证", driver_name="123_reverse")
                        await self._authenticate()
                        headers = self._build_headers()
                        headers.update({
                            'Content-Type': 'application/json;charset=UTF-8',
                            'Referer': 'https://yun.123pan.cn/',
                            'Origin': 'https://yun.123pan.cn',
                            'User-Agent': user_agent or 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
                        })
                        
                        await self._apply_operation_delay()
                        async with session.post(api_url, headers=headers, json=download_params) as retry_response:
                            if retry_response.status != 200:
                                retry_text = await retry_response.text()
                                raise Exception(f"下载请求失败 ({retry_response.status}): {retry_text}")
                            
                            data = await retry_response.json()
                    else:
                        raise Exception(f"下载请求失败 ({response.status}): {text}")
                else:
                    data = await response.json()

                
                success, error_msg = Pan123ReverseApiHelper.check_success(data, "download_info")
                if not success:
                    error_details = f"下载API错误: {error_msg}"
                    error_details += f"\n使用的参数: fileId={file_id}, s3keyFlag={s3key_flag}, etag={file_info.extra.get('etag', '')}"
                    error_details += f"\n文件信息: name={file_info.name}, size={file_info.size}"
                    raise Exception(error_details)
                
                download_data = Pan123ReverseApiHelper.extract_data(data, "download_info")
                if not download_data:
                    raise Exception("下载响应数据为空")
                
                download_url = download_data.get("download_url")
                if not download_url:
                    raise Exception("下载链接为空")
                
                self._log.debug(f"成功获取下载链接: {download_url[:100]}...", driver_name="123_reverse")
                
                if "web-pro2.123952.com" in download_url:
                    self._log.debug("检测到代理URL，尝试解析真正的下载链接", driver_name="123_reverse")
                    try:
                        cdn_url = self._extract_cdn_url_from_proxy(download_url)
                        if cdn_url:
                            final_url = await self._resolve_cdn_url(cdn_url, user_agent)
                            if final_url:
                                return final_url
                            else:
                                return cdn_url
                        
                        real_url = await self._resolve_proxy_url(download_url, user_agent)
                        if real_url:
                            return real_url
                    except Exception as e:
                        self._log.warning(f"解析代理URL失败，使用原始链接: {str(e)}", driver_name="123_reverse")
                
                return download_url
                
        except Exception as e:
            error_msg = f"获取下载链接失败: {str(e)}"
            self._log.error(f"123云盘 {error_msg}", driver_name="123_reverse")
            raise Exception(error_msg)

    async def get_download_headers(self, file_id: str, user_agent: str = None) -> Dict[str, str]:
        """获取下载时需要的请求头"""
        try:
            # 123云盘逆向接口需要特定的请求头
            # 根据官方web端的请求头设置
            headers = {
                'User-Agent': user_agent or 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate, br, zstd',
                'Accept-Language': 'zh-CN,zh;q=0.9',
                'Referer': 'https://www.123pan.cn/',
                'Origin': 'https://www.123pan.cn',
                'Connection': 'keep-alive',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',

            }
            
            if self.access_token:
                headers['Authorization'] = f'Bearer {self.access_token}'

            return headers
            
        except Exception as e:
            self._log.error(f"获取下载请求头失败: {str(e)}", driver_name="123_reverse")
            raise

    async def _resolve_proxy_url(self, proxy_url: str, user_agent: str = None) -> Optional[str]:
        """解析123云盘代理URL，获取真正的下载链接"""
        try:
            self._log.debug(f"开始解析代理URL: {proxy_url[:100]}...", driver_name="123_reverse")
            
            headers = await self.get_download_headers("", user_agent)
            
            session = await self._get_session()
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            
            await self._apply_operation_delay()
            async with session.get(proxy_url, headers=headers, timeout=timeout, allow_redirects=False) as response:
                self._log.debug(f"代理URL响应状态: {response.status}", driver_name="123_reverse")
                
                if response.status in [301, 302, 303, 307, 308]:
                    location = response.headers.get('Location')
                    if location:
                        self._log.debug(f"发现重定向到: {location[:100]}...", driver_name="123_reverse")
                        return location
                
                content_type = response.headers.get('Content-Type', '')
                if 'application/json' in content_type:
                    try:
                        data = await response.json()
                        self._log.debug(f"JSON响应: {data}", driver_name="123_reverse")
                        
                        for field in ['download_url', 'url', 'redirect_url', 'link']:
                            if field in data:
                                url = data[field]
                                if url and isinstance(url, str) and url.startswith('http'):
                                    self._log.debug(f"从JSON中找到下载链接: {url[:100]}...", driver_name="123_reverse")
                                    return url
                    except Exception as e:
                        self._log.debug(f"解析JSON响应失败: {str(e)}", driver_name="123_reverse")
                
                for header_name in ['X-Download-Url', 'X-Redirect-Url', 'X-Location']:
                    url = response.headers.get(header_name)
                    if url and url.startswith('http'):
                        self._log.debug(f"从响应头中找到下载链接: {url[:100]}...", driver_name="123_reverse")
                        return url
                
                self._log.debug("未找到真正的下载链接", driver_name="123_reverse")
                return None
                
        except Exception as e:
            self._log.error(f"解析代理URL失败: {str(e)}", driver_name="123_reverse")
            return None

    def _extract_cdn_url_from_proxy(self, proxy_url: str) -> Optional[str]:
        """从代理URL的参数中提取base64编码的CDN链接"""
        try:
            from urllib.parse import urlparse, parse_qs
            import base64
            
            parsed = urlparse(proxy_url)
            query_params = parse_qs(parsed.query)
            
            params = query_params.get('params', [None])[0]
            if not params:
                self._log.debug("未找到params参数", driver_name="123_reverse")
                return None
            
            try:
                decoded_params = base64.b64decode(params).decode('utf-8')
                self._log.debug(f"解码后的params: {decoded_params[:100]}...", driver_name="123_reverse")
                
                if decoded_params.startswith('http'):
                    self._log.debug(f"成功提取CDN链接: {decoded_params[:100]}...", driver_name="123_reverse")
                    return decoded_params
                else:
                    self._log.debug(f"解码后的内容不是有效URL: {decoded_params[:50]}...", driver_name="123_reverse")
                    return None
                    
            except Exception as e:
                self._log.debug(f"base64解码失败: {str(e)}", driver_name="123_reverse")
                return None
                
        except Exception as e:
            self._log.error(f"从代理URL提取CDN链接失败: {str(e)}", driver_name="123_reverse")
            return None

    async def _resolve_cdn_url(self, cdn_url: str, user_agent: str = None) -> Optional[str]:
        """解析CDN链接，获取最终的下载链接"""
        try:
            self._log.debug(f"开始解析CDN链接: {cdn_url[:100]}...", driver_name="123_reverse")
            
            headers = await self.get_download_headers("", user_agent)
            
            session = await self._get_session()
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            
            await self._apply_operation_delay()
            async with session.get(cdn_url, headers=headers, timeout=timeout, allow_redirects=False) as response:
                self._log.debug(f"CDN链接响应状态: {response.status}", driver_name="123_reverse")
                
                if response.status in [301, 302, 303, 307, 308]:
                    location = response.headers.get('Location')
                    if location:
                        self._log.debug(f"CDN重定向到: {location[:100]}...", driver_name="123_reverse")
                        return location
                
                content_type = response.headers.get('Content-Type', '')
                if 'application/json' in content_type:
                    try:
                        data = await response.json()
                        self._log.debug(f"CDN JSON响应: {data}", driver_name="123_reverse")
                        
                        if 'data' in data and 'redirect_url' in data['data']:
                            redirect_url = data['data']['redirect_url']
                            if redirect_url and isinstance(redirect_url, str) and redirect_url.startswith('http'):
                                self._log.debug(f"从JSON中找到redirect_url: {redirect_url[:100]}...", driver_name="123_reverse")
                                return redirect_url
                        
                        for field in ['redirect_url', 'download_url', 'url', 'link']:
                            if field in data:
                                url = data[field]
                                if url and isinstance(url, str) and url.startswith('http'):
                                    self._log.debug(f"从JSON中找到{field}: {url[:100]}...", driver_name="123_reverse")
                                    return url
                    except Exception as e:
                        self._log.debug(f"解析CDN JSON响应失败: {str(e)}", driver_name="123_reverse")
                
                if response.status == 200 and ('application/octet-stream' in content_type or 'video/' in content_type or 'audio/' in content_type):
                    self._log.debug("CDN链接直接返回文件内容", driver_name="123_reverse")
                    return cdn_url
                
                self._log.debug("CDN链接解析完成，返回原始链接", driver_name="123_reverse")
                return cdn_url
                
        except Exception as e:
            self._log.error(f"解析CDN链接失败: {str(e)}", driver_name="123_reverse")
            return cdn_url  # 失败时返回原始CDN链接



    
    @auto_cleanup_cache('create_folder')
    async def create_folder(self, parent_id: str, name: str) -> OperationResult:
        try:
            if not name or not name.strip():
                return OperationResult(success=False, message="文件夹名称不能为空")
            
            if not parent_id:
                parent_id = "0"  # 默认根目录
            
            await self._api_request("create_folder", "POST", json={
                "parent_id": parent_id,
                "folder_name": name.strip()
            })
            
            # 状态收敛等待：目录创建成功后，需等待网盘侧目录真正可见，再回查新目录 ID。
            if self.config.operation_delay > 0:
                self._log.debug(f"目录创建后状态收敛等待 {self.config.operation_delay}ms", driver_name="123_reverse")
                await asyncio.sleep(self.config.operation_delay / 1000.0)
            
            try:
                if getattr(self, "_account_id", None):
                    await clear_operation_cache(str(self._account_id), 'directory_update', parent_id=parent_id)
                files = await self.list_files(parent_id)
                new_folder = None
                for file in files:
                    if file.name == name.strip() and file.is_dir:
                        new_folder = file
                        break
                
                if new_folder:
                    return OperationResult(
                        success=True, 
                        message=f"文件夹 '{name}' 创建成功",
                        data={
                            "folder_id": new_folder.id, 
                            "parent_path": parent_id, 
                            "folder_name": name
                        }
                    )
                else:
                    return OperationResult(
                        success=True, 
                        message=f"文件夹 '{name}' 创建成功",
                        data={
                            "folder_id": None, 
                            "parent_path": parent_id, 
                            "folder_name": name
                        }
                    )
            except Exception as e:
                self._log.warning(f"获取新文件夹ID失败: {str(e)}", driver_name="123_reverse")
                return OperationResult(
                    success=True, 
                    message=f"文件夹 '{name}' 创建成功",
                    data={
                        "folder_id": None, 
                        "parent_path": parent_id, 
                        "folder_name": name
                    }
                )
                
        except Exception as e:
            error_msg = f"新建文件夹异常: {str(e)}"
            self._log.error(f"123云盘 {error_msg}", driver_name="123_reverse")
            return OperationResult(success=False, message=error_msg)
    
    
    @auto_cleanup_cache('rename_file')
    async def rename_file(self, file_id: str, new_name: str) -> OperationResult:
        try:
            if not new_name or not new_name.strip():
                return OperationResult(success=False, message="新名称不能为空")
            
            if not file_id:
                return OperationResult(success=False, message="文件ID不能为空")
            
            await self._api_request("rename", "POST", json={
                "file_id": int(file_id),
                "new_name": new_name.strip()
            })
            
            # 状态收敛等待：重命名成功后，目录列表与缓存未必立刻反映新名称。
            if self.config.operation_delay > 0:
                self._log.debug(f"重命名后状态收敛等待 {self.config.operation_delay}ms", driver_name="123_reverse")
                await asyncio.sleep(self.config.operation_delay / 1000.0)

            # 强制清理所有目录缓存，确保前端能正确显示
            try:
                from core.dependency_container import get_cache_cleaner
                cache_cleaner = get_cache_cleaner()
                if cache_cleaner and hasattr(self, '_account_id'):
                    prefix = f"dir:{self._account_id}:"
                    await cache_cleaner.cache_manager.clear_by_prefix(prefix)
            except Exception as e:
                self._log.warning(f"强制清理缓存失败: {str(e)}", driver_name="123_reverse")
            
            return OperationResult(
                success=True, 
                message=f"文件重命名为 '{new_name}' 成功",
                data={"file_id": file_id, "new_name": new_name}
            )
                
        except Exception as e:
            error_msg = f"重命名异常: {str(e)}"
            self._log.error(f"123云盘 {error_msg}", driver_name="123_reverse")
            return OperationResult(success=False, message=error_msg)
    
    
    @auto_cleanup_cache('delete_file')
    async def delete_file(self, file_id: str) -> OperationResult:
        return await self._delete_files([file_id])

    @auto_cleanup_cache('batch_delete_file')
    async def batch_delete_file(self, file_ids: List[str]) -> OperationResult:
        if not file_ids:
            return OperationResult(success=True, message="没有文件需要删除")

        return await self._delete_files(file_ids)

    async def _delete_files(self, file_ids: List[str]) -> OperationResult:
        try:
            parent_ids = set()
            for file_id in file_ids:
                try:
                    file_info = await self.file_info(file_id)
                    if file_info and file_info.extra and 'parent_id' in file_info.extra:
                        parent_ids.add(file_info.extra['parent_id'])
                except Exception:
                    pass
            
            if self.config.delete_mode == "delete":
                # 永久删除模式：先移到回收站，再永久删除
                self._log.debug(f"永久删除模式：准备将 {len(file_ids)} 个文件移入回收站...", driver_name="123_reverse")
                
                # 1. 先移到回收站
                await self._api_request("trash", "POST", json={
                    "file_ids": file_ids
                })
                
                # 状态收敛等待：文件移入回收站后，需等待网盘后台真正完成回收站落库。
                if self.config.operation_delay > 0:
                    self._log.debug(f"回收站落库状态收敛等待 {self.config.operation_delay}ms", driver_name="123_reverse")
                    await asyncio.sleep(self.config.operation_delay / 1000.0)
                
                # 2. 再永久删除
                await self._api_request("delete", "POST", json={
                    "file_ids": file_ids
                })
                
                # 状态收敛等待：永久删除提交成功后，等待网盘后台删除流程真正收敛。
                if self.config.operation_delay > 0:
                    self._log.debug(f"永久删除后状态收敛等待 {self.config.operation_delay}ms", driver_name="123_reverse")
                    await asyncio.sleep(self.config.operation_delay / 1000.0)
                
                message = f"已永久删除 {len(file_ids)} 个文件"
                
            else:
                self._log.debug(f"回收站模式：准备将 {len(file_ids)} 个文件移入回收站...", driver_name="123_reverse")
                
                await self._api_request("trash", "POST", json={
                    "file_ids": file_ids
                })
                
                # 状态收敛等待：回收站模式下也要等待目录状态与缓存稳定。
                if self.config.operation_delay > 0:
                    self._log.debug(f"回收站状态收敛等待 {self.config.operation_delay}ms", driver_name="123_reverse")
                    await asyncio.sleep(self.config.operation_delay / 1000.0)
                
                message = f"已将 {len(file_ids)} 个文件移到回收站"
            
            return OperationResult(
                success=True, 
                message=message, 
                data={
                    "deleted_count": len(file_ids),
                    "file_ids": file_ids,
                    "parent_ids": list(parent_ids)
                }
            )
            
        except Exception as e:
            error_msg = f"删除失败: {str(e)}"
            self._log.error(f"123云盘 {error_msg}", driver_name="123_reverse")
            return OperationResult(success=False, message=error_msg)
    
    @auto_cleanup_cache('move_file')
    async def move_file(self, file_ids: List[str], target_parent_id: str) -> OperationResult:
        try:
            if not file_ids:
                return OperationResult(success=True, message="没有文件需要移动")
            
            if len(file_ids) > 100:
                return OperationResult(success=False, message="单次移动文件数量不能超过100个")
            
            parent_ids = set()
            for file_id in file_ids:
                try:
                    file_info = await self.file_info(file_id)
                    if file_info and file_info.extra and 'parent_id' in file_info.extra:
                        parent_id = file_info.extra['parent_id']
                        parent_ids.add(parent_id)
                        self._log.debug(f"文件 {file_id} 的父目录ID: {parent_id}", driver_name="123_reverse")
                    else:
                        self._log.warning(f"无法获取文件 {file_id} 的父目录信息", driver_name="123_reverse")
                except Exception as e:
                    self._log.error(f"获取文件 {file_id} 信息失败: {str(e)}", driver_name="123_reverse")
                    pass
            
            params = {
                "file_ids": file_ids,
                "target_parent_id": target_parent_id
            }
            await self._api_request("move", "POST", json=params)
            
            # 状态收敛等待：移动成功后，源目录与目标目录未必立刻完成状态同步。
            if self.config.operation_delay > 0:
                self._log.debug(f"移动后状态收敛等待 {self.config.operation_delay}ms", driver_name="123_reverse")
                await asyncio.sleep(self.config.operation_delay / 1000.0)
            
            message = f"已移动 {len(file_ids)} 个文件到目标目录"
            
            return OperationResult(
                success=True, 
                message=message, 
                data={
                    "moved_count": len(file_ids), 
                    "file_ids": file_ids,
                    "target_parent_id": target_parent_id,
                    "source_parent_ids": list(parent_ids)  # 添加源父目录ID列表
                }
            )
            
        except Exception as e:
            error_msg = f"移动失败: {str(e)}"
            self._log.error(f"123云盘 {error_msg}", driver_name="123_reverse")
            return OperationResult(success=False, message=error_msg)
    
    async def batch_move_file(self, file_ids: List[str], target_parent_id: str) -> OperationResult:
        return await self.move_file(file_ids, target_parent_id)

    @auto_cleanup_cache('copy_file')
    async def copy_file(self, file_ids: List[str], target_parent_id: str, source_parent_id: str = None) -> OperationResult:
        try:
            if not file_ids:
                return OperationResult(success=True, message="没有文件需要复制")

            if source_parent_id and str(source_parent_id) == str(target_parent_id):
                return OperationResult(
                    success=False,
                    message="123云盘不支持复制到同一目录",
                    data={"warning": True}
                )

            file_list = []
            for file_id in file_ids:
                info = await self.file_info(file_id)
                if not info:
                    return OperationResult(success=False, message=f"无法获取文件 {file_id} 的信息")
                file_list.append({
                    "fileId": int(file_id),
                    "size": info.size or 0,
                    "etag": (info.extra.get("etag", "") if info.extra else "").lower(),
                    "type": 1 if info.is_dir else 0,
                    "parentFileId": int(info.extra.get("parent_id", "0")) if info.extra and info.extra.get("parent_id") else 0,
                    "fileName": info.name,
                    "driveId": 0,
                })

            response = await self._api_request("copy", "POST", json={
                "fileList": file_list,
                "targetFileId": int(target_parent_id),
            })
            if not response:
                return OperationResult(success=False, message="复制请求失败")

            task_id = response.get("task_id")
            if not task_id:
                return OperationResult(success=False, message="未获取到复制任务ID")

            for _ in range(30):
                await asyncio.sleep(2)
                task_result = await self._api_request("copy_task", "GET", params={"taskId": task_id})
                if not task_result:
                    continue
                status = task_result.get("status")
                error_code = task_result.get("error_code")
                if status == 2:
                    if error_code == 0:
                        return OperationResult(
                            success=True,
                            message=f"已复制 {len(file_ids)} 个文件到目标目录",
                            data={
                                "copied_count": len(file_ids),
                                "file_ids": file_ids,
                                "target_parent_id": target_parent_id,
                                "source_parent_ids": [source_parent_id] if source_parent_id else []
                            }
                        )
                    reason = task_result.get("reason", "未知原因")
                    return OperationResult(success=False, message=f"复制任务失败: {reason}")

            return OperationResult(success=False, message="复制任务超时")

        except Exception as e:
            error_msg = f"复制失败: {str(e)}"
            self._log.error(f"123云盘 {error_msg}", driver_name="123_reverse")
            return OperationResult(success=False, message=error_msg)

    async def batch_copy_file(self, file_ids: List[str], target_parent_id: str) -> OperationResult:
        return await self.copy_file(file_ids, target_parent_id)

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

    @auto_cleanup_cache('upload_file')
    async def upload_local_file(
        self,
        local_path: str,
        file_name: str,
        parent_path: str = "0",
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        conflict_policy: str = "overwrite",
    ) -> OperationResult:
        return await self._upload_local_file_impl(
            local_path=local_path,
            file_name=file_name,
            parent_path=parent_path,
            progress_callback=progress_callback,
            conflict_policy=conflict_policy,
        )

    @auto_cleanup_cache('upload_file')
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
        return await self._upload_local_file_impl(
            local_path=local_path,
            file_name=file_name,
            parent_path=parent_path,
            progress_callback=progress_callback,
            conflict_policy=conflict_policy,
            resume_state=resume_state,
            state_callback=state_callback,
        )

    async def _upload_local_file_impl(
        self,
        *,
        local_path: str,
        file_name: str,
        parent_path: str = "0",
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        conflict_policy: str = "overwrite",
        resume_state: Optional[Dict[str, Any]] = None,
        state_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
    ) -> OperationResult:
        try:
            target_name = os.path.basename((file_name or "").strip())
            if not target_name:
                return OperationResult(success=False, message="上传文件名不能为空")
            if not local_path or not os.path.exists(local_path):
                return OperationResult(success=False, message="待上传文件不存在")

            file_size = os.path.getsize(local_path)

            if conflict_policy == "skip":
                existing = await self._find_existing_file_in_parent(parent_path, target_name)
                if existing:
                    return OperationResult(
                        success=True,
                        message=f"文件 '{target_name}' 已存在，已跳过",
                        data={
                            "skipped": True,
                            "file_name": target_name,
                            "parent_id": parent_path,
                        },
                    )

            file_md5 = await asyncio.to_thread(self._calculate_file_md5, local_path)
            normalized_resume_state = self._normalize_upload_resume_state(
                resume_state,
                parent_id=parent_path,
                target_name=target_name,
                file_size=file_size,
                file_md5=file_md5,
            )
            completed_parts = normalized_resume_state["completed_parts"] if normalized_resume_state else []

            upload_request = await self._prepare_123_upload_request(
                parent_id=parent_path,
                target_name=target_name,
                file_size=file_size,
                file_md5=file_md5,
                conflict_policy=conflict_policy,
                progress_callback=progress_callback,
                normalized_resume_state=normalized_resume_state,
            )

            if not isinstance(upload_request, dict):
                raise Exception("123云盘上传初始化返回为空")

            file_id = str(upload_request.get("file_id") or "").strip()
            reuse = bool(upload_request.get("reuse"))
            upload_key = str(upload_request.get("key") or "").strip()
            storage_node = str(upload_request.get("storage_node") or "").strip()
            upload_id = str(upload_request.get("upload_id") or "").strip()
            upload_info = upload_request.get("info") if isinstance(upload_request.get("info"), dict) else None

            if reuse or not upload_key:
                resolved_item = None
                if upload_info:
                    info_item = Pan123ReverseFile.from_dict(upload_info).to_file_item(parent_path)
                    if not info_item.is_dir and int(info_item.size or 0) == int(file_size):
                        resolved_item = info_item
                if resolved_item is None:
                    resolved_item = await self._resolve_uploaded_file_in_parent(
                        parent_id=parent_path,
                        target_name=target_name,
                        file_size=file_size,
                        preferred_file_id=file_id,
                    )
                if resolved_item:
                    file_id = str(resolved_item.id)
                    target_name = resolved_item.name

                await self._notify_upload_progress(progress_callback, file_size, file_size, "秒传成功")
                return self._build_123_upload_success_result(
                    parent_id=parent_path,
                    target_name=target_name,
                    file_size=file_size,
                    file_id=file_id,
                    rapid_upload=True,
                )

            if not storage_node or not upload_id:
                raise Exception("123云盘上传初始化缺少 storage_node 或 upload_id")

            if state_callback:
                await self._persist_upload_resume_state(
                    state_callback,
                    parent_id=parent_path,
                    target_name=target_name,
                    file_size=file_size,
                    file_md5=file_md5,
                    upload_request=upload_request,
                    completed_parts=completed_parts,
                )

            await self._upload_via_presigned_urls(
                local_path=local_path,
                file_size=file_size,
                upload_request=upload_request,
                progress_callback=progress_callback,
                parent_id=parent_path,
                target_name=target_name,
                file_md5=file_md5,
                state_callback=state_callback,
                completed_parts=completed_parts,
            )

            resolved_item = await self._resolve_uploaded_file_in_parent(
                parent_id=parent_path,
                target_name=target_name,
                file_size=file_size,
                preferred_file_id=file_id,
            )
            if resolved_item:
                file_id = str(resolved_item.id)
                target_name = resolved_item.name

            file_id = await self._complete_123_upload(
                upload_request=upload_request,
                file_id=file_id,
                file_size=file_size,
                parent_id=parent_path,
                target_name=target_name,
            )

            await self._notify_upload_progress(progress_callback, file_size, file_size, "上传成功")
            return self._build_123_upload_success_result(
                parent_id=parent_path,
                target_name=target_name,
                file_size=file_size,
                file_id=file_id,
            )
        except Exception as e:
            return OperationResult(success=False, message=f"上传文件失败: {str(e)}")

    async def _save_upload_to_tempfile(self, upload_file: UploadFile) -> str:
        suffix = os.path.splitext(upload_file.filename or "")[1]
        fd, temp_path = tempfile.mkstemp(prefix="litepan_123_", suffix=suffix)
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

    async def _notify_upload_progress(
        self,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        uploaded_bytes: int = 0,
        total_bytes: int = 0,
        message: str = "",
    ) -> None:
        if progress_callback:
            await progress_callback(uploaded_bytes, total_bytes, message)

    async def _create_123_upload_stream(
        self,
        *,
        local_path: str,
        offset: int,
        part_size: int,
        uploaded_base: int,
        total_bytes: int,
        message: str,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        stream_chunk_size: int = 1024 * 1024,
    ):
        sent_bytes = 0
        last_reported = uploaded_base

        with open(local_path, "rb") as fp:
            fp.seek(offset)
            while sent_bytes < part_size:
                chunk = fp.read(min(stream_chunk_size, part_size - sent_bytes))
                if not chunk:
                    break
                sent_bytes += len(chunk)
                current_uploaded = min(total_bytes, uploaded_base + sent_bytes)
                if current_uploaded > last_reported:
                    await self._notify_upload_progress(
                        progress_callback,
                        current_uploaded,
                        total_bytes,
                        message,
                    )
                    last_reported = current_uploaded
                yield chunk

    def _map_conflict_policy_to_duplicate_mode(self, conflict_policy: str) -> int:
        policy = str(conflict_policy or "overwrite").strip().lower()
        if policy == "overwrite":
            return 2
        if policy in {"keep_both", "keep_both_new", "rename"}:
            return 1
        return 0

    def _build_123_upload_request_payload(
        self,
        *,
        parent_id: str,
        target_name: str,
        file_size: int,
        file_md5: str,
        conflict_policy: str,
    ) -> Dict[str, Any]:
        return {
            "driveId": 0,
            "duplicate": self._map_conflict_policy_to_duplicate_mode(conflict_policy),
            "etag": file_md5.lower(),
            "fileName": target_name,
            "parentFileId": parent_id,
            "size": file_size,
            "type": 0,
        }

    async def _prepare_123_upload_request(
        self,
        *,
        parent_id: str,
        target_name: str,
        file_size: int,
        file_md5: str,
        conflict_policy: str,
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        normalized_resume_state: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if normalized_resume_state:
            resumed_uploaded_bytes = normalized_resume_state["uploaded_bytes"]
            if resumed_uploaded_bytes > 0:
                await self._notify_upload_progress(
                    progress_callback,
                    resumed_uploaded_bytes,
                    file_size,
                    "正在继续上传到123云盘",
                )
            return normalized_resume_state["upload_request"]

        await self._notify_upload_progress(progress_callback, 0, file_size, "正在准备上传")
        return await self._api_request(
            "upload_request",
            "POST",
            json=self._build_123_upload_request_payload(
                parent_id=parent_id,
                target_name=target_name,
                file_size=file_size,
                file_md5=file_md5,
                conflict_policy=conflict_policy,
            ),
        )

    def _build_123_upload_success_result(
        self,
        *,
        parent_id: str,
        target_name: str,
        file_size: int,
        file_id: str = "",
        rapid_upload: bool = False,
    ) -> OperationResult:
        data = {
            "file_id": file_id or None,
            "file_name": target_name,
            "parent_id": parent_id,
            "size": file_size,
        }
        if rapid_upload:
            data["rapid_upload"] = True
        success_message = f"文件 '{target_name}' 秒传成功" if rapid_upload else f"文件 '{target_name}' 上传成功"
        return OperationResult(
            success=True,
            message=success_message,
            data=data,
        )

    async def _complete_123_upload(
        self,
        *,
        upload_request: Dict[str, Any],
        file_id: str,
        file_size: int,
        parent_id: str,
        target_name: str,
    ) -> str:
        resolved_file_id = str(file_id or "").strip()
        if not resolved_file_id:
            resolved_item = await self._resolve_uploaded_file_in_parent(
                parent_id=parent_id,
                target_name=target_name,
                file_size=file_size,
            )
            if resolved_item:
                resolved_file_id = str(resolved_item.id)

        try:
            await self._api_request(
                "upload_complete_v2",
                "POST",
                json={
                    "StorageNode": upload_request.get("storage_node"),
                    "bucket": upload_request.get("bucket"),
                    "fileId": int(resolved_file_id) if resolved_file_id else None,
                    "fileSize": file_size,
                    "isMultipart": file_size > self._get_123_upload_chunk_size(),
                    "key": upload_request.get("key"),
                    "uploadId": upload_request.get("upload_id"),
                },
            )
            return resolved_file_id
        except Exception as exc:
            if not self._is_123_missing_file_id_error(exc):
                raise

            resolved_item = await self._resolve_uploaded_file_in_parent(
                parent_id=parent_id,
                target_name=target_name,
                file_size=file_size,
                preferred_file_id=resolved_file_id,
            )
            if not resolved_item:
                raise

            resolved_file_id = str(resolved_item.id)
            await self._api_request(
                "upload_complete_v2",
                "POST",
                json={
                    "StorageNode": upload_request.get("storage_node"),
                    "bucket": upload_request.get("bucket"),
                    "fileId": int(resolved_file_id),
                    "fileSize": file_size,
                    "isMultipart": file_size > self._get_123_upload_chunk_size(),
                    "key": upload_request.get("key"),
                    "uploadId": upload_request.get("upload_id"),
                },
            )
            return resolved_file_id

    def _is_123_missing_file_id_error(self, exc: Exception) -> bool:
        message = str(exc or "")
        return "请输入FileId" in message or "请输入 FileId" in message

    def _get_123_upload_chunk_size(self) -> int:
        return 16 * 1024 * 1024

    def _calculate_file_md5(self, local_path: str) -> str:
        md5 = hashlib.md5()
        with open(local_path, "rb") as fp:
            for chunk in iter(lambda: fp.read(1024 * 1024), b""):
                if not chunk:
                    break
                md5.update(chunk)
        return md5.hexdigest()

    async def _find_existing_file_in_parent(self, parent_id: str, target_name: str) -> Optional[FileItem]:
        files = await self.list_files(parent_id or "0")
        for item in files or []:
            if item.name == target_name:
                return item
        return None

    def _calculate_uploaded_bytes_by_parts(
        self,
        *,
        completed_parts: List[int],
        file_size: int,
        chunk_size: int,
        chunk_count: int,
    ) -> int:
        uploaded_bytes = 0
        seen = set()
        for part_number in completed_parts:
            try:
                normalized_part = int(part_number)
            except (TypeError, ValueError):
                continue
            if normalized_part in seen or normalized_part < 1 or normalized_part > chunk_count:
                continue
            seen.add(normalized_part)
            offset = (normalized_part - 1) * chunk_size
            uploaded_bytes += min(chunk_size, max(file_size - offset, 0))
        return min(uploaded_bytes, file_size)

    def _normalize_upload_resume_state(
        self,
        resume_state: Optional[Dict[str, Any]],
        *,
        parent_id: str,
        target_name: str,
        file_size: int,
        file_md5: str,
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(resume_state, dict):
            return None

        upload_request = resume_state.get("upload_request")
        if not isinstance(upload_request, dict):
            return None

        resume_parent_id = str(resume_state.get("parent_id") or "").strip()
        resume_target_name = str(resume_state.get("target_name") or "").strip()
        resume_file_md5 = str(resume_state.get("file_md5") or "").strip().lower()
        resume_file_size = int(resume_state.get("file_size") or 0)

        if resume_parent_id != str(parent_id or "0"):
            return None
        if resume_target_name != target_name:
            return None
        if resume_file_md5 != file_md5.lower():
            return None
        if resume_file_size != int(file_size):
            return None

        required_keys = ("bucket", "key", "storage_node", "upload_id")
        if any(not str(upload_request.get(key) or "").strip() for key in required_keys):
            return None

        chunk_size = self._get_123_upload_chunk_size()
        chunk_count = max(1, (file_size + chunk_size - 1) // chunk_size)
        completed_parts: List[int] = []
        for part_number in resume_state.get("completed_parts") or []:
            try:
                normalized_part = int(part_number)
            except (TypeError, ValueError):
                continue
            if 1 <= normalized_part <= chunk_count and normalized_part not in completed_parts:
                completed_parts.append(normalized_part)

        uploaded_bytes = self._calculate_uploaded_bytes_by_parts(
            completed_parts=completed_parts,
            file_size=file_size,
            chunk_size=chunk_size,
            chunk_count=chunk_count,
        )
        progress = min(99, int(uploaded_bytes * 100 / max(file_size, 1))) if uploaded_bytes < file_size else 100

        return {
            "upload_request": upload_request,
            "completed_parts": sorted(completed_parts),
            "uploaded_bytes": uploaded_bytes,
            "progress": progress,
        }

    async def _persist_upload_resume_state(
        self,
        state_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]],
        *,
        parent_id: str,
        target_name: str,
        file_size: int,
        file_md5: str,
        upload_request: Dict[str, Any],
        completed_parts: List[int],
    ) -> None:
        if not state_callback:
            return

        chunk_size = self._get_123_upload_chunk_size()
        chunk_count = max(1, (file_size + chunk_size - 1) // chunk_size)
        uploaded_bytes = self._calculate_uploaded_bytes_by_parts(
            completed_parts=completed_parts,
            file_size=file_size,
            chunk_size=chunk_size,
            chunk_count=chunk_count,
        )
        progress = min(99, int(uploaded_bytes * 100 / max(file_size, 1))) if uploaded_bytes < file_size else 100

        await state_callback({
            "parent_id": str(parent_id or "0"),
            "target_name": target_name,
            "file_size": int(file_size),
            "file_md5": file_md5.lower(),
            "completed_parts": sorted({int(part) for part in completed_parts if int(part) > 0}),
            "uploaded_bytes": uploaded_bytes,
            "progress": progress,
            "upload_request": {
                "bucket": upload_request.get("bucket"),
                "key": upload_request.get("key"),
                "storage_node": upload_request.get("storage_node"),
                "upload_id": upload_request.get("upload_id"),
                "file_id": upload_request.get("file_id"),
                "reuse": upload_request.get("reuse"),
            },
        })

    async def _resolve_uploaded_file_in_parent(
        self,
        *,
        parent_id: str,
        target_name: str,
        file_size: int,
        preferred_file_id: str = "",
    ) -> Optional[FileItem]:
        account_id = str(getattr(self, "_account_id", "") or "")
        for _ in range(3):
            if account_id:
                try:
                    await clear_operation_cache(account_id, 'directory_update', parent_id=parent_id or "0")
                except Exception:
                    pass

            files = await self.list_files(parent_id or "0")
            candidates: List[FileItem] = []
            for item in files or []:
                if item.is_dir:
                    continue
                if preferred_file_id and str(item.id) == str(preferred_file_id):
                    return item
                if item.name == target_name and int(item.size or 0) == int(file_size or 0):
                    candidates.append(item)

            if len(candidates) == 1:
                return candidates[0]
            if len(candidates) > 1:
                candidates.sort(key=lambda current: current.modified or datetime.min, reverse=True)
                return candidates[0]

            # 状态收敛等待：上传完成后轮询目录，等待新文件真正能被列表接口返回。
            await asyncio.sleep(0.4)

        return None

    async def _get_123_presigned_urls(
        self,
        upload_request: Dict[str, Any],
        start: int,
        end_exclusive: int,
        is_multipart: bool,
    ) -> Dict[str, str]:
        operation = "s3_presigned_urls" if is_multipart else "s3_auth"
        response = await self._api_request(
            operation,
            "POST",
            json={
                "StorageNode": upload_request.get("storage_node"),
                "bucket": upload_request.get("bucket"),
                "key": upload_request.get("key"),
                "partNumberStart": start,
                "partNumberEnd": end_exclusive,
                "uploadId": upload_request.get("upload_id"),
            },
        )
        presigned_urls = response.get("presigned_urls") if isinstance(response, dict) else None
        if not isinstance(presigned_urls, dict) or not presigned_urls:
            raise Exception("123云盘未返回有效的上传地址")
        return {str(key): str(value) for key, value in presigned_urls.items() if value}

    async def _upload_via_presigned_urls(
        self,
        *,
        local_path: str,
        file_size: int,
        upload_request: Dict[str, Any],
        progress_callback: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
        parent_id: str,
        target_name: str,
        file_md5: str,
        state_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
        completed_parts: Optional[List[int]] = None,
    ) -> None:
        chunk_size = self._get_123_upload_chunk_size()
        chunk_count = max(1, (file_size + chunk_size - 1) // chunk_size)
        batch_size = 10 if chunk_count > 1 else 1
        is_multipart = chunk_count > 1
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=60, sock_read=None)
        completed_set = set()
        for part_number in completed_parts or []:
            try:
                normalized_part = int(part_number)
            except (TypeError, ValueError):
                continue
            if 1 <= normalized_part <= chunk_count:
                completed_set.add(normalized_part)

        async with aiohttp.ClientSession(cookie_jar=aiohttp.DummyCookieJar(), timeout=timeout) as put_session:
            start = 1
            while start <= chunk_count:
                end_exclusive = min(start + batch_size, chunk_count + 1)
                presigned_urls = await self._get_123_presigned_urls(
                    upload_request,
                    start,
                    end_exclusive,
                    is_multipart,
                )

                for part_number in range(start, end_exclusive):
                    if part_number in completed_set:
                        continue
                    offset = (part_number - 1) * chunk_size
                    current_part_size = min(chunk_size, file_size - offset)
                    upload_url = presigned_urls.get(str(part_number), "")
                    if not upload_url:
                        raise Exception(f"123云盘未返回第 {part_number} 片上传地址")

                    with open(local_path, "rb") as fp:
                        fp.seek(offset)
                        payload = fp.read(current_part_size)

                    last_error = None
                    for attempt in range(2):
                        async with put_session.put(
                            upload_url,
                            data=payload,
                            headers={"Content-Length": str(current_part_size)},
                        ) as response:
                            if response.status == 200:
                                last_error = None
                                break

                            body = await response.text()
                            last_error = Exception(
                                f"123云盘上传分片 {part_number} 失败: HTTP {response.status}, {body}"
                            )
                            if response.status == 403 and attempt == 0:
                                refreshed_urls = await self._get_123_presigned_urls(
                                    upload_request,
                                    part_number,
                                    end_exclusive,
                                    is_multipart,
                                )
                                presigned_urls.update(refreshed_urls)
                                upload_url = presigned_urls.get(str(part_number), upload_url)
                                continue
                            break

                    if last_error:
                        raise last_error

                    uploaded_bytes = min(file_size, offset + current_part_size)
                    completed_set.add(part_number)
                    await self._persist_upload_resume_state(
                        state_callback,
                        parent_id=parent_id,
                        target_name=target_name,
                        file_size=file_size,
                        file_md5=file_md5,
                        upload_request=upload_request,
                        completed_parts=list(completed_set),
                    )
                    await self._notify_upload_progress(
                        progress_callback,
                        uploaded_bytes,
                        file_size,
                        f"正在上传到123云盘，分片（{part_number}/{chunk_count}）",
                    )

                start = end_exclusive

    def set_auth_manager(self, auth_manager):
        self._auth_manager = auth_manager
    
