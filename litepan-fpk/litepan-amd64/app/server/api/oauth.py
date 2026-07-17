"""OAuth 代理：把前端请求转发到外部 OAuth 服务。"""

import asyncio
import aiohttp
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from typing import Optional
from config import get_oauth_server_url

router = APIRouter()

def get_client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"


async def _oauth_request_with_retry(
    method: str,
    url: str,
    *,
    headers: dict = None,
    data: bytes = None,
    max_retries: int = 2,
    timeout_per_attempt: float = 5.0,
    connect_timeout: float = 3.0,
    retry_delay: float = 1.0,
    error_label: str = "OAuth服务",
) -> dict:
    """OAuth 代理带重试。网络类异常重试，非网络异常直接 503；最终返回 {status, data}。"""
    last_error = None

    for attempt in range(1 + max_retries):
        try:
            timeout = aiohttp.ClientTimeout(
                total=timeout_per_attempt,
                connect=connect_timeout,
            )
            async with aiohttp.ClientSession(timeout=timeout) as session:
                kwargs = {"headers": headers} if headers else {}
                if data is not None:
                    kwargs["data"] = data

                async with session.request(method, url, **kwargs) as response:
                    response_data = await response.json()
                    return {"status": response.status, "data": response_data}

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            last_error = e
            if attempt < max_retries:
                await asyncio.sleep(retry_delay)

        except Exception as e:
            # 非网络异常不走重试，避免业务错误被反复打到外部
            raise HTTPException(status_code=503, detail=f"{error_label}请求失败: {type(e).__name__}")

    raise HTTPException(
        status_code=503,
        detail=f"{error_label}暂时不可用（已重试{max_retries}次），请稍后再试或手动输入Token",
    )


@router.get("/api/oauth/check-service")
async def check_oauth_service(request: Request):
    client_ip = get_client_ip(request)

    result = await _oauth_request_with_retry(
        "GET",
        f"{get_oauth_server_url()}/api/oauth/check-service",
        headers={"X-Forwarded-For": client_ip},
        max_retries=2,
        timeout_per_attempt=5.0,
        connect_timeout=3.0,
        retry_delay=1.0,
        error_label="OAuth服务",
    )

    if result["status"] == 200:
        return result["data"]
    raise HTTPException(status_code=503, detail="OAuth服务不可用")


@router.post("/api/oauth/start")
async def start_oauth(request: Request):
    body = await request.body()
    client_ip = get_client_ip(request)

    # 把本地驱动名改写成 OAuth 服务识别的中文名，并补 callback_url；解析失败就原样转发
    import json
    try:
        request_data = json.loads(body)
        driver_type = request_data.get('driver_type', '')

        driver_type_mapping = {
            "115_open": "115网盘Open",
            "115_Open": "115网盘Open",
            "baidu_open": "百度网盘Open",
            "Baidu_Open": "百度网盘Open",
            "123_open": "123云盘Open",
            "123_Open": "123云盘Open",
            "pan123_open": "123云盘Open",
            "onedrive": "OneDrive",
            "OneDrive": "OneDrive",
            "onedrive_open": "OneDrive",
            "OneDrive_Open": "OneDrive",
            "guangya": "光鸭云盘"
        }

        if driver_type in driver_type_mapping:
            request_data['driver_type'] = driver_type_mapping[driver_type]
        request_data.setdefault('callback_url', f"{get_oauth_server_url()}/callback-popup")
        body = json.dumps(request_data).encode()
    except Exception:
        pass

    result = await _oauth_request_with_retry(
        "POST",
        f"{get_oauth_server_url()}/api/oauth/start",
        headers={
            "Content-Type": "application/json",
            "X-Forwarded-For": client_ip,
        },
        data=body,
        max_retries=2,
        timeout_per_attempt=8.0,
        connect_timeout=3.0,
        retry_delay=1.0,
        error_label="OAuth认证启动",
    )

    if result["status"] == 200:
        return result["data"]
    return JSONResponse(status_code=result["status"], content=result["data"])


@router.get("/api/oauth/status/{session_id}")
async def check_oauth_status(session_id: str, request: Request):
    client_ip = get_client_ip(request)

    result = await _oauth_request_with_retry(
        "GET",
        f"{get_oauth_server_url()}/api/oauth/status/{session_id}",
        headers={"X-Forwarded-For": client_ip},
        max_retries=2,
        timeout_per_attempt=5.0,
        connect_timeout=2.0,
        retry_delay=0.5,
        error_label="OAuth状态查询",
    )

    if result["status"] == 200:
        return result["data"]
    return JSONResponse(status_code=result["status"], content=result["data"])


@router.post("/api/oauth/confirm-received/{session_id}")
async def confirm_token_received(session_id: str, request: Request):
    client_ip = get_client_ip(request)

    result = await _oauth_request_with_retry(
        "POST",
        f"{get_oauth_server_url()}/api/oauth/confirm-received/{session_id}",
        headers={"X-Forwarded-For": client_ip},
        max_retries=2,
        timeout_per_attempt=5.0,
        connect_timeout=3.0,
        retry_delay=1.0,
        error_label="Token确认",
    )

    if result["status"] == 200:
        return result["data"]
    return JSONResponse(status_code=result["status"], content=result["data"])


@router.post("/api/oauth/refresh")
async def refresh_oauth_token(request: Request):
    body = await request.body()
    client_ip = get_client_ip(request)

    result = await _oauth_request_with_retry(
        "POST",
        f"{get_oauth_server_url()}/api/oauth/refresh",
        headers={
            "Content-Type": "application/json",
            "X-Forwarded-For": client_ip,
        },
        data=body,
        max_retries=2,
        timeout_per_attempt=10.0,
        connect_timeout=3.0,
        retry_delay=1.5,
        error_label="Token刷新",
    )

    if result["status"] == 200:
        return result["data"]
    return JSONResponse(status_code=result["status"], content=result["data"])


@router.get("/api/oauth/quick-auth/{driver_type}")
async def quick_oauth_auth(driver_type: str, request: Request):
    """不经过 start 流程，直接给前端一个 OAuth 服务的 quick-start URL。"""
    try:
        driver_type_mapping = {
            "115_open": "115网盘Open",
            "115_Open": "115网盘Open",
            "baidu_open": "百度网盘Open",
            "Baidu_Open": "百度网盘Open",
            "123_open": "123云盘Open",
            "123_Open": "123云盘Open",
            "pan123_open": "123云盘Open",
            "onedrive": "OneDrive",
            "OneDrive": "OneDrive",
            "onedrive_open": "OneDrive",
            "OneDrive_Open": "OneDrive",
            "guangya": "光鸭云盘"
        }

        mapped_driver_type = driver_type_mapping.get(driver_type, driver_type)

        oauth_url = f"{get_oauth_server_url()}/quick-start?driver_type={mapped_driver_type}&server_use=true"

        return JSONResponse(content={
            "success": True,
            "data": {
                "oauth_url": oauth_url
            }
        })

    except Exception as e:
        raise HTTPException(status_code=503, detail="OAuth服务连接失败")
