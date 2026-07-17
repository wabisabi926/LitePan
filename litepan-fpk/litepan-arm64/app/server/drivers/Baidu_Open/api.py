"""百度网盘 Open API 端点、错误码与辅助工具。
- 直链下载必须带 User-Agent: pan.baidu.com
"""

from typing import Dict, Any, Tuple


class BaiduOpenAPI:
    BASE_URL = "https://pan.baidu.com"
    PCS_BASE_URL = "https://d.pcs.baidu.com"
    USER_AGENT = "pan.baidu.com"
    UPLOAD_APP_ID = "250528"
    DEFAULT_UPLOAD_CHUNK_SIZE = 4 * 1024 * 1024

    ENDPOINTS = {
        "user_info":     "/rest/2.0/xpan/nas",
        "quota":         "/api/quota",
        "file_list":     "/rest/2.0/xpan/file",
        "file_listall":  "/rest/2.0/xpan/multimedia",
        "file_metas":    "/rest/2.0/xpan/multimedia",
        "file_search":   "/rest/2.0/xpan/file",
        "file_manager":  "/rest/2.0/xpan/file",
        "file_create":   "/rest/2.0/xpan/file",
        "file_precreate": "/rest/2.0/xpan/file",
        "locate_upload": "/rest/2.0/pcs/file",
        "superfile_upload": "/rest/2.0/pcs/superfile2",
    }

    HEADERS = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Connection": "keep-alive",
    }


class BaiduOpenApiHelper:
    # 错误码取自百度开放平台文档
    ERROR_MESSAGES: Dict[int, str] = {
        -1:    "权益已过期",
        -3:    "文件不存在",
        -6:    "身份验证失败（access_token无效或已过期）",
        -7:    "文件或目录名错误或无权访问",
        -8:    "文件或目录已存在",
        -9:    "文件或目录不存在",
        -10:   "云端容量已满",
        2:     "参数错误",
        6:     "不允许接入用户数据，建议10分钟后重试",
        10:    "转存文件已经存在",
        12:    "批量转存出错",
        111:   "有其他异步任务正在执行",
        133:   "播放广告（稍后重试）",
        255:   "转存数量太多",
        20011: "应用审核中，仅限前10个授权用户测试",
        20012: "访问超限，调用次数已达上限",
        20013: "权限不足，当前应用无接口权限",
        31023: "参数错误",
        31024: "没有访问权限",
        31034: "命中接口频控",
        31045: "access_token验证未通过",
        31061: "文件已存在",
        31062: "文件名无效",
        31064: "上传路径错误",
        31066: "文件名不存在",
        31190: "文件不存在（分片问题）",
        31299: "第一个分片的大小小于4MB",
        31326: "命中防盗链（检查User-Agent）",
        31360: "链接过期（dlink有效期8小时）",
        31362: "签名错误（检查链接完整性）",
        31363: "分片缺失",
        31364: "超出分片大小限制",
        31365: "文件总大小超限",
    }

    TOKEN_EXPIRED_ERRNOS = {-6, 31045}

    @staticmethod
    def build_headers() -> Dict[str, str]:
        return BaiduOpenAPI.HEADERS.copy()

    @staticmethod
    def build_params(operation: str, access_token: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        merged: Dict[str, Any] = {"access_token": access_token}

        # operation 映射成百度 API 必带的 method query 参数
        method_map = {
            "user_info":    "uinfo",
            "file_list":    "list",
            "file_listall": "listall",
            "file_metas":   "filemetas",
            "file_search":  "search",
            "file_manager": "filemanager",
            "file_create":  "create",
            "file_precreate": "precreate",
            "locate_upload": "locateupload",
            "superfile_upload": "upload",
        }
        if operation in method_map:
            merged["method"] = method_map[operation]

        if operation == "user_info":
            merged["vip_version"] = "v2"

        if params:
            merged.update(params)

        return {k: v for k, v in merged.items() if v is not None}

    @staticmethod
    def check_success(data: Dict[str, Any]) -> Tuple[bool, str, int]:
        """返回 (success, error_message, errno_int)。"""
        errno = data.get("errno", 0)
        if errno in (0, "0", None):
            return True, "", 0
        try:
            errno_int = int(errno)
        except (TypeError, ValueError):
            errno_int = -1

        error_message = (
            data.get("errmsg")
            or data.get("error_msg")
            or BaiduOpenApiHelper.ERROR_MESSAGES.get(errno_int)
            or f"未知错误 {errno}"
        )
        return False, f"百度网盘API错误: {errno_int} ({error_message})", errno_int

    @staticmethod
    def is_token_expired(errno_value: int, error_msg: str) -> bool:
        if errno_value in BaiduOpenApiHelper.TOKEN_EXPIRED_ERRNOS:
            return True
        # 有时百度会返回英文提示而不是标准错误码
        token_keywords = ("expired_token", "invalid_token", "Access token invalid", "expired access", "Invalid Bduss")
        return any(kw.lower() in error_msg.lower() for kw in token_keywords)
