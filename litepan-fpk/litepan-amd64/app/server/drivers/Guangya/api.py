from typing import Any, Dict


class GuangYaAPI:
    ACCOUNT_BASE_URL = "https://account.guangyapan.com"
    API_BASE_URL = "https://api.guangyapan.com"
    WEB_BASE_URL = "https://www.guangyapan.com"

    ENDPOINTS = {
        "user_me": "/v1/user/me",
        "file_list": "/userres/v1/file/get_file_list",
        "file_detail": "/userres/v1/file/get_file_detail",
        "download_url": "/userres/v1/get_res_download_url",
        "upload_token": "/userres/v1/get_res_center_token",
        "check_can_flash_upload": "/userres/v1/check_can_flash_upload",
        "upload_task_info": "/userres/v1/file/get_info_by_task_id",
        "create_dir": "/userres/v1/file/create_dir",
        "rename": "/userres/v1/file/rename",
        "move_file": "/userres/v1/file/move_file",
        "delete_file": "/userres/v1/file/delete_file",
        "copy_file": "/userres/v1/file/copy_file",
        "task_status": "/userres/v1/get_task_status",
    }


class GuangYaApiHelper:
    @staticmethod
    def build_account_headers(client_id: str, device_id: str) -> Dict[str, str]:
        return {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "X-Device-Model": "chrome%2F147.0.0.0",
            "X-Device-Name": "PC-Chrome",
            "X-Device-Sign": f"wdi10.{device_id}xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "X-Net-Work-Type": "NONE",
            "X-OS-Version": "MacIntel",
            "X-Platform-Version": "1",
            "X-Protocol-Version": "301",
            "X-Provider-Name": "NONE",
            "X-SDK-Version": "9.0.2",
            "X-Client-Id": client_id,
            "X-Client-Version": "0.0.1",
            "X-Device-Id": device_id,
        }

    @staticmethod
    def build_api_headers(device_id: str, access_token: str = "") -> Dict[str, str]:
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Content-Type": "application/json",
            "Did": device_id,
            "Dt": "4",
            "Origin": GuangYaAPI.WEB_BASE_URL,
            "Referer": GuangYaAPI.WEB_BASE_URL + "/",
        }
        if access_token:
            headers["Authorization"] = f"Bearer {access_token}"
        return headers

    @staticmethod
    def get_error_message(data: Dict[str, Any], fallback: str) -> str:
        if not isinstance(data, dict):
            return fallback
        return (
            str(data.get("error_description") or "").strip()
            or str(data.get("error") or "").strip()
            or str(data.get("msg") or "").strip()
            or fallback
        )
