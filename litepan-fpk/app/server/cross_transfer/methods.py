"""秒传方法注册表。

每种方法对应一种文件指纹（hash）。源盘需能提供该指纹，目标盘需支持该指纹秒传。
新增其它指纹（如 md5）时，只需在此登记一条，并让对应驱动在 DRIVER_INFO 声明
provide_hashes / rapid_upload 即可，无需改动公共层与前端。
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class TransferMethod:
    id: str
    label: str


TRANSFER_METHODS = {
    "sha1": TransferMethod(id="sha1", label="SHA1秒传"),
    "md5": TransferMethod(id="md5", label="MD5秒传"),
}


def get_method(method_id: str) -> TransferMethod:
    method = TRANSFER_METHODS.get(str(method_id or "").lower())
    if not method:
        raise ValueError(f"未知的秒传方法: {method_id}")
    return method
