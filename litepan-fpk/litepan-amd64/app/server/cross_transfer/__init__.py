"""跨盘秒传：基于文件指纹的跨网盘秒传能力。

- methods：可插拔的秒传方法（sha1 / md5 / ...）
- routes：由各驱动声明的能力推导出可行线路（卡片）
- service：扫描源目录指纹、试探可秒传、执行秒传
"""

from .methods import TRANSFER_METHODS, get_method
from .routes import build_routes
from .service import scan_source, probe_stream, execute_stream, execute_transfer

__all__ = [
    "TRANSFER_METHODS",
    "get_method",
    "build_routes",
    "scan_source",
    "probe_stream",
    "execute_stream",
    "execute_transfer",
]
