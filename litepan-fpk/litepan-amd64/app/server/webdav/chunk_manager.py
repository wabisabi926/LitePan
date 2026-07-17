"""WebDAV 流式传输块大小：支持智能自适应、固定值、基于速率动态调整。"""

from config import config_manager
from core.log_manager import get_writer, LogModule


def get_webdav_logger():
    # 延迟初始化，避免模块导入期日志系统还没 ready
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


class StreamingChunkManager:
    def __init__(self):
        self.min_chunk_size = 64 * 1024
        self.max_chunk_size = 8 * 1024 * 1024
        self.default_chunk_size = 1024 * 1024

    async def get_chunk_size(self, file_size: int) -> int:
        smart_chunk_enabled = config_manager.get('webdav_smart_chunk_enabled')

        # 配置项历史上会被写成字符串 'True'/'False'，这里统一兜底
        if smart_chunk_enabled is None or smart_chunk_enabled == 'True':
            smart_chunk_enabled = True
        elif isinstance(smart_chunk_enabled, str):
            smart_chunk_enabled = smart_chunk_enabled.lower() == 'true'
        else:
            smart_chunk_enabled = bool(smart_chunk_enabled)

        if smart_chunk_enabled:
            chunk_size = await self.get_smart_chunk_size(file_size)
            webdav_logger.debug(f"智能块大小: {chunk_size // 1024}KB (文件: {file_size // 1024 // 1024}MB)")
            return chunk_size
        else:
            fixed_chunk_size = config_manager.get('webdav_chunk_size')
            try:
                if fixed_chunk_size is None:
                    chunk_size = 262144
                else:
                    chunk_size = int(fixed_chunk_size)
                chunk_size = max(self.min_chunk_size, min(chunk_size, self.max_chunk_size))
                webdav_logger.debug(f"固定块大小: {chunk_size // 1024}KB")
                return chunk_size
            except (ValueError, TypeError):
                webdav_logger.warning(f"无效的块大小配置: {fixed_chunk_size}，使用默认值")
                return self.default_chunk_size

    async def get_smart_chunk_size(self, file_size: int) -> int:
        if file_size < 1024 * 1024:
            return self.min_chunk_size
        elif file_size < 100 * 1024 * 1024:
            return self.default_chunk_size
        else:
            return self.max_chunk_size

    def adjust_chunk_size(self, current_size: int, speed_mbps: float, error_rate: float) -> int:
        """根据当前速率/错误率上下调整 chunk，错误率优先级最高。"""
        if error_rate > 0.1:
            new_size = max(current_size // 2, self.min_chunk_size)
            webdav_logger.debug(f"错误率过高({error_rate:.1%})，减小块大小: {current_size // 1024}KB -> {new_size // 1024}KB")
            return new_size

        if speed_mbps < 1.0:
            new_size = max(current_size // 2, self.min_chunk_size)
            webdav_logger.debug(f"网络速度慢({speed_mbps:.1f}Mbps)，减小块大小: {current_size // 1024}KB -> {new_size // 1024}KB")
        elif speed_mbps > 10.0:
            new_size = min(current_size * 2, self.max_chunk_size)
            webdav_logger.debug(f"网络速度快({speed_mbps:.1f}Mbps)，增大块大小: {current_size // 1024}KB -> {new_size // 1024}KB")
        else:
            new_size = current_size

        return new_size


chunk_manager = StreamingChunkManager()


def get_chunk_manager() -> StreamingChunkManager:
    return chunk_manager
