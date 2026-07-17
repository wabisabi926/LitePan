import asyncio
import json
import threading
import uuid
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple

from mediaorganize import rules


TMDB_REQUEST_TIMEOUT_SECONDS = 10
_tmdb_lock = threading.Lock()


@dataclass
class PlanAction:
    id: str
    kind: str
    source_id: str = ""
    source_name: str = ""
    source_parent_id: str = ""
    target_parent_id: str = ""
    target_name: str = ""
    reason: str = ""
    confidence: float = 0.0
    depends_on: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    status: str = "pending"
    error: Optional[str] = None
    resolved_id: str = ""
    executed_at: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "PlanAction":
        return cls(**{k: data.get(k) for k in cls.__dataclass_fields__.keys() if data.get(k) is not None})


@dataclass
class Plan:
    task_id: str
    created_at: str
    target_root_id: str
    target_parent_id: str
    actions: List[PlanAction] = field(default_factory=list)
    skipped: List[Dict[str, Any]] = field(default_factory=list)
    diagnostics: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "created_at": self.created_at,
            "target_root_id": self.target_root_id,
            "target_parent_id": self.target_parent_id,
            "actions": [a.to_dict() for a in self.actions],
            "skipped": list(self.skipped),
            "diagnostics": dict(self.diagnostics),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Plan":
        plan = cls(
            task_id=data.get("task_id", ""),
            created_at=data.get("created_at", ""),
            target_root_id=data.get("target_root_id", ""),
            target_parent_id=data.get("target_parent_id", ""),
        )
        plan.actions = [PlanAction.from_dict(a) for a in data.get("actions") or []]
        plan.skipped = list(data.get("skipped") or [])
        plan.diagnostics = dict(data.get("diagnostics") or {})
        return plan


def _search_tmdb_sync(
    query: str,
    year: Optional[int],
    language: str,
    api_key: str,
    proxy_url: str,
    media_type: str,
) -> list:
    try:
        import tmdbsimple as tmdb
        import requests as _requests
    except Exception:
        return []

    class TimeoutSession(_requests.Session):
        def request(self, method, url, **kwargs):
            kwargs.setdefault("timeout", TMDB_REQUEST_TIMEOUT_SECONDS)
            return super().request(method, url, **kwargs)

    if api_key:
        tmdb.API_KEY = api_key
    session = TimeoutSession()
    if proxy_url:
        session.proxies = {"http": proxy_url, "https": proxy_url}
    with _tmdb_lock:
        old_session = tmdb.REQUESTS_SESSION
        tmdb.REQUESTS_SESSION = session
        try:
            search = tmdb.Search()
            normalized = (media_type or "movie").lower()
            if normalized == "tv":
                params = {"query": query, "language": language}
                if year:
                    params["first_air_date_year"] = str(year)
                response = search.tv(**params)
                return response.get("results", [])[:10]
            params = {"query": query, "language": language}
            if year:
                params["year"] = str(year)
            response = search.movie(**params)
            results = response.get("results", [])[:10]
            if results or normalized != "auto":
                return results
            tv_params = {"query": query, "language": language}
            if year:
                tv_params["first_air_date_year"] = str(year)
            response_tv = search.tv(**tv_params)
            return response_tv.get("results", [])[:10]
        except Exception:
            return []
        finally:
            tmdb.REQUESTS_SESSION = old_session


async def search_tmdb_async(query: str, year, language, api_key, proxy_url, media_type) -> list:
    return await asyncio.to_thread(_search_tmdb_sync, query, year, language, api_key, proxy_url, media_type)


def _lookup_tmdb_by_id_sync(
    tmdb_id: str,
    language: str,
    api_key: str,
    proxy_url: str,
    media_type: str,
) -> Optional[dict]:
    try:
        import tmdbsimple as tmdb
        import requests as _requests
    except Exception:
        return None
    try:
        tid = int(str(tmdb_id).strip())
    except Exception:
        return None
    if tid <= 0:
        return None

    class TimeoutSession(_requests.Session):
        def request(self, method, url, **kwargs):
            kwargs.setdefault("timeout", TMDB_REQUEST_TIMEOUT_SECONDS)
            return super().request(method, url, **kwargs)

    if api_key:
        tmdb.API_KEY = api_key
    session = TimeoutSession()
    if proxy_url:
        session.proxies = {"http": proxy_url, "https": proxy_url}
    with _tmdb_lock:
        old_session = tmdb.REQUESTS_SESSION
        tmdb.REQUESTS_SESSION = session
        try:
            normalized = (media_type or "movie").lower()
            try:
                if normalized == "tv":
                    return tmdb.TV(tid).info(language=language)
                return tmdb.Movies(tid).info(language=language)
            except Exception:
                try:
                    if normalized == "tv":
                        return tmdb.Movies(tid).info(language=language)
                    return tmdb.TV(tid).info(language=language)
                except Exception:
                    return None
        finally:
            tmdb.REQUESTS_SESSION = old_session


async def lookup_tmdb_by_id_async(tmdb_id, language, api_key, proxy_url, media_type) -> Optional[dict]:
    return await asyncio.to_thread(
        _lookup_tmdb_by_id_sync, tmdb_id, language, api_key, proxy_url, media_type
    )


def _fetch_tv_seasons_sync(tmdb_id: str, language: str, api_key: str, proxy_url: str) -> list:
    try:
        import tmdbsimple as tmdb
        import requests as _requests
    except Exception:
        return []
    try:
        tid = int(str(tmdb_id).strip())
    except Exception:
        return []
    if tid <= 0:
        return []

    class TimeoutSession(_requests.Session):
        def request(self, method, url, **kwargs):
            kwargs.setdefault("timeout", TMDB_REQUEST_TIMEOUT_SECONDS)
            return super().request(method, url, **kwargs)

    if api_key:
        tmdb.API_KEY = api_key
    session = TimeoutSession()
    if proxy_url:
        session.proxies = {"http": proxy_url, "https": proxy_url}
    with _tmdb_lock:
        old_session = tmdb.REQUESTS_SESSION
        tmdb.REQUESTS_SESSION = session
        try:
            info = tmdb.TV(tid).info(language=language)
            seasons = info.get("seasons") if isinstance(info, dict) else None
            return list(seasons or [])
        except Exception:
            return []
        finally:
            tmdb.REQUESTS_SESSION = old_session


async def fetch_tv_seasons_async(tmdb_id, language, api_key, proxy_url) -> list:
    return await asyncio.to_thread(
        _fetch_tv_seasons_sync, tmdb_id, language, api_key, proxy_url
    )


async def validate_tmdb_connection(api_key: str, language: str, proxy_url: str) -> bool:
    def _check() -> bool:
        try:
            import tmdbsimple as tmdb
            import requests as _requests
        except Exception:
            return False

        class TimeoutSession(_requests.Session):
            def request(self, method, url, **kwargs):
                kwargs.setdefault("timeout", TMDB_REQUEST_TIMEOUT_SECONDS)
                return super().request(method, url, **kwargs)

        tmdb.API_KEY = api_key
        session = TimeoutSession()
        if proxy_url:
            session.proxies = {"http": proxy_url, "https": proxy_url}
        with _tmdb_lock:
            old_session = tmdb.REQUESTS_SESSION
            tmdb.REQUESTS_SESSION = session
            try:
                tmdb.Search().movie(query="test", language=language)
                return True
            except Exception:
                return False
            finally:
                tmdb.REQUESTS_SESSION = old_session

    return await asyncio.to_thread(_check)


def build_proxy_url(settings: dict) -> str:
    if not rules.setting_bool(settings.get("proxy_enabled")):
        return ""
    url = (settings.get("proxy_url") or "").strip()
    if not url:
        return ""
    user = (settings.get("proxy_username") or "").strip()
    pwd = (settings.get("proxy_password") or "").strip()
    if user and pwd:
        import re as _re
        m = _re.match(r'^(https?://)(.+)$', url)
        if m:
            url = f"{m.group(1)}{user}:{pwd}@{m.group(2)}"
    return url


async def probe_media_info_with_ffprobe(
    driver, file_item, timeout_seconds: int = 30
) -> Tuple[dict, Optional[dict]]:
    try:
        from core.driver_service import build_upstream_download_headers, resolve_download
    except Exception as e:
        return {}, {"error": f"无法导入下载工具: {e}"}

    try:
        resolved = await resolve_download(driver, file_item.id, "", file_info=file_item)
        if not resolved.download_url:
            return {}, None
        headers = await build_upstream_download_headers(driver, file_item.id, "", prefer_identity=True)
        header_text = "".join(f"{key}: {value}\r\n" for key, value in (headers or {}).items())
        cmd = ["ffprobe", "-v", "error", "-print_format", "json", "-show_streams"]
        if header_text:
            cmd.extend(["-headers", header_text])
        cmd.append(resolved.download_url)
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=max(int(timeout_seconds or 30), 5))
        if proc.returncode != 0:
            message = (stderr or b"").decode("utf-8", errors="ignore").strip()
            return {}, {"error": message or f"ffprobe exited {proc.returncode}"}
        data = json.loads(stdout.decode("utf-8", errors="ignore") or "{}")
        result: Dict[str, Any] = {}
        for stream in data.get("streams") or []:
            if stream.get("codec_type") == "video" and not result.get("video_codec"):
                screen_size = rules.screen_size_from_dimensions(stream.get("width"), stream.get("height"))
                if screen_size:
                    result["screen_size"] = screen_size
                frame_rate = stream.get("avg_frame_rate") or stream.get("r_frame_rate")
                if frame_rate:
                    result["frame_rate"] = frame_rate
                codec = stream.get("codec_name")
                if codec:
                    result["video_codec"] = codec
            elif stream.get("codec_type") == "audio" and not result.get("audio_codec"):
                codec = stream.get("codec_name")
                channels = rules.audio_channels_label(stream.get("channels"))
                if codec:
                    result["audio_codec"] = codec
                if channels:
                    result["audio_channels"] = channels
        return result, data
    except asyncio.TimeoutError:
        return {}, {"error": "ffprobe 超时"}
    except FileNotFoundError:
        return {}, {"error": "未找到 ffprobe 可执行文件"}
    except Exception as e:
        return {}, {"error": str(e)}


class PlannerStopped(Exception):
    pass


class Planner:
    def __init__(
        self,
        driver,
        task_config: Dict[str, Any],
        settings: Dict[str, Any],
        task_id: str,
        log_fn: Callable[[str], None],
        check_stop: Callable[[], None],
        progress_fn: Optional[Callable[[Dict[str, Any]], None]] = None,
    ):
        self.driver = driver
        self.cfg = task_config
        self.settings = settings or {}
        self.task_id = task_id
        self.log = log_fn
        self.check_stop = check_stop
        self.progress_fn = progress_fn

        self.action_seq = 0
        self.actions: List[PlanAction] = []
        self.skipped_items: List[Dict[str, Any]] = []
        self.diagnostics: Dict[str, Any] = {"groups": []}

        self.scanned_dirs = 0
        self.scanned_dir_names: Dict[str, str] = {}
        self.scanned_dir_parents: Dict[str, str] = {}
        self.scanned_files = 0
        self.processed_batches = 0
        self.current_dir = ""

        try:
            self.max_works_per_run = int(self.settings.get("max_works_per_run") or 0)
        except Exception:
            self.max_works_per_run = 0
        self.planned_work_count = 0
        self.quota_reached = False

        self.media_exts = rules.parse_extension_set(
            self.settings.get("file_extensions") or self.cfg.get("file_extensions") or rules.DEFAULT_MEDIA_EXTENSIONS
        )
        self.meta_exts = rules.parse_extension_set(
            self.settings.get("metadata_extensions") or self.cfg.get("metadata_extensions") or rules.DEFAULT_METADATA_EXTENSIONS
        )
        try:
            self.media_tag_order = json.loads(self.settings.get("media_tag_order") or "[]") or rules.DEFAULT_MEDIA_TAG_ORDER
        except Exception:
            self.media_tag_order = rules.DEFAULT_MEDIA_TAG_ORDER
        self.align_media_tags = rules.setting_bool(self.settings.get("align_media_tags"), False)

        self.action_type = (self.cfg.get("action_type") or "move").lower()
        self.marker = (self.cfg.get("rename_marker") or "") if self.action_type == "rename" else ""
        self.task_media_type = (self.cfg.get("media_type") or "auto").lower()
        self.recursive = bool(self.cfg.get("recursive", True))
        self.overwrite = bool(self.settings.get("overwrite_existing", False))
        self.season_folder_tpl = self.cfg.get("season_folder_template") or "Season {season:02d}"
        self.parent_id = str(self.cfg.get("target_directory_id") or self.cfg.get("target_directory") or "")
        self.target_root_id = str(self.cfg.get("target_root_id") or "") if self.action_type == "move" else ""

        self.use_tmdb = bool(self.cfg.get("use_tmdb", True))
        self.use_ffprobe = bool(self.cfg.get("use_ffprobe", False))

        self.tmdb_api_key = self.settings.get("tmdb_api_key") or ""
        self.tmdb_lang = self.settings.get("tmdb_language") or "zh-CN"
        self.tmdb_interval = (self.settings.get("tmdb_request_interval_ms") or 250) / 1000.0
        self.tmdb_proxy = build_proxy_url(self.settings)
        self.tmdb_available = False

        self.ffprobe_interval = (self.settings.get("ffprobe_request_interval_ms") or 3000) / 1000.0
        self.ffprobe_timeout = int(self.settings.get("ffprobe_timeout_seconds") or 30)
        self.ffprobe_concurrency = max(1, int(self.settings.get("ffprobe_concurrency") or 2))
        self._tv_seasons_cache: Dict[str, list] = {}

    def _next_id(self) -> str:
        self.action_seq += 1
        return f"a{self.action_seq}"

    def _add(self, action: PlanAction) -> PlanAction:
        self.actions.append(action)
        return action

    async def build(self) -> Plan:
        await self._validate_tmdb()
        self.check_stop()

        if not self.parent_id:
            return self._finalize()

        await self._clear_dir_cache()
        await self._scan_and_plan(self.parent_id)
        self._try_whole_dir_move_optimization()
        self._detect_same_work_dir_conflicts()
        self._plan_empty_dir_cleanup()
        return self._finalize()

    def _detect_same_work_dir_conflicts(self):
        """检测「多个源作品目录撞同一目标名」的冲突，**自动合并文件**到第一个目录。

        典型场景：rename 模式下，两个不同的源作品目录被识别为同一作品
        （如「千与千寻 蓝光原盘」和「[4K]...千与千寻....mkv」都指向 tmdb-129）。

        新策略：
        - 保留第一个 group 的作品目录改名
        - 后续 group 的目录改名 action 标 skipped（目录本身不能合并）
        - 后续 group 内部的**文件** relocate action **重定向**到第一个目录
          → 等价于跨目录搬运 + 改名，自动合并
        - 跳过列表只列**目录本身**一条，不再列内部文件（合并不是跳过）
        - 文件名仍有冲突时（如同 1080p 撞名），由 executor prescan 兜底处理
        """
        dir_rename_actions = [
            a for a in self.actions
            if a.kind == "relocate"
            and isinstance(a.metadata, dict)
            and a.metadata.get("kind_label") == "dir_rename"
        ]

        by_target: Dict[Tuple[str, str], List[PlanAction]] = {}
        for a in dir_rename_actions:
            key = (str(a.target_parent_id), a.target_name)
            by_target.setdefault(key, []).append(a)

        for (parent, target_name), conflicts in by_target.items():
            if len(conflicts) < 2:
                continue
            winning = conflicts[0]
            winning_dir_id = str(winning.source_id)
            for losing in conflicts[1:]:
                losing_dir_id = str(losing.source_id)
                losing.status = "skipped"
                losing.error = f"作品已在「{winning.source_name}」整理，文件已自动并入"
                for fa in self.actions:
                    if fa is losing or fa is winning:
                        continue
                    if fa.kind != "relocate":
                        continue
                    if str(fa.source_parent_id) != losing_dir_id:
                        continue
                    fa.target_parent_id = winning_dir_id
                    deps = list(fa.depends_on or [])
                    if winning.id not in deps:
                        deps.append(winning.id)
                    fa.depends_on = deps
                    fa.reason = (fa.reason or "") + f"（从「{losing.source_name}」合并到「{target_name}」）"
                self.skipped_items.append({
                    "file_id": str(losing.source_id),
                    "file_name": losing.source_name,
                    "reason": f"已合并到「{target_name}」（同一部作品，文件已自动并入，空目录将清理）",
                })
                self.log(
                    f"[计划] 同作品合并：「{losing.source_name}」内文件自动并入"
                    f"「{winning.source_name}」（目标「{target_name}」）"
                )

        if self.action_type != "move":
            return

        work_dir_actions = [
            a for a in self.actions
            if a.kind in ("ensure_dir", "move_and_rename_dir")
            and isinstance(a.metadata, dict)
            and a.metadata.get("is_work_dir")
        ]
        by_work_target: Dict[Tuple[str, str], List[PlanAction]] = {}
        for a in work_dir_actions:
            key = (str(a.target_parent_id), a.target_name)
            by_work_target.setdefault(key, []).append(a)

        for (parent, target_name), conflicts in by_work_target.items():
            if len(conflicts) < 2:
                continue
            winning = conflicts[0]
            winning_ref = f"ref:{winning.id}"
            winning_source_id = str((winning.metadata or {}).get("source_dir_id") or winning.source_id or "")
            for losing in conflicts[1:]:
                losing_source_id = str((losing.metadata or {}).get("source_dir_id") or losing.source_id or "")
                losing.status = "skipped"
                losing.error = f"已并入「{target_name}」"
                if not losing_source_id or losing_source_id == winning_source_id:
                    continue
                for fa in self.actions:
                    if fa.kind != "relocate":
                        continue
                    if str(fa.source_parent_id) != losing_source_id:
                        continue
                    fa.target_parent_id = winning_ref
                    deps = list(fa.depends_on or [])
                    if winning.id not in deps:
                        deps.append(winning.id)
                    fa.depends_on = deps
                    fa.reason = (fa.reason or "") + f"（合并到「{target_name}」）"
                self.skipped_items.append({
                    "file_id": losing_source_id,
                    "file_name": self.scanned_dir_names.get(losing_source_id, losing_source_id),
                    "reason": f"已合并到「{target_name}」；源目录内文件将自动搬入该目录",
                })
                self.log(
                    f"[计划] move 同作品合并：源 {losing_source_id} 的文件并入"
                    f"「{target_name}」（{winning.id}）"
                )

    def _try_whole_dir_move_optimization(self):
        """整体移动文件夹优化：将 ensure_dir 升级为 move_and_rename_dir。

        分两个层级独立判断：

        **A. work_dir 级**（适合扁平源剧集）
        源目录里所有 relocate 都同源 + 源里仅文件无子目录（executor 兜底）
        → 整体 move 源剧集目录到目标根 + rename
        示例：源 `庆余年/[40 个 mp4]` → 1 次 move + 1 次 rename

        **B. season_dir 级**（适合源已分季的剧集）
        某季对应的所有 relocate 都来自同一个 source_dir，且该 source_dir
        不是 work_dir（即用户已有"第一季"这种子目录）
        → 整体 move 该季子目录到目标 work_dir + rename 为 Season NN
        示例：源 `暗河传 2025/第一季/[40 个 mp4]` → 1 次 move + 1 次 rename

        executor 运行时还会做兜底检查：list 一次确认无非计划文件，
        否则降级为 create_folder（行为等同原 ensure_dir）。
        """
        if self.action_type != "move":
            return

        # ============ A. work_dir 级优化 ============
        work_dir_actions = [
            a for a in self.actions
            if a.kind == "ensure_dir"
            and isinstance(a.metadata, dict)
            and a.metadata.get("is_work_dir")
        ]
        for wa in work_dir_actions:
            sdid = (wa.metadata or {}).get("source_dir_id")
            if not sdid or str(sdid) == str(self.parent_id):
                continue
            # 检查 source_dir 下所有 relocate 同源
            relocates_from_this_source = [
                a for a in self.actions
                if a.kind == "relocate" and str(a.source_parent_id) == str(sdid)
            ]
            if not relocates_from_this_source:
                continue
            self._upgrade_to_whole_move(wa, str(sdid))

        # ============ B. season_dir 级优化 ============
        season_dir_actions = [
            a for a in self.actions
            if a.kind == "ensure_dir"
            and isinstance(a.metadata, dict)
            and a.metadata.get("is_season_dir")
        ]
        for sa in season_dir_actions:
            # 找该 season 下的 relocate
            target_ref = f"ref:{sa.id}"
            relocates_under = [
                a for a in self.actions
                if a.kind == "relocate" and a.target_parent_id == target_ref
            ]
            if not relocates_under:
                continue
            # 检查它们是否都同源
            source_dirs = {str(a.source_parent_id) for a in relocates_under}
            if len(source_dirs) != 1:
                continue
            sdid = next(iter(source_dirs))
            if not sdid or str(sdid) == str(self.parent_id):
                continue
            # 该 source_dir 不能是某个 work_dir 优化的目标（避免重复优化同一目录）
            already_used = any(
                a.kind == "move_and_rename_dir" and str(a.source_id) == sdid
                for a in self.actions
            )
            if already_used:
                continue
            # 该 source_dir 的所有 relocate 必须都指向当前 season（不能跨季）
            all_from_source = [
                a for a in self.actions
                if a.kind == "relocate" and str(a.source_parent_id) == sdid
            ]
            all_targets = {a.target_parent_id for a in all_from_source}
            if all_targets != {target_ref}:
                continue
            self._upgrade_to_whole_move(sa, sdid)

    def _upgrade_to_whole_move(self, ensure_action: PlanAction, source_dir_id: str):
        """把 ensure_dir 升级为 move_and_rename_dir。executor 运行时若发现源不纯净会自动降级。"""
        ensure_action.kind = "move_and_rename_dir"
        ensure_action.source_id = str(source_dir_id)
        ensure_action.source_name = self.scanned_dir_names.get(str(source_dir_id), "")
        ensure_action.metadata = dict(ensure_action.metadata or {})
        ensure_action.metadata["whole_dir_optimization"] = True
        ensure_action.reason = f"整体移动源目录 → {ensure_action.target_name}"

    def _plan_empty_dir_cleanup(self):
        """整理完成后，自动清理被掏空的源目录链。

        规则：
        - move 模式：清理所有掏空的源目录链
        - rename 模式：清理"同作品合并后被掏空"的源目录（关键场景：两个千与千寻合并后第二个目录变空）
        - 不删扫描根（用户指定的 target_directory_id）
        - 从「relocate 的 source_parent」和「move_and_rename_dir 搬走的 source 原父」向上递归到扫描根
        - 沿链生成 delete_empty_dir，子目录的 delete 先于父目录执行（depends_on）
        - executor 真正删除前会再 list 一次确认确实为空（有用户文件就不删）
        """
        if self.action_type not in ("move", "rename"):
            return

        # 先扩大候选清理目录，最终是否删除由 executor 的空目录校验兜底。
        dir_relocate_sources: Set[str] = {
            str(action.source_id or "")
            for action in self.actions
            if action.kind == "relocate"
            and isinstance(action.metadata, dict)
            and action.metadata.get("kind_label") in ("dir_rename", "season_dir_rename")
            and action.source_id
            and action.status != "skipped"
        }
        starts: Set[str] = set()
        stop_at: Dict[str, str] = {}
        for action in self.actions:
            if action.kind == "relocate":
                sp = str(action.source_parent_id)
                if not sp or sp == str(self.parent_id):
                    continue
                if str(action.target_parent_id) == sp:
                    continue
                if sp in dir_relocate_sources:
                    continue
                starts.add(sp)
                target_parent = str(action.target_parent_id or "")
                if target_parent and self._is_scanned_ancestor(target_parent, sp):
                    stop_at[sp] = target_parent
                # 该 source 的原父目录也可能因为本 source 被清空而变空 → 向上递归
                pp = self.scanned_dir_parents.get(sp)
                if (
                    pp
                    and pp != str(self.parent_id)
                    and str(action.target_parent_id) != str(pp)
                ):
                    starts.add(pp)
            elif action.kind == "move_and_rename_dir":
                sid = str(action.source_id or "")
                if not sid:
                    continue
                pp = self.scanned_dir_parents.get(sid)
                if pp and pp != str(self.parent_id):
                    starts.add(pp)

        # 沿链向上递归，把所有可能变空的目录都收入待清理
        chain_to_clean: Set[str] = set()
        for start in starts:
            cur = start
            stop_dir = stop_at.get(start, "")
            depth = 0
            while cur and cur != str(self.parent_id):
                if stop_dir and cur == stop_dir:
                    break
                chain_to_clean.add(cur)
                cur = self.scanned_dir_parents.get(cur)
                depth += 1
                if depth > 50:
                    break  # 防御性，避免恶意循环

        if not chain_to_clean:
            return

        # 按深度倒排：先生成最深的 delete_empty_dir，让 depends_on 链建立起来
        def _depth(d: str) -> int:
            depth = 0
            cur = d
            while cur and cur != str(self.parent_id):
                cur = self.scanned_dir_parents.get(cur)
                depth += 1
                if depth > 50:
                    break
            return depth

        sorted_dirs = sorted(chain_to_clean, key=_depth, reverse=True)
        dir_to_delete_id: Dict[str, str] = {}
        for d in sorted_dirs:
            # 该 dir 的 delete 依赖：
            #   - 它的直接 source_parent 的 relocate（所有内部文件被搬走才可能空）
            #   - 它的子目录的 delete_empty_dir（子级清理完才轮到自己）
            #   - 它的子目录的 move_and_rename_dir（子级被整体搬走才空）
            depends: List[str] = []
            for a in self.actions:
                if a.kind == "relocate" and str(a.source_parent_id) == d:
                    depends.append(a.id)
                elif a.kind == "move_and_rename_dir" and self.scanned_dir_parents.get(str(a.source_id or "")) == d:
                    depends.append(a.id)
            # 子目录的 delete_empty_dir 依赖
            for child_dir, child_action_id in dir_to_delete_id.items():
                if self.scanned_dir_parents.get(child_dir) == d:
                    depends.append(child_action_id)

            del_action = PlanAction(
                id=self._next_id(),
                kind="delete_empty_dir",
                source_id=d,
                source_name=self.scanned_dir_names.get(d, ""),
                source_parent_id="",
                target_parent_id="",
                target_name="",
                reason="清理空的源目录",
                depends_on=depends,
                metadata={"kind_label": "delete_empty_dir"},
            )
            self._add(del_action)
            dir_to_delete_id[d] = del_action.id

    def _is_scanned_ancestor(self, ancestor_id: str, child_id: str) -> bool:
        cur = str(child_id or "")
        ancestor = str(ancestor_id or "")
        depth = 0
        while cur and depth <= 50:
            if cur == ancestor:
                return True
            cur = self.scanned_dir_parents.get(cur)
            depth += 1
        return False

    async def _validate_tmdb(self):
        if not self.use_tmdb:
            self.log("[计划] 任务未启用 TMDB 匹配，仅使用文件名识别")
            self.diagnostics["tmdb_status"] = "disabled_task"
            return
        if not self.tmdb_api_key:
            self.log("[计划] 未配置 TMDB API Key，仅使用文件名识别")
            self.diagnostics["tmdb_status"] = "no_api_key"
            return
        self.log("[计划] 验证 TMDB 连通性...")
        ok = await validate_tmdb_connection(self.tmdb_api_key, self.tmdb_lang, self.tmdb_proxy)
        self.check_stop()
        self.tmdb_available = ok
        self.diagnostics["tmdb_status"] = "available" if ok else "unreachable"
        self.log("[计划] TMDB 连通正常" if ok else "[计划] TMDB 无法连通，将跳过 TMDB 匹配")

    def _finalize(self) -> Plan:
        plan = Plan(
            task_id=self.task_id,
            created_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            target_root_id=self.target_root_id,
            target_parent_id=self.parent_id,
        )
        plan.actions = list(self.actions)
        plan.skipped = list(self.skipped_items)
        plan.diagnostics = self.diagnostics
        return plan

    def _emit_progress(self):
        if not self.progress_fn:
            return
        try:
            self.progress_fn({
                "stage": "planning",
                "scanned_dirs": self.scanned_dirs,
                "scanned_files": self.scanned_files,
                "groups": len(self.diagnostics.get("groups") or []),
                "actions": len(self.actions),
                "skipped": len(self.skipped_items),
                "current_dir": self.current_dir,
                "planned_works": self.planned_work_count,
                "max_works": self.max_works_per_run,
                "quota_reached": self.quota_reached,
            })
        except Exception:
            pass

    async def _clear_dir_cache(self):
        try:
            cache_manager = getattr(self.driver, "_cache_manager", None)
            if not cache_manager:
                return
            account_id = str(
                getattr(self.driver, "account_id", None)
                or getattr(self.driver, "_account_id", None)
                or ""
            )
            if not account_id:
                return
            await cache_manager.clear_by_prefix(f"dir:{account_id}:")
            self.log("[计划] 已清理目录缓存，确保按最新目录结构扫描")
        except Exception as e:
            self.log(f"[计划] 清理目录缓存失败（忽略，按现有缓存继续）: {e}")

    async def _list_with_retry(self, dir_id: str):
        self.check_stop()
        try:
            items = await self.driver.list_files(dir_id) or []
        except Exception as e:
            self.log(f"[计划] 目录扫描失败: {dir_id} - {e}")
            return []
        self.scanned_dirs += 1
        if self.scanned_dirs % 5 == 0:
            self._emit_progress()
        return items

    def _is_media(self, item) -> bool:
        if item.is_dir or "." not in item.name:
            return False
        return rules.file_extension(item.name) in self.media_exts

    def _is_category_dir(self, name: str, items: list) -> bool:
        if rules.is_generic_media_dir(name):
            return True
        direct_media_count = sum(1 for item in items if self._is_media(item))
        if direct_media_count > 0:
            return False
        child_dirs = [item for item in items if item.is_dir]
        if len(child_dirs) < 2:
            return False
        season_dir_count = sum(1 for item in child_dirs if rules.is_season_dir_name(item.name))
        if season_dir_count:
            return False
        work_name_count = sum(1 for item in child_dirs if rules.looks_like_work_dir_name(item.name))
        return work_name_count >= 2 and work_name_count / len(child_dirs) >= 0.5

    async def _scan_and_plan(self, root_id: str):
        items = await self._list_with_retry(root_id)
        root_entries = [(item, []) for item in items if self._is_media(item)]
        if root_entries:
            await self._plan_batch(root_entries, "根目录文件")
        if self.quota_reached:
            return
        if not self.recursive:
            return
        for item in items:
            self.check_stop()
            if self.quota_reached:
                return
            if item.is_dir:
                await self._walk_for_batches(item.id, [(item.id, item.name)])

    async def _walk_for_batches(self, dir_id: str, ancestors: list):
        if self.quota_reached:
            return
        items = await self._list_with_retry(dir_id)
        self.check_stop()
        dir_name = ancestors[-1][1] if ancestors else ""
        if ancestors:
            cur_id = str(ancestors[-1][0])
            self.scanned_dir_names[cur_id] = ancestors[-1][1]
            if len(ancestors) >= 2:
                self.scanned_dir_parents[cur_id] = str(ancestors[-2][0])
            else:
                self.scanned_dir_parents[cur_id] = str(self.parent_id)
        if self._is_category_dir(dir_name, items):
            # category 目录：先把直接散落的媒体文件当一批处理（每个文件独立 group）
            scatter_entries: List[Tuple[Any, list]] = []
            for item in items:
                if self._is_media(item):
                    scatter_entries.append((item, list(ancestors)))
            if scatter_entries:
                await self._plan_batch(scatter_entries, f"{dir_name or '分类目录'} 散落文件")
            # 再递归子目录
            for child in items:
                self.check_stop()
                if self.quota_reached:
                    return
                if child.is_dir:
                    await self._walk_for_batches(child.id, ancestors + [(child.id, child.name)])
            return
        batch_entries: List[Tuple[Any, list]] = []
        for item in items:
            if self._is_media(item):
                batch_entries.append((item, list(ancestors)))
        for child in items:
            if not child.is_dir:
                continue
            await self._collect_descendants(child.id, ancestors + [(child.id, child.name)], batch_entries)
        if batch_entries:
            await self._plan_batch(batch_entries, dir_name)

    async def _collect_descendants(self, dir_id: str, ancestors: list, out: list):
        items = await self._list_with_retry(dir_id)
        if ancestors:
            cur_id = str(ancestors[-1][0])
            self.scanned_dir_names[cur_id] = ancestors[-1][1]
            if len(ancestors) >= 2:
                self.scanned_dir_parents[cur_id] = str(ancestors[-2][0])
            else:
                self.scanned_dir_parents[cur_id] = str(self.parent_id)
        for item in items:
            self.check_stop()
            if item.is_dir:
                await self._collect_descendants(item.id, ancestors + [(item.id, item.name)], out)
            elif self._is_media(item):
                out.append((item, list(ancestors)))

    async def _plan_batch(self, entries: List[Tuple[Any, list]], label: str):
        self.check_stop()
        if not entries:
            return
        self.processed_batches += 1
        self.scanned_files += len(entries)
        self.current_dir = label
        self.log(f"[计划] 处理批次 #{self.processed_batches}: {label}，发现 {len(entries)} 个媒体文件 (累计扫描 {self.scanned_dirs} 个目录 / {self.scanned_files} 个文件)")
        self._emit_progress()

        groups = self._group_entries(entries)
        pending_skips = groups.pop("__pending_skips__", [])
        for file_item, reason in pending_skips:
            self._skip(file_item, reason)
        self.log(f"[计划] 分组为 {len(groups)} 个作品")
        for (media_kind, dir_id, dir_name, title, _, _, _), items in groups.items():
            marker_text = "有目录" if dir_id else "散落文件"
            kind_text = "剧集" if media_kind == "tv" else "电影"
            self.log(f"[计划]   组: {kind_text} | {marker_text} | 目录={dir_name!r} | 标题={title!r} | {len(items)}个文件")
            self.diagnostics.setdefault("groups", []).append({
                "media_kind": media_kind,
                "dir_id": dir_id,
                "dir_name": dir_name,
                "title": title,
                "count": len(items),
            })

        align_defaults = self._compute_align_defaults(groups) if self.align_media_tags else {}

        for group_key, items in groups.items():
            self.check_stop()
            if self.max_works_per_run > 0 and self.planned_work_count >= self.max_works_per_run:
                self.quota_reached = True
                self.log(f"[计划] 已达到本次最多 {self.max_works_per_run} 部作品上限，剩余作品将在下次重新生成计划时处理")
                return
            before_actions = len(self.actions)
            await self._plan_group(group_key, items, align_defaults.get(group_key, {}))
            # 只有真正产生了整理动作才算占用配额；全跳过（已整理等）的作品不占名额
            if len(self.actions) > before_actions:
                self.planned_work_count += 1
            self._emit_progress()

    def _group_entries(self, entries: List[Tuple[Any, list]]) -> Dict[Tuple, list]:
        groups: Dict[Tuple, list] = defaultdict(list)
        pending_skips: List[Tuple[Any, str]] = []
        layout = rules.analyze_tv_tree_layout(entries)
        for file_item, ancestors in entries:
            self.check_stop()
            file_parsed_raw = rules.normalize_parsed_media(rules.parse_filename_strict(file_item.name))

            dir_parsed_raw: Dict[str, Any] = {}
            root_parsed_raw: Dict[str, Any] = {}
            non_special_ancestors: List[Tuple[Any, str]] = []
            for anc_id, anc_name in ancestors:
                if rules.is_generic_media_dir(anc_name) or rules.is_season_dir_name(anc_name):
                    continue
                if rules.is_collection_container_dir(anc_name):
                    continue
                if rules.is_special_content_dir_name(anc_name):
                    continue
                non_special_ancestors.append((anc_id, anc_name))
            if non_special_ancestors:
                dir_parsed_raw = rules.normalize_parsed_media(rules.parse_dir_name(non_special_ancestors[-1][1]))
            if len(non_special_ancestors) >= 2:
                root_parsed_raw = rules.normalize_parsed_media(rules.parse_dir_name(non_special_ancestors[-2][1]))

            file_parsed = rules.merge_three_layer_parsed(file_parsed_raw, dir_parsed_raw, root_parsed_raw)
            file_parsed["_part_label"] = rules.extract_part_label(file_item.name)
            file_parsed["_special_label"] = rules.extract_special_label(file_item.name)
            file_parsed = rules._prepare_tv_file_parsed(file_parsed, ancestors)
            source_dir_id = ancestors[-1][0] if ancestors else self.parent_id
            source_dir_name = ancestors[-1][1] if ancestors else ""

            nested_movie_id, nested_movie_name = rules.find_nearest_standalone_movie_dir(ancestors)
            force_movie = bool(nested_movie_id)

            tv_rule = rules.looks_like_tv_file(file_parsed, ancestors)
            is_tv = (
                not force_movie
                and (self.task_media_type == "tv" or (self.task_media_type == "auto" and tv_rule.matched))
            )

            if is_tv:
                show_dir_id, show_dir_name, show_parsed = rules.pick_tv_show_info(ancestors, file_parsed)
                if rules.is_ambiguous_root_tv_scatter(ancestors, layout, show_dir_id):
                    pending_skips.append((
                        file_item,
                        "检测到多季子目录，根目录散落文件无法确定季号，请移入对应季文件夹",
                    ))
                    continue
                title = (show_parsed.get("title") or file_parsed.get("title") or "").strip()
                year = rules.resolve_tv_group_year(show_parsed)
                key = ("tv", show_dir_id, show_dir_name, title, year, None, None)
                groups[key].append((file_item, source_dir_id, source_dir_name, file_parsed, ancestors))
                continue

            movie_dir_id = None
            movie_dir_name = None
            movie_parsed: Dict[str, Any] = {}
            if force_movie and nested_movie_id:
                movie_dir_id = nested_movie_id
                movie_dir_name = nested_movie_name or ""
                movie_parsed = rules.normalize_parsed_media(rules.parse_dir_name(movie_dir_name))
            else:
                for dir_id, dir_name in reversed(ancestors):
                    # 跳过通用 generic 目录（电影/电视剧/动漫...）和季目录
                    if rules.is_generic_media_dir(dir_name) or rules.is_season_dir_name(dir_name):
                        continue
                    if rules.is_collection_container_dir(dir_name):
                        continue
                    parsed = rules.normalize_parsed_media(rules.parse_dir_name(dir_name))
                    if parsed.get("title"):
                        movie_dir_id = dir_id
                        movie_dir_name = dir_name
                        movie_parsed = parsed
                        break

            if not movie_dir_id and ancestors:
                dir_id, dir_name = ancestors[-1]
                if (
                    not rules.is_generic_media_dir(dir_name)
                    and not rules.is_season_dir_name(dir_name)
                ):
                    parsed = rules.normalize_parsed_media(rules.parse_dir_name(dir_name))
                    if parsed.get("title"):
                        movie_dir_id = dir_id
                        movie_dir_name = dir_name
                        movie_parsed = parsed

            if movie_dir_id:
                group_title, group_year = rules.resolve_movie_group_identity(movie_dir_name, file_parsed)
                key = (
                    "movie",
                    movie_dir_id,
                    movie_dir_name,
                    group_title or movie_parsed.get("title") or "",
                    group_year if group_year is not None else movie_parsed.get("year"),
                    movie_parsed.get("season"),
                    movie_parsed.get("episode"),
                )
            else:
                key = (
                    "movie",
                    None,
                    None,
                    file_parsed.get("title") or "",
                    file_parsed.get("year"),
                    file_parsed.get("season"),
                    file_parsed.get("episode"),
                )
            groups[key].append((file_item, source_dir_id, source_dir_name, file_parsed, ancestors))
        if pending_skips:
            groups["__pending_skips__"] = pending_skips
        return groups

    def _compute_align_defaults(self, groups: Dict[Tuple, list]) -> Dict[Tuple, Dict[Tuple, Dict[str, Any]]]:
        out: Dict[Tuple, Dict[Tuple, Dict[str, Any]]] = {}
        for group_key, items in groups.items():
            stats: Dict[Tuple, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
            totals: Dict[Tuple, int] = defaultdict(int)
            for file_item, _, _, file_parsed, _ in items:
                ext = rules.file_extension(file_item.name)
                season_key = file_parsed.get("season") or 0
                bucket_key = (season_key, ext)
                totals[bucket_key] += 1
                for tag_key in rules.MEDIA_TAG_FIELDS:
                    normalized = rules.normalize_media_tag_value(tag_key, file_parsed.get(tag_key))
                    if not normalized:
                        continue
                    values = normalized if isinstance(normalized, list) else [normalized]
                    for value in values:
                        stats[(bucket_key, tag_key)][value] += 1
            defaults_by_bucket: Dict[Tuple, Dict[str, Any]] = defaultdict(dict)
            for (bucket_key, tag_key), counter in stats.items():
                bucket_total = totals.get(bucket_key) or 0
                if bucket_total <= 0:
                    continue
                value, count = max(counter.items(), key=lambda item: item[1])
                if count / bucket_total > 0.6:
                    defaults_by_bucket[bucket_key][tag_key] = value
            if defaults_by_bucket:
                out[group_key] = dict(defaults_by_bucket)
        return out

    async def _plan_group(self, group_key: Tuple, items: list, align_defaults: Dict[Tuple, Dict[str, Any]]):
        media_kind, dir_id, dir_name, title, year, season, episode = group_key
        is_tv = media_kind == "tv"
        group_uid = "|".join(str(x) for x in group_key)

        if not title:
            for file_item, _, _, _, _ in items:
                self._skip(file_item, "无法识别")
            return

        # 兜底：title 过短/全是 alnum + 既没年份也没集数也没 tmdb id → 视为低置信度文件，跳过
        if self._is_low_confidence_group(group_key, items):
            for file_item, _, _, _, _ in items:
                self._skip(file_item, "识别置信度过低")
            return

        tmdb_info = await self._match_tmdb_for_group(group_key, items) if (self.use_tmdb and self.tmdb_available) else {}

        # 多版本歧义：跳过整组，让用户补年份后重试
        if tmdb_info.get("ambiguous"):
            cands = tmdb_info.get("candidates") or []
            versions_brief = " / ".join(f"{c.get('title', '?')} ({c.get('year', '?')})" for c in cands[:4])
            reason = f"TMDB 存在多个版本（{versions_brief}），请给源文件夹补上年份后重试"
            for file_item, _, _, _, _ in items:
                self._skip(file_item, reason)
            return

        if not tmdb_info.get("tmdb_id"):
            preserved_id = self._find_existing_tmdb_id_in_group(items)
            if preserved_id:
                tmdb_info = {
                    "tmdb_id": preserved_id,
                    "tmdb_title": "",
                    "tmdb_original": "",
                    "year": None,
                    "title": "",
                    "confidence": 0.5,
                    "inferred_season": None,
                }
                self.log(f"[计划] 保留原有 TMDB 标识（TMDB 未匹配/不可达）: tmdb-{preserved_id}")
        tmdb_id = tmdb_info.get("tmdb_id", "")
        tmdb_title = tmdb_info.get("tmdb_title", "")
        tmdb_original = tmdb_info.get("tmdb_original", "")
        if is_tv and tmdb_id:
            show_raw = tmdb_info.get("raw") or {}
            tv_seasons = await self._get_tv_seasons(tmdb_id)
            series_year = rules.resolve_tmdb_tv_series_year(show_raw, tv_seasons)
            if series_year:
                year = series_year
            elif tmdb_info.get("year") and not year:
                year = tmdb_info["year"]
        elif tmdb_info.get("year") and not year:
            year = tmdb_info["year"]
        if tmdb_info.get("title"):
            title = tmdb_info["title"]
        inferred_season = rules.as_first_int(tmdb_info.get("inferred_season"))
        if inferred_season:
            is_tv = True
            if season is None:
                season = inferred_season

        short_title = tmdb_title or title
        folder_info = {"title": short_title, "year": year}
        new_folder_name = rules.sanitize_filename(rules.build_folder_name(folder_info, tmdb_id))
        display_title = rules.build_display_title(tmdb_title, tmdb_original, title)

        group_dir_meta: Dict[str, Any] = {"group_uid": group_uid}
        if dir_name and new_folder_name and not rules.is_same_generated_name(dir_name, new_folder_name):
            group_dir_meta["group_old_dir_name"] = dir_name
            group_dir_meta["group_new_dir_name"] = new_folder_name

        promoted_movie_parent: Optional[str] = None
        promoted_move_target: Optional[str] = None
        parent_of_dir: Optional[str] = None
        if dir_id and items:
            for fi, _, _, _, ancestors in items:
                for idx, (anc_id, anc_name) in enumerate(ancestors):
                    if str(anc_id) == str(dir_id):
                        parent_of_dir = ancestors[idx - 1][0] if idx > 0 else self.parent_id
                        break
                if parent_of_dir is not None:
                    break
        if parent_of_dir is None:
            parent_of_dir = self.parent_id

        if media_kind == "movie" and items:
            _, _, _, _, sample_ancestors = items[0]
            promoted_movie_parent = rules.get_promoted_movie_parent_id(
                sample_ancestors,
                dir_id,
                self.parent_id,
                self.scanned_dir_parents,
            )
            promoted_move_target = self._resolve_promoted_movie_target_parent(
                sample_ancestors, dir_id
            )

        promoted_move_ref = ""
        if (
            self.action_type == "move"
            and media_kind == "movie"
            and dir_id
            and promoted_move_target
        ):
            promoted_move_ref = self._ensure_promoted_movie_move_action(
                dir_id,
                dir_name,
                promoted_move_target,
                new_folder_name,
                tmdb_id,
                tmdb_info.get("confidence", 0.6 if tmdb_id else 0.4),
            )

        if self.action_type == "rename" and dir_id and new_folder_name and dir_name:
            target_parent_for_dir = promoted_movie_parent or str(parent_of_dir)
            needs_rename = not rules.is_same_generated_name(dir_name, new_folder_name)
            needs_promote = (
                promoted_movie_parent
                and str(parent_of_dir) != str(promoted_movie_parent)
            )
            if needs_rename or needs_promote:
                dir_reason = f"作品目录改名 | {dir_name} -> {new_folder_name}{' | tmdb-' + tmdb_id if tmdb_id else ''}"
                if needs_promote:
                    dir_reason = (
                        f"独立电影移出剧集目录 | {dir_name} -> {new_folder_name}"
                        f"{' | tmdb-' + tmdb_id if tmdb_id else ''}"
                    )
                self._add(PlanAction(
                    id=self._next_id(),
                    kind="relocate",
                    source_id=str(dir_id),
                    source_name=dir_name,
                    source_parent_id=str(parent_of_dir),
                    target_parent_id=target_parent_for_dir,
                    target_name=new_folder_name if needs_rename else dir_name,
                    reason=dir_reason,
                    confidence=tmdb_info.get("confidence", 0.6 if tmdb_id else 0.4),
                    metadata={
                        "tmdb_id": tmdb_id,
                        "media_kind": media_kind,
                        "kind_label": "dir_rename",
                        "group_uid": group_uid,
                        "promoted_from_tv_tree": bool(needs_promote),
                    },
                ))

        ffprobe_results: Dict[str, dict] = {}
        if self.use_ffprobe:
            ffprobe_results = await self._batch_ffprobe(items)

        target_work_id_or_ref = self._ensure_work_dir_action(
            group_key, new_folder_name, items, promoted_move_ref=promoted_move_ref
        )

        season_dir_cache: Dict[int, str] = {}
        tv_seasons_cache: Optional[list] = None
        season_dir_rename_cache: Dict[str, PlanAction] = {}

        for file_item, source_dir_id, source_dir_name, file_parsed, ancestors in items:
            self.check_stop()
            ext = rules.file_extension(file_item.name)
            current_year = year
            current_season = file_parsed.get("season") if is_tv else season
            current_episode = file_parsed.get("episode") if is_tv else episode
            if is_tv and tmdb_id:
                ctx = rules.get_nearest_tv_dir_context(ancestors)
                if ctx.get("kind") == "season" and ctx.get("season") is not None:
                    current_season = ctx.get("season")
                elif ctx.get("kind") == "special":
                    if tv_seasons_cache is None:
                        tv_seasons_cache = await self._get_tv_seasons(tmdb_id)
                    inferred_season = rules.infer_season_from_tmdb_seasons(
                        ctx.get("year"),
                        ctx.get("dir_name") or "",
                        tv_seasons_cache,
                        prefer_special=True,
                    )
                    if inferred_season is not None:
                        current_season = inferred_season
            if is_tv and current_season is None:
                current_season = inferred_season or season
            season_dir_rename_action: Optional[PlanAction] = None
            if is_tv and self.action_type == "rename":
                season_dir_rename_action = self._ensure_season_dir_rename_action(
                    source_dir_id,
                    source_dir_name,
                    ancestors,
                    dir_id,
                    current_season,
                    tmdb_id,
                    season_dir_rename_cache,
                )
                if season_dir_rename_action is not None:
                    season_dir_rename_action.metadata["group_uid"] = group_uid

            # 兜底：剧集 group 里某个文件没解析出集数（也没 special label）→ 跳过该文件
            # 避免被错误地塞进剧集 group 后生成无意义新名（甚至导致同名冲突）
            if is_tv and current_episode is None and not file_parsed.get("_special_label"):
                self._skip(file_item, "无法识别集数")
                continue

            if self.action_type == "rename" and rules.is_already_organized(file_item.name, self.marker):
                self._skip(file_item, "已整理")
                continue

            parsed_for_tag = dict(file_parsed)
            ffp = ffprobe_results.get(file_item.id) or {}
            for key in ("screen_size", "frame_rate", "video_codec", "audio_codec", "audio_channels"):
                if ffp.get(key):
                    parsed_for_tag[key] = ffp[key]
            if self.align_media_tags:
                bucket_key = ((current_season if is_tv else 0) or 0, ext)
                bucket_defaults = align_defaults.get(bucket_key, {}) if align_defaults else {}
                parsed_for_tag = rules.merge_aligned_media_tags(parsed_for_tag, bucket_defaults)

            media_info_tag = rules.build_media_info_tags(parsed_for_tag, self.media_tag_order)

            file_info_parsed = {
                "title": display_title,
                "year": current_year,
                "season": current_season,
                "episode": current_episode,
            }
            base = rules.build_target_filename(file_info_parsed, self.marker, tmdb_id)
            if not base:
                self._skip(file_item, "无法生成新名")
                continue
            new_base = rules.sanitize_filename(base)
            special_label = file_parsed.get("_special_label")
            if special_label and special_label not in (display_title or ""):
                new_base = f"{new_base} {special_label}"
            part_label = file_parsed.get("_part_label")
            if part_label:
                new_base = f"{new_base} {part_label}"
            if media_info_tag:
                new_filename = f"{new_base} {media_info_tag}.{ext}" if ext else f"{new_base} {media_info_tag}"
            else:
                new_filename = f"{new_base}.{ext}" if ext else new_base
            new_filename = rules.fit_filename_bytes(new_filename, self.tmdb_lang)
            new_meta_base = new_filename[: -(len(ext) + 1)] if ext and new_filename.lower().endswith("." + ext.lower()) else new_filename

            if self.action_type == "rename":
                if rules.is_same_generated_name(file_item.name, new_filename):
                    self._skip(file_item, "已是目标名")
                    continue
                src_dir_name = self.scanned_dir_names.get(str(source_dir_id), "")
                is_scattered = (
                    str(source_dir_id) == str(self.parent_id)
                    or rules.is_generic_media_dir(src_dir_name)
                )
                rename_target_parent = str(source_dir_id)
                rename_deps: List[str] = []
                if is_scattered:
                    sub_work_ref = self._ensure_dir_action(str(source_dir_id), new_folder_name)
                    if is_tv:
                        sub_season_ref, season_deps = self._resolve_target_parent_for_move(
                            sub_work_ref, is_tv, current_season, season_dir_cache
                        )
                        rename_target_parent = sub_season_ref
                        rename_deps = list(season_deps)
                    else:
                        rename_target_parent = sub_work_ref
                        if sub_work_ref.startswith("ref:"):
                            rename_deps = [sub_work_ref[4:]]
                elif media_kind == "movie" and promoted_movie_parent:
                    rename_target_parent = str(dir_id or source_dir_id)
                action = PlanAction(
                    id=self._next_id(),
                    kind="relocate",
                    source_id=file_item.id,
                    source_name=file_item.name,
                    source_parent_id=str(source_dir_id),
                    target_parent_id=rename_target_parent,
                    target_name=new_filename,
                    reason=self._build_reason(group_key, tmdb_id, display_title, current_season, current_episode, rename_only=True),
                    confidence=tmdb_info.get("confidence", 0.6 if tmdb_id else 0.4),
                    depends_on=rename_deps,
                    metadata={"tmdb_id": tmdb_id, "media_kind": media_kind, "title": short_title, "mode": "rename", "season": current_season, "episode": current_episode, **group_dir_meta},
                )
                self._add(action)
                if season_dir_rename_action:
                    deps = list(season_dir_rename_action.depends_on or [])
                    if action.id not in deps:
                        deps.append(action.id)
                    season_dir_rename_action.depends_on = deps
                self._plan_meta_followers(
                    file_item, source_dir_id, new_meta_base, ext, action.id, file_parsed=file_parsed
                )
                continue

            target_parent_id, deps = self._resolve_target_parent_for_move(
                target_work_id_or_ref, is_tv, current_season, season_dir_cache
            )

            action = PlanAction(
                id=self._next_id(),
                kind="relocate",
                source_id=file_item.id,
                source_name=file_item.name,
                source_parent_id=str(source_dir_id),
                target_parent_id=target_parent_id,
                target_name=new_filename,
                reason=self._build_reason(group_key, tmdb_id, display_title, current_season, current_episode, rename_only=False),
                confidence=tmdb_info.get("confidence", 0.6 if tmdb_id else 0.4),
                depends_on=list(deps),
                metadata={"tmdb_id": tmdb_id, "media_kind": media_kind, "title": short_title, "mode": "move", "season": current_season, "episode": current_episode, **group_dir_meta},
            )
            self._add(action)
            self._plan_meta_followers(file_item, source_dir_id, new_meta_base, ext, action.id)

    def _is_group_already_organized(self, items: list) -> bool:
        if not items:
            return True
        if not self.marker or not self.marker.strip():
            return False
        for file_item, _, _, _, _ in items:
            if not rules.is_already_organized(file_item.name, self.marker):
                return False
        return True

    def _build_reason(self, group_key, tmdb_id, display_title, season, episode, rename_only: bool) -> str:
        media_kind = group_key[0]
        kind_text = "剧集" if media_kind == "tv" else "电影"
        bits = [kind_text, display_title or "未识别标题"]
        if media_kind == "tv" and season is not None and episode is not None:
            bits.append(f"S{int(season):02d}E{int(episode):02d}")
        if tmdb_id:
            bits.append(f"TMDB {tmdb_id}")
        else:
            bits.append("仅文件名识别")
        bits.append("原地重命名" if rename_only else "移动并重命名")
        return " | ".join(bits)

    def _skip(self, file_item, reason: str):
        self.skipped_items.append({
            "file_id": str(file_item.id),
            "file_name": file_item.name,
            "reason": reason,
        })

    def _ensure_season_dir_rename_action(
        self,
        source_dir_id,
        source_dir_name: str,
        ancestors: list,
        show_dir_id,
        season: Optional[int],
        tmdb_id: str,
        cache: Dict[str, PlanAction],
    ) -> Optional[PlanAction]:
        if season is None:
            return None
        src_id = str(source_dir_id or "")
        if not src_id or not source_dir_name:
            return None
        if not (
            rules.is_season_dir_name(source_dir_name)
            or rules.is_special_content_dir_name(source_dir_name)
        ):
            return None
        target_name = rules.build_season_folder_name(season, self.season_folder_tpl)
        if not target_name:
            return None
        if src_id in cache:
            return cache[src_id]

        parent_id = self.scanned_dir_parents.get(src_id)
        if not parent_id:
            for idx, (anc_id, _anc_name) in enumerate(ancestors or []):
                if str(anc_id) == src_id:
                    parent_id = str(ancestors[idx - 1][0]) if idx > 0 else str(self.parent_id)
                    break
        if not parent_id:
            return None
        target_parent_id = str(parent_id)
        flatten_collection = False
        parent_name = self.scanned_dir_names.get(str(parent_id), "")
        parent_parent_id = self.scanned_dir_parents.get(str(parent_id))
        if (
            show_dir_id
            and parent_parent_id
            and str(parent_parent_id) == str(show_dir_id)
            and rules.is_collection_container_dir(parent_name)
        ):
            target_parent_id = str(show_dir_id)
            flatten_collection = True
        if rules.is_same_generated_name(source_dir_name, target_name) and not flatten_collection:
            return None
        reason_prefix = (
            f"去除剧集合集层并标准化季目录 | {parent_name}/{source_dir_name} -> {target_name}"
            if flatten_collection
            else f"季目录标准化 | {source_dir_name} -> {target_name}"
        )

        action = PlanAction(
            id=self._next_id(),
            kind="relocate",
            source_id=src_id,
            source_name=source_dir_name,
            source_parent_id=str(parent_id),
            target_parent_id=target_parent_id,
            target_name=target_name,
            reason=reason_prefix + (f" | tmdb-{tmdb_id}" if tmdb_id else ""),
            confidence=0.9 if tmdb_id else 0.6,
            metadata={
                "tmdb_id": tmdb_id,
                "media_kind": "tv",
                "kind_label": "season_dir_rename",
                "season": int(season),
                "flatten_collection_dir": flatten_collection,
                "collection_dir_id": str(parent_id) if flatten_collection else "",
            },
        )
        self._add(action)
        cache[src_id] = action
        return action

    def _category_ancestors_before_tv_show(self, ancestors: list, movie_dir_id) -> list:
        """独立电影提升时，保留扫描根到剧集目录之间的分类层（如 影音库）。"""
        if not movie_dir_id or not ancestors:
            return []
        movie_idx = next(
            (idx for idx, (anc_id, _) in enumerate(ancestors) if str(anc_id) == str(movie_dir_id)),
            None,
        )
        if movie_idx is None:
            return []
        show_id, _, _ = rules.pick_tv_show_info(
            ancestors[:movie_idx], {"season": 1, "episode": 1}
        )
        if not show_id:
            return []
        scan_root = str(self.parent_id)
        category = []
        for ancestor_id, ancestor_name in ancestors:
            if str(ancestor_id) == str(show_id):
                break
            if str(ancestor_id) == scan_root:
                continue
            category.append((ancestor_id, ancestor_name))
        return category

    def _build_target_category_parent_ref(self, category_ancestors: list) -> str:
        parent_ref = self.target_root_id or self.parent_id
        for _, cat_name in category_ancestors:
            cat_name = (cat_name or "").strip()
            if not cat_name:
                continue
            parent_ref = self._ensure_dir_action(parent_ref, cat_name)
        return parent_ref

    def _resolve_promoted_movie_target_parent(self, ancestors: list, movie_dir_id) -> Optional[str]:
        """嵌在剧集目录树里的独立电影应提升到的目标父目录。

        move 模式 → target_root + 扫描根到剧集之间的分类层（与剧集目录结构对齐）
        rename 模式 → 源库中剧集作品目录的同级
        """
        nested_parent = rules.get_promoted_movie_parent_id(
            ancestors,
            movie_dir_id,
            self.parent_id,
            self.scanned_dir_parents,
        )
        if not nested_parent:
            return None
        if self.action_type == "move":
            cats = self._category_ancestors_before_tv_show(ancestors, movie_dir_id)
            return self._build_target_category_parent_ref(cats)
        return str(nested_parent)

    def _ensure_promoted_movie_move_action(
        self,
        dir_id,
        dir_name: str,
        target_parent: str,
        target_name: str,
        tmdb_id: str,
        confidence: float,
    ) -> str:
        """嵌在剧集目录树里的独立电影：move 模式下整体搬出到目标库中剧集同级目录。"""
        for action in self.actions:
            if (
                action.kind == "move_and_rename_dir"
                and str(action.source_id) == str(dir_id)
                and str(action.target_parent_id) == str(target_parent)
                and action.target_name == target_name
            ):
                return f"ref:{action.id}"
        action = PlanAction(
            id=self._next_id(),
            kind="move_and_rename_dir",
            source_id=str(dir_id),
            source_name=dir_name or "",
            target_parent_id=str(target_parent),
            target_name=target_name,
            reason=(
                f"独立电影移出剧集目录 | {dir_name} -> {target_name}"
                f"{' | tmdb-' + tmdb_id if tmdb_id else ''}"
            ),
            confidence=confidence,
            metadata={
                "is_work_dir": True,
                "source_dir_id": str(dir_id),
                "promoted_from_tv_tree": True,
                "tmdb_id": tmdb_id,
            },
        )
        self._add(action)
        return f"ref:{action.id}"

    def _ensure_work_dir_action(
        self,
        group_key,
        work_dir_name: str,
        items: list,
        promoted_move_ref: str = "",
    ) -> str:
        if self.action_type != "move":
            return ""
        if not work_dir_name:
            return ""
        if promoted_move_ref:
            return promoted_move_ref
        media_kind, group_dir_id, _, _, _, _, _ = group_key
        category_ancestors = self._category_ancestors(group_key, items)
        parent_ref = self._build_target_category_parent_ref(category_ancestors)
        ref = self._ensure_dir_action(parent_ref, work_dir_name)
        # 标记为作品目录，并记录对应的源 dir 用于整体移动优化
        src_dir_id = str(group_key[1]) if group_key[1] else ""
        if ref.startswith("ref:"):
            for action in self.actions:
                if action.id == ref[4:]:
                    action.metadata = action.metadata or {}
                    action.metadata["is_work_dir"] = True
                    if src_dir_id and src_dir_id != str(self.parent_id):
                        action.metadata["source_dir_id"] = src_dir_id
                    break
        return ref

    def _category_ancestors(self, group_key, items: list) -> list:
        media_kind, group_dir_id, _, _, _, _, _ = group_key
        if not items:
            return []
        _, _, _, _, ancestors = items[0]
        scan_root = str(self.parent_id)
        category_ancestors = []
        for ancestor_id, ancestor_name in ancestors:
            if group_dir_id and str(ancestor_id) == str(group_dir_id):
                break
            if str(ancestor_id) == scan_root:
                continue
            category_ancestors.append((ancestor_id, ancestor_name))
        if group_dir_id:
            return category_ancestors
        # 散落文件：只保留 generic 分类层（电影/电视剧/动漫），避免在源目录内嵌套目标目录
        return [
            (ancestor_id, ancestor_name)
            for ancestor_id, ancestor_name in category_ancestors
            if rules.is_generic_media_dir(ancestor_name)
        ]

    def _ensure_dir_action(self, parent_ref: str, folder_name: str) -> str:
        return self._ensure_dir_action_at(parent_ref, folder_name)

    def _ensure_dir_action_at(self, parent_ref: str, folder_name: str) -> str:
        for action in self.actions:
            if (
                action.kind in ("ensure_dir", "move_and_rename_dir")
                and action.target_parent_id == parent_ref
                and action.target_name == folder_name
            ):
                return f"ref:{action.id}"
        action = PlanAction(
            id=self._next_id(),
            kind="ensure_dir",
            target_parent_id=parent_ref,
            target_name=folder_name,
            reason=f"确保目录存在: {folder_name}",
            confidence=1.0,
            depends_on=[parent_ref[4:]] if parent_ref.startswith("ref:") else [],
        )
        self._add(action)
        return f"ref:{action.id}"

    def _resolve_target_parent_for_move(
        self,
        work_dir_ref: str,
        is_tv: bool,
        season: Optional[int],
        season_dir_cache: Dict[int, str],
    ) -> Tuple[str, List[str]]:
        deps: List[str] = []
        if work_dir_ref.startswith("ref:"):
            deps.append(work_dir_ref[4:])
        if not is_tv:
            return work_dir_ref, deps
        if season in season_dir_cache:
            cached = season_dir_cache[season]
            if cached.startswith("ref:"):
                deps.append(cached[4:])
            return cached, deps
        season_folder = rules.build_season_folder_name(season, self.season_folder_tpl)
        season_ref = self._ensure_dir_action(work_dir_ref, season_folder)
        # 给该 season 的 ensure_dir 标记，便于后续整体移动优化
        if season_ref.startswith("ref:"):
            for action in self.actions:
                if action.id == season_ref[4:]:
                    action.metadata = action.metadata or {}
                    action.metadata["is_season_dir"] = True
                    if season is not None:
                        action.metadata["season_index"] = int(season)
                    break
        season_dir_cache[season] = season_ref
        if season_ref.startswith("ref:"):
            deps.append(season_ref[4:])
        return season_ref, deps

    def _plan_meta_followers(
        self,
        file_item,
        source_dir_id,
        new_base: str,
        ext: str,
        depend_action_id: str,
        *,
        file_parsed: Optional[dict] = None,
    ):
        if not self.meta_exts:
            return
        stem, _ = rules.split_basename(file_item.name)
        parsed = dict(file_parsed or {})
        followers = self.diagnostics.setdefault("meta_followers", [])
        followers.append({
            "file_id": str(file_item.id),
            "source_dir_id": str(source_dir_id),
            "old_base": stem,
            "match_bases": rules.build_meta_match_bases(stem, parsed),
            "episode_token": rules.extract_episode_token(file_item.name, parsed),
            "new_base": new_base,
            "depend_on": depend_action_id,
            "meta_exts": sorted(self.meta_exts),
            "action_type": self.action_type,
        })

    async def _tmdb_search(self, title: str, year, group_media_type: str) -> list:
        self.check_stop()
        return await search_tmdb_async(
            title, year, self.tmdb_lang, self.tmdb_api_key, self.tmdb_proxy, group_media_type
        )

    async def _tmdb_try_match(self, title: str, year, group_media_type: str) -> Tuple[Optional[dict], list, bool, str]:
        if not (title or "").strip():
            return None, [], False, ""
        results = await self._tmdb_search(title, year, group_media_type)
        selected = rules.pick_tmdb_match_for_year(results, year, group_media_type, query_title=title)
        if selected:
            return selected, results, True, "exact"
        if year:
            results_no_year = await self._tmdb_search(title, None, group_media_type)
            selected2 = rules.pick_tmdb_match_for_year(results_no_year, year, group_media_type, query_title=title)
            if selected2:
                return selected2, results_no_year, True, "no_year"
            for hit in results_no_year:
                _, t, o, _ = rules.extract_tmdb_display_fields(hit, group_media_type)
                if rules.is_tmdb_title_compatible(title, t, o):
                    return hit, results_no_year, False, "no_year_fallback_compatible"
        for hit in results:
            _, t, o, _ = rules.extract_tmdb_display_fields(hit, group_media_type)
            if rules.is_tmdb_title_compatible(title, t, o):
                return hit, results, False, "fuzzy_compatible"
        return None, [], False, "miss"

    def _is_low_confidence_group(self, group_key, items: list) -> bool:
        """识别置信度过低：title 短小可疑且无任何辅助信号。"""
        import re as _re
        media_kind, _dir_id, _dir_name, title, year, season, episode = group_key
        t = (title or "").strip()
        if not t:
            return True
        # 强信号任一存在即可信
        if year:
            return False
        if season is not None or episode is not None:
            return False
        # items 里任一文件解出 season/episode 都算可信（电视剧的 group_key season/episode 是 None）
        for _file_item, _, _, file_parsed, _ in items:
            if file_parsed.get("season") is not None or file_parsed.get("episode") is not None:
                return False
            if file_parsed.get("year"):
                return False
        if self._find_existing_tmdb_id_in_group(items):
            return False
        # title 仅是短小英数（如 aaa / abc / 01 / file），且没年份/集数 → 跳过
        if _re.fullmatch(r"[A-Za-z0-9._\-]{1,6}", t):
            return True
        # 通用占位词
        if t.lower() in {"video", "movie", "new folder", "未命名", "未分类", "新建文件夹"}:
            return True
        return False

    def _detect_multi_version_ambiguity(self, results: list, query_title: str, group_media_type: str) -> Optional[list]:
        """检测「完全同名作品多版本」的歧义：返回候选列表或 None。

        严格触发条件：
        - 结果数 ≥ 2
        - 至少 2 个结果的 title 或 original_title **完全等于** query（不含其它字符）
          ↑ 这一条是关键：避免「千与千寻」误匹配到「舞台剧「千与千寻」」「千与千寻诞生秘话」
        - 这些完全同名的候选有 ≥ 2 个不同发行年份
        """
        qt = (query_title or "").strip().lower()
        if not qt or not results or len(results) < 2:
            return None
        relevant: list = []
        for hit in results[:10]:
            _, t, original, y = rules.extract_tmdb_display_fields(hit, group_media_type)
            if not y:
                continue
            tt = (t or "").strip().lower()
            ot = (original or "").strip().lower()
            # 严格相等：title 或 original 任一完全等于 query 才算同名
            if qt == tt or qt == ot:
                relevant.append(hit)
        if len(relevant) < 2:
            return None
        years = set()
        for hit in relevant:
            _, _, _, y = rules.extract_tmdb_display_fields(hit, group_media_type)
            if y:
                years.add(int(y))
        if len(years) >= 2:
            return relevant
        return None

    @staticmethod
    def _tmdb_titles_share_chars(query: str, *candidates: str) -> bool:
        """检查 query 跟 TMDB 命中标题是否字面相关（委托 rules.is_tmdb_title_compatible）。"""
        cands = [c for c in candidates if (c or "").strip()]
        if not cands:
            return False
        title = cands[0]
        original = cands[1] if len(cands) > 1 else ""
        return rules.is_tmdb_title_compatible(query, title, original)

    def _find_existing_tmdb_id_in_group(self, items: list) -> str:
        for file_item, _, source_dir_name, _, ancestors in items:
            existing = rules.find_tmdb_id_in_name(file_item.name)
            if existing:
                return existing
            if source_dir_name:
                existing = rules.find_tmdb_id_in_name(source_dir_name)
                if existing:
                    return existing
            for _, anc_name in ancestors or []:
                existing = rules.find_tmdb_id_in_name(anc_name)
                if existing:
                    return existing
        return ""

    async def _match_tmdb_for_group(self, group_key, items: list) -> Dict[str, Any]:
        media_kind, dir_id, dir_name, title, year, _, _ = group_key
        if not title:
            return {}
        group_media_type = "tv" if media_kind == "tv" else "movie"

        existing_tmdb_id = self._find_existing_tmdb_id_in_group(items)

        if existing_tmdb_id:
            try:
                info = await lookup_tmdb_by_id_async(
                    existing_tmdb_id, self.tmdb_lang, self.tmdb_api_key, self.tmdb_proxy, group_media_type
                )
            except Exception as e:
                info = None
                self.log(f"[计划] TMDB ID 直查异常 tmdb-{existing_tmdb_id}: {e}")
            await asyncio.sleep(self.tmdb_interval)
            if info and info.get("id"):
                tmdb_id, tmdb_title, tmdb_original, tmdb_year = rules.extract_tmdb_display_fields(info, group_media_type)
                self.log(
                    f"[计划] TMDB ID 直查命中: tmdb-{tmdb_id} -> {tmdb_title} ({tmdb_year})"
                )
                return {
                    "tmdb_id": tmdb_id,
                    "tmdb_title": tmdb_title,
                    "tmdb_original": tmdb_original,
                    "year": tmdb_year,
                    "title": tmdb_title,
                    "confidence": 0.99,
                    "inferred_season": None,
                }
            self.log(f"[计划] TMDB ID 直查失败，回退到关键字搜索: tmdb-{existing_tmdb_id}")

        dir_parsed = rules.normalize_parsed_media(rules.parse_dir_name(dir_name or "")) if dir_name else {}
        dir_title = (dir_parsed.get("title") or title or "").strip()
        dir_year = dir_parsed.get("year")
        dir_season = rules.as_first_int(dir_parsed.get("season"))
        file_parses = [_file_parsed for _file_item, _, _, _file_parsed, _ in items]
        if group_media_type == "tv":
            attempts = rules.build_tv_show_match_attempts(title, year, dir_name or "")
        else:
            attempts = rules.build_tmdb_match_attempts(title, year, dir_name or "", file_parses)
        file_title_for_year = ""
        file_year_for_year = None
        for _file_parsed in file_parses:
            cand_title = (_file_parsed.get("title") or "").strip()
            cand_year = _file_parsed.get("year")
            if cand_year and (cand_title or title):
                file_title_for_year = cand_title or title
                file_year_for_year = cand_year
                break

        selected: Optional[dict] = None
        chosen_title = attempts[0][0] if attempts else title
        chosen_year = attempts[0][1] if attempts else year
        inferred_season: Optional[int] = None

        # 多版本歧义检测：group 完全没年份信息 + TMDB 同名作品有多个不同年份版本
        # → 返回歧义标记，由调用方跳过整组并提示用户补年份
        if group_media_type == "tv":
            effective_year = chosen_year or year or dir_year
        else:
            effective_year = chosen_year or year or dir_year or file_year_for_year
        ambiguity_title = chosen_title or title
        if not effective_year and not self._find_existing_tmdb_id_in_group(items):
            try:
                preview_results = await self._tmdb_search(ambiguity_title, None, group_media_type)
                ambiguity = self._detect_multi_version_ambiguity(preview_results, ambiguity_title, group_media_type)
                if ambiguity:
                    versions = []
                    for hit in ambiguity[:5]:
                        _, ht, _, hy = rules.extract_tmdb_display_fields(hit, group_media_type)
                        versions.append(f"{ht} ({hy})" if hy else ht)
                    self.log(f"[计划] TMDB「{ambiguity_title}」存在多个版本：{', '.join(versions)}，缺少年份无法精确匹配")
                    return {
                        "ambiguous": True,
                        "candidates": [
                            {"title": (h.get("title") or h.get("name", "")),
                             "year": (h.get("release_date") or h.get("first_air_date") or "")[:4]}
                            for h in ambiguity[:5]
                        ],
                    }
            except Exception as e:
                self.log(f"[计划] TMDB 歧义检测异常 {ambiguity_title}: {e}")

        try:
            if not attempts:
                attempts = [(title, year, "默认")]

            for cand_title, cand_year, cand_source in attempts:
                if selected:
                    break
                hit, _results, year_ok, _stage = await self._tmdb_try_match(cand_title, cand_year, group_media_type)
                if hit and year_ok:
                    selected = hit
                    chosen_title = cand_title
                    chosen_year = cand_year
                    self.log(f"[计划] TMDB {cand_source}匹配: {cand_title} ({cand_year}) -> tmdb-{hit.get('id')}")

            if not selected:
                for cand_title, cand_year, cand_source in attempts:
                    if selected:
                        break
                    stripped_title, trailing_num = rules.strip_trailing_number(cand_title)
                    if not stripped_title or stripped_title == cand_title:
                        continue
                    hit, _results, year_ok, _stage = await self._tmdb_try_match(
                        stripped_title, cand_year, group_media_type
                    )
                    if hit:
                        selected = hit
                        chosen_title = stripped_title
                        chosen_year = cand_year
                        if group_media_type == "tv" and trailing_num and 1 <= trailing_num <= 50:
                            inferred_season = trailing_num
                        self.log(
                            f"[计划] TMDB 模糊匹配（剥离尾数字 {trailing_num}）: "
                            f"{cand_title} -> {stripped_title} ({cand_year}) -> tmdb-{hit.get('id')}"
                        )

            if not selected:
                for cand_title, cand_year, cand_source in attempts:
                    if selected:
                        break
                    hit, _results, _year_ok, stage = await self._tmdb_try_match(
                        cand_title, cand_year, group_media_type
                    )
                    if hit:
                        _, mismatch_title, mismatch_original, result_year = rules.extract_tmdb_display_fields(hit, group_media_type)
                        # 相似度兜底：候选 title 与命中结果至少要有字符重合，否则拒绝
                        # 避免「千与千寻 简繁中日内封」匹配到「超级马力欧银河大电影」这种字面无关的错配
                        if not self._tmdb_titles_share_chars(cand_title, mismatch_title, mismatch_original):
                            self.log(
                                f"[计划] TMDB 候选与查询字面无关，拒绝采用: "
                                f"{cand_title} → {mismatch_title}（年份 {result_year}）"
                            )
                            continue
                        selected = hit
                        chosen_title = cand_title
                        chosen_year = cand_year
                        self.log(
                            f"[计划] TMDB 年份不匹配但仍采用最优候选 [{stage}]: "
                            f"{cand_title} ({cand_year}) -> {mismatch_title} ({result_year})"
                        )

        except Exception as e:
            self.log(f"[计划] TMDB 查询异常 {title}: {e}")
        finally:
            await asyncio.sleep(self.tmdb_interval)

        if not selected:
            self.log(f"[计划] TMDB 未找到: {chosen_title} ({chosen_year})，使用 guessit 识别结果")
            return {}

        tmdb_id, tmdb_title, tmdb_original, tmdb_year = rules.extract_tmdb_display_fields(selected, group_media_type)
        # 用于置信度计算的"用户声明年份"：group / 目录 / 文件 任一来源都算
        if group_media_type == "tv":
            declared_year = chosen_year or year or dir_year
            display_year = rules.resolve_tmdb_tv_series_year(selected, None) or tmdb_year
        else:
            declared_year = chosen_year or year or dir_year or file_year_for_year
            display_year = chosen_year or tmdb_year
        if declared_year and display_year and abs(int(declared_year) - int(display_year)) <= 1:
            confidence = 0.9  # 标题 + 年份精确匹配
        elif display_year:
            confidence = 0.65  # 标题命中但用户没声明年份，TMDB 给出唯一/最佳候选
        else:
            confidence = 0.5
        out = {
            "tmdb_id": tmdb_id,
            "tmdb_title": tmdb_title,
            "tmdb_original": tmdb_original,
            "title": chosen_title,
            "year": display_year if group_media_type == "tv" else (chosen_year or tmdb_year),
            "raw": selected,
            "confidence": confidence,
        }
        if inferred_season or dir_season:
            out["inferred_season"] = inferred_season or dir_season
        return out

    async def _get_tv_seasons(self, tmdb_id: str) -> list:
        key = str(tmdb_id)
        if key in self._tv_seasons_cache:
            return self._tv_seasons_cache[key]
        seasons = await fetch_tv_seasons_async(
            tmdb_id, self.tmdb_lang, self.tmdb_api_key, self.tmdb_proxy
        )
        await asyncio.sleep(self.tmdb_interval)
        self._tv_seasons_cache[key] = seasons
        return seasons

    async def _batch_ffprobe(self, items: list) -> Dict[str, dict]:
        if not items or not self.use_ffprobe:
            return {}
        sem = asyncio.Semaphore(self.ffprobe_concurrency)
        results: Dict[str, dict] = {}

        async def _worker(file_item):
            async with sem:
                self.check_stop()
                tags, _raw = await probe_media_info_with_ffprobe(self.driver, file_item, self.ffprobe_timeout)
                if tags:
                    results[file_item.id] = tags
                await asyncio.sleep(self.ffprobe_interval)

        await asyncio.gather(*[_worker(it[0]) for it in items], return_exceptions=True)
        return results
