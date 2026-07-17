import asyncio
import importlib.util
import json
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from config import config_manager


class PluginBase:
    def __init__(self, manifest: Dict[str, Any], plugin_dir: Path, config: Optional[Dict[str, Any]] = None):
        self.manifest = manifest
        self.plugin_dir = plugin_dir
        self.id = manifest["id"]
        self.name = manifest.get("name", self.id)
        self.version = manifest.get("version", "0.0.1")
        self.description = manifest.get("description", "")
        self.plugin_type = manifest.get("type", "tool")
        self.config_schema = manifest.get("config_schema", [])
        self.config = config or {}
        self.enabled = False

    async def startup(self):
        return None

    async def shutdown(self):
        return None

    async def on_config_updated(self, config: Dict[str, Any]):
        self.config = config

    async def search(self, keyword: str, page: int = 1) -> Dict[str, Any]:
        raise NotImplementedError("当前插件未实现搜索能力")

    async def test_connection(self) -> Dict[str, Any]:
        raise NotImplementedError("当前插件未实现连通性测试")

    async def sync_index(self, force: bool = False) -> Dict[str, Any]:
        raise NotImplementedError("当前插件未实现同步能力")

    async def stream_search(self, keyword: str, page: int = 1):
        yield await self.search(keyword, page)

    async def execute_action(self, action: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        raise NotImplementedError("当前插件未实现动作能力")


class PluginManager:
    def __init__(self, plugins_dir: str = "plugins"):
        self.plugins_dir = Path(plugins_dir)
        self.plugins_dir.mkdir(parents=True, exist_ok=True)
        self._plugins: Dict[str, PluginBase] = {}
        self._manifests: Dict[str, Dict[str, Any]] = {}
        self._search_jobs: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def initialize(self):
        await self.scan_plugins()

    async def shutdown(self):
        for plugin in list(self._plugins.values()):
            try:
                await plugin.shutdown()
            except Exception:
                continue
        for job in self._search_jobs.values():
            task = job.get("task")
            if task and not task.done():
                task.cancel()
        self._plugins.clear()
        self._manifests.clear()
        self._search_jobs.clear()

    async def scan_plugins(self) -> List[Dict[str, Any]]:
        async with self._lock:
            discovered_ids: List[str] = []
            for plugin_dir in sorted(self.plugins_dir.iterdir()):
                if not plugin_dir.is_dir():
                    continue
                manifest_path = plugin_dir / "manifest.json"
                if not manifest_path.exists():
                    continue

                manifest = self._load_manifest(manifest_path, plugin_dir)
                plugin = await self._load_plugin_instance(manifest, plugin_dir)
                self._manifests[manifest["id"]] = manifest
                self._plugins[manifest["id"]] = plugin
                discovered_ids.append(manifest["id"])

            removed_ids = [plugin_id for plugin_id in self._plugins.keys() if plugin_id not in discovered_ids]
            for plugin_id in removed_ids:
                plugin = self._plugins.pop(plugin_id, None)
                self._manifests.pop(plugin_id, None)
                if plugin:
                    try:
                        await plugin.shutdown()
                    except Exception:
                        pass

            return self.list_plugins()

    def _load_manifest(self, manifest_path: Path, plugin_dir: Path) -> Dict[str, Any]:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["plugin_dir"] = str(plugin_dir)
        manifest["config_file"] = str((plugin_dir / "config.json").resolve())
        frontend = manifest.get("frontend") or {}
        frontend_entry = frontend.get("entry") or manifest.get("frontend_entry")
        frontend_kind = frontend.get("kind") or manifest.get("frontend_kind")
        config_entry = frontend.get("config_entry") or manifest.get("config_frontend_entry")
        config_kind = frontend.get("config_kind") or manifest.get("config_frontend_kind")
        if frontend_entry:
            manifest["frontend_entry"] = str(frontend_entry).replace("\\", "/")
        else:
            manifest["frontend_entry"] = None
        if not frontend_kind and manifest["frontend_entry"]:
            frontend_kind = "module" if Path(manifest["frontend_entry"]).suffix.lower() in {".js", ".mjs"} else "iframe"
        manifest["frontend_kind"] = frontend_kind or None
        if config_entry:
            manifest["config_frontend_entry"] = str(config_entry).replace("\\", "/")
        else:
            manifest["config_frontend_entry"] = None
        if not config_kind and manifest["config_frontend_entry"]:
            config_kind = "module" if Path(manifest["config_frontend_entry"]).suffix.lower() in {".js", ".mjs"} else "iframe"
        manifest["config_frontend_kind"] = config_kind or None

        default_config_path = plugin_dir / "config.default.json"
        default_config = {}
        if default_config_path.exists():
            default_config = json.loads(default_config_path.read_text(encoding="utf-8"))
        manifest["default_config"] = default_config
        return manifest

    def _get_plugin_config_path(self, manifest: Dict[str, Any]) -> Path:
        return Path(manifest.get("config_file") or (Path(manifest["plugin_dir"]) / "config.json")).resolve()

    def _read_plugin_config_file(self, manifest: Dict[str, Any]) -> Dict[str, Any]:
        config_path = self._get_plugin_config_path(manifest)
        if not config_path.exists():
            return {}
        try:
            data = json.loads(config_path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _write_plugin_config_file(self, manifest: Dict[str, Any], config: Dict[str, Any]):
        config_path = self._get_plugin_config_path(manifest)
        config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")

    def _get_plugin_asset_version(self, manifest: Dict[str, Any], asset_path: Optional[str]) -> str:
        if not asset_path:
            return ""
        try:
            plugin_dir = Path(manifest["plugin_dir"]).resolve()
            target_path = (plugin_dir / asset_path).resolve()
            if not target_path.exists() or not target_path.is_file():
                return ""
            return str(target_path.stat().st_mtime_ns)
        except Exception:
            return ""

    async def _load_plugin_instance(self, manifest: Dict[str, Any], plugin_dir: Path) -> PluginBase:
        plugin_id = manifest["id"]
        enabled = await self.is_enabled(plugin_id, manifest)
        config = await self.get_plugin_config(plugin_id, manifest)
        entry_path = plugin_dir / manifest.get("entry", "plugin.py")
        module_name = f"litepan_plugin_{plugin_id}_{int(time.time() * 1000)}"
        spec = importlib.util.spec_from_file_location(module_name, entry_path)
        if not spec or not spec.loader:
            raise RuntimeError(f"插件 {plugin_id} 加载失败")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        plugin_factory = getattr(module, "create_plugin", None)
        if callable(plugin_factory):
            plugin = plugin_factory(manifest, plugin_dir, config)
        else:
            plugin_cls = getattr(module, "Plugin", None)
            if plugin_cls is None:
                raise RuntimeError(f"插件 {plugin_id} 未提供 create_plugin 或 Plugin")
            plugin = plugin_cls(manifest, plugin_dir, config)

        old_plugin = self._plugins.get(plugin_id)
        if old_plugin:
            try:
                await old_plugin.shutdown()
            except Exception:
                pass

        if enabled:
            await plugin.startup()
        plugin.enabled = enabled
        return plugin

    async def is_enabled(self, plugin_id: str, manifest: Optional[Dict[str, Any]] = None) -> bool:
        stored = await config_manager.get_async(f"plugin_enabled:{plugin_id}")
        if stored is None:
            return bool((manifest or self._manifests.get(plugin_id) or {}).get("enabled_by_default", True))
        return bool(stored)

    async def get_plugin_config(self, plugin_id: str, manifest: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        source_manifest = manifest or self._manifests.get(plugin_id) or {}
        defaults = dict(source_manifest.get("default_config") or {})
        file_config = self._read_plugin_config_file(source_manifest)
        if file_config:
            defaults.update(file_config)
            return defaults

        stored = await config_manager.get_async(f"plugin_config:{plugin_id}") or {}
        defaults.update(stored)
        if stored and source_manifest.get("plugin_dir"):
            self._write_plugin_config_file(source_manifest, defaults)
        return defaults

    def get_plugin(self, plugin_id: str) -> Optional[PluginBase]:
        return self._plugins.get(plugin_id)

    def list_plugins(self) -> List[Dict[str, Any]]:
        plugin_items = []
        for plugin_id, manifest in sorted(self._manifests.items(), key=lambda item: item[1].get("name", item[0])):
            plugin = self._plugins.get(plugin_id)
            frontend_entry = manifest.get("frontend_entry")
            frontend_kind = manifest.get("frontend_kind")
            config_frontend_entry = manifest.get("config_frontend_entry")
            config_frontend_kind = manifest.get("config_frontend_kind")
            frontend_asset_version = self._get_plugin_asset_version(manifest, frontend_entry)
            config_frontend_asset_version = self._get_plugin_asset_version(manifest, config_frontend_entry)
            plugin_items.append({
                "id": plugin_id,
                "name": manifest.get("name", plugin_id),
                "author": manifest.get("author", ""),
                "version": manifest.get("version", "0.0.1"),
                "description": manifest.get("description", ""),
                "type": manifest.get("type", "tool"),
                "enabled": bool(plugin and plugin.enabled),
                "has_frontend": bool(frontend_entry),
                "frontend_entry": frontend_entry,
                "frontend_kind": frontend_kind,
                "frontend_entry_url": f"/api/plugins/{plugin_id}/ui/{frontend_entry}" if frontend_entry else "",
                "frontend_asset_version": frontend_asset_version,
                "has_config_frontend": bool(config_frontend_entry),
                "config_frontend_entry": config_frontend_entry,
                "config_frontend_kind": config_frontend_kind,
                "config_frontend_entry_url": f"/api/plugins/{plugin_id}/ui/{config_frontend_entry}" if config_frontend_entry else "",
                "config_frontend_asset_version": config_frontend_asset_version,
                "config_schema": manifest.get("config_schema", []),
                "config": plugin.config if plugin else manifest.get("default_config", {}),
            })
        return plugin_items

    def get_plugin_ui_file(self, plugin_id: str, asset_path: str) -> Path:
        manifest = self._manifests.get(plugin_id)
        if not manifest:
            raise KeyError(f"插件不存在: {plugin_id}")

        plugin_dir = Path(manifest["plugin_dir"]).resolve()
        target_path = (plugin_dir / asset_path).resolve()
        if plugin_dir not in target_path.parents and target_path != plugin_dir:
            raise RuntimeError("插件资源路径非法")
        if not target_path.exists() or not target_path.is_file():
            raise FileNotFoundError(f"插件资源不存在: {asset_path}")
        return target_path

    async def set_plugin_enabled(self, plugin_id: str, enabled: bool) -> Dict[str, Any]:
        manifest = self._manifests.get(plugin_id)
        if not manifest:
            raise KeyError(f"插件不存在: {plugin_id}")

        await config_manager.set_async(f"plugin_enabled:{plugin_id}", enabled)
        plugin = self._plugins.get(plugin_id)

        if enabled and plugin is None:
            plugin = await self._load_plugin_instance(manifest, Path(manifest["plugin_dir"]))
            self._plugins[plugin_id] = plugin
        elif enabled and plugin is not None and not plugin.enabled:
            await plugin.startup()
            plugin.enabled = True
        elif not enabled and plugin is not None and plugin.enabled:
            await plugin.shutdown()
            plugin.enabled = False

        return next(item for item in self.list_plugins() if item["id"] == plugin_id)

    async def update_plugin_config(self, plugin_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
        manifest = self._manifests.get(plugin_id)
        if not manifest:
            raise KeyError(f"插件不存在: {plugin_id}")

        merged_config = dict(manifest.get("default_config") or {})
        merged_config.update(config or {})
        self._write_plugin_config_file(manifest, merged_config)

        plugin = self._plugins.get(plugin_id)
        if plugin:
            await plugin.on_config_updated(merged_config)

        return next(item for item in self.list_plugins() if item["id"] == plugin_id)

    async def execute_search(self, plugin_id: str, keyword: str, page: int = 1) -> Dict[str, Any]:
        plugin = self._plugins.get(plugin_id)
        if not plugin or not plugin.enabled:
            raise KeyError(f"插件未启用或不存在: {plugin_id}")
        if not hasattr(plugin, "search"):
            raise RuntimeError("插件不支持搜索能力")
        return await plugin.search(keyword=keyword, page=page)

    async def test_plugin_connection(self, plugin_id: str) -> Dict[str, Any]:
        plugin = self._plugins.get(plugin_id)
        if not plugin or not plugin.enabled:
            raise KeyError(f"插件未启用或不存在: {plugin_id}")
        if not hasattr(plugin, "test_connection"):
            raise RuntimeError("插件不支持连通性测试")
        return await plugin.test_connection()

    async def sync_plugin_index(self, plugin_id: str, force: bool = False) -> Dict[str, Any]:
        plugin = self._plugins.get(plugin_id)
        if not plugin or not plugin.enabled:
            raise KeyError(f"插件未启用或不存在: {plugin_id}")
        if not hasattr(plugin, "sync_index"):
            raise RuntimeError("插件不支持同步能力")
        return await plugin.sync_index(force=force)

    async def execute_plugin_action(self, plugin_id: str, action: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        plugin = self._plugins.get(plugin_id)
        if not plugin or not plugin.enabled:
            raise KeyError(f"插件未启用或不存在: {plugin_id}")
        return await plugin.execute_action(action, payload or {})

    async def start_search_job(self, plugin_id: str, keyword: str, page: int = 1) -> Dict[str, Any]:
        plugin = self._plugins.get(plugin_id)
        if not plugin or not plugin.enabled:
            raise KeyError(f"插件未启用或不存在: {plugin_id}")

        job_id = uuid.uuid4().hex
        job = {
            "job_id": job_id,
            "plugin_id": plugin_id,
            "keyword": keyword,
            "page": page,
            "status": "running",
            "items": [],
            "message": "搜索已启动",
            "error": "",
            "updated_at": time.time(),
            "progress": {
                "processed_channels": 0,
                "total_channels": 0,
                "result_count": 0,
            },
        }
        task = asyncio.create_task(self._run_search_job(job_id, plugin, keyword, page))
        job["task"] = task
        self._search_jobs[job_id] = job
        return self._serialize_search_job(job)

    async def _run_search_job(self, job_id: str, plugin: PluginBase, keyword: str, page: int):
        job = self._search_jobs[job_id]
        try:
            async for payload in plugin.stream_search(keyword, page):
                if not payload:
                    continue
                items = payload.get("items") or []
                existing = {(item.get("source"), item.get("share_url")) for item in job["items"]}
                for item in items:
                    unique_key = (item.get("source"), item.get("share_url"))
                    if unique_key in existing:
                        continue
                    existing.add(unique_key)
                    job["items"].append(item)
                progress = payload.get("progress") or {}
                job["progress"] = {
                    "processed_channels": progress.get("processed_channels", job["progress"]["processed_channels"]),
                    "total_channels": progress.get("total_channels", job["progress"]["total_channels"]),
                    "result_count": len(job["items"]),
                }
                job["message"] = payload.get("message") or f"已找到 {len(job['items'])} 条结果"
                job["updated_at"] = time.time()

            job["status"] = "completed"
            job["message"] = f"搜索完成，共找到 {len(job['items'])} 条结果"
            job["progress"]["result_count"] = len(job["items"])
            job["updated_at"] = time.time()
        except asyncio.CancelledError:
            job["status"] = "cancelled"
            job["message"] = "搜索已取消"
            job["updated_at"] = time.time()
        except Exception as e:
            job["status"] = "failed"
            job["error"] = str(e)
            job["message"] = f"搜索失败: {e}"
            job["updated_at"] = time.time()

    def get_search_job(self, job_id: str) -> Dict[str, Any]:
        job = self._search_jobs.get(job_id)
        if not job:
            raise KeyError(f"搜索任务不存在: {job_id}")
        return self._serialize_search_job(job)

    async def cancel_search_job(self, job_id: str) -> Dict[str, Any]:
        job = self._search_jobs.get(job_id)
        if not job:
            raise KeyError(f"搜索任务不存在: {job_id}")
        task = job.get("task")
        if task and not task.done():
          task.cancel()
        job["status"] = "cancelled"
        job["message"] = f"搜索已停止，当前保留 {len(job.get('items') or [])} 条结果"
        job["updated_at"] = time.time()
        progress = job.get("progress") or {}
        progress["result_count"] = len(job.get("items") or [])
        job["progress"] = progress
        return self._serialize_search_job(job)

    def _serialize_search_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "job_id": job["job_id"],
            "plugin_id": job["plugin_id"],
            "keyword": job["keyword"],
            "page": job["page"],
            "status": job["status"],
            "items": job["items"],
            "message": job["message"],
            "error": job["error"],
            "updated_at": job["updated_at"],
            "progress": job["progress"],
        }


plugin_manager = PluginManager()
