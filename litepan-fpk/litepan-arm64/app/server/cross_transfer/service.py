import asyncio
import time
from typing import Any, AsyncGenerator, Dict, List, Optional

from core.base import FileItem
from core.driver_service import get_account_driver_instance
from core.log_manager import LogModule, get_writer

from .methods import get_method

MAX_FILES = 3000
MAX_DEPTH = 40
SCAN_DIR_CONCURRENCY = 6


def _log():
    return get_writer(LogModule.API)


def _source_root_prefix(display_path: str) -> str:
    parts = [p for p in str(display_path or "").strip("/").split("/") if p]
    if not parts:
        return ""
    return f"{parts[-1]}/"


async def _append_file_node(
    driver,
    it: FileItem,
    method: str,
    rel_prefix: str,
    acc: Dict,
    acc_lock: asyncio.Lock,
) -> Optional[Dict]:
    async with acc_lock:
        if acc["count"] >= MAX_FILES:
            return None
        file_hash = await driver.resolve_transfer_hash(it, method, allow_stream=False)
        node = {
            "type": "file",
            "id": it.id,
            "name": it.name,
            "size": int(it.size or 0),
            "hash": file_hash,
            "rel_path": f"{rel_prefix}{it.name}",
            "rel_dir": rel_prefix.rstrip("/"),
            "eligible": bool(file_hash),
            "reuse": None,
        }
        acc["files"].append(node)
        acc["count"] += 1
        return node


async def _walk_source(
    driver,
    parent_id: str,
    method: str,
    rel_prefix: str,
    depth: int,
    acc: Dict,
    acc_lock: asyncio.Lock,
    dir_sem: asyncio.Semaphore,
) -> List[Dict]:
    if depth > MAX_DEPTH:
        return []
    async with acc_lock:
        if acc["count"] >= MAX_FILES:
            return []

    items = await driver.list_files(parent_id)
    dirs = [it for it in items if it.is_dir]
    files = [it for it in items if not it.is_dir]

    async def walk_dir(it: FileItem) -> Optional[Dict]:
        async with dir_sem:
            async with acc_lock:
                if acc["count"] >= MAX_FILES:
                    return None
            child_prefix = f"{rel_prefix}{it.name}/"
            children = await _walk_source(
                driver, str(it.id), method, child_prefix, depth + 1, acc, acc_lock, dir_sem,
            )
            return {"type": "dir", "id": it.id, "name": it.name, "children": children}

    dir_nodes = [node for node in await asyncio.gather(*[walk_dir(it) for it in dirs]) if node]

    file_nodes: List[Dict] = []
    for it in files:
        node = await _append_file_node(driver, it, method, rel_prefix, acc, acc_lock)
        if node:
            file_nodes.append(node)
        else:
            break

    return dir_nodes + file_nodes


def _count_shallow_dirs(tree: List[Dict]) -> int:
    total = 0
    for node in tree:
        if node.get("type") != "dir":
            continue
        total += 1
        for child in node.get("children") or []:
            if child.get("type") == "dir":
                total += 1
    return total


async def _ensure_file_hash(src, file_rec: Dict, method_id: str, *, allow_stream: bool = True) -> str:
    existing = str(file_rec.get("hash") or "").strip().lower()
    if existing:
        return existing

    source_file_id = str(file_rec.get("source_file_id") or "").strip()
    if not source_file_id:
        return ""

    file_info = await src.file_info(source_file_id)
    if not file_info:
        return ""

    return await src.resolve_transfer_hash(file_info, method_id, allow_stream=allow_stream)


async def scan_source(
    *,
    source_account_id: int,
    source_parent_id: str,
    method_id: str,
    source_display_path: str = "",
) -> Dict[str, Any]:
    method = get_method(method_id)
    src = await get_account_driver_instance(source_account_id)

    acc: Dict[str, Any] = {"count": 0, "files": []}
    acc_lock = asyncio.Lock()
    dir_sem = asyncio.Semaphore(SCAN_DIR_CONCURRENCY)
    root_prefix = _source_root_prefix(source_display_path)
    tree = await _walk_source(src, source_parent_id, method.id, root_prefix, 0, acc, acc_lock, dir_sem)
    files: List[Dict] = acc["files"]

    return {
        "tree": tree,
        "total": len(files),
        "shallow_dirs": _count_shallow_dirs(tree),
        "truncated": acc["count"] >= MAX_FILES,
        "files": [
            {
                "source_file_id": f["id"],
                "rel_path": f["rel_path"],
                "rel_dir": f["rel_dir"],
                "name": f["name"],
                "size": f["size"],
                "hash": f["hash"],
                "eligible": f["eligible"],
            }
            for f in files
        ],
    }


async def probe_stream(
    *,
    source_account_id: int,
    target_account_id: int,
    target_parent_id: str,
    method_id: str,
    files: List[Dict],
) -> AsyncGenerator[Dict[str, Any], None]:
    method = get_method(method_id)
    src = await get_account_driver_instance(source_account_id)
    tgt = await get_account_driver_instance(target_account_id)

    yield {"event": "start", "total": len(files)}

    probe_folder_id = ""
    ok = 0
    no = 0
    try:
        probe_name = f"_litepan_probe_{int(time.time())}"
        created = await tgt.create_folder(target_parent_id, probe_name)
        if not created.success:
            yield {"event": "error", "message": f"创建临时探测目录失败: {created.message}"}
            return
        probe_folder_id = str((created.data or {}).get("folder_id") or "")
        if not probe_folder_id:
            yield {"event": "error", "message": "创建临时探测目录失败: 未返回目录ID"}
            return

        for f in files:
            rel_path = f.get("rel_path")
            file_hash = str(f.get("hash") or "").strip().lower()
            if not file_hash:
                yield {"event": "hashing", "rel_path": rel_path, "name": f.get("name")}
                file_hash = await _ensure_file_hash(src, f, method.id)
                if file_hash:
                    f["hash"] = file_hash
            reuse = False
            probe_error = ""
            if file_hash:
                try:
                    result = await tgt.rapid_upload_by_hash(
                        probe_folder_id, f.get("name"), method.id, file_hash, int(f.get("size") or 0), duplicate=2,
                    )
                    reuse = bool(result.data and result.data.get("reuse"))
                except Exception as exc:
                    probe_error = str(exc)
                    _log().warning(f"跨盘秒传试探失败 {f.get('name')}: {exc}")
                    reuse = False
            if reuse:
                ok += 1
            else:
                no += 1
            yield {"event": "item", "rel_path": rel_path, "reuse": reuse, "hash": file_hash, "error": probe_error}

        yield {"event": "end", "ok": ok, "no": no}
    finally:
        if probe_folder_id:
            try:
                await tgt.purge_file(probe_folder_id)
            except Exception as exc:
                _log().warning(f"清理临时探测目录失败 {probe_folder_id}: {exc}")


async def _ensure_target_dir(
    driver,
    root_id: str,
    rel_dir: str,
    cache: Dict[str, str],
    created_dirs: Optional[List[str]] = None,
) -> str:
    if rel_dir in cache:
        return cache[rel_dir]

    parts = [p for p in rel_dir.split("/") if p]
    cur = root_id
    cur_rel = ""
    for part in parts:
        cur_rel = f"{cur_rel}/{part}" if cur_rel else part
        if cur_rel in cache:
            cur = cache[cur_rel]
            continue
        children = await driver.list_files(cur)
        found = next((c for c in children if c.is_dir and c.name == part), None)
        if found:
            cur = found.id
        else:
            created = await driver.create_folder(cur, part)
            if not created.success:
                raise RuntimeError(created.message or f"创建目录失败: {part}")
            cur = str((created.data or {}).get("folder_id") or "")
            if not cur:
                raise RuntimeError(f"创建目录失败: {part}")
            if created_dirs is not None:
                created_dirs.append(cur)
        cache[cur_rel] = cur
    cache[rel_dir] = cur
    return cur


async def _execute_transfer_file(
    *,
    f: Dict,
    method_id: str,
    tgt,
    target_parent_id: str,
    dir_cache: Dict[str, str],
    duplicate: int,
    fallback: bool,
    source_account_id: int,
    source_account_name: str,
    source_driver_type: str,
    target_account_id: int,
    target_account_name: str,
    target_driver_type: str,
    target_display_path: str,
    conflict: str,
    relay_task_manager,
    dir_cache_created: Optional[List[str]] = None,
) -> Dict[str, Any]:
    name = f.get("name")
    file_hash = str(f.get("hash") or "").strip().lower()
    rel_dir = f.get("rel_dir") or ""
    rel_path = f.get("rel_path")
    if not file_hash:
        src = await get_account_driver_instance(source_account_id)
        file_hash = await _ensure_file_hash(src, f, method_id)
        if file_hash:
            f["hash"] = file_hash
    if not file_hash:
        return {
            "rel_path": rel_path,
            "name": name,
            "success": False,
            "mode": "skip",
            "error": "缺少指纹",
        }
    try:
        folder_id = await _ensure_target_dir(tgt, target_parent_id, rel_dir, dir_cache, dir_cache_created)
        result = await tgt.rapid_upload_by_hash(
            folder_id, name, method_id, file_hash, int(f.get("size") or 0), duplicate=duplicate,
        )
        reuse = bool(result.data and result.data.get("reuse"))
        if reuse:
            return {
                "rel_path": rel_path,
                "name": name,
                "success": True,
                "mode": "rapid",
                "file_id": (result.data or {}).get("file_id", ""),
                "error": "",
            }

        source_file_id = str(f.get("source_file_id") or "").strip()
        if fallback and source_file_id:
            await relay_task_manager.create_task(
                source_account_id=source_account_id,
                source_account_name=source_account_name,
                source_driver_type=source_driver_type,
                target_account_id=target_account_id,
                target_account_name=target_account_name,
                target_driver_type=target_driver_type,
                source_file_id=source_file_id,
                file_name=name,
                rel_path=f.get("rel_path") or "",
                rel_dir=rel_dir,
                target_parent_id=target_parent_id,
                target_display_path=target_display_path,
                total_bytes=int(f.get("size") or 0),
                method=method_id,
                conflict_policy=conflict,
            )
            return {
                "rel_path": rel_path,
                "name": name,
                "success": False,
                "mode": "relay",
                "file_id": "",
                "error": "",
            }
        return {
            "rel_path": rel_path,
            "name": name,
            "success": False,
            "mode": "rapid",
            "file_id": "",
            "error": "未命中秒传",
        }
    except Exception as exc:
        _log().warning(f"跨盘秒传执行失败 {name}: {exc}")
        return {
            "rel_path": rel_path,
            "name": name,
            "success": False,
            "mode": "error",
            "error": str(exc),
        }


async def execute_stream(
    *,
    source_account_id: int,
    source_account_name: str,
    source_driver_type: str,
    target_account_id: int,
    target_account_name: str,
    target_driver_type: str,
    target_parent_id: str,
    target_display_path: str,
    method_id: str,
    files: List[Dict],
    conflict: str = "rename",
    fallback: bool = False,
) -> AsyncGenerator[Dict[str, Any], None]:
    from cross_transfer.relay_task_manager import relay_task_manager

    method = get_method(method_id)
    tgt = await get_account_driver_instance(target_account_id)
    duplicate = 2 if str(conflict).lower() == "overwrite" else 1
    dir_cache: Dict[str, str] = {"": target_parent_id}
    dir_cache_created: List[str] = []
    results: List[Dict] = []
    relay_queued = 0

    yield {"event": "start", "total": len(files)}

    for f in files:
        item = await _execute_transfer_file(
            f=f,
            method_id=method.id,
            tgt=tgt,
            target_parent_id=target_parent_id,
            dir_cache=dir_cache,
            duplicate=duplicate,
            fallback=fallback,
            source_account_id=source_account_id,
            source_account_name=source_account_name,
            source_driver_type=source_driver_type,
            target_account_id=target_account_id,
            target_account_name=target_account_name,
            target_driver_type=target_driver_type,
            target_display_path=target_display_path,
            conflict=conflict,
            relay_task_manager=relay_task_manager,
            dir_cache_created=dir_cache_created,
        )
        results.append(item)
        if item.get("mode") == "relay":
            relay_queued += 1
        yield {"event": "item", **item}

    # 未开兜底时清理本次创建且仍为空的目录。
    if not fallback and dir_cache_created:
        for folder_id in reversed(dir_cache_created):
            try:
                children = await tgt.list_files(folder_id)
                if not children:
                    await tgt.purge_file(folder_id)
            except Exception as exc:
                _log().warning(f"清理空目录失败 {folder_id}: {exc}")

    rapid_done = sum(1 for r in results if r.get("mode") == "rapid" and r["success"])
    yield {
        "event": "end",
        "done": rapid_done,
        "total": len(files),
        "rapid_done": rapid_done,
        "relay_queued": relay_queued,
        "results": results,
    }


async def execute_transfer(
    *,
    source_account_id: int,
    source_account_name: str,
    source_driver_type: str,
    target_account_id: int,
    target_account_name: str,
    target_driver_type: str,
    target_parent_id: str,
    target_display_path: str,
    method_id: str,
    files: List[Dict],
    conflict: str = "rename",
    fallback: bool = False,
) -> Dict[str, Any]:
    summary: Optional[Dict[str, Any]] = None
    async for msg in execute_stream(
        source_account_id=source_account_id,
        source_account_name=source_account_name,
        source_driver_type=source_driver_type,
        target_account_id=target_account_id,
        target_account_name=target_account_name,
        target_driver_type=target_driver_type,
        target_parent_id=target_parent_id,
        target_display_path=target_display_path,
        method_id=method_id,
        files=files,
        conflict=conflict,
        fallback=fallback,
    ):
        if msg.get("event") == "end":
            summary = msg
    return summary or {
        "done": 0,
        "total": len(files),
        "rapid_done": 0,
        "relay_queued": 0,
        "results": [],
    }
