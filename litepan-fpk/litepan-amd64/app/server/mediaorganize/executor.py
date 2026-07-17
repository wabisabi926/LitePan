"""执行整理计划"""

import asyncio
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from mediaorganize import rules
from mediaorganize.planner import Plan, PlanAction


class ExecutorStopped(Exception):
    pass


_VERIFY_AFTER_MOVE_SECONDS = 1.0


class Executor:
    def __init__(
        self,
        driver,
        plan: Plan,
        task_id: str,
        log_fn: Callable[[str], None],
        check_stop: Callable[[], None],
        progress_fn: Optional[Callable[[int, int, str], None]] = None,
        cache_cleaner=None,
        overwrite_existing: bool = False,
    ):
        self.driver = driver
        self.plan = plan
        self.task_id = task_id
        self.log = log_fn
        self.check_stop = check_stop
        self.progress_fn = progress_fn
        self.cache_cleaner = cache_cleaner
        self.overwrite_existing = bool(overwrite_existing)

        self.dir_listing_cache: Dict[str, Any] = {}
        self.resolved: Dict[str, str] = {}

        self.stats = {
            "ensured_dirs": 0,
            "relocated": 0,
            "renamed_meta": 0,
            "skipped": 0,
            "failed": 0,
            "overwritten": 0,
            "total_actions": len(plan.actions),
        }

    # ============================ apply 入口 ============================

    async def apply(self) -> Dict[str, Any]:
        ordered = self._topo_sort()

        ensure_actions = [a for a in ordered if a.kind in ("ensure_dir", "move_and_rename_dir")]
        relocate_actions = [a for a in ordered if a.kind == "relocate"]
        delete_actions = [a for a in ordered if a.kind == "delete_empty_dir"]

        total = len(ordered)
        progress_index = 0

        for action in ensure_actions:
            self.check_stop()
            progress_index += 1
            try:
                if action.kind == "move_and_rename_dir":
                    await self._exec_move_and_rename_dir(action)
                else:
                    await self._exec_ensure_dir(action)
            except ExecutorStopped:
                raise
            except Exception as e:
                action.status = "failed"
                action.error = str(e)
                self.stats["failed"] += 1
                self.log(f"[执行] 动作失败 {action.id} ({action.kind}): {e}")
            finally:
                action.executed_at = self._now_str()
            self._notify_progress(progress_index, total, action)

        # Phase 2: 冲突预扫描
        await self._prescan_conflicts(relocate_actions)

        # Phase 3: 批量执行 relocate
        await self._execute_relocates(relocate_actions, total, progress_index)
        progress_index += len(relocate_actions)

        # Phase 4: 元数据跟随
        await self._apply_metadata_followers()

        # Phase 5: 删除空目录
        for action in delete_actions:
            self.check_stop()
            progress_index += 1
            try:
                await self._exec_delete_empty_dir(action)
            except ExecutorStopped:
                raise
            except Exception as e:
                action.status = "failed"
                action.error = str(e)
                self.stats["failed"] += 1
                self.log(f"[执行] 动作失败 {action.id} (delete_empty_dir): {e}")
            finally:
                action.executed_at = self._now_str()
            self._notify_progress(progress_index, total, action)

        return {
            "task_id": self.task_id,
            "stats": dict(self.stats),
            "actions": [a.to_dict() for a in self.plan.actions],
        }

    # ============================ Phase 2: 冲突预扫描 ============================

    async def _prescan_conflicts(self, relocate_actions: List[PlanAction]):
        """执行 relocate 前一次性扫描所有目标目录，提前判断冲突。

        - 把待写入目标按 target_parent_id 分组
        - 每个目录一次 list_files，得到 name → id 的映射
        - 命中冲突时：
          - overwrite_existing=True → 标记 _overwrite_target_id 待删除
          - overwrite_existing=False → 直接标 skipped + 写明原因
        """
        if not relocate_actions:
            return

        # 先 resolve 所有目标父目录的真实 id（ensure_dir 已 done 后才能拿到）
        targets_by_dir: Dict[str, List[PlanAction]] = {}
        for action in relocate_actions:
            target_parent_id = self._resolve_ref(action.target_parent_id)
            if not target_parent_id:
                continue
            action._resolved_target_parent_id = target_parent_id
            targets_by_dir.setdefault(target_parent_id, []).append(action)

        conflicts_total = 0
        intra_batch_conflicts = 0
        for parent_id, actions in targets_by_dir.items():
            self.check_stop()
            try:
                items = await self._list_dir(parent_id, force=True)
            except Exception as e:
                self.log(f"[预扫描] 列目标目录失败 {parent_id}: {e}（将退化为执行时检查）")
                continue
            name_index = {item.name: item.id for item in items}
            # 同一 target_parent 下，模拟"已被本批占用的 target_name"
            claimed_names: Dict[str, PlanAction] = {}
            for action in actions:
                if action.status in ("done", "skipped", "failed"):
                    continue
                # 1) 批内冲突：另一个 action 也想生成同名
                prev = claimed_names.get(action.target_name)
                if prev is not None:
                    intra_batch_conflicts += 1
                    action.status = "skipped"
                    action.error = (
                        f"另一项也将生成同名「{action.target_name}」"
                        f"（源「{prev.source_name}」与「{action.source_name}」识别为同一作品，"
                        f"请先在源目录手动合并）"
                    )
                    action.executed_at = self._now_str()
                    self.stats["skipped"] += 1
                    continue
                # 2) 与磁盘上已有的 dir/file 冲突
                existing_id = name_index.get(action.target_name)
                if existing_id and str(existing_id) != str(action.source_id):
                    conflicts_total += 1
                    if self.overwrite_existing:
                        action._overwrite_target_id = existing_id
                        action._overwrite_target_name = action.target_name
                    else:
                        action.status = "skipped"
                        action.error = "目标已存在同名（未开启覆盖）"
                        action.executed_at = self._now_str()
                        self.stats["skipped"] += 1
                        continue
                # 占用 target_name（用于后续 action 检测）
                claimed_names[action.target_name] = action

        if conflicts_total:
            mode = "覆盖" if self.overwrite_existing else "跳过"
            self.log(f"[预扫描] 发现 {conflicts_total} 个目标已存在冲突，按「{mode}」处理")
        if intra_batch_conflicts:
            self.log(f"[预扫描] 发现 {intra_batch_conflicts} 个同批次目标重名（同作品多版本），已跳过")

    # ============================ Phase 3: 批量执行 relocate ============================

    async def _execute_relocates(self, relocate_actions: List[PlanAction], total_for_progress: int, base_progress: int):
        """按 (source_parent, target_parent) 分组聚合批量 move，再逐个 rename。

        策略：
        1. 同 parent + 同 target 的 action 聚合，**先批量 move**（一次 API 调用）
        2. 批量失败 → 退化为单文件 move（隔离失败的那一个）
        3. 单文件依然失败 → safe_move 验证（list 一次源/目标判断真实状态）
        4. 移到目标后逐个 rename
        """
        pending = [a for a in relocate_actions if a.status not in ("done", "skipped", "failed")]

        # 优先处理 overwrite 删除：把同目标目录的所有要删项目聚合成 batch delete
        await self._exec_overwrite_deletions(pending)

        # 按 (current_parent, target_parent) 分组
        groups: Dict[Tuple[str, str], List[PlanAction]] = {}
        same_dir_renames: List[PlanAction] = []
        for action in pending:
            if action.status in ("done", "skipped", "failed"):
                continue
            target_parent_id = getattr(action, "_resolved_target_parent_id", None) or self._resolve_ref(action.target_parent_id)
            if not target_parent_id:
                action.status = "failed"
                action.error = "目标父目录未解析"
                action.executed_at = self._now_str()
                self.stats["failed"] += 1
                continue
            current_parent = str(action.source_parent_id)
            if current_parent == str(target_parent_id):
                same_dir_renames.append(action)
            else:
                groups.setdefault((current_parent, str(target_parent_id)), []).append(action)

        progress_step = base_progress

        # 同目录改名
        for action in same_dir_renames:
            self.check_stop()
            progress_step += 1
            await self._exec_same_dir_rename(action)
            self._notify_progress(progress_step, total_for_progress, action)

        # 跨目录：批量 move + 逐个 rename
        for (current_parent, target_parent_id), actions in groups.items():
            self.check_stop()
            await self._exec_batch_move(actions, current_parent, target_parent_id)
            # 移动完成后逐个 rename 到目标名
            for action in actions:
                self.check_stop()
                progress_step += 1
                if action.status not in ("failed",):
                    await self._post_move_rename(action, target_parent_id)
                self._notify_progress(progress_step, total_for_progress, action)

    async def _exec_overwrite_deletions(self, actions: List[PlanAction]):
        """overwrite=true 场景：把待覆盖的目标文件聚合成 batch delete 先清掉。"""
        to_delete_by_dir: Dict[str, List[Tuple[str, str]]] = {}
        for action in actions:
            target_id = getattr(action, "_overwrite_target_id", None)
            if not target_id:
                continue
            target_parent_id = getattr(action, "_resolved_target_parent_id", None)
            if not target_parent_id:
                continue
            to_delete_by_dir.setdefault(str(target_parent_id), []).append(
                (str(target_id), getattr(action, "_overwrite_target_name", "") or "")
            )

        for parent_id, items in to_delete_by_dir.items():
            self.check_stop()
            # 覆盖删除的冲突项通常很少，直接逐个删并精确计数；
            # 不依赖 batch_delete_file（部分驱动是循环实现，删一半失败会导致整组重删与漏计）。
            success_count = 0
            for iid, name in items:
                try:
                    rr = await self.driver.delete_file(iid)
                    if rr.success:
                        success_count += 1
                    else:
                        self.log(f"[覆盖] 删除失败 {name}: {rr.message}")
                except Exception as e:
                    self.log(f"[覆盖] 删除异常 {name}: {e}")
            self.stats["overwritten"] += success_count
            await self._invalidate_dir_cache(parent_id)
            self.log(f"[覆盖] 已清理目标目录 {parent_id} 内 {success_count}/{len(items)} 个冲突项")

    async def _exec_same_dir_rename(self, action: PlanAction):
        """同目录改名：保持单文件 rename，但带 safe_verify。"""
        try:
            current = await self._find_item_in_dir(
                action.source_parent_id,
                action.source_id,
                source_name_hint=action.source_name or self._path_basename(action.source_id),
            )
            if not current:
                action.status = "failed"
                action.error = f"源文件不存在: {action.source_id}"
                action.executed_at = self._now_str()
                self.stats["failed"] += 1
                return
            if current["name"] == action.target_name:
                action.status = "skipped"
                action.error = "已是目标名"
                action.executed_at = self._now_str()
                self.stats["skipped"] += 1
                return
            before_name = current["name"]
            rename_id = current["id"]
            old_dir_prefix = str(rename_id) if current.get("is_dir") and self._is_path_file_id(rename_id) else ""
            await self._rename_with_verify(rename_id, action.target_name, action.source_parent_id, before_name)
            if self._is_path_file_id(rename_id):
                action.source_id = self._renamed_path_id(rename_id, action.target_name)
            else:
                action.source_id = rename_id
            if old_dir_prefix:
                self._remap_path_prefix(old_dir_prefix, action.source_id)
            action.status = "done"
            action.resolved_id = action.source_id
            action.executed_at = self._now_str()
            self.stats["relocated"] += 1
            self.log(f"[执行] 改名 {before_name} → {action.target_name}")
        except ExecutorStopped:
            raise
        except Exception as e:
            action.status = "failed"
            action.error = str(e)
            action.executed_at = self._now_str()
            self.stats["failed"] += 1
            self.log(f"[执行] 动作失败 {action.id} (rename): {e}")
        finally:
            await self._invalidate_dir_cache(action.source_parent_id)

    async def _exec_batch_move(self, actions: List[PlanAction], current_parent: str, target_parent_id: str):
        """同 (source_parent, target_parent) 的多个 action 聚合成一次批量 move。"""
        if not actions:
            return

        # 过滤已不存在的源 id
        ids = []
        valid_actions = []
        for action in actions:
            name_hint = action.source_name or self._path_basename(action.source_id)
            current = await self._find_item_in_dir(
                action.source_parent_id,
                action.source_id,
                source_name_hint=name_hint,
            )
            if not current:
                # 可能已经被前一次执行移走了：去目标目录确认
                check = await self._find_item_in_dir(
                    target_parent_id,
                    action.source_id,
                    target_name_hint=action.target_name,
                    source_name_hint=name_hint,
                )
                if check:
                    # 已经在目标位置，标记 done（后续仍需 rename 到目标名）
                    action.source_id = check["id"]
                    action.source_parent_id = str(target_parent_id)
                    action._already_in_target = True
                    valid_actions.append(action)
                    continue
                action.status = "failed"
                action.error = f"源文件不存在: {action.source_id}"
                action.executed_at = self._now_str()
                self.stats["failed"] += 1
                continue
            ids.append(action.source_id)
            valid_actions.append(action)

        if not ids:
            # 全部已在目标或都失败
            return

        # 尝试批量 move
        batch_success = False
        try:
            r = await self.driver.move_file(ids, target_parent_id)
            batch_success = bool(r and r.success)
            if not batch_success:
                self.log(f"[执行] 批量移动 {len(ids)} 项失败（{getattr(r, 'message', '未知')}），改为逐个移动")
        except Exception as e:
            self.log(f"[执行] 批量移动 {len(ids)} 项异常（{e}），改为逐个移动")

        # 失效相关目录缓存
        await self._invalidate_dir_cache(current_parent)
        await self._invalidate_dir_cache(target_parent_id)

        if batch_success:
            for action in valid_actions:
                if getattr(action, "_already_in_target", False):
                    continue
                self._apply_path_move_result(action, target_parent_id)
                action._moved_to_target = True
            return

        # 单文件回退
        for action in valid_actions:
            if getattr(action, "_already_in_target", False):
                continue
            self.check_stop()
            try:
                await self._safe_move_single(
                    action.source_id,
                    target_parent_id,
                    current_parent,
                    source_name_hint=action.source_name or self._path_basename(action.source_id),
                )
                self._apply_path_move_result(action, target_parent_id)
                action._moved_to_target = True
            except Exception as e:
                action.status = "failed"
                action.error = f"移动失败: {e}"
                action.executed_at = self._now_str()
                self.stats["failed"] += 1
                self.log(f"[执行] 移动失败 {action.source_name}: {e}")

    async def _safe_move_single(
        self,
        file_id: str,
        target_parent_id: str,
        source_parent_id: str,
        source_name_hint: Optional[str] = None,
    ):
        """单文件 move + 失败时 list 验证：返回成功 / 抛 Exception。"""
        name_hint = source_name_hint or self._path_basename(file_id)
        last_err: Optional[Exception] = None
        try:
            r = await self.driver.move_file([file_id], target_parent_id)
            if r.success:
                await self._invalidate_dir_cache(source_parent_id)
                await self._invalidate_dir_cache(target_parent_id)
                return
            last_err = Exception(r.message or "移动失败（驱动返回 False）")
        except Exception as e:
            last_err = e

        # 失败/异常：等待后重新 list 验证
        await asyncio.sleep(_VERIFY_AFTER_MOVE_SECONDS)
        await self._invalidate_dir_cache(source_parent_id)
        await self._invalidate_dir_cache(target_parent_id)

        lookup_id = self._moved_path_id(file_id, target_parent_id) if self._is_path_file_id(file_id) else file_id
        in_target = await self._find_item_in_dir(
            target_parent_id,
            lookup_id,
            source_name_hint=name_hint,
        )
        if in_target:
            self.log(f"[执行] 移动报错但实际已成功（已二次确认）")
            return
        in_source = await self._find_item_in_dir(source_parent_id, file_id, source_name_hint=name_hint)
        if not in_source:
            self.log(f"[执行] 移动后源/目标都找不到，按失败处理: {file_id}")
        raise last_err or Exception("移动失败")

    async def _post_move_rename(self, action: PlanAction, target_parent_id: str):
        """move 完成后改名到 target_name。"""
        if action.status in ("failed",):
            return
        try:
            name_hint = action.source_name or self._path_basename(action.source_id)
            lookup_id = action.source_id
            if self._is_path_file_id(action.source_id):
                lookup_id = self._moved_path_id(action.source_id, target_parent_id)
            current = await self._find_item_in_dir(
                target_parent_id,
                lookup_id,
                target_name_hint=action.target_name,
                source_name_hint=name_hint,
            )
            if not current:
                # 还在源目录（之前没移动）
                action.status = "failed"
                action.error = "移动后目标目录找不到文件"
                action.executed_at = self._now_str()
                self.stats["failed"] += 1
                return
            current_name = current["name"]
            rename_id = current["id"]
            if current_name == action.target_name:
                action.status = "done"
                action.source_id = rename_id
                action.resolved_id = rename_id
                action.executed_at = self._now_str()
                self.stats["relocated"] += 1
                self.log(f"[执行] 整理 {current_name}（同名免改）")
                return
            # 改名前再检查一次目标名冲突（前面已 prescan，但 list 期间可能新增）
            conflict = await self._find_dir_item_by_name(target_parent_id, action.target_name)
            if conflict and str(conflict) != str(rename_id):
                if self.overwrite_existing:
                    try:
                        await self.driver.delete_file(conflict)
                        await self._invalidate_dir_cache(target_parent_id)
                        self.stats["overwritten"] += 1
                    except Exception as e:
                        action.status = "failed"
                        action.error = f"覆盖冲突文件失败: {e}"
                        action.executed_at = self._now_str()
                        self.stats["failed"] += 1
                        return
                else:
                    action.status = "skipped"
                    action.error = "执行期间目标已存在同名"
                    action.executed_at = self._now_str()
                    self.stats["skipped"] += 1
                    return
            old_dir_prefix = str(rename_id) if current.get("is_dir") and self._is_path_file_id(rename_id) else ""
            await self._rename_with_verify(rename_id, action.target_name, target_parent_id, current_name)
            if self._is_path_file_id(rename_id):
                action.source_id = self._renamed_path_id(rename_id, action.target_name)
            else:
                action.source_id = rename_id
            if old_dir_prefix:
                self._remap_path_prefix(old_dir_prefix, action.source_id)
            action.status = "done"
            action.resolved_id = action.source_id
            action.executed_at = self._now_str()
            self.stats["relocated"] += 1
            self.log(f"[执行] 整理 {current_name} → {action.target_name}")
        except ExecutorStopped:
            raise
        except Exception as e:
            action.status = "failed"
            action.error = f"改名失败: {e}"
            action.executed_at = self._now_str()
            self.stats["failed"] += 1
            self.log(f"[执行] 改名失败 {action.source_name}: {e}")
        finally:
            await self._invalidate_dir_cache(target_parent_id)

    async def _rename_with_verify(self, file_id: str, new_name: str, parent_id: str, before_name: str):
        rename_id = file_id
        if self._is_path_file_id(file_id):
            current = await self._find_item_in_dir(parent_id, file_id, source_name_hint=before_name)
            if current:
                rename_id = current["id"]
        last_err: Optional[Exception] = None
        try:
            r = await self.driver.rename_file(rename_id, new_name)
            if r.success:
                return
            last_err = Exception(r.message or "重命名失败（驱动返回 False）")
        except Exception as e:
            last_err = e

        # 验证：可能驱动返回失败但已成功
        await asyncio.sleep(_VERIFY_AFTER_MOVE_SECONDS)
        await self._invalidate_dir_cache(parent_id)
        lookup_id = self._renamed_path_id(rename_id, new_name) if self._is_path_file_id(rename_id) else rename_id
        current = await self._find_item_in_dir(
            parent_id,
            lookup_id,
            target_name_hint=new_name,
            source_name_hint=before_name,
        )
        if current and current["name"] == new_name:
            self.log(f"[执行] 改名报错但实际已成功（已二次确认）：{before_name} → {new_name}")
            return
        raise last_err or Exception("重命名失败")

    # ============================ Phase 1: ensure_dir ============================

    async def _exec_move_and_rename_dir(self, action: PlanAction):
        target_parent_id = self._resolve_ref(action.target_parent_id)
        if not target_parent_id:
            raise Exception(f"父目录未解析: {action.target_parent_id}")
        source_id = str(action.source_id or "")
        target_name = action.target_name
        source_label = action.source_name or source_id
        promoted_from_tv_tree = bool((action.metadata or {}).get("promoted_from_tv_tree"))

        # 降级条件 1：目标下已有同名目录（独立电影提升时仍允许后续 relocate 并入）
        existing = await self._find_child_dir(target_parent_id, target_name)
        if existing and not promoted_from_tv_tree:
            action.status = "done"
            action.resolved_id = existing
            self.resolved[action.id] = existing
            self.log(f"[执行] 目标已存在「{target_name}」，复用现有目录")
            return
        if existing and promoted_from_tv_tree:
            action.status = "done"
            action.resolved_id = existing
            self.resolved[action.id] = existing
            self.stats["ensured_dirs"] += 1
            self.log(
                f"[执行] 目标已存在「{target_name}」，独立电影将搬入该目录"
                f"（源：{source_label}）"
            )
            return

        # 降级条件 2：源目录里有未参与整理的文件（防止误带走用户的笔记/草稿）
        planned_files = {
            str(a.source_id) for a in self.plan.actions
            if a.kind == "relocate" and str(a.source_parent_id) == source_id
        }
        # 元数据文件也算"计划内"（它们在 meta_followers 里跟随）
        followers = self.plan.diagnostics.get("meta_followers") or []
        planned_meta_dirs = {str(f.get("source_dir_id")) for f in followers if f.get("source_dir_id")}

        try:
            items = await self.driver.list_files(source_id) or []
        except Exception as e:
            self.log(f"[执行] 读取源目录失败 {source_label}: {e}，改用新建目录的方式")
            items = None

        can_whole_move = True
        if items is None:
            can_whole_move = False
        else:
            for it in items:
                if it.is_dir:
                    if promoted_from_tv_tree:
                        continue
                    # 源里有子目录 → 不知道里面有什么，保守降级
                    can_whole_move = False
                    self.log(f"[执行] 源目录「{source_label}」含其它子目录，改用新建目录")
                    break
                if str(it.id) in planned_files:
                    continue
                # 元数据文件（nfo/srt/...）依靠 followers 跟随，整体移动正好把它们一并搬走
                if source_id in planned_meta_dirs and self._is_metadata_file(it.name):
                    continue
                # 未参与计划的随手文件
                can_whole_move = False
                self.log(f"[执行] 源目录「{source_label}」含非整理文件「{it.name}」，改用新建目录")
                break

        if not can_whole_move:
            # 降级：创建空目录（等同 ensure_dir）
            try:
                r = await self.driver.create_folder(target_parent_id, target_name)
            except Exception as e:
                raise Exception(f"创建目录异常: {e}")
            if not r.success:
                existing_id = await self._find_child_dir(target_parent_id, target_name, force=True)
                if existing_id:
                    action.status = "done"
                    action.resolved_id = existing_id
                    self.resolved[action.id] = existing_id
                    self.stats["ensured_dirs"] += 1
                    return
                raise Exception(f"创建目录失败: {r.message}")
            folder_id = (r.data or {}).get("folder_id") if r.data else None
            if not folder_id:
                folder_id = await self._find_child_dir(target_parent_id, target_name, force=True)
            if not folder_id:
                raise Exception("创建目录后无法定位 ID")
            action.status = "done"
            action.resolved_id = folder_id
            self.resolved[action.id] = folder_id
            self.stats["ensured_dirs"] += 1
            return

        # 真正整体移动：move + rename
        old_source_id = source_id
        source_basename = action.source_name or self._path_basename(source_id)
        try:
            r = await self.driver.move_file([source_id], target_parent_id)
        except Exception as e:
            raise Exception(f"整体移动异常: {e}")

        await self._invalidate_dir_cache(source_id)
        await self._invalidate_dir_cache(target_parent_id)

        lookup_id = self._moved_path_id(source_id, target_parent_id) if self._is_path_file_id(source_id) else source_id
        current = await self._find_item_in_dir(
            target_parent_id,
            lookup_id,
            source_name_hint=source_basename,
        )
        if not current:
            if not r.success:
                if self._is_path_file_id(source_id):
                    raise Exception(f"整体移动失败: {r.message}")
                await asyncio.sleep(_VERIFY_AFTER_MOVE_SECONDS)
                info = await self.driver.file_info(source_id)
                if not info or str(getattr(info, "extra", {}).get("parent_id", "")) != str(target_parent_id):
                    raise Exception(f"整体移动失败: {r.message}")
                self.log(f"[执行] 整体搬运报错但实际已成功（已二次确认）：{source_label}")
                current_name = info.name if info else source_basename
                rename_id = source_id
            else:
                raise Exception(f"整体移动后找不到目录: {source_label}")
        else:
            if not r.success:
                self.log(f"[执行] 整体搬运报错但实际已成功（已二次确认）：{source_label}")
            rename_id = current["id"]
            current_name = current["name"]

        final_id = rename_id
        if current_name != target_name:
            await self._rename_with_verify(rename_id, target_name, target_parent_id, current_name)
            renamed = await self._find_item_in_dir(
                target_parent_id,
                self._renamed_path_id(rename_id, target_name) if self._is_path_file_id(rename_id) else rename_id,
                target_name_hint=target_name,
            )
            if renamed:
                final_id = renamed["id"]
            elif self._is_path_file_id(rename_id):
                final_id = self._join_path(target_parent_id, target_name)
            else:
                final_id = rename_id

        if self._is_path_file_id(old_source_id) and old_source_id != final_id:
            self._remap_path_prefix(old_source_id, final_id)

        action.status = "done"
        action.resolved_id = final_id
        self.resolved[action.id] = final_id
        self.stats["ensured_dirs"] += 1
        self.log(f"[执行] 整体搬运目录「{source_label}」→「{target_name}」")

    def _is_metadata_file(self, name: str) -> bool:
        if not name or "." not in name:
            return False
        ext = name.rsplit(".", 1)[-1].lower()
        meta_exts = {"nfo", "ass", "ssa", "srt", "sub", "idx", "sup", "vtt", "jpg", "jpeg", "png", "webp", "bmp"}
        return ext in meta_exts

    async def _exec_ensure_dir(self, action: PlanAction):
        parent_id = self._resolve_ref(action.target_parent_id)
        if not parent_id:
            raise Exception(f"父目录未解析: {action.target_parent_id}")
        existing_id = await self._find_child_dir(parent_id, action.target_name)
        if existing_id:
            action.status = "done"
            action.resolved_id = existing_id
            self.resolved[action.id] = existing_id
            return
        try:
            r = await self.driver.create_folder(parent_id, action.target_name)
        except Exception as e:
            raise Exception(f"创建目录异常: {e}")
        if not r.success:
            existing_id = await self._find_child_dir(parent_id, action.target_name, force=True)
            if existing_id:
                action.status = "done"
                action.resolved_id = existing_id
                self.resolved[action.id] = existing_id
                return
            raise Exception(f"创建目录失败: {r.message}")
        folder_id = (r.data or {}).get("folder_id") if r.data else None
        if not folder_id:
            folder_id = await self._find_child_dir(parent_id, action.target_name, force=True)
        if not folder_id:
            raise Exception("创建目录后无法定位 ID")
        action.status = "done"
        action.resolved_id = folder_id
        self.resolved[action.id] = folder_id
        self.stats["ensured_dirs"] += 1
        self.log(f"[执行] 创建目录 {action.target_name} → {folder_id}")

    # ============================ Phase 5: delete_empty_dir ============================

    async def _exec_delete_empty_dir(self, action: PlanAction):
        dir_id = action.source_id or self._resolve_ref(action.target_parent_id)
        if not dir_id:
            action.status = "skipped"
            self.stats["skipped"] += 1
            return
        await self._invalidate_dir_cache(dir_id)
        try:
            items = await self.driver.list_files(dir_id) or []
        except Exception:
            action.status = "skipped"
            self.stats["skipped"] += 1
            return
        if items:
            action.status = "skipped"
            action.error = "目录非空"
            self.stats["skipped"] += 1
            return
        try:
            r = await self.driver.delete_file(dir_id)
        except Exception as e:
            if self._is_already_deleted_error(str(e)):
                action.status = "done"
                action.resolved_id = dir_id
                self.log(f"[执行] 空目录已不存在，视为已清理 {action.source_name or dir_id}")
                return
            raise Exception(f"删除目录异常: {e}")
        if not r.success:
            if self._is_already_deleted_error(r.message or ""):
                action.status = "done"
                action.resolved_id = dir_id
                self.log(f"[执行] 空目录已不存在，视为已清理 {action.source_name or dir_id}")
                return
            raise Exception(f"删除目录失败: {r.message}")
        action.status = "done"
        action.resolved_id = dir_id
        self.log(f"[执行] 删除空目录 {action.source_name or dir_id}")

    # ============================ Phase 4: 元数据跟随 ============================

    async def _apply_metadata_followers(self):
        followers = self.plan.diagnostics.get("meta_followers") or []
        if not followers:
            return
        action_index = {a.id: a for a in self.plan.actions}

        # 按源目录聚合，避免重复 list
        by_source_dir: Dict[str, List[dict]] = {}
        for entry in followers:
            depend_action = action_index.get(entry.get("depend_on"))
            if not depend_action or depend_action.status != "done":
                continue
            source_dir_id = str(entry.get("source_dir_id") or "")
            old_base = entry.get("old_base") or ""
            new_base = entry.get("new_base") or ""
            meta_exts = set(entry.get("meta_exts") or [])
            action_type = (entry.get("action_type") or "rename").lower()
            if not source_dir_id or not old_base or not new_base or not meta_exts:
                continue
            entry["_action_type"] = action_type
            entry["_target_parent_id"] = (
                getattr(depend_action, "_resolved_target_parent_id", None)
                or self._resolve_ref(depend_action.target_parent_id)
            )
            by_source_dir.setdefault(source_dir_id, []).append(entry)

        for source_dir_id, entries in by_source_dir.items():
            self.check_stop()
            try:
                items = await self.driver.list_files(source_dir_id) or []
            except Exception:
                continue

            claimed_meta_ids: Set[str] = set()
            # 按 (target_parent_id, action_type) 聚合，让 move 模式批量
            move_groups: Dict[str, List[Tuple[str, str, str]]] = {}
            rename_only_pairs: List[Tuple[str, str, str]] = []

            for entry in entries:
                action_type = entry.get("_action_type") or "rename"
                target_parent_id = entry.get("_target_parent_id") or ""
                old_base = entry.get("old_base") or ""
                match_bases = entry.get("match_bases") or ([old_base] if old_base else [])
                episode_token = entry.get("episode_token")
                new_base = entry["new_base"]
                meta_exts = set(entry["meta_exts"])
                matched_items = self._find_meta_files(
                    items,
                    match_bases,
                    meta_exts,
                    episode_token=episode_token,
                    claimed=claimed_meta_ids,
                )
                if not matched_items:
                    continue
                for item, matched_prefix in matched_items:
                    claimed_meta_ids.add(str(item.id))
                    new_name = self._compute_meta_new_name(item.name, matched_prefix, new_base)
                    if action_type == "move" and target_parent_id and str(target_parent_id) != str(source_dir_id):
                        move_groups.setdefault(target_parent_id, []).append(
                            (item.id, item.name, new_name)
                        )
                    else:
                        rename_only_pairs.append((item.id, item.name, new_name))

            # rename 模式：原目录改名
            for file_id, old_name, new_name in rename_only_pairs:
                if old_name == new_name:
                    continue
                await self._rename_meta_file(file_id, old_name, new_name, source_dir_id)

            # move 模式：批量 move 到目标父目录，再逐个改名
            for target_parent_id, triples in move_groups.items():
                self.check_stop()
                meta_ids = [tid for tid, _, _ in triples]
                try:
                    r = await self.driver.move_file(meta_ids, target_parent_id)
                    moved_ok = bool(r and r.success)
                except Exception as e:
                    moved_ok = False
                    self.log(f"[执行] 字幕/海报/nfo 批量搬运异常: {e}")
                await self._invalidate_dir_cache(source_dir_id)
                await self._invalidate_dir_cache(target_parent_id)
                if not moved_ok:
                    # 单文件回退
                    for file_id, old_name, _ in triples:
                        try:
                            rr = await self.driver.move_file([file_id], target_parent_id)
                            if not rr.success:
                                self.log(f"[执行] 元数据移动失败 {old_name}: {rr.message}")
                        except Exception as e:
                            self.log(f"[执行] 元数据移动异常 {old_name}: {e}")
                # 改名
                for file_id, old_name, new_name in triples:
                    if old_name == new_name:
                        continue
                    rename_id = file_id
                    if self._is_path_file_id(file_id):
                        rename_id = self._moved_path_id(file_id, target_parent_id)
                        found = await self._find_item_in_dir(
                            target_parent_id,
                            rename_id,
                            source_name_hint=old_name,
                        )
                        if found:
                            rename_id = found["id"]
                    await self._rename_meta_file(rename_id, old_name, new_name, target_parent_id)

    def _find_meta_files(
        self,
        items,
        match_bases: List[str],
        meta_exts: Set[str],
        *,
        episode_token: Optional[str] = None,
        claimed: Optional[Set[str]] = None,
    ) -> List[Tuple[Any, str]]:
        """从目录项中筛出跟随媒体的元数据文件，返回 (item, matched_prefix)。"""
        claimed = claimed or set()
        matched: List[Tuple[Any, str]] = []
        for item in items:
            if item.is_dir or str(item.id) in claimed:
                continue
            prefix = rules.match_meta_file_prefix(
                item.name,
                match_bases,
                meta_exts,
                episode_token=episode_token,
            )
            if prefix:
                matched.append((item, prefix))
        return matched

    @staticmethod
    def _compute_meta_new_name(old_name: str, matched_prefix: str, new_base: str) -> str:
        suffix = old_name[len(matched_prefix):]
        return new_base + suffix

    async def _rename_meta_file(self, file_id: str, old_name: str, new_name: str, parent_id: Optional[str] = None):
        rename_id = file_id
        if parent_id and self._is_path_file_id(file_id):
            current = await self._find_item_in_dir(parent_id, file_id, source_name_hint=old_name)
            if current:
                rename_id = current["id"]
        try:
            r = await self.driver.rename_file(rename_id, new_name)
            if r.success:
                self.stats["renamed_meta"] += 1
                self.log(f"[执行] 元数据: {old_name} → {new_name}")
            else:
                self.log(f"[执行] 元数据重命名失败: {old_name}: {r.message}")
        except Exception as e:
            self.log(f"[执行] 元数据重命名异常: {old_name}: {e}")

    # ============================ 工具 ============================

    def _notify_progress(self, current: int, total: int, action: PlanAction):
        if not self.progress_fn:
            return
        try:
            self.progress_fn(current, total, f"{action.kind} {action.target_name or action.source_name}")
        except Exception:
            pass

    def _topo_sort(self) -> List[PlanAction]:
        by_id = {a.id: a for a in self.plan.actions}
        ordered: List[PlanAction] = []
        visited: Set[str] = set()
        visiting: Set[str] = set()

        def visit(action_id: str):
            if action_id in visited:
                return
            if action_id in visiting:
                self.log(f"[执行] 发现依赖循环，忽略: {action_id}")
                return
            action = by_id.get(action_id)
            if not action:
                return
            visiting.add(action_id)
            for dep_id in action.depends_on or []:
                if dep_id in by_id:
                    visit(dep_id)
            visiting.discard(action_id)
            visited.add(action_id)
            ordered.append(action)

        for a in self.plan.actions:
            visit(a.id)
        return ordered

    def _resolve_ref(self, value: str) -> Optional[str]:
        if not value:
            return None
        if isinstance(value, str) and value.startswith("ref:"):
            target_id = value[4:]
            return self.resolved.get(target_id)
        return value

    async def _find_child_dir(self, parent_id: str, folder_name: str, force: bool = False) -> Optional[str]:
        items = await self._list_dir(parent_id, force=force)
        for item in items:
            if item.is_dir and item.name == folder_name:
                return item.id
        return None

    async def _find_dir_item_by_name(self, parent_id: str, name: str) -> Optional[str]:
        items = await self._list_dir(parent_id)
        for item in items:
            if item.name == name:
                return item.id
        return None

    async def _find_item_in_dir(
        self,
        parent_id: str,
        file_id: str,
        target_name_hint: Optional[str] = None,
        source_name_hint: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        items = await self._list_dir(parent_id)
        file_id_str = str(file_id)
        for item in items:
            if str(item.id) == file_id_str:
                return {"id": item.id, "name": item.name, "is_dir": item.is_dir}
        if self._is_path_file_id(file_id):
            name_hints: List[str] = []
            for hint in (target_name_hint, source_name_hint, self._path_basename(file_id)):
                if hint and hint not in name_hints:
                    name_hints.append(hint)
            for hint in name_hints:
                for item in items:
                    if item.name == hint:
                        return {"id": item.id, "name": item.name, "is_dir": item.is_dir}
        return None

    @staticmethod
    def _is_path_file_id(file_id: str) -> bool:
        return str(file_id or "").startswith("/")

    @staticmethod
    def _path_basename(path: str) -> str:
        normalized = str(path or "").rstrip("/")
        if not normalized:
            return ""
        return normalized.rsplit("/", 1)[-1]

    @staticmethod
    def _path_dirname(path: str) -> str:
        normalized = str(path or "").rstrip("/")
        if not normalized or normalized == "/":
            return "/"
        parent = normalized.rsplit("/", 1)[0]
        return parent or "/"

    @staticmethod
    def _is_already_deleted_error(message: str) -> bool:
        text = str(message or "").lower()
        return any(
            token in text
            for token in (
                "已经删除",
                "已删除",
                "不存在",
                "not found",
                "not exist",
                "no such file",
            )
        )

    @staticmethod
    def _join_path(parent: str, name: str) -> str:
        parent_norm = str(parent or "").rstrip("/") or "/"
        child_name = str(name or "").strip().strip("/")
        if not child_name:
            return parent_norm
        if parent_norm == "/":
            return f"/{child_name}"
        return f"{parent_norm}/{child_name}"

    def _moved_path_id(self, file_id: str, target_parent_id: str) -> str:
        return self._join_path(target_parent_id, self._path_basename(file_id))

    def _renamed_path_id(self, file_id: str, new_name: str) -> str:
        return self._join_path(self._path_dirname(file_id), new_name)

    def _apply_path_move_result(self, action: PlanAction, target_parent_id: str) -> None:
        if self._is_path_file_id(action.source_id):
            action.source_id = self._moved_path_id(action.source_id, target_parent_id)
        action.source_parent_id = str(target_parent_id)

    def _remap_path_prefix(self, old_prefix: str, new_prefix: str) -> None:
        old_prefix = str(old_prefix or "").rstrip("/")
        new_prefix = str(new_prefix or "").rstrip("/")
        if not old_prefix or old_prefix == new_prefix:
            return
        for action in self.plan.actions:
            target_parent_id = str(action.target_parent_id or "")
            if target_parent_id == old_prefix:
                action.target_parent_id = new_prefix
            elif target_parent_id.startswith(old_prefix + "/"):
                action.target_parent_id = new_prefix + target_parent_id[len(old_prefix):]
            if str(action.source_parent_id or "") == old_prefix:
                action.source_parent_id = new_prefix
            elif str(action.source_parent_id or "").startswith(old_prefix + "/"):
                action.source_parent_id = new_prefix + str(action.source_parent_id)[len(old_prefix):]
            source_id = str(action.source_id or "")
            if source_id == old_prefix:
                action.source_id = new_prefix
            elif source_id.startswith(old_prefix + "/"):
                action.source_id = new_prefix + source_id[len(old_prefix):]
        for entry in self.plan.diagnostics.get("meta_followers") or []:
            source_dir_id = str(entry.get("source_dir_id") or "")
            if source_dir_id == old_prefix:
                entry["source_dir_id"] = new_prefix

    async def _list_dir(self, dir_id: str, force: bool = False):
        if dir_id is None:
            return []
        key = str(dir_id)
        if force:
            await self._invalidate_dir_cache(key)
        if key in self.dir_listing_cache:
            return self.dir_listing_cache[key]
        items = await self.driver.list_files(key) or []
        self.dir_listing_cache[key] = items
        return items

    async def _invalidate_dir_cache(self, dir_id: str):
        self.dir_listing_cache.pop(str(dir_id), None)
        if self.cache_cleaner:
            try:
                await self.cache_cleaner._clear_directory_cache(self._account_id_str(), str(dir_id))
            except Exception:
                pass

    def _account_id_str(self) -> str:
        return self.plan.diagnostics.get("account_id") or ""

    @staticmethod
    def _now_str() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
