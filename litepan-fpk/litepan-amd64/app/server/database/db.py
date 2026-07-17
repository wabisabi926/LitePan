"""基于 aiosqlite 的异步数据库访问层。"""

import os
import aiosqlite
import json
import time
from typing import List, Dict, Optional, Any
from pathlib import Path

try:
    from core.log_manager import get_writer, LogModule
    _log_available = True
except ImportError:
    _log_available = False


class AsyncDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None

    async def initialize(self):
        try:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            self._conn = await aiosqlite.connect(self.db_path, timeout=20.0)
            self._conn.row_factory = aiosqlite.Row

            await self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS cloud_accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    driver_type TEXT NOT NULL,
                    config TEXT NOT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    is_default BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                -- 配置表
                CREATE TABLE IF NOT EXISTS configs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key TEXT NOT NULL UNIQUE,
                    value TEXT NOT NULL,
                    description TEXT
                );
                
                -- 缓存保持配置表
                CREATE TABLE IF NOT EXISTS cache_retention_configs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id INTEGER NOT NULL,
                    parent_id TEXT NOT NULL,
                    path TEXT NOT NULL,
                    recursive BOOLEAN DEFAULT FALSE,
                    scan_depth INTEGER DEFAULT -1,
                    api_interval INTEGER DEFAULT 200,
                    refresh_interval INTEGER DEFAULT 30,
                    status TEXT DEFAULT 'running',
                    paused_reason TEXT,
                    file_count INTEGER DEFAULT 0,
                    last_refresh TIMESTAMP,
                    last_refresh_status TEXT,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (account_id) REFERENCES cloud_accounts(id),
                    UNIQUE(account_id, parent_id)
                );

                -- STRM同步任务配置表
                CREATE TABLE IF NOT EXISTS strm_sync_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    account_id INTEGER NOT NULL,
                    parent_id TEXT NOT NULL,
                    path TEXT NOT NULL,
                    recursive BOOLEAN DEFAULT TRUE,
                    scan_interval INTEGER DEFAULT 60,
                    scan_mode TEXT DEFAULT 'incremental_missing',
                    concurrency INTEGER DEFAULT 3,
                    extensions TEXT DEFAULT '',
                    exclude_dir_keywords TEXT DEFAULT '',
                    exclude_file_keywords TEXT DEFAULT '',
                    sync_metadata BOOLEAN DEFAULT FALSE,
                    api_interval INTEGER DEFAULT 200,
                    branch_check_enabled BOOLEAN DEFAULT FALSE,
                    schedule_mode TEXT DEFAULT 'window',
                    status TEXT DEFAULT 'running',
                    paused_reason TEXT,
                    file_count INTEGER DEFAULT 0,
                    last_created_count INTEGER DEFAULT 0,
                    last_updated_count INTEGER DEFAULT 0,
                    last_deleted_count INTEGER DEFAULT 0,
                    last_duration_ms INTEGER DEFAULT 0,
                    last_scan TIMESTAMP,
                    last_scan_status TEXT,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (account_id) REFERENCES cloud_accounts(id)
                );

                -- STRM分支检查配置表
                CREATE TABLE IF NOT EXISTS strm_sync_branches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL,
                    account_id INTEGER NOT NULL,
                    parent_id TEXT NOT NULL,
                    path TEXT NOT NULL,
                    relative_path TEXT DEFAULT '',
                    recursive BOOLEAN DEFAULT TRUE,
                    retention_days INTEGER DEFAULT 0,
                    expires_at TIMESTAMP,
                    branch_type TEXT DEFAULT 'temporary',
                    status TEXT DEFAULT 'running',
                    source TEXT DEFAULT 'manual',
                    last_scan TIMESTAMP,
                    last_scan_status TEXT,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (task_id) REFERENCES strm_sync_tasks(id),
                    UNIQUE(task_id, parent_id)
                );

                -- Emby反代配置表
                CREATE TABLE IF NOT EXISTS emby_proxy_configs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    emby_url TEXT NOT NULL,
                    api_key TEXT NOT NULL,
                    proxy_port INTEGER DEFAULT 5211,
                    status TEXT DEFAULT 'running',
                    last_error TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                -- new tables
                CREATE TABLE IF NOT EXISTS media_organize_tasks (
                    id              TEXT PRIMARY KEY,
                    task_name       TEXT NOT NULL,
                    account_id      TEXT NOT NULL,
                    config          TEXT NOT NULL,
                    status          TEXT DEFAULT 'idle',
                    last_run_at     TEXT,
                    last_run_result TEXT,
                    created_at      TEXT NOT NULL,
                    updated_at      TEXT NOT NULL
                );

            """)
            await self._conn.commit()

            for legacy_table in (
                "media_organize_results",
                "media_organize_notifications",
                "media_organize_settings",
            ):
                try:
                    await self._conn.execute(f"DROP TABLE IF EXISTS {legacy_table}")
                except Exception:
                    pass
            await self._conn.commit()

            for legacy_col in ("created_at", "updated_at"):
                try:
                    await self._conn.execute(
                        f"ALTER TABLE configs DROP COLUMN {legacy_col}"
                    )
                except Exception:
                    pass
            await self._conn.commit()

            # 时间窗口字段迁移（cache_retention_configs / strm_sync_tasks）
            for table_name in ("cache_retention_configs", "strm_sync_tasks"):
                for col_name, col_def in [
                    ("time_window_enabled", "BOOLEAN DEFAULT FALSE"),
                    ("time_start", "TEXT"),
                    ("time_end", "TEXT"),
                ]:
                    try:
                        await self._conn.execute(
                            f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_def}"
                        )
                    except Exception:
                        pass
            await self._conn.commit()

            # schedule_mode 字段迁移（strm_sync_tasks）：旧任务为 NULL，上层回退成 'window'，行为不变
            try:
                await self._conn.execute(
                    "ALTER TABLE strm_sync_tasks ADD COLUMN schedule_mode TEXT"
                )
            except Exception:
                pass
            await self._conn.commit()

            # scan_depth 字段迁移：不设默认值，旧任务为 NULL，由上层按 recursive 回退推断，行为不变
            try:
                await self._conn.execute(
                    "ALTER TABLE cache_retention_configs ADD COLUMN scan_depth INTEGER"
                )
            except Exception:
                pass
            await self._conn.commit()

            # api_interval 字段迁移（cache_retention_configs 原本就有，strm_sync_tasks 新增）
            try:
                await self._conn.execute(
                    "ALTER TABLE strm_sync_tasks ADD COLUMN api_interval INTEGER DEFAULT 200"
                )
            except Exception:
                pass
            await self._conn.commit()

            # STRM 分支检查字段迁移
            try:
                await self._conn.execute(
                    "ALTER TABLE strm_sync_tasks ADD COLUMN branch_check_enabled BOOLEAN DEFAULT FALSE"
                )
            except Exception:
                pass
            await self._conn.commit()

            try:
                await self._conn.execute(
                    "ALTER TABLE strm_sync_branches ADD COLUMN branch_type TEXT DEFAULT 'temporary'"
                )
            except Exception:
                pass
            await self._conn.commit()

            if _log_available and not os.environ.get('SKIP_LOG_INIT'):
                db_log = get_writer(LogModule.DATABASE)
                db_log.info(f"数据库初始化成功: {self.db_path}")
            else:
                print(f"✓ 数据库初始化成功: {self.db_path}")
        
        except Exception as e:
            if _log_available and not os.environ.get('SKIP_LOG_INIT'):
                db_log = get_writer(LogModule.DATABASE)
                db_log.error(f"数据库初始化失败: {e}")
            else:
                print(f"❌ 数据库初始化失败: {e}")
            raise

    async def close(self):
        if self._conn:
            await self._conn.close()

    async def list_accounts(self, include_inactive: bool = False) -> List[Dict]:
        if include_inactive:
            query = "SELECT * FROM cloud_accounts ORDER BY is_default DESC, id"
            params = ()
        else:
            query = "SELECT * FROM cloud_accounts WHERE is_active = 1 ORDER BY is_default DESC, id"
            params = ()

        async with self._conn.execute(query, params) as cursor:
            rows = await cursor.fetchall()

        accounts = []
        for row in rows:
            account = dict(row)
            config = json.loads(account.get('config', '{}'))
            account['config'] = config
            account['status'] = config.get('status', 'unknown')
            account['enabled'] = account.get('is_active', True)
            accounts.append(account)
        return accounts

    async def add_account(self, name: str, driver_type: str, config: Dict) -> int:
        if 'last_refresh_time' not in config:
            config['last_refresh_time'] = int(time.time())

        async with self._conn.cursor() as cursor:
            await cursor.execute("INSERT INTO cloud_accounts (name, driver_type, config) VALUES (?, ?, ?)",
                                 (name, driver_type, json.dumps(config)))
            await self._conn.commit()
            return cursor.lastrowid

    async def get_account(self, account_id: int) -> Optional[Dict]:
        async with self._conn.execute("SELECT * FROM cloud_accounts WHERE id = ?", (account_id,)) as cursor:
            row = await cursor.fetchone()
        if not row:
            return None
        account = dict(row)
        config = json.loads(account.get('config', '{}'))
        account['config'] = config
        account['status'] = config.get('status', 'unknown')
        account['enabled'] = account.get('is_active', True)
        return account

    async def delete_account(self, account_id: int) -> bool:
        async with self._conn.cursor() as cursor:
            await cursor.execute("SELECT is_default FROM cloud_accounts WHERE id = ?", (account_id,))
            result = await cursor.fetchone()
            is_default_account = result and result[0]

            await cursor.execute("DELETE FROM cloud_accounts WHERE id = ?", (account_id,))
            deleted = cursor.rowcount > 0
            await self._conn.commit()

            # 删的是默认账号时把默认挂到最早的账号上，避免没有默认账号
            if is_default_account and deleted:
                await cursor.execute("SELECT id FROM cloud_accounts ORDER BY id LIMIT 1")
                first_account = await cursor.fetchone()
                if first_account:
                    await cursor.execute("UPDATE cloud_accounts SET is_default = TRUE WHERE id = ?", (first_account['id'],))
                    await self._conn.commit()

            return deleted

    async def update_account(self, account_id: int, name: str = None, config: Dict = None, is_active: bool = None) -> bool:
        updates = []
        params = []
        if name is not None:
            updates.append("name = ?")
            params.append(name)
        if config is not None:
            # 与现有 config 合并，否则认证管理器写入的 token 等内部字段会被前端 payload 覆盖
            existing_account = await self.get_account(account_id)
            if existing_account:
                existing_config = existing_account['config']
                merged_config = {**existing_config, **config}
                updates.append("config = ?")
                params.append(json.dumps(merged_config))
            else:
                updates.append("config = ?")
                params.append(json.dumps(config))
        if is_active is not None:
            updates.append("is_active = ?")
            params.append(is_active)
        if not updates: 
            return False
        params.append(account_id)
        query = f"UPDATE cloud_accounts SET {', '.join(updates)} WHERE id = ?"
        async with self._conn.cursor() as cursor:
            await cursor.execute(query, tuple(params))
            await self._conn.commit()
            return cursor.rowcount > 0

    async def toggle_account_status(self, account_id: int) -> bool:
        account = await self.get_account(account_id)
        if not account:
            return False
        return await self.update_account(account_id, is_active=not account['is_active'])

    async def set_default_account(self, account_id: int) -> bool:
        async with self._conn.cursor() as cursor:
            await cursor.execute("UPDATE cloud_accounts SET is_default = FALSE")
            await cursor.execute("UPDATE cloud_accounts SET is_default = TRUE WHERE id = ?", (account_id,))
            await self._conn.commit()
            return cursor.rowcount > 0

    async def get_config(self, key: str, default: Any = None) -> Any:
        async with self._conn.execute("SELECT value FROM configs WHERE key = ?", (key,)) as cursor:
            row = await cursor.fetchone()
        if not row:
            return default
        try:
            return json.loads(row['value'])
        except:
            return row['value']

    async def set_config(self, key: str, value: Any, description: str = None) -> bool:
        value_str = json.dumps(value) if not isinstance(value, str) else value
        async with self._conn.cursor() as cursor:
            await cursor.execute(
                "INSERT OR REPLACE INTO configs (key, value, description) VALUES (?, ?, ?)",
                (key, value_str, description),
            )
            await self._conn.commit()
            return cursor.rowcount > 0

    async def list_configs(self) -> List[Dict]:
        async with self._conn.execute("SELECT * FROM configs ORDER BY key") as cursor:
            rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def get_cache_retention_configs(self) -> List[Dict]:
        async with self._conn.execute("""
            SELECT crc.*, COALESCE(ca.name, '未知账号') as account_name 
            FROM cache_retention_configs crc
            LEFT JOIN cloud_accounts ca ON crc.account_id = ca.id
            ORDER BY crc.created_at DESC
        """) as cursor:
            rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def get_cache_retention_config(self, config_id: int) -> Optional[Dict]:
        async with self._conn.execute("""
            SELECT crc.*, COALESCE(ca.name, '未知账号') as account_name 
            FROM cache_retention_configs crc
            LEFT JOIN cloud_accounts ca ON crc.account_id = ca.id
            WHERE crc.id = ?
        """, (config_id,)) as cursor:
            row = await cursor.fetchone()
        return dict(row) if row else None

    async def add_cache_retention_config(self, account_id: int, parent_id: str, path: str,
                                       recursive: bool = False, api_interval: int = 200,
                                       refresh_interval: int = 1800,
                                       time_window_enabled: bool = False,
                                       time_start: str = "00:00",
                                       time_end: str = "00:00",
                                       scan_depth: int = -1) -> int:
        async with self._conn.cursor() as cursor:
            await cursor.execute("""
                INSERT INTO cache_retention_configs
                (account_id, parent_id, path, recursive, scan_depth, api_interval, refresh_interval, status, time_window_enabled, time_start, time_end)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'running', ?, ?, ?)
            """, (account_id, parent_id, path, recursive, scan_depth, api_interval, refresh_interval,
                  1 if time_window_enabled else 0, time_start, time_end))
            await self._conn.commit()
            return cursor.lastrowid

    async def update_cache_retention_config(self, config_id: int, **kwargs) -> bool:
        valid_fields = ['account_id', 'parent_id', 'path', 'recursive', 'scan_depth', 'api_interval', 'refresh_interval', 'status',
                       'paused_reason', 'file_count', 'last_refresh', 'last_refresh_status', 'error_message',
                       'time_window_enabled', 'time_start', 'time_end']
        updates = []
        params = []
        
        for key, value in kwargs.items():
            if key in valid_fields:
                updates.append(f"{key} = ?")
                params.append(value)
        
        if not updates:
            return False
        
        params.append(config_id)
        query = f"UPDATE cache_retention_configs SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP WHERE id = ?"
        
        async with self._conn.cursor() as cursor:
            await cursor.execute(query, tuple(params))
            await self._conn.commit()
            return cursor.rowcount > 0

    async def delete_cache_retention_config(self, config_id: int) -> bool:
        async with self._conn.cursor() as cursor:
            await cursor.execute("DELETE FROM cache_retention_configs WHERE id = ?", (config_id,))
            await self._conn.commit()
            return cursor.rowcount > 0

    async def delete_cache_retention_configs_by_account(self, account_id: int) -> int:
        async with self._conn.cursor() as cursor:
            await cursor.execute("DELETE FROM cache_retention_configs WHERE account_id = ?", (account_id,))
            await self._conn.commit()
            return cursor.rowcount

    async def get_cache_retention_config_count(self) -> int:
        async with self._conn.execute("SELECT COUNT(*) as count FROM cache_retention_configs") as cursor:
            row = await cursor.fetchone()
        return row['count'] if row else 0

    async def toggle_cache_retention_status(self, config_id: int) -> bool:
        """用户手动切换运行/暂停；paused_reason='user' 用于区分自动暂停。"""
        config = await self.get_cache_retention_config(config_id)
        if not config:
            return False

        if config['status'] == 'running':
            return await self.update_cache_retention_config(
                config_id, status='paused', paused_reason='user'
            )
        return await self.update_cache_retention_config(
            config_id, status='running', paused_reason=None
        )

    async def get_strm_sync_tasks(self) -> List[Dict]:
        async with self._conn.execute(
            "SELECT * FROM strm_sync_tasks ORDER BY id DESC"
        ) as cursor:
            rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def get_strm_sync_task(self, task_id: int) -> Optional[Dict]:
        async with self._conn.execute(
            "SELECT * FROM strm_sync_tasks WHERE id = ?",
            (task_id,),
        ) as cursor:
            row = await cursor.fetchone()
        return dict(row) if row else None

    async def create_strm_sync_task(
        self,
        name: str,
        account_id: int,
        parent_id: str,
        path: str,
        recursive: bool,
        scan_interval: int,
        scan_mode: str,
        concurrency: int,
        extensions: str,
        exclude_dir_keywords: str,
        exclude_file_keywords: str,
        sync_metadata: bool = False,
        status: str = "running",
        time_window_enabled: bool = False,
        time_start: str = "00:00",
        time_end: str = "00:00",
        api_interval: int = 0,
        branch_check_enabled: bool = False,
        schedule_mode: str = "window",
    ) -> int:
        async with self._conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO strm_sync_tasks
                (name, account_id, parent_id, path, recursive, scan_interval, scan_mode, concurrency, extensions, exclude_dir_keywords, exclude_file_keywords, sync_metadata, api_interval, branch_check_enabled, schedule_mode, status, time_window_enabled, time_start, time_end, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    name,
                    account_id,
                    parent_id,
                    path,
                    1 if recursive else 0,
                    scan_interval,
                    scan_mode,
                    concurrency,
                    extensions,
                    exclude_dir_keywords,
                    exclude_file_keywords,
                    1 if sync_metadata else 0,
                    api_interval,
                    1 if branch_check_enabled else 0,
                    schedule_mode,
                    status,
                    1 if time_window_enabled else 0,
                    time_start,
                    time_end,
                ),
            )
            await self._conn.commit()
            return cursor.lastrowid

    async def update_strm_sync_task(self, task_id: int, **kwargs) -> bool:
        allowed = {
            "name",
            "account_id",
            "parent_id",
            "path",
            "recursive",
            "scan_interval",
            "scan_mode",
            "concurrency",
            "extensions",
            "exclude_dir_keywords",
            "exclude_file_keywords",
            "sync_metadata",
            "api_interval",
            "branch_check_enabled",
            "status",
            "paused_reason",
            "file_count",
            "last_created_count",
            "last_updated_count",
            "last_deleted_count",
            "last_duration_ms",
            "last_scan",
            "last_scan_status",
            "error_message",
            "time_window_enabled",
            "time_start",
            "time_end",
            "schedule_mode",
        }
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False

        fields = []
        values = []
        for key, value in updates.items():
            fields.append(f"{key} = ?")
            if key in {"recursive", "sync_metadata", "time_window_enabled", "branch_check_enabled"}:
                values.append(1 if bool(value) else 0)
            else:
                values.append(value)

        values.append(task_id)
        query = f"UPDATE strm_sync_tasks SET {', '.join(fields)}, updated_at = CURRENT_TIMESTAMP WHERE id = ?"
        async with self._conn.cursor() as cursor:
            await cursor.execute(query, tuple(values))
            await self._conn.commit()
            return cursor.rowcount > 0

    async def delete_strm_sync_task(self, task_id: int) -> bool:
        async with self._conn.cursor() as cursor:
            await cursor.execute("DELETE FROM strm_sync_branches WHERE task_id = ?", (task_id,))
            await cursor.execute("DELETE FROM strm_sync_tasks WHERE id = ?", (task_id,))
            await self._conn.commit()
            return cursor.rowcount > 0

    async def get_strm_sync_branches(self, task_id: int, only_active: bool = False) -> List[Dict]:
        if only_active:
            query = """
                SELECT * FROM strm_sync_branches
                WHERE task_id = ?
                  AND status = 'running'
                  AND (expires_at IS NULL OR expires_at = '' OR datetime(expires_at) > datetime('now', 'localtime'))
                ORDER BY path
            """
        else:
            query = "SELECT * FROM strm_sync_branches WHERE task_id = ? ORDER BY path"
        async with self._conn.execute(query, (task_id,)) as cursor:
            rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def get_strm_sync_branch(self, branch_id: int) -> Optional[Dict]:
        async with self._conn.execute(
            "SELECT * FROM strm_sync_branches WHERE id = ?",
            (branch_id,),
        ) as cursor:
            row = await cursor.fetchone()
        return dict(row) if row else None

    async def create_strm_sync_branch(
        self,
        task_id: int,
        account_id: int,
        parent_id: str,
        path: str,
        relative_path: str,
        recursive: bool = True,
        retention_days: int = 0,
        expires_at: Optional[str] = None,
        branch_type: str = "temporary",
        source: str = "manual",
        status: str = "running",
    ) -> int:
        async with self._conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO strm_sync_branches
                (task_id, account_id, parent_id, path, relative_path, recursive, retention_days, expires_at, branch_type, source, status, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    task_id,
                    account_id,
                    parent_id,
                    path,
                    relative_path,
                    1 if recursive else 0,
                    retention_days,
                    expires_at,
                    branch_type,
                    source,
                    status,
                ),
            )
            await self._conn.commit()
            return cursor.lastrowid

    async def update_strm_sync_branch(self, branch_id: int, **kwargs) -> bool:
        allowed = {
            "parent_id",
            "path",
            "relative_path",
            "recursive",
            "retention_days",
            "expires_at",
            "branch_type",
            "source",
            "status",
            "last_scan",
            "last_scan_status",
            "error_message",
        }
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False

        fields = []
        values = []
        for key, value in updates.items():
            fields.append(f"{key} = ?")
            if key == "recursive":
                values.append(1 if bool(value) else 0)
            else:
                values.append(value)
        values.append(branch_id)
        query = f"UPDATE strm_sync_branches SET {', '.join(fields)}, updated_at = CURRENT_TIMESTAMP WHERE id = ?"
        async with self._conn.cursor() as cursor:
            await cursor.execute(query, tuple(values))
            await self._conn.commit()
            return cursor.rowcount > 0

    async def delete_strm_sync_branch(self, branch_id: int) -> bool:
        async with self._conn.cursor() as cursor:
            await cursor.execute("DELETE FROM strm_sync_branches WHERE id = ?", (branch_id,))
            await self._conn.commit()
            return cursor.rowcount > 0

    async def delete_expired_strm_sync_branches(self, task_id: Optional[int] = None) -> int:
        if task_id is None:
            query = """
                DELETE FROM strm_sync_branches
                WHERE expires_at IS NOT NULL
                  AND expires_at != ''
                  AND datetime(expires_at) <= datetime('now', 'localtime')
            """
            params = ()
        else:
            query = """
                DELETE FROM strm_sync_branches
                WHERE task_id = ?
                  AND expires_at IS NOT NULL
                  AND expires_at != ''
                  AND datetime(expires_at) <= datetime('now', 'localtime')
            """
            params = (task_id,)
        async with self._conn.cursor() as cursor:
            await cursor.execute(query, params)
            await self._conn.commit()
            return cursor.rowcount

    async def delete_strm_sync_tasks_by_account(self, account_id: int) -> int:
        async with self._conn.cursor() as cursor:
            await cursor.execute("DELETE FROM strm_sync_tasks WHERE account_id = ?", (account_id,))
            await self._conn.commit()
            return cursor.rowcount

    async def toggle_strm_sync_task_status(self, task_id: int) -> bool:
        """用户手动切换运行/暂停；paused_reason='user' 用于区分自动暂停。"""
        task = await self.get_strm_sync_task(task_id)
        if not task:
            return False
        if task.get("status") == "running":
            return await self.update_strm_sync_task(
                task_id, status="paused", paused_reason="user"
            )
        return await self.update_strm_sync_task(
            task_id, status="running", paused_reason=None
        )

    async def get_emby_proxy_configs(self) -> List[Dict]:
        async with self._conn.execute(
            "SELECT * FROM emby_proxy_configs ORDER BY id DESC"
        ) as cursor:
            rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def get_emby_proxy_config(self, config_id: int) -> Optional[Dict]:
        async with self._conn.execute(
            "SELECT * FROM emby_proxy_configs WHERE id = ?",
            (config_id,),
        ) as cursor:
            row = await cursor.fetchone()
        return dict(row) if row else None

    async def create_emby_proxy_config(
        self,
        name: str,
        emby_url: str,
        api_key: str,
        proxy_port: int,
        status: str = "running",
    ) -> int:
        async with self._conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO emby_proxy_configs
                (name, emby_url, api_key, proxy_port, status, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (name, emby_url, api_key, int(proxy_port), status),
            )
            await self._conn.commit()
            return cursor.lastrowid

    async def update_emby_proxy_config(self, config_id: int, **kwargs) -> bool:
        allowed = {"name", "emby_url", "api_key", "proxy_port", "status", "last_error"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False

        fields = []
        values = []
        for key, value in updates.items():
            fields.append(f"{key} = ?")
            values.append(value)

        values.append(config_id)
        query = f"UPDATE emby_proxy_configs SET {', '.join(fields)}, updated_at = CURRENT_TIMESTAMP WHERE id = ?"
        async with self._conn.cursor() as cursor:
            await cursor.execute(query, tuple(values))
            await self._conn.commit()
            return cursor.rowcount > 0

    async def delete_emby_proxy_config(self, config_id: int) -> bool:
        async with self._conn.cursor() as cursor:
            await cursor.execute("DELETE FROM emby_proxy_configs WHERE id = ?", (config_id,))
            await self._conn.commit()
            return cursor.rowcount > 0

    # ========== 媒体整理任务 CRUD ==========

    async def get_media_organize_tasks(self) -> List[Dict]:
        async with self._conn.execute("SELECT * FROM media_organize_tasks ORDER BY created_at DESC") as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_media_organize_task(self, task_id: str) -> Optional[Dict]:
        async with self._conn.execute(
            "SELECT * FROM media_organize_tasks WHERE id = ?", (task_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def create_media_organize_task(
        self, task_name: str, account_id: str, config: dict,
        status: str = "idle"
    ) -> str:
        import uuid
        from datetime import datetime
        task_id = str(uuid.uuid4())
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        config_json = json.dumps(config, ensure_ascii=False)
        async with self._conn.cursor() as cursor:
            await cursor.execute(
                """INSERT INTO media_organize_tasks (id, task_name, account_id, config, status, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (task_id, task_name, str(account_id), config_json, status, now, now)
            )
            await self._conn.commit()
            return task_id

    async def update_media_organize_task(self, task_id: str, **kwargs) -> bool:
        allowed = {"task_name", "account_id", "config", "status",
                   "last_run_at", "last_run_result"}
        updates = {}
        for k, v in kwargs.items():
            if k in allowed:
                if k == "config" and isinstance(v, dict):
                    updates[k] = json.dumps(v, ensure_ascii=False)
                elif k == "last_run_result" and isinstance(v, dict):
                    updates[k] = json.dumps(v, ensure_ascii=False)
                else:
                    updates[k] = v
        if not updates:
            return False
        from datetime import datetime
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [now, task_id]
        async with self._conn.cursor() as cursor:
            await cursor.execute(
                f"UPDATE media_organize_tasks SET {set_clause}, updated_at = ? WHERE id = ?",
                values
            )
            await self._conn.commit()
            return cursor.rowcount > 0

    async def delete_media_organize_task(self, task_id: str) -> bool:
        async with self._conn.cursor() as cursor:
            await cursor.execute("DELETE FROM media_organize_tasks WHERE id = ?", (task_id,))
            await self._conn.commit()
            return cursor.rowcount > 0

    async def delete_media_organize_tasks_by_account(self, account_id: str) -> int:
        async with self._conn.cursor() as cursor:
            await cursor.execute(
                "DELETE FROM media_organize_tasks WHERE account_id = ?", (str(account_id),)
            )
            await self._conn.commit()
            return cursor.rowcount

DATABASE_DIR = Path("data")
DATABASE_FILE = DATABASE_DIR / "litepan.db"
DATABASE_PATH = str(DATABASE_FILE)

db = AsyncDatabase(DATABASE_PATH)


async def init_database():
    await db.initialize()


async def get_db():
    return db._conn


def get_db_session():
    raise NotImplementedError("请使用异步数据库接口")


__all__ = ['db', 'init_database', 'get_db', 'AsyncDatabase']
