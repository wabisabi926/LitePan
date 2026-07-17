"""驱动注册中心：自动发现 + 实例池管理 + 能力查询。"""

import importlib
import hashlib
import time
from typing import Dict, List, Optional
from pathlib import Path

from .base import get_driver_capabilities, normalize_driver_capabilities
from .log_manager import get_writer, LogModule


class DriverRegistry:
    def __init__(self):
        self._drivers: Dict[str, Dict] = {}
        # 驱动实例池：{account_id: {driver, config_hash, last_used, ...}}
        self._driver_instances: Dict[str, Dict] = {}
        self._debug_mode = False

    def auto_discover_drivers(self, drivers_package: str = "drivers"):
        for item in Path(drivers_package).iterdir():
            if item.is_dir() and not item.name.startswith('_'):
                self._load_driver(f"{drivers_package}.{item.name}")

    def _load_driver(self, package_name: str):
        driver_log = get_writer(LogModule.DRIVER_SYSTEM)
        try:
            module = importlib.import_module(package_name)
            if not hasattr(module, 'DRIVER_INFO'):
                return

            info = dict(module.DRIVER_INFO)
            info["capabilities"] = normalize_driver_capabilities(info.get("capabilities", []))
            self._drivers[info["name"]] = {
                "driver_class": info["driver_class"],
                "config_class": info["config_class"],
                "info": info
            }
        except Exception as e:
            driver_log.error(f"加载驱动失败 {package_name}: {e}")

    def get_driver_names(self) -> List[str]:
        return list(self._drivers.keys())

    def get_driver_info(self, name: str) -> Optional[Dict]:
        return self._drivers.get(name, {}).get("info")

    def get_all_driver_info(self) -> Dict[str, Dict]:
        drivers_info = {name: data["info"] for name, data in self._drivers.items()}
        sorted_drivers = sorted(
            drivers_info.items(),
            key=lambda x: x[1].get("sort_order", 999)
        )
        return dict(sorted_drivers)

    def _get_config_hash(self, config: Dict) -> str:
        """计算可复用实例的配置指纹：只考虑稳定字段，避免 token/cookie 刷新触发无谓的实例重建。"""
        excluded_fields = {
            'last_refresh_time',
            'access_token',
            'refresh_token',
            'cookie',
            'expires_at',
            'token_expires_at',
            'last_auth_time',
            'auth_timestamp',
            'auth_status',
            'refresh_attempts',
            'status',
            'error_message',
            'last_tested',
        }

        stable_config = {k: v for k, v in config.items() if k not in excluded_fields}

        config_str = str(sorted(stable_config.items()))
        return hashlib.md5(config_str.encode()).hexdigest()

    async def get_driver_instance(self, account_id: str, driver_name: str, config: Dict):
        if driver_name not in self._drivers:
            raise ValueError(f"未注册的驱动: {driver_name}")
        driver_log = get_writer(LogModule.DRIVER_SYSTEM)

        config_hash = self._get_config_hash(config)
        current_time = time.time()

        if account_id in self._driver_instances:
            instance_data = self._driver_instances[account_id]

            if (
                instance_data.get("driver_name") == driver_name
                and instance_data["config_hash"] == config_hash
            ):
                instance_data["last_used"] = current_time

                # 稳定字段没变也要同步 token/cookie 这些动态字段，不然驱动里残留的是旧凭据
                driver = instance_data["driver"]
                if hasattr(driver, 'config'):
                    dynamic_fields = ['cookie', 'access_token', 'refresh_token', 'last_refresh_time']
                    for field in dynamic_fields:
                        if field in config and hasattr(driver.config, field):
                            setattr(driver.config, field, config[field])
                        if field in config and hasattr(driver, field):
                            setattr(driver, field, config[field])

                if hasattr(driver, 'sync_runtime_auth_state'):
                    await driver.sync_runtime_auth_state(config)

                return driver

            await self._close_driver_instance(account_id)

        driver_data = self._drivers[driver_name]
        config_obj = driver_data["config_class"](**config)
        driver = driver_data["driver_class"](config_obj)

        driver.account_id = account_id
        # 兼容旧代码路径里 `_account_id` 的读法
        driver._account_id = account_id

        if hasattr(driver, 'set_cache_manager'):
            from core.dependency_container import get_cache_manager
            cache_manager = get_cache_manager()
            driver.set_cache_manager(cache_manager)

        await driver.init()

        # cookie 驱动在会话中途刷新到了新 cookie，通过这个回调把新 cookie 落库并把账号状态收回 active
        if hasattr(driver, 'set_cookie_update_callback'):
            async def cookie_update_callback(new_cookie: str):
                try:
                    import time
                    from database.db import db

                    account = await db.get_account(int(account_id))
                    if account:
                        current_config = account['config']
                        current_config['cookie'] = new_cookie
                        current_config['last_refresh_time'] = int(time.time())
                        current_config['auth_status'] = 'active'
                        current_config['refresh_attempts'] = 0
                        current_config['error_message'] = None
                        await db.update_account(int(account_id), config=current_config)
                except Exception as e:
                    driver_log.warning(f"Cookie持久化失败: {e}")

            driver.set_cookie_update_callback(cookie_update_callback)

        # 实例启动后再做一次实际能力探测，覆盖掉 manifest 里的声明能力
        actual_capabilities = get_driver_capabilities(driver)
        if actual_capabilities:
            self._drivers[driver_name]["info"]["capabilities"] = actual_capabilities

        self._driver_instances[account_id] = {
            "driver": driver,
            "driver_name": driver_name,
            "config_hash": config_hash,
            "last_used": current_time,
            "created_at": current_time
        }

        return driver

    async def _close_driver_instance(self, account_id: str):
        if account_id in self._driver_instances:
            instance_data = self._driver_instances[account_id]
            driver_log = get_writer(LogModule.DRIVER_SYSTEM)
            try:
                await instance_data["driver"].close()
            except Exception as e:
                driver_log.warning(f"关闭驱动实例失败: {e}")
            finally:
                del self._driver_instances[account_id]

    async def cleanup_idle_instances(self, max_idle_time: int = 1800):
        """回收空闲实例；注意已注册到 auth_scheduler 的驱动不能被回收，否则刷新会断链。"""
        current_time = time.time()
        idle_accounts = []
        try:
            from core.auth_manager import auth_scheduler
            protected_accounts = {str(account_id) for account_id in auth_scheduler.auth_managers.keys()}
        except Exception:
            protected_accounts = set()

        for account_id, instance_data in self._driver_instances.items():
            driver = instance_data["driver"]
            if account_id in protected_accounts or getattr(driver, "_auth_manager", None) is not None:
                continue
            if current_time - instance_data["last_used"] > max_idle_time:
                idle_accounts.append(account_id)

        for account_id in idle_accounts:
            await self._close_driver_instance(account_id)

    async def close_all_instances(self):
        account_ids = list(self._driver_instances.keys())
        for account_id in account_ids:
            await self._close_driver_instance(account_id)

    def get_config_schema(self, driver_name: str) -> Optional[Dict]:
        if driver_name not in self._drivers:
            return None
        return self._drivers[driver_name]["config_class"].get_form_schema()

    def validate_driver_config(self, driver_name: str, config: Dict) -> tuple[bool, List[str]]:
        if driver_name not in self._drivers:
            return False, [f"未知驱动: {driver_name}"]

        try:
            self._drivers[driver_name]["config_class"](**config)
            return True, []
        except TypeError as e:
            # 表单里传了 dataclass 不认识的字段，把具体字段名抠出来给前端
            error_msg = str(e)
            if "unexpected keyword argument" in error_msg:
                import re
                match = re.search(r"unexpected keyword argument '([^']+)'", error_msg)
                if match:
                    field_name = match.group(1)
                    return False, [f"不支持的配置字段: {field_name}"]
            return False, [f"配置格式错误: {error_msg}"]
        except ValueError as e:
            return False, [str(e)]
        except Exception as e:
            return False, [f"配置验证失败: {str(e)}"]


driver_registry = DriverRegistry()

get_all_driver_info = driver_registry.get_all_driver_info
get_driver_names = driver_registry.get_driver_names
get_driver_info = driver_registry.get_driver_info
get_config_schema = driver_registry.get_config_schema
validate_driver_config = driver_registry.validate_driver_config


def init_drivers(cache_manager=None):
    driver_log = get_writer(LogModule.DRIVER_SYSTEM)
    driver_registry.auto_discover_drivers()

    if cache_manager:
        for driver_name, driver_data in driver_registry._drivers.items():
            driver_class = driver_data["driver_class"]
            if hasattr(driver_class, 'set_cache_manager'):
                driver_class._cache_manager = cache_manager

    driver_log.debug(f"驱动系统初始化完成，已加载 {len(driver_registry.get_driver_names())} 个驱动")

