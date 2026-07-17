"""内存通知系统：角标、列表、去重，不落库。"""

import uuid
import time
import asyncio
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class Notification:
    id: str
    type: str          # "auth_expired" | "task_error" | "system"
    level: str         # "error" | "warning" | "info"
    title: str
    message: str
    account_id: Optional[int] = None
    action_label: str = ""
    action_route: str = ""
    created_at: float = field(default_factory=time.time)
    read: bool = False

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "type": self.type,
            "level": self.level,
            "title": self.title,
            "message": self.message,
            "account_id": self.account_id,
            "action_label": self.action_label,
            "action_route": self.action_route,
            "created_at": self.created_at,
            "read": self.read,
        }


class NotificationManager:
    def __init__(self):
        self._notifications: List[Notification] = []
        self._lock = asyncio.Lock()
        self._max_items = 100

    async def notify(
        self,
        type: str,
        level: str,
        title: str,
        message: str,
        account_id: Optional[int] = None,
        action_label: str = "",
        action_route: str = "",
        dedup_key: Optional[str] = None,
    ) -> Optional[Notification]:
        async with self._lock:
            # 去重：同 dedup_key 且未读的已存在，则跳过
            if dedup_key:
                for n in self._notifications:
                    if getattr(n, "dedup_key", None) == dedup_key and not n.read:
                        return None

            notif = Notification(
                id=uuid.uuid4().hex[:12],
                type=type,
                level=level,
                title=title,
                message=message,
                account_id=account_id,
                action_label=action_label,
                action_route=action_route,
            )
            setattr(notif, "dedup_key", dedup_key)

            self._notifications.insert(0, notif)
            if len(self._notifications) > self._max_items:
                self._notifications = self._notifications[:self._max_items]

            return notif

    async def get_all(self) -> List[dict]:
        async with self._lock:
            return [n.to_dict() for n in self._notifications]

    async def get_unread_count(self) -> int:
        async with self._lock:
            return sum(1 for n in self._notifications if not n.read)

    async def mark_read(self, notification_id: str) -> bool:
        async with self._lock:
            for n in self._notifications:
                if n.id == notification_id:
                    n.read = True
                    return True
            return False

    async def mark_all_read(self) -> int:
        async with self._lock:
            count = sum(1 for n in self._notifications if not n.read)
            for n in self._notifications:
                n.read = True
            return count

    async def delete(self, notification_id: str) -> bool:
        async with self._lock:
            for i, n in enumerate(self._notifications):
                if n.id == notification_id:
                    self._notifications.pop(i)
                    return True
            return False

    async def clear_read(self) -> int:
        async with self._lock:
            before = len(self._notifications)
            self._notifications = [n for n in self._notifications if not n.read]
            return before - len(self._notifications)


notification_manager = NotificationManager()
