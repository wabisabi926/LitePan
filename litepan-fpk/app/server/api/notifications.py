from fastapi import APIRouter, Depends, HTTPException
from api.deps import require_admin_auth
from core.notification_manager import notification_manager

router = APIRouter(
    prefix="/notifications",
    tags=["通知"],
    dependencies=[Depends(require_admin_auth)],
)


@router.get("")
async def list_notifications():
    return {"success": True, "data": await notification_manager.get_all()}


@router.get("/unread-count")
async def unread_count():
    return {"success": True, "data": await notification_manager.get_unread_count()}


@router.post("/{notification_id}/read")
async def mark_read(notification_id: str):
    ok = await notification_manager.mark_read(notification_id)
    if not ok:
        raise HTTPException(status_code=404, detail="通知不存在")
    return {"success": True, "message": "已标记已读"}


@router.post("/read-all")
async def mark_all_read():
    count = await notification_manager.mark_all_read()
    return {"success": True, "message": f"已标记 {count} 条为已读"}


@router.delete("/{notification_id}")
async def delete_notification(notification_id: str):
    ok = await notification_manager.delete(notification_id)
    if not ok:
        raise HTTPException(status_code=404, detail="通知不存在")
    return {"success": True, "message": "已删除"}
