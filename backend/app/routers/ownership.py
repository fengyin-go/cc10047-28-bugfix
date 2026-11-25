"""归属治理接口：目标表文件归属清单、合并复核清单、计划提交与目录事件。

分区合并与压缩任务各入口读取同一份目标表文件归属清单；这里提供计划级的
准备/提交分离能力，便于在归属变更后核对整项中止或逐文件隔离两种处置边界。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.ownership import BOUNDARY_ABORT, files_for_entry, ownership_guard
from app.services import service_for

router = APIRouter(prefix="/api/ownership", tags=["归属治理"])


class PlanPayload(BaseModel):
    module: str | None = None
    entry_id: int | None = None
    table_ref: str | None = None
    files: list[str] | None = None
    boundary: str = BOUNDARY_ABORT
    priority: int = 0


class RegisterPayload(BaseModel):
    physical_id: str
    name: str | None = None
    priority: int = 0
    writable: bool = True


class RenamePayload(BaseModel):
    physical_id: str
    new_name: str


class ReassignPayload(BaseModel):
    paths: list[str] = Field(default_factory=list)
    to_table: str


@router.get("/manifests/{table_ref}")
def get_manifest(table_ref: str) -> dict[str, Any]:
    """读取目标表文件归属清单（分区合并与压缩任务共享的同一份）。"""
    physical_id = ownership_guard.resolve_table(table_ref)
    if physical_id is None:
        raise HTTPException(status_code=404, detail=f"湖表「{table_ref}」未在湖表目录登记")
    manifest = ownership_guard.get_manifest(physical_id)
    if manifest is None:
        raise HTTPException(status_code=404, detail=f"湖表「{table_ref}」尚未生成归属清单")
    manifest["files"] = sorted(manifest["files"].values(), key=lambda f: f["path"])
    return {"ok": True, "manifest": manifest}


@router.get("/tables/{table_ref}/detail")
def get_table_detail(table_ref: str) -> dict[str, Any]:
    """按物理表输出重写文件清单、冲突待办与写入次数。"""
    detail = ownership_guard.table_detail(table_ref)
    if not detail["ok"]:
        raise HTTPException(status_code=404, detail=detail["message"])
    return detail


@router.get("/review-list")
def get_review_list(only_todos: bool = False) -> dict[str, Any]:
    """合并复核清单：冲突结果回写于此，待复核项即冲突待办。"""
    items = ownership_guard.review_items(only_todos=only_todos)
    return {
        "ok": True,
        "items": items,
        "total": len(items),
        "todos": sum(1 for item in items if item["status"] == "待复核"),
    }


@router.post("/plans")
def prepare_plan(payload: PlanPayload) -> dict[str, Any]:
    """准备计划：校验湖表写入权限和共享归属，生成目标表文件归属清单。"""
    if payload.module and payload.entry_id is not None:
        service = service_for(payload.module)
        if service is None:
            raise HTTPException(status_code=404, detail=f"未知业务模块：{payload.module}")
        entry = service.get_entry(payload.entry_id)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"{service.ENTITY} {payload.entry_id} 不存在或已归档")
        table_ref, files = files_for_entry(payload.module, entry)
    elif payload.table_ref and payload.files is not None:
        table_ref, files = payload.table_ref, list(payload.files)
    else:
        raise HTTPException(status_code=400, detail="需提供 module+entry_id 或 table_ref+files")
    prepared = ownership_guard.prepare_plan(
        module=payload.module or "manual",
        entry_id=payload.entry_id,
        table_ref=table_ref,
        files=files,
        boundary=payload.boundary,
        priority=payload.priority,
    )
    if not prepared["ok"]:
        return {"ok": False, "message": prepared["message"]}
    return {
        "ok": True,
        "plan_id": prepared["plan"]["plan_id"],
        "manifest_id": prepared["manifest"]["manifest_id"],
        "plan": prepared["plan"],
    }


@router.post("/plans/{plan_id}/commit")
def commit_plan(plan_id: str) -> dict[str, Any]:
    """提交计划：同一计划只生效一次，并发提交也只执行一次实际写入。"""
    if plan_id not in ownership_guard.plans:
        raise HTTPException(status_code=404, detail=f"计划 {plan_id} 不存在")
    return ownership_guard.commit_plan(plan_id)


@router.post("/plans/{plan_id}/interrupt")
def interrupt_plan(plan_id: str) -> dict[str, Any]:
    if plan_id not in ownership_guard.plans:
        raise HTTPException(status_code=404, detail=f"计划 {plan_id} 不存在")
    return ownership_guard.interrupt_plan(plan_id)


@router.post("/plans/{plan_id}/recover")
def recover_plan(plan_id: str) -> dict[str, Any]:
    """中断恢复：保留原表身份，不把旧别名重新解释成新湖表。"""
    if plan_id not in ownership_guard.plans:
        raise HTTPException(status_code=404, detail=f"计划 {plan_id} 不存在")
    return ownership_guard.recover_plan(plan_id)


@router.post("/catalog/register")
def register_table(payload: RegisterPayload) -> dict[str, Any]:
    table = ownership_guard.register_lake_table(
        payload.physical_id,
        payload.name,
        priority=payload.priority,
        writable=payload.writable,
    )
    return {"ok": True, "table": table}


@router.post("/catalog/rename")
def rename_table(payload: RenamePayload) -> dict[str, Any]:
    """改名：释放旧名；物理身份与文件归属保持不变。"""
    renamed = ownership_guard.rename_table(payload.physical_id, payload.new_name)
    if renamed is None:
        raise HTTPException(status_code=404, detail=f"湖表「{payload.physical_id}」未在湖表目录登记")
    return {"ok": True, **renamed}


@router.post("/catalog/reassign-files")
def reassign_files(payload: ReassignPayload) -> dict[str, Any]:
    """目录层归属变更：把文件指向另一个物理表。"""
    moved, error = ownership_guard.reassign_files(payload.paths, payload.to_table)
    if error:
        raise HTTPException(status_code=404, detail=error)
    return {"ok": True, "moved": moved}
