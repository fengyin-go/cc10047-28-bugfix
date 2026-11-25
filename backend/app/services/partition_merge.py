"""分区合并领域服务：合并提交前经归属守卫校验目标湖表写入权限与共享归属。"""
from __future__ import annotations

from typing import Any

from app.ownership import BOUNDARY_ABORT, files_for_entry, ownership_guard
from app.services.base import BaseModuleService
from app.store import store


class PartitionMergeService(BaseModuleService):
    MODULE = "partition_merge"
    ENTITY = "合并任务"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["任务标识", "目标湖表", "源分区", "目标分区", "合并策略", "文件大小", "执行状态", "耗时秒数"]
    REQUIRED_FIELDS = ["任务标识", "目标湖表", "源分区"]
    STATUS_ORDER = ["排队中", "合并中", "已完成", "合并失败"]
    ACTION_RULES = {"启动合并": "排队中", "取消合并": "合并中"}
    NEGATIVE_ACTIONS = []

    def _safe_int(self, val: Any, default: int = 0) -> int:
        try: return int(val)
        except (ValueError, TypeError): return default

    def domain_merge_partitions(
        self,
        entry_id: int,
        *,
        boundary: str = BOUNDARY_ABORT,
        priority: int = 0,
    ) -> dict[str, Any]:
        """处理流程：读取配置→归属校验→合并批次→写入存储。与压缩任务共享同一份
        目标表文件归属清单；冲突按整项中止或逐文件隔离处置，结果回写合并复核清单。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"合并任务 {entry_id} 不存在"}
        table_name = str(entry.get("目标湖表", ""))
        source = [p for p in str(entry.get("源分区", "0")).split(",") if p.strip()]
        _, files = files_for_entry(self.MODULE, entry)

        prepared = ownership_guard.prepare_plan(
            module=self.MODULE,
            entry_id=entry_id,
            table_ref=table_name,
            files=files,
            boundary=boundary,
            priority=priority,
        )
        if not prepared["ok"]:
            return {
                "ok": False,
                "task_id": entry.get("任务标识"),
                "table": table_name,
                "message": prepared["message"],
            }
        plan_id = prepared["plan"]["plan_id"]
        committed = ownership_guard.commit_plan(plan_id)

        result: dict[str, Any] = {
            "ok": committed["ok"],
            "task_id": entry.get("任务标识"),
            "table": table_name,
            "source_partitions": source,
            "target_partition": entry.get("目标分区", source[0] if source else ""),
            "merged_files": len(source) * 3,
            "output_size_mb": self._safe_int(entry.get("文件大小"), 128),
            "strategy": entry.get("合并策略", "bin-packing"),
            "plan_id": plan_id,
            "manifest_id": prepared["manifest"]["manifest_id"],
            "boundary": committed.get("boundary", boundary),
            "rewritten_files": committed.get("rewritten_files", []),
            "conflicts": committed.get("conflicts", []),
            "write_count": committed.get("write_count", 0),
        }
        if not committed["ok"]:
            result["message"] = committed.get("message", "归属校验未通过")
        return result
