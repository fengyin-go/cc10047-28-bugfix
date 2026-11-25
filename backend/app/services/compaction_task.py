"""压缩任务领域服务：压缩提交前经归属守卫校验目标湖表写入权限与共享归属。"""
from __future__ import annotations

from typing import Any

from app.ownership import BOUNDARY_ABORT, files_for_entry, ownership_guard
from app.services.base import BaseModuleService
from app.store import store


class CompactionTaskService(BaseModuleService):
    MODULE = "compaction_task"
    ENTITY = "压缩任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["任务标识", "目标湖表", "压缩算法", "目标大小", "并行度", "执行状态", "压缩比率", "完成时间"]
    REQUIRED_FIELDS = ["任务标识", "目标湖表", "压缩算法"]
    STATUS_ORDER = ["排队中", "压缩中", "已完成", "压缩失败"]
    ACTION_RULES = {"启动压缩": "排队中", "取消压缩": "压缩中"}
    NEGATIVE_ACTIONS = []

    def _safe_int(self, val: Any, default: int = 0) -> int:
        try: return int(val)
        except (ValueError, TypeError): return default

    def _safe_float(self, val: Any, default: float = 0.0) -> float:
        try: return float(val)
        except (ValueError, TypeError): return default

    def domain_plan_compaction(
        self,
        entry_id: int,
        *,
        boundary: str = BOUNDARY_ABORT,
        priority: int = 0,
    ) -> dict[str, Any]:
        """处理流程：读取配置→归属校验→规划批次。与分区合并读取同一份目标表文件
        归属清单；本步只准备计划，实际写入由压缩执行提交。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"压缩任务 {entry_id} 不存在"}
        table_name = str(entry.get("目标湖表", ""))
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
                "message": prepared["message"],
            }
        from app.compute import compaction_planner

        return {
            **compaction_planner(
                table_path=table_name,
                file_sizes=[64, 32, 16, 256, 512, 8, 128],
                target_file_size=134217728,
                max_parallelism=self._safe_int(entry.get("并行度"), 4),
            ),
            "ok": True,
            "task_id": entry.get("任务标识"),
            "plan_id": prepared["plan"]["plan_id"],
            "manifest_id": prepared["manifest"]["manifest_id"],
            "boundary": boundary,
        }

    def domain_compact_table(
        self,
        entry_id: int,
        *,
        boundary: str = BOUNDARY_ABORT,
        priority: int = 0,
    ) -> dict[str, Any]:
        """处理流程：读取配置→归属校验→提交写入。提交本入口最新待执行计划，
        同一计划只生效一次；逐文件隔离时只处理仍能证明属于原表的文件。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"压缩任务 {entry_id} 不存在"}
        table_name = str(entry.get("目标湖表", ""))

        plan = ownership_guard.latest_pending_plan(self.MODULE, entry_id)
        if plan is None:
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
                    "message": prepared["message"],
                }
            plan_id = prepared["plan"]["plan_id"]
        else:
            plan_id = plan["plan_id"]
        committed = ownership_guard.commit_plan(plan_id)

        result: dict[str, Any] = {
            "ok": committed["ok"],
            "task_id": entry.get("任务标识"),
            "table": table_name,
            "algorithm": entry.get("压缩算法", "zstd"),
            "before_size_mb": 512,
            "after_size_mb": 128,
            "compression_ratio": self._safe_float(entry.get("压缩比率"), 4.0),
            "files_compacted": len(committed.get("rewritten_files", [])),
            "duration_seconds": 45,
            "plan_id": plan_id,
            "boundary": committed.get("boundary", boundary),
            "rewritten_files": committed.get("rewritten_files", []),
            "conflicts": committed.get("conflicts", []),
            "write_count": committed.get("write_count", 0),
        }
        if not committed["ok"]:
            result["message"] = committed.get("message", "归属校验未通过")
        return result
