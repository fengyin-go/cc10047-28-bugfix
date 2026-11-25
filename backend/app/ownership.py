"""湖表归属守卫：表目录、目标表文件归属清单、合并复核清单与幂等提交。

共同根因修复：分区合并与压缩任务在提交前共享同一份目标表文件归属清单，
校验湖表写入权限与共享归属，拒绝越权；同名文件和分区按物理表区分；
同一计划只生效一次，并发提交也只执行一次实际写入；中断恢复保留原表身份，
不把旧别名重新解释成新湖表。
"""
from __future__ import annotations

import copy
import threading
from typing import Any

from app.store import store

# 两种处置边界：整项中止 / 逐文件隔离
BOUNDARY_ABORT = "abort"      # 整项中止：任一归属冲突 → 本计划没有重写
BOUNDARY_ISOLATE = "isolate"  # 逐文件隔离：只实际处理仍能证明属于原表的文件
BOUNDARIES = (BOUNDARY_ABORT, BOUNDARY_ISOLATE)

REVIEW_PENDING = "待复核"
REVIEW_RESOLVED = "已处理"


# ------------------------------------------------------------------
# 文件推导：合并/压缩各入口共用同一份推导，保证清单口径一致
# ------------------------------------------------------------------

def partition_of(path: str) -> str:
    parts = str(path).split("/")
    return parts[-2] if len(parts) >= 2 else ""


def merge_files_for(table_name: str, partitions: list[str]) -> list[str]:
    """分区合并的候选文件：每个源分区 3 个文件，路径以物理表名限定。"""
    files: list[str] = []
    for part in partitions:
        name = str(part).strip()
        if not name:
            continue
        for i in range(3):
            files.append(f"{table_name}/{name}/part-{i}.parquet")
    return files


def compaction_files_for(table_name: str, count: int = 7) -> list[str]:
    """压缩任务的候选文件：与压缩规划的文件数保持一致。"""
    return [f"{table_name}/data/part-{i}.parquet" for i in range(count)]


def files_for_entry(module: str, entry: dict[str, Any]) -> tuple[str, list[str]]:
    """按模块入口推导目标表引用与候选文件列表。"""
    table_ref = str(entry.get("目标湖表", "") or "")
    if module == "partition_merge":
        partitions = [p for p in str(entry.get("源分区", "0")).split(",") if p.strip()]
        return table_ref, merge_files_for(table_ref, partitions)
    if module == "compaction_task":
        return table_ref, compaction_files_for(table_ref)
    return table_ref, []


# ------------------------------------------------------------------
# 表目录：表名 → 物理表身份；文件路径 → 所属物理表
# ------------------------------------------------------------------

class TableDirectory:
    """目录只关心物理身份：改名只动名字，不动物理身份和文件归属。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.tables: dict[str, dict[str, Any]] = {}   # physical_id -> 表记录
        self.name_index: dict[str, str] = {}          # 表名 -> physical_id
        self.files: dict[str, dict[str, Any]] = {}    # 路径 -> 文件记录

    def register_table(
        self,
        physical_id: str,
        name: str | None = None,
        *,
        priority: int = 0,
        writable: bool = True,
    ) -> dict[str, Any]:
        """登记物理表；已存在则原样返回。名字被占用时不抢占。"""
        with self._lock:
            existing = self.tables.get(physical_id)
            if existing is not None:
                return existing
            table = {
                "physical_id": physical_id,
                "name": name,
                "priority": int(priority),
                "writable": bool(writable),
            }
            self.tables[physical_id] = table
            if name and name not in self.name_index:
                self.name_index[name] = physical_id
            return table

    def resolve(self, ref: str | None) -> str | None:
        """按物理标识优先、表名其次解析；同名时物理标识胜出。"""
        key = str(ref or "").strip()
        if not key:
            return None
        with self._lock:
            if key in self.tables:
                return key
            return self.name_index.get(key)

    def rename_table(self, physical_id: str, new_name: str) -> dict[str, Any] | None:
        """改名：释放旧名，占用新名；物理身份与文件归属保持不变。"""
        with self._lock:
            table = self.tables.get(physical_id)
            if table is None:
                return None
            old_name = table.get("name")
            if old_name and self.name_index.get(old_name) == physical_id:
                del self.name_index[old_name]
            self.name_index[new_name] = physical_id
            table["name"] = new_name
            return {"physical_id": physical_id, "old_name": old_name, "new_name": new_name}

    def claim_file(self, physical_id: str, path: str) -> str:
        """未归属的文件登记给该表；返回该路径当前的所属物理表。"""
        with self._lock:
            record = self.files.get(path)
            if record is None:
                record = {"path": path, "partition": partition_of(path), "owner": physical_id}
                self.files[path] = record
            return record["owner"]

    def owner_of(self, path: str) -> str | None:
        with self._lock:
            record = self.files.get(path)
            return record["owner"] if record else None

    def set_owner(self, path: str, physical_id: str) -> None:
        with self._lock:
            record = self.files.get(path)
            if record is None:
                record = {"path": path, "partition": partition_of(path), "owner": physical_id}
                self.files[path] = record
            else:
                record["owner"] = physical_id

    def reassign_files(self, paths: list[str], to_physical_id: str) -> list[str]:
        """目录层归属变更：把文件指向另一个物理表（如表名被重新分配后）。"""
        moved: list[str] = []
        with self._lock:
            for path in paths:
                self.set_owner(path, to_physical_id)
                moved.append(path)
        return moved

    def table_priority(self, physical_id: str) -> int:
        with self._lock:
            table = self.tables.get(physical_id)
            return int(table.get("priority", 0)) if table else 0


# ------------------------------------------------------------------
# 归属守卫：分区合并与压缩任务各入口读取同一份目标表文件归属清单
# ------------------------------------------------------------------

class OwnershipGuard:
    def __init__(self, directory: TableDirectory | None = None) -> None:
        self.directory = directory or TableDirectory()
        self._lock = threading.RLock()
        self._seq = 0
        self.manifests: dict[str, dict[str, Any]] = {}  # physical_id -> 归属清单（两模块共享）
        self.plans: dict[str, dict[str, Any]] = {}
        self.review_list: list[dict[str, Any]] = []     # 合并复核清单
        self.write_log: list[dict[str, Any]] = []       # 实际写入日志，一次提交一条

    # ---------------- 目录同步与写入权限 ----------------

    def reset(self) -> None:
        with self._lock:
            self.directory = TableDirectory()
            self._seq = 0
            self.manifests = {}
            self.plans = {}
            self.review_list = []
            self.write_log = []

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def sync_lake_table_entry(self, entry: dict[str, Any]) -> dict[str, Any] | None:
        """把湖表定义同步进目录；只登记未知物理表、刷新可写状态，不改名。"""
        physical_id = str(entry.get("表标识", "") or "").strip()
        if not physical_id:
            return None
        name = str(entry.get("表名称", "") or "").strip()
        writable = entry.get("status") != "已冻结"
        with self._lock:
            table = self.directory.register_table(
                physical_id, name or None, writable=writable)
            table["writable"] = writable
            return table

    def _sync_lake_tables(self) -> None:
        for row in list(store.rows("lake_table")):
            self.sync_lake_table_entry(row)

    def register_lake_table(
        self,
        physical_id: str,
        name: str | None = None,
        *,
        priority: int = 0,
        writable: bool = True,
    ) -> dict[str, Any]:
        with self._lock:
            return self.directory.register_table(
                physical_id, name, priority=priority, writable=writable)

    def resolve_table(self, table_ref: str | None) -> str | None:
        self._sync_lake_tables()
        return self.directory.resolve(table_ref)

    def check_write_permission(self, table_ref: str | None) -> tuple[str | None, str | None]:
        """提交前校验湖表写入权限：未登记或已冻结都拒绝越权。"""
        physical_id = self.resolve_table(table_ref)
        if physical_id is None:
            return None, f"目标湖表「{table_ref}」未在湖表目录登记，拒绝越权写入"
        table = self.directory.tables[physical_id]
        if not table.get("writable", True):
            return None, f"目标湖表「{table_ref}」已冻结，拒绝越权写入"
        return physical_id, None

    def rename_table(self, physical_id: str, new_name: str) -> dict[str, Any] | None:
        with self._lock:
            return self.directory.rename_table(physical_id, new_name)

    def reassign_files(self, paths: list[str], to_table: str) -> tuple[list[str] | None, str | None]:
        target = self.resolve_table(to_table)
        if target is None:
            return None, f"目标湖表「{to_table}」未在湖表目录登记"
        with self._lock:
            return self.directory.reassign_files(paths, target), None

    # ---------------- 目标表文件归属清单 ----------------

    def get_manifest(self, physical_id: str) -> dict[str, Any] | None:
        with self._lock:
            manifest = self.manifests.get(physical_id)
            return copy.deepcopy(manifest) if manifest else None

    def _refresh_manifest(self, physical_id: str, files: list[str]) -> dict[str, Any]:
        """生成/刷新目标表文件归属清单；分区合并与压缩任务读取同一份。"""
        manifest = self.manifests.get(physical_id)
        if manifest is None:
            manifest = {
                "manifest_id": f"mfst-{physical_id}",
                "table_identity": physical_id,
                "table_name": None,
                "version": 0,
                "files": {},
            }
            self.manifests[physical_id] = manifest
        table = self.directory.tables.get(physical_id, {})
        manifest["table_name"] = table.get("name")
        for path in files:
            owner = self.directory.claim_file(physical_id, path)
            record = manifest["files"].get(path) or {"path": path, "partition": partition_of(path)}
            record["owner"] = owner
            record["status"] = "owned" if owner == physical_id else "conflict"
            record["conflict_with"] = None if owner == physical_id else owner
            manifest["files"][path] = record
        manifest["version"] += 1
        return manifest

    # ---------------- 计划：准备、提交、中断恢复 ----------------

    def prepare_plan(
        self,
        *,
        module: str,
        entry_id: int | None,
        table_ref: str,
        files: list[str],
        boundary: str = BOUNDARY_ABORT,
        priority: int = 0,
    ) -> dict[str, Any]:
        """提交前校验湖表写入权限和共享归属，生成目标表文件归属清单。"""
        if boundary not in BOUNDARIES:
            return {"ok": False, "message": f"未知处置边界「{boundary}」，可选：{'、'.join(BOUNDARIES)}"}
        with self._lock:
            physical_id, error = self.check_write_permission(table_ref)
            if error:
                return {"ok": False, "message": error}
            assert physical_id is not None
            manifest = self._refresh_manifest(physical_id, files)
            plan = {
                "plan_id": f"plan-{self._next_seq()}",
                "module": module,
                "entry_id": entry_id,
                "table_identity": physical_id,      # 原表身份：恢复时不重新解释旧别名
                "table_name": self.directory.tables[physical_id].get("name"),
                "files": list(files),
                "boundary": boundary,
                "priority": int(priority),
                "status": "prepared",
                "result": None,
                "manifest_id": manifest["manifest_id"],
            }
            self.plans[plan["plan_id"]] = plan
            return {"ok": True, "plan": copy.deepcopy(plan), "manifest": copy.deepcopy(manifest)}

    def latest_pending_plan(self, module: str, entry_id: int) -> dict[str, Any] | None:
        with self._lock:
            candidates = [
                plan for plan in self.plans.values()
                if plan["module"] == module and plan["entry_id"] == entry_id
                and plan["status"] == "prepared"
            ]
            if not candidates:
                return None
            latest = max(candidates, key=lambda p: int(p["plan_id"].split("-")[1]))
            return copy.deepcopy(latest)

    def _append_review(
        self,
        plan: dict[str, Any],
        path: str,
        conflict_with: str,
        *,
        outcome: str,
        status: str,
    ) -> None:
        """冲突结果回写合并复核清单。"""
        self.review_list.append({
            "id": self._next_seq(),
            "plan_id": plan["plan_id"],
            "module": plan["module"],
            "entry_id": plan["entry_id"],
            "table_identity": plan["table_identity"],
            "table_name": plan["table_name"],
            "file": path,
            "partition": partition_of(path),
            "conflict_with": conflict_with,
            "boundary": plan["boundary"],
            "outcome": outcome,
            "status": status,
        })

    def commit_plan(self, plan_id: str) -> dict[str, Any]:
        """提交计划：同一计划只生效一次，并发提交也只执行一次实际写入。"""
        with self._lock:
            plan = self.plans.get(plan_id)
            if plan is None:
                return {"ok": False, "message": f"计划 {plan_id} 不存在"}
            if plan["status"] in ("applied", "aborted"):
                return {**copy.deepcopy(plan["result"]), "already_applied": True}
            if plan["status"] == "interrupted":
                return {"ok": False, "message": f"计划 {plan_id} 已中断，需先恢复再提交"}

            identity = plan["table_identity"]
            owned: list[str] = []
            conflicts: list[dict[str, Any]] = []
            overridden: list[dict[str, Any]] = []
            for path in plan["files"]:
                owner = self.directory.owner_of(path)
                if owner is None:
                    self.directory.set_owner(path, identity)
                    owner = identity
                if owner == identity:
                    owned.append(path)
                    continue
                # 共享归属冲突：处理优先级决定哪份文件归属生效
                if plan["priority"] > self.directory.table_priority(owner):
                    self.directory.set_owner(path, identity)
                    owned.append(path)
                    overridden.append({"path": path, "from": owner})
                    self._append_review(
                        plan, path, owner, outcome="overridden", status=REVIEW_RESOLVED)
                else:
                    conflicts.append({"path": path, "owner": owner})

            if conflicts and plan["boundary"] == BOUNDARY_ABORT:
                # 整项中止：本计划没有重写
                for item in conflicts:
                    self._append_review(
                        plan, item["path"], item["owner"],
                        outcome="aborted", status=REVIEW_PENDING)
                plan["status"] = "aborted"
                result = {
                    "ok": False,
                    "aborted": True,
                    "boundary": plan["boundary"],
                    "plan_id": plan_id,
                    "table_identity": identity,
                    "table_name": plan["table_name"],
                    "rewritten_files": [],
                    "conflicts": conflicts,
                    "write_count": 0,
                    "message": "存在归属冲突，整项中止，本计划没有重写",
                }
                plan["result"] = copy.deepcopy(result)
                self._refresh_manifest(identity, plan["files"])
                return result

            # 逐文件隔离（或无冲突）：只实际处理仍能证明属于原表的文件
            for item in conflicts:
                self._append_review(
                    plan, item["path"], item["owner"],
                    outcome="isolated", status=REVIEW_PENDING)
            write = {
                "seq": self._next_seq(),
                "plan_id": plan_id,
                "table_identity": identity,
                "table_name": plan["table_name"],
                "files": list(owned),
            }
            self.write_log.append(write)
            plan["status"] = "applied"
            result = {
                "ok": True,
                "aborted": False,
                "boundary": plan["boundary"],
                "plan_id": plan_id,
                "table_identity": identity,
                "table_name": plan["table_name"],
                "rewritten_files": list(owned),
                "conflicts": conflicts,
                "overridden": overridden,
                "write_count": 1,
                "write_seq": write["seq"],
            }
            plan["result"] = copy.deepcopy(result)
            self._refresh_manifest(identity, plan["files"])
            return result

    def interrupt_plan(self, plan_id: str) -> dict[str, Any]:
        with self._lock:
            plan = self.plans.get(plan_id)
            if plan is None:
                return {"ok": False, "message": f"计划 {plan_id} 不存在"}
            if plan["status"] != "prepared":
                return {"ok": False, "message": f"计划 {plan_id} 状态为 {plan['status']}，无法中断"}
            plan["status"] = "interrupted"
            return {"ok": True, "plan_id": plan_id, "status": "interrupted"}

    def recover_plan(self, plan_id: str) -> dict[str, Any]:
        """中断恢复：保留原表身份，不把旧别名重新解释成新湖表。"""
        with self._lock:
            plan = self.plans.get(plan_id)
            if plan is None:
                return {"ok": False, "message": f"计划 {plan_id} 不存在"}
            if plan["status"] != "interrupted":
                return {"ok": False, "message": f"计划 {plan_id} 状态为 {plan['status']}，无需恢复"}
            plan["status"] = "prepared"
            return {
                "ok": True,
                "plan_id": plan_id,
                "status": "prepared",
                "table_identity": plan["table_identity"],
            }

    def snapshot_state(self) -> dict[str, Any]:
        """持久化在途状态；目录（表目录）作为共享元存储不在快照内。"""
        with self._lock:
            return {
                "seq": self._seq,
                "plans": copy.deepcopy(self.plans),
                "manifests": copy.deepcopy(self.manifests),
                "review_list": copy.deepcopy(self.review_list),
                "write_log": copy.deepcopy(self.write_log),
            }

    def restore_state(self, snapshot: dict[str, Any]) -> None:
        """从快照恢复：计划的原表身份原样保留。"""
        with self._lock:
            self._seq = int(snapshot.get("seq", 0))
            self.plans = copy.deepcopy(snapshot.get("plans", {}))
            self.manifests = copy.deepcopy(snapshot.get("manifests", {}))
            self.review_list = copy.deepcopy(snapshot.get("review_list", []))
            self.write_log = copy.deepcopy(snapshot.get("write_log", []))

    # ---------------- 详情与复核清单 ----------------

    def table_detail(self, table_ref: str) -> dict[str, Any]:
        """按物理表输出重写文件清单、冲突待办与写入次数。"""
        physical_id = self.resolve_table(table_ref)
        if physical_id is None:
            return {"ok": False, "message": f"湖表「{table_ref}」未在湖表目录登记"}
        with self._lock:
            table = self.directory.tables[physical_id]
            writes = [dict(w) for w in self.write_log if w["table_identity"] == physical_id]
            conflicts = [dict(r) for r in self.review_list if r["table_identity"] == physical_id]
            manifest = self.manifests.get(physical_id)
            return {
                "ok": True,
                "table_identity": physical_id,
                "table_name": table.get("name"),
                "writable": table.get("writable", True),
                "manifest_id": manifest["manifest_id"] if manifest else None,
                "manifest_files": sorted(
                    (dict(f) for f in manifest["files"].values()),
                    key=lambda f: f["path"],
                ) if manifest else [],
                "writes": writes,
                "write_count": len(writes),
                "rewritten_files": [path for w in writes for path in w["files"]],
                "conflicts": conflicts,
                "conflict_todos": sum(1 for r in conflicts if r["status"] == REVIEW_PENDING),
            }

    def review_items(self, *, only_todos: bool = False) -> list[dict[str, Any]]:
        with self._lock:
            items = [dict(r) for r in self.review_list]
        if only_todos:
            items = [r for r in items if r["status"] == REVIEW_PENDING]
        return items


# 共享单例：分区合并与压缩任务各入口读取同一份目标表文件归属清单
ownership_guard = OwnershipGuard()
