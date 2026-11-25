"""归属守卫与存储优化链路测试：覆盖共同根因修复的全部行为。

场景主线：计划取数后目录里的同一个表名被分配给另一个湖表，部分文件仍属
原表 A、其余已指向 B；整项中止与隔离冲突文件会让 A 产生不同的实际重写结果。
"""
from __future__ import annotations

import sys
import threading

sys.path.insert(0, '.')

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.ownership import (
    BOUNDARY_ABORT,
    BOUNDARY_ISOLATE,
    OwnershipGuard,
    compaction_files_for,
    merge_files_for,
    ownership_guard,
)
from app.services import service_for

client = TestClient(app)

A_FILES = [
    "orders/dt=2026-11-01/part-0.parquet",
    "orders/dt=2026-11-01/part-1.parquet",
    "orders/dt=2026-11-02/part-0.parquet",
    "orders/dt=2026-11-02/part-1.parquet",
]


@pytest.fixture(autouse=True)
def clean_guard():
    """每个用例前后都重置共享守卫，避免跨文件状态污染。"""
    ownership_guard.reset()
    yield
    ownership_guard.reset()


def make_guard() -> OwnershipGuard:
    guard = OwnershipGuard()
    guard.register_lake_table("tbl_a", "orders")
    return guard


def shuffle_name_to_b(guard: OwnershipGuard, files: list[str]) -> None:
    """目录事件：A 改名，旧名留给 B，部分文件指向 B。"""
    guard.rename_table("tbl_a", "orders_archive")
    guard.register_lake_table("tbl_b", "orders")
    guard.reassign_files(files, "tbl_b")


class TestWritePermission:
    def test_unknown_table_rejected(self):
        guard = OwnershipGuard()
        result = guard.prepare_plan(
            module="partition_merge", entry_id=1,
            table_ref="不存在的湖表", files=["x/part-0.parquet"],
        )
        assert result["ok"] is False
        assert "拒绝越权" in result["message"]

    def test_frozen_table_rejected(self):
        guard = OwnershipGuard()
        guard.register_lake_table("tbl_frozen", "frozen_tbl", writable=False)
        result = guard.prepare_plan(
            module="compaction_task", entry_id=1,
            table_ref="frozen_tbl", files=["frozen_tbl/data/part-0.parquet"],
        )
        assert result["ok"] is False
        assert "拒绝越权" in result["message"]

    def test_invalid_boundary_rejected(self):
        guard = make_guard()
        result = guard.prepare_plan(
            module="partition_merge", entry_id=1,
            table_ref="orders", files=A_FILES, boundary="overwrite",
        )
        assert result["ok"] is False
        assert "处置边界" in result["message"]

    def test_service_level_unknown_table_rejected(self):
        svc = service_for("partition_merge")
        entry, missing = svc.create_entry({
            "任务标识": "越权合并", "目标湖表": "不存在的湖表", "源分区": "dt=1",
        })
        assert not missing
        result = svc.domain_merge_partitions(entry["id"])
        assert result["ok"] is False
        assert "拒绝越权" in result["message"]


class TestSharedManifest:
    def test_merge_and_compaction_share_one_manifest(self):
        guard = make_guard()
        merge = guard.prepare_plan(
            module="partition_merge", entry_id=1, table_ref="orders",
            files=merge_files_for("orders", ["dt=1"]), boundary=BOUNDARY_ISOLATE,
        )
        comp = guard.prepare_plan(
            module="compaction_task", entry_id=1, table_ref="orders",
            files=compaction_files_for("orders"), boundary=BOUNDARY_ISOLATE,
        )
        assert merge["ok"] and comp["ok"]
        # 两个入口读取同一份目标表文件归属清单
        assert merge["manifest"]["manifest_id"] == comp["manifest"]["manifest_id"]
        manifest = guard.get_manifest("tbl_a")
        paths = set(manifest["files"])
        assert "orders/dt=1/part-0.parquet" in paths
        assert "orders/data/part-0.parquet" in paths


class TestMixedChainRejected:
    def test_chain_table1_merge2_compaction3_rejected(self):
        """表 1、合并任务 2、压缩任务 3 混用：目标湖表不同，拒绝越权。"""
        resp = client.post(
            "/api/chains/storage-optimize",
            params={"table_id": 1, "merge_id": 2, "compaction_id": 3},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is False
        assert "拒绝越权" in data["error"]
        # 第一步定义湖表成功，第二步合并即被拦截
        assert [step["step"] for step in data["steps"]] == ["define_table", "merge_partitions"]
        assert data["steps"][0]["ok"] is True
        assert data["steps"][1]["ok"] is False

    def test_chain_table1_merge1_compaction3_rejected(self):
        resp = client.post(
            "/api/chains/storage-optimize",
            params={"table_id": 1, "merge_id": 1, "compaction_id": 3},
        )
        data = resp.json()
        assert data["ok"] is False
        assert "拒绝越权" in data["error"]
        assert [step["step"] for step in data["steps"]] == [
            "define_table", "merge_partitions", "plan_compaction"]

    def test_chain_consistent_entries_ok(self):
        resp = client.post(
            "/api/chains/storage-optimize",
            params={"table_id": 1, "merge_id": 1, "compaction_id": 1},
        )
        data = resp.json()
        assert data["ok"] is True
        assert len(data["steps"]) == 4
        # 合并与压缩读取同一份归属清单
        merge_detail = data["steps"][1]["details"]
        comp_detail = data["steps"][2]["details"]
        assert merge_detail["manifest_id"] == comp_detail["manifest_id"]


class TestAbortVsIsolate:
    """计划取数后表名被分配给另一个湖表：两种处置边界产生不同重写结果。"""

    def test_abort_boundary_no_rewrite(self):
        guard = make_guard()
        prepared = guard.prepare_plan(
            module="partition_merge", entry_id=1, table_ref="orders",
            files=A_FILES, boundary=BOUNDARY_ABORT,
        )
        assert prepared["ok"] is True
        plan_id = prepared["plan"]["plan_id"]
        assert prepared["plan"]["table_identity"] == "tbl_a"

        shuffle_name_to_b(guard, A_FILES[2:])
        result = guard.commit_plan(plan_id)

        # 整项中止：本计划没有重写
        assert result["ok"] is False
        assert result["aborted"] is True
        assert result["boundary"] == BOUNDARY_ABORT
        assert result["rewritten_files"] == []
        assert result["write_count"] == 0
        assert {c["path"] for c in result["conflicts"]} == set(A_FILES[2:])

        # 冲突结果回写合并复核清单
        review = guard.review_items()
        assert len(review) == 2
        assert all(r["status"] == "待复核" for r in review)
        assert all(r["outcome"] == "aborted" for r in review)
        assert all(r["table_identity"] == "tbl_a" for r in review)
        assert all(r["conflict_with"] == "tbl_b" for r in review)

        # A、B 详情：重写文件清单、冲突待办与写入次数
        detail_a = guard.table_detail("tbl_a")
        assert detail_a["write_count"] == 0
        assert detail_a["rewritten_files"] == []
        assert detail_a["conflict_todos"] == 2
        detail_b = guard.table_detail("tbl_b")
        assert detail_b["write_count"] == 0
        assert detail_b["rewritten_files"] == []

    def test_isolate_boundary_partial_rewrite(self):
        guard = make_guard()
        prepared = guard.prepare_plan(
            module="partition_merge", entry_id=1, table_ref="orders",
            files=A_FILES, boundary=BOUNDARY_ISOLATE,
        )
        plan_id = prepared["plan"]["plan_id"]

        shuffle_name_to_b(guard, A_FILES[2:])
        result = guard.commit_plan(plan_id)

        # 逐文件隔离：只实际处理仍能证明属于原表 A 的文件
        assert result["ok"] is True
        assert result["boundary"] == BOUNDARY_ISOLATE
        assert result["rewritten_files"] == A_FILES[:2]
        assert {c["path"] for c in result["conflicts"]} == set(A_FILES[2:])
        assert result["write_count"] == 1

        review = guard.review_items()
        assert len(review) == 2
        assert all(r["outcome"] == "isolated" for r in review)

        detail_a = guard.table_detail("tbl_a")
        assert detail_a["write_count"] == 1
        assert detail_a["rewritten_files"] == A_FILES[:2]
        assert detail_a["conflict_todos"] == 2
        detail_b = guard.table_detail("tbl_b")
        assert detail_b["write_count"] == 0
        assert detail_b["rewritten_files"] == []

    def test_two_boundaries_give_different_results(self):
        """同一归属变更下，整项中止与逐文件隔离让 A 产生不同实际重写结果。"""
        outcomes = {}
        for boundary in (BOUNDARY_ABORT, BOUNDARY_ISOLATE):
            guard = make_guard()
            prepared = guard.prepare_plan(
                module="partition_merge", entry_id=1, table_ref="orders",
                files=A_FILES, boundary=boundary,
            )
            shuffle_name_to_b(guard, A_FILES[2:])
            outcomes[boundary] = guard.commit_plan(prepared["plan"]["plan_id"])
        assert outcomes[BOUNDARY_ABORT]["rewritten_files"] == []
        assert outcomes[BOUNDARY_ABORT]["write_count"] == 0
        assert outcomes[BOUNDARY_ISOLATE]["rewritten_files"] == A_FILES[:2]
        assert outcomes[BOUNDARY_ISOLATE]["write_count"] == 1


class TestIdempotentCommit:
    def test_same_plan_commits_once(self):
        guard = make_guard()
        prepared = guard.prepare_plan(
            module="compaction_task", entry_id=1, table_ref="orders",
            files=compaction_files_for("orders"), boundary=BOUNDARY_ISOLATE,
        )
        plan_id = prepared["plan"]["plan_id"]
        first = guard.commit_plan(plan_id)
        second = guard.commit_plan(plan_id)
        assert first["ok"] is True and first["write_count"] == 1
        assert second["already_applied"] is True
        assert second["write_count"] == 1
        assert len(guard.write_log) == 1
        assert guard.table_detail("tbl_a")["write_count"] == 1

    def test_aborted_plan_commits_once(self):
        guard = make_guard()
        prepared = guard.prepare_plan(
            module="partition_merge", entry_id=1, table_ref="orders",
            files=A_FILES, boundary=BOUNDARY_ABORT,
        )
        shuffle_name_to_b(guard, A_FILES[2:])
        plan_id = prepared["plan"]["plan_id"]
        first = guard.commit_plan(plan_id)
        second = guard.commit_plan(plan_id)
        assert first["aborted"] is True
        assert second["already_applied"] is True
        assert len(guard.write_log) == 0
        assert len(guard.review_list) == 2  # 冲突结果只回写一次

    def test_concurrent_commits_single_write(self):
        guard = make_guard()
        prepared = guard.prepare_plan(
            module="partition_merge", entry_id=1, table_ref="orders",
            files=A_FILES, boundary=BOUNDARY_ISOLATE,
        )
        plan_id = prepared["plan"]["plan_id"]
        results = []
        threads = [
            threading.Thread(target=lambda: results.append(guard.commit_plan(plan_id)))
            for _ in range(8)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # 并发提交也只执行一次实际写入
        assert len(guard.write_log) == 1
        assert sum(1 for r in results if not r.get("already_applied")) == 1
        assert all(r["ok"] for r in results)
        assert guard.table_detail("tbl_a")["write_count"] == 1


class TestPriority:
    def test_higher_priority_takes_over_contested_file(self):
        guard = make_guard()
        guard.register_lake_table("tbl_b", "other", priority=1)
        guard.directory.claim_file("tbl_b", A_FILES[0])
        prepared = guard.prepare_plan(
            module="partition_merge", entry_id=1, table_ref="orders",
            files=A_FILES[:1], boundary=BOUNDARY_ISOLATE, priority=5,
        )
        result = guard.commit_plan(prepared["plan"]["plan_id"])
        # 处理优先级更高：该文件归属生效于本计划
        assert result["ok"] is True
        assert result["rewritten_files"] == A_FILES[:1]
        assert result["overridden"] == [{"path": A_FILES[0], "from": "tbl_b"}]
        assert guard.directory.owner_of(A_FILES[0]) == "tbl_a"
        review = guard.review_items()
        assert review[0]["outcome"] == "overridden"
        assert review[0]["status"] == "已处理"

    def test_lower_priority_conflict_isolated(self):
        guard = make_guard()
        guard.register_lake_table("tbl_b", "other", priority=5)
        guard.directory.claim_file("tbl_b", A_FILES[0])
        prepared = guard.prepare_plan(
            module="partition_merge", entry_id=1, table_ref="orders",
            files=A_FILES[:1], boundary=BOUNDARY_ISOLATE, priority=1,
        )
        result = guard.commit_plan(prepared["plan"]["plan_id"])
        assert result["ok"] is True
        assert result["rewritten_files"] == []
        assert [c["path"] for c in result["conflicts"]] == A_FILES[:1]
        assert guard.directory.owner_of(A_FILES[0]) == "tbl_b"


class TestRecovery:
    def test_interrupted_plan_needs_recover(self):
        guard = make_guard()
        prepared = guard.prepare_plan(
            module="partition_merge", entry_id=1, table_ref="orders",
            files=A_FILES, boundary=BOUNDARY_ISOLATE,
        )
        plan_id = prepared["plan"]["plan_id"]
        guard.interrupt_plan(plan_id)
        blocked = guard.commit_plan(plan_id)
        assert blocked["ok"] is False
        assert "已中断" in blocked["message"]
        recovered = guard.recover_plan(plan_id)
        assert recovered["ok"] is True
        assert recovered["table_identity"] == "tbl_a"
        assert guard.commit_plan(plan_id)["ok"] is True

    def test_recover_preserves_original_identity(self):
        """中断恢复保留原表身份，不把旧别名重新解释成新湖表。"""
        directory_guard = make_guard()
        prepared = directory_guard.prepare_plan(
            module="partition_merge", entry_id=1, table_ref="orders",
            files=A_FILES, boundary=BOUNDARY_ISOLATE,
        )
        plan_id = prepared["plan"]["plan_id"]
        snapshot = directory_guard.snapshot_state()
        shared_directory = directory_guard.directory

        # 崩溃后目录变更：A 改名，旧名留给 B，部分文件指向 B
        directory_guard.rename_table("tbl_a", "orders_archive")
        directory_guard.register_lake_table("tbl_b", "orders")
        directory_guard.reassign_files(A_FILES[2:], "tbl_b")

        # 用共享目录 + 快照恢复守卫（模拟重启），计划仍属原表 A
        restored = OwnershipGuard(shared_directory)
        restored.restore_state(snapshot)
        assert restored.plans[plan_id]["table_identity"] == "tbl_a"
        # 旧别名 orders 现在解析到 B，但计划不会被重新解释
        assert restored.resolve_table("orders") == "tbl_b"
        result = restored.commit_plan(plan_id)
        assert result["table_identity"] == "tbl_a"
        assert result["rewritten_files"] == A_FILES[:2]
        assert {c["path"] for c in result["conflicts"]} == set(A_FILES[2:])


class TestSameNameDistinction:
    def test_same_name_files_distinguished_by_physical_table(self):
        guard = make_guard()
        guard.register_lake_table("tbl_b", "other")
        shared_path = "shared/dt=1/part-0.parquet"
        plan_a = guard.prepare_plan(
            module="partition_merge", entry_id=1, table_ref="orders",
            files=[shared_path], boundary=BOUNDARY_ISOLATE,
        )
        plan_b = guard.prepare_plan(
            module="partition_merge", entry_id=2, table_ref="other",
            files=[shared_path], boundary=BOUNDARY_ISOLATE,
        )
        # 同名文件按物理表区分：各自清单记录各自的归属视角
        manifest_a = guard.get_manifest("tbl_a")
        manifest_b = guard.get_manifest("tbl_b")
        assert manifest_a["manifest_id"] != manifest_b["manifest_id"]
        assert manifest_a["files"][shared_path]["status"] == "owned"
        assert manifest_b["files"][shared_path]["status"] == "conflict"
        assert manifest_b["files"][shared_path]["conflict_with"] == "tbl_a"

        result_b = guard.commit_plan(plan_b["plan"]["plan_id"])
        assert result_b["rewritten_files"] == []
        review = guard.review_items()
        assert review[0]["table_identity"] == "tbl_b"
        assert review[0]["conflict_with"] == "tbl_a"
        # A 的计划不受影响，仍可正常提交
        result_a = guard.commit_plan(plan_a["plan"]["plan_id"])
        assert result_a["rewritten_files"] == [shared_path]


class TestOwnershipApi:
    def test_full_scenario_isolate_via_api(self):
        """为合法的 A 规划 → 改名把旧名留给 B → 提交原计划 → 核对逐文件边界。"""
        client.post("/api/ownership/catalog/register",
                    json={"physical_id": "tbl_a", "name": "orders"})
        # 用种子合并任务 1（目标湖表=湖表定义样例1）走 module 入口准备计划
        resp = client.post("/api/ownership/plans", json={
            "module": "partition_merge", "entry_id": 1, "boundary": "isolate",
        })
        assert resp.status_code == 200
        prepared = resp.json()
        assert prepared["ok"] is True
        plan_id = prepared["plan_id"]
        identity = prepared["plan"]["table_identity"]
        planned_files = prepared["plan"]["files"]
        assert len(planned_files) == 3

        # 目录变更：A 改名，旧名留给 B，其余文件指向 B
        client.post("/api/ownership/catalog/rename",
                    json={"physical_id": identity, "new_name": "样例1归档"})
        client.post("/api/ownership/catalog/register",
                    json={"physical_id": "tbl_b", "name": "湖表定义样例1"})
        client.post("/api/ownership/catalog/reassign-files",
                    json={"paths": planned_files[1:], "to_table": "tbl_b"})

        # 提交原计划：采用逐文件隔离边界
        committed = client.post(f"/api/ownership/plans/{plan_id}/commit").json()
        assert committed["ok"] is True
        assert committed["boundary"] == "isolate"
        assert committed["table_identity"] == identity
        assert committed["rewritten_files"] == planned_files[:1]
        assert {c["path"] for c in committed["conflicts"]} == set(planned_files[1:])
        assert committed["write_count"] == 1

        # 同一计划只生效一次
        again = client.post(f"/api/ownership/plans/{plan_id}/commit").json()
        assert again["already_applied"] is True
        assert again["write_count"] == 1

        # 切换 A、B 详情：重写文件清单、冲突待办与写入次数
        detail_a = client.get(f"/api/ownership/tables/{identity}/detail").json()
        assert detail_a["write_count"] == 1
        assert detail_a["rewritten_files"] == planned_files[:1]
        assert detail_a["conflict_todos"] == 2
        detail_b = client.get("/api/ownership/tables/tbl_b/detail").json()
        assert detail_b["write_count"] == 0
        assert detail_b["rewritten_files"] == []

        # 冲突结果回写合并复核清单
        review = client.get("/api/ownership/review-list").json()
        assert review["total"] == 2
        assert review["todos"] == 2
        assert all(item["conflict_with"] == "tbl_b" for item in review["items"])

    def test_full_scenario_abort_via_api(self):
        """同一归属变更下整项中止：本计划没有重写。"""
        resp = client.post("/api/ownership/plans", json={
            "module": "compaction_task", "entry_id": 1, "boundary": "abort",
        })
        prepared = resp.json()
        plan_id = prepared["plan_id"]
        identity = prepared["plan"]["table_identity"]
        planned_files = prepared["plan"]["files"]

        client.post("/api/ownership/catalog/rename",
                    json={"physical_id": identity, "new_name": "样例1归档"})
        client.post("/api/ownership/catalog/register",
                    json={"physical_id": "tbl_b", "name": "湖表定义样例1"})
        client.post("/api/ownership/catalog/reassign-files",
                    json={"paths": planned_files[:1], "to_table": "tbl_b"})

        committed = client.post(f"/api/ownership/plans/{plan_id}/commit").json()
        assert committed["ok"] is False
        assert committed["aborted"] is True
        assert committed["boundary"] == "abort"
        assert committed["rewritten_files"] == []
        assert committed["write_count"] == 0

        detail_a = client.get(f"/api/ownership/tables/{identity}/detail").json()
        assert detail_a["write_count"] == 0
        assert detail_a["conflict_todos"] == 1
        review = client.get("/api/ownership/review-list?only_todos=true").json()
        assert review["total"] == 1
        assert review["items"][0]["outcome"] == "aborted"

    def test_manifest_endpoint_shared_between_modules(self):
        client.post("/api/ownership/plans", json={
            "module": "partition_merge", "entry_id": 1, "boundary": "isolate",
        })
        client.post("/api/ownership/plans", json={
            "module": "compaction_task", "entry_id": 1, "boundary": "isolate",
        })
        resp = client.get("/api/ownership/manifests/湖表定义样例1")
        assert resp.status_code == 200
        manifest = resp.json()["manifest"]
        assert manifest["manifest_id"] == "mfst-湖表定义样例1"
        paths = {f["path"] for f in manifest["files"]}
        assert "湖表定义样例1/分区合并样例1/part-0.parquet" in paths
        assert "湖表定义样例1/data/part-0.parquet" in paths

    def test_prepare_unknown_table_rejected_via_api(self):
        resp = client.post("/api/ownership/plans", json={
            "table_ref": "不存在的湖表", "files": ["x/part-0.parquet"],
        })
        assert resp.status_code == 200
        assert resp.json()["ok"] is False
        assert "拒绝越权" in resp.json()["message"]

    def test_commit_missing_plan_404(self):
        resp = client.post("/api/ownership/plans/plan-999/commit")
        assert resp.status_code == 404
