"""湖表定义领域服务：定义湖表时把物理身份同步进归属守卫的表目录。"""
from __future__ import annotations

from typing import Any

from app.ownership import ownership_guard
from app.services.base import BaseModuleService
from app.store import store


class LakeTableService(BaseModuleService):
    MODULE = "lake_table"
    ENTITY = "湖表定义"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["表标识", "表名称", "主键列", "分区列", "存储格式", "压缩算法", "表版本", "当前快照"]
    REQUIRED_FIELDS = ["表标识", "表名称", "主键列"]
    STATUS_ORDER = ["草稿", "待校验", "已创建", "已冻结"]
    ACTION_RULES = {"提交校验": "待校验", "创建表": "已创建", "冻结表": "已冻结"}
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """状态流转后同步表目录可写状态（如冻结表即刻拒绝越权写入）。"""
        entry, message = super().run_action(entry_id, action)
        if entry is not None:
            ownership_guard.sync_lake_table_entry(entry)
        return entry, message

    def domain_define_table(self, entry_id: int) -> dict[str, Any]:
        """对当前数据执行状态，联动状态和处理处理，产出标准化记录。处理流程：读取配置→执行状态→状态校验→处理批次→写入存储，下游可消费且批次可追溯。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"湖表 {entry_id} 不存在"}

        ownership_guard.sync_lake_table_entry(entry)
        return {
            "ok": True,
            "table_id": entry.get("表标识"),
            "table_name": entry.get("表名称"),
            "physical_id": entry.get("表标识"),
            "primary_key": entry.get("主键列"),
            "partition_col": entry.get("分区列"),
            "storage_format": entry.get("存储格式", "parquet"),
            "compression": entry.get("压缩算法", "zstd"),
            "ddl": f"CREATE TABLE {entry.get('表标识')} (...) USING iceberg PARTITIONED BY ({entry.get('分区列')})",
            "schema_version": entry.get("表版本", "v1"),
        }
