"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import chains, ownership, resource

# ownership 含 /api/ownership/... 固定前缀，必须先于 resource 的 /api/{module} 注册，
# 否则两段式固定路径（如 review-list）会被动态路径抢先匹配。
ROUTERS = [ownership, chains, resource]
