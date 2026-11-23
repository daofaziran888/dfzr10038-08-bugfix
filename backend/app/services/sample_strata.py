"""样本分层领域服务：确认出样后沿数据流登记标注派发领取请求。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.sample_dispatch import (
    STATUS_WAITING,
    dispatch_ledger,
)
from app.store import store


class SampleStrataService(BaseModuleService):
    MODULE = "sample_strata"
    ENTITY = "分层策略"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["策略编号", "特征范围", "分层字段", "采样比例", "样本数量", "均衡算法", "覆盖检查", "最后采样", "派发状态", "关联任务号", "任务样本范围"]
    REQUIRED_FIELDS = ["策略编号", "特征范围", "分层字段"]
    STATUS_ORDER = ["草稿", "采样中", "已出样", "覆盖不足"]
    ACTION_RULES = {"启动采样": "采样中", "确认出样": "已出样", "标记不足": "覆盖不足"}
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        if action == "确认出样":
            return self._confirm_release(entry)
        return super().run_action(entry_id, action)

    def _confirm_release(self, entry: dict[str, Any]) -> tuple[dict[str, Any], str]:
        """确认出样：状态进入已出样，同时登记/复用派发明细并尝试领取任务。

        重复确认同一笔已出样策略时保持幂等：已完成派发的不会换任务、不会
        改写原任务编号与样本范围；仍待分配的会重新尝试领取（补名额后生效）。
        """
        entry["status"] = "已出样"
        entry["pending"] = False
        entry["abnormal"] = False

        result = dispatch_ledger.register_sample_release(entry)
        strategy_no = str(entry.get("策略编号", ""))
        if result.claimed:
            message = (
                f"{self.ENTITY}已确认出样，样本派发明细已落到标注任务"
                f"{result.task_no}（标注中）"
            )
        else:
            message = (
                f"{self.ENTITY}已确认出样，待派发池暂无可领取任务，"
                f"出样 {strategy_no} 保持「{STATUS_WAITING}」，任务恢复后自动派发"
            )
        return entry, message
