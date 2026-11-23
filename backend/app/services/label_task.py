"""标注派发领域服务：任务被真正领取时才写入样本派发明细。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.sample_dispatch import dispatch_ledger
from app.store import store


class LabelTaskService(BaseModuleService):
    MODULE = "label_task"
    ENTITY = "标注任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["任务编号", "样本范围", "标注类型", "标注规范", "派发人数", "截止日期", "完成进度", "派发时间", "关联策略号"]
    REQUIRED_FIELDS = ["任务编号", "样本范围", "标注类型"]
    STATUS_ORDER = ["待派发", "标注中", "已完成", "已回收"]
    ACTION_RULES = {"派发任务": "标注中", "回收任务": "已完成", "强制完成": "已回收"}
    NEGATIVE_ACTIONS = []

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """登记/恢复待派发任务后，立即尝试补派给队列中等待的出样。

        历史任务按原编号、原样本范围登记，本服务不会改写它们；补派发命中时
        只更新任务状态并在账本落明细，未命中则任务保持「待派发」。
        """
        entry, missing = super().create_entry(values)
        if entry is None:
            return entry, missing

        # 恢复进入待派发池：按先到先得尝试补派，命中时由账本统一落明细
        dispatch_ledger.on_task_available(entry)
        return entry, missing

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        if action == "派发任务":
            return self._dispatch(entry)
        return super().run_action(entry_id, action)

    def _dispatch(self, entry: dict[str, Any]) -> tuple[dict[str, Any], str]:
        """派发任务：优先把任务领给等待中的出样，落样本派发明细。"""
        if entry.get("status") != "待派发":
            # 已派发任务重复操作不允许改写既有明细
            return entry, (
                f"{self.ENTITY}已处于{entry.get('status')}，"
                f"关联出样 {entry.get('关联策略号', '') or '—'} 的派发记录保持不变"
            )

        ledger = dispatch_ledger.claim_for_manual_dispatch(entry)
        if ledger is not None:
            return entry, (
                f"{self.ENTITY}已派发，样本派发明细已关联出样"
                f"{ledger.get('策略编号', '')}，任务进入标注中"
            )

        # 无等待中的可受理出样：走通用流转，任务自身编号与样本范围不变
        entry["status"] = "标注中"
        entry["pending"] = True
        entry["abnormal"] = False
        return entry, f"{self.ENTITY}已派发（暂无待分配出样，直接进入标注中）"
