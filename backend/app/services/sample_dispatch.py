"""样本出样 → 标注派发 关联过程（链路7的真实数据流）。

设计要点（对应故障修复）：

1. 「确认出样」只在待派发池登记一笔领取请求，不再直接改写任何标注任务；
   样本派发明细只在真正领取到任务的那一刻写入派发明细账本。
2. 派发明细按出样（分层策略）幂等：同一笔出样被两位操作员反复确认，
   账本里最多只有一条有效派发记录，样本范围可追溯到这次出样。
3. 池子里没有可领取任务时，请求保持「待分配」，采样员能在样本分层清单上
   分清「已完成派发」与「还没有领到任务」。
4. 恢复/新登记历史待派发任务时按先到先得匹配：任务原编号、原样本范围保持
   不变；已经分给别的策略的记录不会被改动。
5. 标注任务被领取后进入「标注中」，并注明关联的出样策略号；未被领取的任务
   保持「待派发」，两类状态在派发清单上可区分。

模块只依赖内存仓库与标准库，写入路径统一走 :class:`DispatchLedger` 并由
``threading.RLock`` 串行化，保证并发确认下同一出样只产生一条有效派发。
"""
from __future__ import annotations

import datetime
import re
import threading
from dataclasses import dataclass
from typing import Any

from app.store import store

# 账本表名：与业务模块表并列存放在内存仓库中
LEDGER_MODULE = "sample_dispatch"

# 样本分层侧用于标识派发结果的两个状态
STATUS_WAITING = "待分配"   # 已确认出样，尚未领到标注任务
STATUS_DISPATCHED = "已完成派发"  # 已领取到任务，样本派发明细已落账

# 范围字段允许的分隔符：半角/全角分号、逗号、顿号与空白
_SPLIT_RE = re.compile(r"[;；,，、\s]+")


def _now() -> str:
    return datetime.datetime.now().isoformat()


def range_tokens(raw: Any) -> set[str]:
    """把样本/特征范围文本拆成可比较的 token 集合（保持原始大小写）。"""
    if raw is None:
        return set()
    return {token.strip() for token in _SPLIT_RE.split(str(raw)) if token.strip()}


def is_placeholder_range(raw: Any) -> bool:
    """识别示例数据里的占位范围，占位任务不参与真实出样匹配。"""
    text = str(raw or "").strip()
    if not text:
        return True
    return "样例" in text


@dataclass
class ClaimResult:
    """一次出样确认/任务恢复后的派发结果。"""

    ledger: dict[str, Any]
    claimed: bool
    task: dict[str, Any] | None = None
    is_new_ledger: bool = False

    @property
    def task_no(self) -> str:
        return str(self.ledger.get("关联任务号", "")) if self.claimed else ""


class DispatchLedger:
    """样本派发明细账本与待分配队列的统一管理者。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._seq = 0

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def rows(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(store.rows(LEDGER_MODULE))

    def find_by_strategy(self, strategy_id: int) -> dict[str, Any] | None:
        with self._lock:
            return self._find_ledger(strategy_id)

    def waiting_queue(self) -> list[dict[str, Any]]:
        with self._lock:
            return [
                dict(row)
                for row in store.rows(LEDGER_MODULE)
                if row.get("派发状态") == STATUS_WAITING
            ]

    def dispatched_rows(self) -> list[dict[str, Any]]:
        with self._lock:
            return [
                dict(row)
                for row in store.rows(LEDGER_MODULE)
                if row.get("派发状态") == STATUS_DISPATCHED
            ]

    # ------------------------------------------------------------------
    # 核心写入路径
    # ------------------------------------------------------------------
    def register_sample_release(
        self,
        strategy_row: dict[str, Any],
        *,
        operator: str | None = None,
    ) -> ClaimResult:
        """确认出样后登记领取请求，并尝试立即领取待派发任务。

        幂等：同一 ``strategy_row`` 重复调用不会产生第二条账本，也不会改写
        已经落账的任务号和样本范围；待分配的请求则会在每次调用时重新尝试
        领取（用于任务恢复/补名额后的再次派发）。
        """
        strategy_id = int(strategy_row.get("id", 0))
        with self._lock:
            existing = self._find_ledger(strategy_id)
            if existing is not None:
                if existing.get("派发状态") == STATUS_DISPATCHED:
                    # 已完成派发：重复确认直接返回，原任务编号与范围保持不变
                    return ClaimResult(existing, claimed=True,
                                       task=self._find_task(existing.get("关联任务号")))
                # 仍在待分配：重新尝试领取，不新建账本
                return self._try_claim(existing, new_ledger=False)

            ledger = self._new_ledger(strategy_row, operator)
            store.rows(LEDGER_MODULE).append(ledger)
            return self._try_claim(ledger, new_ledger=True)

    def on_task_available(self, task_row: dict[str, Any]) -> ClaimResult | None:
        """任务恢复/新登记进入待派发池后，按先到先得尝试补派发。

        只匹配仍处于「待分配」的出样请求；已派发的账本不会被触碰，已经分给
        别的策略的任务（非待派发）也不会被改写。
        """
        with self._lock:
            if task_row.get("status") != "待派发":
                return None
            if is_placeholder_range(task_row.get("样本范围")):
                return None
            for ledger in store.rows(LEDGER_MODULE):
                if ledger.get("派发状态") != STATUS_WAITING:
                    continue
                if not self._task_acceptable(ledger, task_row):
                    continue
                self._apply_claim(ledger, task_row)
                return ClaimResult(ledger, claimed=True, task=task_row)
            return None

    def claim_for_manual_dispatch(self, task_row: dict[str, Any]) -> dict[str, Any] | None:
        """标注派发页手工「派发任务」：优先把任务交给队列中最早可受理的出样。

        没有匹配的待分配出样时返回 ``None``，由调用方走通用状态流转；任务的
        原编号和样本范围始终保持不变。
        """
        with self._lock:
            if task_row.get("status") != "待派发":
                return None
            for ledger in store.rows(LEDGER_MODULE):
                if ledger.get("派发状态") != STATUS_WAITING:
                    continue
                if not self._task_acceptable(ledger, task_row):
                    continue
                self._apply_claim(ledger, task_row)
                return ledger
            return None

    # ------------------------------------------------------------------
    # 内部实现（调用方须持锁）
    # ------------------------------------------------------------------
    def _find_ledger(self, strategy_id: int) -> dict[str, Any] | None:
        for row in store.rows(LEDGER_MODULE):
            if int(row.get("出样id", 0)) == strategy_id:
                return row
        return None

    def _find_task(self, task_no: Any) -> dict[str, Any] | None:
        if not task_no:
            return None
        for row in store.rows("label_task"):
            if str(row.get("任务编号", "")) == str(task_no):
                return row
        return None

    def _new_ledger(
        self, strategy_row: dict[str, Any], operator: str | None
    ) -> dict[str, Any]:
        self._seq += 1
        strategy_no = str(strategy_row.get("策略编号", ""))
        return {
            "id": self._seq,
            "明细编号": f"DISP-{self._seq:04d}",
            "出样id": int(strategy_row.get("id", 0)),
            "策略编号": strategy_no,
            "申请范围": str(strategy_row.get("特征范围", "")),
            "可受理范围": str(strategy_row.get("特征范围", "")),
            "样本数量": strategy_row.get("样本数量", 0),
            "派发状态": STATUS_WAITING,
            "关联任务号": "",
            "任务样本范围": "",
            "登记时间": _now(),
            "派发时间": "",
            "确认操作员": operator or "",
        }

    def _try_claim(
        self, ledger: dict[str, Any], *, new_ledger: bool
    ) -> ClaimResult:
        """在待派发池中为一笔出样请求寻找可领取的任务（先到先得由队列顺序保证）。"""
        for task in store.rows("label_task"):
            if task.get("status") != "待派发":
                continue
            if is_placeholder_range(task.get("样本范围")):
                continue
            if not self._task_acceptable(ledger, task):
                continue
            self._apply_claim(ledger, task)
            return ClaimResult(ledger, claimed=True, task=task,
                               is_new_ledger=new_ledger)

        ledger["派发状态"] = STATUS_WAITING
        ledger["关联任务号"] = ""
        ledger["任务样本范围"] = ""
        ledger["派发时间"] = ""
        self._sync_strategy(ledger)
        return ClaimResult(ledger, claimed=False, is_new_ledger=new_ledger)

    @staticmethod
    def _task_acceptable(
        ledger: dict[str, Any], task_row: dict[str, Any]
    ) -> bool:
        """任务样本范围必须完整落在出样可受理范围内（集合包含关系）。"""
        task_tokens = range_tokens(task_row.get("样本范围"))
        if not task_tokens:
            return False
        accepted = range_tokens(ledger.get("可受理范围"))
        if not accepted:
            return False
        return task_tokens <= accepted

    def _apply_claim(
        self, ledger: dict[str, Any], task_row: dict[str, Any]
    ) -> None:
        """真正领取任务：这是样本派发明细唯一的写入点。"""
        strategy_no = str(ledger.get("策略编号", ""))
        task_no = str(task_row.get("任务编号", ""))
        timestamp = _now()

        # 任务侧：状态进入标注中并注明关联出样；任务编号、样本范围不改写
        task_row["status"] = "标注中"
        task_row["pending"] = True
        task_row["abnormal"] = False
        task_row["关联策略号"] = strategy_no
        task_row["派发时间"] = timestamp
        task_row["triggered_by"] = f"sample_strata/{strategy_no}"
        if not str(task_row.get("完成进度", "")).strip() or is_placeholder_range(
            task_row.get("完成进度")
        ):
            task_row["完成进度"] = "0%"

        # 账本侧：样本派发明细落到任务，范围可追溯到这次出样
        ledger["派发状态"] = STATUS_DISPATCHED
        ledger["关联任务号"] = task_no
        ledger["任务样本范围"] = str(task_row.get("样本范围", ""))
        ledger["派发时间"] = timestamp
        self._sync_strategy(ledger)

    def _sync_strategy(self, ledger: dict[str, Any]) -> None:
        """把出样动作的派发结果同步回样本分层清单。"""
        strategy = self._find_strategy(int(ledger.get("出样id", 0)))
        if strategy is None:
            return
        if ledger.get("派发状态") == STATUS_DISPATCHED:
            strategy["派发状态"] = STATUS_DISPATCHED
            strategy["关联任务号"] = str(ledger.get("关联任务号", ""))
            strategy["任务样本范围"] = str(ledger.get("任务样本范围", ""))
            strategy["派发时间"] = str(ledger.get("派发时间", ""))
        else:
            strategy["派发状态"] = STATUS_WAITING
            strategy.setdefault("关联任务号", "")
            strategy["任务样本范围"] = ""
            strategy["派发时间"] = ""

    @staticmethod
    def _find_strategy(strategy_id: int) -> dict[str, Any] | None:
        for row in store.rows("sample_strata"):
            if int(row.get("id", 0)) == strategy_id:
                return row
        return None


dispatch_ledger = DispatchLedger()
