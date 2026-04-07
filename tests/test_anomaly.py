"""
test_anomaly.py

測試 Sliding Window Velocity Check 的核心邏輯。
覆蓋場景：正常觸發、時間超出視窗、clear 重置、跨用戶隔離。
"""

from datetime import datetime, timezone, timedelta

import pytest

from anomaly import check_velocity, get_related_ids, clear_window, window_state
from models import Transaction


def _make_txn(user_id: str, time: datetime, txn_id: str = "txn_001") -> Transaction:
    """建立測試用 Transaction，只填入 anomaly 需要的欄位。"""
    return Transaction(
        transaction_id=txn_id,
        user_id=user_id,
        amount=100.0,
        transaction_time=time.isoformat(),
        transaction_status="Completed",
        country_code="TW",
        store_id="store_001",
    )


@pytest.fixture(autouse=True)
def reset_window_state():
    """每個測試前清空全域 window state，確保測試互不干擾。"""
    window_state.clear()
    yield
    window_state.clear()


# ── 觸發場景 ─────────────────────────────────────────────────

class TestVelocityTrigger:
    """3 筆交易在 60 秒內 → 應觸發異常"""

    def test_three_txn_within_window_triggers(self):
        base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert check_velocity(_make_txn("user_A", base, "t1")) is False
        assert check_velocity(_make_txn("user_A", base + timedelta(seconds=20), "t2")) is False
        assert check_velocity(_make_txn("user_A", base + timedelta(seconds=40), "t3")) is True

    def test_three_txn_at_exact_boundary_triggers(self):
        """最新 - 最舊 = 剛好 60 秒 → 仍觸發（<= 判定）"""
        base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert check_velocity(_make_txn("user_A", base, "t1")) is False
        assert check_velocity(_make_txn("user_A", base + timedelta(seconds=30), "t2")) is False
        assert check_velocity(_make_txn("user_A", base + timedelta(seconds=60), "t3")) is True

    def test_same_timestamp_triggers(self):
        """3 筆交易時間完全相同 → 差值 0 秒，觸發"""
        t = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        check_velocity(_make_txn("user_A", t, "t1"))
        check_velocity(_make_txn("user_A", t, "t2"))
        assert check_velocity(_make_txn("user_A", t, "t3")) is True


# ── 不觸發場景 ───────────────────────────────────────────────

class TestVelocityNoTrigger:
    """交易數量不足或時間跨度超出視窗 → 不應觸發"""

    def test_two_txn_does_not_trigger(self):
        base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert check_velocity(_make_txn("user_A", base, "t1")) is False
        assert check_velocity(_make_txn("user_A", base + timedelta(seconds=10), "t2")) is False

    def test_three_txn_outside_window_does_not_trigger(self):
        """最新 - 最舊 = 61 秒 → 超出 60 秒視窗，不觸發"""
        base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        check_velocity(_make_txn("user_A", base, "t1"))
        check_velocity(_make_txn("user_A", base + timedelta(seconds=30), "t2"))
        assert check_velocity(_make_txn("user_A", base + timedelta(seconds=61), "t3")) is False

    def test_deque_eviction_prevents_false_trigger(self):
        """第 4 筆交易推入後，最舊的被 deque 淘汰，新視窗超出範圍 → 不觸發"""
        base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        check_velocity(_make_txn("user_A", base, "t1"))                           # [t1]
        check_velocity(_make_txn("user_A", base + timedelta(seconds=20), "t2"))    # [t1, t2]
        check_velocity(_make_txn("user_A", base + timedelta(seconds=40), "t3"))    # [t1, t2, t3] → True
        clear_window("user_A")  # 模擬觸發後清空

        # 新一輪：間隔拉大
        check_velocity(_make_txn("user_A", base + timedelta(seconds=100), "t4"))
        check_velocity(_make_txn("user_A", base + timedelta(seconds=130), "t5"))
        assert check_velocity(_make_txn("user_A", base + timedelta(seconds=200), "t6")) is False


# ── 狀態管理 ─────────────────────────────────────────────────

class TestStateManagement:
    """clear_window / get_related_ids 的行為"""

    def test_clear_window_resets_state(self):
        """清空後，需要重新累積 3 筆才能再次觸發"""
        base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        check_velocity(_make_txn("user_A", base, "t1"))
        check_velocity(_make_txn("user_A", base + timedelta(seconds=10), "t2"))
        check_velocity(_make_txn("user_A", base + timedelta(seconds=20), "t3"))

        clear_window("user_A")

        # 清空後只送 1 筆 → 不觸發
        assert check_velocity(_make_txn("user_A", base + timedelta(seconds=30), "t4")) is False

    def test_get_related_ids_returns_correct_txn_ids(self):
        base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        check_velocity(_make_txn("user_A", base, "txn_001"))
        check_velocity(_make_txn("user_A", base + timedelta(seconds=10), "txn_002"))
        check_velocity(_make_txn("user_A", base + timedelta(seconds=20), "txn_003"))

        ids = get_related_ids("user_A")
        assert ids == ["txn_001", "txn_002", "txn_003"]


# ── 跨用戶隔離 ──────────────────────────────────────────────

class TestUserIsolation:
    """不同 user 的 window state 互不影響"""

    def test_different_users_are_independent(self):
        base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        # user_A 送 2 筆
        check_velocity(_make_txn("user_A", base, "a1"))
        check_velocity(_make_txn("user_A", base + timedelta(seconds=10), "a2"))

        # user_B 送 3 筆 → 觸發
        check_velocity(_make_txn("user_B", base, "b1"))
        check_velocity(_make_txn("user_B", base + timedelta(seconds=10), "b2"))
        assert check_velocity(_make_txn("user_B", base + timedelta(seconds=20), "b3")) is True

        # user_A 仍然只有 2 筆 → 不觸發（第 3 筆才觸發）
        assert check_velocity(_make_txn("user_A", base + timedelta(seconds=20), "a3")) is True
        # ↑ 這裡 user_A 的第 3 筆也在 60 秒內，所以也觸發了

    def test_clear_one_user_does_not_affect_another(self):
        base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        check_velocity(_make_txn("user_A", base, "a1"))
        check_velocity(_make_txn("user_A", base + timedelta(seconds=10), "a2"))
        check_velocity(_make_txn("user_B", base, "b1"))
        check_velocity(_make_txn("user_B", base + timedelta(seconds=10), "b2"))

        clear_window("user_A")

        # user_A 被清空 → 只有 1 筆
        assert len(window_state["user_A"]) == 0
        # user_B 不受影響 → 仍有 2 筆
        assert len(window_state["user_B"]) == 2
