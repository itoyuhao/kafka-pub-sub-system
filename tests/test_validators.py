"""
test_validators.py

測試 is_late_data() 的邊界條件。
使用 freezegun 凍結時間以避免測試結果受執行時間影響。
"""

from datetime import datetime, timezone, timedelta
from unittest.mock import patch

import pytest

from validators import is_late_data


def _freeze_now(fake_now: datetime):
    """回傳一個 mock patch，將 validators.datetime.now 凍結在指定時間。"""
    return patch("validators.datetime", wraps=datetime, **{
        "now.return_value": fake_now,
        "fromisoformat": datetime.fromisoformat,
    })


NOW = datetime(2026, 1, 1, 12, 5, 0, tzinfo=timezone.utc)  # 固定的「現在」


class TestLateData:
    """LATE_DATA_THRESHOLD_SEC = 300 (5 分鐘)"""

    def test_fresh_data_is_not_late(self):
        """事件時間 = 現在 → 延遲 0 秒，不是 late"""
        event_time = NOW.isoformat()
        with _freeze_now(NOW):
            assert is_late_data(event_time) is False

    def test_within_threshold_is_not_late(self):
        """事件時間比現在早 299 秒 → 仍在容忍範圍內"""
        event_time = (NOW - timedelta(seconds=299)).isoformat()
        with _freeze_now(NOW):
            assert is_late_data(event_time) is False

    def test_at_exact_threshold_is_not_late(self):
        """事件時間比現在早剛好 300 秒 → 等於門檻，不觸發（> 而非 >=）"""
        event_time = (NOW - timedelta(seconds=300)).isoformat()
        with _freeze_now(NOW):
            assert is_late_data(event_time) is False

    def test_one_second_over_threshold_is_late(self):
        """事件時間比現在早 301 秒 → 超過門檻，判定為 late"""
        event_time = (NOW - timedelta(seconds=301)).isoformat()
        with _freeze_now(NOW):
            assert is_late_data(event_time) is True

    def test_very_old_data_is_late(self):
        """事件時間比現在早 1 小時 → 明顯 late"""
        event_time = (NOW - timedelta(hours=1)).isoformat()
        with _freeze_now(NOW):
            assert is_late_data(event_time) is True

    def test_future_event_is_not_late(self):
        """事件時間在未來 → 延遲為負數，不是 late"""
        event_time = (NOW + timedelta(seconds=60)).isoformat()
        with _freeze_now(NOW):
            assert is_late_data(event_time) is False
