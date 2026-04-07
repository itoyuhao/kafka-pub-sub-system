"""
Business Logic Validators

負責業務層級的資料驗證（非結構驗證，結構驗證由 models.py dataclass 處理）。
"""

from datetime import datetime, timezone

from config import LATE_DATA_THRESHOLD_SEC


def is_late_data(event_time_iso: str) -> bool:
    """檢查事件時間是否超過容忍值，判定為遲到資料"""
    event_time = datetime.fromisoformat(event_time_iso)
    now = datetime.now(timezone.utc)
    delay_sec = (now - event_time).total_seconds()
    return delay_sec > LATE_DATA_THRESHOLD_SEC
