"""
Anomaly Detection

Sliding Window (Velocity Check) 異常偵測邏輯。
以 deque 維護每位使用者的近期交易時間戳，
當同一使用者在短時間內交易次數超過門檻時觸發異常。
"""

from collections import defaultdict, deque

from config import WINDOW_SIZE_SEC, WINDOW_TRIGGER_COUNT
from models import Transaction

# In-Memory State：key = user_id, value = deque of (unix_ts, transaction_id)
window_state: dict[str, deque] = defaultdict(
    lambda: deque(maxlen=WINDOW_TRIGGER_COUNT)
)


def check_velocity(txn: Transaction) -> bool:
    """
    Sliding Window 異常偵測。

    將交易的 (timestamp, transaction_id) 加入該 user 的 deque。
    當 deque 滿了（達到 WINDOW_TRIGGER_COUNT 筆），檢查最新與最舊
    的時間差是否在 WINDOW_SIZE_SEC 以內。

    Returns:
        True 如果觸發異常，False 如果正常。
    """
    window_state[txn.user_id].append((
        txn.parsed_time().timestamp(),
        txn.transaction_id,
    ))

    if len(window_state[txn.user_id]) == WINDOW_TRIGGER_COUNT:
        oldest_time = window_state[txn.user_id][0][0]
        newest_time = window_state[txn.user_id][-1][0]
        if newest_time - oldest_time <= WINDOW_SIZE_SEC:
            return True

    return False


def get_related_ids(user_id: str) -> list[str]:
    """取得觸發異常的相關交易 ID 清單"""
    return [entry[1] for entry in window_state[user_id]]


def clear_window(user_id: str):
    """清空該使用者的滑動視窗，避免重複觸發"""
    window_state[user_id].clear()
