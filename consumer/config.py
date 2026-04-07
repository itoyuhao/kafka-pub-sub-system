"""
Consumer Configuration

所有可調參數集中管理，方便未來透過環境變數覆寫。
"""

import os

# ── Kafka ────────────────────────────────────────────────
KAFKA_BROKER = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TXN_TOPIC = "epay_transactions"
LOGIN_TOPIC = "epay_logins"
DLQ_TOPIC = "epay_dlq"
ALERTS_TOPIC = "epay_account_alerts"

# ── ClickHouse ───────────────────────────────────────────
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))

# ── Velocity Check ───────────────────────────────────────
WINDOW_SIZE_SEC = 60            # 時間視窗：60 秒
WINDOW_TRIGGER_COUNT = 3        # 觸發門檻：3 筆交易

# ── Late Data ────────────────────────────────────────────
LATE_DATA_THRESHOLD_SEC = 300   # 超過 5 分鐘視為遲到資料

# ── Logging ──────────────────────────────────────────────
LOG_EVERY_N_EVENTS = 50         # 每 N 筆印一次進度
