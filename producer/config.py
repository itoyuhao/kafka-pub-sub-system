"""
Producer Configuration

所有可調參數集中管理，方便未來透過環境變數覆寫。
"""

import os

# ── Kafka ────────────────────────────────────────────────
KAFKA_BROKER = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TXN_TOPIC = "epay_transactions"
LOGIN_TOPIC = "epay_logins"

# ── Async Batch Settings ─────────────────────────────────
BATCH_SIZE = 10                     # 每批次同時發送的交易筆數
BATCH_INTERVAL_SEC = 1.0            # 每批次之間的間隔
LOGIN_EVERY_N_BATCHES = 3           # 每 N 批次搭配一批登入事件
LOG_EVERY_N_EVENTS = 100            # 每 N 筆交易印一次進度

# ── Data Generation ──────────────────────────────────────
DOMESTIC_RATIO = 0.85               # 國內交易佔比
NUM_USERS = 1000
NUM_STORES = 100
DOMESTIC_COUNTRIES = ["TW"]
FOREIGN_COUNTRIES = ["US", "JP", "SG", "GB", "NG"]

# ── Fraud Simulation ─────────────────────────────────────
FRAUD_EVERY_N_BATCHES = 10          # 每 N 批次觸發一次異常模擬
FRAUD_BURST_COUNT = 4               # 一次 burst 連發筆數（> Velocity 門檻）
FRAUD_BURST_INTERVAL_SEC = 0.5      # burst 內每筆間隔（秒），模擬真實盜刷節奏
