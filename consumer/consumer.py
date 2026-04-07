"""
E-Payment Fraud Detection Consumer

從 Kafka 消費交易與登入事件，執行：
1. 資料驗證（dataclass 結構驗證 + 業務邏輯驗證）
2. 寫入 ClickHouse fact tables
3. Sliding Window 異常偵測 (Velocity Check)
4. 觸發帳號凍結：寫入 SCD2 底表 + 發送 Kafka alert
"""

import json
import time

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient

from config import (
    KAFKA_BROKER, TXN_TOPIC, LOGIN_TOPIC,
    LOG_EVERY_N_EVENTS,
)
from models import Transaction, Login
from dlq import send_to_dlq
from validators import is_late_data
from writers import insert_transaction, insert_login, insert_status_change
from anomaly import check_velocity, get_related_ids, clear_window
from alerts import send_account_alert


# ── Wait for Topics ──────────────────────────────────────

def wait_for_topics(broker: str, topics: list[str], timeout: int = 60):
    """在開始消費前，確認所有 topic 都已存在於 Kafka cluster"""
    admin = AdminClient({"bootstrap.servers": broker})
    required = set(topics)
    deadline = time.time() + timeout

    while time.time() < deadline:
        metadata = admin.list_topics(timeout=5)
        existing = set(metadata.topics.keys())
        missing = required - existing
        if not missing:
            print(f"所有 topic 已就緒: {topics}")
            return
        print(f"等待 topic 建立: {missing}")
        time.sleep(2)

    raise RuntimeError(f"Timeout: topics {missing} 未在 {timeout} 秒內建立")


# ── Main Loop ────────────────────────────────────────────

def main():
    subscribed_topics = [TXN_TOPIC, LOGIN_TOPIC]
    wait_for_topics(KAFKA_BROKER, subscribed_topics)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "fraud-detection-group",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(subscribed_topics)

    print("Consumer 啟動完成")
    print(f"  Kafka: {KAFKA_BROKER}")
    print(f"  Topics: {subscribed_topics}")
    print("開始消費事件...")

    txn_count = 0
    login_count = 0
    anomaly_count = 0

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"[ERROR] Kafka: {msg.error()}")
            continue

        topic = msg.topic()
        raw_payload = msg.value()

        # Step 1: JSON 解析
        try:
            data = json.loads(raw_payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            send_to_dlq(raw_payload, f"JSON parse error: {e}")
            continue

        # Step 2: Dataclass 結構驗證 + 分流處理
        if topic == TXN_TOPIC:
            try:
                txn = Transaction(**data)
            except TypeError as e:
                send_to_dlq(raw_payload, f"Schema error: {e}")
                continue

            insert_transaction(txn)
            txn_count += 1

            # Late Data 檢查：遲到資料不進 Velocity Check
            if is_late_data(txn.transaction_time):
                send_to_dlq(raw_payload, "Late data: exceeded threshold")
                continue

            # Velocity Check
            if check_velocity(txn):
                related_ids = get_related_ids(txn.user_id)
                reason = "High Frequency Transactions"

                insert_status_change(
                    user_id=txn.user_id,
                    reason=reason,
                    related_ids=related_ids,
                )
                send_account_alert(
                    user_id=txn.user_id,
                    reason=reason,
                    related_ids=related_ids,
                )

                anomaly_count += 1
                print(f"異常交易帳號: {txn.user_id} "
                      f"(相關交易: {related_ids})")
                clear_window(txn.user_id)

            if txn_count % LOG_EVERY_N_EVENTS == 0:
                print(f"交易: {txn_count} 筆 | 異常: {anomaly_count} 筆")

        elif topic == LOGIN_TOPIC:
            try:
                login = Login(**data)
            except TypeError as e:
                send_to_dlq(raw_payload, f"Schema error: {e}")
                continue

            insert_login(login)
            login_count += 1

            if login_count % LOG_EVERY_N_EVENTS == 0:
                print(f"登入: {login_count} 筆")


if __name__ == "__main__":
    main()
