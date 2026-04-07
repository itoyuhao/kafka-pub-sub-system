"""
Account Alert Publisher

當 Velocity Check 偵測到異常交易時，將帳號凍結事件發送到
epay_account_alerts topic，供下游服務即時消費：
- 交易閘道：攔截該使用者的後續交易
- 通知服務：發送 SMS / Email 通知用戶
- 風控後台：推播 alert 給審核人員

與 writers.insert_status_change 的差異：
- insert_status_change → 寫入 ClickHouse，用於歷史分析（離線）
- send_account_alert  → 發送 Kafka event，用於即時反應（線上）
"""

import json
from datetime import datetime, timezone

from confluent_kafka import Producer

from config import KAFKA_BROKER, ALERTS_TOPIC

alerts_producer = Producer({"bootstrap.servers": KAFKA_BROKER})


def send_account_alert(user_id: str, reason: str, related_ids: list[str]):
    """將帳號凍結事件發送到 alerts topic，供下游服務即時消費。"""
    alert_event = {
        "user_id": user_id,
        "alert_type": "ACCOUNT_SUSPENDED",
        "reason": reason,
        "related_transaction_ids": related_ids,
        "triggered_at": datetime.now(timezone.utc).isoformat(),
    }
    alerts_producer.produce(
        topic=ALERTS_TOPIC,
        key=user_id.encode("utf-8"),
        value=json.dumps(alert_event).encode("utf-8"),
    )
    alerts_producer.poll(0)
    print(f"[ALERT] 已發送帳號凍結通知: {user_id}")
