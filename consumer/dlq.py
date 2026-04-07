"""
Dead Letter Queue (DLQ)

處理無法正常消費的 poison pill 訊息：
- JSON 解析失敗
- Schema 驗證失敗
- 嚴重遲到的資料
"""

import json
from datetime import datetime, timezone

from confluent_kafka import Producer

from config import KAFKA_BROKER, DLQ_TOPIC

dlq_producer = Producer({"bootstrap.servers": KAFKA_BROKER})


def send_to_dlq(raw_payload: bytes, error_reason: str):
    """將異常資料連同錯誤原因打包送入 Dead Letter Queue"""
    dlq_message = {
        "error_reason": error_reason,
        "original_payload": raw_payload.decode("utf-8", errors="ignore"),
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }
    dlq_producer.produce(
        topic=DLQ_TOPIC,
        value=json.dumps(dlq_message).encode("utf-8"),
    )
    dlq_producer.poll(0)
    print(f"[DLQ] {error_reason}")
