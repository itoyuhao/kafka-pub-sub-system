"""
Fraud Simulator (Async)

模擬盜刷行為，用於測試 consumer 端的 Velocity Check。
使用 await + asyncio.sleep 逐筆發送，模擬真實盜刷的時間節奏。
"""

import asyncio
import json

from confluent_kafka.aio import AIOProducer

from config import (
    TXN_TOPIC, FRAUD_BURST_COUNT, FRAUD_BURST_INTERVAL_SEC,
)
from generators import fake, USERS, generate_transaction


async def simulate_fraud_burst(producer_instance: AIOProducer):
    """
    鎖定一個 user，短時間內連續發送多筆高額交易。

    與 produce_batch 不同，這裡刻意用 await + sleep 逐筆發送，
    而不是用 gather 瞬間全發。原因是 Velocity Check 需要看到
    transaction_time 之間有微小的時間差，才能正確判定為
    「短時間內的連續交易」。
    """
    target_user = fake.random_element(USERS)
    print(f"[FRAUD SIM] 模擬異常交易 → {target_user} "
          f"({FRAUD_BURST_COUNT} 筆 / {FRAUD_BURST_INTERVAL_SEC}s 間隔)")

    for _ in range(FRAUD_BURST_COUNT):
        txn = generate_transaction()
        txn["user_id"] = target_user
        txn["amount"] = round(fake.random.uniform(3000.0, 5000.0), 2)

        await producer_instance.produce(
            topic=TXN_TOPIC,
            key=txn["user_id"].encode("utf-8"),
            value=json.dumps(txn).encode("utf-8"),
        )
        await asyncio.sleep(FRAUD_BURST_INTERVAL_SEC)

    print(f"[FRAUD SIM] 完成，已對 {target_user} 發送 {FRAUD_BURST_COUNT} 筆交易")
