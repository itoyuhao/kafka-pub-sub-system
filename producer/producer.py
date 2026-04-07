"""
E-Payment Data Producer (Async)

使用 AIOProducer 以非同步方式批次發送模擬的電子支付交易事件與會員登入事件。
以 user_id 作為 Partition Key 送入對應的 Kafka Topic。
"""

import asyncio
import json

from confluent_kafka.aio import AIOProducer

from config import (
    KAFKA_BROKER, TXN_TOPIC, LOGIN_TOPIC,
    BATCH_SIZE, BATCH_INTERVAL_SEC, LOGIN_EVERY_N_BATCHES,
    LOG_EVERY_N_EVENTS, FRAUD_EVERY_N_BATCHES,
)
from generators import generate_transaction, generate_login
from fraud_simulator import simulate_fraud_burst


def _serialize(event: dict) -> bytes:
    """將事件 dict 序列化為 UTF-8 bytes"""
    return json.dumps(event).encode("utf-8")


async def produce_batch(producer: AIOProducer, batch_size: int) -> int:
    """
    批次產生交易事件並同時發送。
    利用 asyncio.gather 並行等待所有 broker ack。

    Returns:
        實際成功發送的筆數。
    """
    tasks = []
    for _ in range(batch_size):
        txn = generate_transaction()
        task = producer.produce(
            topic=TXN_TOPIC,
            key=txn["user_id"].encode("utf-8"),
            value=_serialize(txn),
        )
        tasks.append(task)

    await asyncio.gather(*tasks)
    return len(tasks)


async def produce_login_batch(producer: AIOProducer, batch_size: int) -> int:
    """批次產生登入事件並同時發送。"""
    tasks = []
    for _ in range(batch_size):
        login = generate_login()
        task = producer.produce(
            topic=LOGIN_TOPIC,
            key=login["user_id"].encode("utf-8"),
            value=_serialize(login),
        )
        tasks.append(task)

    await asyncio.gather(*tasks)
    return len(tasks)


async def main():
    producer = AIOProducer({"bootstrap.servers": KAFKA_BROKER})
    print(f"Producer 啟動 (Async mode)，連線至 {KAFKA_BROKER}")
    print(f"Topics: {TXN_TOPIC}, {LOGIN_TOPIC}")
    print(f"Batch size: {BATCH_SIZE}, Interval: {BATCH_INTERVAL_SEC}s")

    total_txn = 0
    batch_count = 0

    try:
        while True:
            # 每一輪批次發送交易事件
            sent = await produce_batch(producer, BATCH_SIZE)
            total_txn += sent
            batch_count += 1

            # 每 N 批次搭配一批登入事件
            if batch_count % LOGIN_EVERY_N_BATCHES == 0:
                await produce_login_batch(producer, BATCH_SIZE)

            # 定期觸發 fraud burst 模擬
            if batch_count % FRAUD_EVERY_N_BATCHES == 0:
                await simulate_fraud_burst(producer)

            if total_txn % LOG_EVERY_N_EVENTS == 0:
                print(f"已發送 {total_txn} 筆交易事件 (batch #{batch_count})")

            await asyncio.sleep(BATCH_INTERVAL_SEC)

    except KeyboardInterrupt:
        print("Producer 關閉中...")
    finally:
        await producer.close()
        print("Producer 已關閉")


if __name__ == "__main__":
    asyncio.run(main())
