"""
ClickHouse Writers

負責將驗證過的事件寫入 ClickHouse fact tables。
"""

from datetime import datetime, timezone

import clickhouse_connect

from config import CLICKHOUSE_HOST, CLICKHOUSE_PORT
from models import Transaction, Login

print(f"Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}...")
ch_client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
)


def insert_transaction(txn: Transaction):
    """將一筆交易事件寫入 fact_transactions"""
    ch_client.insert(
        table="fact_transactions",
        data=[[
            txn.transaction_id,
            txn.user_id,
            txn.amount,
            txn.parsed_time(),
            txn.transaction_status,
            txn.country_code,
            txn.store_id,
        ]],
        column_names=[
            "transaction_id", "user_id", "amount",
            "transaction_time", "transaction_status",
            "country_code", "store_id",
        ],
    )


def insert_login(login: Login):
    """將一筆登入事件寫入 fact_logins"""
    ch_client.insert(
        table="fact_logins",
        data=[[
            login.login_id,
            login.user_id,
            login.parsed_time(),
            login.ip_address,
            login.login_status,
            login.country_code,
        ]],
        column_names=[
            "login_id", "user_id", "login_time",
            "ip_address", "login_status", "country_code",
        ],
    )


def insert_status_change(user_id: str, reason: str, related_ids: list[str]):
    """將一筆帳號凍結事件寫入 fact_user_status_changes"""
    ch_client.insert(
        table="fact_user_status_changes",
        data=[[
            user_id,
            "Suspended",
            reason,
            related_ids,
            datetime.now(timezone.utc),
        ]],
        column_names=[
            "user_id", "new_status", "reason",
            "related_transaction_ids", "valid_from",
        ],
    )
