"""
Event Generators

產生模擬的交易事件與登入事件。
"""

from datetime import datetime, timezone

from faker import Faker
from ulid import ULID

from config import (
    DOMESTIC_RATIO, NUM_USERS, NUM_STORES,
    DOMESTIC_COUNTRIES, FOREIGN_COUNTRIES,
)

fake = Faker()

USERS = [f"user_{i:04d}" for i in range(1, NUM_USERS + 1)]
STORES = [f"store_{i:03d}" for i in range(1, NUM_STORES + 1)]


def _pick_country() -> str:
    """依據設定的國內外比例隨機選取國家代碼"""
    if fake.random.random() < DOMESTIC_RATIO:
        return fake.random_element(DOMESTIC_COUNTRIES)
    return fake.random_element(FOREIGN_COUNTRIES)


def _now_iso() -> str:
    """回傳當前 UTC 時間的 ISO 8601 格式字串"""
    return datetime.now(timezone.utc).isoformat()


def generate_transaction() -> dict:
    """產生一筆模擬電子支付交易事件"""
    return {
        "transaction_id": str(ULID()),
        "user_id": fake.random_element(USERS),
        "amount": round(fake.random.uniform(10.0, 5000.0), 2),
        "transaction_time": _now_iso(),
        "transaction_status": fake.random_element(
            elements=("SUCCESS", "SUCCESS", "SUCCESS", "FAILED", "PENDING")
        ),
        "country_code": _pick_country(),
        "store_id": fake.random_element(STORES),
    }


def generate_login() -> dict:
    """產生一筆模擬會員登入事件"""
    return {
        "login_id": str(ULID()),
        "user_id": fake.random_element(USERS),
        "login_time": _now_iso(),
        "ip_address": fake.ipv4(),
        "login_status": fake.random_element(
            elements=("SUCCESS", "SUCCESS", "SUCCESS", "FAILED")
        ),
        "country_code": _pick_country(),
    }
