"""
test_models.py

測試 dataclass 作為 schema validation 的行為。
重點：建構成功、缺欄位 TypeError、多餘欄位 TypeError、parsed_time() 轉換。
"""

from datetime import datetime, timezone

import pytest

from models import Transaction, Login


# ── 合法建構的基本 fixture ────────────────────────────────────

VALID_TXN = {
    "transaction_id": "01HWXYZ1234567890ABCDEF",
    "user_id": "user_0001",
    "amount": 150.50,
    "transaction_time": "2026-01-01T12:00:00+00:00",
    "transaction_status": "Completed",
    "country_code": "TW",
    "store_id": "store_001",
}

VALID_LOGIN = {
    "login_id": "01HWXYZ1234567890GHIJKL",
    "user_id": "user_0001",
    "login_time": "2026-01-01T12:00:00+00:00",
    "ip_address": "203.0.113.42",
    "login_status": "Success",
    "country_code": "TW",
}


# ── Transaction 測試 ─────────────────────────────────────────

class TestTransactionConstruction:

    def test_valid_construction(self):
        txn = Transaction(**VALID_TXN)
        assert txn.transaction_id == "01HWXYZ1234567890ABCDEF"
        assert txn.user_id == "user_0001"
        assert txn.amount == 150.50
        assert txn.country_code == "TW"

    def test_missing_required_field_raises_type_error(self):
        """缺少 store_id → TypeError"""
        incomplete = {k: v for k, v in VALID_TXN.items() if k != "store_id"}
        with pytest.raises(TypeError, match="store_id"):
            Transaction(**incomplete)

    def test_missing_multiple_fields_raises_type_error(self):
        """缺少多個欄位 → TypeError"""
        minimal = {"transaction_id": "t1", "user_id": "u1"}
        with pytest.raises(TypeError):
            Transaction(**minimal)

    def test_extra_field_raises_type_error(self):
        """多出未定義的欄位 → TypeError"""
        extra = {**VALID_TXN, "unknown_field": "oops"}
        with pytest.raises(TypeError, match="unknown_field"):
            Transaction(**extra)

    def test_empty_dict_raises_type_error(self):
        """空 dict → TypeError"""
        with pytest.raises(TypeError):
            Transaction(**{})

    def test_parsed_time_returns_datetime(self):
        txn = Transaction(**VALID_TXN)
        result = txn.parsed_time()
        assert isinstance(result, datetime)
        assert result.year == 2026
        assert result.month == 1
        assert result.tzinfo is not None

    def test_parsed_time_with_invalid_iso_raises(self):
        """不合法的 ISO 時間字串 → parsed_time() 拋出 ValueError"""
        bad = {**VALID_TXN, "transaction_time": "not-a-date"}
        txn = Transaction(**bad)
        with pytest.raises(ValueError):
            txn.parsed_time()


# ── Login 測試 ───────────────────────────────────────────────

class TestLoginConstruction:

    def test_valid_construction(self):
        login = Login(**VALID_LOGIN)
        assert login.login_id == "01HWXYZ1234567890GHIJKL"
        assert login.ip_address == "203.0.113.42"
        assert login.login_status == "Success"

    def test_missing_required_field_raises_type_error(self):
        """缺少 ip_address → TypeError"""
        incomplete = {k: v for k, v in VALID_LOGIN.items() if k != "ip_address"}
        with pytest.raises(TypeError, match="ip_address"):
            Login(**incomplete)

    def test_extra_field_raises_type_error(self):
        extra = {**VALID_LOGIN, "device_type": "mobile"}
        with pytest.raises(TypeError, match="device_type"):
            Login(**extra)

    def test_parsed_time_returns_datetime(self):
        login = Login(**VALID_LOGIN)
        result = login.parsed_time()
        assert isinstance(result, datetime)
        assert result.hour == 12


# ── 型別寬容度（Python dataclass 的特性）─────────────────────

class TestTypeCoercion:
    """
    Python @dataclass 不做型別檢查（不像 Pydantic）。
    這組測試記錄了這個已知的 trade-off。
    """

    def test_amount_as_string_is_accepted(self):
        """
        amount 定義為 float，但傳入 str 不會報錯。
        這是 dataclass 的已知限制，非 bug。
        若需嚴格型別驗證，需升級至 Pydantic。
        """
        data = {**VALID_TXN, "amount": "not_a_number"}
        txn = Transaction(**data)
        assert txn.amount == "not_a_number"  # 接受任意型別

    def test_none_values_are_accepted(self):
        """欄位值為 None 不會報錯（只要欄位名稱齊全）"""
        data = {**VALID_TXN, "amount": None, "country_code": None}
        txn = Transaction(**data)
        assert txn.amount is None
