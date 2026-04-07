"""
Event Data Models

定義所有事件的資料結構，提供：
- 明確的欄位定義與型別標註
- 結構驗證（缺少欄位時 TypeError）
- IDE autocomplete 與靜態分析支援
"""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class Transaction:
    """電子支付交易事件"""
    transaction_id: str
    user_id: str
    amount: float
    transaction_time: str
    transaction_status: str
    country_code: str
    store_id: str

    def parsed_time(self) -> datetime:
        """將 ISO 8601 字串轉為 datetime 物件"""
        return datetime.fromisoformat(self.transaction_time)


@dataclass
class Login:
    """會員登入事件"""
    login_id: str
    user_id: str
    login_time: str
    ip_address: str
    login_status: str
    country_code: str

    def parsed_time(self) -> datetime:
        """將 ISO 8601 字串轉為 datetime 物件"""
        return datetime.fromisoformat(self.login_time)
