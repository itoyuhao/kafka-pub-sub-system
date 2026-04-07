-- ============================================================
-- E-Payment Fraud Detection — ClickHouse DDL
-- ============================================================
-- 資料分層：
--   Fact Tables (Append-only):
--     fact_transactions          : 即時交易紀錄
--     fact_logins                : 即時登入紀錄
--     fact_user_status_changes   : 會員狀態變更日誌 (SCD2 底表)
--   Dimension Views:
--     view_dim_user_status       : SCD2 維度視圖 (LEAD() 動態計算 valid_to)
--   Analytical Views (Dashboard-Ready):
--     view_user_transaction_summary  : 使用者交易行為概覽
--     view_user_login_countries      : 使用者登入國家分布 (異地登入風險)
--     view_hourly_transaction_stats  : 每小時交易趨勢統計
--     view_country_transaction_stats : 國家維度交易統計 (跨國交易風險)
-- ============================================================

-- 1. 即時交易紀錄表
CREATE TABLE IF NOT EXISTS fact_transactions
(
    transaction_id      String,
    user_id             String,
    amount              Decimal64(2),
    transaction_time    DateTime64(3, 'UTC'),
    transaction_status  String,
    country_code        String,
    store_id            String,
    _inserted_at        DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = MergeTree()
ORDER BY (user_id, transaction_time);


-- 2. 即時登入紀錄表
CREATE TABLE IF NOT EXISTS fact_logins
(
    login_id            String,
    user_id             String,
    login_time          DateTime64(3, 'UTC'),
    ip_address          String,
    login_status        String,
    country_code        String,
    _inserted_at        DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = MergeTree()
ORDER BY (user_id, login_time);


-- 3. 會員狀態變更日誌 (SCD2 底表)
CREATE TABLE IF NOT EXISTS fact_user_status_changes
(
    user_id                  String,
    new_status               String,         -- Active / Suspended
    reason                   String,         -- e.g. High Frequency Transactions
    related_transaction_ids  Array(String),   -- 觸發本次異常的交易 ID 清單
    valid_from               DateTime64(3, 'UTC'),
    _inserted_at             DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = MergeTree()
ORDER BY (user_id, valid_from);


-- 4. SCD2 維度視圖：動態計算 valid_to 與 is_current
CREATE VIEW IF NOT EXISTS view_dim_user_status AS
SELECT
    user_id,
    new_status AS account_status,
    reason,
    valid_from,
    LEAD(valid_from, 1, toDateTime64('9999-12-31 23:59:59.999', 3, 'UTC'))
        OVER (PARTITION BY user_id ORDER BY valid_from)
        AS valid_to,
    if(
        valid_from = max(valid_from) OVER (PARTITION BY user_id),
        1, 0
    ) AS is_current
FROM fact_user_status_changes;


-- ============================================================
-- Analytical Views (Dashboard-Ready)
-- ============================================================

-- 5. 使用者交易摘要：每位使用者的交易行為概覽
CREATE VIEW IF NOT EXISTS view_user_transaction_summary AS
SELECT
    user_id,
    count()                                            AS total_transactions,
    sum(amount)                                        AS total_amount,
    round(avg(amount), 2)                              AS avg_amount,
    max(amount)                                        AS max_amount,
    round(countIf(transaction_status = 'SUCCESS')
        / count() * 100, 2)                            AS success_rate_pct,
    min(transaction_time)                              AS first_transaction_at,
    max(transaction_time)                              AS last_transaction_at
FROM fact_transactions
GROUP BY user_id;


-- 6. 使用者登入國家分布：識別異地登入風險
CREATE VIEW IF NOT EXISTS view_user_login_countries AS
SELECT
    user_id,
    count()                                            AS total_logins,
    uniq(country_code)                                 AS distinct_countries,
    groupUniqArray(country_code)                       AS country_list,
    round(countIf(login_status = 'FAILED')
        / count() * 100, 2)                            AS login_failure_rate_pct,
    max(login_time)                                    AS last_login_at
FROM fact_logins
GROUP BY user_id;


-- 7. 每小時交易統計：時間維度趨勢分析
CREATE VIEW IF NOT EXISTS view_hourly_transaction_stats AS
SELECT
    toStartOfHour(transaction_time)                    AS hour,
    count()                                            AS transaction_count,
    sum(amount)                                        AS total_amount,
    uniq(user_id)                                      AS unique_users,
    countIf(transaction_status = 'FAILED')             AS failed_count
FROM fact_transactions
GROUP BY hour
ORDER BY hour;


-- 8. 國家維度交易統計：識別跨國交易風險與地域分布
CREATE VIEW IF NOT EXISTS view_country_transaction_stats AS
SELECT
    country_code,
    count()                                            AS transaction_count,
    sum(amount)                                        AS total_amount,
    round(avg(amount), 2)                              AS avg_amount,
    uniq(user_id)                                      AS unique_users,
    round(countIf(transaction_status = 'FAILED')
        / count() * 100, 2)                            AS failure_rate_pct
FROM fact_transactions
GROUP BY country_code;
