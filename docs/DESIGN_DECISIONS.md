# Design Decisions

本文件記錄系統開發過程中的各項設計討論與選擇邏輯，供面試討論與後續迭代參考。

---

## 1. Infrastructure

### 1.1 Kafka KRaft Mode（無 ZooKeeper）

**選擇**：使用 KRaft 模式，單一節點同時擔任 broker 和 controller。

**理由**：KRaft 是 Kafka 3.3+ 的正式 GA 功能，不再需要額外維護 ZooKeeper cluster。對 demo 規模的系統來說，少一個服務意味著更簡潔的 docker-compose、更快的啟動速度、更少的維運負擔。面試時可說明「生產環境會使用多節點 KRaft cluster 以實現 fault tolerance」。

**注意事項**：`CLUSTER_ID` 必須是合法的 base64-encoded UUID，不能使用任意字串。

### 1.2 kafka-init One-Shot Container

**選擇**：用獨立的一次性容器建立 topics，而非讓 producer/consumer 自動建立。

**理由**：Separation of Concerns。Topic 的建立屬於基礎設施的初始化，不應該耦合在應用程式邏輯裡。這樣做的好處是 partition 數量、replication factor 等設定集中管理在一個地方，而不是散落在各個 application 的程式碼中。

**搭配決策**：在 Kafka broker 上設定 `KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'false'`，禁止自動建立 topic。這確保所有 topic 的 partition 數量完全由 kafka-init 控制，避免 producer 意外觸發 auto-creation 導致 partition 數量不符合預期。

### 1.3 Healthcheck + depends_on 啟動順序

**選擇**：使用 Docker healthcheck 搭配 `condition: service_healthy` / `service_completed_successfully`。

**理由**：確保服務啟動的正確順序。Consumer 必須等 ClickHouse 和 kafka-init 都就緒後才啟動，否則會遇到連線失敗或 topic 不存在的問題。單純的 `depends_on` 只保證容器啟動順序，不保證服務真正可用。

**啟動鏈**：kafka (healthy) → kafka-init (completed) → producer / consumer，clickhouse (healthy) → consumer。

### 1.4 Named Volumes

**選擇**：Kafka 和 ClickHouse 使用 named volumes（`kafka_data`、`clickhouse_data`）。

**理由**：Named volume 的生命週期獨立於容器，`docker compose down` 不會刪除資料，方便開發時重啟服務而不遺失已有的 messages 和 table data。需要完全重置時使用 `docker compose down -v`。

**取捨**：Named volume 的缺點是清理時需要刻意加 `-v`，容易忘記而留下舊的 topic metadata（例如 partition 數量不符預期的問題就是因此產生的）。

### 1.5 ClickHouse 版本與認證

**選擇**：使用 `clickhouse/clickhouse-server:26.3.3`，設定 `CLICKHOUSE_PASSWORD: ""` 允許無密碼存取。

**理由**：ClickHouse v26.3+ 預設要求密碼驗證。Demo 環境為求簡便使用空密碼，但生產環境應設定強密碼並搭配網路隔離。

---

## 2. Data Modeling

### 2.1 ULID vs UUID

**選擇**：使用 ULID（Universally Unique Lexicographically Sortable Identifier）作為 transaction_id 和 login_id。

**理由**：UUID v4 是完全隨機的，作為 MergeTree 的排序鍵會導致大量隨機 I/O，影響 insert 和 query 效能。ULID 的前 48 bits 是毫秒級時間戳，保證了字典序（lexicographic order）與時間順序一致。這讓 ClickHouse 的 MergeTree 在按時間範圍查詢時可以高效地跳過不相關的 data parts。

**未考慮的替代方案**：自增 sequential ID。雖然排序效能最佳，但在分散式環境中需要中心化的 ID 生成器，增加了架構複雜度。ULID 不需要協調即可保證唯一性和排序性。

### 2.2 Decimal64(2) vs Float64

**選擇**：金額欄位使用 `Decimal64(2)` 而非 `Float64`。

**理由**：Float 的浮點數精度問題會導致金額計算出現如 `99.9999999997` 的結果。在金融場景中，金額必須是精確的定點數。`Decimal64(2)` 保證小數點後 2 位的精確度，避免累積誤差。

### 2.3 Partition Key = user_id

**選擇**：Producer 以 `user_id` 作為 Kafka message key（即 partition key）。

**理由**：Kafka 保證同一 partition 內的 message 是有序的。以 `user_id` 為 key，同一使用者的所有交易事件會被分配到同一個 partition，確保 consumer 端的 Velocity Check（sliding window）能看到該使用者的完整時間序列。

**對 Scalability 的影響**：當 consumer 水平擴展時（多個 instance 共用同一 group.id），Kafka 會將不同 partition 分配給不同 consumer instance。因為同一 user 的事件一定在同一 partition，所以 Velocity Check 的 in-memory state 不需要跨 instance 共享，避免了引入 Redis 等外部 state store 的複雜度。

### 2.4 SCD2 實作方式

**選擇**：Append-only 底表（`fact_user_status_changes`）搭配 View（`view_dim_user_status`）動態計算 `valid_to` 和 `is_current`。

**理由**：ClickHouse 的 MergeTree 是 append-optimized 的，不擅長 UPDATE 操作。傳統 SCD2 需要在新紀錄寫入時更新前一筆的 `valid_to`，這在 ClickHouse 中效能很差。改用 append-only 底表 + LEAD() window function 的 view，在查詢時動態計算出 `valid_to`，完全避免了 UPDATE。

**`valid_to` 計算邏輯**：`LEAD(valid_from, 1, '9999-12-31')` — 取同一 user 下一筆紀錄的 `valid_from` 作為當前紀錄的 `valid_to`，最後一筆（最新狀態）預設為遙遠未來。

### 2.5 Array(String) 儲存關聯交易 ID

**選擇**：`fact_user_status_changes.related_transaction_ids` 使用 `Array(String)` 而非 join table。

**理由**：Velocity Check 觸發時需要記錄「哪幾筆交易導致了這次凍結」。ClickHouse 原生支援 Array 型別，省去了建立關聯表和做 JOIN 的開銷。在 OLAP 場景中，反正規化（denormalization）是常見且推薦的做法。

---

## 3. Stream Processing

### 3.1 Sliding Window Velocity Check

**選擇**：使用 Python `collections.deque`（maxlen=3）維護每位使用者的近期交易紀錄，當 3 筆交易的時間跨度在 60 秒以內時觸發異常。

**理由**：deque 的 maxlen 自動淘汰舊資料，不需要手動清理。以 in-memory state 實作的好處是延遲極低（微秒級），缺點是 consumer 重啟時 state 會遺失。對 demo 來說這是可接受的取捨；生產環境可考慮 Kafka Streams 的 state store 或 Flink 的 window 機制。

**觸發後行為**：異常觸發後清空該 user 的 window（`clear_window()`），避免同一批交易重複觸發。

### 3.2 Dead Letter Queue (DLQ)

**選擇**：將無法正常處理的訊息（poison pills）送入專用的 `epay_dlq` topic。

**進入 DLQ 的三種情況**：
1. **JSON 解析失敗**：message 不是合法 JSON（捕捉 `json.JSONDecodeError` / `UnicodeDecodeError`）
2. **Schema 驗證失敗**：`Transaction(**data)` 或 `Login(**data)` 建構時拋出 `TypeError`（缺少必要欄位或有多餘欄位）
3. **嚴重遲到的資料**：`transaction_time` 與 consumer 處理時間相差超過 5 分鐘

**DLQ 訊息格式**：包含 `error_reason`（錯誤原因）、`original_payload`（原始資料）、`failed_at`（失敗時間），方便事後 debug。

### 3.3 Late Data 處理策略

**選擇**：遲到超過 5 分鐘的交易事件，仍然寫入 `fact_transactions`（保留完整歷史），但不進入 Velocity Check，同時送一份到 DLQ 留底。

**理由**：遲到的資料如果進入 sliding window，會污染時間序列的連續性，可能導致誤判（例如把 5 分鐘前的交易和剛發生的交易放在同一個 60 秒視窗內比較）。但原始資料仍然有分析價值，所以照寫 fact table，只是排除在 real-time detection 之外。

### 3.5 Event-Driven Account Alerts（epay_account_alerts）

**選擇**：Velocity Check 觸發帳號凍結時，除了寫入 ClickHouse，同時發送一則事件到獨立的 `epay_account_alerts` Kafka topic。

**理由**：`fact_user_status_changes` 是分析用途的歷史紀錄，但「凍結帳號」是一個需要即時通知下游服務的業務動作。例如 Gateway Service 需要即時阻擋該帳號的後續交易、Notification Service 需要發送 SMS/email 給使用者。這些下游服務不應該去輪詢 ClickHouse，而是透過 Kafka 的 pub/sub 機制被動接收通知。

**與 DLQ 的差異**：DLQ 是「處理失敗的垃圾桶」，alerts 是「正常業務流程的事件通知」。兩者用途完全不同，不應混用。DLQ 的 consumer 是 ops 團隊做 debug，alerts 的 consumer 是其他業務服務。

**Alert 訊息格式**：包含 `user_id`、`alert_type`（如 `ACCOUNT_SUSPENDED`）、`reason`、`related_transaction_ids`、`triggered_at`。格式設計上考慮了下游服務的通用需求。

**獨立 Producer**：`alerts.py` 使用自己的 `Producer` instance（sync），而非共用 DLQ 的 producer。職責隔離，避免一方的 flush/error 影響另一方。

### 3.4 單一 Consumer 消費多個 Topics

**選擇**：一個 consumer 同時訂閱 `epay_transactions` 和 `epay_logins`，在 main loop 中用 `if topic == TXN_TOPIC` 分流。

**理由**：目前兩個 topic 的處理邏輯量都不大，合在一起降低了維運複雜度。如果未來 login 的處理邏輯變複雜（例如加入 IP 異常偵測），可以拆成獨立的 consumer group。

---

## 4. Fraud Simulation

### 4.1 Producer 端 Fraud Burst 注入

**選擇**：每 30 筆正常交易後，鎖定一個隨機 user，連續快速發送 4 筆高額交易（0.5 秒間隔）。

**為什麼不靠自然碰撞**：1000 個 user、每秒 2 筆交易，同一 user 在 60 秒內自然出現 3 筆的機率極低（(1/1000)^2 量級），demo 時幾乎看不到異常觸發。

**FRAUD_BURST_COUNT = 4**：設成 4 而非剛好 3，是因為 Velocity Check 的 deque maxlen 是 3，需要 deque 滿了才檢查。發 4 筆確保 window 內一定有足夠的交易觸發判定。

**設計原則**：fraud simulation 是獨立的模組（`fraud_simulator.py`），不影響正常交易的生成邏輯和資料分佈。面試時可解釋「這是 QA 測試中常見的 pattern injection」。

---

## 5. Analytics Layer

### 5.1 Regular View vs Materialized View

**選擇**：所有 analytical views 使用 regular view。

**理由**：ClickHouse 是 columnar OLAP 引擎，聚合查詢是其核心強項。在千萬級以下的資料量，regular view 的即時計算完全沒有效能問題。Regular view 的優勢是永遠與底表同步，不需要額外的更新機制。

**何時升級為 Materialized View**：當底表資料量達到億級、且 dashboard 需要高頻刷新時。ClickHouse 的 Materialized View 在 INSERT 時觸發增量計算（非定期重建），搭配 `AggregatingMergeTree` 引擎可實現 incremental aggregation。但需要注意 `avg()` 等函數不能直接增量計算，需改存 `sum` + `count` 再在查詢時相除。

### 5.2 Analytical Views 設計

**分析維度覆蓋**：

| View | 維度 | 用途 |
|------|------|------|
| `view_dim_user_status` | User × Time | 帳號狀態歷程（SCD2），追蹤凍結/解凍時間線 |
| `view_user_transaction_summary` | User | 交易行為概覽：次數、金額、成功率 |
| `view_user_login_countries` | User × Geography | 異地登入風險：多國登入、失敗率 |
| `view_hourly_transaction_stats` | Time | 交易量時間趨勢，識別異常時段 |
| `view_country_transaction_stats` | Geography | 跨國交易分布與失敗率 |

### 5.3 Login 表的定位

**討論過程**：最初 login 表只是被 consumer 收集但沒有實際用途，討論後決定保留它，但改變定位 — 不用於 real-time detection（避免跨 topic correlation 的複雜度），而是用於 analytical layer（`view_user_login_countries`）。

**設計邏輯**：Real-time layer 負責高頻異常的即時偵測（Velocity Check），Analytical layer 負責更複雜的跨維度分析（異地登入）。兩者職責分離。

---

## 6. Code Architecture

### 6.1 模組拆分

**Producer 結構**：

| 模組 | 職責 |
|------|------|
| `config.py` | 集中管理所有常數與環境變數 |
| `generators.py` | 事件資料生成（transaction、login） |
| `fraud_simulator.py` | 異常交易模擬注入 |
| `producer.py` | Main loop 編排邏輯 |

**Consumer 結構**：

| 模組 | 職責 |
|------|------|
| `config.py` | 集中管理所有常數與環境變數 |
| `models.py` | `@dataclass` 定義事件結構（Transaction、Login），兼具 schema validation |
| `dlq.py` | DLQ producer 初始化與發送 |
| `validators.py` | 業務邏輯驗證（Late Data 偵測） |
| `writers.py` | ClickHouse client 初始化與 insert（接收 dataclass instance） |
| `anomaly.py` | Sliding Window state 與 Velocity Check（接收 Transaction dataclass） |
| `alerts.py` | 帳號凍結事件發布至 `epay_account_alerts` topic |
| `consumer.py` | Main loop 編排邏輯 |

**原則**：Single Responsibility。每個模組的職責明確，修改某一層邏輯不會影響其他模組。Main loop 只負責編排，不包含任何業務邏輯。

### 6.2 Async Producer + Sync Consumer

**選擇**：Producer 使用 `confluent-kafka` v2.14.0 的 `AIOProducer`（async），Consumer 維持 sync `Consumer`。

**理由**：Producer 的瓶頸在於 I/O — 它需要持續發送大量 message 到 Kafka broker，`asyncio.gather()` 可以讓同一 batch 內的多次 `produce()` 並行等待 broker ACK，提高吞吐量。Consumer 的瓶頸不在 Kafka polling，而在下游處理邏輯（ClickHouse insert、anomaly detection）。sync consumer 的 `poll()` → process → commit 順序保證了 at-least-once 語義的簡單性，不需要 async 帶來的額外複雜度。

**架構上的合理性**：Kafka 本身就是 Producer 和 Consumer 之間的解耦層。兩端的實作方式（async vs sync）是獨立的技術決策，不需要對稱。面試時可解釋「Kafka 的 broker 不關心 client 是 async 還是 sync，它只處理 TCP 連線上的 produce/fetch requests」。

**Batch 策略差異**：Producer 使用 `asyncio.gather()` 讓 batch 內的 produce 並行（fan-out），是吞吐量導向。Consumer 是逐筆處理（per-message），是正確性導向。未來如需提升 Consumer 吞吐量，可考慮 micro-batching（累積 N 筆後批次 insert ClickHouse），而非改用 async。

### 6.3 @dataclass 取代 dict-based Schema Validation

**選擇**：使用 `@dataclass`（`models.py` 中的 `Transaction` 和 `Login`）作為事件的資料結構，取代原本的 dict + `validate_schema()` 函數。

**理由**：
1. **型別安全**：`Transaction(**data)` 建構時如果 dict 缺少必要欄位或多出未預期的欄位，Python 會直接拋出 `TypeError`。這比手動檢查 `if "field" not in data` 更簡潔、更不容易遺漏。
2. **IDE 支援**：`txn.user_id` 有自動補全和型別提示，`data["user_id"]` 沒有。
3. **可讀性**：`insert_transaction(txn: Transaction)` 的函式簽名明確表達了「這個函數需要一筆交易事件」，比 `insert_transaction(data: dict)` 更有表達力。
4. **去掉了 `validators.py` 中的 `validate_schema()`**：dataclass constructor 本身就是 schema validation，不需要額外的驗證函數。`validators.py` 只保留了 `is_late_data()`，因為這是業務邏輯而非結構驗證。

**Consumer main loop 的變化**：
```python
# Before (dict-based)
if not validate_schema(data, ["transaction_id", "user_id", ...]): send_to_dlq(...)
# After (dataclass)
try:
    txn = Transaction(**data)
except TypeError as e:
    send_to_dlq(raw_payload, f"Schema error: {e}")
```

**為什麼不用 Pydantic**：Pydantic 提供更強大的驗證功能（型別轉換、欄位約束），但需要額外安裝第三方套件。`@dataclass` 是 Python 標準庫，零依賴，對 demo 規模足夠。

**已知限制（以測試記錄）**：Python `@dataclass` 不做執行期型別檢查。例如 `amount` 標註為 `float`，但傳入 `str` 不會報錯。這是 dataclass 的設計哲學（type hints 是給靜態分析工具用的，不是 runtime enforcement）。若需嚴格型別驗證，需升級至 Pydantic。測試中的 `TestTypeCoercion` 明確記錄了這個 trade-off。

### 6.4 Repo 結構：保持 Flat Structure

**選擇**：Producer 和 Consumer 各自維持 flat structure（所有 `.py` 檔案在同一層），不進一步拆分子目錄。

**理由**：曾考慮過將 consumer 拆為 `config/`、`utils/`、`generators/` 等子目錄，但最終決定不做，原因如下：
1. **單檔案子目錄無意義**：`config/config.py` 只多了一層 nesting，import 路徑變成冗餘的 `from config.config import ...`。
2. **`utils/` 是 anti-pattern**：anomaly.py、writers.py、alerts.py 是核心業務邏輯，不是工具函數。放進 `utils/` 會誤導讀者對這些模組重要性的判斷。
3. **Docker build context 複雜化**：子目錄需要 `__init__.py`，Dockerfile 的 COPY 路徑也要跟著調整。
4. **規模不需要**：每個 service 只有 6-8 個檔案，flat structure 一眼就能看清全貌。

**原則**：Over-engineering 和 under-engineering 都是問題。目前的規模用 flat structure 就是最乾淨的選擇，等模組數量超過 15 個再考慮分層。

---

## 7. Testing Strategy

### 7.1 測試範圍選擇

**選擇**：優先為三類核心業務邏輯撰寫 unit test，不測 I/O 層。

**測什麼**：
| 模組 | 測試重點 | 理由 |
|------|----------|------|
| `anomaly.py` | Velocity Check 的邊界條件、狀態管理、跨用戶隔離 | 系統最核心的業務邏輯，有狀態（deque），邊界條件多 |
| `validators.py` | is_late_data 的時間邊界 | 涉及 `>` vs `>=` 的微妙判定，需要精確測試 |
| `models.py` | dataclass 建構、缺欄位/多欄位的 TypeError、parsed_time() | 驗證 schema validation 機制是否如預期運作 |

**不測什麼**：
- `writers.py`、`dlq.py`、`alerts.py`：純 I/O 操作（寫 ClickHouse、寫 Kafka），需要 mock 外部服務，投入產出比低。
- `consumer.py`、`producer.py`：編排層，適合用 integration test 而非 unit test。

### 7.2 測試設計原則

**時間敏感測試的處理**：`is_late_data()` 依賴 `datetime.now()`，測試中用 `unittest.mock.patch` 凍結時間，確保測試結果不因執行時刻而變化。

**全域狀態隔離**：`check_velocity()` 使用模組級的 `window_state` 全域變數。透過 `pytest.fixture(autouse=True)` 在每個測試前後自動清空狀態，避免測試間互相污染。

**邊界值測試**：每個判定條件都測試了「剛好在邊界上」和「超過邊界一個單位」的情況。例如 Velocity Check 測了 60 秒（觸發）和 61 秒（不觸發），Late Data 測了 300 秒（不觸發）和 301 秒（觸發）。

**記錄已知限制**：`TestTypeCoercion` 不是為了發現 bug，而是用測試「文件化」dataclass 不做型別檢查的已知行為。面試時可以引用這組測試來展示對工具限制的理解。

### 7.3 測試執行方式

```bash
# 在 host 執行（不需要 Docker）
cd kafka-pub-sub-system
pip install pytest
pytest tests/ -v
```

測試透過 `conftest.py` 將 `consumer/` 加入 `sys.path`，模擬 Docker 容器內的 import 環境。所有 29 個測試純 CPU 執行，不依賴 Kafka 或 ClickHouse，0.06 秒內完成。

---

## 8. Scalability（未來方向）

### 8.1 Kafka Broker 水平擴展

目前為單 broker。擴展為多 broker cluster 時需要：
- 新增 broker 節點，各自設定不同的 `KAFKA_NODE_ID`
- 更新 `KAFKA_CONTROLLER_QUORUM_VOTERS` 包含所有節點
- 將 `replication-factor` 從 1 提高至 2 或 3

### 8.2 Consumer 水平擴展

目前 topics 已設定 3 個 partitions，理論上可支援最多 3 個 consumer instances 平行消費。在 docker-compose 中加入 `deploy.replicas: 3` 即可。因為 partition key 是 `user_id`，同一使用者的事件一定在同一 partition，Velocity Check 的 in-memory state 不會跨 instance，無需外部 state store。

### 8.3 ClickHouse 叢集化

單節點 ClickHouse 可支撐每秒數十萬筆 insert。如需進一步擴展，可使用 `ReplicatedMergeTree` + `Distributed` table 實現資料分片和副本。

### 8.4 store_id 欄位的保留

`fact_transactions.store_id` 目前沒有對應的 analytical view。保留在 fact table 中是為了未來可 join `dim_stores` 維度表做商家維度分析（如各商家交易失敗率排名）。目前 demo 未建立商家維度表，故暫不做聚合 view。
