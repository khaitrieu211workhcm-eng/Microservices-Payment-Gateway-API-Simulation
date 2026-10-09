# Microservices Payment Gateway API

Mô phỏng **Cổng thanh toán độ tin cậy cao chuẩn ngân hàng** (High-Throughput & Idempotent) theo kiến trúc microservices, đóng gói và chạy bằng Docker Compose.

Đồ án tập trung vào các vấn đề cốt lõi của một hệ thống thanh toán:

| Vấn đề | Giải pháp trong đồ án |
|---|---|
| Toàn vẹn dữ liệu tiền | Giao dịch **ACID** trên PostgreSQL, `CHECK (balance >= 0)`, audit log trước/sau mỗi lần cộng trừ |
| Nhiều giao dịch tranh chấp cùng tài khoản | **Pessimistic Locking** (`SELECT ... FOR UPDATE`) |
| Giao dịch chéo A↔B gây deadlock | Luôn khóa tài khoản theo **thứ tự UUID tăng dần** |
| Client gửi lại request, mạng chập chờn | **Idempotency-Key** với Redis (lớp 1) và `UNIQUE` trong DB (lớp 2) |
| Merchant gọi API từ bên ngoài | Xác thực **HMAC-SHA256** (`X-API-Key` + `X-Signature`) |
| Xử lý việc phụ không làm chậm giao dịch | **Event-driven**: RabbitMQ + Notification Worker |
| Theo dõi hệ thống | **Prometheus + Grafana** với custom metrics |

---

## Mục lục

1. [Kiến trúc hệ thống](#1-kiến-trúc-hệ-thống)
2. [Công nghệ sử dụng](#2-công-nghệ-sử-dụng)
3. [Cấu trúc thư mục](#3-cấu-trúc-thư-mục)
4. [Cơ sở dữ liệu](#4-cơ-sở-dữ-liệu)
5. [Cách chạy dự án](#5-cách-chạy-dự-án)
6. [API Endpoints](#6-api-endpoints)
7. [Luồng xử lý chi tiết](#7-luồng-xử-lý-chi-tiết)
8. [Monitoring: Prometheus & Grafana](#8-monitoring-prometheus--grafana)
9. [Kế hoạch kiểm thử (10 Test Case)](#9-kế-hoạch-kiểm-thử-10-test-case)
10. [Minh chứng kết quả](#10-minh-chứng-kết-quả)
11. [Hiện trạng và giới hạn](#11-hiện-trạng-và-giới-hạn)

---

## 1. Kiến trúc hệ thống

```
                           ┌────────────────────────────┐
   Client / Swagger UI ──► │  payment_api (FastAPI)     │
                           │  uvicorn --workers 4 :8000 │
                           └────┬──────────┬─────────┬──┘
                                │          │         │
                  ┌─────────────┘          │         └──────────────┐
                  ▼                        ▼                        ▼
        ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────────┐
        │ PostgreSQL 15    │     │ Redis 7          │     │ RabbitMQ 3 (mgmt)    │
        │ Ledger & Txn     │     │ Idempotency Lock │     │ Exchange: payment_   │
        │ :5432            │     │ :6379            │     │ events (topic)       │
        └──────────────────┘     └──────────────────┘     │ :5672 / UI :15672    │
                                                          └──────────┬───────────┘
                                                                     ▼
                                                        ┌─────────────────────────┐
                                                        │ notification_worker     │
                                                        │ queue: notification_    │
                                                        │ queue (bind transaction.#)
                                                        └─────────────────────────┘

        GET /metrics ◄──── Prometheus :9090 ────► Grafana :3000
```

| # | Service | Container | Vai trò | Cổng |
|---|---|---|---|---|
| 1 | `postgres` | `payment_postgres` | Database tài chính: Ledger & Transaction Storage | 5432 |
| 2 | `redis` | `payment_redis` | Cache & khóa Idempotency | 6379 |
| 3 | `rabbitmq` | `payment_rabbitmq` | Message Broker (Event-Driven Architecture) + Management UI | 5672, 15672 |
| 4 | `payment_api` | `payment_fastapi_app` | FastAPI Payment Core Engine (API Gateway / App Core) | 8000 |
| 5 | `notification_worker` | `payment_notification_worker` | Worker thông báo bất đồng bộ | - |
| 6 | `prometheus` | `payment_prometheus` | Thu thập metrics | 9090 |
| 7 | `grafana` | `payment_grafana` | Dashboard giám sát | 3000 |

`payment_api` chỉ khởi động khi `postgres`, `redis`, `rabbitmq` đã **healthy** (`healthcheck` + `depends_on: condition: service_healthy`). `notification_worker` chờ `rabbitmq` healthy. Log của mọi container dùng driver `json-file`, giới hạn 10 MB × 3 file.

---

## 2. Công nghệ sử dụng

| Nhóm | Công nghệ |
|---|---|
| Web framework | FastAPI 0.111.0, Uvicorn 0.30.1 (4 workers) |
| Database | PostgreSQL 15 (alpine), SQLAlchemy 2.0.31 (async), asyncpg 0.29.0, psycopg2-binary |
| Cache / Idempotency | Redis 7 (alpine), redis-py 5.0.7 (`redis.asyncio`) |
| Message Broker | RabbitMQ 3 management (alpine), aio-pika 9.4.1, pika 1.3.2 |
| Validation / Config | Pydantic 2.7.4, pydantic-settings 2.3.4 |
| Monitoring | prometheus-client 0.20.0, prometheus-fastapi-instrumentator 7.0.0, Prometheus, Grafana |
| Resilience (mở rộng) | pybreaker, httpx (module `circuit_breaker.py`) |
| Kiểm thử tải | Locust (`tests/locustfile.py`), httpx (các script test) |
| Đóng gói | Docker, Docker Compose, `python:3.11-slim` |

---

## 3. Cấu trúc thư mục

```
.
├── app/
│   ├── main.py              # FastAPI app, lifespan, Instrumentator, middleware, /health, /transfer
│   ├── config.py            # Settings (pydantic-settings, đọc env / .env)
│   ├── database.py          # Async engine + connection pool, get_db()
│   ├── models.py            # ORM: User, Account, Merchant, Transaction, AccountAuditLog
│   ├── schemas.py           # TransferRequest, TransactionResponse
│   ├── services.py          # PaymentService.process_transfer (ACID + Pessimistic Lock)
│   ├── idempotency.py       # Middleware Idempotency dùng Redis
│   ├── redis_client.py      # Redis client, init / close
│   ├── event_bus.py         # EventPublisher (aio-pika) -> exchange payment_events
│   ├── security.py          # verify_hmac_signature cho Merchant
│   ├── merchant_api.py      # Router /api/v1/merchants/pay
│   ├── metrics.py           # Custom Prometheus metrics
│   ├── circuit_breaker.py   # Circuit Breaker gọi ngân hàng đối tác (module mở rộng)
│   ├── test_concurrent.py   # Test case 5: request đồng thời cùng Idempotency-Key
│   ├── test_deadlock.py     # Test case 6: chuyển tiền chéo A->B / B->A
│   ├── test_hmac_security.py# Test case 8: chữ ký HMAC
│   └── test_overdraft_stress.py
├── workers/
│   └── notification_worker.py   # Consumer RabbitMQ
├── alembic/
│   └── versions/0001_initial_financial_schema.py   # Migration schema ban đầu
├── tests/
│   ├── locustfile.py        # Stress test bằng Locust
│   └── Dockerfile
├── prometheus/
│   └── prometheus.yml       # Cấu hình scrape
├── grafana/
│   └── dashboards/          # Thư mục dashboard Grafana
├── schema.sql               # Tạo bảng + index (tự chạy khi khởi tạo Postgres)
├── seed.sql                 # Dữ liệu mẫu
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── error.log                # Log truy cập /metrics từ Prometheus
├── Microservices Payment Gateway API - Swagger UI.pdf
├── Explore - prometheus - Grafana.pdf
├── localhost8000_metrics.pdf
└── RabbitQM/                # Ảnh chụp RabbitMQ Management (Overview, Connections,
                             #   Channels, Exchanges, Queues, Admin)
```

---

## 4. Cơ sở dữ liệu

`schema.sql` và `seed.sql` được mount vào `/docker-entrypoint-initdb.d/` (`01_schema.sql`, `02_seed.sql`) nên **tự chạy ở lần khởi tạo đầu tiên** của volume `postgres_data`. Thư mục `alembic/versions/0001_initial_financial_schema.py` là migration tương đương cho cùng schema.

### 4.1. Các bảng

| Bảng | Mô tả |
|---|---|
| `users` | Hồ sơ khách hàng, `status`: `ACTIVE` / `SUSPENDED` / `CLOSED` |
| `accounts` | Tài khoản thanh toán. `balance NUMERIC(18,4)`, `currency` mặc định `VND`, `status`: `ACTIVE` / `FROZEN` / `CLOSED`, cột `version` (schema ghi chú dành cho Optimistic Locking; trong code hiện chỉ được tăng `+1` sau mỗi giao dịch, **không** có điều kiện `WHERE version = ...`) |
| `merchants` | Đơn vị chấp nhận thanh toán: `account_id` (tài khoản nhận tiền), `api_key` (UNIQUE), `secret_key` (dùng cho HMAC) |
| `transactions` | Giao dịch (append-only). `idempotency_key` UNIQUE là chốt chặn cuối chống duplicate. `type`: `DEPOSIT` / `WITHDRAW` / `TRANSFER` / `MERCHANT_PAYMENT`; `status`: `PENDING` / `SUCCESS` / `FAILED` / `REVERSED` |
| `account_audit_logs` | Nhật ký kiểm toán: `action` (`CREDIT` / `DEBIT`), `balance_before`, `amount`, `balance_after` |

### 4.2. Ràng buộc và index

- `accounts.balance >= 0`: không bao giờ âm.
- `transactions.amount > 0`.
- `chk_different_accounts`: tài khoản nguồn và đích phải khác nhau.
- Khóa ngoại dùng `ON DELETE RESTRICT` cho `accounts → users` và `merchants → accounts`.
- Index phục vụ high-throughput lookup: `idx_accounts_user_id`, `idx_transactions_source_acc`, `idx_transactions_dest_acc`, `idx_transactions_created_at (DESC)`, `idx_audit_logs_account_id`.

### 4.3. Dữ liệu mẫu (`seed.sql`)

| Đối tượng | Thông tin | Số dư |
|---|---|---|
| Alice Nguyen (`alice@vpbank.demo`) | account `aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa`, số TK `1000000001` | 5.000.000 VND |
| Bob Tran (`bob@vpbank.demo`) | account `bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb`, số TK `1000000002` | 2.000.000 VND |
| Merchant "Shopee Store" | `api_key = merchant_key_shopee_01`, `secret_key = super_secret_hmac_key_999`, nhận tiền vào tài khoản của Bob | - |

`seed.sql` mở đầu bằng `TRUNCATE ... CASCADE` để có thể seed lại.

---

## 5. Cách chạy dự án

### Yêu cầu

Docker và Docker Compose; các cổng `5432`, `6379`, `5672`, `15672`, `8000`, `9090`, `3000` còn trống.

### Khởi chạy

```bash
docker compose up -d --build      # build image và chạy toàn bộ 7 service
docker compose ps                 # kiểm tra trạng thái
docker compose logs -f payment_api
```

### Truy cập

| Thành phần | URL | Tài khoản |
|---|---|---|
| Swagger UI | http://localhost:8000/docs | - |
| OpenAPI | http://localhost:8000/openapi.json | - |
| Health check | http://localhost:8000/health | - |
| Metrics | http://localhost:8000/metrics | - |
| RabbitMQ Management | http://localhost:15672 | guest / guest |
| Prometheus | http://localhost:9090 | - |
| Grafana | http://localhost:3000 | admin / admin |

### Dừng / reset

```bash
docker compose down        # dừng, giữ dữ liệu
docker compose down -v     # dừng và xóa volume postgres_data (schema + seed chạy lại ở lần sau)
```

### Biến môi trường

Cấu hình đọc bằng `pydantic-settings` (biến môi trường hoặc `.env`); docker-compose đã khai báo sẵn cho `payment_api` và `notification_worker`.

| Nhóm | Biến | Mặc định |
|---|---|---|
| App | `APP_NAME`, `ENV`, `DEBUG`, `PORT` | `Microservices Payment Gateway API`, `development`, `True`, `8000` |
| PostgreSQL | `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB` | `postgres`, `postgrespassword`, `postgres`, `5432`, `payment_db` |
| Redis | `REDIS_HOST`, `REDIS_PORT`, `REDIS_DB`, `REDIS_PASSWORD` | `redis`, `6379`, `0`, `None` |
| RabbitMQ | `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD` | `rabbitmq`, `5672`, `guest`, `guest` |

Connection pool PostgreSQL: `pool_size=20`, `max_overflow=10`, `pool_timeout=30`, `pool_pre_ping=True`. Redis: `max_connections=50`, `socket_timeout=5.0`.

---

## 6. API Endpoints

| Method | Path | Mô tả | Header bắt buộc |
|---|---|---|---|
| `GET` | `/health` | Health Check | - |
| `GET` | `/metrics` | Metrics cho Prometheus | - |
| `POST` | `/api/v1/payments/transfer` | Chuyển tiền 24/7 giữa 2 tài khoản (ACID, Pessimistic Locking & Idempotent) | `Idempotency-Key` |
| `POST` | `/api/v1/merchants/pay` | Thanh toán đơn hàng qua Merchant (HMAC Security & Async Event Bus) | `Idempotency-Key`, `X-API-Key`, `X-Signature` |

### 6.1. `POST /api/v1/payments/transfer`

```bash
curl -X POST 'http://localhost:8000/api/v1/payments/transfer' \
  -H 'accept: application/json' \
  -H 'Idempotency-Key: TEST-KEY-001' \
  -H 'Content-Type: application/json' \
  -d '{
    "source_account_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "destination_account_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    "amount": 500000,
    "description": "Chuyen tien test case 1 qua Swagger"
  }'
```

Response `200`:

```json
{
  "transaction_id": "07c96fcc-bffd-4766-af02-b5d29f07ca19",
  "idempotency_key": "TEST-KEY-001",
  "source_account_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
  "destination_account_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
  "amount": "500000.0000",
  "fee": "0.0000",
  "currency": "VND",
  "type": "TRANSFER",
  "status": "SUCCESS",
  "description": "Chuyen tien test case 1 qua Swagger",
  "created_at": "2026-10-08T15:45:25.130445Z"
}
```

Khi gửi lại **cùng `Idempotency-Key`**, response trả về từ cache và có header `x-cache: HIT-IDEMPOTENCY`.

### 6.2. `POST /api/v1/merchants/pay`

- Body dùng cùng schema `TransferRequest` như `/transfer`; `source_account_id` là tài khoản người mua.
- **Tài khoản nhận luôn là tài khoản của Merchant** (lấy từ `X-API-Key`), `destination_account_id` trong body không được dùng để chuyển tiền.
- Mô tả giao dịch được đặt thành `Thanh toán đơn hàng qua Merchant: <tên merchant>`.
- `X-Signature` = `HMAC-SHA256(secret_key, raw_request_body)` dạng hex, tính trên **đúng bytes** của body gửi đi.

### 6.3. Mã lỗi

| Mã | Tình huống |
|---|---|
| `400` | Thiếu header `Idempotency-Key`; tài khoản nguồn trùng đích; `amount <= 0`; số dư không đủ; tài khoản không ở trạng thái `ACTIVE` |
| `401` | Thiếu `X-API-Key` / `X-Signature`; API Key không hợp lệ hoặc Merchant bị khóa |
| `403` | Chữ ký `X-Signature` không hợp lệ |
| `404` | Một trong hai tài khoản không tồn tại |
| `409` | Giao dịch với `Idempotency-Key` này đang được xử lý |
| `422` | Body sai định dạng (Pydantic validation) |

---

## 7. Luồng xử lý chi tiết

### 7.1. Idempotency Middleware (`app/idempotency.py`)

Áp dụng cho mọi request `POST / PUT / PATCH`:

1. Bắt buộc có header `Idempotency-Key`, nếu không trả `400`.
2. Ghi `idempotency:<key> = {"status": "PROCESSING"}` bằng `SET NX EX 30` (atomic).
   - Ghi được: request mới, đi tiếp vào controller.
   - Key đã tồn tại với `PROCESSING`: trả `409 Conflict`.
   - Key đã `COMPLETED`: trả ngay response đã lưu kèm header `X-Cache: HIT-IDEMPOTENCY`.
3. Sau khi xử lý xong:
   - Response `2xx`: lưu `{status: COMPLETED, status_code, response}` vào Redis, TTL **24 giờ**.
   - Response lỗi: **xóa key** để client có thể retry.
4. Nếu Redis mất kết nối (`ConnectionError`): ghi cảnh báo và **bypass** kiểm tra idempotency, hệ thống vẫn xử lý giao dịch. Khi đó `UNIQUE (idempotency_key)` trong bảng `transactions` là chốt chặn cuối.

### 7.2. Chuyển tiền ACID (`app/services.py` → `PaymentService.process_transfer`)

1. Kiểm tra nguồn ≠ đích và `amount > 0`.
2. **Chống deadlock**: `sorted([source_id, destination_id])`, luôn khóa tài khoản có UUID nhỏ hơn trước.
3. **Pessimistic Lock**: `SELECT ... FOR UPDATE` lần lượt hai tài khoản.
4. Kiểm tra cả hai tài khoản tồn tại, ở trạng thái `ACTIVE`, và tài khoản nguồn đủ số dư.
5. Trừ tiền nguồn, cộng tiền đích, tăng `version` của cả hai.
6. Tạo bản ghi `Transaction` (`type=TRANSFER`, `status=SUCCESS`, `fee=0`, `currency=VND`) và hai `AccountAuditLog` (`DEBIT` cho nguồn, `CREDIT` cho đích) ghi lại `balance_before` / `balance_after`.
7. `COMMIT` một lần duy nhất; lỗi ở bất kỳ bước nào thì không có thay đổi nào được ghi.

### 7.3. Merchant Payment và Event Bus

1. `verify_hmac_signature` đọc `X-API-Key`, `X-Signature`; tra Merchant `ACTIVE` theo `api_key`; tính HMAC-SHA256 trên body gốc bằng `secret_key` và so sánh bằng `hmac.compare_digest`.
2. Gọi `PaymentService.process_transfer` với tài khoản đích là tài khoản của Merchant.
3. Publish event routing key `transaction.success` lên exchange `payment_events` (kiểu **topic**, durable, message **PERSISTENT**) với nội dung: `transaction_id`, `merchant_id`, `amount`, `status`, `created_at`.
4. Ghi nhận custom metrics cho Prometheus.

`EventPublisher` kết nối bằng `aio_pika.connect_robust`, retry 5 lần cách nhau 3 giây khi khởi động; nếu RabbitMQ không khả dụng, API vẫn khởi động và bỏ qua việc publish event.

### 7.4. Notification Worker (`workers/notification_worker.py`)

- Kết nối RabbitMQ (retry 5 lần, cách nhau 3 giây).
- Khai báo exchange `payment_events` (topic, durable) và queue `notification_queue` (durable), bind với routing key `transaction.#`.
- Với mỗi event nhận được, worker ghi log `transaction_id`, `amount` và mô phỏng việc gửi Email & SMS thông báo biến động số dư. Message được ack sau khi xử lý (`message.process()`).

### 7.5. Khởi động và tắt ứng dụng

`lifespan` của FastAPI: khi khởi động kiểm tra kết nối Redis (`ping`) và kết nối RabbitMQ; khi tắt đóng Redis, RabbitMQ và `engine.dispose()`.

### 7.6. Circuit Breaker (module mở rộng, `app/circuit_breaker.py`)

Mô phỏng gọi API sang ngân hàng đối tác bên ngoài, bọc bởi `pybreaker`:
- Ngắt mạch sau **3 lỗi liên tiếp** (`fail_max=3`), thời gian chờ khôi phục **20 giây** (`reset_timeout=20`).
- HTTP client `httpx` có timeout 3 giây.
- Khi mạch ở trạng thái OPEN, `safe_call_core_banking()` trả `503 Service Unavailable` ngay lập tức thay vì chờ timeout.

---

## 8. Monitoring: Prometheus & Grafana

### 8.1. Custom metrics (`app/metrics.py`)

| Metric | Loại | Label | Ý nghĩa |
|---|---|---|---|
| `payment_transaction_volume_total` | Counter | `payment_type`, `currency` | Tổng số tiền đã giao dịch thành công (VND) |
| `payment_transactions_total` | Counter | `type`, `status`, `error_code` | Tổng số giao dịch theo trạng thái và mã lỗi |
| `payment_processing_latency_seconds` | Histogram | `payment_type` | Độ trễ xử lý giao dịch trong Core Engine (bucket 5ms → 5s) |
| `active_payment_locks_count` | Gauge | - | Số giao dịch đang giữ Pessimistic Lock |

Các metric được ghi ở cả `/transfer` (`payment_type="TRANSFER"`) và `/merchants/pay` (`payment_type="MERCHANT_PAYMENT"`). Giao dịch lỗi (`HTTPException`) được đếm vào `payment_transactions_total` với `status="FAILED"` và `error_code` là mã HTTP.

Ngoài ra `prometheus-fastapi-instrumentator` cung cấp metric HTTP chuẩn: `http_requests_total`, `http_request_duration_seconds`, `http_request_duration_highr_seconds`, `http_requests_inprogress`, `http_request_size_bytes`, `http_response_size_bytes`. Các handler `/metrics`, `/health`, `/docs`, `/openapi.json` được loại khỏi instrumentation; mã trạng thái được giữ nguyên (không gộp 2xx/4xx).

### 8.2. Cấu hình Prometheus (`prometheus/prometheus.yml`)

```yaml
global:
  scrape_interval: 2s        # Thu thập mỗi 2 giây (real-time cho Fintech)
  evaluation_interval: 2s

scrape_configs:
  - job_name: 'payment-gateway-service'
    metrics_path: '/metrics'
    static_configs:
      # host.docker.internal giúp Prometheus trong Docker gọi ngược ra FastAPI trên host
      - targets: ['host.docker.internal:8000']
        labels:
          env: 'production'
          service: 'payment-core-api'
```

Mọi series được gắn label `env="production"` và `service="payment-core-api"`.

### 8.3. Grafana

- Truy cập http://localhost:3000 (`admin` / `admin`), thêm data source **Prometheus** với URL `http://prometheus:9090`.
- Dùng **Explore** để truy vấn, ví dụ metric `payment_processing_latency_seconds_sum` và `payment_transactions_total` (xem [Minh chứng](#10-minh-chứng-kết-quả)).

---

## 9. Kế hoạch kiểm thử (10 Test Case)

Bộ test case phủ Functional Testing, Security, Concurrency và System Resiliency.

### 9.1. Logic nghiệp vụ & số dư (ACID)

| TC | Tên | Đầu vào | Kỳ vọng |
|---|---|---|---|
| 1 | Chuyển tiền thành công (Happy Path) | Nguồn đủ tiền, đích hợp lệ, `amount > 0`, `Idempotency-Key` mới | `200 OK`; nguồn bị trừ, đích được cộng đúng `amount`; tạo 2 audit log `DEBIT` & `CREDIT`; (luồng Merchant) bắn 1 event sang RabbitMQ |
| 2 | Số dư không đủ | `amount` lớn hơn số dư nguồn | `400` "Số dư không đủ"; số dư hai bên giữ nguyên; không tạo audit log |
| 3 | Chuyển cho chính mình | `source_account_id` = `destination_account_id` | `400` "Tài khoản nguồn và tài khoản đích không được trùng nhau"; không khóa, không ghi DB |

### 9.2. Idempotency & request đồng thời

| TC | Tên | Đầu vào | Kỳ vọng |
|---|---|---|---|
| 4 | Request trùng lặp nối tiếp | Gửi request A (200), rồi gửi lại y hệt cùng `Idempotency-Key` | Request B trả kết quả giống A kèm `X-Cache: HIT-IDEMPOTENCY`; số dư chỉ bị trừ 1 lần |
| 5 | Request trùng lặp đồng thời | 2 request song song cùng 1 `Idempotency-Key` | Request đầu xử lý, request sau bị chặn ở middleware với `409 Conflict` ("đang được xử lý") |

### 9.3. Tranh chấp & chịu tải

| TC | Tên | Đầu vào | Kỳ vọng |
|---|---|---|---|
| 6 | Chuyển tiền chéo đồng thời | 2 luồng đồng thời: A→B và B→A | Cả hai thành công (hoặc một bên chờ lock rồi chạy tiếp); PostgreSQL không báo `DeadlockDetected` |
| 7 | Rút sạch số dư đồng thời | Số dư 1.000.000; 10 request chuyển 1.000.000 với 10 key khác nhau | Đúng 1 request `200`, 9 request `400`; số dư cuối = 0 (không âm) |

### 9.4. Bảo mật & khả năng chịu lỗi

| TC | Tên | Đầu vào | Kỳ vọng |
|---|---|---|---|
| 8 | Sai chữ ký HMAC | `/merchants/pay` với `X-API-Key` đúng nhưng `X-Signature` sai, hoặc sửa `amount` sau khi ký | `403` "Chữ ký dữ liệu (X-Signature) không hợp lệ" |
| 9 | Redis ngừng hoạt động | `docker compose stop redis` rồi gửi request chuyển tiền | Middleware ghi warning, **bypass** idempotency, giao dịch vẫn ghi vào PostgreSQL thành công (`200`) |
| 10 (*) | Cổng ngân hàng đối tác sập | Partner Gateway lỗi/timeout 3 lần liên tiếp | Từ lần gọi thứ 4, `pybreaker` ở trạng thái OPEN và trả ngay `503` mà không phải chờ timeout 3 giây |

> (*) TC 10 kiểm thử module `circuit_breaker.py` ở mức độc lập (gọi trực tiếp `safe_call_core_banking()` với đối tác giả lập), vì module này chưa được nối vào API nào (xem [mục 11](#11-hiện-trạng-và-giới-hạn)).

### 9.5. Công cụ kiểm thử trong dự án

| Công cụ | Phục vụ |
|---|---|
| Swagger UI (`/docs`) / `curl` | TC 1-4 (kiểm thử chức năng, quan sát header `x-cache`) |
| `app/test_concurrent.py` | TC 5: 2 request đồng thời cùng `Idempotency-Key` |
| `app/test_deadlock.py` | TC 6: A→B và B→A cùng lúc |
| `tests/locustfile.py` | TC 4, 7: stress test với 3 kịch bản (chuyển tiền thường, trùng `Idempotency-Key`, rút số tiền lớn để thử chống âm số dư); cả 3 đều gọi `/api/v1/payments/transfer`, chưa có kịch bản cho `/merchants/pay` |
| `app/test_hmac_security.py` | TC 8: chữ ký hợp lệ / sửa body / chữ ký giả |

Chạy script test khi hệ thống đang chạy:

```bash
python app/test_concurrent.py
python app/test_deadlock.py
python app/test_hmac_security.py
locust -f tests/locustfile.py --host http://localhost:8000
```

---

## 10. Minh chứng kết quả

Các file ảnh/PDF chụp màn hình có trong repo:

| File | Nội dung |
|---|---|
| `Microservices Payment Gateway API - Swagger UI.pdf` | Gọi `POST /api/v1/payments/transfer` với `Idempotency-Key: TEST-KEY-001`: chuyển 500.000 VND từ Alice sang Bob, response `200 SUCCESS`; lần gửi lại có header `x-cache: HIT-IDEMPOTENCY` |
| `localhost8000_metrics.pdf` | Trang `/metrics`: `payment_transaction_volume_total{payment_type="TRANSFER"} = 500000`, `payment_transactions_total{status="SUCCESS"} = 1`, `payment_processing_latency_seconds_sum ≈ 0.098s`, `http_requests_total` gồm 1 request `200` và 1 request `422` |
| `Explore - prometheus - Grafana.pdf` | Grafana Explore truy vấn `payment_processing_latency_seconds_sum` và `payment_transactions_total` từ Prometheus (có label `env="production"`) |
| `RabbitQM/*.pdf` | RabbitMQ Management (3.13.7): Overview, Connections, Channels, Exchanges (có `payment_events`), Queues (`notification_queue`, trạng thái running), Admin (user `guest`) |
| `error.log` | Log truy cập `GET /metrics` `200 OK` do Prometheus scrape định kỳ |

---

## 11. Hiện trạng và giới hạn

- `circuit_breaker.py` là module độc lập, **chưa được gọi** từ luồng chuyển tiền; URL đối tác `mock-partner-bank.com` chỉ là giá trị giả lập.
- Metric `active_payment_locks_count` đã được khai báo nhưng hiện chưa được cập nhật trong `PaymentService` (giá trị 0).
- Phí giao dịch (`fee`) luôn bằng `0`, đơn vị tiền tệ cố định `VND`.
- Giao dịch qua Merchant được ghi vào bảng `transactions` với `type = TRANSFER` (nhãn `MERCHANT_PAYMENT` chỉ dùng cho metrics).
- Cột `version` của `accounts` được tăng sau mỗi giao dịch; cơ chế chống tranh chấp thực tế dựa trên Pessimistic Lock.
- `grafana/dashboards/payment_dashboard.json` hiện để trống; việc giám sát thực hiện qua Grafana Explore.
- Thông tin đăng nhập trong `docker-compose.yml` và `seed.sql` (Postgres, RabbitMQ `guest`, Grafana `admin`, secret key Merchant) là giá trị demo.
