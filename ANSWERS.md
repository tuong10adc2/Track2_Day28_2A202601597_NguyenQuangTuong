# ANSWERS — Day 28 Track 2 (làm cá nhân)

Người thực hiện: Nguyễn Quang Tường (tuong10adc2@gmail.com), nhánh `ca-nhan-tuong`.
Toàn bộ vai trò (Ingestion & Orchestration, Data & ML, Serving & Retrieval, Nền tảng &
giám sát, Trình bày) đều do một người đảm nhiệm, đi tuần tự theo checklist trong LAB28.md.

## 1. Bốn hàm cốt lõi đã hoàn thiện

`src/lab28_platform/integration_tasks.py`:

- `event_headers` (IP01/IP10) — luôn gắn `idempotency-key`; chỉ gắn `traceparent` khi có,
  tránh gửi header W3C rỗng/không hợp lệ.
- `dedupe_latest` (IP03) — gom theo `idempotency_key`, giữ bản ghi có `(occurred_at, event_id)`
  lớn nhất, sắp xếp kết quả theo khóa để MERGE Delta idempotent và tất định.
- `feast_online_request` (IP04) — dựng đúng payload `/get-online-features` từ
  `contracts.FEATURE_REFS`, không chép lại danh sách đặc trưng ở nơi khác.
- `readiness_status` (IP07/IP08) — lỗi bắt buộc → `not_ready`; chỉ lỗi tùy chọn → `degraded`;
  không lỗi → `ready`.

Toàn bộ `starter-tests` (4/4) và `tests` (87/87) pass; `ruff check .`,
`scripts/verify_matrix.py`, `scripts/check_portability.py`, `scripts/validate_manifests.py`
đều trả mã 0.

## 2. Happy path thật (bằng chứng trong `evidence/`, không commit vào Git)

Một request đi xuyên toàn bộ 10 điểm kết nối, chạy trên Docker profile `full`:

- Trace ID: `4b9f1e9787af43779937900a553677cd` (`evidence/ip01-kafka-consume.json`)
- Kafka: topic `data.raw`, partition 2, offset 20, header `traceparent` giữ nguyên trace id.
- Airflow DAG run: `it-3c7a966d`, cả 4 task (`drain_kafka_into_delta`,
  `refresh_online_features`, `index_new_documents`, `announce_processed_batch`) `success`,
  phát đủ 4 asset event (`lab28://delta/documents`, `.../feedback`, `.../feast/asker_activity`,
  `.../qdrant/lab28_documents`).
- Feast: `asker_id=it-j1-e3e9fd38` → `feedback_count=1`, `delta_version=5`, `degraded=false`.
- MLflow: `lab28-rag-release` — release ban đầu v2 (từ `lab28 release`), sau khi chạy J3
  (promotion/rollback) lên v4, `alias=champion`, `promoted_from="2"`.
- Gateway (IP08): `configured_rps=10`, 30 request gửi dồn → 10 accepted (200), 20 rejected
  (429), cả hai loại đều có `x-request-id`.

`uv run pytest integration-tests -m "not gpu and not langsmith" -q` → **56 passed, 16
deselected** (J1–J5, gateway rate-limit, trace-span-coverage, prometheus-targets). 16 test
bị deselect là các test gắn `gpu`/`langsmith` — bị gate đúng thiết kế vì lớp không cấp GPU
endpoint thật; báo cáo là **UNVERIFIED**, không giả lập vLLM.

## 3. Sự cố đã tạo, khôi phục, không mất dữ liệu (mục 5 SUBMISSION)

Sự cố **không cố ý** xảy ra ngay ở lần chạy DAG đầu tiên (`it-7e0152fa`): task
`drain_kafka_into_delta` trả về `polled=0, processed=0` dù dữ liệu đã có sẵn trên topic
`data.raw`. Dấu hiệu: `test_the_lakehouse_advanced_and_holds_the_row`,
`test_the_feature_store_serves_the_new_asker`,
`test_the_document_is_retrievable_from_the_vector_store` và span
`lab28.kafka.consume`/`lab28.spark.delta_merge` trên Jaeger đều thiếu.

Nguyên nhân: consumer group `group.id` cố định dùng `auto.offset.reset=earliest` nhưng
`poll_batch` chỉ chờ tối đa `idle_polls=3 × poll_timeout=1s` = 3 giây trước khi coi batch là
rỗng; lần đầu group coordinator/rebalance trên broker KRaft mới khởi động chưa kịp gán
partition trong 3 giây đó, nên poll trả về `None` liên tục và task kết luận "không có gì để
xử lý" — nhưng vì offset chưa commit, dữ liệu không mất.

Khôi phục: trigger lại DAG thủ công qua Airflow REST API. Lần này consumer group đã tồn tại,
gán partition ngay, `polled=58, processed=22` (58 message tồn đọng từ các lần seed trước,
`dedupe_latest` gom về 22 bản ghi duy nhất theo `idempotency_key`) — đúng chứng minh
**no-data-loss**: không message nào bị bỏ qua, chỉ chậm một nhịp trigger.

Bài học production: 3 giây idle-budget quá ngắn cho lần rebalance đầu tiên của một consumer
group mới trên broker vừa khởi động; production nên tăng `idle_polls`/`poll_timeout` cho lần
poll đầu, hoặc thêm bước "chờ partition assignment" tường minh trước khi coi batch rỗng là
hợp lệ.

## 4. Load profile & bottleneck

`uv run python load-tests/run_profile.py --requests 200 --workers 8` nhắm vào
`gateway:8080/ready`:

```
p50 = 3.9 ms, p95 = 444.2 ms, p99 = 524.8 ms
status 200: 21/200, còn lại bị script gộp vào "0"
```

Số "0" gây hiểu lầm là lỗi kết nối; kiểm chứng lại bằng vòng lặp thủ công cho thấy đó thực
chất là **HTTP 429** từ token-bucket rate limit của Envoy (`max_tokens=10,
tokens_per_fill=10, fill_interval=1s`, xem `gateway/envoy.yaml`) — khớp với
`evidence/ip08-gateway.json` (`configured_rps: 10`). Bottleneck ở tải này không phải backend
mà là **giới hạn tốc độ tại gateway**: 8 worker gửi gần như đồng thời vượt quá 10 token/giây
nên phần lớn bị từ chối ngay ở Envoy trước khi tới API. Production nên tách rate limit theo
API key/tenant thay vì một bucket toàn cục, và client nên có backoff khi gặp 429 thay vì coi
là lỗi.

## 5. Trade-off đã chọn

- Chạy thẳng `docker compose --profile full` thay vì base rồi mới full: máy có 31 GB RAM,
  12 CPU, đủ dư để tránh build image hai lần.
- Không sửa `load-tests/run_profile.py` dù cách nó gộp `HTTPError` về status "0" hơi gây
  hiểu lầm — vì đây là tool có sẵn của bài lab, không nằm trong 4 hàm được giao; thay vào đó
  ghi rõ nguyên nhân thật (429) trong báo cáo này.
- Seed hai lần qua gateway thay vì hạ token bucket: giữ nguyên policy rate-limit mặc định vì
  đó chính là hành vi IP08 cần chứng minh, chấp nhận một phần feedback bị 429 ở lần seed.

## 6. Production gaps (chưa làm/không thể làm trong môi trường lab)

- **IP07 (vLLM thật)**: không có GPU cục bộ hay endpoint Kaggle được cấp trong phiên này →
  `require_real` báo `not_ready`/`degraded` đúng thiết kế; 16 test `gpu`/`langsmith` bị skip
  có chủ đích, không giả lập.
- **K8s/GitOps thật**: `scripts/validate_manifests.py` chỉ xác thực cấu trúc manifest
  (`deploy/kubernetes`, `gitops/`) tĩnh; chưa deploy lên cluster thật nên chưa có bằng chứng
  drift/rollback trên cluster sống, chỉ có evidence ở mức MLflow alias rollback (J3, mục 2).
- **LangSmith export**: không có `LANGSMITH_API_KEY`; trace local qua Jaeger/OTel đã đủ
  chứng minh continuity (J5, `evidence/ip10-trace.json`), nhưng nhánh export LangSmith chưa
  được xác minh.
- **MLflow CLI trên Windows**: `mlflow.tracking` in emoji ra stdout gây
  `UnicodeEncodeError` với console mặc định `cp1252`; phải set `PYTHONUTF8=1
  PYTHONIOENCODING=utf-8` mới chạy được `lab28 release` — gap trải nghiệm dev trên Windows,
  đáng đưa vào script khởi tạo môi trường thay vì để mỗi người tự phát hiện.

## 7. Đóng góp cá nhân

Làm cá nhân, đã đi qua đủ toàn bộ vai trò: hoàn thiện 4 hàm cốt lõi; dựng và vận hành toàn bộ
Docker stack (`full` profile, 14 service); tạo topic, index Qdrant, release MLflow, seed qua
gateway; điều tra và khôi phục sự cố Kafka-consumer thật phát sinh trong lúc chạy; chạy và
xác minh J1–J5 cùng các test observability; thu thập evidence bundle 10 file; chạy load
profile và phân tích bottleneck; viết báo cáo này.
