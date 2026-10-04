# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Lê Minh Hiếu / 2A202602848
**Repo:** `https://github.com/HieuLM7714/K4-Track02-Day17-LeMinhHieu-2A202602848-DataPipelineEngineering`
**Commit bài nộp:** `<commit hash>`
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Claude Code (Claude Opus) — đọc code, chạy pipeline/verify, đề xuất 3 cách sửa và nháp REPORT; tôi đã review và giải thích được từng dòng sửa.
**Nguồn tham khảo khác (nếu có):** slide Day 17; tài liệu Debezium (định dạng envelope); DuckDB `MERGE INTO`.

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | verify: `24 rows for 12 tickets`; T-91 có 3 hàng `low/open`, `high/open`, `high/closed/bug` | `gold_feature_daily` lệch full recompute (`c50b8851affe != 8630e04a61d1`); u05 ngày 08-12 = `(2, 0)` thay vì `(5, 1)` | T-97 vẫn `is_deleted=False`, còn `user_id`/subject/body; còn 1 hàng ở snapshot mới nhất và 2 chunk trong RAG |
| **Nguyên nhân gốc** | `upsert_silver_tickets` dùng `INSERT` — chỉ dedup *trong* batch, mỗi batch mới lại thêm hàng; không có khoá và không có LSN guard | `LOOKBACK_DAYS = 0` dựa trên giả định "event đến trong vài giây"; mỗi run chỉ tính lại ngày của nó nên event 08-12 đến ngày 08-15 rơi vào partition không bao giờ được tính lại | Delete Debezium có `after = null`; staging lấy `ticket_id` chỉ từ `after` → `NULL` → bị `WHERE ticket_id IS NOT NULL` loại; delete không bao giờ tới Silver/Gold |
| **Cách sửa** | `pipeline/silver.py`: `MERGE INTO silver_tickets ON ticket_id`, `WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT` | `pipeline/config.py`: `LOOKBACK_DAYS = 3` = ceil(P99) đo từ Bronze; mỗi run overwrite `[day-3, day]` | `pipeline/staging.py`: `coalesce(after.ticket_id, before.ticket_id)`; delete đi qua MERGE thành tombstone (cột `after` đều null), snapshot lọc `_op <> 'd'`, RAG lọc `NOT is_deleted` |
| **Khái niệm trên slide** | Silver — có khoá; MERGE theo khoá; LSN guard (batch cũ không đè trạng thái mới) | Late data: event time ≠ ingest time; lookback = P99 đo, không đoán; overwrite-partition | CDC log-based; delete ≠ Kafka tombstone; "xoá phải lan" Silver → Gold |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày (p50=0, p95=2.90, max=3, n=43) → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: **PASS** — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: **PARITY** (silver_tickets `3c15dfd43701`, gold_feature_daily `8630e04a61d1`)

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: ticket là *thực thể có trạng thái* (1 hàng/khoá, LSN quyết định bản nào mới hơn nên chạy lại batch cũ là no-op), còn feature là *aggregate theo ngày* tính lại được hoàn toàn từ Silver, nên xoá rồi tính lại cả cửa sổ `[day-3, day]` vừa idempotent vừa nhận được event muộn.
- Tombstone thay vì xoá hẳn hàng trong Silver: hàng tombstone giữ `ticket_id` + `_lsn` nên một bản replay cũ hơn (LSN nhỏ hơn) không thể "hồi sinh" ticket, đồng thời PII đã bị xoá; đánh đổi là hàng (không chứa PII) tồn tại mãi.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: đảm bảo tái lập được thí nghiệm và đúng point-in-time (không rò rỉ thông tin tương lai); xoá được xử lý bằng cách tạo phiên bản mới không có ticket.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: dữ liệu vài trăm hàng/ngày vừa bộ nhớ một máy, DuckDB chạy SQL in-process không cần cluster; dbt cho cùng logic (merge + `merge_update_condition`, microbatch + `lookback=3`) kèm contract và test — Spark chỉ đáng khi dữ liệu vượt một máy.

## 4. Hai câu hỏi suy ngẫm

1. **Snapshot bất biến vs quyền được xoá.** Quyền xoá (pháp lý) phải thắng. Snapshot "bất biến" nghĩa là *không sửa lặng lẽ*, không phải *không bao giờ xoá*: tôi giữ một danh sách erasure (ticket_id + ngày yêu cầu), khi có yêu cầu xoá thì tạo lại các snapshot cũ chứa ticket đó thành phiên bản mới (vd `v2026-08-12-r1`) không có T-97, xoá vật lý bản cũ, ghi vào changelog/lineage để thí nghiệm vẫn truy được vì sao checksum đổi. Model đã train trên bản cũ được đánh dấu để retrain theo lịch. Cách giảm việc: snapshot chỉ lưu `ticket_id` + đặc trưng đã khử PII, text tra từ Silver lúc train.
2. **PII ngoài regex (tên "Nguyễn Văn An").** Đặt chốt ở ranh giới Bronze → Silver (không gì rời Bronze mà chưa qua): dùng NER/model PII (vd Presidio + model tiếng Việt) để che tên, địa chỉ, CCCD, cộng thêm danh sách tên từ bảng user (`user_id` → họ tên) để thay thế chính xác. Bronze giữ nguyên, bị hạn chế quyền truy cập và có thời hạn lưu. Đo bằng: một bộ test có nhãn PII (precision/recall cho từng loại), một data test chạy mỗi ngày quét Silver/Gold đếm số match còn sót (mục tiêu 0, cảnh báo nếu > 0), và lấy mẫu kiểm tra thủ công định kỳ.

## 5. Output (dán nguyên văn)

Chạy trên Windows PowerShell (lệnh tương đương theo SUBMISSION.md).

```text
$ .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS

$ .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
34 passed in 3.30s

$ .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ dbt build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17   (trong dbt_project/)
...
05:35:44  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.02s]
05:35:44  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 3.82 seconds (3.82s).
05:35:44  Completed successfully
05:35:44  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

Baseline trước khi sửa (bản đề bài): `RESULT: 8/18 checks — FAILURES ABOVE` — fail các check Silver key, T-91, T-97 tombstone, feature reconcile, u05 08-12 `(2, 0)`, LOOKBACK `0 < 3`, T-97 trong snapshot/RAG, doc_chunks `22 rows / 9 chunks`, rerun.
