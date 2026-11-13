# 工厂批次用电量核算 API

把累计电表示数加工成生产批次用电量，作为碳核算的输入。FastAPI + SQLite（WAL），
单进程单 worker 线程；后台重算与查询互不阻塞。

## 快速启动（Docker Compose）

```bash
docker compose up --build
# 交互式演示页： http://localhost:8000/
# OpenAPI 文档：   http://localhost:8000/docs
# 演示脚本：
python3 scripts/demo.py http://localhost:8000
```

数据保存在命名卷 `carbon-data` 的 `/data/carbon.db`，容器重启后自动恢复未完成的重算。

本地不用 Docker：

```bash
python3 -m pip install -r requirements.txt
CARBON_DB_PATH=data/carbon.db uvicorn app.main:app --port 8000 --workers 1
```

> 必须以 **单 worker** 运行。写入串行化、后台重算和重启恢复都假设进程内只有一个
> worker 线程；横向扩容需要换成外部队列（见“设计边界”）。

## 输入格式

示数 CSV（`POST /api/v1/uploads/readings`）：

```csv
meter_id,ts_utc,value_wh
M-001,1767225600,1200000
M-001,1767229200,1203400
```

批次 CSV（`POST /api/v1/uploads/batches`）：

```csv
batch_no,meter_id,start_utc,end_utc
B-2026-01-01-A,M-001,1767227400,1767231000
```

- `ts_utc` / `start_utc` / `end_utc`：UTC 整秒（Unix epoch 秒）。
- `value_wh`：累计整数 Wh，必须非负。
- 表头必须含上述列（大小写不敏感，允许行首尾空白与 UTF-8 BOM）。
- 上传方式：multipart 字段 `file`，或直接把 CSV 作为原始请求体（`Content-Type: text/csv`）。
- **同键覆盖**：示数以 `(meter_id, ts_utc)` 为键，批次以 `batch_no` 为键；
  同一文件内重复键后者覆盖前者。
- **整批拒收**：任何一行违规都不会写入任何一行，也不会排队重算。返回 HTTP 422，
  响应体含 `code`、`message`、`details`、`stored: false`。

### 校验规则

| 规则 | code | 说明 |
| --- | --- | --- |
| 示数不得倒退 | `reading_decreased` | 与库中现有数据合并后，同一电表按时间排序的累计值必须单调不减 |
| 同表批次不得重叠 | `batch_overlap` | 同一电表的批次区间（半开 `[start, end)`）不得相交；首尾相接允许 |
| 格式 | `bad_header` / `invalid_rows` / `invalid_encoding` / `empty_upload` / `no_data_rows` | 表头、整数、编码、空文件 |

校验与写入在同一个 `BEGIN IMMEDIATE` 事务内完成，因此并发上传不会“各自通过校验、
合起来违规”。

## 计算规则

对批次两端 `start` / `end`，用同一电表上相邻的两条已观测示数线性插值：

```
v(t) = v0 + (v1 - v0) * (t - t0) / (t1 - t0)      t0 <= t <= t1
```

若 `t` 恰好等于某条示数，直接取该值（`role = exact`）。若任一端**没有**将其包围的
前后两条示数（即早于首条或晚于末条），该批次状态为 `pending_readings`，`energy_wh`
为 `null`，`note` 说明缺哪一侧；补传示数后会自动重算。

用电量 = `v(end) - v(start)`，整数化结果为 `energy_wh`（四舍五入），同时给出未取整的
`energy_wh_exact`。响应里 `used_readings` 给出插值实际用到的示数（`lower` / `upper` /
`exact` 及时间戳），可直接手工复核。

## 一致性与并发语义

**原子成组发布。** 一次上传涉及的所有电表构成一个任务；后台把共享电表的任务
（直接或传递）合并，在**一个写事务**里一起发布。因此“同次上传涉及的电表结果一起更新”
在交叠上传时同样成立：查询永远看到某一整套已发布结果，不会看到半个任务。

**旧计算不能改回新结果。** 计算在只读快照里读取输入，随后用一条短写事务发布，发布前
逐个校验电表的 `data_version` 未变。若读取之后又有上传落库，发布被拒绝，任务重读最新
数据并重算。因此无论谁先算完，最终发布的总是基于最新确认数据的整套结果。计算期间
**不持有正式库写事务**（读用 `BEGIN` 快照，写只在最终发布那一瞬间）。

**重算期间查询。** 查询返回上一整套已发布结果，并标注 `result_pending_update: true`
与 `pending_job_id`；单批次接口在批次行内标注，电表接口在顶层与每行都标注。不会返回
部分新、部分旧的混合结果。

**失败不暴露部分更新。** 发布事务任何一步抛错都会整体回滚：旧快照仍是 `is_current`，
不产生孤儿快照行。原始上传记录（`uploads` 表保存完整 CSV 正文与 SHA-256）与待重算
信息（`jobs` / `job_meters` / `meter_state.pending`）在同一事务中保存，失败时一起回滚。

**重启继续。** 启动时把 `running` 状态的任务重置为 `queued`；仍被标记为 pending 但没有任何存活
任务覆盖的电表会补建恢复任务。任务状态全部持久化在 SQLite 中。异常反复发生的任务在
10 次尝试后标记为 `failed`（电表保持 pending 并记录 `last_error`），由下次启动或新上传
再次触发，避免热循环。

## API

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| POST | `/api/v1/uploads/readings` | 上传示数 CSV |
| POST | `/api/v1/uploads/batches` | 上传批次 CSV |
| GET | `/api/v1/progress` | 重算队列进度（任务计数、待更新电表、worker 状态） |
| GET | `/api/v1/jobs` | 最近任务（`?limit=`） |
| GET | `/api/v1/jobs/{job_id}` | 单任务详情与涉及电表 |
| GET | `/api/v1/batches/{batch_no}` | 批次用电量、所用示数、快照版本 |
| GET | `/api/v1/meters/{meter_id}/results` | 某电表整套结果（含待更新标记） |
| GET | `/api/v1/uploads` | 原始上传记录列表（不含正文） |
| GET | `/api/v1/uploads/{upload_id}` | 单条原始上传记录（含正文） |
| GET | `/health` | 健康检查 |
| GET | `/` | 交互式演示页 |
| GET | `/docs` | Swagger UI |

`GET /api/v1/batches/{batch_no}` 示例响应：

```json
{
  "batch_no": "B-2026-01-01-A",
  "definition": {"batch_no": "B-2026-01-01-A", "meter_id": "M-001",
                 "start_utc": 1767227400, "end_utc": 1767231000,
                 "start_time": "2026-01-01T00:30:00Z", "end_time": "2026-01-01T01:30:00Z"},
  "result": {
    "batch_no": "B-2026-01-01-A",
    "meter_id": "M-001",
    "status": "ok",
    "energy_wh": 3050,
    "energy_wh_exact": 3050.0,
    "start_utc": 1767227400, "end_utc": 1767231000,
    "start_time": "2026-01-01T00:30:00Z", "end_time": "2026-01-01T01:30:00Z",
    "start_value_wh": 1201700.0, "end_value_wh": 1204750.0,
    "note": null,
    "used_readings": {
      "start": [{"role": "lower", "ts_utc": 1767225600, "time": "2026-01-01T00:00:00Z", "value_wh": 1200000},
                {"role": "upper", "ts_utc": 1767229200, "time": "2026-01-01T01:00:00Z", "value_wh": 1203400}],
      "end":   [{"role": "lower", "ts_utc": 1767229200, "time": "2026-01-01T01:00:00Z", "value_wh": 1203400},
                {"role": "upper", "ts_utc": 1767232800, "time": "2026-01-01T02:00:00Z", "value_wh": 1206100}]
    },
    "result_pending_update": false,
    "pending_job_id": null,
    "snapshot_id": 4,
    "version": 3
  }
}
```

校验失败示例（HTTP 422）：

```json
{
  "status": "rejected", "code": "reading_decreased", "stored": false,
  "message": "Upload rejected: cumulative readings must never decrease. No rows from this upload were stored.",
  "details": [{"code": "reading_decreased", "meter_id": "M-001",
               "ts_utc": 1767229200, "time": "2026-01-01T01:00:00Z", "value_wh": 999999,
               "previous_ts_utc": 1767225600, "previous_time": "2026-01-01T00:00:00Z",
               "previous_value_wh": 1200000,
               "message": "meter M-001: cumulative reading decreased at ..."}]
}
```

## 测试

```bash
python3 -m pip install pytest httpx
python3 -m pytest tests -q
```

覆盖：插值数值与所用示数、缺包围示数待补、示数倒退/批次重叠整批拒收、同键覆盖重算、
重算期间返回旧结果并标注、交叠上传成组发布、晚结束旧计算不能覆盖新结果、发布失败
整体回滚、跨进程重启恢复、并发冲突上传只接纳一个、并发压力下收敛到最新数据。

## 设计边界

- 单进程单 worker 线程。多副本部署需把 `jobs` 表换成外部队列/锁服务，并保证发布
  的版本守卫仍在数据库事务内完成。
- 线性插值按需求实现；不处理电表回零/换表（示数倒退会被直接拒收）。
- 结果保留全部历史快照（`meter_snapshots` 中 `is_current = 0` 的行），便于审计；
  如需压缩可加定期清理。
