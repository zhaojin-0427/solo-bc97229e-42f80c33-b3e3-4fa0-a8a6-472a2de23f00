"""FastAPI application: CSV upload endpoints, queries and progress."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, File, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse

from . import service
from .csv_ingest import UploadRejected, parse_batches_csv, parse_readings_csv
from .db import get_db
from .worker import Worker, get_worker, set_worker

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("carbon.api")

DEMO_PAGE = Path(__file__).with_name("demo.html")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db = get_db()
    worker = get_worker()
    worker.start()
    log.info("recompute worker started; db=%s", db.path)
    try:
        yield
    finally:
        worker.stop()
        set_worker(None)


app = FastAPI(
    title="工厂批次用电量核算 API",
    version="1.0.0",
    description=(
        "将累计电表示数加工为生产批次用电量，作为碳核算输入。\n\n"
        "* 上传示数 CSV 与批次 CSV，同键覆盖；示数不得倒退，同表批次不得重叠，"
        "违规整批拒收。\n"
        "* 相邻示数线性插值求批次两端读数，相减得用电量；缺少包围读数时标为待补数。\n"
        "* 补传或更正触发后台重算；重算期间查询返回整套旧结果并注明待更新。\n"
        "* 同次上传涉及的电表结果一起原子发布；旧计算不能覆盖新数据。\n"
        "* 查询进度：`GET /api/v1/progress`。"
    ),
    lifespan=lifespan,
)


@app.exception_handler(UploadRejected)
async def upload_rejected_handler(request: Request, exc: UploadRejected):
    return JSONResponse(
        status_code=422,
        content={
            "status": "rejected",
            "code": exc.code,
            "message": exc.message,
            "details": exc.details,
            "stored": False,
        },
    )


async def _read_upload(file: UploadFile | None, request: Request) -> tuple[bytes, str | None]:
    if file is not None:
        return await file.read(), file.filename
    raw = await request.body()
    if raw:
        return raw, None
    raise UploadRejected(
        "empty_upload",
        "Send a CSV file as multipart field 'file' (or as the raw request body).",
    )


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def demo() -> HTMLResponse:
    return HTMLResponse(DEMO_PAGE.read_text(encoding="utf-8"))


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.post(
    "/api/v1/uploads/readings",
    tags=["uploads"],
    summary="上传累计电表示数 CSV（meter_id, ts_utc, value_wh）",
)
async def upload_readings(
    request: Request, file: UploadFile | None = File(default=None)
) -> JSONResponse:
    raw, filename = await _read_upload(file, request)
    upload = parse_readings_csv(raw, filename)
    result = service.accept_readings(get_db(), upload)
    get_worker().wake()
    return JSONResponse(status_code=202, content=result)


@app.post(
    "/api/v1/uploads/batches",
    tags=["uploads"],
    summary="上传生产批次 CSV（batch_no, meter_id, start_utc, end_utc）",
)
async def upload_batches(
    request: Request, file: UploadFile | None = File(default=None)
) -> JSONResponse:
    raw, filename = await _read_upload(file, request)
    upload = parse_batches_csv(raw, filename)
    result = service.accept_batches(get_db(), upload)
    get_worker().wake()
    return JSONResponse(status_code=202, content=result)


@app.get("/api/v1/progress", tags=["queries"], summary="重算队列进度")
async def progress() -> dict:
    return {
        "queue": service.get_progress(get_db()),
        "worker": get_worker().status(),
    }


@app.get("/api/v1/jobs", tags=["queries"], summary="最近的后台重算任务")
async def jobs(limit: int = 50) -> dict:
    return service.list_jobs(get_db(), limit=min(max(limit, 1), 500))


@app.get("/api/v1/jobs/{job_id}", tags=["queries"], summary="单个重算任务详情")
async def job_detail(job_id: int):
    job = service.get_job(get_db(), job_id)
    if job is None:
        return JSONResponse(status_code=404, content={"message": "job not found"})
    return job


@app.get(
    "/api/v1/batches/{batch_no}",
    tags=["queries"],
    summary="批次用电量及所用示数",
)
async def batch_result(batch_no: str):
    data = service.get_batch(get_db(), batch_no)
    if data is None:
        return JSONResponse(status_code=404, content={"message": "batch not found"})
    return data


@app.get(
    "/api/v1/meters/{meter_id}/results",
    tags=["queries"],
    summary="某电表的整套批次结果（含待更新标记）",
)
async def meter_results(meter_id: str) -> dict:
    return service.list_meter_results(get_db(), meter_id)


@app.get("/api/v1/uploads", tags=["queries"], summary="原始上传记录列表（不含正文）")
async def uploads(limit: int = 50) -> dict:
    items = service.list_uploads(get_db(), limit=min(max(limit, 1), 500))
    return {"count": len(items), "uploads": items}


@app.get("/api/v1/uploads/{upload_id}", tags=["queries"], summary="原始上传记录")
async def upload_detail(upload_id: int):
    data = service.get_upload(get_db(), upload_id)
    if data is None:
        return JSONResponse(status_code=404, content={"message": "upload not found"})
    return data


def create_worker_for_tests(db_path: str) -> Worker:  # pragma: no cover - helper
    return Worker(get_db())
