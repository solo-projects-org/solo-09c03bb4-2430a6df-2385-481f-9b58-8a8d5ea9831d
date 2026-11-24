"""FastAPI 应用：按 UTC 过磅日取文件/取数、触发发布换版，以及净重批次结算。"""

from __future__ import annotations

import fastapi
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from .config import Settings
from .netbatch import (
    NetBatchError,
    NetBatchService,
    TicketPairInput,
    TicketRef,
)
from .publisher import PublishError, publish
from .service import DatasetService
from .timeutil import parse_date

app = FastAPI(
    title="码头过磅明细按日数据集",
    version="1.1.0",
    description=(
        "按过磅时间的 UTC 日期发布/读取 Parquet；失败保留上一版清单。"
        "另提供净重批次 API：毛/皮票对按 (UTC 日, 票号) 定位核算净重。"
    ),
)


def get_settings() -> Settings:
    return Settings.from_env()


def get_service() -> DatasetService:
    return DatasetService(get_settings())


class PublishRequest(BaseModel):
    files: list[str] = Field(
        ..., min_length=1, description="相对 input_dir 的同结构 Parquet 路径清单"
    )


class OutcomeOut(BaseModel):
    input_file: str
    mode: str
    days: list[str]
    rows: int


class PublishResponse(BaseModel):
    version: int
    created_at: str
    time_column: str
    input_files: list[str]
    day_count: int
    outcomes: list[OutcomeOut]


# ------------------------------------------------------------ 净重批次模型
class TicketRefIn(BaseModel):
    date: str = Field(..., description="票的 UTC 过磅日 YYYY-MM-DD")
    ticket_id: str = Field(..., min_length=1, description="票号")


class TicketPairIn(BaseModel):
    gross: TicketRefIn = Field(..., description="毛票定位（UTC 日 + 票号）")
    tare: TicketRefIn = Field(..., description="皮票定位（UTC 日 + 票号）")


class NetBatchRequest(BaseModel):
    batch_id: str = Field(..., min_length=1, description="批号（业务唯一，重传用）")
    manifest_version: int = Field(..., ge=1, description="核对过的清单版号")
    pairs: list[TicketPairIn] = Field(..., min_length=1, description="毛/皮票对清单")


class TicketLocationOut(BaseModel):
    date: str
    ticket_id: str
    source: str
    source_file: str
    path: str
    row_index: int


class NetPairOut(BaseModel):
    gross: TicketLocationOut
    tare: TicketLocationOut
    manifest_version: int
    container_no: str
    net_weight_kg: float


class NetBatchResponse(BaseModel):
    batch_id: str
    manifest_version: int
    created_at: str
    replayed: bool = Field(..., description="true=原请求幂等重放，返回首次结果")
    pair_count: int
    pairs: list[NetPairOut]


@app.get("/health")
def health():
    service = get_service()
    manifest = service.current_manifest()
    return {
        "status": "ok",
        "current_version": manifest.version if manifest else None,
        "input_dir": str(service.settings.input_dir),
        "output_dir": str(service.settings.output_dir),
    }


@app.get("/manifest")
def get_manifest():
    manifest = get_service().current_manifest()
    if manifest is None:
        return {"message": "尚未发布任何版本", "manifest": None}
    return {"manifest": manifest.to_dict()}


@app.get("/days/{day}/files")
def list_day_files(day: str):
    try:
        parse_date(day)
    except ValueError:
        raise HTTPException(status_code=422, detail="日期格式应为 YYYY-MM-DD")
    service = get_service()
    manifest = service.current_manifest()
    if manifest is None:
        # 无版本可返回：明确告知，而不是伪造空清单
        raise HTTPException(status_code=404, detail="尚无已发布版本")
    entries = manifest.files_for_date(day)
    return {
        "version": manifest.version,
        "date": day,
        "files": [
            {
                "path": e.path,
                "source": e.source,
                "source_file": e.source_file,
                "rows": e.rows,
                "download_url": f"/days/{day}/files/{e.source}/{_b64(e.path)}",
            }
            for e in entries
        ],
    }


@app.get("/days/{day}/files/{source}/{name}")
def download_day_file(day: str, source: str, name: str):
    if source not in ("original", "split"):
        raise HTTPException(status_code=404, detail="source 仅支持 original/split")
    try:
        parse_date(day)
    except ValueError:
        raise HTTPException(status_code=422, detail="日期格式应为 YYYY-MM-DD")
    import base64

    try:
        padded = name + "=" * (-len(name) % 4)
        rel_path = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
    except Exception:
        raise HTTPException(status_code=404, detail="文件名编码无效")
    service = get_service()
    manifest = service.current_manifest()
    if manifest is None:
        raise HTTPException(status_code=404, detail="尚无已发布版本")
    for entry in manifest.files_for_date(day):
        if entry.source == source and entry.path == rel_path:
            fs_path = service.resolve(entry)
            if not fs_path.is_file():
                raise HTTPException(status_code=410, detail="清单文件已丢失")
            return FileResponse(
                fs_path,
                media_type="application/vnd.apache.parquet",
                filename=fs_path.name,
            )
    raise HTTPException(status_code=404, detail="当前版本清单中无此文件")


@app.get("/days/{day}/rows")
def read_day_rows(
    day: str,
    limit: int = Query(default=10_000, ge=1, le=1_000_000),
):
    try:
        parse_date(day)
    except ValueError:
        raise HTTPException(status_code=422, detail="日期格式应为 YYYY-MM-DD")
    service = get_service()
    if service.current_manifest() is None:
        raise HTTPException(status_code=404, detail="尚无已发布版本")
    table = service.read_date(day)
    if table is None:
        raise HTTPException(status_code=404, detail="尚无已发布版本")
    truncated = table.slice(0, limit)
    return {
        "date": day,
        "version": service.current_manifest().version,
        "rows_returned": truncated.num_rows,
        "rows_total": table.num_rows,
        "columns": table.column_names,
        "records": truncated.to_pylist(),
    }


@app.post("/publish", response_model=PublishResponse, status_code=201)
def publish_files(req: PublishRequest):
    settings = get_settings()
    try:
        result = publish(req.files, settings)
    except PublishError as exc:
        # 发布失败：current.json 未被替换，按日 API 仍读到上一版
        manifest = DatasetService(settings).current_manifest()
        raise HTTPException(
            status_code=422,
            detail={
                "error": str(exc),
                "retained_version": manifest.version if manifest else None,
            },
        )
    m = result.manifest
    return PublishResponse(
        version=m.version,
        created_at=m.created_at,
        time_column=m.time_column,
        input_files=m.input_files,
        day_count=len({d.date for d in m.days}),
        outcomes=[OutcomeOut(**vars(o)) for o in result.outcomes],
    )


# ------------------------------------------------------------ 净重批次 API
def get_net_batch_service() -> NetBatchService:
    return NetBatchService(get_settings())


def _net_batch_to_response(batch, replayed: bool) -> NetBatchResponse:
    return NetBatchResponse(
        batch_id=batch.batch_id,
        manifest_version=batch.manifest_version,
        created_at=batch.created_at,
        replayed=replayed,
        pair_count=len(batch.pairs),
        pairs=[
            NetPairOut(
                gross=TicketLocationOut(**p.gross.__dict__),
                tare=TicketLocationOut(**p.tare.__dict__),
                manifest_version=p.manifest_version,
                container_no=p.container_no,
                net_weight_kg=p.net_weight_kg,
            )
            for p in batch.pairs
        ],
    )


@app.post("/net-batches", response_model=NetBatchResponse)
def submit_net_batch(req: NetBatchRequest, response: fastapi.Response):
    # 日期格式先做 422 校验，避免把请求体问题混进业务冲突
    for idx, pair in enumerate(req.pairs, start=1):
        for role, ref in (("毛票", pair.gross), ("皮票", pair.tare)):
            try:
                parse_date(ref.date)
            except ValueError:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        f"第 {idx} 对{role}日期 {ref.date!r} 格式应为 YYYY-MM-DD"
                    ),
                )
    pairs = [
        TicketPairInput(
            gross=TicketRef(date=p.gross.date, ticket_id=p.gross.ticket_id),
            tare=TicketRef(date=p.tare.date, ticket_id=p.tare.ticket_id),
        )
        for p in req.pairs
    ]
    service = get_net_batch_service()
    try:
        batch, replayed = service.submit(
            req.batch_id, req.manifest_version, pairs
        )
    except NetBatchError as exc:
        # 整批退回：422=请求/票数据校验问题，409=冲突/换版；reasons 逐条说明
        raise HTTPException(
            status_code=exc.status_code,
            detail={
                "error": "净重批次被整批退回",
                "batch_id": req.batch_id,
                "reasons": exc.reasons,
            },
        )
    # 首次入库 201；原请求幂等重放 200
    response.status_code = 200 if replayed else 201
    return _net_batch_to_response(batch, replayed)


@app.get("/net-batches/{batch_id}", response_model=NetBatchResponse)
def get_net_batch(batch_id: str):
    batch = get_net_batch_service().get_batch(batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail=f"批号 {batch_id} 不存在")
    return _net_batch_to_response(batch, replayed=False)


def _b64(s: str) -> str:
    import base64

    return base64.urlsafe_b64encode(s.encode("utf-8")).decode("ascii").rstrip("=")
