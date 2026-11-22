"""批次重放接口：采集回执登记、失败分段凭据、重放派发/中断/恢复与核对。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from app.services.batch_replay import batch_replay

router = APIRouter(prefix="/api/batch-replay", tags=["批次重放"])


class ReceiptBatchPayload(BaseModel):
    receipts: list[dict[str, Any]] = Field(default_factory=list)


@router.post("/receipts")
def submit_receipt(payload: dict[str, Any]) -> dict[str, Any]:
    """登记一条采集回执；重复回执只累加投递次数，不重复产生待处理记录。"""
    return batch_replay.submit_receipt(payload)


@router.post("/receipts/batch")
def submit_receipts(payload: ReceiptBatchPayload) -> dict[str, Any]:
    return batch_replay.submit_receipts(payload.receipts)


@router.get("/receipts")
def list_receipts(
    batch_id: str | None = Query(default=None),
    segment_id: str | None = Query(default=None),
) -> dict[str, Any]:
    return batch_replay.list_receipts(batch_id, segment_id)


@router.get("/credentials")
def list_credentials(
    batch_id: str | None = Query(default=None),
    include_legacy: bool = Query(default=True),
) -> dict[str, Any]:
    return batch_replay.list_credentials(batch_id, include_legacy=include_legacy)


@router.get("/events")
def list_events(
    batch_id: str | None = Query(default=None),
    event_type: str | None = Query(default=None),
) -> dict[str, Any]:
    return batch_replay.list_events(batch_id, event_type)


@router.post("/dispatch")
def dispatch(
    batch_id: str = Query(...),
    limit: int | None = Query(default=None),
    segment_ids: str | None = Query(default=None, description="逗号分隔的分段编号，缺省派发全部"),
) -> dict[str, Any]:
    return batch_replay.dispatch(batch_id, limit=limit, segment_ids=segment_ids)


@router.post("/interrupt")
def interrupt(batch_id: str = Query(...), reason: str = Query(default="")) -> dict[str, Any]:
    return batch_replay.interrupt(batch_id, reason=reason)


@router.post("/resume")
def resume(batch_id: str = Query(...)) -> dict[str, Any]:
    return batch_replay.resume(batch_id)


@router.get("/reconciliation")
def reconciliation(batch_id: str = Query(...)) -> dict[str, Any]:
    return batch_replay.reconciliation(batch_id)


@router.get("/snapshot")
def snapshot() -> dict[str, Any]:
    return batch_replay.domain_snapshot()
