"""部分失败批次回执去重、失败分段重放凭据、重放中断恢复与核对。

业务背景
--------
请求批次以“采集分段”为单位执行采集，部分失败时会回送采集回执。真实环境里：

* 同一条回执可能重复到达（至少一次投递语义），不能因此给同一失败请求重复派发；
* 同一个采集分段也可能用新的回执重送，历史来源必须继续可核对；
* 同一端点下可能同时存在多个真实缺口，按端点同步会错误地把两个分段合并；
* 一条分段里不同请求即使失败原因完全一致，也各自是一次独立恢复机会；
* 重放可能中途中断，恢复时只能依赖历史采集回执重建待补范围。

本模块把两类记录分开保存：

1. ``batch_receipt``：回执台账。每条不同来源的回执一行，内容只追加不改写；
   重复投递只累加 ``投递次数`` 并追加 ``投递记录``，不会新增待处理记录。
2. ``retry_queue`` 中 ``凭据类型 = 失败分段`` 的记录：重放凭据。按
   ``(批次, 分段)`` 唯一，不按端点合并；``待补范围`` 按请求编号去重，
   独立请求即使原因相同也各占一个编号。

失败请求的身份判定
--------------------
* 回执带请求编号时按请求编号去重，跨回执、跨来源都只算一次；
* 回执未带请求编号时，身份在“采集分段”范围内按（失败原因, 同原因出现序号）
  稳定生成：同一分段重送同一匿名失败不会重复派发，同一条回执里两个原因相同
  的独立失败则各占一个序号、各自保留恢复机会，绝不按失败原因合并成一条。

所有派发、中断、恢复与覆盖回写都会写 ``replay_event`` 审计表，并同步回写
``request_batch`` 的请求/完成/失败数量。
"""
from __future__ import annotations

import hashlib
import json
import re
import threading
from datetime import datetime
from typing import Any

from app.store import store

RECEIPT_TABLE = "batch_receipt"
EVENT_TABLE = "replay_event"

SEGMENT_CREDENTIAL = "失败分段"
LEGACY_CREDENTIAL = "批次汇总"

STATUS_PENDING = "待重试"
STATUS_RUNNING = "重试中"
STATUS_DONE = "已成功"
STATUS_DEAD = "永久失败"

QUEUE_PENDING = "失败分段待重放"
QUEUE_COVERED = "已覆盖"
QUEUE_INTERRUPTED = "中断待恢复"
QUEUE_RECOVERED = "从历史回执恢复"

_LOCK = threading.RLock()

# 采集端字段命名并不统一，这里统一兼容中英文与常见驼峰写法。
_BATCH_KEYS = ("batch_id", "batchId", "batch_no", "batchNo", "批次编号", "批次", "batch")
_SEGMENT_KEYS = ("segment_id", "segmentId", "segment_no", "segmentNo", "分段编号", "采集分段", "分段", "segment")
_RECEIPT_ID_KEYS = ("receipt_id", "receiptId", "receipt_no", "receiptNo", "回执编号", "回执号", "回执ID")
_ENDPOINT_KEYS = ("endpoint", "采集端点", "接口端点", "失败端点", "目标端点", "url")
_SOURCE_KEYS = ("source", "采集来源", "来源", "数据来源", "collector")
_RESULT_KEYS = ("result", "采集结果", "结果", "receipt_status")
_FAILURE_KEYS = ("failures", "failed_requests", "failedRequests", "失败请求", "失败明细", "failed")
_SUCCESS_KEYS = ("successes", "success_requests", "successRequests", "成功请求", "成功明细", "succeeded")
_SCOPE_KEYS = ("scope_total", "total_requests", "totalRequests", "请求数量", "采集范围", "total")
_REQUESTS_KEYS = ("requests", "items", "明细", "请求明细")
_REQUEST_ID_KEYS = ("request_id", "requestId", "请求编号", "请求ID", "id", "编号")
_REASON_KEYS = ("reason", "error", "失败原因", "原因", "错误信息", "message")
_RECEIVED_AT_KEYS = ("received_at", "receivedAt", "回执时间", "采集时间", "timestamp")

_SUCCESS_WORDS = {"成功", "success", "ok", "succeeded", "passed", "已完成", "已成功", "200"}
_FAILURE_WORDS = {"失败", "failed", "failure", "error", "异常", "超时", "timeout", "部分失败"}


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _pick(payload: dict[str, Any], keys: tuple[str, ...], default: Any = None) -> Any:
    for key in keys:
        if key in payload:
            value = payload[key]
            if value is not None and value != "":
                return value
    return default


def _as_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return default


def _unique(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value)
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return result


def _next_id(table: str) -> int:
    rows = store.rows(table)
    return max((_as_int(row.get("id"), 0) for row in rows), default=0) + 1


def _sanitize(value: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z_\-一-鿿]+", "-", str(value).strip())
    return cleaned.strip("-") or "NA"


def _append_delivery(receipt: dict[str, Any], receipt_id: str, source: str, at: str,
                     signature: str, duplicate: bool, note: str = "") -> None:
    """每次到达都留一条可核对的投递记录；投递记录只追加，不覆盖历史来源。"""
    log = receipt.setdefault("投递记录", [])
    log.append({
        "回执编号": receipt_id,
        "采集来源": source,
        "时间": at,
        "内容签名": signature[:12],
        "重复": duplicate,
        "说明": note,
    })
    sources = _unique(list(receipt.get("来源集合", []) or []) + [source])
    receipt["来源集合"] = sources
    receipt["采集来源"] = sources[-1] if sources else source


# ---------------------------------------------------------------------------
# 回执入参归一化
# ---------------------------------------------------------------------------

def _failure_entries(raw: Any) -> list[tuple[str | None, str]]:
    """把各种失败明细写法归一为 ``[(请求编号或None, 失败原因), ...]``。"""
    if raw is None:
        return []
    if isinstance(raw, dict):
        if any(key in raw for key in _REQUEST_ID_KEYS):
            items: list[Any] = [raw]
        else:
            items = [{"request_id": key, "reason": value} for key, value in raw.items()]
    elif isinstance(raw, bool):
        return []
    elif isinstance(raw, (int, float)):
        items = [{"reason": "未注明失败原因"} for _ in range(max(0, int(raw)))]
    elif isinstance(raw, str):
        pieces = [piece.strip() for piece in re.split(r"[,，;；\n]", raw) if piece.strip()]
        items = pieces or [raw]
    elif isinstance(raw, (list, tuple, set)):
        items = list(raw)
    else:
        return []

    entries: list[tuple[str | None, str]] = []
    for entry in items:
        if isinstance(entry, dict):
            request_id = _pick(entry, _REQUEST_ID_KEYS)
            reason = _pick(entry, _REASON_KEYS, "未注明失败原因")
            if isinstance(reason, (dict, list)):
                reason = json.dumps(reason, ensure_ascii=False, sort_keys=True)
        else:
            request_id = None
            reason = entry
        reason = str(reason).strip() if reason not in (None, "") else "未注明失败原因"
        if request_id in (None, ""):
            entries.append((None, reason))
        else:
            entries.append((str(request_id).strip(), reason))
    return entries


def _canonicalize(entries: list[tuple[str | None, str]]) -> list[dict[str, Any]]:
    """生成与投递无关的规范身份：显式请求编号优先，匿名失败按原因序号区分。"""
    occurrences: dict[str, int] = {}
    seen: set[str] = set()
    canonical: list[dict[str, Any]] = []
    for request_id, reason in entries:
        if request_id is not None:
            base = f"id:{request_id}"
        else:
            index = occurrences.get(reason, 0)
            occurrences[reason] = index + 1
            base = f"anon:{reason}#{index}"
        if base in seen:
            # 同一条请求在一条回执里重复出现，只记一次。
            continue
        seen.add(base)
        canonical.append({
            "规范键": base,
            "请求编号": request_id,
            "失败原因": reason,
        })
    return canonical


def _decorate(canonical: list[dict[str, Any]], segment: str, digest: str) -> list[dict[str, Any]]:
    """把规范身份落成凭据里可去重的请求编号。

    显式请求编号直接使用，跨回执、跨来源都只算一次。匿名失败的身份由
    “分段 + 回执内容摘要 + 失败原因 + 同原因序号”构成：同一采集分段重送同一
    内容会命中同一批编号，因此不会重复派发；而同一条回执里原因相同的多个独立
    失败各自占一个序号，不同回执的匿名失败也各自保留恢复机会。
    """
    decorated: list[dict[str, Any]] = []
    for item in canonical:
        request_id = item.get("请求编号")
        if request_id in (None, ""):
            request_id = f"匿名:{segment}:{digest}:{item['规范键']}"
        decorated.append({
            "请求编号": str(request_id),
            "失败原因": str(item.get("失败原因", "未注明失败原因")),
            "规范键": str(item.get("规范键", "")),
        })
    return decorated


def _content_digest(canonical: list[dict[str, Any]], successes: list[str],
                    scope: int, endpoint: str) -> str:
    """回执内容摘要：刻意不含采集来源与回执号，用于跨来源识别同一批失败。"""
    payload = {
        "endpoint": endpoint,
        "scope": scope,
        "failures": sorted(str(item.get("规范键", "")) for item in canonical),
        "successes": sorted(successes),
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]


def _successes(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, dict):
        items: list[Any] = list(raw.keys()) if not any(k in raw for k in _REQUEST_ID_KEYS) else [raw]
    elif isinstance(raw, bool):
        return []
    elif isinstance(raw, (int, float)):
        return [f"anon:成功#{index}" for index in range(max(0, int(raw)))]
    elif isinstance(raw, str):
        pieces = [piece.strip() for piece in re.split(r"[,，;；\n]", raw) if piece.strip()]
        items = pieces or [raw]
    elif isinstance(raw, (list, tuple, set)):
        items = list(raw)
    else:
        return []
    result: list[str] = []
    seen: set[str] = set()
    for entry in items:
        if isinstance(entry, dict):
            request_id = _pick(entry, _REQUEST_ID_KEYS)
        else:
            request_id = entry
        if request_id in (None, ""):
            continue
        text = str(request_id)
        if text not in seen:
            seen.add(text)
            result.append(text)
    return result


def _split_request_details(raw: Any) -> tuple[list[Any], list[str]]:
    """支持 ``requests``/``明细`` 里混排成功与失败请求。"""
    if not isinstance(raw, (list, tuple)):
        return [], []
    failures: list[Any] = []
    successes: list[str] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        status = str(_pick(entry, ("status", "result", "结果", "状态"), "失败")).strip()
        request_id = _pick(entry, _REQUEST_ID_KEYS)
        if status in _SUCCESS_WORDS or status.lower() in _SUCCESS_WORDS:
            if request_id not in (None, ""):
                successes.append(str(request_id))
        else:
            failures.append(entry)
    return failures, successes


def _receipt_signature(endpoint: str, source: str, scope: int,
                       canonical: list[dict[str, Any]], successes: list[str]) -> str:
    payload = {
        "endpoint": endpoint,
        "source": source,
        "scope": scope,
        "failures": sorted(
            [str(item.get("规范键", "")), str(item.get("失败原因", ""))]
            for item in canonical
        ),
        "successes": sorted(successes),
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()


# ---------------------------------------------------------------------------
# 批次、回执与凭据查询
# ---------------------------------------------------------------------------

def _resolve_batch(payload: dict[str, Any], create_ok: bool = False) -> tuple[dict[str, Any] | None, bool, str]:
    """按批次编号或记录 ID 定位批次；登记回执时可补建，保证采集端可回送。"""
    reference = _pick(payload, _BATCH_KEYS)
    if reference in (None, ""):
        return None, False, "缺少批次编号（batch_id/批次编号）"
    text = str(reference).strip()

    rows = store.rows("request_batch")
    for row in rows:
        if str(row.get("批次编号", "")).strip() == text:
            return row, False, ""
    if text.isdigit():
        row = store.find("request_batch", int(text))
        if row is not None:
            return row, False, ""
    if not create_ok:
        return None, False, f"批次{text}不存在"

    endpoint = str(_pick(payload, _ENDPOINT_KEYS, "") or "")
    segment = str(_pick(payload, _SEGMENT_KEYS, "") or "")
    created = {
        "id": _next_id("request_batch"),
        "status": "部分失败",
        "pending": True,
        "abnormal": True,
        "批次编号": text,
        "关联端点": endpoint,
        "参数模板": f"receipt:{segment or '-'}",
        "请求数量": 0,
        "并发控制": "receipt-driven",
        "超时上限": 5000,
        "完成数量": 0,
        "失败数量": 0,
        "批次来源": "回执登记自动补建",
    }
    rows.append(created)
    return created, True, ""


def _find_credential(batch_id: int, segment: str) -> dict[str, Any] | None:
    for row in store.rows("retry_queue"):
        if (
            row.get("凭据类型") == SEGMENT_CREDENTIAL
            and _as_int(row.get("批次记录ID"), -1) == batch_id
            and str(row.get("分段编号", "")) == segment
        ):
            return row
    return None


def _credentials_for(batch_id: int) -> list[dict[str, Any]]:
    rows = [
        row for row in store.rows("retry_queue")
        if row.get("凭据类型") == SEGMENT_CREDENTIAL
        and _as_int(row.get("批次记录ID"), -1) == batch_id
    ]
    rows.sort(key=lambda row: str(row.get("分段编号", "")))
    return rows


def _receipts_for(batch_id: int, segment: str | None = None) -> list[dict[str, Any]]:
    receipts = [
        row for row in store.rows(RECEIPT_TABLE)
        if _as_int(row.get("批次记录ID"), -1) == batch_id
    ]
    if segment is not None:
        receipts = [row for row in receipts if str(row.get("分段编号", "")) == segment]
    receipts.sort(key=lambda row: _as_int(row.get("id"), 0))
    return receipts


def _segment_evidence(batch_id: int, segment: str) -> tuple[dict[str, dict[str, Any]], set[str]]:
    """从历史回执重建某个分段的失败证据与已被回执确认成功的请求。"""
    failures: dict[str, dict[str, Any]] = {}
    successes: set[str] = set()
    for receipt in _receipts_for(batch_id, segment):
        for item in receipt.get("失败明细", []) or []:
            request_id = str(item.get("请求编号", ""))
            if not request_id:
                continue
            entry = failures.setdefault(
                request_id, {"请求编号": request_id, "失败原因": [], "来源": [], "回执编号": []}
            )
            reason = str(item.get("失败原因", "未注明失败原因"))
            if reason not in entry["失败原因"]:
                entry["失败原因"].append(reason)
        for request_id in receipt.get("成功明细", []) or []:
            successes.add(str(request_id))
    return failures, successes


def _recovered_from_events(batch_id: int, segment: str) -> list[str]:
    """派发审计也是历史证据：即使凭据被重建，也不重复派发已覆盖请求。"""
    recovered: list[str] = []
    for event in store.rows(EVENT_TABLE):
        if (
            event.get("类型") == "派发"
            and _as_int(event.get("批次记录ID"), -1) == batch_id
            and str(event.get("分段编号", "")) == segment
        ):
            recovered.extend(str(item) for item in event.get("请求范围", []) or [])
    return _unique(recovered)


def _task_number(batch: dict[str, Any], segment: str) -> str:
    base = f"RETR-SEG-{_sanitize(str(batch.get('批次编号', batch.get('id'))))}-{_sanitize(segment)}"
    existing = {str(row.get("任务编号", "")) for row in store.rows("retry_queue")}
    candidate = base
    suffix = 2
    while candidate in existing:
        candidate = f"{base}-{suffix}"
        suffix += 1
    return candidate


def _summary_reason(failures: dict[str, dict[str, Any]]) -> str:
    reasons: list[str] = []
    for entry in failures.values():
        for reason in entry.get("失败原因", []):
            if reason not in reasons:
                reasons.append(reason)
    detail = "；".join(reasons[:3]) or "回执未注明失败原因"
    more = f" 等{len(reasons)}类" if len(reasons) > 3 else ""
    return f"实际失败{len(failures)}项：{detail}{more}"


def _append_event(
    *,
    batch: dict[str, Any],
    segment: str,
    event_type: str,
    requests: list[str],
    credential: dict[str, Any] | None = None,
    note: str = "",
) -> dict[str, Any]:
    row = {
        "id": _next_id(EVENT_TABLE),
        "批次记录ID": batch["id"],
        "批次编号": batch.get("批次编号", ""),
        "分段编号": segment,
        "任务编号": (credential or {}).get("任务编号", ""),
        "类型": event_type,
        "请求范围": list(requests),
        "数量": len(requests),
        "说明": note,
        "时间": _now(),
    }
    store.rows(EVENT_TABLE).append(row)
    return row


# ---------------------------------------------------------------------------
# 凭据维护与批次统计
# ---------------------------------------------------------------------------

def _rebuild_credential(batch: dict[str, Any], segment: str) -> dict[str, Any]:
    """按历史回执重建/刷新一个分段凭据，包含待补、已恢复与回执消除范围。"""
    batch_id = int(batch["id"])
    credential = _find_credential(batch_id, segment)
    if credential is None:
        endpoint = ""
        for receipt in _receipts_for(batch_id, segment):
            endpoint = str(receipt.get("采集端点", "") or "")
            if endpoint:
                break
        credential = {
            "id": _next_id("retry_queue"),
            "status": STATUS_PENDING,
            "pending": True,
            "abnormal": False,
            "任务编号": _task_number(batch, segment),
            "失败端点": endpoint or str(batch.get("关联端点", "")),
            "失败原因": "",
            "重试次数": 0,
            "最大重试": 3,
            "退避策略": "exponential:2s/baseline",
            "最后重试": "",
            "队列状态": QUEUE_PENDING,
            "凭据类型": SEGMENT_CREDENTIAL,
            "批次编号": batch.get("批次编号", ""),
            "批次记录ID": batch_id,
            "批次来源": batch.get("批次来源", "请求批次"),
            "分段编号": segment,
            "采集来源集合": [],
            "回执编号集合": [],
            "回执数量": 0,
            "失败请求总数": 0,
            "待补范围": [],
            "已恢复范围": [],
            "回执消除范围": [],
            "失败原因集合": [],
            "派发次数": 0,
            "最近派发": "",
            "中断次数": 0,
            "最近中断": "",
            "历史来源": [],
        }
        store.rows("retry_queue").append(credential)

    receipts = _receipts_for(batch_id, segment)
    failures, succeeded = _segment_evidence(batch_id, segment)

    sources = _unique([
        str(source)
        for row in receipts
        for source in (list(row.get("来源集合", []) or []) or [row.get("采集来源", "") or "未注明来源"])
    ])
    receipt_numbers = _unique([str(row.get("回执编号", "")) for row in receipts if row.get("回执编号")])
    history: list[dict[str, Any]] = []
    for receipt in receipts:
        history.append({
            "回执编号": receipt.get("回执编号", ""),
            "来源集合": list(receipt.get("来源集合", []) or []),
            "采集结果": receipt.get("采集结果", ""),
            "请求数量": receipt.get("请求数量", 0),
            "投递次数": _as_int(receipt.get("投递次数"), 1),
            "失败明细": list(receipt.get("失败明细", []) or []),
            "成功明细": list(receipt.get("成功明细", []) or []),
            "首次接收": receipt.get("首次接收", ""),
            "最近投递": receipt.get("最近投递", ""),
        })

    recovered = set(_recovered_from_events(batch_id, segment))
    recovered.update(str(item) for item in credential.get("已恢复范围", []) or [])
    eliminated: list[str] = []
    pending: list[str] = []
    for request_id in failures:
        if request_id in recovered:
            continue
        if request_id in succeeded:
            eliminated.append(request_id)
        else:
            pending.append(request_id)

    reason_set: list[str] = []
    for entry in failures.values():
        for reason in entry.get("失败原因", []):
            if reason not in reason_set:
                reason_set.append(reason)

    endpoint = str(credential.get("失败端点", "") or batch.get("关联端点", ""))
    if not endpoint:
        for receipt in receipts:
            if receipt.get("采集端点"):
                endpoint = str(receipt["采集端点"])
                break

    credential["失败端点"] = endpoint
    credential["批次来源"] = batch.get("批次来源", credential.get("批次来源", "请求批次"))
    credential["采集来源集合"] = sources
    credential["回执编号集合"] = receipt_numbers
    credential["回执数量"] = len(receipts)
    credential["失败请求总数"] = len(failures)
    credential["待补范围"] = pending
    credential["已恢复范围"] = sorted(recovered)
    credential["回执消除范围"] = eliminated
    credential["失败原因集合"] = reason_set
    credential["失败原因"] = _summary_reason(failures)
    credential["历史来源"] = history
    credential["待补数量"] = len(pending)
    credential["已恢复数量"] = len(recovered)
    credential["最近刷新"] = _now()

    if pending:
        was_interrupted = credential.get("队列状态") == QUEUE_INTERRUPTED
        credential["status"] = STATUS_PENDING
        credential["pending"] = True
        credential["abnormal"] = True
        credential["队列状态"] = QUEUE_INTERRUPTED if was_interrupted else QUEUE_PENDING
    else:
        credential["status"] = STATUS_DONE
        credential["pending"] = False
        credential["abnormal"] = False
        credential["队列状态"] = QUEUE_COVERED
    return credential


def _mark_legacy_superseded(batch: dict[str, Any]) -> list[dict[str, Any]]:
    """已有批次曾把部分失败汇总成一条按端点的重放任务，这里只做历史标记。"""
    legacy: list[dict[str, Any]] = []
    legacy_no = f"RETR-BATCH-{str(batch['id']).zfill(4)}"
    credentials = _credentials_for(int(batch["id"]))
    for row in store.rows("retry_queue"):
        if row.get("凭据类型") == SEGMENT_CREDENTIAL:
            continue
        is_legacy = (
            str(row.get("任务编号", "")) == legacy_no
            or (
                row.get("历史汇总")
                and _as_int(row.get("批次记录ID"), -1) == int(batch["id"])
            )
        )
        if not is_legacy:
            continue
        row["历史汇总"] = True
        row["凭据类型"] = LEGACY_CREDENTIAL
        row["批次记录ID"] = batch["id"]
        row["批次编号"] = batch.get("批次编号", "")
        row["被分段凭据接管"] = True
        row["接管凭据"] = [item.get("任务编号", "") for item in credentials]
        row["队列状态"] = "历史批次汇总(由失败分段凭据接管)"
        legacy.append(row)
    return legacy


def _segment_scopes(batch_id: int) -> dict[str, int]:
    """每个分段的实际采集范围取该分段各次回执的最大请求数量，分段之间求和。"""
    scopes: dict[str, int] = {}
    for receipt in _receipts_for(batch_id):
        segment = str(receipt.get("分段编号", ""))
        scopes[segment] = max(scopes.get(segment, 0), _as_int(receipt.get("请求数量"), 0))
    return scopes


def _writeback_batch(batch: dict[str, Any]) -> dict[str, Any]:
    """覆盖结果回写批次统计：请求数量与失败数量始终对齐实际采集范围。"""
    batch_id = int(batch["id"])
    receipts = _receipts_for(batch_id)
    credentials = _credentials_for(batch_id)

    if receipts:
        scope_total = sum(_segment_scopes(batch_id).values())
    else:
        scope_total = _as_int(batch.get("请求数量"), 0)

    pending: list[str] = []
    recovered: list[str] = []
    for credential in credentials:
        pending.extend(str(item) for item in credential.get("待补范围", []) or [])
        recovered.extend(str(item) for item in credential.get("已恢复范围", []) or [])
    pending = _unique(pending)
    recovered = _unique(recovered)

    completed = max(0, scope_total - len(pending)) if scope_total else len(recovered)
    batch["请求数量"] = scope_total
    batch["完成数量"] = completed
    batch["失败数量"] = len(pending)
    batch["待补数量"] = len(pending)
    batch["统计来源"] = "batch_receipt"
    batch["统计回写时间"] = _now()
    batch["stats"] = {
        "请求数量": scope_total,
        "完成数量": completed,
        "失败数量": len(pending),
        "已恢复数量": len(recovered),
    }
    if pending:
        batch["status"] = "部分失败"
        batch["pending"] = True
        batch["abnormal"] = True
    elif receipts:
        batch["status"] = "已完成"
        batch["pending"] = False
        batch["abnormal"] = False
    return {
        "请求数量": scope_total,
        "完成数量": completed,
        "失败数量": len(pending),
        "已恢复数量": len(recovered),
        "分段数量": len(credentials),
        "分段范围": _segment_scopes(batch_id),
    }


def _dispatch_locked(batch: dict[str, Any], segment_ids: list[str] | None = None) -> list[dict[str, Any]]:
    credentials = _credentials_for(int(batch["id"]))
    if segment_ids is not None:
        wanted = {str(item) for item in segment_ids}
        credentials = [row for row in credentials if str(row.get("分段编号", "")) in wanted]

    events: list[dict[str, Any]] = []
    for credential in credentials:
        pending = _unique(list(credential.get("待补范围", []) or []))
        if not pending:
            continue
        attempt = _as_int(credential.get("派发次数"), 0) + 1
        recovered = _unique(list(credential.get("已恢复范围", []) or []) + pending)
        credential["已恢复范围"] = recovered
        credential["已恢复数量"] = len(recovered)
        credential["待补范围"] = []
        credential["待补数量"] = 0
        credential["派发次数"] = attempt
        credential["重试次数"] = attempt
        credential["最近派发"] = _now()
        credential["最后重试"] = _now()
        credential["status"] = STATUS_DONE
        credential["pending"] = False
        credential["abnormal"] = False
        credential["队列状态"] = QUEUE_COVERED
        events.append(_append_event(
            batch=batch,
            segment=str(credential.get("分段编号", "")),
            event_type="派发",
            requests=pending,
            credential=credential,
            note=f"第{attempt}次派发，覆盖{len(pending)}项失败请求",
        ))
    _writeback_batch(batch)
    return events


# ---------------------------------------------------------------------------
# 服务门面
# ---------------------------------------------------------------------------

class BatchReplayService:
    """部分失败批次回执去重、失败分段凭据、重放中断恢复与核对。"""

    MODULE = "batch_replay"
    ENTITY = "批次重放"
    OPERATION_TERM = "重放"

    # -- 回执登记 ---------------------------------------------------------
    def submit_receipt(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload = dict(payload or {})
        nested = payload.get("receipt")
        if isinstance(nested, dict):
            merged: dict[str, Any] = dict(nested)
            merged.update({key: value for key, value in payload.items() if key != "receipt"})
            payload = merged

        with _LOCK:
            batch, batch_created, error = _resolve_batch(payload, create_ok=True)
            if batch is None:
                return {"ok": False, "message": error}
            segment = _pick(payload, _SEGMENT_KEYS)
            if segment in (None, ""):
                return {"ok": False, "message": "缺少采集分段编号（segment_id/分段编号），不能按端点合并"}
            segment = str(segment).strip()

            endpoint = str(_pick(payload, _ENDPOINT_KEYS, "") or batch.get("关联端点", "") or "")
            source = str(_pick(payload, _SOURCE_KEYS, "") or "未注明来源")
            receipt_id = _pick(payload, _RECEIPT_ID_KEYS)

            detail_failures, detail_successes = _split_request_details(_pick(payload, _REQUESTS_KEYS))
            raw_failures = _pick(payload, _FAILURE_KEYS)
            if raw_failures is None and detail_failures:
                raw_failures = detail_failures
            canonical = _canonicalize(_failure_entries(raw_failures))
            successes = _unique(_successes(_pick(payload, _SUCCESS_KEYS)) + detail_successes)

            scope_total = _pick(payload, _SCOPE_KEYS)
            scope_total = _as_int(scope_total, -1) if scope_total not in (None, "") else -1
            if scope_total < 0:
                actual = len(canonical) + len(successes)
                scope_total = actual if actual else _as_int(batch.get("请求数量"), 0)

            result = _pick(payload, _RESULT_KEYS)
            if result in (None, ""):
                result = "部分失败" if canonical else ("成功" if successes else "无失败")
            result = str(result)
            received_at = str(_pick(payload, _RECEIVED_AT_KEYS, "") or _now())
            signature = _receipt_signature(endpoint, source, scope_total, canonical, successes)
            digest = _content_digest(canonical, successes, scope_total, endpoint)

            explicit_id = None if receipt_id in (None, "") else str(receipt_id).strip()
            existing: dict[str, Any] | None = None
            dedupe_reason = ""
            batch_receipts = _receipts_for(int(batch["id"]), segment)
            if explicit_id is not None:
                # 同一回执编号在同一采集分段内重复到达：视为同一份回执。
                existing = next(
                    (row for row in batch_receipts
                     if str(row.get("回执编号", "")) == explicit_id),
                    None,
                )
                if existing is not None:
                    dedupe_reason = "同一回执编号重复到达"
            else:
                # 未带回执号时按内容签名去重：连续或再次提交同一内容都只记一次。
                existing = next(
                    (row for row in batch_receipts
                     if not row.get("显式回执编号")
                     and row.get("内容签名") == signature),
                    None,
                )
                if existing is not None:
                    dedupe_reason = "提交同一回执内容"
                # 无回执号时按内容生成稳定编号，便于核对投递历史。
                digest = signature[:12]
                receipt_id = f"RCPT-{_sanitize(segment)}-{digest}"
            receipt_id = str(receipt_id)

            if existing is not None:
                return self._merge_delivery(
                    batch=batch,
                    segment=segment,
                    existing=existing,
                    canonical=canonical,
                    successes=successes,
                    scope_total=scope_total,
                    signature=signature,
                    source=source,
                    received_at=received_at,
                    receipt_id=receipt_id,
                    batch_created=batch_created,
                    dedupe_reason=dedupe_reason,
                    digest=digest,
                )

            receipt_row_id = _next_id(RECEIPT_TABLE)
            receipt = {
                "id": receipt_row_id,
                "status": "已登记",
                "pending": False,
                "abnormal": bool(canonical),
                "回执编号": receipt_id,
                "显式回执编号": explicit_id is not None,
                "内容签名": signature,
                "批次编号": batch.get("批次编号", ""),
                "批次记录ID": batch["id"],
                "分段编号": segment,
                "采集端点": endpoint,
                "采集来源": source,
                "来源集合": [source],
                "采集结果": result,
                "请求数量": scope_total,
                "成功数量": len(successes),
                "失败数量": len(canonical),
                "失败明细": _decorate(canonical, segment, digest),
                "成功明细": successes,
                "投递次数": 1,
                "重复投递": False,
                "首次接收": received_at,
                "最近投递": received_at,
                "投递记录": [],
            }
            _append_delivery(receipt, receipt_id, source, received_at, signature, duplicate=False, note="首次接收")
            store.rows(RECEIPT_TABLE).append(receipt)

            credential = _rebuild_credential(batch, segment)
            receipt["凭据任务编号"] = credential.get("任务编号", "")
            _mark_legacy_superseded(batch)
            stats = _writeback_batch(batch)
            _append_event(
                batch=batch,
                segment=segment,
                event_type="登记回执",
                requests=[item["请求编号"] for item in receipt["失败明细"]],
                credential=credential,
                note=f"来源{source}，失败{len(canonical)}项，成功{len(successes)}项",
            )
            return {
                "ok": True,
                "deduplicated": False,
                "message": (
                    f"分段{segment}回执已登记，失败{len(canonical)}项进入重放凭据"
                    if canonical else f"分段{segment}回执已登记，无新增失败"
                ),
                "batch_created": batch_created,
                "batch": batch,
                "receipt": receipt,
                "credential": credential,
                "batch_stats": stats,
                "dispatch_count": _as_int(credential.get("派发次数"), 0),
            }

    def _merge_delivery(
        self,
        *,
        batch: dict[str, Any],
        segment: str,
        existing: dict[str, Any],
        canonical: list[dict[str, Any]],
        successes: list[str],
        scope_total: int,
        signature: str,
        source: str,
        received_at: str,
        receipt_id: str,
        batch_created: bool,
        dedupe_reason: str,
        digest: str,
    ) -> dict[str, Any]:
        existing["投递次数"] = _as_int(existing.get("投递次数"), 1) + 1
        existing["最近投递"] = received_at
        existing["重复投递"] = True

        stored = list(existing.get("失败明细", []) or [])
        stored_keys = {str(item.get("规范键", "")) for item in stored}
        additions = [item for item in canonical if item["规范键"] not in stored_keys]

        if additions:
            # 同一回执携带补充失败：只并集新增项，匿名补充项按投递批次隔离。
            stored.extend(_decorate(additions, segment, digest))
            existing["失败明细"] = stored
            existing["失败数量"] = len(stored)
            existing["abnormal"] = True
        existing["成功明细"] = _unique(list(existing.get("成功明细", []) or []) + successes)
        existing["成功数量"] = len(existing["成功明细"])
        existing["请求数量"] = max(_as_int(existing.get("请求数量"), 0), scope_total)
        existing["abnormal"] = bool(existing.get("失败明细"))
        existing["内容签名"] = _receipt_signature(
            str(existing.get("采集端点", "")),
            str(existing.get("采集来源", "")),
            _as_int(existing.get("请求数量"), scope_total),
            [
                {
                    "规范键": str(item.get("规范键", "")),
                    "失败原因": str(item.get("失败原因", "")),
                }
                for item in existing.get("失败明细", [])
            ],
            list(existing.get("成功明细", []) or []),
        )
        _append_delivery(
            existing, receipt_id, source, received_at, signature,
            duplicate=not additions,
            note=dedupe_reason + ("；仅补充新增失败" if additions else "；未新增待处理记录"),
        )

        credential = _rebuild_credential(batch, segment)
        _mark_legacy_superseded(batch)
        stats = _writeback_batch(batch)
        _append_event(
            batch=batch,
            segment=segment,
            event_type="重复回执" if not additions else "补充回执",
            requests=[item["请求编号"] for item in _decorate(additions, segment, digest)]
            if additions else [],
            credential=credential,
            note=f"回执{receipt_id}：{dedupe_reason}，新增失败{len(additions)}项",
        )
        return {
            "ok": True,
            "deduplicated": not additions,
            "supplemented": bool(additions),
            "message": (
                f"回执{receipt_id}重复投递，仅累加投递次数，未新增待处理记录"
                if not additions
                else f"回执{receipt_id}补充了{len(additions)}项新失败，已并入同一分段凭据"
            ),
            "batch_created": batch_created,
            "batch": batch,
            "receipt": existing,
            "credential": credential,
            "batch_stats": stats,
            "dispatch_count": _as_int(credential.get("派发次数"), 0),
        }

    def submit_receipts(self, payloads: list[dict[str, Any]]) -> dict[str, Any]:
        results = [self.submit_receipt(payload) for payload in payloads or []]
        return {
            "ok": all(item.get("ok") for item in results),
            "total": len(results),
            "deduplicated": sum(1 for item in results if item.get("deduplicated")),
            "results": results,
        }

    # -- 查询 -------------------------------------------------------------
    def list_receipts(self, batch_id: Any = None, segment_id: Any = None) -> dict[str, Any]:
        with _LOCK:
            rows = list(store.rows(RECEIPT_TABLE))
            if batch_id not in (None, ""):
                batch, _, error = _resolve_batch({"batch_id": batch_id})
                if batch is None:
                    return {"items": [], "total": 0, "message": error}
                rows = [row for row in rows if _as_int(row.get("批次记录ID"), -1) == int(batch["id"])]
            if segment_id not in (None, ""):
                rows = [row for row in rows if str(row.get("分段编号", "")) == str(segment_id)]
            rows.sort(key=lambda row: _as_int(row.get("id"), 0))
            return {"items": rows, "total": len(rows)}

    def list_credentials(self, batch_id: Any = None, include_legacy: bool = False) -> dict[str, Any]:
        with _LOCK:
            rows = list(store.rows("retry_queue"))
            rows = [
                row for row in rows
                if row.get("凭据类型") == SEGMENT_CREDENTIAL
                or (include_legacy and row.get("历史汇总"))
            ]
            if batch_id not in (None, ""):
                batch, _, error = _resolve_batch({"batch_id": batch_id})
                if batch is None:
                    return {"items": [], "total": 0, "message": error}
                rows = [row for row in rows if _as_int(row.get("批次记录ID"), -1) == int(batch["id"])]
            rows.sort(key=lambda row: (_as_int(row.get("批次记录ID"), 0), str(row.get("分段编号", ""))))
            return {"items": rows, "total": len(rows)}

    def list_events(self, batch_id: Any = None, event_type: str | None = None) -> dict[str, Any]:
        with _LOCK:
            rows = list(store.rows(EVENT_TABLE))
            if batch_id not in (None, ""):
                batch, _, error = _resolve_batch({"batch_id": batch_id})
                if batch is None:
                    return {"items": [], "total": 0, "message": error}
                rows = [row for row in rows if _as_int(row.get("批次记录ID"), -1) == int(batch["id"])]
            if event_type:
                rows = [row for row in rows if row.get("类型") == event_type]
            rows.sort(key=lambda row: _as_int(row.get("id"), 0))
            return {"items": rows, "total": len(rows)}

    # -- 重放 -------------------------------------------------------------
    def dispatch(
        self,
        batch_id: Any,
        limit: Any = None,
        segment_ids: Any = None,
    ) -> dict[str, Any]:
        with _LOCK:
            batch, _, error = _resolve_batch({"batch_id": batch_id})
            if batch is None:
                return {"ok": False, "message": error}
            if segment_ids in (None, "", []):
                wanted: list[str] | None = None
            elif isinstance(segment_ids, str):
                wanted = [item.strip() for item in segment_ids.split(",") if item.strip()]
            else:
                wanted = [str(item) for item in segment_ids]

            max_events = _as_int(limit, -1) if limit not in (None, "") else -1
            pending_credentials = [
                row for row in _credentials_for(int(batch["id"]))
                if row.get("待补范围")
                and (wanted is None or str(row.get("分段编号", "")) in wanted)
            ]
            if not pending_credentials:
                return {
                    "ok": True,
                    "message": "没有待补的失败分段；重复回执不会产生新的派发",
                    "dispatch_events": [],
                    "dispatch_count": 0,
                    "dispatched_requests": 0,
                    "batch": batch,
                    "batch_stats": _writeback_batch(batch),
                }

            events: list[dict[str, Any]] = []
            if wanted is not None:
                events = _dispatch_locked(batch, wanted)
            else:
                # 逐段派发，limit 用于模拟“派发到一半中断”。
                for credential in pending_credentials:
                    if max_events >= 0 and len(events) >= max_events:
                        break
                    events.extend(_dispatch_locked(batch, [str(credential.get("分段编号", ""))]))

            stats = _writeback_batch(batch)
            return {
                "ok": True,
                "message": f"已派发{len(events)}个失败分段缺口",
                "dispatch_events": events,
                "dispatch_count": len(events),
                "dispatched_requests": sum(_as_int(event.get("数量"), 0) for event in events),
                "batch": batch,
                "batch_stats": stats,
                "credentials": self.list_credentials(batch.get("批次编号"), include_legacy=True)["items"],
            }

    def interrupt(self, batch_id: Any, reason: str = "") -> dict[str, Any]:
        with _LOCK:
            batch, _, error = _resolve_batch({"batch_id": batch_id})
            if batch is None:
                return {"ok": False, "message": error}
            affected: list[dict[str, Any]] = []
            for credential in _credentials_for(int(batch["id"])):
                if not credential.get("待补范围"):
                    continue
                credential["队列状态"] = QUEUE_INTERRUPTED
                credential["status"] = STATUS_PENDING
                credential["pending"] = True
                credential["abnormal"] = True
                credential["中断次数"] = _as_int(credential.get("中断次数"), 0) + 1
                credential["最近中断"] = _now()
                affected.append(credential)
                _append_event(
                    batch=batch,
                    segment=str(credential.get("分段编号", "")),
                    event_type="中断",
                    requests=list(credential.get("待补范围", []) or []),
                    credential=credential,
                    note=reason or "重放中断，保留历史回执与已恢复范围",
                )
            stats = _writeback_batch(batch)
            if not affected:
                return {
                    "ok": True,
                    "message": "没有可中断的待补分段",
                    "interrupted_segments": [],
                    "pending_scope": {},
                    "batch_stats": stats,
                }
            return {
                "ok": True,
                "message": f"已中断{len(affected)}个分段的派发，待恢复",
                "interrupted_segments": [str(item.get("分段编号", "")) for item in affected],
                "pending_scope": {
                    str(item.get("分段编号", "")): list(item.get("待补范围", []) or [])
                    for item in affected
                },
                "batch_stats": stats,
            }

    def resume(self, batch_id: Any) -> dict[str, Any]:
        """只依据历史采集回执重建缺口，再派发剩余部分，已恢复部分不会重复派发。"""
        with _LOCK:
            batch, _, error = _resolve_batch({"batch_id": batch_id})
            if batch is None:
                return {"ok": False, "message": error}
            receipts = _receipts_for(int(batch["id"]))
            if not receipts:
                return {"ok": False, "message": "没有历史采集回执，无法恢复重放"}

            segments = _unique([str(row.get("分段编号", "")) for row in receipts])
            restored: dict[str, dict[str, list[str]]] = {}
            already_recovered: dict[str, list[str]] = {}
            for segment in segments:
                credential = _rebuild_credential(batch, segment)
                restored[segment] = {
                    "待补范围": list(credential.get("待补范围", []) or []),
                    "已恢复范围": list(credential.get("已恢复范围", []) or []),
                }
                already_recovered[segment] = list(credential.get("已恢复范围", []) or [])
                credential["队列状态"] = QUEUE_RECOVERED

            restored_count = sum(len(scope["待补范围"]) for scope in restored.values())
            recovered_count = sum(len(scope["已恢复范围"]) for scope in restored.values())
            _append_event(
                batch=batch,
                segment="*",
                event_type="恢复",
                requests=[
                    request_id
                    for scope in restored.values()
                    for request_id in scope["待补范围"]
                ],
                note=f"沿用{len(receipts)}条历史回执重建待补范围，已恢复{recovered_count}项不再派发",
            )

            events = _dispatch_locked(batch, segments)
            stats = _writeback_batch(batch)
            return {
                "ok": True,
                "message": (
                    f"已按历史回执恢复：待补{restored_count}项，"
                    f"跳过已恢复{recovered_count}项，派发{len(events)}个分段"
                ),
                "restored_from_receipts": len(receipts),
                "restored_scope": restored,
                "pending_before_resume": restored_count,
                "already_recovered": already_recovered,
                "recovered_total": recovered_count,
                "scope_total": restored_count + recovered_count,
                "dispatch_events": events,
                "dispatch_count": len(events),
                "dispatched_requests": sum(_as_int(event.get("数量"), 0) for event in events),
                "batch": batch,
                "batch_stats": stats,
                "credentials": self.list_credentials(batch.get("批次编号"), include_legacy=True)["items"],
            }

    # -- 核对 -------------------------------------------------------------
    def reconciliation(self, batch_id: Any) -> dict[str, Any]:
        with _LOCK:
            batch, batch_created, error = _resolve_batch({"batch_id": batch_id})
            if batch is None:
                return {"ok": False, "message": error}
            _writeback_batch(batch)
            return _reconciliation_locked(batch, batch_created=batch_created)

    def domain_snapshot(self) -> dict[str, Any]:
        with _LOCK:
            credentials = _credentials_for_all()
            events = store.rows(EVENT_TABLE)
            return {
                "module": self.MODULE,
                "entity": self.ENTITY,
                "artifact": "批次回执台账与失败分段重放凭据",
                "receipts": len(store.rows(RECEIPT_TABLE)),
                "credentials": len(credentials),
                "pending_segments": sum(1 for row in credentials if row.get("待补范围")),
                "covered_segments": sum(1 for row in credentials if not row.get("待补范围")),
                "dispatch_events": sum(1 for row in events if row.get("类型") == "派发"),
                "interrupt_events": sum(1 for row in events if row.get("类型") == "中断"),
                "abnormal": sum(1 for row in credentials if row.get("abnormal")),
            }


def _credentials_for_all() -> list[dict[str, Any]]:
    return [
        row for row in store.rows("retry_queue")
        if row.get("凭据类型") == SEGMENT_CREDENTIAL
    ]


def _reconciliation_locked(batch: dict[str, Any], batch_created: bool = False) -> dict[str, Any]:
    batch_id = int(batch["id"])
    receipts = _receipts_for(batch_id)
    credentials = _credentials_for(batch_id)
    legacy = [
        row for row in store.rows("retry_queue")
        if row.get("历史汇总") and _as_int(row.get("批次记录ID"), -1) == batch_id
    ]
    events = [
        row for row in store.rows(EVENT_TABLE)
        if _as_int(row.get("批次记录ID"), -1) == batch_id
    ]

    failed_union: list[str] = []
    success_union: list[str] = []
    for receipt in receipts:
        failed_union.extend(str(item.get("请求编号", "")) for item in receipt.get("失败明细", []) or [])
        success_union.extend(str(item) for item in receipt.get("成功明细", []) or [])
    failed_union = [item for item in _unique(failed_union) if item]
    success_union = _unique(success_union)
    segment_scopes = _segment_scopes(batch_id)
    scope_total = sum(segment_scopes.values()) if receipts else _as_int(batch.get("请求数量"), 0)

    segments: list[dict[str, Any]] = []
    pending_union: list[str] = []
    recovered_union: list[str] = []
    eliminated_union: list[str] = []
    credential_failed_union: list[str] = []
    dispatch_by_segment: dict[str, int] = {}
    segment_checks: list[bool] = []
    for credential in credentials:
        segment = str(credential.get("分段编号", ""))
        pending = _unique(list(credential.get("待补范围", []) or []))
        recovered = _unique(list(credential.get("已恢复范围", []) or []))
        eliminated = _unique(list(credential.get("回执消除范围", []) or []))
        evidence = _segment_evidence(batch_id, segment)[0]
        evidence_ids = _unique(list(evidence.keys()))
        pending_union.extend(pending)
        recovered_union.extend(recovered)
        eliminated_union.extend(eliminated)
        credential_failed_union.extend(evidence_ids)
        dispatch_count = sum(
            1 for event in events
            if event.get("类型") == "派发" and str(event.get("分段编号", "")) == segment
        )
        dispatch_by_segment[segment] = dispatch_count
        partition = _unique(pending + recovered + eliminated)
        partition_ok = (
            set(partition) == set(evidence_ids)
            and len(partition) == len(evidence_ids)
        )
        segment_checks.append(partition_ok)
        segments.append({
            "分段编号": segment,
            "任务编号": credential.get("任务编号", ""),
            "状态": credential.get("status", ""),
            "队列状态": credential.get("队列状态", ""),
            "失败端点": credential.get("失败端点", ""),
            "批次来源": credential.get("批次来源", ""),
            "采集来源集合": list(credential.get("采集来源集合", []) or []),
            "回执编号集合": list(credential.get("回执编号集合", []) or []),
            "回执数量": _as_int(credential.get("回执数量"), 0),
            "历史来源": list(credential.get("历史来源", []) or []),
            "失败请求总数": _as_int(credential.get("失败请求总数"), len(evidence_ids)),
            "待补范围": pending,
            "已恢复范围": recovered,
            "回执消除范围": eliminated,
            "范围划分一致": partition_ok,
            "派发次数": _as_int(credential.get("派发次数"), 0),
            "审计派发次数": dispatch_count,
            "派发次数一致": _as_int(credential.get("派发次数"), 0) == dispatch_count,
            "中断次数": _as_int(credential.get("中断次数"), 0),
        })

    pending_union = _unique(pending_union)
    recovered_union = _unique(recovered_union)
    eliminated_union = _unique(eliminated_union)
    credential_failed_union = _unique(credential_failed_union)

    missing_in_credential = [item for item in failed_union if item not in credential_failed_union]
    extra_in_credential = [item for item in credential_failed_union if item not in failed_union]
    overlap = [
        item for item in pending_union
        if item in recovered_union or item in eliminated_union
    ]
    two_records_consistent = (
        not missing_in_credential and not extra_in_credential and not overlap
        and all(segment_checks)
    )

    completed = max(0, scope_total - len(pending_union)) if scope_total else len(recovered_union)
    scope_consistent = (
        scope_total >= len(failed_union)
        and len(pending_union) <= scope_total
        and completed + len(pending_union) <= scope_total
    )
    stats_consistent = (
        _as_int(batch.get("请求数量"), -1) == scope_total
        and _as_int(batch.get("失败数量"), -1) == len(pending_union)
        and _as_int(batch.get("完成数量"), -1) == completed
    )

    stats = {
        "请求数量": scope_total,
        "完成数量": completed,
        "失败数量": len(pending_union),
    }
    batch_view = {
        "记录ID": batch.get("id"),
        "批次编号": batch.get("批次编号", ""),
        "关联端点": batch.get("关联端点", ""),
        "参数模板": batch.get("参数模板", ""),
        "当前状态": batch.get("status", ""),
        "统计": batch.get("stats", stats),
        "请求数量": batch.get("请求数量", scope_total),
        "完成数量": batch.get("完成数量", completed),
        "失败数量": batch.get("失败数量", len(pending_union)),
        "统计来源": batch.get("统计来源", ""),
        "统计回写时间": batch.get("统计回写时间", ""),
    }
    batch_source = {
        "来源模块": "request_batch",
        "记录ID": batch.get("id"),
        "批次编号": batch.get("批次编号", ""),
        "关联端点": batch.get("关联端点", ""),
        "参数模板": batch.get("参数模板", ""),
        "来源标记": batch.get("批次来源", "请求批次"),
        "自动补建": batch_created,
        "历史分段来源": {
            str(credential.get("分段编号", "")): list(credential.get("采集来源集合", []) or [])
            for credential in credentials
        },
    }

    return {
        "ok": True,
        "batch": batch_view,
        "batch_source": batch_source,
        "scope": {
            "请求数量": scope_total,
            "分段范围": segment_scopes,
            "成功回执数": len(success_union),
            "历史失败请求数": len(failed_union),
            "待补数量": len(pending_union),
            "已恢复数量": len(recovered_union),
            "回执消除数量": len(eliminated_union),
            "隐式完成数量": max(0, scope_total - len(failed_union)),
            "完成数量": completed,
            "失败数量": len(pending_union),
            "两份记录一致": two_records_consistent,
            "范围一致": scope_consistent,
            "批次统计一致": stats_consistent,
            "缺失于凭据": missing_in_credential,
            "凭据多出": extra_in_credential,
            "范围重叠": overlap,
        },
        "segments": segments,
        "dispatch": {
            "总派发次数": sum(dispatch_by_segment.values()),
            "派发请求总数": sum(
                _as_int(event.get("数量"), 0) for event in events if event.get("类型") == "派发"
            ),
            "按分段": dispatch_by_segment,
            "事件": [event for event in events if event.get("类型") == "派发"],
        },
        "events": events,
        "receipts": receipts,
        "credentials": credentials,
        "legacy_credentials": legacy,
        "consistent": two_records_consistent and scope_consistent and stats_consistent,
    }


batch_replay = BatchReplayService()
