/**
 * 批次重放前端数据层：回执登记、失败分段凭据、派发/中断/恢复与核对。
 *
 * 后端对应 /api/batch-replay/*；这里保留纯前端的类型定义和请求函数，
 * 页面只负责展示与触发，所有去重与派发判断都在后端领域服务内完成。
 */

export type ReceiptFailure = {
  请求编号: string
  失败原因: string
  规范键?: string
}

export type ReceiptDelivery = {
  回执编号: string
  采集来源: string
  时间: string
  内容签名: string
  重复: boolean
  说明: string
}

export type BatchReceipt = {
  id: number
  回执编号: string
  显式回执编号: boolean
  内容签名: string
  批次编号: string
  批次记录ID: number
  分段编号: string
  采集端点: string
  采集来源: string
  来源集合: string[]
  采集结果: string
  请求数量: number
  成功数量: number
  失败数量: number
  失败明细: ReceiptFailure[]
  成功明细: string[]
  投递次数: number
  重复投递: boolean
  首次接收: string
  最近投递: string
  投递记录: ReceiptDelivery[]
}

export type SegmentCredential = {
  id: number
  任务编号: string
  凭据类型: string
  批次编号: string
  批次记录ID: number
  分段编号: string
  批次来源?: string
  失败端点: string
  失败原因: string
  采集来源集合: string[]
  回执编号集合: string[]
  回执数量: number
  失败请求总数: number
  待补范围: string[]
  已恢复范围: string[]
  回执消除范围: string[]
  失败原因集合: string[]
  派发次数: number
  中断次数: number
  队列状态: string
  status: string
}

export type ReplayEvent = {
  id: number
  批次记录ID: number
  批次编号: string
  分段编号: string
  任务编号: string
  类型: string
  请求范围: string[]
  数量: number
  说明: string
  时间: string
}

export type SegmentCheck = SegmentCredential & {
  范围划分一致: boolean
  审计派发次数: number
  派发次数一致: boolean
  历史来源: Array<Record<string, unknown>>
}

export type Reconciliation = {
  ok: boolean
  batch: Record<string, unknown>
  batch_source: Record<string, unknown>
  scope: Record<string, number | boolean | string[] | Record<string, number>>
  segments: SegmentCheck[]
  dispatch: {
    总派发次数: number
    派发请求总数: number
    按分段: Record<string, number>
    事件: ReplayEvent[]
  }
  events: ReplayEvent[]
  receipts: BatchReceipt[]
  credentials: SegmentCredential[]
  consistent: boolean
}

export type ReceiptSubmitResult = {
  ok: boolean
  deduplicated?: boolean
  supplemented?: boolean
  message: string
  batch_created?: boolean
  batch?: Record<string, unknown>
  receipt?: BatchReceipt
  credential?: SegmentCredential
  batch_stats?: Record<string, unknown>
  dispatch_count?: number
}

export type DispatchResult = {
  ok: boolean
  message: string
  dispatch_count: number
  dispatched_requests: number
  dispatch_events: ReplayEvent[]
  batch_stats: Record<string, unknown>
}

export type InterruptResult = {
  ok: boolean
  message: string
  interrupted_segments: string[]
  pending_scope: Record<string, string[]>
  batch_stats: Record<string, unknown>
}

export type ResumeResult = DispatchResult & {
  restored_from_receipts: number
  pending_before_resume: number
  recovered_total: number
  already_recovered: Record<string, string[]>
  restored_scope: Record<string, { 待补范围: string[]; 已恢复范围: string[] }>
}

const BASE = '/api/batch-replay'

async function call<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${BASE}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  })
  if (!response.ok) {
    throw new Error(`批次重放接口返回 ${response.status}`)
  }
  return (await response.json()) as T
}

export function submitReceipt(payload: Record<string, unknown>): Promise<ReceiptSubmitResult> {
  return call<ReceiptSubmitResult>('/receipts', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

export function submitReceipts(receipts: Record<string, unknown>[]): Promise<{
  ok: boolean
  total: number
  deduplicated: number
  results: ReceiptSubmitResult[]
}> {
  return call('/receipts/batch', {
    method: 'POST',
    body: JSON.stringify({ receipts }),
  })
}

export function listReceipts(batchId: string, segmentId?: string): Promise<{
  items: BatchReceipt[]
  total: number
}> {
  const params = new URLSearchParams({ batch_id: batchId })
  if (segmentId) params.set('segment_id', segmentId)
  return call(`/receipts?${params.toString()}`)
}

export function listCredentials(batchId: string): Promise<{
  items: SegmentCredential[]
  total: number
}> {
  return call(`/credentials?batch_id=${encodeURIComponent(batchId)}&include_legacy=true`)
}

export function dispatch(batchId: string, segmentIds?: string, limit?: number): Promise<DispatchResult> {
  const params = new URLSearchParams({ batch_id: batchId })
  if (segmentIds) params.set('segment_ids', segmentIds)
  if (limit !== undefined) params.set('limit', String(limit))
  return call(`/dispatch?${params.toString()}`, { method: 'POST' })
}

export function interrupt(batchId: string, reason?: string): Promise<InterruptResult> {
  const params = new URLSearchParams({ batch_id: batchId })
  if (reason) params.set('reason', reason)
  return call(`/interrupt?${params.toString()}`, { method: 'POST' })
}

export function resume(batchId: string): Promise<ResumeResult> {
  return call(`/resume?batch_id=${encodeURIComponent(batchId)}`, { method: 'POST' })
}

export function reconcile(batchId: string): Promise<Reconciliation> {
  return call(`/reconciliation?batch_id=${encodeURIComponent(batchId)}`)
}
