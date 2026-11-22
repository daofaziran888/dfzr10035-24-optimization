<template>
  <section class="page" data-module="batch_replay">
    <header class="page-head">
      <div>
        <h2>批次重放</h2>
        <p class="page-desc">
          按采集分段去重登记回执，用实际失败分段形成重放凭据；重复回执不会重复派发，
          中断后只依据历史回执恢复。
        </p>
      </div>
      <form class="page-actions" @submit.prevent="loadAll">
        <input
          v-model="batchId"
          data-testid="batch-id"
          placeholder="批次编号，如 REQU-0002"
        />
        <button class="btn" data-testid="reconcile" type="submit">核对批次</button>
      </form>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="error">{{ errorMessage }}</p>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">实际采集范围</span>
        <strong class="stat-value" data-testid="stat-scope">{{ scope.请求数量 ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">待补缺口</span>
        <strong class="stat-value" data-testid="stat-pending">{{ scope.待补数量 ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">已恢复</span>
        <strong class="stat-value" data-testid="stat-recovered">{{ scope.已恢复数量 ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">派发次数</span>
        <strong class="stat-value" data-testid="stat-dispatch">{{ dispatch.总派发次数 ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">两份记录一致</span>
        <strong class="stat-value" data-testid="stat-consistent">{{ consistent ? '是' : '否' }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="submitReceipt">
      <label class="filter-item">
        <span>批次编号</span>
        <input v-model="form.batchId" data-testid="receipt-batch" :placeholder="batchId" />
      </label>
      <label class="filter-item">
        <span>采集分段</span>
        <input v-model="form.segmentId" data-testid="receipt-segment" placeholder="如 SEG-A" />
      </label>
      <label class="filter-item">
        <span>回执编号</span>
        <input v-model="form.receiptId" data-testid="receipt-id" placeholder="可留空按内容去重" />
      </label>
      <label class="filter-item">
        <span>采集来源</span>
        <input v-model="form.source" data-testid="receipt-source" placeholder="如 crawler-1" />
      </label>
      <label class="filter-item">
        <span>采集范围</span>
        <input v-model="form.scopeTotal" data-testid="receipt-scope" placeholder="请求数量" />
      </label>
      <label class="filter-item">
        <span>失败请求（请求编号:原因，逗号分隔）</span>
        <input v-model="form.failures" data-testid="receipt-failures" placeholder="r1:timeout,r2:502" />
      </label>
      <label class="filter-item">
        <span>成功请求（逗号分隔）</span>
        <input v-model="form.successes" data-testid="receipt-successes" placeholder="r3,r4" />
      </label>
      <button class="btn primary" data-testid="submit-receipt" type="submit">登记回执</button>
    </form>

    <p v-if="lastMessage" class="status-legend" data-testid="last-message">{{ lastMessage }}</p>

    <div class="page-actions">
      <button class="btn" data-testid="dispatch" type="button" @click="runDispatch">派发待补</button>
      <button class="btn" data-testid="interrupt" type="button" @click="runInterrupt">中断重放</button>
      <button class="btn" data-testid="resume" type="button" @click="runResume">按历史回执恢复</button>
    </div>

    <h3>失败分段凭据</h3>
    <table class="data-table" data-testid="credential-table">
      <thead>
        <tr>
          <th>分段编号</th>
          <th>任务编号</th>
          <th>批次来源</th>
          <th>采集来源</th>
          <th>回执编号</th>
          <th>失败总数</th>
          <th>待补范围</th>
          <th>已恢复范围</th>
          <th>派发次数</th>
          <th>队列状态</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="row in credentials"
          :key="String(row.id)"
          :data-testid="`credential-${row.分段编号}`"
        >
          <td>{{ row.分段编号 }}</td>
          <td>{{ row.任务编号 }}</td>
          <td>{{ row.批次来源 ?? '请求批次' }}</td>
          <td>{{ (row.采集来源集合 || []).join(' / ') }}</td>
          <td>{{ (row.回执编号集合 || []).join(' / ') }}</td>
          <td>{{ row.失败请求总数 }}</td>
          <td>{{ (row.待补范围 || []).join(' / ') || '—' }}</td>
          <td>{{ (row.已恢复范围 || []).join(' / ') || '—' }}</td>
          <td>{{ row.派发次数 }}</td>
          <td>{{ row.队列状态 }}</td>
        </tr>
        <tr v-if="!credentials.length">
          <td colspan="10" class="empty-state">尚无失败分段凭据，先登记采集回执</td>
        </tr>
      </tbody>
    </table>

    <h3>回执台账</h3>
    <table class="data-table" data-testid="receipt-table">
      <thead>
        <tr>
          <th>回执编号</th>
          <th>分段</th>
          <th>来源</th>
          <th>结果</th>
          <th>请求数量</th>
          <th>失败数量</th>
          <th>投递次数</th>
          <th>最近投递</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in receipts" :key="String(row.id)" :data-testid="`receipt-${row.回执编号}`">
          <td>{{ row.回执编号 }}</td>
          <td>{{ row.分段编号 }}</td>
          <td>{{ (row.来源集合 || [row.采集来源]).join(' / ') }}</td>
          <td>{{ row.采集结果 }}</td>
          <td>{{ row.请求数量 }}</td>
          <td>{{ row.失败数量 }}</td>
          <td>{{ row.投递次数 }}</td>
          <td>{{ row.最近投递 }}</td>
        </tr>
        <tr v-if="!receipts.length">
          <td colspan="8" class="empty-state">该批次尚无采集回执</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  dispatch as dispatchApi,
  interrupt as interruptApi,
  listCredentials,
  listReceipts,
  reconcile,
  resume as resumeApi,
  submitReceipt as submitReceiptApi,
} from '@/domain/batch_replay'
import type {
  BatchReceipt,
  Reconciliation,
  SegmentCheck,
} from '@/domain/batch_replay'

const batchId = ref('REQU-0002')
const errorMessage = ref('')
const lastMessage = ref('')
const scope = ref<Reconciliation['scope']>({})
const dispatch = ref<Reconciliation['dispatch']>({ 总派发次数: 0, 派发请求总数: 0, 按分段: {}, 事件: [] })
const consistent = ref(false)
const credentials = ref<SegmentCheck[]>([])
const receipts = ref<BatchReceipt[]>([])

const form = ref({
  batchId: '',
  segmentId: '',
  receiptId: '',
  source: '',
  scopeTotal: '',
  failures: '',
  successes: '',
})

const effectiveBatch = computed(() => form.value.batchId.trim() || batchId.value.trim())

function parseFailures(raw: string) {
  return raw
    .split(/[,，;；\n]/)
    .map((item) => item.trim())
    .filter(Boolean)
    .map((item) => {
      const [requestId, ...rest] = item.split(/[:：]/)
      const reason = rest.join(':').trim() || '未注明失败原因'
      return requestId.trim() ? { request_id: requestId.trim(), reason } : { reason }
    })
}

function parseSuccesses(raw: string) {
  return raw
    .split(/[,，;；\n]/)
    .map((item) => item.trim())
    .filter(Boolean)
}

async function loadAll() {
  errorMessage.value = ''
  const id = batchId.value.trim()
  if (!id) {
    errorMessage.value = '请先填写批次编号'
    return
  }
  try {
    const payload = await reconcile(id)
    scope.value = payload.scope
    dispatch.value = payload.dispatch
    consistent.value = payload.consistent
    credentials.value = payload.segments
    receipts.value = payload.receipts
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批次核对失败'
  }
}

async function submitReceipt() {
  errorMessage.value = ''
  if (!effectiveBatch.value) {
    errorMessage.value = '请先填写批次编号'
    return
  }
  try {
    const result = await submitReceiptApi({
      batch_id: effectiveBatch.value,
      segment_id: form.value.segmentId,
      receipt_id: form.value.receiptId,
      source: form.value.source,
      scope_total: form.value.scopeTotal,
      failures: parseFailures(form.value.failures),
      successes: parseSuccesses(form.value.successes),
    })
    lastMessage.value = result.message
    batchId.value = effectiveBatch.value
    form.value.batchId = ''
    await loadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回执登记失败'
  }
}

async function runDispatch() {
  errorMessage.value = ''
  try {
    const result = await dispatchApi(batchId.value.trim())
    lastMessage.value = result.message
    await loadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '派发失败'
  }
}

async function runInterrupt() {
  errorMessage.value = ''
  try {
    const result = await interruptApi(batchId.value.trim(), '页面手动中断')
    lastMessage.value = result.message
    await loadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '中断失败'
  }
}

async function runResume() {
  errorMessage.value = ''
  try {
    const result = await resumeApi(batchId.value.trim())
    lastMessage.value = result.message
    await loadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '恢复失败'
  }
}

onMounted(loadAll)
</script>
