<template>
  <div class="app-shell">
    <aside class="app-side">
      <h1 class="app-title">网关工坊 · API接口采集与数据网关平台</h1>
      <nav class="nav-list">
        <RouterLink v-for="item in navItems" :key="item.path" :to="item.path" class="nav-item">
          {{ item.label }}
        </RouterLink>
      </nav>
    </aside>
    <main class="app-main">
      <header class="app-head">
        <span class="head-desc">面向第三方API接入注册、接口鉴权编排、限流熔断、响应解析、schema映射、结果缓存、重放恢复与交付订阅的API接口采集与数据网关平台。</span>
        <span class="head-user">当前值班：数据工程组</span>
      </header>
      <RouterView />
    </main>
  </div>
</template>

<script setup lang="ts">
const navItems = [{ label: "运营概览", path: "/" }, { label: "接口提供方", path: "/api_provider" }, { label: "鉴权配置", path: "/auth_profile" }, { label: "接口定义", path: "/endpoint_def" }, { label: "限流熔断", path: "/rate_limit" }, { label: "请求批次", path: "/request_batch" }, { label: "批次重放", path: "/batch_replay" }, { label: "响应解析", path: "/response_parse" }, { label: "Schema差异", path: "/schema_diff" }, { label: "结果缓存", path: "/data_cache" }, { label: "重放队列", path: "/retry_queue" }, { label: "数据投递", path: "/data_delivery" }, { label: "订阅管理", path: "/subscriber" }, { label: "接口监控", path: "/api_monitor" }, { label: "Webhook路由", path: "/webhook_router" }, { label: "传输加密", path: "/data_encrypt" }, { label: "调用日志", path: "/call_log" }, { label: "数据版本", path: "/data_version" }, { label: "管道审计", path: "/pipeline_audit" }, { label: "治理报告", path: "/govern_report" }, { label: "增量追踪", path: "/incr_tracker" }, { label: "抽样校验", path: "/sample_check" }, { label: "用量计费", path: "/cost_account" }, { label: "任务调度", path: "/task_schedule" }]
</script>
