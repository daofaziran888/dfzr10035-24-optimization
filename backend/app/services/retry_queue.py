"""重放队列领域服务：管理失败请求重试，与 request_batch、call_log、api_monitor 联动。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.store import store


class RetryQueueService(BaseModuleService):
    MODULE = "retry_queue"
    ENTITY = "重放任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["任务编号", "失败端点", "失败原因", "重试次数", "最大重试", "退避策略", "最后重试", "队列状态"]
    REQUIRED_FIELDS = ["任务编号", "失败端点", "失败原因"]
    STATUS_ORDER = ["待重试", "重试中", "已成功", "永久失败"]
    ACTION_RULES = {"加入队列": "重试中", "手动重试": "已成功", "标记失败": "永久失败"}
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str):
        """重试中/已成功时联动写调用日志和更新监控；永久失败时触发告警。

        失败分段凭据的“手动重试”按凭据的实际待补范围派发，不按端点合并；
        批次汇总凭据只保留历史来源，转入重试时提示以分段凭据为准。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is not None and entry.get("凭据类型") == "失败分段":
            return self._run_segment_action(entry, action)

        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        if action == "加入队列":
            self._log_retry_start(entry)
        elif action == "手动重试":
            self._log_retry_success(entry)
            self._update_monitor_health(entry)
            message += "，已更新调用日志和监控健康上报"
        elif action == "标记失败":
            self._trigger_alert(entry)
            message += "，已触发监控告警"
        return entry, message

    def _run_segment_action(self, entry: dict, action: str):
        """失败分段凭据的派发/确认走 batch_replay，保证按分段而不是按端点处理。"""
        from app.services.batch_replay import batch_replay

        batch = store.find("request_batch", int(entry.get("批次记录ID", 0)))
        if batch is None:
            return None, f"重放任务 {entry.get('任务编号', '')} 关联的批次已不存在"
        segment = str(entry.get("分段编号", ""))

        if action in ("加入队列", "手动重试"):
            if not entry.get("待补范围"):
                return entry, f"失败分段{segment}没有待补范围，重复回执不会重复派发"
            result = batch_replay.dispatch(batch.get("批次编号", batch["id"]), segment_ids=[segment])
            if not result.get("ok"):
                return None, result.get("message", "派发失败")
            credential = next(
                (row for row in store.rows(self.MODULE)
                 if row.get("任务编号") == entry.get("任务编号")),
                entry,
            )
            self._update_monitor_health(credential)
            return credential, (
                f"失败分段{segment}已按实际缺口派发"
                f"{result.get('dispatched_requests', 0)}项，批次统计已回写"
            )
        if action == "标记失败":
            entry["status"] = "永久失败"
            entry["pending"] = False
            entry["abnormal"] = True
            entry["队列状态"] = "永久失败"
            self._trigger_alert(entry)
            return entry, f"失败分段{segment}已标记永久失败，已触发监控告警"
        return None, f"动作「{action}」不属于失败分段凭据可执行范围"

    def _log_retry_start(self, task: dict):
        log_rows = store.rows("call_log")
        tid = str(task["id"]).zfill(4)
        log_rows.append({
            "id": max((int(r.get("id", 0)) for r in log_rows), default=0) + 1,
            "记录编号": f"CALL-RETRY-{tid}",
            "接口端点": task.get("失败端点", ""),
            "请求参数": f"retry:task={tid}",
            "响应状态": "retrying",
            "响应耗时(ms)": 0,
            "返回行数": 0,
            "错误信息": task.get("失败原因", ""),
            "调用时间": "2026-11-17",
            "status": "超时", "pending": True, "abnormal": True,
        })

    def _log_retry_success(self, task: dict):
        tid = str(task["id"]).zfill(4)
        for log in store.rows("call_log"):
            if log.get("记录编号") == f"CALL-RETRY-{tid}":
                log["status"] = "成功"
                log["响应状态"] = "200"
                log["响应耗时(ms)"] = 150
                log["返回行数"] = 1
                break

    def _update_monitor_health(self, task: dict):
        """重试成功后联动刷新监控健康度。"""
        endpoint = task.get("失败端点", "")
        for mon in store.rows("api_monitor"):
            if mon.get("目标端点") == endpoint:
                mon["最近采样"] = "2026-11-17"
                if mon.get("status") == "已告警":
                    mon["status"] = "监控中"
                    mon["pending"] = True

    def _trigger_alert(self, task: dict):
        """永久失败触发监控告警和治理报告。"""
        endpoint = task.get("失败端点", "")
        tid = str(task["id"]).zfill(4)
        # 更新监控状态
        for mon in store.rows("api_monitor"):
            if mon.get("目标端点") == endpoint:
                mon["status"] = "已告警"
                mon["pending"] = False
                mon["最近采样"] = "2026-11-17"
        # 创建治理报告条目
        gov_rows = store.rows("govern_report")
        gov_rows.append({
            "id": max((int(r.get("id", 0)) for r in gov_rows), default=0) + 1,
            "报告编号": f"GOVE-RETRY-{tid}",
            "评估范围": f"重放队列永久失败: {endpoint}",
            "合规指标": "数据采集完整性",
            "当前得分": 60.0,
            "改进事项": f"任务{tid}重试耗尽，需人工介入",
            "评估周期": "2026-Q4",
            "负责人": "运维值班",
            "生成时间": "2026-11-17",
            "status": "待生成", "pending": True, "abnormal": True,
        })

    def calc_recovery_rate(self) -> dict:
        """跨模块恢复率统计：分析重放队列成功/失败分布及关联调用日志。"""
        retries = store.rows("retry_queue")
        total = len(retries)
        success = sum(1 for r in retries if r.get("status") == "已成功")
        perm_fail = sum(1 for r in retries if r.get("status") == "永久失败")
        # 关联调用日志
        logs_related = [
            l for l in store.rows("call_log")
            if "retry" in str(l.get("请求参数", ""))
        ]
        return {
            "total_retries": total,
            "success": success,
            "permanent_fail": perm_fail,
            "recovery_rate": success / total if total else 0.0,
            "logged_events": len(logs_related),
        }