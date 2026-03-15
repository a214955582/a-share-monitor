from __future__ import annotations

import asyncio
import logging
from datetime import time, timedelta, date
import akshare as ak

from .quote_provider import QuoteProvider
from .repository import MonitorRepository
from .schemas import MonitorOut, QuoteSnapshot, QuoteTarget, RuleOperator, RuleOut
from .utils import display_stock_name, now_local, parse_iso, quote_cache_key


logger = logging.getLogger(__name__)

AM_SESSION_START = time(hour=9, minute=20)
AM_SESSION_END = time(hour=11, minute=30)
PM_SESSION_START = time(hour=13, minute=0)
PM_SESSION_END = time(hour=15, minute=0)


FIELD_LABELS = {
    "last_price": "最新价",
    "change_pct": "涨跌幅",
    "open_price": "开盘价",
    "high_price": "最高价",
    "low_price": "最低价",
    "volume": "成交量",
    "turnover": "成交额",
}

OPERATOR_LABELS = {
    "gte": ">=",
    "lte": "<=",
    "eq": "=",
    "neq": "!=",
}


def get_rule_value(rule: RuleOut, quote: QuoteSnapshot) -> float:
    return float(getattr(quote, rule.field.value))


def compare_value(operator: RuleOperator, actual: float, threshold: float) -> bool:
    if operator == RuleOperator.GTE:
        return actual >= threshold
    if operator == RuleOperator.LTE:
        return actual <= threshold
    if operator == RuleOperator.EQ:
        return abs(actual - threshold) < 1e-9
    if operator == RuleOperator.NEQ:
        return abs(actual - threshold) >= 1e-9
    return False


def cooldown_ready(rule: RuleOut) -> bool:
    if rule.cooldown_minutes == 0 or not rule.last_triggered_at:
        return True

    last_triggered = parse_iso(rule.last_triggered_at)
    if last_triggered is None:
        return True

    return now_local() - last_triggered >= timedelta(minutes=rule.cooldown_minutes)


def next_consecutive_hits(rule: RuleOut, condition_matched: bool) -> int:
    if not condition_matched:
        return 0
    return min(rule.current_consecutive_hits + 1, rule.consecutive_hits_required)


def is_monitor_active_time() -> bool:
    # 获取工具接口
    tool_trade_date_hist_df = ak.tool_trade_date_hist_sina()
    today = date.today().strftime("%Y-%m-%d")
    is_trading_day = today in tool_trade_date_hist_df['trade_date'].astype(str).values
    current = now_local().time()
    in_am_session = AM_SESSION_START <= current <= AM_SESSION_END
    in_pm_session = PM_SESSION_START <= current <= PM_SESSION_END
    return is_trading_day and (in_am_session or in_pm_session)


def build_text_alert(
    monitor: MonitorOut,
    rule: RuleOut,
    quote: QuoteSnapshot,
    actual: float,
    consecutive_hits: int,
) -> str:
    asset_label = "指数" if monitor.instrument_type == "index" else "股票"
    stock_name = display_stock_name(monitor.code, monitor.name or quote.name)
    field_label = FIELD_LABELS[rule.field.value]
    operator_label = OPERATOR_LABELS[rule.operator.value]
    consecutive_description = (
        f"\n连续命中：{consecutive_hits}/{rule.consecutive_hits_required}"
        if rule.consecutive_hits_required > 1
        else ""
    )
    note_line = f"\n备注：{rule.description}" if rule.description else ""

    return (
        "【A股监控提醒】\n"
        f"{asset_label}：{stock_name} ({monitor.code})\n"
        f"规则：{field_label} {operator_label} {rule.threshold}\n"
        f"命中值：{format(actual, '.3f')}\n"
        f"最新价：{format(quote.last_price, '.3f')}\n"
        f"涨跌幅：{quote.change_pct:.2f}%\n"
        f"时间：{quote.timestamp}\n"
        f"数据源：{quote.source}"
        f"{consecutive_description}"
        f"{note_line}"
    )


class MonitorService:
    def __init__(
        self,
        repository: MonitorRepository,
        quote_provider: QuoteProvider,
        poll_interval_seconds: int,
    ) -> None:
        self.repository = repository
        self.quote_provider = quote_provider
        self.poll_interval_seconds = poll_interval_seconds
        self._quote_refresh_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            await self._task

    async def _reload_poll_interval(self) -> None:
        saved_value = await asyncio.to_thread(self.repository.get_app_setting, "poll_interval_seconds")
        if not saved_value:
            return
        try:
            self.poll_interval_seconds = max(int(saved_value), 5)
        except ValueError:
            logger.warning("Ignored invalid poll interval setting: %s", saved_value)

    async def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self._reload_poll_interval()
                await self._run_spread_cycle()
            except Exception:
                logger.exception("执行监控调度时发生异常")
                await self._sleep_or_stop(1.0)

    async def _run_spread_cycle(self) -> None:
        if not is_monitor_active_time():
            await self._sleep_or_stop(self.poll_interval_seconds)
            return

        monitors = await asyncio.to_thread(self.repository.list_enabled_monitors)
        if not monitors:
            await self._sleep_or_stop(self.poll_interval_seconds)
            return

        loop = asyncio.get_running_loop()
        cycle_started = loop.time()
        task_count = len(monitors)
        spacing = self.poll_interval_seconds / task_count if task_count else self.poll_interval_seconds
        quote_cache: dict[str, QuoteSnapshot | None] = {}

        for index, monitor in enumerate(monitors):
            if self._stop_event.is_set():
                return

            deadline = cycle_started + (index * spacing)
            await self._wait_until(deadline)
            if self._stop_event.is_set():
                return

            try:
                key = quote_cache_key(monitor.code, monitor.instrument_type.value)
                if key not in quote_cache:
                    quotes = await self.fetch_quotes([monitor])
                    quote_cache[key] = quotes.get(key)
                await self._evaluate_monitor(monitor, quote_cache.get(key))
            except Exception:
                logger.exception("执行单个监控任务失败 monitor=%s", monitor.id)

        await self._wait_until(cycle_started + self.poll_interval_seconds)

    async def _wait_until(self, deadline: float) -> None:
        loop = asyncio.get_running_loop()
        await self._sleep_or_stop(max(deadline - loop.time(), 0.0))

    async def _sleep_or_stop(self, seconds: float) -> None:
        if seconds <= 0:
            return
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            return

    async def _emit_event(self, event_name: str, payload: dict) -> None:
        await asyncio.to_thread(self.repository.create_system_event, event_name, payload)

    async def fetch_quotes(
        self,
        monitors: list[MonitorOut],
        *,
        publish_event: bool = True,
    ) -> dict[str, QuoteSnapshot]:
        if not monitors:
            return {}

        async with self._quote_refresh_lock:
            # 多账户可能配置相同标的，这里按 code+instrument_type 去重，避免重复请求行情接口。
            targets_by_key: dict[str, QuoteTarget] = {}
            for monitor in monitors:
                target_key = quote_cache_key(monitor.code, monitor.instrument_type.value)
                if target_key not in targets_by_key:
                    targets_by_key[target_key] = QuoteTarget(
                        code=monitor.code,
                        instrument_type=monitor.instrument_type,
                    )

            quotes = await self.quote_provider.fetch_many(list(targets_by_key.values()))
            await asyncio.to_thread(self.repository.upsert_quote_snapshots, quotes)
            if publish_event and quotes:
                latest_timestamp = max(quote.timestamp for quote in quotes.values())
                await self._emit_event(
                    "quotes_updated",
                    {
                        "updated": len(quotes),
                        "latest_quote_time": latest_timestamp,
                    },
                )
            return quotes

    async def list_monitors_with_quotes(
        self,
        monitors: list[MonitorOut] | None = None,
        user_id: int | None = None,
    ) -> list[MonitorOut]:
        if monitors is None:
            monitors = await asyncio.to_thread(self.repository.list_monitors, user_id)
        quote_map = await asyncio.to_thread(self.repository.get_quote_snapshot_map)

        enriched: list[MonitorOut] = []
        for monitor in monitors:
            key = quote_cache_key(monitor.code, monitor.instrument_type.value)
            enriched.append(monitor.model_copy(update={"latest_quote": quote_map.get(key)}))
        return enriched

    async def refresh_quotes_for_all(self, user_id: int | None = None) -> dict[str, int]:
        monitors = await asyncio.to_thread(self.repository.list_monitors, user_id)
        quotes = await self.fetch_quotes(monitors)
        return {
            "processed": len(monitors),
            "updated": len(quotes),
        }

    async def _reset_rule_hits(self, rules: list[RuleOut]) -> None:
        for rule in rules:
            if rule.current_consecutive_hits > 0:
                await asyncio.to_thread(self.repository.reset_rule_consecutive_hits, rule.id)

    async def _evaluate_monitor(self, monitor: MonitorOut, quote: QuoteSnapshot | None) -> dict[str, int]:
        summary = {"processed": 1, "matched": 0, "queued": 0, "failed": 0}
        if not monitor.enabled:
            await self._reset_rule_hits(monitor.rules)
            return summary
        if quote is None:
            return summary

        enabled_rules: list[RuleOut] = []
        for rule in monitor.rules:
            if not rule.enabled:
                if rule.current_consecutive_hits > 0:
                    await asyncio.to_thread(self.repository.reset_rule_consecutive_hits, rule.id)
                continue
            enabled_rules.append(rule)

        evaluated: list[tuple[RuleOut, float, int, bool]] = []
        for rule in enabled_rules:
            actual = get_rule_value(rule, quote)
            condition_matched = compare_value(rule.operator, actual, rule.threshold)
            consecutive_hits = next_consecutive_hits(rule, condition_matched)

            if consecutive_hits != rule.current_consecutive_hits:
                await asyncio.to_thread(self.repository.set_rule_consecutive_hits, rule.id, consecutive_hits)

            if condition_matched:
                summary["matched"] += 1
            ready = bool(condition_matched and consecutive_hits >= rule.consecutive_hits_required and cooldown_ready(rule))
            evaluated.append((rule, actual, consecutive_hits, ready))

        if monitor.require_all_rules:
            if not evaluated:
                return summary
            if not all(item[3] for item in evaluated):
                return summary
            candidates = evaluated
        else:
            candidates = [item for item in evaluated if item[3]]

        for rule, actual, consecutive_hits, _ in candidates:
            message = build_text_alert(monitor, rule, quote, actual, consecutive_hits)
            try:
                await asyncio.to_thread(
                    self.repository.queue_alert_delivery,
                    monitor_id=monitor.id,
                    rule_id=rule.id,
                    code=monitor.code,
                    webhook_url=monitor.webhook_url,
                    message=message,
                    mentioned_mobiles=monitor.mentioned_mobiles,
                    mentioned_user_ids=monitor.mentioned_user_ids,
                    triggered_value=actual,
                )
                summary["queued"] += 1
            except Exception:
                logger.exception("创建告警任务失败 monitor=%s rule=%s", monitor.id, rule.id)
                summary["failed"] += 1

        if summary["queued"] or summary["failed"]:
            await self._emit_event(
                "alerts_updated",
                {
                    "monitor_id": monitor.id,
                    "queued": summary["queued"],
                    "failed": summary["failed"],
                },
            )
        return summary

    async def run_monitor_task(self, monitor: MonitorOut) -> dict[str, int]:
        if not is_monitor_active_time():
            return {"processed": 0, "matched": 0, "queued": 0, "failed": 0}
        quotes = await self.fetch_quotes([monitor])
        key = quote_cache_key(monitor.code, monitor.instrument_type.value)
        return await self._evaluate_monitor(monitor, quotes.get(key))

    async def run_cycle(self, user_id: int | None = None) -> dict[str, int]:
        if not is_monitor_active_time():
            return {"processed": 0, "matched": 0, "queued": 0, "failed": 0}
        all_monitors = await asyncio.to_thread(self.repository.list_monitors, user_id)
        if not all_monitors:
            return {"processed": 0, "matched": 0, "queued": 0, "failed": 0}

        quotes = await self.fetch_quotes(all_monitors)
        summary = {"processed": len(all_monitors), "matched": 0, "queued": 0, "failed": 0}

        for monitor in all_monitors:
            key = quote_cache_key(monitor.code, monitor.instrument_type.value)
            result = await self._evaluate_monitor(monitor, quotes.get(key))
            summary["matched"] += result["matched"]
            summary["queued"] += result["queued"]
            summary["failed"] += result["failed"]

        return summary
