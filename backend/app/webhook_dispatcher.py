from __future__ import annotations

import asyncio
import logging

from .notifier import WeComNotifier
from .repository import MonitorRepository
from .schemas import WebhookJobRecord


logger = logging.getLogger(__name__)


class WebhookDispatcher:
    def __init__(
        self,
        repository: MonitorRepository,
        notifier: WeComNotifier,
        *,
        idle_poll_seconds: float = 1.0,
        batch_size: int = 10,
        max_retry_delay_seconds: int = 300,
        max_concurrency: int = 4,
        capacity_check_interval_seconds: int = 60,
        queue_warn_threshold: int = 500,
        sqlite_size_warn_mb: int = 512,
        capacity_warn_cooldown_seconds: int = 600,
    ) -> None:
        self.repository = repository
        self.notifier = notifier
        self.idle_poll_seconds = max(idle_poll_seconds, 0.2)
        self.batch_size = max(batch_size, 1)
        self.max_retry_delay_seconds = max(max_retry_delay_seconds, 30)
        self.max_concurrency = max(max_concurrency, 1)
        self.capacity_check_interval_seconds = max(capacity_check_interval_seconds, 10)
        self.queue_warn_threshold = max(queue_warn_threshold, 1)
        self.sqlite_size_warn_bytes = max(sqlite_size_warn_mb, 1) * 1024 * 1024
        self.capacity_warn_cooldown_seconds = max(capacity_warn_cooldown_seconds, 30)
        self._stop_event = asyncio.Event()
        self._next_capacity_check_at = 0.0
        self._last_queue_warn_at = 0.0
        self._last_sqlite_warn_at = 0.0

    async def run_forever(self) -> None:
        self._stop_event.clear()
        while not self._stop_event.is_set():
            await self._maybe_check_runtime_capacity()
            jobs = await asyncio.to_thread(self.repository.claim_pending_webhook_jobs, self.batch_size)
            if not jobs:
                await self._sleep_or_stop(self.idle_poll_seconds)
                continue

            await self._process_batch(jobs)

    def stop(self) -> None:
        self._stop_event.set()

    async def _sleep_or_stop(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            return

    async def _emit_event(self, event_name: str, payload: dict) -> None:
        await asyncio.to_thread(self.repository.create_system_event, event_name, payload)

    async def _process_batch(self, jobs: list[WebhookJobRecord]) -> None:
        semaphore = asyncio.Semaphore(self.max_concurrency)

        async def run_one(job: WebhookJobRecord) -> None:
            async with semaphore:
                if self._stop_event.is_set():
                    return
                await self._process_job(job)

        await asyncio.gather(*(run_one(job) for job in jobs))

    async def _maybe_check_runtime_capacity(self) -> None:
        loop = asyncio.get_running_loop()
        now = loop.time()
        if now < self._next_capacity_check_at:
            return
        self._next_capacity_check_at = now + self.capacity_check_interval_seconds

        stats = await asyncio.to_thread(self.repository.get_webhook_runtime_stats)
        queued_total = int(stats.get("queued_total", 0))
        sqlite_bytes = int(stats.get("sqlite_bytes", 0))

        if (
            queued_total >= self.queue_warn_threshold
            and now - self._last_queue_warn_at >= self.capacity_warn_cooldown_seconds
        ):
            self._last_queue_warn_at = now
            logger.warning(
                "Webhook queue backlog is high: queued=%s threshold=%s",
                queued_total,
                self.queue_warn_threshold,
            )
            await self._emit_event(
                "system_updated",
                {
                    "kind": "webhook_queue_warning",
                    "queued_total": queued_total,
                    "threshold": self.queue_warn_threshold,
                },
            )

        if (
            sqlite_bytes >= self.sqlite_size_warn_bytes
            and now - self._last_sqlite_warn_at >= self.capacity_warn_cooldown_seconds
        ):
            self._last_sqlite_warn_at = now
            size_mb = round(sqlite_bytes / (1024 * 1024), 2)
            threshold_mb = round(self.sqlite_size_warn_bytes / (1024 * 1024), 2)
            logger.warning(
                "SQLite size is high: size_mb=%s threshold_mb=%s",
                size_mb,
                threshold_mb,
            )
            await self._emit_event(
                "system_updated",
                {
                    "kind": "sqlite_size_warning",
                    "sqlite_size_mb": size_mb,
                    "threshold_mb": threshold_mb,
                },
            )

    async def _process_job(self, job: WebhookJobRecord) -> None:
        attempt_count = job.attempt_count + 1
        try:
            await self.notifier.send_text(
                job.webhook_url,
                job.message,
                mentioned_mobiles=job.mentioned_mobiles,
                mentioned_user_ids=job.mentioned_user_ids,
            )
            await asyncio.to_thread(
                self.repository.mark_webhook_job_sent,
                job.id,
                job.alert_event_id,
                attempt_count,
            )
            await self._emit_event(
                "alerts_updated",
                {
                    "alert_event_id": job.alert_event_id,
                    "status": "sent",
                },
            )
        except Exception as exc:
            logger.exception("Webhook dispatch failed job=%s", job.id)
            error = str(exc)
            if attempt_count >= job.max_attempts:
                await asyncio.to_thread(
                    self.repository.mark_webhook_job_failed,
                    job.id,
                    job.alert_event_id,
                    attempt_count,
                    error,
                )
                await self._emit_event(
                    "alerts_updated",
                    {
                        "alert_event_id": job.alert_event_id,
                        "status": "failed",
                    },
                )
                return

            delay_seconds = min(15 * (2 ** (attempt_count - 1)), self.max_retry_delay_seconds)
            await asyncio.to_thread(
                self.repository.mark_webhook_job_retry,
                job.id,
                job.alert_event_id,
                attempt_count,
                error,
                delay_seconds,
            )
            await self._emit_event(
                "alerts_updated",
                {
                    "alert_event_id": job.alert_event_id,
                    "status": "retrying",
                    "attempt_count": attempt_count,
                },
            )
