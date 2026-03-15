from __future__ import annotations

import asyncio
import logging

from .config import Settings
from .database import Database
from .notifier import WeComNotifier
from .repository import MonitorRepository
from .webhook_dispatcher import WebhookDispatcher


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


async def main() -> None:
    settings = Settings.from_env()
    settings.ensure_directories()

    database = Database(settings.sqlite_path)
    repository = MonitorRepository(database)
    notifier = WeComNotifier()
    dispatcher = WebhookDispatcher(
        repository,
        notifier,
        idle_poll_seconds=settings.webhook_idle_poll_seconds,
        batch_size=settings.webhook_batch_size,
        max_retry_delay_seconds=settings.webhook_max_retry_delay_seconds,
        max_concurrency=settings.webhook_max_concurrency,
        capacity_check_interval_seconds=settings.webhook_capacity_check_interval_seconds,
        queue_warn_threshold=settings.webhook_queue_warn_threshold,
        sqlite_size_warn_mb=settings.sqlite_size_warn_mb,
        capacity_warn_cooldown_seconds=settings.webhook_capacity_warn_cooldown_seconds,
    )

    database.init_db()
    await notifier.start()

    try:
        await dispatcher.run_forever()
    except (KeyboardInterrupt, asyncio.CancelledError):
        logging.getLogger(__name__).info("Webhook worker stopping")
    finally:
        dispatcher.stop()
        await notifier.stop()


if __name__ == "__main__":
    asyncio.run(main())
