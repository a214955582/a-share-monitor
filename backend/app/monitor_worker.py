from __future__ import annotations

import asyncio
import logging

from .config import Settings
from .database import Database
from .monitoring import MonitorService
from .quote_provider import build_quote_provider
from .repository import MonitorRepository


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


async def main() -> None:
    settings = Settings.from_env()
    settings.ensure_directories()

    database = Database(settings.sqlite_path)
    repository = MonitorRepository(database)
    quote_provider = build_quote_provider(settings.quote_provider)
    monitor_service = MonitorService(repository, quote_provider, settings.poll_interval_seconds)

    database.init_db()
    await monitor_service.start()

    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logging.getLogger(__name__).info("Monitor worker stopping")
    finally:
        await monitor_service.stop()


if __name__ == "__main__":
    asyncio.run(main())
