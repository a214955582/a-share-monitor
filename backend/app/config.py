from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"


@dataclass(slots=True)
class Settings:
    app_name: str = "A股股票监视器"
    sqlite_path: Path = DATA_DIR / "monitor.db"
    poll_interval_seconds: int = 30
    quote_provider: str = "eastmoney"
    login_username: str = ""
    login_password: str = ""
    registration_code: str = "change-this-registration-code"
    webhook_idle_poll_seconds: float = 1.0
    webhook_batch_size: int = 10
    webhook_max_concurrency: int = 4
    webhook_max_retry_delay_seconds: int = 300
    webhook_capacity_check_interval_seconds: int = 60
    webhook_queue_warn_threshold: int = 500
    sqlite_size_warn_mb: int = 512
    webhook_capacity_warn_cooldown_seconds: int = 600

    @classmethod
    def from_env(cls) -> "Settings":
        sqlite_path = Path(os.getenv("SQLITE_PATH", DATA_DIR / "monitor.db"))
        if not sqlite_path.is_absolute():
            sqlite_path = BASE_DIR / sqlite_path

        poll_interval_seconds = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))
        quote_provider = os.getenv("QUOTE_PROVIDER", "eastmoney").strip().lower() or "eastmoney"
        login_username = os.getenv("LOGIN_USERNAME", "").strip()
        login_password = os.getenv("LOGIN_PASSWORD", "").strip()
        registration_code = os.getenv("REGISTRATION_CODE", "change-this-registration-code").strip()
        webhook_idle_poll_seconds = float(os.getenv("WEBHOOK_IDLE_POLL_SECONDS", "1.0"))
        webhook_batch_size = int(os.getenv("WEBHOOK_BATCH_SIZE", "10"))
        webhook_max_concurrency = int(os.getenv("WEBHOOK_MAX_CONCURRENCY", "4"))
        webhook_max_retry_delay_seconds = int(os.getenv("WEBHOOK_MAX_RETRY_DELAY_SECONDS", "300"))
        webhook_capacity_check_interval_seconds = int(os.getenv("WEBHOOK_CAPACITY_CHECK_INTERVAL_SECONDS", "60"))
        webhook_queue_warn_threshold = int(os.getenv("WEBHOOK_QUEUE_WARN_THRESHOLD", "500"))
        sqlite_size_warn_mb = int(os.getenv("SQLITE_SIZE_WARN_MB", "512"))
        webhook_capacity_warn_cooldown_seconds = int(os.getenv("WEBHOOK_CAPACITY_WARN_COOLDOWN_SECONDS", "600"))

        return cls(
            sqlite_path=sqlite_path,
            poll_interval_seconds=max(poll_interval_seconds, 5),
            quote_provider=quote_provider,
            login_username=login_username,
            login_password=login_password,
            registration_code=registration_code or "change-this-registration-code",
            webhook_idle_poll_seconds=max(webhook_idle_poll_seconds, 0.2),
            webhook_batch_size=max(webhook_batch_size, 1),
            webhook_max_concurrency=max(webhook_max_concurrency, 1),
            webhook_max_retry_delay_seconds=max(webhook_max_retry_delay_seconds, 30),
            webhook_capacity_check_interval_seconds=max(webhook_capacity_check_interval_seconds, 10),
            webhook_queue_warn_threshold=max(webhook_queue_warn_threshold, 1),
            sqlite_size_warn_mb=max(sqlite_size_warn_mb, 1),
            webhook_capacity_warn_cooldown_seconds=max(webhook_capacity_warn_cooldown_seconds, 30),
        )

    def ensure_directories(self) -> None:
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
