from __future__ import annotations

import sqlite3
from pathlib import Path


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    def init_db(self) -> None:
        with self.connect() as conn:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS monitors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    instrument_type TEXT NOT NULL DEFAULT 'stock',
                    code TEXT NOT NULL,
                    market TEXT NOT NULL,
                    name TEXT NOT NULL DEFAULT '',
                    webhook_url TEXT NOT NULL,
                    mentioned_mobiles TEXT NOT NULL DEFAULT '',
                    mentioned_user_ids TEXT NOT NULL DEFAULT '',
                    require_all_rules INTEGER NOT NULL DEFAULT 0,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    note TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                    UNIQUE (user_id, instrument_type, code)
                );

                CREATE TABLE IF NOT EXISTS rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    monitor_id INTEGER NOT NULL,
                    field TEXT NOT NULL,
                    operator TEXT NOT NULL,
                    threshold REAL NOT NULL,
                    cooldown_minutes INTEGER NOT NULL DEFAULT 5,
                    consecutive_hits_required INTEGER NOT NULL DEFAULT 1,
                    current_consecutive_hits INTEGER NOT NULL DEFAULT 0,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    description TEXT NOT NULL DEFAULT '',
                    last_triggered_at TEXT,
                    last_trigger_value REAL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (monitor_id) REFERENCES monitors(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL COLLATE NOCASE UNIQUE,
                    password_hash TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS alert_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    monitor_id INTEGER NOT NULL,
                    rule_id INTEGER,
                    code TEXT NOT NULL,
                    message TEXT NOT NULL,
                    status TEXT NOT NULL,
                    triggered_value REAL,
                    error TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (monitor_id) REFERENCES monitors(id) ON DELETE CASCADE,
                    FOREIGN KEY (rule_id) REFERENCES rules(id) ON DELETE SET NULL
                );

                CREATE TABLE IF NOT EXISTS app_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS quote_snapshots (
                    cache_key TEXT PRIMARY KEY,
                    code TEXT NOT NULL,
                    instrument_type TEXT NOT NULL,
                    name TEXT NOT NULL DEFAULT '',
                    last_price REAL NOT NULL DEFAULT 0,
                    change_pct REAL NOT NULL DEFAULT 0,
                    open_price REAL NOT NULL DEFAULT 0,
                    high_price REAL NOT NULL DEFAULT 0,
                    low_price REAL NOT NULL DEFAULT 0,
                    volume REAL NOT NULL DEFAULT 0,
                    turnover REAL NOT NULL DEFAULT 0,
                    quote_timestamp TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS webhook_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    alert_event_id INTEGER NOT NULL,
                    monitor_id INTEGER NOT NULL,
                    rule_id INTEGER,
                    code TEXT NOT NULL,
                    webhook_url TEXT NOT NULL,
                    message TEXT NOT NULL,
                    mentioned_mobiles TEXT NOT NULL DEFAULT '',
                    mentioned_user_ids TEXT NOT NULL DEFAULT '',
                    triggered_value REAL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 3,
                    available_at TEXT NOT NULL,
                    locked_at TEXT,
                    last_error TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (alert_event_id) REFERENCES alert_events(id) ON DELETE CASCADE,
                    FOREIGN KEY (monitor_id) REFERENCES monitors(id) ON DELETE CASCADE,
                    FOREIGN KEY (rule_id) REFERENCES rules(id) ON DELETE SET NULL
                );

                CREATE TABLE IF NOT EXISTS system_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_name TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_rules_monitor_id ON rules (monitor_id);
                CREATE INDEX IF NOT EXISTS idx_alert_events_created_at ON alert_events (created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_quote_snapshots_updated_at ON quote_snapshots (updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_webhook_jobs_status_available_at ON webhook_jobs (status, available_at, id);
                CREATE INDEX IF NOT EXISTS idx_system_events_id ON system_events (id);
                """
            )
