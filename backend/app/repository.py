from __future__ import annotations

from collections import defaultdict
from datetime import timedelta
import json
import sqlite3

from .database import Database
from .schemas import (
    AlertEventOut,
    MonitorCreate,
    MonitorOut,
    MonitorUpdate,
    QuoteSnapshot,
    RuleCreate,
    RuleOut,
    RuleUpdate,
    SystemEventRecord,
    UserAuthRecord,
    WebhookJobRecord,
)
from .utils import infer_market, normalize_code, now_iso, now_local, quote_cache_key


class MonitorRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def _deserialize_mentions(self, value: str) -> list[str]:
        if not value:
            return []
        return [item.strip() for item in value.split(",") if item.strip()]

    def _serialize_mentions(self, values: list[str]) -> str:
        return ",".join(item.strip() for item in values if item.strip())

    def _row_to_rule(self, row: sqlite3.Row) -> RuleOut:
        return RuleOut(
            id=row["id"],
            monitor_id=row["monitor_id"],
            field=row["field"],
            operator=row["operator"],
            threshold=row["threshold"],
            cooldown_minutes=row["cooldown_minutes"],
            consecutive_hits_required=row["consecutive_hits_required"],
            current_consecutive_hits=row["current_consecutive_hits"],
            enabled=bool(row["enabled"]),
            description=row["description"],
            last_triggered_at=row["last_triggered_at"],
            last_trigger_value=row["last_trigger_value"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_monitor(self, row: sqlite3.Row, rules: list[RuleOut] | None = None) -> MonitorOut:
        return MonitorOut(
            id=row["id"],
            instrument_type=row["instrument_type"],
            code=row["code"],
            market=row["market"],
            name=row["name"],
            webhook_url=row["webhook_url"],
            mentioned_mobiles=self._deserialize_mentions(row["mentioned_mobiles"]),
            mentioned_user_ids=self._deserialize_mentions(row["mentioned_user_ids"]),
            require_all_rules=bool(row["require_all_rules"]),
            enabled=bool(row["enabled"]),
            note=row["note"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            rules=rules or [],
        )

    def _row_to_alert(self, row: sqlite3.Row) -> AlertEventOut:
        return AlertEventOut(
            id=row["id"],
            monitor_id=row["monitor_id"],
            rule_id=row["rule_id"],
            code=row["code"],
            message=row["message"],
            status=row["status"],
            triggered_value=row["triggered_value"],
            error=row["error"],
            created_at=row["created_at"],
        )

    def _row_to_quote_snapshot(self, row: sqlite3.Row) -> QuoteSnapshot:
        return QuoteSnapshot(
            code=row["code"],
            instrument_type=row["instrument_type"],
            name=row["name"],
            last_price=row["last_price"],
            change_pct=row["change_pct"],
            open_price=row["open_price"],
            high_price=row["high_price"],
            low_price=row["low_price"],
            volume=row["volume"],
            turnover=row["turnover"],
            timestamp=row["quote_timestamp"],
            source=row["source"],
        )

    def _row_to_system_event(self, row: sqlite3.Row) -> SystemEventRecord:
        return SystemEventRecord(
            id=row["id"],
            event_name=row["event_name"],
            payload=row["payload"],
            created_at=row["created_at"],
        )

    def _row_to_user(self, row: sqlite3.Row) -> UserAuthRecord:
        return UserAuthRecord(
            id=row["id"],
            username=row["username"],
            password_hash=row["password_hash"],
            enabled=bool(row["enabled"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_webhook_job(self, row: sqlite3.Row) -> WebhookJobRecord:
        return WebhookJobRecord(
            id=row["id"],
            alert_event_id=row["alert_event_id"],
            monitor_id=row["monitor_id"],
            rule_id=row["rule_id"],
            code=row["code"],
            webhook_url=row["webhook_url"],
            message=row["message"],
            mentioned_mobiles=self._deserialize_mentions(row["mentioned_mobiles"]),
            mentioned_user_ids=self._deserialize_mentions(row["mentioned_user_ids"]),
            triggered_value=row["triggered_value"],
            status=row["status"],
            attempt_count=row["attempt_count"],
            max_attempts=row["max_attempts"],
            available_at=row["available_at"],
            locked_at=row["locked_at"],
            last_error=row["last_error"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def count_users(self) -> int:
        with self.db.connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS total FROM users").fetchone()
        return int(row["total"])

    def get_user_by_username(self, username: str) -> UserAuthRecord | None:
        clean_username = (username or "").strip()
        if not clean_username:
            return None
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE username = ?",
                (clean_username,),
            ).fetchone()
        return self._row_to_user(row) if row else None

    def create_user(self, username: str, password_hash: str) -> UserAuthRecord:
        clean_username = (username or "").strip()
        if not clean_username:
            raise ValueError("用户名不能为空")
        timestamp = now_iso()
        with self.db.connect() as conn:
            try:
                cursor = conn.execute(
                    """
                    INSERT INTO users (username, password_hash, enabled, created_at, updated_at)
                    VALUES (?, ?, 1, ?, ?)
                    """,
                    (clean_username, password_hash, timestamp, timestamp),
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError(f"用户名 {clean_username} 已存在") from exc
            row = conn.execute("SELECT * FROM users WHERE id = ?", (cursor.lastrowid,)).fetchone()
        return self._row_to_user(row)

    def update_user_password(self, username: str, password_hash: str) -> UserAuthRecord:
        clean_username = (username or "").strip()
        timestamp = now_iso()
        with self.db.connect() as conn:
            row = conn.execute("SELECT * FROM users WHERE username = ?", (clean_username,)).fetchone()
            if not row:
                raise ValueError("用户不存在")
            conn.execute(
                """
                UPDATE users
                SET password_hash = ?, updated_at = ?
                WHERE username = ?
                """,
                (password_hash, timestamp, clean_username),
            )
            updated_row = conn.execute("SELECT * FROM users WHERE username = ?", (clean_username,)).fetchone()
        return self._row_to_user(updated_row)

    def list_monitors(self, user_id: int | None = None) -> list[MonitorOut]:
        with self.db.connect() as conn:
            if user_id is None:
                monitor_rows = conn.execute(
                    "SELECT * FROM monitors ORDER BY enabled DESC, instrument_type ASC, code ASC"
                ).fetchall()
                rule_rows = conn.execute("SELECT * FROM rules ORDER BY created_at DESC").fetchall()
            else:
                monitor_rows = conn.execute(
                    """
                    SELECT * FROM monitors
                    WHERE user_id = ?
                    ORDER BY enabled DESC, instrument_type ASC, code ASC
                    """,
                    (user_id,),
                ).fetchall()
                rule_rows = conn.execute(
                    """
                    SELECT rules.* FROM rules
                    INNER JOIN monitors ON monitors.id = rules.monitor_id
                    WHERE monitors.user_id = ?
                    ORDER BY rules.created_at DESC
                    """,
                    (user_id,),
                ).fetchall()

        rules_by_monitor: dict[int, list[RuleOut]] = defaultdict(list)
        for row in rule_rows:
            rules_by_monitor[row["monitor_id"]].append(self._row_to_rule(row))

        return [self._row_to_monitor(row, rules_by_monitor.get(row["id"], [])) for row in monitor_rows]

    def list_enabled_monitors(self, user_id: int | None = None) -> list[MonitorOut]:
        return [monitor for monitor in self.list_monitors(user_id=user_id) if monitor.enabled]

    def get_monitor(self, monitor_id: int, user_id: int | None = None) -> MonitorOut | None:
        for monitor in self.list_monitors(user_id=user_id):
            if monitor.id == monitor_id:
                return monitor
        return None

    def create_monitor(self, payload: MonitorCreate, user_id: int) -> MonitorOut:
        timestamp = now_iso()
        instrument_type = payload.instrument_type.value
        code = normalize_code(payload.code, instrument_type)
        market = infer_market(code[:6], instrument_type)

        with self.db.connect() as conn:
            try:
                cursor = conn.execute(
                    """
                    INSERT INTO monitors (
                        user_id, instrument_type, code, market, name, webhook_url, mentioned_mobiles,
                        mentioned_user_ids, require_all_rules, enabled, note, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        user_id,
                        instrument_type,
                        code,
                        market,
                        payload.name.strip(),
                        payload.webhook_url.strip(),
                        self._serialize_mentions(payload.mentioned_mobiles),
                        self._serialize_mentions(payload.mentioned_user_ids),
                        int(payload.require_all_rules),
                        int(payload.enabled),
                        payload.note.strip(),
                        timestamp,
                        timestamp,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError(f"监控对象 {code} 已存在，请直接编辑现有配置") from exc

            row = conn.execute("SELECT * FROM monitors WHERE id = ? AND user_id = ?", (cursor.lastrowid, user_id)).fetchone()

        return self._row_to_monitor(row, [])

    def update_monitor(self, monitor_id: int, payload: MonitorUpdate, user_id: int) -> MonitorOut:
        timestamp = now_iso()
        instrument_type = payload.instrument_type.value
        code = normalize_code(payload.code, instrument_type)
        market = infer_market(code[:6], instrument_type)

        with self.db.connect() as conn:
            existing = conn.execute("SELECT id FROM monitors WHERE id = ? AND user_id = ?", (monitor_id, user_id)).fetchone()
            if not existing:
                raise ValueError("监控项不存在")

            duplicate = conn.execute(
                "SELECT id FROM monitors WHERE user_id = ? AND code = ? AND instrument_type = ? AND id != ?",
                (user_id, code, instrument_type, monitor_id),
            ).fetchone()
            if duplicate:
                raise ValueError(f"监控对象 {code} 已被其他监控项占用")

            conn.execute(
                """
                UPDATE monitors
                SET instrument_type = ?, code = ?, market = ?, name = ?, webhook_url = ?,
                    mentioned_mobiles = ?, mentioned_user_ids = ?, require_all_rules = ?, enabled = ?, note = ?, updated_at = ?
                WHERE id = ? AND user_id = ?
                """,
                (
                    instrument_type,
                    code,
                    market,
                    payload.name.strip(),
                    payload.webhook_url.strip(),
                    self._serialize_mentions(payload.mentioned_mobiles),
                    self._serialize_mentions(payload.mentioned_user_ids),
                    int(payload.require_all_rules),
                    int(payload.enabled),
                    payload.note.strip(),
                    timestamp,
                    monitor_id,
                    user_id,
                ),
            )
            row = conn.execute("SELECT * FROM monitors WHERE id = ? AND user_id = ?", (monitor_id, user_id)).fetchone()
            rule_rows = conn.execute(
                "SELECT * FROM rules WHERE monitor_id = ? ORDER BY created_at DESC",
                (monitor_id,),
            ).fetchall()

        return self._row_to_monitor(row, [self._row_to_rule(rule_row) for rule_row in rule_rows])

    def delete_monitor(self, monitor_id: int, user_id: int) -> None:
        with self.db.connect() as conn:
            conn.execute("DELETE FROM monitors WHERE id = ? AND user_id = ?", (monitor_id, user_id))

    def create_rule(self, monitor_id: int, payload: RuleCreate, user_id: int) -> RuleOut:
        timestamp = now_iso()
        with self.db.connect() as conn:
            monitor = conn.execute("SELECT id FROM monitors WHERE id = ? AND user_id = ?", (monitor_id, user_id)).fetchone()
            if not monitor:
                raise ValueError("监控项不存在，无法添加规则")

            cursor = conn.execute(
                """
                INSERT INTO rules (
                    monitor_id, field, operator, threshold, cooldown_minutes,
                    consecutive_hits_required, current_consecutive_hits, enabled,
                    description, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
                """,
                (
                    monitor_id,
                    payload.field.value,
                    payload.operator.value,
                    payload.threshold,
                    payload.cooldown_minutes,
                    payload.consecutive_hits_required,
                    int(payload.enabled),
                    payload.description.strip(),
                    timestamp,
                    timestamp,
                ),
            )
            row = conn.execute("SELECT * FROM rules WHERE id = ?", (cursor.lastrowid,)).fetchone()

        return self._row_to_rule(row)

    def update_rule(self, rule_id: int, payload: RuleUpdate, user_id: int) -> RuleOut:
        timestamp = now_iso()
        with self.db.connect() as conn:
            existing = conn.execute(
                """
                SELECT rules.id
                FROM rules
                INNER JOIN monitors ON monitors.id = rules.monitor_id
                WHERE rules.id = ? AND monitors.user_id = ?
                """,
                (rule_id, user_id),
            ).fetchone()
            if not existing:
                raise ValueError("规则不存在")

            conn.execute(
                """
                UPDATE rules
                SET field = ?, operator = ?, threshold = ?, cooldown_minutes = ?,
                    consecutive_hits_required = ?, current_consecutive_hits = 0,
                    enabled = ?, description = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    payload.field.value,
                    payload.operator.value,
                    payload.threshold,
                    payload.cooldown_minutes,
                    payload.consecutive_hits_required,
                    int(payload.enabled),
                    payload.description.strip(),
                    timestamp,
                    rule_id,
                ),
            )
            row = conn.execute(
                """
                SELECT rules.* FROM rules
                INNER JOIN monitors ON monitors.id = rules.monitor_id
                WHERE rules.id = ? AND monitors.user_id = ?
                """,
                (rule_id, user_id),
            ).fetchone()

        return self._row_to_rule(row)

    def set_rule_consecutive_hits(self, rule_id: int, consecutive_hits: int) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE rules
                SET current_consecutive_hits = ?
                WHERE id = ?
                """,
                (max(consecutive_hits, 0), rule_id),
            )

    def reset_rule_consecutive_hits(self, rule_id: int) -> None:
        self.set_rule_consecutive_hits(rule_id, 0)

    def delete_rule(self, rule_id: int, user_id: int) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                DELETE FROM rules
                WHERE id IN (
                    SELECT rules.id
                    FROM rules
                    INNER JOIN monitors ON monitors.id = rules.monitor_id
                    WHERE rules.id = ? AND monitors.user_id = ?
                )
                """,
                (rule_id, user_id),
            )

    def get_quote_snapshot_map(self) -> dict[str, QuoteSnapshot]:
        with self.db.connect() as conn:
            rows = conn.execute("SELECT * FROM quote_snapshots").fetchall()

        snapshots: dict[str, QuoteSnapshot] = {}
        for row in rows:
            snapshot = self._row_to_quote_snapshot(row)
            snapshots[quote_cache_key(snapshot.code, snapshot.instrument_type.value)] = snapshot
        return snapshots

    def upsert_quote_snapshots(self, quotes: dict[str, QuoteSnapshot]) -> int:
        if not quotes:
            return 0

        timestamp = now_iso()
        rows = []
        for snapshot in quotes.values():
            rows.append(
                (
                    quote_cache_key(snapshot.code, snapshot.instrument_type.value),
                    snapshot.code,
                    snapshot.instrument_type.value,
                    snapshot.name,
                    snapshot.last_price,
                    snapshot.change_pct,
                    snapshot.open_price,
                    snapshot.high_price,
                    snapshot.low_price,
                    snapshot.volume,
                    snapshot.turnover,
                    snapshot.timestamp,
                    snapshot.source,
                    timestamp,
                )
            )

        with self.db.connect() as conn:
            conn.executemany(
                """
                INSERT INTO quote_snapshots (
                    cache_key, code, instrument_type, name, last_price, change_pct,
                    open_price, high_price, low_price, volume, turnover,
                    quote_timestamp, source, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    code = excluded.code,
                    instrument_type = excluded.instrument_type,
                    name = excluded.name,
                    last_price = excluded.last_price,
                    change_pct = excluded.change_pct,
                    open_price = excluded.open_price,
                    high_price = excluded.high_price,
                    low_price = excluded.low_price,
                    volume = excluded.volume,
                    turnover = excluded.turnover,
                    quote_timestamp = excluded.quote_timestamp,
                    source = excluded.source,
                    updated_at = excluded.updated_at
                """,
                rows,
            )
        return len(rows)

    def list_alerts(self, page: int = 1, page_size: int = 10, user_id: int | None = None) -> tuple[list[AlertEventOut], int]:
        offset = max(page - 1, 0) * page_size
        with self.db.connect() as conn:
            if user_id is None:
                total = conn.execute("SELECT COUNT(*) AS total FROM alert_events").fetchone()["total"]
                rows = conn.execute(
                    """
                    SELECT * FROM alert_events
                    ORDER BY created_at DESC, id DESC
                    LIMIT ? OFFSET ?
                    """,
                    (page_size, offset),
                ).fetchall()
            else:
                total = conn.execute(
                    """
                    SELECT COUNT(*) AS total
                    FROM alert_events
                    INNER JOIN monitors ON monitors.id = alert_events.monitor_id
                    WHERE monitors.user_id = ?
                    """,
                    (user_id,),
                ).fetchone()["total"]
                rows = conn.execute(
                    """
                    SELECT alert_events.*
                    FROM alert_events
                    INNER JOIN monitors ON monitors.id = alert_events.monitor_id
                    WHERE monitors.user_id = ?
                    ORDER BY alert_events.created_at DESC, alert_events.id DESC
                    LIMIT ? OFFSET ?
                    """,
                    (user_id, page_size, offset),
                ).fetchall()
        return [self._row_to_alert(row) for row in rows], int(total)

    def queue_alert_delivery(
        self,
        *,
        monitor_id: int,
        rule_id: int,
        code: str,
        webhook_url: str,
        message: str,
        mentioned_mobiles: list[str],
        mentioned_user_ids: list[str],
        triggered_value: float,
        max_attempts: int = 3,
    ) -> AlertEventOut:
        timestamp = now_iso()
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE rules
                SET last_triggered_at = ?, last_trigger_value = ?, current_consecutive_hits = 0
                WHERE id = ?
                """,
                (timestamp, triggered_value, rule_id),
            )
            cursor = conn.execute(
                """
                INSERT INTO alert_events (monitor_id, rule_id, code, message, status, triggered_value, error, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (monitor_id, rule_id, code, message, "queued", triggered_value, "", timestamp),
            )
            alert_event_id = int(cursor.lastrowid)
            conn.execute(
                """
                INSERT INTO webhook_jobs (
                    alert_event_id, monitor_id, rule_id, code, webhook_url, message,
                    mentioned_mobiles, mentioned_user_ids, triggered_value, status,
                    attempt_count, max_attempts, available_at, locked_at, last_error,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, NULL, '', ?, ?)
                """,
                (
                    alert_event_id,
                    monitor_id,
                    rule_id,
                    code,
                    webhook_url,
                    message,
                    self._serialize_mentions(mentioned_mobiles),
                    self._serialize_mentions(mentioned_user_ids),
                    triggered_value,
                    max_attempts,
                    timestamp,
                    timestamp,
                    timestamp,
                ),
            )
            row = conn.execute("SELECT * FROM alert_events WHERE id = ?", (alert_event_id,)).fetchone()

        return self._row_to_alert(row)

    def update_alert_status(self, alert_event_id: int, status: str, error: str = "") -> AlertEventOut | None:
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE alert_events
                SET status = ?, error = ?
                WHERE id = ?
                """,
                (status, error, alert_event_id),
            )
            if status in {"sent", "failed"}:
                owner = conn.execute(
                    """
                    SELECT monitors.user_id AS user_id
                    FROM alert_events
                    INNER JOIN monitors ON monitors.id = alert_events.monitor_id
                    WHERE alert_events.id = ?
                    """,
                    (alert_event_id,),
                ).fetchone()
                if owner:
                    self._trim_alert_history(conn, int(owner["user_id"]))
            row = conn.execute("SELECT * FROM alert_events WHERE id = ?", (alert_event_id,)).fetchone()
        return self._row_to_alert(row) if row else None

    def create_system_event(self, event_name: str, payload: dict) -> int:
        timestamp = now_iso()
        payload_text = json.dumps(payload, ensure_ascii=False)
        with self.db.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO system_events (event_name, payload, created_at)
                VALUES (?, ?, ?)
                """,
                (event_name, payload_text, timestamp),
            )
            conn.execute(
                """
                DELETE FROM system_events
                WHERE id NOT IN (
                    SELECT id FROM system_events
                    ORDER BY id DESC
                    LIMIT 500
                )
                """
            )
        return int(cursor.lastrowid)

    def list_system_events_after(self, last_event_id: int, limit: int = 50) -> list[SystemEventRecord]:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM system_events
                WHERE id > ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (max(last_event_id, 0), max(limit, 1)),
            ).fetchall()
        return [self._row_to_system_event(row) for row in rows]

    def get_webhook_runtime_stats(self) -> dict[str, int]:
        with self.db.connect() as conn:
            queued_total = conn.execute(
                """
                SELECT COUNT(*) AS total
                FROM webhook_jobs
                WHERE status IN ('pending', 'retrying', 'processing')
                """
            ).fetchone()["total"]

        try:
            sqlite_bytes = int(self.db.path.stat().st_size)
        except OSError:
            sqlite_bytes = 0

        return {
            "queued_total": int(queued_total),
            "sqlite_bytes": sqlite_bytes,
        }

    def claim_pending_webhook_jobs(self, limit: int = 10, stale_after_minutes: int = 5) -> list[WebhookJobRecord]:
        timestamp = now_iso()
        stale_before = (now_local() - timedelta(minutes=max(stale_after_minutes, 1))).isoformat(timespec="seconds")
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """
                SELECT * FROM webhook_jobs
                WHERE
                    ((status = 'pending' OR status = 'retrying') AND available_at <= ?)
                    OR (status = 'processing' AND locked_at IS NOT NULL AND locked_at <= ?)
                ORDER BY available_at ASC, id ASC
                LIMIT ?
                """,
                (timestamp, stale_before, max(limit, 1)),
            ).fetchall()
            if not rows:
                return []

            conn.executemany(
                """
                UPDATE webhook_jobs
                SET status = 'processing', locked_at = ?, updated_at = ?
                WHERE id = ?
                """,
                [(timestamp, timestamp, row["id"]) for row in rows],
            )
            refreshed_rows = conn.execute(
                f"""
                SELECT * FROM webhook_jobs
                WHERE id IN ({",".join("?" for _ in rows)})
                ORDER BY id ASC
                """,
                [row["id"] for row in rows],
            ).fetchall()

        return [self._row_to_webhook_job(row) for row in refreshed_rows]

    def mark_webhook_job_sent(self, job_id: int, alert_event_id: int, attempt_count: int) -> None:
        timestamp = now_iso()
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE webhook_jobs
                SET status = 'sent', attempt_count = ?, locked_at = NULL, last_error = '', updated_at = ?
                WHERE id = ?
                """,
                (attempt_count, timestamp, job_id),
            )
        self.update_alert_status(alert_event_id, "sent", "")

    def mark_webhook_job_retry(
        self,
        job_id: int,
        alert_event_id: int,
        attempt_count: int,
        error: str,
        delay_seconds: int,
    ) -> None:
        timestamp = now_iso()
        available_at = (now_local() + timedelta(seconds=max(delay_seconds, 1))).isoformat(timespec="seconds")
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE webhook_jobs
                SET status = 'retrying', attempt_count = ?, available_at = ?, locked_at = NULL,
                    last_error = ?, updated_at = ?
                WHERE id = ?
                """,
                (attempt_count, available_at, error, timestamp, job_id),
            )
        self.update_alert_status(alert_event_id, "retrying", error)

    def mark_webhook_job_failed(
        self,
        job_id: int,
        alert_event_id: int,
        attempt_count: int,
        error: str,
    ) -> None:
        timestamp = now_iso()
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE webhook_jobs
                SET status = 'failed', attempt_count = ?, locked_at = NULL, last_error = ?, updated_at = ?
                WHERE id = ?
                """,
                (attempt_count, error, timestamp, job_id),
            )
        self.update_alert_status(alert_event_id, "failed", error)

    def get_app_setting(self, key: str) -> str | None:
        with self.db.connect() as conn:
            row = conn.execute("SELECT value FROM app_settings WHERE key = ?", (key,)).fetchone()
        return str(row["value"]) if row else None

    def set_app_setting(self, key: str, value: str) -> None:
        timestamp = now_iso()
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO app_settings (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                (key, value, timestamp),
            )

    def clear_alerts(self, user_id: int) -> int:
        with self.db.connect() as conn:
            total = conn.execute(
                """
                SELECT COUNT(*) AS total
                FROM alert_events
                INNER JOIN monitors ON monitors.id = alert_events.monitor_id
                WHERE monitors.user_id = ?
                  AND alert_events.status IN ('sent', 'failed')
                """
                ,
                (user_id,),
            ).fetchone()["total"]
            conn.execute(
                """
                DELETE FROM webhook_jobs
                WHERE alert_event_id IN (
                    SELECT alert_events.id
                    FROM alert_events
                    INNER JOIN monitors ON monitors.id = alert_events.monitor_id
                    WHERE monitors.user_id = ?
                      AND alert_events.status IN ('sent', 'failed')
                )
                """,
                (user_id,),
            )
            conn.execute(
                """
                DELETE FROM alert_events
                WHERE id IN (
                    SELECT alert_events.id
                    FROM alert_events
                    INNER JOIN monitors ON monitors.id = alert_events.monitor_id
                    WHERE monitors.user_id = ?
                      AND alert_events.status IN ('sent', 'failed')
                )
                """,
                (user_id,),
            )
        return int(total)

    def _trim_alert_history(self, conn: sqlite3.Connection, user_id: int, limit: int = 200) -> None:
        conn.execute(
            """
            DELETE FROM alert_events
            WHERE id IN (
                SELECT alert_events.id
                FROM alert_events
                INNER JOIN monitors ON monitors.id = alert_events.monitor_id
                WHERE monitors.user_id = ?
                  AND alert_events.status IN ('sent', 'failed')
                ORDER BY alert_events.created_at DESC, alert_events.id DESC
                LIMIT -1 OFFSET ?
              )
            """,
            (user_id, max(limit, 1)),
        )
