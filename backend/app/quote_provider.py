from __future__ import annotations

import asyncio
import logging
import re
from typing import Protocol

import httpx

from .schemas import QuoteSnapshot, QuoteTarget
from .utils import (
    display_from_date_time,
    display_from_timestamp,
    eastmoney_secid,
    normalize_code,
    quote_cache_key,
    sina_symbol,
)


logger = logging.getLogger(__name__)


class QuoteProvider(Protocol):
    async def fetch_many(self, targets: list[QuoteTarget]) -> dict[str, QuoteSnapshot]:
        ...


class EastmoneyQuoteProvider:
    API_URL = "https://push2.eastmoney.com/api/qt/stock/get"
    SINA_API_URL = "https://hq.sinajs.cn/list="
    FIELDS = "f57,f58,f43,f44,f45,f46,f47,f48,f59,f170,f86"
    UT = "fa5fd1943c7b386f172d6893dbfba10b"
    PRICE_FIELDS = {"f43", "f44", "f45", "f46"}
    SINA_LINE_PATTERN = re.compile(r'^var hq_str_(?P<symbol>[^=]+)="(?P<data>.*)";$')

    def __init__(
        self,
        concurrency: int = 5,
        batch_size: int = 10,
        batch_pause_seconds: float = 0.35,
    ) -> None:
        self.concurrency = max(concurrency, 1)
        self.batch_size = max(batch_size, 1)
        self.batch_pause_seconds = max(batch_pause_seconds, 0.0)
        self._semaphore = asyncio.Semaphore(self.concurrency)

    def _headers(self) -> dict[str, str]:
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/132.0.0.0 Safari/537.36"
            ),
            "Referer": "https://quote.eastmoney.com/",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9",
        }

    def _decode_value(self, payload: dict, key: str, scale: int = 100) -> float:
        value = payload.get(key)
        if value in (None, "-"):
            return 0.0
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return 0.0
        if key in self.PRICE_FIELDS:
            return numeric / scale
        return numeric

    async def _fetch_one(self, client: httpx.AsyncClient, target: QuoteTarget) -> tuple[str, QuoteSnapshot | None]:
        instrument_type = target.instrument_type.value
        code = normalize_code(target.code, instrument_type)
        cache_key = quote_cache_key(code, instrument_type)
        params = {
            "secid": eastmoney_secid(code, instrument_type),
            "fields": self.FIELDS,
            "fltt": "1",
            "invt": "2",
            "ut": self.UT,
        }

        async with self._semaphore:
            try:
                response = await client.get(self.API_URL, params=params)
                response.raise_for_status()
                payload = response.json()
            except Exception as exc:
                logger.warning("Eastmoney quote request failed for %s: %s", code, exc)
                return cache_key, None

        data = payload.get("data") or {}
        if not data:
            logger.warning("Eastmoney returned empty payload for %s", code)
            return cache_key, None

        name = str(data.get("f58") or code).strip() or code
        decimal_digit = int(data.get("f59") or 2)
        scale = 10 ** decimal_digit
        snapshot = QuoteSnapshot(
            code=code,
            instrument_type=target.instrument_type,
            name=name,
            last_price=self._decode_value(data, "f43", scale),
            change_pct=self._decode_value(data, "f170") / 100,
            open_price=self._decode_value(data, "f46", scale),
            high_price=self._decode_value(data, "f44", scale),
            low_price=self._decode_value(data, "f45", scale),
            volume=self._decode_value(data, "f47"),
            turnover=self._decode_value(data, "f48"),
            timestamp=display_from_timestamp(data.get("f86")),
            source="eastmoney-stock-get",
        )
        return cache_key, snapshot

    def _sina_headers(self) -> dict[str, str]:
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/132.0.0.0 Safari/537.36"
            ),
            "Referer": "https://finance.sina.com.cn/",
        }

    def _parse_sina_payload(
        self,
        line: str,
        target: QuoteTarget,
    ) -> tuple[str, QuoteSnapshot | None]:
        instrument_type = target.instrument_type.value
        code = normalize_code(target.code, instrument_type)
        cache_key = quote_cache_key(code, instrument_type)

        match = self.SINA_LINE_PATTERN.match(line.strip())
        if not match:
            return cache_key, None

        raw_data = match.group("data")
        if not raw_data:
            return cache_key, None

        parts = [part.strip() for part in raw_data.split(",")]
        while parts and parts[-1] == "":
            parts.pop()
        if len(parts) < 10:
            return cache_key, None

        name = parts[0].strip() or code
        open_price = float(parts[1] or 0)
        prev_close = float(parts[2] or 0)
        last_price = float(parts[3] or 0)
        high_price = float(parts[4] or 0)
        low_price = float(parts[5] or 0)
        volume = float(parts[8] or 0)
        turnover = float(parts[9] or 0)
        change_pct = ((last_price - prev_close) / prev_close * 100) if prev_close else 0.0
        timestamp = display_from_date_time(parts[-3] if len(parts) >= 3 else "", parts[-2] if len(parts) >= 2 else "")

        snapshot = QuoteSnapshot(
            code=code,
            instrument_type=target.instrument_type,
            name=name,
            last_price=last_price,
            change_pct=change_pct,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            volume=volume,
            turnover=turnover,
            timestamp=timestamp,
            source="sina-hq",
        )
        return cache_key, snapshot

    async def _fetch_sina_backup(self, targets: list[QuoteTarget]) -> dict[str, QuoteSnapshot]:
        if not targets:
            return {}

        timeout = httpx.Timeout(connect=5.0, read=8.0, write=5.0, pool=5.0)
        limits = httpx.Limits(max_keepalive_connections=4, max_connections=4)
        snapshots: dict[str, QuoteSnapshot] = {}

        async with httpx.AsyncClient(
            timeout=timeout,
            limits=limits,
            headers=self._sina_headers(),
            trust_env=False,
            http2=False,
        ) as client:
            for start in range(0, len(targets), 80):
                batch = targets[start : start + 80]
                symbols = ",".join(sina_symbol(target.code, target.instrument_type.value) for target in batch)
                try:
                    response = await client.get(f"{self.SINA_API_URL}{symbols}")
                    response.raise_for_status()
                    payload_text = response.content.decode("gbk", errors="ignore")
                except Exception as exc:
                    logger.warning("Sina backup quote request failed for %s target(s): %s", len(batch), exc)
                    continue

                target_by_symbol = {
                    sina_symbol(target.code, target.instrument_type.value): target
                    for target in batch
                }
                for line in payload_text.splitlines():
                    match = self.SINA_LINE_PATTERN.match(line.strip())
                    if not match:
                        continue
                    symbol = match.group("symbol")
                    target = target_by_symbol.get(symbol)
                    if not target:
                        continue
                    cache_key, snapshot = self._parse_sina_payload(line, target)
                    if snapshot is not None:
                        snapshots[cache_key] = snapshot

                if start + 80 < len(targets):
                    await asyncio.sleep(self.batch_pause_seconds)

        return snapshots

    async def fetch_many(self, targets: list[QuoteTarget]) -> dict[str, QuoteSnapshot]:
        if not targets:
            return {}

        timeout = httpx.Timeout(connect=5.0, read=8.0, write=5.0, pool=5.0)
        limits = httpx.Limits(max_keepalive_connections=self.concurrency, max_connections=self.concurrency)
        results: list[tuple[str, QuoteSnapshot | None]] = []

        async with httpx.AsyncClient(
            timeout=timeout,
            limits=limits,
            headers=self._headers(),
            trust_env=False,
            http2=False,
        ) as client:
            for start in range(0, len(targets), self.batch_size):
                batch = targets[start : start + self.batch_size]
                batch_results = await asyncio.gather(*(self._fetch_one(client, target) for target in batch))
                results.extend(batch_results)
                if start + self.batch_size < len(targets) and self.batch_pause_seconds > 0:
                    await asyncio.sleep(self.batch_pause_seconds)

        snapshots: dict[str, QuoteSnapshot] = {}
        for cache_key, snapshot in results:
            if snapshot is not None:
                snapshots[cache_key] = snapshot

        missing_targets = [
            target
            for target in targets
            if quote_cache_key(target.code, target.instrument_type.value) not in snapshots
        ]
        if missing_targets:
            backup_snapshots = await self._fetch_sina_backup(missing_targets)
            snapshots.update(backup_snapshots)
        return snapshots


def build_quote_provider(name: str) -> QuoteProvider:
    normalized = (name or "eastmoney").strip().lower()
    if normalized not in {"eastmoney", "eastmoney-dual"}:
        logger.warning("Ignoring unsupported quote provider '%s'; using eastmoney only.", normalized)
    return EastmoneyQuoteProvider()
