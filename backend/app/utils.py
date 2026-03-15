from __future__ import annotations

from datetime import datetime
import re
from zoneinfo import ZoneInfo


CHINA_TZ = ZoneInfo("Asia/Shanghai")
MARKET_PATTERN = re.compile(r"^(?:(SH|SZ|BJ)[\.\-_ ]?(\d{6})|(\d{6})[\.\-_ ]?(SH|SZ|BJ))$")

INDEX_ALIAS_MAP = {
    "1A0001": ("000001", "SH", "上证指数"),
    "上证指数": ("000001", "SH", "上证指数"),
    "上证综指": ("000001", "SH", "上证指数"),
    "SH000001": ("000001", "SH", "上证指数"),
    "000001.SH": ("000001", "SH", "上证指数"),
}

KNOWN_INDEX_NAMES = {
    "000001.SH": "上证指数",
    "000016.SH": "上证50",
    "000300.SH": "沪深300",
    "000688.SH": "科创50",
    "399001.SZ": "深证成指",
    "399006.SZ": "创业板指",
    "899050.BJ": "北证50",
}


def now_local() -> datetime:
    return datetime.now(CHINA_TZ)


def now_iso() -> str:
    return now_local().isoformat(timespec="seconds")


def now_display() -> str:
    return now_local().strftime("%Y-%m-%d %H:%M:%S")


def display_from_timestamp(timestamp: int | float | str | None) -> str:
    if timestamp in (None, "", 0, "0"):
        return now_display()
    return datetime.fromtimestamp(float(timestamp), CHINA_TZ).strftime("%Y-%m-%d %H:%M:%S")


def display_from_date_time(date_text: str | None, time_text: str | None) -> str:
    if date_text and time_text:
        return f"{date_text.strip()} {time_text.strip()}"
    return now_display()


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def infer_stock_market(code: str) -> str:
    digits = code[:6]
    if digits.startswith(("5", "6", "9")):
        return "SH"
    if digits.startswith(("4", "8")):
        return "BJ"
    return "SZ"


def infer_index_market(code: str) -> str:
    digits = code[:6]
    if digits.startswith("399"):
        return "SZ"
    if digits.startswith("899"):
        return "BJ"
    return "SH"


def infer_market(code: str, instrument_type: str = "stock") -> str:
    return infer_index_market(code) if instrument_type == "index" else infer_stock_market(code)


def split_market_code(raw_code: str) -> tuple[str, str | None]:
    value = (raw_code or "").strip().upper()
    matched = MARKET_PATTERN.match(value)
    if matched:
        prefix_market, prefix_digits, suffix_digits, suffix_market = matched.groups()
        return (prefix_digits or suffix_digits), (prefix_market or suffix_market)

    digits = "".join(ch for ch in value if ch.isdigit())
    return digits, None


def normalize_code(raw_code: str, instrument_type: str = "stock") -> str:
    value = (raw_code or "").strip().upper()
    if not value:
        raise ValueError("代码不能为空")

    if instrument_type == "index" and value in INDEX_ALIAS_MAP:
        digits, market, _ = INDEX_ALIAS_MAP[value]
        return f"{digits}.{market}"

    digits, explicit_market = split_market_code(value)
    if len(digits) != 6:
        if instrument_type == "index":
            raise ValueError("指数代码格式不正确，例如 1A0001、000001.SH 或 SH000001")
        raise ValueError("股票代码必须是 6 位数字，例如 600519 或 000001.SZ")

    if instrument_type == "index":
        market = explicit_market or infer_index_market(digits)
    else:
        inferred_market = infer_stock_market(digits)
        if explicit_market and explicit_market != inferred_market:
            raise ValueError(f"股票代码 {digits} 的市场应为 {inferred_market}，请检查输入")
        market = explicit_market or inferred_market

    return f"{digits}.{market}"


def safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def display_stock_name(code: str, name: str | None) -> str:
    clean_name = (name or "").strip()
    return clean_name or code


def quote_cache_key(code: str, instrument_type: str) -> str:
    normalized_code = normalize_code(code, instrument_type)
    return f"{instrument_type}:{normalized_code}"


def eastmoney_secid(code: str, instrument_type: str) -> str:
    normalized_code = normalize_code(code, instrument_type)
    digits, market = normalized_code.split(".")
    market_map = {
        "SH": "1",
        "SZ": "0",
        "BJ": "0",
    }
    return f"{market_map.get(market, '0')}.{digits}"


def sina_symbol(code: str, instrument_type: str) -> str:
    normalized_code = normalize_code(code, instrument_type)
    digits, market = normalized_code.split(".")
    return f"{market.lower()}{digits}"


def to_provider_symbol(code: str, instrument_type: str) -> str:
    normalized_code = normalize_code(code, instrument_type)
    digits, market = normalized_code.split(".")
    if instrument_type == "index":
        return f"{market.lower()}{digits}"
    return normalized_code


def default_display_name(code: str, instrument_type: str) -> str:
    normalized_code = normalize_code(code, instrument_type)
    if instrument_type == "index":
        return KNOWN_INDEX_NAMES.get(normalized_code, normalized_code)
    return normalized_code
