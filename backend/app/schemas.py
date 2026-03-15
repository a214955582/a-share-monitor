from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .utils import normalize_code


class InstrumentType(str, Enum):
    STOCK = "stock"
    INDEX = "index"


class RuleField(str, Enum):
    LAST_PRICE = "last_price"
    CHANGE_PCT = "change_pct"
    OPEN_PRICE = "open_price"
    HIGH_PRICE = "high_price"
    LOW_PRICE = "low_price"
    VOLUME = "volume"
    TURNOVER = "turnover"


class RuleOperator(str, Enum):
    GTE = "gte"
    LTE = "lte"
    EQ = "eq"
    NEQ = "neq"


class MonitorBase(BaseModel):
    instrument_type: InstrumentType = InstrumentType.STOCK
    code: str = Field(..., description="代码，例如 600519、000001.SZ、1A0001")
    name: str = Field(default="", max_length=64)
    webhook_url: str = Field(..., description="企业微信机器人 webhook")
    mentioned_mobiles: list[str] = Field(default_factory=list, description="企业微信提醒手机号列表")
    mentioned_user_ids: list[str] = Field(default_factory=list, description="企业微信提醒 UserId 列表")
    require_all_rules: bool = Field(default=False, description="是否要求全部启用规则同时满足才触发")
    enabled: bool = True
    note: str = Field(default="", max_length=255)

    @model_validator(mode="after")
    def normalize_monitor_code(self) -> "MonitorBase":
        self.code = normalize_code(self.code, self.instrument_type.value)
        return self

    @field_validator("webhook_url")
    @classmethod
    def validate_webhook_url(cls, value: str) -> str:
        if not value.startswith(("http://", "https://")):
            raise ValueError("企业微信 webhook 必须以 http:// 或 https:// 开头")
        return value

    @field_validator("mentioned_mobiles", "mentioned_user_ids", mode="before")
    @classmethod
    def normalize_mentions(cls, value):
        if value is None or value == "":
            return []
        if isinstance(value, str):
            raw_items = value.replace("，", ",").split(",")
            return [item.strip() for item in raw_items if item.strip()]
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        return []


class MonitorCreate(MonitorBase):
    pass


class MonitorUpdate(MonitorBase):
    pass


class RuleBase(BaseModel):
    field: RuleField
    operator: RuleOperator
    threshold: float
    cooldown_minutes: int = Field(default=5, ge=0, le=1440)
    consecutive_hits_required: int = Field(default=1, ge=1, le=100)
    enabled: bool = True
    description: str = Field(default="", max_length=255)


class RuleCreate(RuleBase):
    pass


class RuleUpdate(RuleBase):
    pass


class RuleOut(RuleBase):
    id: int
    monitor_id: int
    current_consecutive_hits: int = 0
    last_triggered_at: str | None = None
    last_trigger_value: float | None = None
    created_at: str
    updated_at: str

    model_config = ConfigDict(from_attributes=True)


class QuoteTarget(BaseModel):
    code: str
    instrument_type: InstrumentType


class QuoteSnapshot(BaseModel):
    code: str
    instrument_type: InstrumentType
    name: str = ""
    last_price: float
    change_pct: float
    open_price: float
    high_price: float
    low_price: float
    volume: float
    turnover: float
    timestamp: str
    source: str


class MonitorOut(MonitorBase):
    id: int
    market: str
    created_at: str
    updated_at: str
    rules: list[RuleOut] = Field(default_factory=list)
    latest_quote: QuoteSnapshot | None = None

    model_config = ConfigDict(from_attributes=True)


class AlertEventOut(BaseModel):
    id: int
    monitor_id: int
    rule_id: int | None
    code: str
    message: str
    status: str
    triggered_value: float | None = None
    error: str = ""
    created_at: str

    model_config = ConfigDict(from_attributes=True)


class AlertPageOut(BaseModel):
    items: list[AlertEventOut]
    page: int
    page_size: int
    total: int
    total_pages: int


class UserAuthRecord(BaseModel):
    id: int
    username: str
    password_hash: str
    enabled: bool
    created_at: str
    updated_at: str

    model_config = ConfigDict(from_attributes=True)


class SystemInfo(BaseModel):
    app_name: str
    poll_interval_seconds: int
    quote_provider: str


class PollIntervalUpdate(BaseModel):
    poll_interval_seconds: int = Field(..., ge=5, le=3600)


class AuthLoginRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=64)
    password: str = Field(..., min_length=1, max_length=256)


class AuthRegisterRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=64)
    password: str = Field(..., min_length=1, max_length=256)
    registration_code: str = Field(..., min_length=1, max_length=256)


class PasswordResetRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=64)
    new_password: str = Field(..., min_length=1, max_length=256)
    registration_code: str = Field(..., min_length=1, max_length=256)


class AuthLoginResponse(BaseModel):
    authenticated: bool
    username: str = ""
    token: str = ""
    expires_at: str = ""


class AuthStatus(BaseModel):
    authenticated: bool
    username: str = ""
    account_initialized: bool = False
    registered_user_count: int = 0


class ClearAlertsResponse(BaseModel):
    cleared: int


class MetadataItem(BaseModel):
    value: str
    label: str


class MetadataOut(BaseModel):
    instrument_types: list[MetadataItem]
    fields: list[MetadataItem]
    operators: list[MetadataItem]


class SystemEventRecord(BaseModel):
    id: int
    event_name: str
    payload: str
    created_at: str

    model_config = ConfigDict(from_attributes=True)


class WebhookJobRecord(BaseModel):
    id: int
    alert_event_id: int
    monitor_id: int
    rule_id: int | None
    code: str
    webhook_url: str
    message: str
    mentioned_mobiles: list[str] = Field(default_factory=list)
    mentioned_user_ids: list[str] = Field(default_factory=list)
    triggered_value: float | None = None
    status: str
    attempt_count: int
    max_attempts: int
    available_at: str
    locked_at: str | None = None
    last_error: str = ""
    created_at: str
    updated_at: str

    model_config = ConfigDict(from_attributes=True)
