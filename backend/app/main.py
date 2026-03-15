from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from .auth import LoginAuthManager
from .config import Settings
from .database import Database
from .monitoring import FIELD_LABELS, OPERATOR_LABELS, MonitorService
from .quote_provider import build_quote_provider
from .repository import MonitorRepository
from .schemas import (
    AlertPageOut,
    AuthLoginRequest,
    AuthLoginResponse,
    AuthRegisterRequest,
    AuthStatus,
    ClearAlertsResponse,
    InstrumentType,
    MetadataOut,
    MonitorCreate,
    MonitorOut,
    MonitorUpdate,
    PasswordResetRequest,
    PollIntervalUpdate,
    RuleCreate,
    RuleOut,
    RuleUpdate,
    SystemInfo,
    UserAuthRecord,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
settings = Settings.from_env()
settings.ensure_directories()
database = Database(settings.sqlite_path)
repository = MonitorRepository(database)
quote_provider = build_quote_provider(settings.quote_provider)
monitor_service = MonitorService(repository, quote_provider, settings.poll_interval_seconds)
auth_manager = LoginAuthManager()
registration_code_hash = ""


def load_saved_poll_interval() -> None:
    saved_value = repository.get_app_setting("poll_interval_seconds")
    if not saved_value:
        return
    try:
        settings.poll_interval_seconds = max(int(saved_value), 5)
    except ValueError:
        return
    monitor_service.poll_interval_seconds = settings.poll_interval_seconds


def ensure_registration_code_hash() -> None:
    global registration_code_hash

    saved_hash = repository.get_app_setting("registration_code_hash")
    if saved_hash:
        registration_code_hash = saved_hash
        return

    registration_code_hash = auth_manager.create_secret_hash(settings.registration_code)
    repository.set_app_setting("registration_code_hash", registration_code_hash)


def bootstrap_default_user() -> None:
    if repository.count_users() > 0:
        return

    if settings.login_username and settings.login_password:
        password_hash = auth_manager.create_secret_hash(settings.login_password)
        try:
            repository.create_user(settings.login_username, password_hash)
        except ValueError:
            return


def registered_user_count() -> int:
    return repository.count_users()


def verify_registration_code(registration_code: str) -> bool:
    return bool(registration_code_hash) and auth_manager.verify_secret_hash(registration_code, registration_code_hash)


def auth_token_from_request(request: Request) -> str:
    return request.headers.get("X-Auth-Token", "").strip()


def require_current_user(request: Request) -> UserAuthRecord:
    token = auth_token_from_request(request)
    session = auth_manager.get_session(token)
    if session is None:
        raise HTTPException(status_code=401, detail="请先登录")

    user = repository.get_user_by_username(session.username)
    if user is None or not user.enabled:
        auth_manager.logout(token)
        raise HTTPException(status_code=401, detail="请先登录")
    return user


def create_system_event(event_name: str, payload: dict) -> None:
    repository.create_system_event(event_name, payload)


@asynccontextmanager
async def lifespan(app: FastAPI):
    database.init_db()
    ensure_registration_code_hash()
    bootstrap_default_user()
    load_saved_poll_interval()
    yield


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.mount("/assets", StaticFiles(directory=str(STATIC_DIR)), name="assets")


def metadata_payload() -> MetadataOut:
    instrument_types = [
        {"value": InstrumentType.STOCK.value, "label": "股票"},
        {"value": InstrumentType.INDEX.value, "label": "指数"},
    ]
    fields = [{"value": value, "label": label} for value, label in FIELD_LABELS.items()]
    operators = [{"value": value, "label": label} for value, label in OPERATOR_LABELS.items()]
    return MetadataOut(instrument_types=instrument_types, fields=fields, operators=operators)


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return (STATIC_DIR / "index.html").read_text(encoding="utf-8")


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/system", response_model=SystemInfo)
def get_system_info() -> SystemInfo:
    return SystemInfo(
        app_name=settings.app_name,
        poll_interval_seconds=settings.poll_interval_seconds,
        quote_provider="eastmoney + sina",
    )


@app.get("/api/auth/status", response_model=AuthStatus)
def get_auth_status(request: Request) -> AuthStatus:
    session = auth_manager.get_session(auth_token_from_request(request))
    user_count = registered_user_count()
    return AuthStatus(
        authenticated=session is not None,
        username=session.username if session else "",
        account_initialized=user_count > 0,
        registered_user_count=user_count,
    )


@app.post("/api/auth/login", response_model=AuthLoginResponse)
def login(payload: AuthLoginRequest) -> AuthLoginResponse:
    if registered_user_count() == 0:
        raise HTTPException(status_code=400, detail="还没有注册账号，请先使用注册码注册")

    user = repository.get_user_by_username(payload.username)
    if user is None or not user.enabled:
        raise HTTPException(status_code=401, detail="用户名或密码错误")
    if not auth_manager.verify_secret_hash(payload.password, user.password_hash):
        raise HTTPException(status_code=401, detail="用户名或密码错误")

    session = auth_manager.create_session(user.username)
    return AuthLoginResponse(
        authenticated=True,
        username=session.username,
        token=session.token,
        expires_at=session.expires_at,
    )


@app.post("/api/auth/register", response_model=AuthLoginResponse)
def register(payload: AuthRegisterRequest) -> AuthLoginResponse:
    if not verify_registration_code(payload.registration_code):
        raise HTTPException(status_code=401, detail="注册码错误")

    password_hash = auth_manager.create_secret_hash(payload.password)
    try:
        user = repository.create_user(payload.username, password_hash)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    create_system_event("config_changed", {"kind": "user_registered", "username": user.username})
    session = auth_manager.create_session(user.username)
    return AuthLoginResponse(
        authenticated=True,
        username=session.username,
        token=session.token,
        expires_at=session.expires_at,
    )


@app.post("/api/auth/reset-password", response_model=AuthLoginResponse)
def reset_password(payload: PasswordResetRequest) -> AuthLoginResponse:
    if registered_user_count() == 0:
        raise HTTPException(status_code=400, detail="还没有注册账号，请先注册")
    if not verify_registration_code(payload.registration_code):
        raise HTTPException(status_code=401, detail="注册码错误")

    user = repository.get_user_by_username(payload.username)
    if user is None:
        raise HTTPException(status_code=400, detail="用户不存在")

    password_hash = auth_manager.create_secret_hash(payload.new_password)
    updated_user = repository.update_user_password(payload.username, password_hash)
    auth_manager.revoke_user_sessions(updated_user.username)
    create_system_event("config_changed", {"kind": "user_password_reset", "username": updated_user.username})

    session = auth_manager.create_session(updated_user.username)
    return AuthLoginResponse(
        authenticated=True,
        username=session.username,
        token=session.token,
        expires_at=session.expires_at,
    )


@app.post("/api/auth/logout", status_code=204)
def logout(request: Request) -> Response:
    auth_manager.logout(auth_token_from_request(request))
    return Response(status_code=204)


@app.get("/api/events")
async def stream_events(request: Request) -> StreamingResponse:
    try:
        last_event_id = int(request.headers.get("Last-Event-ID", "0") or "0")
    except ValueError:
        last_event_id = 0

    async def event_stream():
        nonlocal last_event_id
        last_ping = asyncio.get_running_loop().time()
        yield "event: ready\ndata: {}\n\n"

        while True:
            if await request.is_disconnected():
                break

            events = await asyncio.to_thread(repository.list_system_events_after, last_event_id, 50)
            if events:
                for event in events:
                    last_event_id = event.id
                    yield (
                        f"id: {event.id}\n"
                        f"event: {event.event_name}\n"
                        f"data: {event.payload}\n\n"
                    )
                last_ping = asyncio.get_running_loop().time()
                continue

            if asyncio.get_running_loop().time() - last_ping >= 20:
                last_ping = asyncio.get_running_loop().time()
                yield ": ping\n\n"
            await asyncio.sleep(1)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/metadata", response_model=MetadataOut)
def get_metadata() -> MetadataOut:
    return metadata_payload()


@app.get("/api/monitors", response_model=list[MonitorOut])
async def list_monitors(request: Request) -> list[MonitorOut]:
    user = require_current_user(request)
    return await monitor_service.list_monitors_with_quotes(user_id=user.id)


@app.post("/api/quotes/refresh")
async def refresh_quotes(request: Request) -> dict[str, int]:
    user = require_current_user(request)
    return await monitor_service.refresh_quotes_for_all(user_id=user.id)


@app.put("/api/system/poll-interval", response_model=SystemInfo)
def update_poll_interval(payload: PollIntervalUpdate, request: Request) -> SystemInfo:
    require_current_user(request)
    repository.set_app_setting("poll_interval_seconds", str(payload.poll_interval_seconds))
    settings.poll_interval_seconds = payload.poll_interval_seconds
    monitor_service.poll_interval_seconds = payload.poll_interval_seconds
    create_system_event(
        "system_updated",
        {
            "poll_interval_seconds": payload.poll_interval_seconds,
        },
    )
    return get_system_info()


@app.post("/api/monitors", response_model=MonitorOut, status_code=201)
def create_monitor(payload: MonitorCreate, request: Request) -> MonitorOut:
    user = require_current_user(request)
    try:
        monitor = repository.create_monitor(payload, user_id=user.id)
        create_system_event("config_changed", {"kind": "monitor_created"})
        return monitor
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.put("/api/monitors/{monitor_id}", response_model=MonitorOut)
def update_monitor(monitor_id: int, payload: MonitorUpdate, request: Request) -> MonitorOut:
    user = require_current_user(request)
    try:
        monitor = repository.update_monitor(monitor_id, payload, user_id=user.id)
        create_system_event("config_changed", {"kind": "monitor_updated", "monitor_id": monitor_id})
        return monitor
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/monitors/{monitor_id}", status_code=204)
def delete_monitor(monitor_id: int, request: Request) -> Response:
    user = require_current_user(request)
    repository.delete_monitor(monitor_id, user_id=user.id)
    create_system_event("config_changed", {"kind": "monitor_deleted", "monitor_id": monitor_id})
    return Response(status_code=204)


@app.post("/api/monitors/{monitor_id}/rules", response_model=RuleOut, status_code=201)
def create_rule(monitor_id: int, payload: RuleCreate, request: Request) -> RuleOut:
    user = require_current_user(request)
    try:
        rule = repository.create_rule(monitor_id, payload, user_id=user.id)
        create_system_event("config_changed", {"kind": "rule_created", "monitor_id": monitor_id})
        return rule
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.put("/api/rules/{rule_id}", response_model=RuleOut)
def update_rule(rule_id: int, payload: RuleUpdate, request: Request) -> RuleOut:
    user = require_current_user(request)
    try:
        rule = repository.update_rule(rule_id, payload, user_id=user.id)
        create_system_event("config_changed", {"kind": "rule_updated", "rule_id": rule_id})
        return rule
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/rules/{rule_id}", status_code=204)
def delete_rule(rule_id: int, request: Request) -> Response:
    user = require_current_user(request)
    repository.delete_rule(rule_id, user_id=user.id)
    create_system_event("config_changed", {"kind": "rule_deleted", "rule_id": rule_id})
    return Response(status_code=204)


@app.get("/api/alerts", response_model=AlertPageOut)
def list_alerts(request: Request, page: int = 1, page_size: int = 10) -> AlertPageOut:
    user = require_current_user(request)
    safe_page = max(page, 1)
    safe_page_size = min(max(page_size, 1), 50)
    items, total = repository.list_alerts(page=safe_page, page_size=safe_page_size, user_id=user.id)
    total_pages = max((total + safe_page_size - 1) // safe_page_size, 1) if total else 1
    return AlertPageOut(
        items=items,
        page=safe_page,
        page_size=safe_page_size,
        total=total,
        total_pages=total_pages,
    )


@app.delete("/api/alerts", response_model=ClearAlertsResponse)
def clear_alerts(request: Request) -> ClearAlertsResponse:
    user = require_current_user(request)
    cleared = repository.clear_alerts(user_id=user.id)
    create_system_event("alerts_updated", {"kind": "alerts_cleared", "cleared": cleared})
    return ClearAlertsResponse(cleared=cleared)


@app.post("/api/run-once")
async def run_once(request: Request) -> dict[str, int]:
    user = require_current_user(request)
    result = await monitor_service.run_cycle(user_id=user.id)
    create_system_event("system_updated", {"kind": "run_once_completed", **result})
    return result
