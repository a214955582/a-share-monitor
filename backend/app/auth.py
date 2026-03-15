from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
import base64
import binascii
import hashlib
import hmac
import secrets

from .utils import now_local, parse_iso


PBKDF2_ITERATIONS = 390000


@dataclass(slots=True)
class AuthSession:
    token: str
    username: str
    expires_at: str


class LoginAuthManager:
    def __init__(self, session_hours: int = 12) -> None:
        self.session_hours = max(session_hours, 1)
        self._sessions: dict[str, AuthSession] = {}

    def create_secret_hash(self, secret: str) -> str:
        salt = secrets.token_bytes(16)
        digest = hashlib.pbkdf2_hmac(
            "sha256",
            (secret or "").encode("utf-8"),
            salt,
            PBKDF2_ITERATIONS,
        )
        salt_text = base64.b64encode(salt).decode("ascii")
        digest_text = base64.b64encode(digest).decode("ascii")
        return f"pbkdf2_sha256${PBKDF2_ITERATIONS}${salt_text}${digest_text}"

    def verify_secret_hash(self, secret: str, encoded_hash: str) -> bool:
        try:
            algorithm, iterations_text, salt_text, digest_text = encoded_hash.split("$", 3)
        except ValueError:
            return False
        if algorithm != "pbkdf2_sha256":
            return False

        try:
            iterations = int(iterations_text)
            salt = base64.b64decode(salt_text.encode("ascii"))
            expected_digest = base64.b64decode(digest_text.encode("ascii"))
        except (ValueError, binascii.Error):
            return False

        actual_digest = hashlib.pbkdf2_hmac(
            "sha256",
            (secret or "").encode("utf-8"),
            salt,
            iterations,
        )
        return hmac.compare_digest(actual_digest, expected_digest)

    def create_session(self, username: str) -> AuthSession:
        self._cleanup_sessions()
        token = secrets.token_urlsafe(32)
        expires_at = (now_local() + timedelta(hours=self.session_hours)).isoformat(timespec="seconds")
        session = AuthSession(token=token, username=username.strip(), expires_at=expires_at)
        self._sessions[token] = session
        return session

    def is_authenticated(self, token: str | None) -> bool:
        if not token:
            return False
        self._cleanup_sessions()
        return token in self._sessions

    def get_session(self, token: str | None) -> AuthSession | None:
        if not token:
            return None
        self._cleanup_sessions()
        return self._sessions.get(token)

    def revoke_user_sessions(self, username: str) -> None:
        clean_username = (username or "").strip()
        if not clean_username:
            return
        expired_tokens = [token for token, session in self._sessions.items() if session.username == clean_username]
        for token in expired_tokens:
            self._sessions.pop(token, None)

    def logout(self, token: str | None) -> None:
        if not token:
            return
        self._sessions.pop(token, None)

    def _cleanup_sessions(self) -> None:
        expired_tokens: list[str] = []
        current_time = now_local()

        for token, session in self._sessions.items():
            expires_at = parse_iso(session.expires_at)
            if expires_at is None or expires_at <= current_time:
                expired_tokens.append(token)

        for token in expired_tokens:
            self._sessions.pop(token, None)
