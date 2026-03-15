from __future__ import annotations

import asyncio

import httpx


class WeComNotifier:
    def __init__(self, timeout_seconds: float = 10.0) -> None:
        self.timeout_seconds = timeout_seconds
        self._client: httpx.AsyncClient | None = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        async with self._lock:
            if self._client is None:
                self._client = httpx.AsyncClient(timeout=self.timeout_seconds)

    async def stop(self) -> None:
        async with self._lock:
            if self._client is not None:
                await self._client.aclose()
                self._client = None

    def _pick_mentions(
        self,
        mentioned_mobiles: list[str] | None,
        mentioned_user_ids: list[str] | None,
    ) -> tuple[list[str], list[str]]:
        cleaned_user_ids = [item.strip() for item in (mentioned_user_ids or []) if item and item.strip()]
        cleaned_mobiles = [item.strip() for item in (mentioned_mobiles or []) if item and item.strip()]
        return cleaned_mobiles, cleaned_user_ids

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            await self.start()
        assert self._client is not None
        return self._client

    async def send_text(
        self,
        webhook_url: str,
        content: str,
        mentioned_mobiles: list[str] | None = None,
        mentioned_user_ids: list[str] | None = None,
    ) -> None:
        mobile_list, user_id_list = self._pick_mentions(mentioned_mobiles, mentioned_user_ids)
        payload = {
            "msgtype": "text",
            "text": {
                "content": content,
                "mentioned_mobile_list": mobile_list,
                "mentioned_list": user_id_list,
            },
        }

        client = await self._get_client()
        response = await client.post(webhook_url, json=payload)
        response.raise_for_status()
        data = response.json()

        if data.get("errcode", 0) != 0:
            raise RuntimeError(f"企业微信机器人返回错误: {data}")
