# bybit_http_client.py — асинхронный REST-клиент Bybit v5 (подпись, ретраи, базовые методы ордеров/позиций)

# 🔸 Импорты
import os
import hmac
import json
import time
import math
import random
import hashlib
import logging
from typing import Any, Dict, Optional

import httpx
from urllib.parse import urlencode

# 🔸 Логгер
log = logging.getLogger("BYBIT_HTTP")

# 🔸 Конфиг (ENV mainnet)
API_KEY = os.getenv("BYBIT_API_KEY", "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com")
RECV_WINDOW = os.getenv("BYBIT_RECV_WINDOW", "5000")  # строкой, как требует заголовок
ACCOUNT_TYPE = os.getenv("BYBIT_ACCOUNT_TYPE", "UNIFIED")  # UNIFIED | CONTRACT | SPOT

# 🔸 Ретраи/тайминги
DEFAULT_TIMEOUT_SEC = 10
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 0.5  # секунд


# 🔸 Исключения клиента
class BybitAPIError(Exception):
    def __init__(self, message: str, *, status_code: int | None = None, ret_code: int | None = None, payload: dict | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.ret_code = ret_code
        self.payload = payload or {}

class BybitRateLimitError(BybitAPIError):
    pass


# 🔸 Клиент Bybit v5 (HTTP + подпись)
class BybitHttpClient:
    def __init__(self, *, base_url: str | None = None, api_key: str | None = None, api_secret: str | None = None, recv_window: str | None = None):
        self.base_url = (base_url or BASE_URL).rstrip("/")
        self.api_key = api_key or API_KEY
        self.api_secret = api_secret or API_SECRET
        self.recv_window = recv_window or RECV_WINDOW
        self._client: Optional[httpx.AsyncClient] = None

    # 🔸 Инициализация HTTP-клиента
    async def _ensure_client(self):
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=DEFAULT_TIMEOUT_SEC)

    # 🔸 Закрытие клиента
    async def aclose(self):
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # 🔸 Подпись запроса v5
    def _sign(self, timestamp_ms: int, query_or_body: str) -> str:
        # правило: timestamp + api_key + recvWindow + (queryString|jsonBodyString)
        payload = f"{timestamp_ms}{self.api_key}{self.recv_window}{query_or_body}"
        return hmac.new(self.api_secret.encode(), payload.encode(), hashlib.sha256).hexdigest()

    # 🔸 Заголовки с подписью
    def _auth_headers(self, timestamp_ms: int, signature: str) -> Dict[str, str]:
        return {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": str(timestamp_ms),
            "X-BAPI-RECV-WINDOW": self.recv_window,
            "X-BAPI-SIGN": signature,
            "Content-Type": "application/json",
        }

    # 🔸 Помощники: GET/POST с ретраями
    async def _request_get(self, path: str, params: Dict[str, Any] | None = None, *, auth: bool = False) -> dict:
        await self._ensure_client()
        url = f"{self.base_url}{path}"
        params = params or {}
        # для подписи нужен ровно тот queryString, что уйдёт в URL
        query_string = urlencode(params, doseq=True, safe=":,")
        tries = 0

        while True:
            ts = int(time.time() * 1000)
            headers = {}
            if auth:
                signature = self._sign(ts, query_string)
                headers = self._auth_headers(ts, signature)

            try:
                r = await self._client.get(url, params=params, headers=headers)
                payload = await self._parse_response(r, path)
                return payload
            except BybitRateLimitError as e:
                tries += 1
                if tries > MAX_RETRIES:
                    log.info("RATE LIMIT exhausted: %s", e)
                    raise
                backoff = self._backoff_time(tries)
                log.info("429/Rate limit at GET %s — retry in %.2fs", path, backoff)
                await self._sleep(backoff)
            except BybitAPIError as e:
                # ретраи только для 5xx и некоторых retCode по сети
                if not self._should_retry(e):
                    raise
                tries += 1
                if tries > MAX_RETRIES:
                    log.info("Retries exhausted at GET %s: %s", path, e)
                    raise
                backoff = self._backoff_time(tries)
                log.info("Retry GET %s in %.2fs (reason=%s)", path, backoff, e)
                await self._sleep(backoff)

    async def _request_post(self, path: str, body: Dict[str, Any], *, auth: bool = True) -> dict:
        await self._ensure_client()
        url = f"{self.base_url}{path}"
        body_clean = _drop_none(body)
        body_str = json.dumps(body_clean, ensure_ascii=False, separators=(",", ":"))
        tries = 0

        while True:
            ts = int(time.time() * 1000)
            signature = self._sign(ts, body_str) if auth else ""
            headers = self._auth_headers(ts, signature) if auth else {"Content-Type": "application/json"}

            try:
                r = await self._client.post(url, content=body_str, headers=headers)
                payload = await self._parse_response(r, path)
                return payload
            except BybitRateLimitError as e:
                tries += 1
                if tries > MAX_RETRIES:
                    log.info("RATE LIMIT exhausted: %s", e)
                    raise
                backoff = self._backoff_time(tries)
                log.info("429/Rate limit at POST %s — retry in %.2fs", path, backoff)
                await self._sleep(backoff)
            except BybitAPIError as e:
                if not self._should_retry(e):
                    raise
                tries += 1
                if tries > MAX_RETRIES:
                    log.info("Retries exhausted at POST %s: %s", path, e)
                    raise
                backoff = self._backoff_time(tries)
                log.info("Retry POST %s in %.2fs (reason=%s)", path, backoff, e)
                await self._sleep(backoff)

    # 🔸 Парсер ответов + базовая валидация retCode
    async def _parse_response(self, r: httpx.Response, path: str) -> dict:
        status = r.status_code
        text = r.text
        try:
            data = r.json()
        except Exception:
            data = {"raw": text}

        # 429 — отдельный класс ошибок
        if status == 429:
            raise BybitRateLimitError(f"429 Too Many Requests at {path}", status_code=status, payload=data)

        # HTTP-ошибки
        if status >= 500:
            raise BybitAPIError(f"5xx at {path}", status_code=status, payload=data)
        if status >= 400:
            raise BybitAPIError(f"{status} at {path}: {data}", status_code=status, payload=data)

        # Bybit-ошибки по retCode (200 OK, но retCode != 0)
        ret_code = data.get("retCode")
        if ret_code is None:
            # не v5-формат — возвращаем как есть
            return data
        if ret_code != 0:
            msg = data.get("retMsg", "Unknown Bybit error")
            raise BybitAPIError(f"Bybit retCode={ret_code} at {path}: {msg}", status_code=status, ret_code=ret_code, payload=data)

        return data

    # 🔸 Политика ретраев: 5xx / сетевые / некоторые retCode
    def _should_retry(self, e: BybitAPIError) -> bool:
        if isinstance(e, BybitRateLimitError):
            return True
        if e.status_code and e.status_code >= 500:
            return True
        # retCode-логика: можно расширить при необходимости
        if e.ret_code in {10002, 10006, 110001}:  # generic temp errors / not found (race) / etc.
            return True
        return False

    # 🔸 Бэк-офф с лёгким джиттером
    def _backoff_time(self, attempt: int) -> float:
        base = RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
        base = min(base, 5.0)
        jitter = random.uniform(0.0, 0.2)
        return base + jitter

    async def _sleep(self, sec: float):
        # вынесено для тестируемости/patch
        import asyncio
        await asyncio.sleep(sec)

    # ─────────────────────────────────────────────────────────────────────────────
    # 🔸 Публичные методы (минимально необходимые на пилот)
    # ─────────────────────────────────────────────────────────────────────────────

    # 🔸 Баланс кошелька (проверка авторизации/связности)
    async def get_wallet_balance(self, account_type: str | None = None) -> dict:
        params = {"accountType": account_type or ACCOUNT_TYPE}
        log.info("HTTP get_wallet_balance(%s)", params["accountType"])
        return await self._request_get("/v5/account/wallet-balance", params, auth=True)

    # 🔸 Список позиций (linear)
    async def get_positions_list(self, *, category: str = "linear", symbol: str | None = None) -> dict:
        params: Dict[str, Any] = {"category": category}
        if symbol:
            params["symbol"] = symbol
        log.info("HTTP get_positions_list(%s)", params)
        return await self._request_get("/v5/position/list", params, auth=True)

    # 🔸 Список ордеров (фильтры опциональны)
    async def get_orders_list(self, *, category: str = "linear", symbol: str | None = None, open_only: bool | None = None, cursor: str | None = None, limit: int | None = None) -> dict:
        params: Dict[str, Any] = {"category": category}
        if symbol:
            params["symbol"] = symbol
        if open_only is not None:
            params["openOnly"] = 1 if open_only else 0
        if cursor:
            params["cursor"] = cursor
        if limit:
            params["limit"] = limit
        log.info("HTTP get_orders_list(%s)", params)
        return await self._request_get("/v5/order/list", params, auth=True)

    # 🔸 Список исполнений (fills)
    async def get_executions_list(self, *, category: str = "linear", symbol: str | None = None, start: int | None = None, end: int | None = None, cursor: str | None = None, limit: int | None = None) -> dict:
        params: Dict[str, Any] = {"category": category}
        if symbol:
            params["symbol"] = symbol
        if start:
            params["startTime"] = start
        if end:
            params["endTime"] = end
        if cursor:
            params["cursor"] = cursor
        if limit:
            params["limit"] = limit
        log.info("HTTP get_executions_list(%s)", params)
        return await self._request_get("/v5/execution/list", params, auth=True)

    # 🔸 Создание ордера (entry/TP/SL-stop как отдельный ордер)
    async def create_order(
        self,
        *,
        category: str = "linear",
        symbol: str,
        side: str,                     # 'Buy' | 'Sell'
        order_type: str,               # 'Market' | 'Limit'
        qty: str,                      # строкой по правилам Bybit
        price: str | None = None,      # для Limit
        time_in_force: str | None = None,   # 'GTC' | 'IOC' | 'FOK'
        reduce_only: bool | None = None,
        order_link_id: str | None = None,
        position_idx: int | None = None,    # для hedge-режима (у нас one-way -> не передаём)
        take_profit: str | None = None,     # опц.: если решим сразу ставить
        stop_loss: str | None = None,       # опц.
        tp_trigger_by: str | None = None,   # 'LastPrice'|'IndexPrice'|'MarkPrice'
        sl_trigger_by: str | None = None,   # ...
        trigger_direction: int | None = None,  # для условных
        trigger_price: str | None = None,
        **extra: Any,
    ) -> dict:
        body: Dict[str, Any] = {
            "category": category,
            "symbol": symbol,
            "side": side,
            "orderType": order_type,
            "qty": qty,
            "price": price,
            "timeInForce": time_in_force,
            "reduceOnly": reduce_only,
            "orderLinkId": order_link_id,
            "positionIdx": position_idx,
            "takeProfit": take_profit,
            "stopLoss": stop_loss,
            "tpTriggerBy": tp_trigger_by,
            "slTriggerBy": sl_trigger_by,
            "triggerDirection": trigger_direction,
            "triggerPrice": trigger_price,
            **extra,
        }
        log.info("HTTP create_order(%s %s %s) linkId=%s", symbol, side, order_type, order_link_id)
        return await self._request_post("/v5/order/create", body)

    # 🔸 Отмена ордера (по orderId или orderLinkId)
    async def cancel_order(self, *, category: str = "linear", symbol: str, order_id: str | None = None, order_link_id: str | None = None) -> dict:
        body: Dict[str, Any] = {
            "category": category,
            "symbol": symbol,
            "orderId": order_id,
            "orderLinkId": order_link_id,
        }
        log.info("HTTP cancel_order(%s) by %s", symbol, order_id or order_link_id)
        return await self._request_post("/v5/order/cancel", body)

    # 🔸 Изменение (amend) ордера (цена/кол-во/TP/SL) по orderId или orderLinkId
    async def amend_order(
        self,
        *,
        category: str = "linear",
        symbol: str,
        order_id: str | None = None,
        order_link_id: str | None = None,
        new_qty: str | None = None,
        new_price: str | None = None,
        **extra: Any,
    ) -> dict:
        body: Dict[str, Any] = {
            "category": category,
            "symbol": symbol,
            "orderId": order_id,
            "orderLinkId": order_link_id,
            "qty": new_qty,
            "price": new_price,
            **extra,
        }
        log.info("HTTP amend_order(%s) %s qty=%s price=%s", symbol, order_id or order_link_id, new_qty, new_price)
        return await self._request_post("/v5/order/amend", body)

    # 🔸 Установка плеча (buy/sell) — если потребуется явно сетать
    async def set_leverage(self, *, category: str = "linear", symbol: str, buy_leverage: int | str, sell_leverage: int | str) -> dict:
        body: Dict[str, Any] = {
            "category": category,
            "symbol": symbol,
            "buyLeverage": str(buy_leverage),
            "sellLeverage": str(sell_leverage),
        }
        log.info("HTTP set_leverage(%s) buy=%s sell=%s", symbol, buy_leverage, sell_leverage)
        return await self._request_post("/v5/position/set-leverage", body)

    # 🔸 Trading Stop (position-level TP/SL/TS) — базовый SL как trading-stop
    async def set_trading_stop(
        self,
        *,
        category: str = "linear",
        symbol: str,
        stop_loss: str | None = None,
        take_profit: str | None = None,
        trailing_stop: str | None = None,
        sl_trigger_by: str | None = None,
        tp_trigger_by: str | None = None,
        position_idx: int | None = None,  # one-way не требует
        **extra: Any,
    ) -> dict:
        body: Dict[str, Any] = {
            "category": category,
            "symbol": symbol,
            "stopLoss": stop_loss,
            "takeProfit": take_profit,
            "trailingStop": trailing_stop,
            "slTriggerBy": sl_trigger_by,
            "tpTriggerBy": tp_trigger_by,
            "positionIdx": position_idx,
            **extra,
        }
        log.info("HTTP set_trading_stop(%s) SL=%s TP=%s TS=%s", symbol, stop_loss, take_profit, trailing_stop)
        return await self._request_post("/v5/position/set-trading-stop", body)


# 🔸 Утилиты (локальные)

def _drop_none(d: Dict[str, Any]) -> Dict[str, Any]:
    # убираем None, но сохраняем ложные значения типа 0/False
    return {k: v for k, v in d.items() if v is not None}