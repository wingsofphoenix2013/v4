# bybit_connect_smoke.py — REST+WS smoke-тест подключения к Bybit mainnet (без ордеров)

# 🔸 Импорты
import os
import hmac
import time
import json
import hashlib
import asyncio
import logging
import httpx
import websockets

# 🔸 Логгер
log = logging.getLogger("BYBIT_SMOKE")

# 🔸 Конфиг из ENV (mainnet)
API_KEY = os.getenv("BYBIT_API_KEY", "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com")
RECV_WINDOW = os.getenv("BYBIT_RECV_WINDOW", "5000")   # мс
ACCOUNT_TYPE = os.getenv("BYBIT_ACCOUNT_TYPE", "UNIFIED")  # UNIFIED | CONTRACT | SPOT
WS_PRIVATE = os.getenv("BYBIT_WS_PRIVATE", "wss://stream.bybit.com/v5/private")

# 🔸 Подпись для REST v5
def _rest_sign(timestamp_ms: int, query_or_body: str) -> str:
    # timestamp + api_key + recv_window + (queryString|jsonBodyString)
    payload = f"{timestamp_ms}{API_KEY}{RECV_WINDOW}{query_or_body}"
    return hmac.new(API_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()

# 🔸 REST: время сервера
async def _get_server_time() -> dict:
    url = f"{BASE_URL}/v5/market/time"
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(url)
        r.raise_for_status()
        return r.json()

# 🔸 REST: приватный вызов — баланс кошелька
async def _get_wallet_balance(account_type: str) -> dict:
    query = f"accountType={account_type}"
    url = f"{BASE_URL}/v5/account/wallet-balance?{query}"
    ts = int(time.time() * 1000)
    sign = _rest_sign(ts, query)
    headers = {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-TIMESTAMP": str(ts),
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
        "X-BAPI-SIGN": sign,
    }
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(url, headers=headers)
        r.raise_for_status()
        return r.json()

# 🔸 WS v5 private: аутентификация и чтение одного сообщения
async def _ws_private_auth_and_read_once() -> None:
    # auth формат: {"op":"auth","args":[api_key, expires(ms), signature]}
    expires = int((time.time() + 5) * 1000)
    sign_payload = f"GET/realtime{expires}"
    signature = hmac.new(API_SECRET.encode(), sign_payload.encode(), hashlib.sha256).hexdigest()

    async with websockets.connect(WS_PRIVATE, ping_interval=None, close_timeout=5) as ws:
        await ws.send(json.dumps({"op": "auth", "args": [API_KEY, expires, signature]}))
        auth_resp = json.loads(await ws.recv())
        log.info("WS auth resp: %s", auth_resp)

        # простой ping/pong
        await ws.send(json.dumps({"op": "ping"}))
        msg = json.loads(await ws.recv())
        log.info("WS recv: %s", msg)

# 🔸 Публичная точка входа: одноразовый smoke-пробег
async def run_bybit_connectivity_probe():
    if not API_KEY or not API_SECRET:
        log.warning("BYBIT_SMOKE: BYBIT_API_KEY/BYBIT_API_SECRET не заданы — пропуск smoke")
        return

    try:
        t = await _get_server_time()
        log.info("REST /v5/market/time OK: %s", t.get("result"))
    except Exception:
        log.exception("REST time FAILED")

    try:
        bal = await _get_wallet_balance(ACCOUNT_TYPE)
        log.info("REST wallet-balance (%s): retCode=%s", ACCOUNT_TYPE, bal.get("retCode"))
        log.debug("REST wallet-balance result: %s", bal.get("result"))
    except Exception:
        log.exception("REST wallet-balance FAILED")

    try:
        await _ws_private_auth_and_read_once()
        log.info("WS private auth OK")
    except Exception:
        log.exception("WS private auth FAILED")