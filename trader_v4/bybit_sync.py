# bybit_sync.py — приватный WS-синк Bybit (read-only): auth + wallet/position/order/execution + авто-reconnect
# + периодический REST-ресинк баланса и позиций (linear), без записи в БД

# 🔸 Импорты
import os
import hmac
import time
import json
import hashlib
import asyncio
import logging
import websockets
import httpx

# 🔸 Логгер
log = logging.getLogger("BYBIT_SYNC")

# 🔸 Конфиг (mainnet)
API_KEY = os.getenv("BYBIT_API_KEY", "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")
WS_PRIVATE = os.getenv("BYBIT_WS_PRIVATE", "wss://stream.bybit.com/v5/private")
BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com")
RECV_WINDOW = os.getenv("BYBIT_RECV_WINDOW", "5000")          # мс
ACCOUNT_TYPE = os.getenv("BYBIT_ACCOUNT_TYPE", "UNIFIED")     # UNIFIED | CONTRACT | SPOT
CATEGORY = "linear"                                           # деривативы USDT-perp

PING_INTERVAL_SEC = 20.0      # отправляем ping, если нет сообщений дольше этого интервала
RECONNECT_DELAY_SEC = 3.0     # пауза между переподключениями


# 🔸 Основной цикл воркера приватного WS (держим канал + подписки)
async def run_bybit_private_ws_sync_loop():
    if not API_KEY or not API_SECRET:
        log.info("BYBIT_SYNC: ключи не заданы (BYBIT_API_KEY/SECRET) — пропуск запуска")
        return

    log.info("BYBIT_SYNC: старт приватного WS-синка %s", WS_PRIVATE)

    while True:
        try:
            async with websockets.connect(WS_PRIVATE, ping_interval=None, close_timeout=5) as ws:
                # auth
                expires = int((time.time() + 5) * 1000)
                sign_payload = f"GET/realtime{expires}"
                signature = hmac.new(API_SECRET.encode(), sign_payload.encode(), hashlib.sha256).hexdigest()
                await ws.send(json.dumps({"op": "auth", "args": [API_KEY, expires, signature]}))
                auth_resp = json.loads(await ws.recv())
                log.info("BYBIT_SYNC auth: %s", auth_resp)

                # подписки: wallet + position + order + execution
                await ws.send(json.dumps({"op": "subscribe", "args": ["wallet", "position", "order", "execution"]}))
                sub_resp = json.loads(await ws.recv())
                log.info("BYBIT_SYNC subscribe ack: %s", sub_resp)

                # цикл чтения с таймаутом (для пингов)
                while True:
                    try:
                        msg_raw = await asyncio.wait_for(ws.recv(), timeout=PING_INTERVAL_SEC)
                        _handle_ws_message(msg_raw)
                    except asyncio.TimeoutError:
                        await ws.send(json.dumps({"op": "ping"}))
                        log.info("BYBIT_SYNC → ping")
                        try:
                            pong_raw = await asyncio.wait_for(ws.recv(), timeout=5)
                            _handle_ws_message(pong_raw)
                        except asyncio.TimeoutError:
                            log.info("BYBIT_SYNC: нет pong — переподключение")
                            raise ConnectionError("pong timeout")

        except Exception:
            log.exception("BYBIT_SYNC: сбой канала, переподключение через %.1fs", RECONNECT_DELAY_SEC)
            await asyncio.sleep(RECONNECT_DELAY_SEC)


# 🔸 Обработка входящих WS-сообщений (логирование кратко)
def _handle_ws_message(msg_raw: str):
    try:
        msg = json.loads(msg_raw)
    except Exception:
        log.info("BYBIT_SYNC recv (raw): %s", msg_raw)
        return

    # служебные op-сообщения
    if "op" in msg and "topic" not in msg:
        log.info("BYBIT_SYNC recv op: %s", msg)
        return

    # топиковые события
    topic = msg.get("topic")
    data = msg.get("data")
    ts = msg.get("ts")

    if topic == "wallet":
        items = data if isinstance(data, list) else []
        head = items[0] if items else {}
        log.info("BYBIT_SYNC wallet: items=%d head=%s ts=%s", len(items), head, ts)
        return

    if topic == "position":
        items = data if isinstance(data, list) else []
        head = items[0] if items else {}
        log.info("BYBIT_SYNC position: items=%d head=%s ts=%s", len(items), head, ts)
        return

    if topic == "order":
        items = data if isinstance(data, list) else []
        head = items[0] if items else {}
        log.info("BYBIT_SYNC order: items=%d head=%s ts=%s", len(items), head, ts)
        return

    if topic == "execution":
        items = data if isinstance(data, list) else []
        head = items[0] if items else {}
        log.info("BYBIT_SYNC execution: items=%d head=%s ts=%s", len(items), head, ts)
        return

    # прочее
    log.info("BYBIT_SYNC recv topic=%s: %s", topic, msg)


# 🔸 Периодический REST-ресинк (баланс + позиции linear)
async def run_bybit_rest_resync_job():
    if not API_KEY or not API_SECRET:
        log.info("BYBIT_RESYNC: ключи не заданы (BYBIT_API_KEY/SECRET) — пропуск")
        return

    try:
        bal = await _get_wallet_balance(ACCOUNT_TYPE)
        _log_balance_summary(bal)
    except Exception:
        log.exception("BYBIT_RESYNC: wallet-balance FAILED")

    try:
        pos = await _get_positions_list()
        _log_positions_summary(pos)
    except Exception:
        log.exception("BYBIT_RESYNC: position list FAILED")


# 🔸 REST-помощники

def _rest_sign(timestamp_ms: int, query_or_body: str) -> str:
    # timestamp + api_key + recv_window + (queryString|jsonBodyString)
    payload = f"{timestamp_ms}{API_KEY}{RECV_WINDOW}{query_or_body}"
    return hmac.new(API_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()

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

async def _get_positions_list() -> dict:
    # символ не обязателен — вернёт все позиции по категории
    query = f"category={CATEGORY}"
    url = f"{BASE_URL}/v5/position/list?{query}"
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


# 🔸 Форматтеры сводок в лог

def _log_balance_summary(bal: dict):
    acc = (bal.get("result") or {}).get("list") or []
    if not acc:
        log.info("BYBIT_RESYNC balance: <empty>")
        return
    acc0 = acc[0]
    log.info("BYBIT_RESYNC balance: totalEquity=%s totalWallet=%s perpUPL=%s",
             acc0.get("totalEquity"), acc0.get("totalWalletBalance"), acc0.get("totalPerpUPL"))
    coins = acc0.get("coin") or []
    head = coins[0] if coins else {}
    log.info("BYBIT_RESYNC coins: items=%d head=%s", len(coins), head)

def _log_positions_summary(pos: dict):
    lst = (pos.get("result") or {}).get("list") or []
    head = lst[0] if lst else {}
    log.info("BYBIT_RESYNC positions: items=%d head=%s", len(lst), head)