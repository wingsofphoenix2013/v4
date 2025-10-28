# bybit_sync.py — приватный WS-синк Bybit (read-only) с авто-reconnect + REST-ресинк; публикация order/execution в Redis Streams

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

from trader_infra import infra

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

# 🔸 Целевые Redis Streams для нормализованных событий Bybit (приватный канал)
ORDER_STREAM = "bybit_order_stream"
EXECUTION_STREAM = "bybit_execution_stream"
POSITION_STREAM = "bybit_position_stream"  # опционально: публикуем сводки позиций


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
                auth_resp_raw = await ws.recv()
                await _handle_ws_message(auth_resp_raw)

                # подписки: wallet + position + order + execution
                await ws.send(json.dumps({"op": "subscribe", "args": ["wallet", "position", "order", "execution"]}))
                sub_resp_raw = await ws.recv()
                await _handle_ws_message(sub_resp_raw)

                # цикл чтения с таймаутом (для пингов)
                while True:
                    try:
                        msg_raw = await asyncio.wait_for(ws.recv(), timeout=PING_INTERVAL_SEC)
                        await _handle_ws_message(msg_raw)
                    except asyncio.TimeoutError:
                        await ws.send(json.dumps({"op": "ping"}))
                        # пинг-понги уводим в DEBUG, чтобы не мусорили INFO
                        log.debug("BYBIT_SYNC → ping")
                        try:
                            pong_raw = await asyncio.wait_for(ws.recv(), timeout=5)
                            await _handle_ws_message(pong_raw)
                        except asyncio.TimeoutError:
                            log.info("BYBIT_SYNC: нет pong — переподключение")
                            raise ConnectionError("pong timeout")

        except Exception:
            log.exception("BYBIT_SYNC: сбой канала, переподключение через %.1fs", RECONNECT_DELAY_SEC)
            await asyncio.sleep(RECONNECT_DELAY_SEC)


# 🔸 Обработка входящих WS-сообщений (логирование + публикация нормализованных событий в Redis Streams)
async def _handle_ws_message(msg_raw: str):
    try:
        msg = json.loads(msg_raw)
    except Exception:
        log.info("BYBIT_SYNC recv (raw): %s", msg_raw)
        return

    # служебные op-сообщения (auth/subscribe/ping/pong/и т. п.)
    if "op" in msg and "topic" not in msg:
        op = msg.get("op")
        # пинг/понг — это «шум»: логируем на DEBUG
        if op in ("ping", "pong"):
            args = msg.get("args")
            log.debug("BYBIT_SYNC recv %s: %s", op, args)
            return
        # важные служебные подтверждения — на INFO
        if op in ("auth", "subscribe"):
            log.info("BYBIT_SYNC recv %s: %s", op, msg)
            return
        # прочие служебные — оставим на INFO для видимости
        log.info("BYBIT_SYNC recv op: %s", msg)
        return

    # топиковые события
    topic = msg.get("topic")
    data = msg.get("data")
    ts = msg.get("ts")

    # нормализуем data в список
    items = data if isinstance(data, list) else (data if data is not None else [])
    items = items if isinstance(items, list) else [items]

    # wallet — просто сводка в логах (как было)
    if topic == "wallet":
        head = items[0] if items else {}
        log.info("BYBIT_SYNC wallet: items=%d head=%s ts=%s", len(items), head, ts)
        return

    # position — сводка + публикация в POSITION_STREAM (по одному payload на запись)
    if topic == "position":
        published = 0
        for it in items:
            payload = {
                "event": "position",
                "category": it.get("category"),
                "symbol": it.get("symbol"),
                "side": it.get("side"),                # Buy | Sell
                "size": it.get("size"),
                "avgPrice": it.get("avgPrice"),
                "positionValue": it.get("positionValue"),
                "positionStatus": it.get("positionStatus"),
                "positionIdx": it.get("positionIdx"),
                "ts": ts,
            }
            try:
                await infra.redis_client.xadd(POSITION_STREAM, {"data": json.dumps(payload)})
                published += 1
            except Exception:
                log.exception("BYBIT_SYNC: publish position failed: %s", payload)
        head = items[0] if items else {}
        log.info("BYBIT_SYNC position: items=%d pub=%d head=%s ts=%s", len(items), published, head, ts)
        return

    # order — публикация нормализованных статусов ордеров в ORDER_STREAM
    if topic == "order":
        published = 0
        for it in items:
            payload = {
                "event": "order",
                "category": it.get("category"),
                "symbol": it.get("symbol"),
                "orderId": it.get("orderId"),
                "orderLinkId": it.get("orderLinkId"),
                "side": it.get("side"),                    # Buy|Sell
                "orderType": it.get("orderType"),          # Market|Limit|...
                "timeInForce": it.get("timeInForce"),
                "orderStatus": it.get("orderStatus"),      # New|PartiallyFilled|Filled|Cancelled|Rejected|...
                "reduceOnly": it.get("reduceOnly"),
                "qty": it.get("qty"),
                "cumExecQty": it.get("cumExecQty"),
                "leavesQty": it.get("leavesQty"),
                "avgPrice": it.get("avgPrice"),
                "price": it.get("price"),
                "ts": ts,
            }
            try:
                await infra.redis_client.xadd(ORDER_STREAM, {"data": json.dumps(payload)})
                published += 1
            except Exception:
                log.exception("BYBIT_SYNC: publish order failed: %s", payload)
        head = items[0] if items else {}
        log.info("BYBIT_SYNC order: items=%d pub=%d head=%s ts=%s", len(items), published, head, ts)
        return

    # execution — публикация трейдов/исполнений в EXECUTION_STREAM
    if topic == "execution":
        published = 0
        for it in items:
            payload = {
                "event": "execution",
                "category": it.get("category"),
                "symbol": it.get("symbol"),
                "orderId": it.get("orderId"),
                "orderLinkId": it.get("orderLinkId"),
                "execId": it.get("execId"),
                "execType": it.get("execType"),           # Trade|AdlTrade|Funding|...
                "isMaker": it.get("isMaker"),
                "execQty": it.get("execQty"),
                "execPrice": it.get("execPrice"),
                "execValue": it.get("execValue"),
                "tradeTime": it.get("tradeTime"),         # ms
                "ts": ts,
            }
            try:
                await infra.redis_client.xadd(EXECUTION_STREAM, {"data": json.dumps(payload)})
                published += 1
            except Exception:
                log.exception("BYBIT_SYNC: publish execution failed: %s", payload)
        head = items[0] if items else {}
        log.info("BYBIT_SYNC execution: items=%d pub=%d head=%s ts=%s", len(items), published, head, ts)
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
    log.info(
        "BYBIT_RESYNC balance: totalEquity=%s totalWallet=%s perpUPL=%s",
        acc0.get("totalEquity"),
        acc0.get("totalWalletBalance"),
        acc0.get("totalPerpUPL"),
    )
    coins = acc0.get("coin") or []
    head = coins[0] if coins else {}
    log.info("BYBIT_RESYNC coins: items=%d head=%s", len(coins), head)

def _log_positions_summary(pos: dict):
    lst = (pos.get("result") or {}).get("list") or []
    head = lst[0] if lst else {}
    log.info("BYBIT_RESYNC positions: items=%d head=%s", len(lst), head)