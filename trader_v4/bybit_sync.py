# bybit_sync.py — приватный WS-синк Bybit (read-only): auth + подписки wallet/position + авто-reconnect

# 🔸 Импорты
import os
import hmac
import time
import json
import hashlib
import asyncio
import logging
import websockets

# 🔸 Логгер
log = logging.getLogger("BYBIT_SYNC")

# 🔸 Конфиг (mainnet)
API_KEY = os.getenv("BYBIT_API_KEY", "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")
WS_PRIVATE = os.getenv("BYBIT_WS_PRIVATE", "wss://stream.bybit.com/v5/private")

PING_INTERVAL_SEC = 20.0      # отправляем ping, если нет сообщений дольше этого интервала
RECONNECT_DELAY_SEC = 3.0     # пауза между переподключениями


# 🔸 Основной цикл воркера (держим приватный WS + подписки)
async def run_bybit_private_ws_sync_loop():
    if not API_KEY or not API_SECRET:
        log.info("BYBIT_SYNC: ключи не заданы (BYBIT_API_KEY/SECRET) — пропуск запуска")
        return

    log.info("BYBIT_SYNC: старт приватного WS-синка %s", WS_PRIVATE)

    while True:
        try:
            # подключение к приватному WS
            async with websockets.connect(WS_PRIVATE, ping_interval=None, close_timeout=5) as ws:
                # auth
                expires = int((time.time() + 5) * 1000)
                sign_payload = f"GET/realtime{expires}"
                signature = hmac.new(API_SECRET.encode(), sign_payload.encode(), hashlib.sha256).hexdigest()
                await ws.send(json.dumps({"op": "auth", "args": [API_KEY, expires, signature]}))
                auth_resp = json.loads(await ws.recv())
                log.info("BYBIT_SYNC auth: %s", auth_resp)

                # подписки: wallet + position
                await ws.send(json.dumps({"op": "subscribe", "args": ["wallet", "position"]}))
                sub_resp = json.loads(await ws.recv())
                log.info("BYBIT_SYNC subscribe ack: %s", sub_resp)

                # цикл чтения с таймаутом (для пингов)
                last_io = time.monotonic()
                while True:
                    try:
                        # ждём сообщение с таймаутом
                        msg_raw = await asyncio.wait_for(ws.recv(), timeout=PING_INTERVAL_SEC)
                        last_io = time.monotonic()
                        _handle_ws_message(msg_raw)
                    except asyncio.TimeoutError:
                        # тишина — шлём ping
                        await ws.send(json.dumps({"op": "ping"}))
                        log.info("BYBIT_SYNC → ping")
                        # ждём ответ (не блокируем чтение надолго)
                        try:
                            pong_raw = await asyncio.wait_for(ws.recv(), timeout=5)
                            _handle_ws_message(pong_raw)
                            last_io = time.monotonic()
                        except asyncio.TimeoutError:
                            log.info("BYBIT_SYNC: нет pong — переподключение")
                            raise ConnectionError("pong timeout")

        except Exception:
            log.exception("BYBIT_SYNC: сбой канала, переподключение через %.1fs", RECONNECT_DELAY_SEC)
            await asyncio.sleep(RECONNECT_DELAY_SEC)


# 🔸 Обработка входящих WS-сообщений (логирование)
def _handle_ws_message(msg_raw: str):
    try:
        msg = json.loads(msg_raw)
    except Exception:
        log.info("BYBIT_SYNC recv (raw): %s", msg_raw)
        return

    # события уровня op (pong/subscribe/auth/…)
    if "op" in msg and "topic" not in msg:
        log.info("BYBIT_SYNC recv op: %s", msg)
        return

    # события по топикам (wallet / position / …)
    topic = msg.get("topic")
    data = msg.get("data")
    ts = msg.get("ts")
    if topic == "wallet":
        # формат: список монет/изменений — выведем кратко размер и первую монету
        coins = data if isinstance(data, list) else []
        head = coins[0] if coins else {}
        log.info("BYBIT_SYNC wallet: items=%d head=%s ts=%s", len(coins), head, ts)
    elif topic == "position":
        # формат: список позиций — выведем размер и первую
        items = data if isinstance(data, list) else []
        head = items[0] if items else {}
        log.info("BYBIT_SYNC position: items=%d head=%s ts=%s", len(items), head, ts)
    else:
        # прочие или служебные темы
        log.info("BYBIT_SYNC recv topic=%s: %s", topic, msg)