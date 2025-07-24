# volatility_worker.py

import asyncio
import logging
from datetime import datetime

import infra
import redis.exceptions

log = logging.getLogger("VOLATILITY_WORKER")

# 🔸 Нужные индикаторы из ts_ind
REQUIRED_PARAMS_TS = [
    "atr14",
    "rsi14",
    "bb20_2_0_upper",
    "bb20_2_0_lower",
    "bb20_2_0_center",
]

# 🔸 OHLCV поля из ts (feed_v4)
REQUIRED_PARAMS_OHLCV = [
    "open",
    "high",
    "low",
    "close",
]

# 🔸 Сбор истории по всем параметрам
async def wait_for_all_volatility_data(symbol: str, open_time: str):
    redis = infra.redis_client
    tf = "m5"
    count = 20

    log.info(f"⏳ Сбор данных для расчёта volatility_state: {symbol} @ {open_time}")

    target_dt = datetime.fromisoformat(open_time.replace("Z", ""))
    target_ts = int(target_dt.timestamp() * 1000)
    interval_ms = 300_000
    from_ts = target_ts - interval_ms * (count - 1)

    history = {"ts_ind": {}, "ts": {}}

    # --- ts_ind параметры ---
    for param in REQUIRED_PARAMS_TS:
        key = f"ts_ind:{symbol}:{tf}:{param}"
        try:
            series = await redis.ts().range(key, from_ts, target_ts, count=count)
            values = [(datetime.utcfromtimestamp(ts / 1000), float(v)) for ts, v in series]
            history["ts_ind"][param] = values
        except redis.exceptions.ResponseError as e:
            log.warning(f"⚠️ Ошибка чтения {key}: {e}")
            history["ts_ind"][param] = []

    # --- OHLCV параметры из ts ---
    for field in REQUIRED_PARAMS_OHLCV:
        key = f"ts:{symbol}:{tf}:{field}"
        try:
            series = await redis.ts().range(key, from_ts, target_ts, count=count)
            values = [(datetime.utcfromtimestamp(ts / 1000), float(v)) for ts, v in series]
            history["ts"][field] = values
        except redis.exceptions.ResponseError as e:
            log.warning(f"⚠️ Ошибка чтения {key}: {e}")
            history["ts"][field] = []

    # --- Логирование собранного ---
    log.info(f"📊 История ts_ind (индикаторы):")
    for param, series in history["ts_ind"].items():
        log.info(f"🔍 {param}")
        for ts, val in series:
            log.info(f"    • {ts.isoformat()} → {val}")

    log.info(f"📊 История ts (OHLCV):")
    for field, series in history["ts"].items():
        log.info(f"🔍 {field}")
        for ts, val in series:
            log.info(f"    • {ts.isoformat()} → {val}")

    # расчёты будут позже


# 🔸 Обработка инициирующего сигнала
async def handle_initiator(message: dict):
    symbol = message.get("symbol")
    tf = message.get("timeframe")
    indicator = message.get("indicator")
    open_time = message.get("open_time")
    status = message.get("status")

    if not all([symbol, tf, indicator, open_time, status]):
        log.warning(f"⚠️ Неполное сообщение: {message}")
        return

    if tf != "m5" or indicator != "atr14" or status != "ready":
        return

    log.info(f"🔔 Сигнал для расчёта volatility_state: {symbol} | {indicator} | {tf} | {open_time}")
    await wait_for_all_volatility_data(symbol, open_time)


# 🔸 Основной воркер
async def run_volatility_worker():
    redis = infra.redis_client
    stream_name = "indicator_stream"
    last_id = "$"

    log.info("📡 Подписка на Redis Stream: indicator_stream (volatility)")

    while True:
        try:
            response = await redis.xread(
                streams={stream_name: last_id},
                count=10,
                block=1000
            )
            for stream, messages in response:
                for msg_id, msg_data in messages:
                    parsed = {k: v for k, v in msg_data.items()}
                    asyncio.create_task(handle_initiator(parsed))
        except Exception:
            log.exception("❌ Ошибка чтения из indicator_stream")
            await asyncio.sleep(1)