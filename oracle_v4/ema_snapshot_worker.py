# ema_snapshot_worker.py

import asyncio
import logging
from datetime import datetime

import infra

log = logging.getLogger("EMA_SNAPSHOT_WORKER")

VALID_EMAS = {"ema9", "ema21", "ema50", "ema100", "ema200"}
VALID_INTERVALS = {"m5", "m15", "h1"}

# Ожидания по (symbol, interval, open_time)
pending_snapshots = {}

# 🔸 Обработка одного сообщения из Redis Stream
async def handle_ema_snapshot_message(message: dict):
    symbol = message.get("symbol")
    interval = message.get("timeframe")
    indicator = message.get("indicator")
    open_time = message.get("open_time")
    status = message.get("status")

    if not all([symbol, interval, indicator, open_time, status]):
        return

    if indicator not in VALID_EMAS:
        return
    if interval not in VALID_INTERVALS:
        return
    if status != "ready":
        return

    key = (symbol, interval, open_time)

    # Обновляем set полученных индикаторов
    if key not in pending_snapshots:
        pending_snapshots[key] = set()
    pending_snapshots[key].add(indicator)

    # Если собраны все 5
    if pending_snapshots[key] == VALID_EMAS:
        log.info(f"📸 Снимок EMA ГОТОВ: {symbol} | {interval} | {open_time}")
        del pending_snapshots[key]


# 🔸 Основной воркер
async def run_ema_snapshot_worker():
    redis = infra.redis_client
    stream_name = "indicator_stream"

    try:
        stream_info = await redis.xinfo_stream(stream_name)
        last_id = stream_info["last-generated-id"]
    except Exception as e:
        log.warning(f"⚠️ Не удалось получить last ID из stream: {e}")
        last_id = "$"

    log.info(f"📡 Подписка на Redis Stream: {stream_name} (EMA SNAPSHOT) с last_id = {last_id}")

    while True:
        try:
            response = await redis.xread(
                streams={stream_name: last_id},
                count=50,
                block=1000
            )
            for stream, messages in response:
                for msg_id, msg_data in messages:
                    parsed = {k: v for k, v in msg_data.items()}
                    asyncio.create_task(handle_ema_snapshot_message(parsed))
                    last_id = msg_id
        except Exception:
            log.exception("❌ Ошибка чтения из indicator_stream")
            await asyncio.sleep(1)