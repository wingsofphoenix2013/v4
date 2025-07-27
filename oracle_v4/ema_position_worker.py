# ema_position_worker.py

import asyncio
import logging
from collections import Counter

import infra

log = logging.getLogger("EMA_WORKER")

VALID_EMAS = {"ema9", "ema21", "ema50", "ema100", "ema200"}
VALID_INTERVALS = {"m5", "m15", "h1"}

# Глобальный счётчик сигналов по тикерам
signal_counter = Counter()


# 🔸 Обработка одного сообщения из Redis Stream
async def handle_ema_message(message: dict):
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

    signal_counter[symbol] += 1
    log.info(f"📥 Сигнал EMA: {symbol} | {interval} | {indicator} @ {open_time}")

    # Логируем топ тикеров каждые 100 сигналов
    if sum(signal_counter.values()) % 100 == 0:
        top = signal_counter.most_common(10)
        log.info("📊 Топ тикеров по количеству сигналов:")
        for sym, count in top:
            log.info(f"    • {sym}: {count}")


# 🔸 Основной воркер
async def run_ema_position_worker():
    redis = infra.redis_client
    stream_name = "indicator_stream"

    # Стартуем с самого последнего сообщения (без риска потерять последующие)
    try:
        stream_info = await redis.xinfo_stream(stream_name)
        last_id = stream_info["last-generated-id"]
    except Exception as e:
        log.warning(f"⚠️ Не удалось получить last ID из stream: {e}")
        last_id = "$"

    log.info(f"📡 Подписка на indicator_stream (EMA) с last_id = {last_id}")

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
                    asyncio.create_task(handle_ema_message(parsed))
                    last_id = msg_id  # 🔁 Обновляем позицию, чтобы не пропустить
        except Exception:
            log.exception("❌ Ошибка чтения из indicator_stream")
            await asyncio.sleep(1)