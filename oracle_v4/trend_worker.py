# trend_worker.py

import asyncio
import logging
from datetime import datetime
import infra
import redis.exceptions

log = logging.getLogger("TREND_WORKER")

# 🔸 Параметры, которые нужно получить перед расчётом
REQUIRED_PARAMS = {
    "m15": [
        "ema9",
        "ema21",
        "adx_dmi14_adx",
        "adx_dmi14_plus_di",
        "adx_dmi14_minus_di",
    ],
    "m5": [
        "macd12_macd",
        "macd12_macd_signal",
        "macd12_macd_hist",
        "rsi14",
    ],
}
# 🔸 Асинхронное ожидание всех нужных значений в Redis TS (с историей)
async def wait_for_all_indicators(symbol: str, open_time: str):
    redis = infra.redis_client
    max_wait_sec = 20
    check_interval = 1
    waited = 0

    log.info(f"⏳ Ожидание индикаторов для {symbol} @ {open_time}")

    # Преобразуем open_time в timestamp
    target_dt = datetime.fromisoformat(open_time.replace("Z", ""))
    target_ts = int(target_dt.timestamp() * 1000)

    while waited < max_wait_sec:
        all_ready = True
        values_ready = {}

        for tf, params in REQUIRED_PARAMS.items():
            for param in params:
                key = f"ts_ind:{symbol}:{tf}:{param}"
                try:
                    val = await redis.ts().get(key)
                except redis.exceptions.ResponseError as e:
                    if "WRONGTYPE" in str(e):
                        log.warning(f"⚠️ Неверный тип Redis ключа: {key} — не TimeSeries")
                        all_ready = False
                        break
                    else:
                        raise

                if not val:
                    log.debug(f"⏳ Ожидание: {key} пока отсутствует")
                    all_ready = False
                    break

                values_ready[f"{tf}:{param}"] = val[1]

            if not all_ready:
                break

        if all_ready:
            log.info(f"✅ Все параметры получены для {symbol} @ {open_time}")

            log.info("📊 История значений из Redis TS:")
            for tf, params in REQUIRED_PARAMS.items():
                for param in params:
                    key = f"ts_ind:{symbol}:{tf}:{param}"

                    # Предполагаем, что таймфрейм m15 = 900_000 мс, m5 = 300_000 мс
                    interval_ms = 900_000 if tf == "m15" else 300_000
                    from_ts = target_ts - interval_ms * 4  # 5 точек включая целевую

                    try:
                        series = await redis.ts().range(
                            key,
                            from_ts,
                            target_ts,
                            count=5
                        )
                    except redis.exceptions.ResponseError as e:
                        log.warning(f"⚠️ Ошибка чтения TS для {key}: {e}")
                        continue

                    log.info(f"🔍 {key}")
                    for ts, value in series:
                        ts_str = datetime.utcfromtimestamp(ts / 1000).isoformat()
                        log.info(f"    • {ts_str} → {value}")

            return

        await asyncio.sleep(check_interval)
        waited += check_interval

    log.warning(f"⚠️ Не удалось собрать все параметры для {symbol} @ {open_time} за {max_wait_sec} сек")
# 🔸 Обработка одного инициирующего сигнала
async def handle_initiator(message: dict):
    symbol = message.get("symbol")
    tf = message.get("timeframe")
    indicator = message.get("indicator")
    open_time = message.get("open_time")
    status = message.get("status")

    if not all([symbol, tf, indicator, open_time, status]):
        log.warning(f"⚠️ Неполное сообщение: {message}")
        return

    if tf != "m15" or indicator != "ema9" or status != "ready":
        return

    log.info(f"🔔 Инициирующий сигнал получен: {symbol} | {indicator} | {tf} | {open_time}")
    await wait_for_all_indicators(symbol, open_time)


# 🔸 Основной воркер: слушает Redis Stream
async def run_trend_worker():
    redis = infra.redis_client
    stream_name = "indicator_stream"
    last_id = "$"

    log.info("📡 Подписка на Redis Stream: indicator_stream")

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
            log.exception("❌ Ошибка при чтении из indicator_stream")
            await asyncio.sleep(1)