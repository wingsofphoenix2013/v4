# ema_position_worker.py

import asyncio
import logging
from datetime import datetime

import infra
from core_io import save_ema_phase

log = logging.getLogger("EMA_WORKER")

VALID_EMAS = {"ema9", "ema21", "ema50", "ema100", "ema200"}
VALID_INTERVALS = {"m5", "m15", "h1"}
EPSILON_PCT = 0.001  # 0.1%

# 🔸 Расчёт фазы
def determine_phase(close_prev, close_now, ema_prev, ema_now) -> str:
    delta_prev = close_prev - ema_prev
    delta_now = close_now - ema_now

    near_prev = abs(delta_prev) < EPSILON_PCT * ema_prev
    near_now = abs(delta_now) < EPSILON_PCT * ema_now

    if near_now:
        return "near_ema"
    if not near_prev and not near_now:
        if delta_prev < 0 and delta_now > 0:
            return "cross_up"
        elif delta_prev > 0 and delta_now < 0:
            return "cross_down"
        elif delta_now > 0 and abs(delta_now) > abs(delta_prev):
            return "above_moving_away"
        elif delta_now > 0:
            return "above_approaching"
        elif delta_now < 0 and abs(delta_now) > abs(delta_prev):
            return "below_moving_away"
        elif delta_now < 0:
            return "below_approaching"
    return "near_ema"


# 🔸 Обработка одного сигнала
async def process_ema_phase(symbol: str, interval: str, open_time: str, ema_name: str):
    redis = infra.redis_client
    try:
        # Ключи
        close_key = f"ts:{symbol}:{interval}:c"
        ema_key = f"ts_ind:{symbol}:{interval}:{ema_name}"

        # Данные
        close_series = await redis.ts().revrange(close_key, "-", "+", count=2)
        ema_series = await redis.ts().revrange(ema_key, "-", "+", count=2)

        if len(close_series) < 2 or len(ema_series) < 2:
            log.warning(f"⚠️ Недостаточно данных для {symbol} {interval} {ema_name}")
            phase = "n/a"
        else:
            close_series.reverse()
            ema_series.reverse()

            close_prev, close_now = float(close_series[0][1]), float(close_series[1][1])
            ema_prev, ema_now = float(ema_series[0][1]), float(ema_series[1][1])

            phase = determine_phase(close_prev, close_now, ema_prev, ema_now)

        log.info(f"📊 EMA Фаза: {symbol} | {interval} | {ema_name} → {phase}")

    except Exception as e:
        log.exception(f"❌ Ошибка обработки {symbol} {interval} {ema_name}: {e}")


# 🔸 Обработка сообщения из стрима
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

    log.debug(f"🔔 EMA сигнал: {symbol} {interval} {indicator} @ {open_time}")
    await process_ema_phase(symbol, interval, open_time, indicator)


# 🔸 Основной воркер
async def run_ema_position_worker():
    redis = infra.redis_client
    stream_name = "indicator_stream"
    last_id = "$"

    log.info("📡 Подписка на indicator_stream (EMA)")

    while True:
        try:
            response = await redis.xread(
                streams={stream_name: last_id},
                count=10,
                block=1000
            )
            for stream, messages in response:
                for _, msg_data in messages:
                    parsed = {k: v for k, v in msg_data.items()}
                    asyncio.create_task(handle_ema_message(parsed))
        except Exception:
            log.exception("❌ Ошибка чтения из indicator_stream")
            await asyncio.sleep(1)