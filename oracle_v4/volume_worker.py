# volume_worker.py

import asyncio
import logging
from datetime import datetime

import infra
from core_io import save_flag

log = logging.getLogger("VOLUME_WORKER")


# 🔸 Расчёт флага volume_state
async def wait_for_volume_data(symbol: str, open_time: str):
    redis = infra.redis_client
    tf = "m5"
    count = 20

    log.debug(f"⏳ Сбор данных для volume_state: {symbol} @ {open_time}")

    key = f"ts:{symbol}:{tf}:v"
    try:
        series = await redis.ts().revrange(key, "-", "+", count=count)
        series.reverse()
        volume = [float(v) for _, v in series]
    except Exception as e:
        log.warning(f"⚠️ Ошибка чтения объёмов: {e}")
        return

    if len(volume) < 6:
        log.warning(f"⚠️ Недостаточно данных для расчёта объёма ({len(volume)} точек)")
        return

    V_curr = volume[-1]
    V_median = sorted(volume)[len(volume) // 2]
    V_last5 = volume[-5:]
    V_max_prev5 = max(volume[-6:-1])

    explanation = []
    result = None

    # --- SPIKE ---
    if V_curr >= 2 * V_median and V_curr >= 2 * V_max_prev5:
        result = "SPIKE"
        explanation.append("• V_curr ≥ 2×V_median и 2×V_max_prev5 — SPIKE")

    # --- RISING ---
    if not result:
        up_steps = sum(1 for i in range(1, 5) if V_last5[i] > V_last5[i - 1])
        if V_curr >= 1.5 * V_median and up_steps >= 3:
            result = "RISING"
            explanation.append(f"• V_curr ≥ 1.5×V_median и {up_steps} шагов роста — RISING")
        else:
            explanation.append(f"• RISING: V_curr={V_curr:.2f}, median={V_median:.2f}, ростов={up_steps} — FAILED")

    # --- FALLING ---
    if not result:
        down_steps = sum(1 for i in range(1, 5) if V_last5[i] < V_last5[i - 1])
        if V_curr <= 0.66 * V_median and down_steps >= 3:
            result = "FALLING"
            explanation.append(f"• V_curr ≤ 0.66×V_median и {down_steps} шагов падения — FALLING")
        else:
            explanation.append(f"• FALLING: V_curr={V_curr:.2f}, median={V_median:.2f}, спадов={down_steps} — FAILED")

    # --- STABLE ---
    if not result:
        in_range = 0.8 * V_median <= V_curr <= 1.2 * V_median
        if in_range and up_steps < 3 and down_steps < 3:
            result = "STABLE"
            explanation.append("• Объём в норме и нет устойчивой тенденции — STABLE")
        else:
            explanation.append("• STABLE не подтверждён — FAILED")

    if not result:
        result = "STABLE"
        explanation.append("• Не выполнены условия других флагов — STABLE по умолчанию")

    log.debug(f"🧭 volume_state = {result} для {symbol} @ {open_time}")
    for line in explanation:
        log.debug("    " + line)

    await save_flag(symbol, open_time, "volume_state", result)


# 🔸 Обработка сигнала из indicator_stream
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

    log.debug(f"🔔 Сигнал для расчёта volume_state: {symbol} | {indicator} | {tf} | {open_time}")
    await wait_for_volume_data(symbol, open_time)


# 🔸 Воркер: слушает indicator_stream
async def run_volume_worker():
    redis = infra.redis_client
    stream_name = "indicator_stream"
    last_id = "$"

    log.debug("📡 Подписка на Redis Stream: indicator_stream (volume)")

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