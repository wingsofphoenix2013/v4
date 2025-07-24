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
# 🔸 Асинхронное ожидание всех нужных значений в Redis TS (с историей и расчётом флага)
async def wait_for_all_indicators(symbol: str, open_time: str):
    redis = infra.redis_client
    max_wait_sec = 20
    check_interval = 1
    waited = 0

    log.debug(f"⏳ Ожидание индикаторов для {symbol} @ {open_time}")

    # Целевая точка во времени (в мс)
    target_dt = datetime.fromisoformat(open_time.replace("Z", ""))
    target_ts = int(target_dt.timestamp() * 1000)

    while waited < max_wait_sec:
        all_ready = True

        # Проверка наличия последней точки
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
            if not all_ready:
                break

        if not all_ready:
            await asyncio.sleep(check_interval)
            waited += check_interval
            continue

        # ✅ Все параметры готовы — загружаем историю
        history = {}  # tf -> param -> List[float]
        for tf, params in REQUIRED_PARAMS.items():
            interval_ms = 900_000 if tf == "m15" else 300_000
            from_ts = target_ts - interval_ms * 4
            history[tf] = {}

            for param in params:
                key = f"ts_ind:{symbol}:{tf}:{param}"
                try:
                    series = await redis.ts().range(key, from_ts, target_ts, count=5)
                    values = [float(v) for _, v in series]
                    history[tf][param] = values
                except redis.exceptions.ResponseError as e:
                    log.warning(f"⚠️ Ошибка чтения TS для {key}: {e}")
                    history[tf][param] = []

        # 🔍 Расчёт условий
        explanation = []
        result = None

        # --- UP ---
        cond_up = 0
        try:
            if history["m15"]["ema9"][-1] > history["m15"]["ema21"][-1] and \
               history["m15"]["ema9"][-1] > history["m15"]["ema9"][0]:
                cond_up += 1
                explanation.append("• EMA(9) > EMA(21) и наклон вверх — OK")
            else:
                explanation.append("• EMA(9) > EMA(21) и наклон вверх — FAILED")

            if history["m15"]["adx_dmi14_adx"][-1] > 20 and \
               history["m15"]["adx_dmi14_plus_di"][-1] > history["m15"]["adx_dmi14_minus_di"][-1]:
                cond_up += 1
                explanation.append("• ADX > 20 и DMI+ > DMI− — OK")
            else:
                explanation.append("• ADX > 20 и DMI+ > DMI− — FAILED")

            h_hist = history["m5"]["macd12_macd_hist"]
            if history["m5"]["macd12_macd"][-1] > history["m5"]["macd12_macd_signal"][-1] and \
               h_hist[-1] > 0 and h_hist[-1] > h_hist[-2]:
                cond_up += 1
                explanation.append("• MACD > сигнал, гистограмма растёт — OK")
            else:
                explanation.append("• MACD > сигнал, гистограмма растёт — FAILED")

            h_rsi = history["m5"]["rsi14"]
            if h_rsi[-1] > 55 and min(h_rsi) > 50:
                cond_up += 1
                explanation.append("• RSI > 55 и выше 50 на всех 5 свечах — OK")
            else:
                explanation.append("• RSI > 55 и выше 50 на всех 5 свечах — FAILED")
        except Exception as e:
            explanation.append(f"⚠️ Ошибка при расчёте условий UP: {e}")

        if cond_up >= 3:
            result = "UP"

        # --- DOWN ---
        if not result:
            cond_down = 0
            try:
                if history["m15"]["ema9"][-1] < history["m15"]["ema21"][-1] and \
                   history["m15"]["ema9"][-1] < history["m15"]["ema9"][0]:
                    cond_down += 1
                    explanation.append("• EMA(9) < EMA(21) и наклон вниз — OK")
                else:
                    explanation.append("• EMA(9) < EMA(21) и наклон вниз — FAILED")

                if history["m15"]["adx_dmi14_adx"][-1] > 20 and \
                   history["m15"]["adx_dmi14_minus_di"][-1] > history["m15"]["adx_dmi14_plus_di"][-1]:
                    cond_down += 1
                    explanation.append("• ADX > 20 и DMI− > DMI+ — OK")
                else:
                    explanation.append("• ADX > 20 и DMI− > DMI+ — FAILED")

                h_hist = history["m5"]["macd12_macd_hist"]
                if history["m5"]["macd12_macd"][-1] < history["m5"]["macd12_macd_signal"][-1] and \
                   h_hist[-1] < 0 and h_hist[-1] < h_hist[-2]:
                    cond_down += 1
                    explanation.append("• MACD < сигнал, гистограмма убывает — OK")
                else:
                    explanation.append("• MACD < сигнал, гистограмма убывает — FAILED")

                h_rsi = history["m5"]["rsi14"]
                if h_rsi[-1] < 45 and max(h_rsi) < 50:
                    cond_down += 1
                    explanation.append("• RSI < 45 и ниже 50 на всех 5 свечах — OK")
                else:
                    explanation.append("• RSI < 45 и ниже 50 на всех 5 свечах — FAILED")
            except Exception as e:
                explanation.append(f"⚠️ Ошибка при расчёте условий DOWN: {e}")

            if cond_down >= 3:
                result = "DOWN"

        # --- TRANSITION ---
        if not result:
            cond_trans = 0
            try:
                h_adx = history["m15"]["adx_dmi14_adx"]
                if h_adx[0] < 15 and h_adx[-1] > 20:
                    cond_trans += 1
                    explanation.append("• ADX растёт с <15 до >20 — OK")
                else:
                    explanation.append("• ADX растёт с <15 до >20 — FAILED")

                e9 = history["m15"]["ema9"]
                e21 = history["m15"]["ema21"]
                if (e9[0] < e21[0] and e9[-1] > e21[-1]) or (e9[0] > e21[0] and e9[-1] < e21[-1]):
                    cond_trans += 1
                    explanation.append("• EMA(9) пересекает EMA(21) — OK")
                else:
                    explanation.append("• EMA(9) пересекает EMA(21) — FAILED")

                h_hist = history["m5"]["macd12_macd_hist"]
                if h_hist[-1] * h_hist[-2] < 0:
                    cond_trans += 1
                    explanation.append("• MACD hist меняет знак — OK")
                else:
                    explanation.append("• MACD hist меняет знак — FAILED")

                h_rsi = history["m5"]["rsi14"]
                if 47 <= h_rsi[0] <= 53 and (h_rsi[-1] > 55 or h_rsi[-1] < 45):
                    cond_trans += 1
                    explanation.append("• RSI выходит из зоны 47–53 — OK")
                else:
                    explanation.append("• RSI выходит из зоны 47–53 — FAILED")
            except Exception as e:
                explanation.append(f"⚠️ Ошибка при расчёте условий TRANSITION: {e}")

            if cond_trans >= 3:
                result = "TRANSITION"

        # --- FLAT по умолчанию ---
        if not result:
            result = "FLAT"
            explanation.append("• Не выполнено ≥3 условий ни для одного сценария → FLAT")

        log.debug(f"🧭 trend_state = {result} для {symbol} @ {open_time}")
        for line in explanation:
            log.debug("    " + line)

        await save_flag(symbol, open_time, "trend_state", result)

        return
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

    log.debug(f"🔔 Инициирующий сигнал получен: {symbol} | {indicator} | {tf} | {open_time}")
    await wait_for_all_indicators(symbol, open_time)


# 🔸 Основной воркер: слушает Redis Stream
async def run_trend_worker():
    redis = infra.redis_client
    stream_name = "indicator_stream"
    last_id = "$"

    log.debug("📡 Подписка на Redis Stream: indicator_stream")

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
# 🔸 Сохранение результата во флаговую таблицу
async def save_flag(symbol: str, open_time: str, flag_type: str, flag_value: str):
    query = """
        INSERT INTO oracle_flags_v4 (symbol, open_time, flag_type, flag_value)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT DO NOTHING
    """
    async with infra.pg_pool.acquire() as conn:
        await conn.execute(query, symbol, open_time, flag_type, flag_value)
        log.info(f"💾 Сохранён флаг {flag_type}={flag_value} для {symbol} @ {open_time}")