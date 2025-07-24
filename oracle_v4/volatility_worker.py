# volatility_worker.py

import asyncio
import logging
from datetime import datetime

import infra
from core_io import save_flag
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

    history = {"ts_ind": {}, "ts": {}}

    # --- ts_ind ---
    for param in REQUIRED_PARAMS_TS:
        key = f"ts_ind:{symbol}:{tf}:{param}"
        try:
            series = await redis.ts().revrange(key, "-", "+", count=count)
            series.reverse()
            values = [float(v) for _, v in series]
            history["ts_ind"][param] = values
            log.debug(f"🔍 ts_ind:{param} — {len(values)} точек")
        except Exception as e:
            log.warning(f"⚠️ Ошибка чтения {key}: {e}")
            history["ts_ind"][param] = []

    # --- ts ---
    ohlcv_mapping = {"open": "o", "high": "h", "low": "l", "close": "c"}
    for label, short in ohlcv_mapping.items():
        key = f"ts:{symbol}:{tf}:{short}"
        try:
            series = await redis.ts().revrange(key, "-", "+", count=count)
            series.reverse()
            values = [float(v) for _, v in series]
            history["ts"][label] = values
            log.debug(f"🔍 ts:{label} — {len(values)} точек")
        except Exception as e:
            log.warning(f"⚠️ Ошибка чтения {key}: {e}")
            history["ts"][label] = []

    log.info("✅ История индикаторов и OHLCV собрана успешно.")

    # --- Расчёт volatility_state ---
    explanation = []
    result = None

    # helper
    def median(lst):
        s = sorted(lst)
        n = len(s)
        return (s[n//2] if n % 2 == 1 else (s[n//2 - 1] + s[n//2]) / 2) if s else 0

    try:
        atr = history["ts_ind"]["atr14"]
        bb_upper = history["ts_ind"]["bb20_2_0_upper"]
        bb_lower = history["ts_ind"]["bb20_2_0_lower"]
        rsi = history["ts_ind"]["rsi14"]
        high = history["ts"]["high"]
        low = history["ts"]["low"]
        close = history["ts"]["close"]
        open_ = history["ts"]["open"]

        # derived
        bb_width = [u - l for u, l in zip(bb_upper, bb_lower)]
        bb_width_median = median(bb_width)
        atr_sma = sum(atr) / len(atr) if atr else 0
        atr_med = median(atr)
        last_close = close[-1]
        avg_range = sum(h - l for h, l in zip(high, low)) / len(high)
        recent_ranges = [h - l for h, l in zip(high[-3:], low[-3:])]
        recent_avg_range = sum(recent_ranges) / len(recent_ranges)

        # direction switches (EXPLODING)
        directions = [1 if c > o else -1 if c < o else 0 for c, o in zip(close, open_)]
        switches = sum(1 for i in range(1, len(directions)) if directions[i] != directions[i-1] and directions[i] != 0)

        # --- check flags ---
        # LOW
        low_cond = 0
        if atr[-1] < 0.75 * atr_sma:
            low_cond += 1
            explanation.append("• ATR < 0.75 × SMA(ATR) — OK")
        else:
            explanation.append("• ATR < 0.75 × SMA(ATR) — FAILED")

        if bb_width[-1] < 1.25 * bb_width_median:
            low_cond += 1
            explanation.append("• BB ширина < 1.25 × медианы — OK")
        else:
            explanation.append("• BB ширина < 1.25 × медианы — FAILED")

        if (high[-1] - low[-1]) / last_close < 0.005:
            low_cond += 1
            explanation.append("• Диапазон свечи < 0.5% от close — OK")
        else:
            explanation.append("• Диапазон свечи < 0.5% от close — FAILED")

        if all(45 <= x <= 55 for x in rsi[-5:]):
            low_cond += 1
            explanation.append("• RSI стабилен в диапазоне 45–55 — OK")
        else:
            explanation.append("• RSI стабилен в диапазоне 45–55 — FAILED")

        if low_cond >= 3:
            result = "LOW"

        # HIGH
        if not result:
            high_cond = 0
            if atr[-1] > 1.5 * atr_sma:
                high_cond += 1
                explanation.append("• ATR > 1.5 × SMA — OK")
            else:
                explanation.append("• ATR > 1.5 × SMA — FAILED")

            if bb_width[-1] > 1.5 * bb_width_median:
                high_cond += 1
                explanation.append("• BB ширина > 1.5 × медианы — OK")
            else:
                explanation.append("• BB ширина > 1.5 × медианы — FAILED")

            if max(rsi[-5:]) > 70 and min(rsi[-5:]) < 30:
                high_cond += 1
                explanation.append("• RSI колеблется между зонами >70 и <30 — OK")
            else:
                explanation.append("• RSI колеблется между зонами >70 и <30 — FAILED")

            if switches >= 3:
                high_cond += 1
                explanation.append(f"• ≥3 смены направления в свечах — OK ({switches})")
            else:
                explanation.append(f"• ≥3 смены направления — FAILED ({switches})")

            if high_cond >= 3:
                result = "HIGH"

        # EXPLODING
        if not result:
            expl_cond = 0
            if atr[-1] > 2 * atr_med:
                expl_cond += 1
                explanation.append("• ATR > 2× медианы — OK")
            else:
                explanation.append("• ATR > 2× медианы — FAILED")

            if bb_width[-1] > bb_width[-3] * 1.3:
                expl_cond += 1
                explanation.append("• BB ширина выросла >30% за 3 свечи — OK")
            else:
                explanation.append("• BB ширина выросла >30% — FAILED")

            if (rsi[-3] > 70 and rsi[-1] < 40) or (rsi[-3] < 30 and rsi[-1] > 60):
                expl_cond += 1
                explanation.append("• RSI резко сменил зону — OK")
            else:
                explanation.append("• RSI резко сменил зону — FAILED")

            if sum(recent_ranges) > 2 * avg_range * 3:
                expl_cond += 1
                explanation.append("• Суммарный диапазон 3 свечей > 2× нормы — OK")
            else:
                explanation.append("• Диапазон 3 свечей > 2× нормы — FAILED")

            if expl_cond >= 2:
                result = "EXPLODING"

        # default: MEDIUM
        if not result:
            result = "MEDIUM"
            explanation.append("• Нет чётких признаков — MEDIUM по умолчанию")

        log.info(f"🧭 volatility_state = {result} для {symbol} @ {open_time}")
        for line in explanation:
            log.info("    " + line)
            
        await save_flag(symbol, open_time, "volatility_state", result)

    except Exception as e:
        log.exception(f"❌ Ошибка при расчёте volatility_state: {e}")
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