# 🔸 indicators_ema_status_backfill.py — EMA Status backfill: 14 суток, батчи по времени, суммарные INFO-логи

import os
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

# 🔸 Логи
log = logging.getLogger("EMA_STATUS_BF")

# 🔸 Конфиг бэкофилла
START_DELAY_SEC = int(os.getenv("EMA_STATUS_BF_START_DELAY_SEC", "120"))     # 2 мин до первого прогона
BF_MAX_RUN_SECONDS = int(os.getenv("EMA_STATUS_BF_MAX_RUN_SECONDS", "900"))  # бюджет на цикл (15 мин)
WINDOW_DAYS = int(os.getenv("EMA_STATUS_BF_WINDOW_DAYS", "14"))              # глубина истории
BATCH_SLEEP_MS = int(os.getenv("EMA_STATUS_BF_SLEEP_MS", "100"))             # пауза между символами
EMA_LENS = [int(x) for x in (os.getenv("EMA_STATUS_EMA_LENS", "9,21,50,100,200").split(","))]
EPS0 = float(os.getenv("EMA_STATUS_EPS0", "0.05"))
EPS1 = float(os.getenv("EMA_STATUS_EPS1", "0.02"))
REQUIRED_TFS = ("m5", "m15", "h1")

RETENTION_TS_MS = 14 * 24 * 60 * 60 * 1000  # 14d

# 🔸 Ключи TS
def k_close(sym: str, tf: str) -> str:
    return f"ts:{sym}:{tf}:c"

def k_ema(sym: str, tf: str, L: int) -> str:
    return f"ts_ind:{sym}:{tf}:ema{L}"

def k_atr(sym: str, tf: str) -> str:
    return f"ts_ind:{sym}:{tf}:atr14"

def k_bb(sym: str, tf: str, part: str) -> str:
    return f"ts_ind:{sym}:{tf}:bb20_2_0_{part}"

# 🔸 Redis ключи для статуса
def kv_key(symbol: str, tf: str, L: int) -> str:
    return f"ind:{symbol}:{tf}:ema{L}_status"

def ts_key(symbol: str, tf: str, L: int) -> str:
    return f"ts_ind:{symbol}:{tf}:ema{L}_status"

# 🔸 Маппинг кода → label
STATE_LABELS = {
    0: "below_away",
    1: "below_towards",
    2: "equal",
    3: "above_towards",
    4: "above_away",
}

# 🔸 Утилиты времени
def _tf_step_ms(tf: str) -> int:
    return 300_000 if tf == "m5" else (900_000 if tf == "m15" else 3_600_000)

def _to_dt(ms: int) -> datetime:
    return datetime.utcfromtimestamp(ms / 1000).replace(tzinfo=None)

# 🔸 TS helpers
async def ts_range_map(redis, key: str, start_ms: int, end_ms: int) -> dict[int, float]:
    try:
        r = await redis.execute_command("TS.RANGE", key, start_ms, end_ms)
        return {int(ts): float(v) for ts, v in (r or [])}
    except Exception:
        return {}

# 🔸 Классификация одной точки (есть close/ema/scale на t и t-1)
def classify(close_t: float, close_p: float,
             ema_t: float, ema_p: float,
             scale_t: float, scale_p: float,
             eps0: float, eps1: float) -> tuple[int, str, float, float, float] | None:
    if None in (close_t, close_p, ema_t, ema_p, scale_t, scale_p):
        return None
    if scale_t <= 0.0 or scale_p <= 0.0:
        return None

    nd_t = (close_t - ema_t) / scale_t
    nd_p = (close_p - ema_p) / scale_p
    d_t = abs(nd_t)
    d_p = abs(nd_p)
    delta_d = d_t - d_p

    if d_t <= eps0:
        return 2, STATE_LABELS[2], nd_t, d_t, delta_d

    above = nd_t > 0.0
    if delta_d >= eps1:
        code = 4 if above else 0
    elif delta_d <= -eps1:
        code = 3 if above else 1
    else:
        # без памяти суффикса, консервативно towards
        code = 3 if above else 1

    return code, STATE_LABELS[code], nd_t, d_t, delta_d

# 🔸 Запись одной точки
async def publish_one(redis, pg, symbol: str, tf: str, L: int, t_ms: int,
                      code: int, label: str, nd: float, d: float, delta_d: float):
    # KV
    try:
        await redis.set(kv_key(symbol, tf, L), str(code))
    except Exception:
        pass
    # TS
    try:
        await redis.execute_command(
            "TS.ADD", ts_key(symbol, tf, L), t_ms, str(code),
            "RETENTION", RETENTION_TS_MS, "DUPLICATE_POLICY", "last"
        )
    except Exception:
        pass
    # PG
    try:
        async with pg.acquire() as conn:
            await conn.execute("""
                INSERT INTO indicator_emastatus
                  (symbol, timeframe, ema_len, open_time,
                   state_code, state_label, nd, d, delta_d, eps0, eps1, version_id, updated_at)
                VALUES ($1,$2,$3,$4,
                        $5,$6,$7,$8,$9,$10,$11,1,NOW())
                ON CONFLICT (symbol, timeframe, ema_len, open_time)
                DO UPDATE SET
                  state_code = EXCLUDED.state_code,
                  state_label = EXCLUDED.state_label,
                  nd = EXCLUDED.nd,
                  d = EXCLUDED.d,
                  delta_d = EXCLUDED.delta_d,
                  eps0 = EXCLUDED.eps0,
                  eps1 = EXCLUDED.eps1,
                  version_id = EXCLUDED.version_id,
                  updated_at = NOW()
            """, symbol, tf, L, _to_dt(t_ms), code, label, nd, d, delta_d, EPS0, EPS1)
    except Exception as e:
        log.debug("[PG] upsert err %s/%s/ema%d @ %s: %s", symbol, tf, L, _to_dt(t_ms), e)

# 🔸 Прогон одного символа × TF
async def backfill_symbol_tf(pg, redis, symbol: str, tf: str, start_ms: int, end_ms: int) -> tuple[int, int]:
    step = _tf_step_ms(tf)
    # серийные данные
    close_map = await ts_range_map(redis, k_close(symbol, tf), start_ms - step, end_ms)
    if not close_map:
        return 0, 0

    ema_maps = {L: await ts_range_map(redis, k_ema(symbol, tf, L), start_ms - step, end_ms) for L in EMA_LENS}
    need_atr = (tf in ("m5", "m15"))
    atr_map = await ts_range_map(redis, k_atr(symbol, tf), start_ms - step, end_ms) if need_atr else {}
    bb_up = await ts_range_map(redis, k_bb(symbol, tf, "upper"), start_ms - step, end_ms)
    bb_lo = await ts_range_map(redis, k_bb(symbol, tf, "lower"), start_ms - step, end_ms)

    # набор баров по close
    bars = sorted(ts for ts in close_map.keys() if start_ms <= ts <= end_ms)
    processed = 0
    skipped = 0

    for t in bars:
        t_prev = t - step
        close_t = close_map.get(t)
        close_p = close_map.get(t_prev)
        # масштабы
        if need_atr:
            scale_t = atr_map.get(t) if atr_map.get(t, 0.0) > 0.0 else (
                (bb_up.get(t) - bb_lo.get(t)) if (t in bb_up and t in bb_lo and (bb_up[t] - bb_lo[t]) > 0.0) else None
            )
            scale_p = atr_map.get(t_prev) if atr_map.get(t_prev, 0.0) > 0.0 else (
                (bb_up.get(t_prev) - bb_lo.get(t_prev)) if (t_prev in bb_up and t_prev in bb_lo and (bb_up[t_prev] - bb_lo[t_prev]) > 0.0) else None
            )
        else:
            scale_t = (bb_up.get(t) - bb_lo.get(t)) if (t in bb_up and t in bb_lo and (bb_up[t] - bb_lo[t]) > 0.0) else None
            scale_p = (bb_up.get(t_prev) - bb_lo.get(t_prev)) if (t_prev in bb_up and t_prev in bb_lo and (bb_up[t_prev] - bb_lo[t_prev]) > 0.0) else None

        if None in (close_t, close_p, scale_t, scale_p):
            skipped += 1
            continue

        # по всем EMA длинам
        for L in EMA_LENS:
            ema_t = ema_maps[L].get(t)
            ema_p = ema_maps[L].get(t_prev)
            if ema_t is None or ema_p is None:
                continue
            cls = classify(close_t, close_p, ema_t, ema_p, scale_t, scale_p, EPS0, EPS1)
            if cls is None:
                continue
            code, label, nd, d, delta_d = cls
            await publish_one(redis, pg, symbol, tf, L, t, code, label, nd, d, delta_d)
            processed += 1

    return processed, skipped

# 🔸 Получение списка активных символов из БД
async def load_active_symbols(pg) -> list[str]:
    async with pg.acquire() as conn:
        rows = await conn.fetch("""
            SELECT symbol
            FROM tickers_v4
            WHERE status = 'enabled' AND tradepermission = 'enabled'
        """)
    return [r["symbol"] for r in rows]

# 🔸 Один проход бэкофилла за WINDOW_DAYS
async def run_indicators_ema_status_backfill_once(pg, redis):
    end_dt = datetime.utcnow()
    start_dt = end_dt - timedelta(days=WINDOW_DAYS)
    end_ms = int(end_dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
    start_ms = int(start_dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

    symbols = await load_active_symbols(pg)
    processed_total = 0
    skipped_total = 0
    started = datetime.utcnow()

    for sym in symbols:
        for tf in REQUIRED_TFS:
            try:
                processed, skipped = await backfill_symbol_tf(pg, redis, sym, tf, start_ms, end_ms)
                processed_total += processed
                skipped_total += skipped
            except Exception as e:
                log.debug("[BF] error %s/%s: %s", sym, tf, e)
        await asyncio.sleep(BATCH_SLEEP_MS / 1000)

        # бюджет времени
        if (datetime.utcnow() - started).total_seconds() >= BF_MAX_RUN_SECONDS:
            log.info("[BF] time budget reached: processed=%d skipped=%d symbols_done=%d",
                     processed_total, skipped_total, symbols.index(sym) + 1)
            return

    log.info("[BF] finished: processed=%d skipped=%d symbols=%d", processed_total, skipped_total, len(symbols))

# 🔸 Периодический цикл: старт через 2 минуты, далее каждый час
async def run_indicators_ema_status_backfill(pg, redis):
    log.info("🚀 EMA Status BF: старт через %d с, окно %d дней, бюджет %d с",
             START_DELAY_SEC, WINDOW_DAYS, BF_MAX_RUN_SECONDS)
    await asyncio.sleep(START_DELAY_SEC)
    while True:
        try:
            await run_indicators_ema_status_backfill_once(pg, redis)
        except asyncio.CancelledError:
            log.info("⏹️ EMA Status BF остановлен")
            raise
        except Exception as e:
            log.exception("❌ EMA Status BF error: %s", e)
        await asyncio.sleep(3600)  # час