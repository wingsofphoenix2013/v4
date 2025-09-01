# 🔸 indicators_ema_status.py — Этап 3–4: расчёт EMA-статуса (fixed eps0/eps1), запись в Redis/PG

import os
import asyncio
import json
import logging
from datetime import datetime

log = logging.getLogger("EMA_STATUS")

# 🔸 Конфиг
READY_STREAM   = "indicator_stream"
GROUP_NAME     = os.getenv("EMA_STATUS_GROUP", "ema_status_v1")
CONSUMER_NAME  = os.getenv("EMA_STATUS_CONSUMER", "ema_status_1")
REQUIRED_TFS   = {"m5", "m15", "h1"}

DEBOUNCE_MS       = int(os.getenv("EMA_STATUS_DEBOUNCE_MS", "250"))
MAX_CONCURRENCY   = int(os.getenv("EMA_STATUS_MAX_CONCURRENCY", "64"))
MAX_PER_SYMBOL    = int(os.getenv("EMA_STATUS_MAX_PER_SYMBOL", "4"))
XREAD_BLOCK_MS    = int(os.getenv("EMA_STATUS_BLOCK_MS", "1000"))
XREAD_COUNT       = int(os.getenv("EMA_STATUS_COUNT", "50"))

# EMA длины
def _parse_ema_lens(raw: str) -> list[int]:
    out = []
    for part in raw.split(","):
        s = part.strip()
        if not s:
            continue
        try:
            out.append(int(s))
        except:
            pass
    return out or [9, 21, 50, 100, 200]

EMA_LENS = _parse_ema_lens(os.getenv("EMA_STATUS_EMA_LENS", "9,21,50,100,200"))

# Пороговые дефолты (в долях ATR / BB-width)
EPS0 = float(os.getenv("EMA_STATUS_EPS0", "0.05"))  # зона equal
EPS1 = float(os.getenv("EMA_STATUS_EPS1", "0.02"))  # значимое изменение ΔD

# 🔸 Пул параллелизма
task_gate = asyncio.Semaphore(MAX_CONCURRENCY)
symbol_semaphores: dict[str, asyncio.Semaphore] = {}
bucket_tasks: dict[tuple, asyncio.Task] = {}

# 🔸 Утилиты
def _iso_to_ms(iso_str: str) -> int:
    dt = datetime.fromisoformat(iso_str)
    return int(dt.timestamp() * 1000)

def _tf_step_ms(tf: str) -> int:
    return 300_000 if tf == "m5" else (900_000 if tf == "m15" else 3_600_000)

# 🔸 Ключи TS
def k_close(sym: str, tf: str) -> str:
    return f"ts:{sym}:{tf}:c"

def k_ema(sym: str, tf: str, L: int) -> str:
    return f"ts_ind:{sym}:{tf}:ema{L}"

def k_atr(sym: str, tf: str) -> str:
    return f"ts_ind:{sym}:{tf}:atr14"

def k_bb(sym: str, tf: str, part: str) -> str:
    return f"ts_ind:{sym}:{tf}:bb20_2_0_{part}"

# 🔸 Чтение одного сэмпла ровно на open_time
async def _get_point(redis, key: str, ts_ms: int):
    try:
        r = await redis.execute_command("TS.RANGE", key, ts_ms, ts_ms)
        if r and int(r[0][0]) == ts_ms:
            return float(r[0][1])
    except Exception as e:
        log.debug("[TSERR] key=%s err=%s", key, e)
    return None

# 🔸 Маппинг кода → label
STATE_LABELS = {
    0: "below_away",
    1: "below_towards",
    2: "equal",
    3: "above_towards",
    4: "above_away",
}

# 🔸 Расчёт статуса для одной EMA длины
def _classify_one(close_t: float,
                  ema_t: float, ema_prev: float,
                  scale_t: float, scale_prev: float) -> tuple[int, str, float, float, float, float, float]:
    # защита от нулевого масштаба
    if scale_t is None or scale_t <= 0.0 or ema_t is None or ema_prev is None or close_t is None or scale_prev is None or scale_prev <= 0.0:
        return None

    nd_t = (close_t - ema_t) / scale_t
    d_t = abs(nd_t)

    # на предыдущем баре: оценим close_{t-1} как ema_prev + nd_prev*scale_prev.
    # Но close_{t-1} нам не нужен: D_{t-1} считаем по (close_{t-1}-ema_prev)/scale_prev; без close_{t-1} не посчитать.
    # Аппроксимация: используем D_{t-1} ≈ |(ema_t - ema_prev)/scale_prev|? Это не корректно.
    # Правильно: нам нужен close_prev. Добавим его в сбор фич.
    return None  # заглушка: возвращаем None, если нет close_prev

# 🔸 Полная классификация (с учётом close_prev / scale_prev)
def _classify_with_prev(close_t: float, close_prev: float,
                        ema_t: float, ema_prev: float,
                        scale_t: float, scale_prev: float,
                        eps0: float, eps1: float,
                        prev_suffix: str | None) -> tuple[int, str, float, float, float]:
    if scale_t is None or scale_t <= 0.0 or scale_prev is None or scale_prev <= 0.0:
        return None
    if None in (close_t, close_prev, ema_t, ema_prev):
        return None

    nd_t = (close_t - ema_t) / scale_t
    nd_prev = (close_prev - ema_prev) / scale_prev

    d_t = abs(nd_t)
    d_prev = abs(nd_prev)
    delta_d = d_t - d_prev

    # equal-зона
    if d_t <= eps0:
        return 2, STATE_LABELS[2], nd_t, d_t, delta_d

    # сторона
    above = nd_t > 0.0

    # тренд расстояния
    if delta_d >= eps1:
        code = 4 if above else 0
    elif delta_d <= -eps1:
        code = 3 if above else 1
    else:
        # гистерезис: если изменение незначимо — удерживаем суффикс
        # если prev_suffix неизвестен — выберем towards по умолчанию (ближе к EMA) при малых d_t
        if prev_suffix in ("away", "towards"):
            if above:
                code = 4 if prev_suffix == "away" else 3
            else:
                code = 0 if prev_suffix == "away" else 1
        else:
            # нет истории суффикса — выберем towards как более консервативный
            code = 3 if above else 1

    return code, STATE_LABELS[code], nd_t, d_t, delta_d

# 🔸 Redis ключи для статуса
def kv_key(symbol: str, tf: str, L: int) -> str:
    return f"ind:{symbol}:{tf}:ema{L}_status"

def ts_key(symbol: str, tf: str, L: int) -> str:
    return f"ts_ind:{symbol}:{tf}:ema{L}_status"

RETENTION_TS_MS = 14 * 24 * 60 * 60 * 1000  # 14d

# 🔸 Сбор фич для одного (symbol, tf, open_time)
async def collect_features(redis, symbol: str, tf: str, open_ms: int) -> dict:
    step = _tf_step_ms(tf)
    prev_ms = open_ms - step

    need_atr = (tf in {"m5", "m15"})
    # Текущие и предыдущие close
    calls = [
        _get_point(redis, k_close(symbol, tf), open_ms),
        _get_point(redis, k_close(symbol, tf), prev_ms),
    ]
    # EMA (текущие и предыдущие)
    for L in EMA_LENS:
        calls.append(_get_point(redis, k_ema(symbol, tf, L), open_ms))
        calls.append(_get_point(redis, k_ema(symbol, tf, L), prev_ms))

    # Масштаб: ATR для m5/m15; BB для h1; плюс prev масштабы
    if need_atr:
        calls.append(_get_point(redis, k_atr(symbol, tf), open_ms))
        calls.append(_get_point(redis, k_atr(symbol, tf), prev_ms))
        # fallback BB на текущем баре (и prev, чтобы в случае ATR≈0 был scale_prev)
        calls.append(_get_point(redis, k_bb(symbol, tf, "upper"), open_ms))
        calls.append(_get_point(redis, k_bb(symbol, tf, "lower"), open_ms))
        calls.append(_get_point(redis, k_bb(symbol, tf, "upper"), prev_ms))
        calls.append(_get_point(redis, k_bb(symbol, tf, "lower"), prev_ms))
    else:
        # h1: только BB (тек/prev)
        calls.append(_get_point(redis, k_bb(symbol, tf, "upper"), open_ms))
        calls.append(_get_point(redis, k_bb(symbol, tf, "lower"), open_ms))
        calls.append(_get_point(redis, k_bb(symbol, tf, "upper"), prev_ms))
        calls.append(_get_point(redis, k_bb(symbol, tf, "lower"), prev_ms))

    res = await asyncio.gather(*calls, return_exceptions=True)
    idx = 0

    close_t  = res[idx]; idx += 1
    close_p  = res[idx]; idx += 1

    ema_t = {}
    ema_p = {}
    for L in EMA_LENS:
        ema_t[L] = res[idx]; idx += 1
        ema_p[L] = res[idx]; idx += 1

    if need_atr:
        atr_t = res[idx]; idx += 1
        atr_p = res[idx]; idx += 1
        bbu_t = res[idx]; idx += 1
        bbl_t = res[idx]; idx += 1
        bbu_p = res[idx]; idx += 1
        bbl_p = res[idx]; idx += 1
        # scale текущий:
        scale_t = atr_t if (atr_t is not None and atr_t > 0.0) else (
            (bbu_t - bbl_t) if (bbu_t is not None and bbl_t is not None and (bbu_t - bbl_t) > 0.0) else None
        )
        # scale предыдущий:
        scale_p = atr_p if (atr_p is not None and atr_p > 0.0) else (
            (bbu_p - bbl_p) if (bbu_p is not None and bbl_p is not None and (bbu_p - bbl_p) > 0.0) else None
        )
    else:
        bbu_t = res[idx]; idx += 1
        bbl_t = res[idx]; idx += 1
        bbu_p = res[idx]; idx += 1
        bbl_p = res[idx]; idx += 1
        scale_t = (bbu_t - bbl_t) if (bbu_t is not None and bbl_t is not None and (bbu_t - bbl_t) > 0.0) else None
        scale_p = (bbu_p - bbl_p) if (bbu_p is not None and bbl_p is not None and (bbu_p - bbl_p) > 0.0) else None

    return {
        "close_t": close_t, "close_p": close_p,
        "ema_t": ema_t, "ema_p": ema_p,
        "scale_t": scale_t, "scale_p": scale_p,
    }

# 🔸 Запись в Redis и PG
async def _publish_status(redis, pg, symbol: str, tf: str, L: int, open_ms: int,
                          code: int, label: str, nd: float, d: float, delta_d: float,
                          eps0: float, eps1: float):
    open_iso = datetime.utcfromtimestamp(open_ms / 1000).isoformat()

    # Redis KV
    try:
        await redis.set(kv_key(symbol, tf, L), str(code))
    except Exception as e:
        log.debug("[KVERR] %s err=%s", kv_key(symbol, tf, L), e)

    # Redis TS
    try:
        await redis.execute_command(
            "TS.ADD", ts_key(symbol, tf, L), open_ms, str(code),
            "RETENTION", RETENTION_TS_MS, "DUPLICATE_POLICY", "last"
        )
    except Exception as e:
        log.debug("[TSADDERR] %s err=%s", ts_key(symbol, tf, L), e)

    # PG UPSERT
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
            """, symbol, tf, L, datetime.fromisoformat(open_iso),
                 code, label, nd, d, delta_d, eps0, eps1)
    except Exception as e:
        log.exception("❌ PG upsert error for %s/%s/ema%d @ %s: %s", symbol, tf, L, open_iso, e)

# 🔸 Обработка «бакета»: debounce → фичи → расчёт → запись
async def handle_bucket(symbol: str, tf: str, open_time_ms: int, redis, pg):
    await asyncio.sleep(DEBOUNCE_MS / 1000)

    feats = await collect_features(redis, symbol, tf, open_time_ms)

    close_t  = feats["close_t"]
    close_p  = feats["close_p"]
    scale_t  = feats["scale_t"]
    scale_p  = feats["scale_p"]

    if close_t is None or close_p is None or scale_t is None or scale_p is None:
        log.info("[SKIP] %s/%s @ %d → missing close/scale", symbol, tf, open_time_ms)
        return

    # предыдущий суффикс: в первой версии не храним, используем None → будет towards при малых Δ
    prev_suffix = None

    for L in EMA_LENS:
        ema_t = feats["ema_t"].get(L)
        ema_p = feats["ema_p"].get(L)
        if ema_t is None or ema_p is None:
            continue

        cls = _classify_with_prev(close_t, close_p, ema_t, ema_p, scale_t, scale_p, EPS0, EPS1, prev_suffix)
        if cls is None:
            log.debug("[MISS] %s/%s/ema%d @ %d → not enough data", symbol, tf, L, open_time_ms)
            continue

        code, label, nd, d, delta_d = cls
        # публикация
        await _publish_status(redis, pg, symbol, tf, L, open_time_ms, code, label, nd, d, delta_d, EPS0, EPS1)

        log.info("[STATE] %s/%s/ema%d @ %d → code=%d label=%s nd=%.4f d=%.4f Δd=%.4f",
                 symbol, tf, L, open_time_ms, code, label, nd, d, delta_d)

# 🔸 Основной цикл: XREADGROUP по indicator_stream
async def run_indicators_ema_status(pg, redis):
    log.info("EMA Status: init consumer-group")
    try:
        await redis.xgroup_create(READY_STREAM, GROUP_NAME, id="$", mkstream=True)
        log.info("✅ consumer-group '%s' создана на '%s'", GROUP_NAME, READY_STREAM)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info("ℹ️ consumer-group '%s' уже существует", GROUP_NAME)
        else:
            log.exception("❌ XGROUP CREATE error: %s", e)
            raise

    log.info("🚀 Этап 3–4: слушаем '%s' (group=%s, consumer=%s)", READY_STREAM, GROUP_NAME, CONSUMER_NAME)

    while True:
        try:
            resp = await redis.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={READY_STREAM: ">"},
                count=XREAD_COUNT,
                block=XREAD_BLOCK_MS
            )
            if not resp:
                continue

            to_ack = []
            for _, messages in resp:
                for msg_id, data in messages:
                    try:
                        symbol  = data.get("symbol")
                        tf      = data.get("timeframe") or data.get("interval")
                        status  = data.get("status")
                        open_iso= data.get("open_time")

                        if not symbol or tf not in REQUIRED_TFS or status != "ready" or not open_iso:
                            to_ack.append(msg_id)
                            continue

                        open_ms = _iso_to_ms(open_iso)
                        bucket  = (symbol, tf, open_ms)

                        if bucket in bucket_tasks and not bucket_tasks[bucket].done():
                            to_ack.append(msg_id)
                            continue

                        if symbol not in symbol_semaphores:
                            symbol_semaphores[symbol] = asyncio.Semaphore(MAX_PER_SYMBOL)

                        log.info("[READY] %s/%s @ %s → schedule EMA-status", symbol, tf, open_iso)

                        async def bucket_runner():
                            async with task_gate:
                                async with symbol_semaphores[symbol]:
                                    await handle_bucket(symbol, tf, open_ms, redis, None if pg is None else pg)

                        bucket_tasks[bucket] = asyncio.create_task(bucket_runner())
                        to_ack.append(msg_id)

                    except Exception as parse_err:
                        to_ack.append(msg_id)
                        log.exception("❌ message parse error: %s", parse_err)

            if to_ack:
                await redis.xack(READY_STREAM, GROUP_NAME, *to_ack)

        except Exception as e:
            log.exception("❌ XREADGROUP loop error: %s", e)
            await asyncio.sleep(1)