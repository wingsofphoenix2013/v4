# 🔸 indicators_market_watcher.py — regime9_code v2: гистерезис + нормированный наклон, запись в Redis KV/TS и PG

import os
import asyncio
import json
import logging
from datetime import datetime

# 🔸 regime9 v2 core
from regime9_core import RegimeState, RegimeParams, decide_regime_code

# 🔸 Константы и конфиг
READY_STREAM = "indicator_stream"
GROUP_NAME = os.getenv("MRW_GROUP", "mrw_v1_group")
CONSUMER_NAME = os.getenv("MRW_CONSUMER", "mrw_1")
REQUIRED_TFS = {"m5", "m15", "h1"}

DEBOUNCE_MS = int(os.getenv("MRW_DEBOUNCE_MS", "250"))
MAX_CONCURRENCY = int(os.getenv("MRW_MAX_CONCURRENCY", "64"))
MAX_PER_SYMBOL = int(os.getenv("MRW_MAX_PER_SYMBOL", "4"))
XREAD_BLOCK_MS = int(os.getenv("MRW_BLOCK_MS", "1000"))
XREAD_COUNT = int(os.getenv("MRW_COUNT", "50"))

N_PCT = int(os.getenv("MRW_N_PCT", "200"))     # окно p30/p70
N_ACC = int(os.getenv("MRW_N_ACC", "50"))      # окно z-score ΔMACD
EPS_Z = float(os.getenv("MRW_EPS_Z", "0.5"))   # порог ускорения для z-score

RETENTION_TS_MS = 14 * 24 * 60 * 60 * 1000     # 14 суток
REGIME_VERSION = 2                              # версия правил regime9 v2
REGIME_PARAM = "regime9_code"                   # имя параметра в KV/TS

# Гистерезис (подтверждения смены состояния)
HYST_TREND_BARS = int(os.getenv("MRW_R9_HYST_TREND_BARS", "2"))  # тренд↔флет
HYST_SUB_BARS = int(os.getenv("MRW_R9_HYST_SUB_BARS", "1"))      # accel/stable/decel

# 🔸 Глобальные структуры параллелизма
task_gate = asyncio.Semaphore(MAX_CONCURRENCY)
symbol_semaphores: dict[str, asyncio.Semaphore] = {}
bucket_tasks: dict[tuple, asyncio.Task] = {}


# 🔸 Утилиты
def _iso_to_ms(iso_str: str) -> int:
    dt = datetime.fromisoformat(iso_str)
    return int(dt.timestamp() * 1000)

def _tf_step_ms(tf: str) -> int:
    return 300_000 if tf == "m5" else (900_000 if tf == "m15" else 3_600_000)


# 🔸 Хранилище состояния гистерезиса в Redis (Hash)
def _state_key(symbol: str, tf: str) -> str:
    return f"mrw_state:{symbol}:{tf}"

async def _load_state(redis, symbol: str, tf: str) -> RegimeState:
    data = await redis.hgetall(_state_key(symbol, tf))
    if not data:
        return RegimeState()
    try:
        return RegimeState(
            core=data.get("core", "flat"),
            core_cnt=int(data.get("core_cnt", 0)),
            sub=data.get("sub", "stable"),
            sub_cnt=int(data.get("sub_cnt", 0)),
        )
    except Exception:
        return RegimeState()

async def _save_state(redis, symbol: str, tf: str, st: RegimeState) -> None:
    await redis.hset(
        _state_key(symbol, tf),
        mapping={
            "core": st.core,
            "core_cnt": str(st.core_cnt),
            "sub": st.sub,
            "sub_cnt": str(st.sub_cnt),
        },
    )


# 🔸 Чтение фич на бар: проверяем комплект и подгружаем окна
async def fetch_features_for_bar(redis, symbol: str, tf: str, open_time_ms: int) -> dict | None:
    log = logging.getLogger("MRW")

    adx_key = f"ts_ind:{symbol}:{tf}:adx_dmi14_adx" if tf in {"m5", "m15"} else f"ts_ind:{symbol}:{tf}:adx_dmi28_adx"
    ema_key = f"ts_ind:{symbol}:{tf}:ema21"
    macd_key = f"ts_ind:{symbol}:{tf}:macd12_macd_hist"
    bb_u = f"ts_ind:{symbol}:{tf}:bb20_2_0_upper"
    bb_l = f"ts_ind:{symbol}:{tf}:bb20_2_0_lower"
    bb_c = f"ts_ind:{symbol}:{tf}:bb20_2_0_center"
    atr_key = f"ts_ind:{symbol}:{tf}:atr14" if tf in {"m5", "m15"} else None

    # Проверка наличия точки ровно на open_time у всех ключей
    keys_now = [adx_key, ema_key, macd_key, bb_u, bb_l, bb_c] + ([atr_key] if atr_key else [])
    now_calls = [redis.execute_command("TS.RANGE", k, open_time_ms, open_time_ms) for k in keys_now]
    now_results = await asyncio.gather(*now_calls, return_exceptions=True)
    for k, r in zip(keys_now, now_results):
        if isinstance(r, Exception) or not r or int(r[0][0]) != open_time_ms:
            logging.getLogger("MRW").debug(f"[INCOMPLETE] {symbol}/{tf} @ {open_time_ms} → missing {k}")
            return None

    # Окна
    step_ms = _tf_step_ms(tf)
    t_prev = open_time_ms - step_ms
    t_start_pct = open_time_ms - (N_PCT - 1) * step_ms
    t_start_acc = open_time_ms - N_ACC * step_ms

    window_calls = [
        redis.execute_command("TS.RANGE", ema_key, t_prev, open_time_ms),        # 2 точки
        redis.execute_command("TS.RANGE", macd_key, t_start_acc, open_time_ms),  # N_ACC+1
        redis.execute_command("TS.RANGE", adx_key, t_start_pct, open_time_ms),   # N_PCT
        redis.execute_command("TS.RANGE", bb_u,   t_start_pct, open_time_ms),
        redis.execute_command("TS.RANGE", bb_l,   t_start_pct, open_time_ms),
        redis.execute_command("TS.RANGE", bb_c,   t_start_pct, open_time_ms),
    ]
    if atr_key:
        window_calls.append(redis.execute_command("TS.RANGE", atr_key, t_start_pct, open_time_ms))

    out = await asyncio.gather(*window_calls, return_exceptions=True)

    def _vals(series):
        return [float(v) for _, v in series] if series and not isinstance(series, Exception) else []

    ema_vals  = _vals(out[0])
    macd_vals = _vals(out[1])
    adx_vals  = _vals(out[2])
    bbu_vals  = _vals(out[3])
    bbl_vals  = _vals(out[4])
    bbc_vals  = _vals(out[5])
    atr_vals  = _vals(out[6]) if (atr_key and len(out) > 6) else None

    logging.getLogger("MRW").debug(
        f"[FEATURES] {symbol}/{tf} @ {open_time_ms} → "
        f"ema={len(ema_vals)} macd={len(macd_vals)} adx={len(adx_vals)} "
        f"bb(u/l/c)={[len(bbu_vals), len(bbl_vals), len(bbc_vals)]} "
        f"atr={'-' if atr_vals is None else len(atr_vals)}"
    )

    return {
        "ema_vals": ema_vals,          # [t-1, t]
        "macd_vals": macd_vals,        # N_ACC+1
        "adx_vals": adx_vals,          # N_PCT
        "bb_u": bbu_vals,              # N_PCT
        "bb_l": bbl_vals,              # N_PCT
        "bb_c": bbc_vals,              # N_PCT
        "atr_vals": atr_vals,          # N_PCT (только m5/m15) или None
    }


# 🔸 Подготовка features для ядра v2
def _build_features(tf: str, feats: dict) -> dict:
    ema_vals = feats["ema_vals"]
    macd_vals = feats["macd_vals"]
    adx_vals = feats["adx_vals"]
    bb_u = feats["bb_u"]
    bb_l = feats["bb_l"]
    bb_c = feats["bb_c"]
    atr_vals = feats.get("atr_vals")

    # ΔMACD окно (разности соседних значений)
    if len(macd_vals) >= 2:
        dhist = [macd_vals[i + 1] - macd_vals[i] for i in range(len(macd_vals) - 1)]
    else:
        dhist = []

    return {
        "ema_t1": ema_vals[-2],
        "ema_t":  ema_vals[-1],
        "macd_t1": macd_vals[-2],
        "macd_t":  macd_vals[-1],
        "dhist_win": dhist[-N_ACC:],         # окно ΔMACD
        "adx_win":   adx_vals[-N_PCT:],      # окно ADX
        "bb_u_win":  bb_u[-N_PCT:],
        "bb_l_win":  bb_l[-N_PCT:],
        "bb_c_win":  bb_c[-N_PCT:],
        "atr_t": (atr_vals[-1] if (atr_vals and tf in {"m5", "m15"}) else None),
        "atr_win": (atr_vals[-N_PCT:] if (atr_vals and tf in {"m5", "m15"}) else None),
    }


# 🔸 Запись результата: Redis KV/TS + PostgreSQL (indicator_marketwatcher_v4)
async def publish_regime(redis, pg, symbol: str, tf: str, open_time_ms: int, code: int, diag: dict):
    open_time_iso = datetime.utcfromtimestamp(open_time_ms / 1000).isoformat()

    # Пишем KV и TS параллельно
    kv_key = f"ind:{symbol}:{tf}:{REGIME_PARAM}"
    ts_key = f"ts_ind:{symbol}:{tf}:{REGIME_PARAM}"

    kv = redis.set(kv_key, str(code))
    ts = redis.execute_command(
        "TS.ADD", ts_key, open_time_ms, str(code),
        "RETENTION", RETENTION_TS_MS,
        "DUPLICATE_POLICY", "last"
    )

    # Запись в PG (upsert) в indicator_marketwatcher_v4
    async with pg.acquire() as conn:
        await conn.execute("""
            INSERT INTO indicator_marketwatcher_v4
              (symbol, timeframe, open_time, regime_code, version_id,
               adx, adx_low, adx_high,
               bb_width, bb_low, bb_high,
               atr, atr_low, atr_high,
               ema_slope, macd_hist, d_hist, z_d_hist)
            VALUES
              ($1, $2, $3, $4, $5,
               $6, $7, $8,
               $9, $10, $11,
               $12, $13, $14,
               $15, $16, $17, $18)
            ON CONFLICT (symbol, timeframe, open_time)
            DO UPDATE SET
              regime_code = EXCLUDED.regime_code,
              version_id  = EXCLUDED.version_id,
              adx         = EXCLUDED.adx,
              adx_low     = EXCLUDED.adx_low,
              adx_high    = EXCLUDED.adx_high,
              bb_width    = EXCLUDED.bb_width,
              bb_low      = EXCLUDED.bb_low,
              bb_high     = EXCLUDED.bb_high,
              atr         = EXCLUDED.atr,
              atr_low     = EXCLUDED.atr_low,
              atr_high    = EXCLUDED.atr_high,
              ema_slope   = EXCLUDED.ema_slope,
              macd_hist   = EXCLUDED.macd_hist,
              d_hist      = EXCLUDED.d_hist,
              z_d_hist    = EXCLUDED.z_d_hist,
              updated_at  = NOW()
        """,
        symbol, tf, datetime.fromisoformat(open_time_iso), code, REGIME_VERSION,
        diag.get("adx"), diag.get("adx_low"), diag.get("adx_high"),
        diag.get("bb_width"), diag.get("bb_low"), diag.get("bb_high"),
        diag.get("atr"), diag.get("atr_low"), diag.get("atr_high"),
        diag.get("ema_slope"), diag.get("macd_hist"), diag.get("d_hist"), diag.get("z_d_hist"))

    await asyncio.gather(kv, ts, return_exceptions=True)


# 🔸 Обработка бакета: debounce → фичи → ядро v2 (гистерезис) → запись
async def handle_bucket(symbol: str, tf: str, open_time_ms: int, redis, pg):
    log = logging.getLogger("MRW")
    await asyncio.sleep(DEBOUNCE_MS / 1000)

    for attempt in range(2):
        feats_raw = await fetch_features_for_bar(redis, symbol, tf, open_time_ms)
        if feats_raw is None:
            await asyncio.sleep(0.15)
            continue

        # Загружаем стейт гистерезиса из Redis
        state = await _load_state(redis, symbol, tf)

        # Готовим features для ядра и принимаем решение
        features = _build_features(tf, feats_raw)
        code, new_state, diag = decide_regime_code(
            tf,
            features,
            state,
            RegimeParams(hyst_trend_bars=HYST_TREND_BARS, hyst_sub_bars=HYST_SUB_BARS, eps_z=EPS_Z)
        )

        # Сохраняем стейт, публикуем результат
        await _save_state(redis, symbol, tf, new_state)
        await publish_regime(redis, pg, symbol, tf, open_time_ms, code, diag)

        log.debug(
            f"[REGIME] {symbol}/{tf} @ {open_time_ms} → code={code} "
            f"(adx={diag['adx']:.2f}/{diag['adx_low']:.2f}-{diag['adx_high']:.2f}, "
            f"bbw={diag['bb_width']:.4f}/{diag['bb_low']:.4f}-{diag['bb_high']:.4f}, "
            f"zΔ={diag['z_d_hist']:.2f})"
        )
        return

    log.debug(f"[SKIP] features incomplete: {symbol}/{tf} @ {open_time_ms}")


# 🔸 Основной цикл компонента (XREADGROUP)
async def run_market_watcher(pg, redis):
    log = logging.getLogger("MRW")
    log.debug("market_watcher starting: XGROUP init")

    try:
        await redis.xgroup_create(READY_STREAM, GROUP_NAME, id="$", mkstream=True)
        log.debug(f"consumer group '{GROUP_NAME}' created on '{READY_STREAM}'")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug(f"consumer group '{GROUP_NAME}' already exists")
        else:
            log.error(f"XGROUP CREATE error: {e}", exc_info=True)

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
                        symbol = data.get("symbol")
                        tf = data.get("timeframe") or data.get("interval")
                        status = data.get("status")
                        open_time_iso = data.get("open_time")

                        if not symbol or tf not in REQUIRED_TFS or status != "ready" or not open_time_iso:
                            to_ack.append(msg_id)
                            continue

                        open_time_ms = _iso_to_ms(open_time_iso)
                        bucket = (symbol, tf, open_time_ms)

                        if bucket in bucket_tasks and not bucket_tasks[bucket].done():
                            to_ack.append(msg_id)
                            continue

                        if symbol not in symbol_semaphores:
                            symbol_semaphores[symbol] = asyncio.Semaphore(MAX_PER_SYMBOL)

                        log.debug(f"[READY] {symbol}/{tf} @ {open_time_iso} → schedule bucket")

                        async def bucket_runner():
                            async with task_gate:
                                async with symbol_semaphores[symbol]:
                                    await handle_bucket(symbol, tf, open_time_ms, redis, pg)

                        bucket_tasks[bucket] = asyncio.create_task(bucket_runner())
                        to_ack.append(msg_id)

                    except Exception as parse_err:
                        to_ack.append(msg_id)
                        log.error(f"message parse error: {parse_err}", exc_info=True)

            if to_ack:
                await redis.xack(READY_STREAM, GROUP_NAME, *to_ack)

        except Exception as e:
            log.error(f"XREADGROUP loop error: {e}", exc_info=True)
            await asyncio.sleep(1)