# indicator_mw_trend.py — воркер расчёта рыночного условия Trend (up/down/sideways + strong/weak) с TS-барьером и «смягчённым» голосованием

import asyncio
import json
import logging
from datetime import datetime, timedelta

# 🔸 Константы и настройки
STREAM_READY = "indicator_stream"          # вход: готовность инстансов (из compute_and_store)
GROUP       = "mw_trend_group"
CONSUMER    = "mw_trend_1"

# без «partial»: считаем только когда все точки есть в TS; если долго нет — фиксируем gap и выходим
BARRIER_FAST_POLL_MS = 300                 # быстрый поллинг TS (первые N секунд)
BARRIER_FAST_SECONDS = 15
BARRIER_SLOW_POLL_MS = 1200                # медленный поллинг TS после FAST-окна
BARRIER_MAX_WAIT_SEC = 90                  # максимум ожидания полного комплекта (≈ один бар m5)

# таймшаги TF (мс)
STEP_MS = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}

# ожидаемые базы (без суффиксов)
EXPECTED_BASES = {"ema21", "ema50", "ema200", "lr50", "lr100", "adx_dmi14", "adx_dmi21"}

# префиксы Redis
TS_IND_PREFIX = "ts_ind"   # ts_ind:{symbol}:{tf}:{param}
BB_TS_PREFIX  = "bb:ts"    # bb:ts:{symbol}:{tf}:c
KV_MW_PREFIX  = "ind_mw"   # ind_mw:{symbol}:{tf}:{kind}

# «сила тренда»
ADX_STRONG_LEVEL    = 25.0                  # базовый порог strong
ADX_SIDEWAYS_LEVEL  = 12.0                  # если ниже — считаем sideways независимо от голосов

# «острота» силы (дельты последнего бара)
ADX_DROP_EPS        = {"m5": 0.5, "m15": 0.7, "h1": 1.0}
EMA_DIST_DROP_EPS   = {"m5": 0.15, "m15": 0.20, "h1": 0.30}   # п.п. (|dist| уменьшается)
LR_FLATTEN_ALLOW    = {"m5": 0.0, "m15": 0.0, "h1": 0.0}      # Δугла <= 0 → сглаживание

# deadband’ы для «смягчённого» голосования (эмитируем больше флэта)
EMA_EQ_EPS_PCT      = {"m5": 0.05, "m15": 0.07, "h1": 0.10}   # |price-EMA|/EMA*100 <= eps → нейтрально
ANGLE_EPS           = {"m5": 1e-4, "m15": 8e-4, "h1": 2e-3}   # |угол| <= eps → нейтрально

# лимит параллелизма на ключи (symbol, tf, open_time)
MAX_CONCURRENCY = 30

# 🔸 Логгер
log = logging.getLogger("MW_TREND")


# 🔸 Утилиты времени
def iso_to_ms(iso: str) -> int:
    dt = datetime.fromisoformat(iso)
    return int(dt.timestamp() * 1000)

def ms_to_iso(ms: int) -> str:
    return datetime.utcfromtimestamp(ms / 1000).isoformat()

def prev_bar_ms(open_ms: int, tf: str) -> int:
    return open_ms - STEP_MS[tf]


# 🔸 Чтение одной точки TS по exact open_time (from=to)
async def ts_get_at(redis, key: str, ts_ms: int):
    try:
        res = await redis.execute_command("TS.RANGE", key, ts_ms, ts_ms)
        if res:
            return float(res[0][1])
    except Exception as e:
        log.warning(f"[TS] read error {key}@{ts_ms}: {e}")
    return None


# 🔸 Проверка полноты набора значений в TS на open_time
async def ts_has_all(redis, symbol: str, tf: str, open_ms: int) -> bool:
    keys = [
        f"{TS_IND_PREFIX}:{symbol}:{tf}:ema21",
        f"{TS_IND_PREFIX}:{symbol}:{tf}:ema50",
        f"{TS_IND_PREFIX}:{symbol}:{tf}:ema200",
        f"{TS_IND_PREFIX}:{symbol}:{tf}:lr50_angle",
        f"{TS_IND_PREFIX}:{symbol}:{tf}:lr100_angle",
        f"{TS_IND_PREFIX}:{symbol}:{tf}:adx_dmi14_adx",
        f"{TS_IND_PREFIX}:{symbol}:{tf}:adx_dmi21_adx",
        f"{BB_TS_PREFIX}:{symbol}:{tf}:c",
    ]
    tasks = [ts_get_at(redis, k, open_ms) for k in keys]
    vals = await asyncio.gather(*tasks, return_exceptions=False)
    # условия достаточности
    return all(v is not None for v in vals)


# 🔸 Ожидание полного комплекта TS-точек (TS-барьер)
async def wait_for_ts_barrier(redis, symbol: str, tf: str, open_ms: int) -> bool:
    deadline = datetime.utcnow() + timedelta(seconds=BARRIER_MAX_WAIT_SEC)
    # быстрый поллинг
    fast_until = datetime.utcnow() + timedelta(seconds=BARRIER_FAST_SECONDS)
    while datetime.utcnow() < deadline:
        ok = await ts_has_all(redis, symbol, tf, open_ms)
        if ok:
            return True
        # период поллинга
        now = datetime.utcnow()
        interval_ms = BARRIER_FAST_POLL_MS if now < fast_until else BARRIER_SLOW_POLL_MS
        await asyncio.sleep(interval_ms / 1000.0)
    return False


# 🔸 Загрузка значений на два бара (cur/prev) из TS
async def load_trend_inputs(redis, symbol: str, tf: str, open_ms: int) -> dict:
    prev_ms = prev_bar_ms(open_ms, tf)
    keys = {
        "ema21":       f"{TS_IND_PREFIX}:{symbol}:{tf}:ema21",
        "ema50":       f"{TS_IND_PREFIX}:{symbol}:{tf}:ema50",
        "ema200":      f"{TS_IND_PREFIX}:{symbol}:{tf}:ema200",
        "lr50_angle":  f"{TS_IND_PREFIX}:{symbol}:{tf}:lr50_angle",
        "lr100_angle": f"{TS_IND_PREFIX}:{symbol}:{tf}:lr100_angle",
        "adx14":       f"{TS_IND_PREFIX}:{symbol}:{tf}:adx_dmi14_adx",
        "adx21":       f"{TS_IND_PREFIX}:{symbol}:{tf}:adx_dmi21_adx",
        "close":       f"{BB_TS_PREFIX}:{symbol}:{tf}:c",
    }

    async def read_pair(key: str):
        cur = await ts_get_at(redis, key, open_ms)
        prev = await ts_get_at(redis, key, prev_ms)
        return cur, prev

    tasks = {name: read_pair(key) for name, key in keys.items()}
    res = await asyncio.gather(*tasks.values(), return_exceptions=False)

    out = {}
    for (name, _), (cur, prev) in zip(tasks.items(), res):
        out[name] = {"cur": cur, "prev": prev}
    out["open_ms"] = open_ms
    out["prev_ms"] = prev_ms
    return out


# 🔸 «Смягчённое» голосование направления по текущему бару
def infer_direction_soft(tf: str,
                         price: float | None,
                         ema21: float | None, ema50: float | None, ema200: float | None,
                         ang50: float | None, ang100: float | None) -> str:
    up_ema = 0
    down_ema = 0
    up_lr = 0
    down_lr = 0
    # deadband по EMA
    eps_pct = EMA_EQ_EPS_PCT.get(tf, 0.05)

    def vote_ema(price, ema):
        if price is None or ema is None or ema == 0:
            return 0, 0
        dist_pct = (price - ema) / ema * 100.0
        if abs(dist_pct) <= eps_pct:
            return 0, 0
        return (1, 0) if dist_pct > 0 else (0, 1)

    u, d = vote_ema(price, ema21); up_ema += u; down_ema += d
    u, d = vote_ema(price, ema50); up_ema += u; down_ema += d
    u, d = vote_ema(price, ema200); up_ema += u; down_ema += d

    # deadband по углам LR
    aeps = ANGLE_EPS.get(tf, 1e-4)

    def vote_angle(angle):
        if angle is None:
            return 0, 0
        if abs(angle) <= aeps:
            return 0, 0
        return (1, 0) if angle > 0 else (0, 1)

    u, d = vote_angle(ang50);  up_lr += u;  down_lr += d
    u, d = vote_angle(ang100); up_lr += u;  down_lr += d

    # правило согласованности:
    # up → минимум 2 из 3 EMA в плюс И минимум 1 LR в плюс
    # down → минимум 2 из 3 EMA в минус И минимум 1 LR в минус
    if up_ema >= 2 and up_lr >= 1:
        return "up"
    if down_ema >= 2 and down_lr >= 1:
        return "down"
    return "sideways"


# 🔸 Базовая сила по уровню ADX
def base_strength_now(adx14: float | None, adx21: float | None) -> tuple[bool, float]:
    vals = [v for v in (adx14, adx21) if v is not None]
    if not vals:
        return False, 0.0
    max_adx = max(vals)
    return max_adx >= ADX_STRONG_LEVEL, max_adx


# 🔸 Коррекция силы по дельтам (ослабление strong → weak)
def weaken_by_deltas(tf: str,
                     adx14_cur: float | None, adx14_prev: float | None,
                     adx21_cur: float | None, adx21_prev: float | None,
                     ema50_cur: float | None, ema50_prev: float | None,
                     close_cur: float | None, close_prev: float | None,
                     ang50_cur: float | None, ang50_prev: float | None,
                     ang100_cur: float | None, ang100_prev: float | None) -> dict:
    adx_drop_eps = ADX_DROP_EPS.get(tf, 0.7)
    ema_drop_eps = EMA_DIST_DROP_EPS.get(tf, 0.2)
    lr_flat_allow = LR_FLATTEN_ALLOW.get(tf, 0.0)

    max_adx_cur = max([v for v in (adx14_cur, adx21_cur) if v is not None], default=None)
    max_adx_prev = max([v for v in (adx14_prev, adx21_prev) if v is not None], default=None)

    d_adx = None
    adx_is_falling = False
    if max_adx_cur is not None and max_adx_prev is not None:
        d_adx = max_adx_cur - max_adx_prev
        adx_is_falling = (d_adx <= -adx_drop_eps)

    d_abs_dist = None
    abs_dist_is_shrinking = False
    if (ema50_cur is not None and ema50_cur != 0 and close_cur is not None and
        ema50_prev is not None and ema50_prev != 0 and close_prev is not None):
        dist_cur = abs((close_cur - ema50_cur) / ema50_cur) * 100.0
        dist_prev = abs((close_prev - ema50_prev) / ema50_prev) * 100.0
        d_abs_dist = dist_cur - dist_prev
        abs_dist_is_shrinking = (d_abs_dist <= -ema_drop_eps)

    d_ang50 = (ang50_cur - ang50_prev) if (ang50_cur is not None and ang50_prev is not None) else None
    d_ang100 = (ang100_cur - ang100_prev) if (ang100_cur is not None and ang100_prev is not None) else None
    lr_is_flatten = False
    conds = []
    if d_ang50 is not None:  conds.append(d_ang50 <= lr_flat_allow)
    if d_ang100 is not None: conds.append(d_ang100 <= lr_flat_allow)
    if conds: lr_is_flatten = all(conds)

    weaken = adx_is_falling or abs_dist_is_shrinking or lr_is_flatten

    # округлим для деталей
    def r2(x): return None if x is None else round(float(x), 2)
    def r5(x): return None if x is None else round(float(x), 5)

    return {
        "weaken": weaken,
        "d_adx": r2(d_adx),
        "d_abs_dist_pct": r2(d_abs_dist),
        "d_lr50_angle": r5(d_ang50),
        "d_lr100_angle": r5(d_ang100),
        "flags": {
            "adx_is_falling": adx_is_falling,
            "abs_dist_is_shrinking": abs_dist_is_shrinking,
            "lr_is_flatten": lr_is_flatten,
        }
    }


# 🔸 Запись результата в Redis KV и PostgreSQL
async def persist_result(pg, redis, symbol: str, tf: str, open_time_iso: str,
                         state: str, direction: str, strong: bool,
                         status: str, used_bases: list[str], missing_bases: list[str],
                         extras: dict | None = None,
                         source: str = "live", version: int = 1):
    # KV
    kv_key = f"{KV_MW_PREFIX}:{symbol}:{tf}:trend"
    payload = {
        "state": state,
        "direction": direction,
        "strong": bool(strong),
        "status": status,
        "version": version,
        "open_time": open_time_iso,
        "computed_at": datetime.utcnow().isoformat(),
        "details": {"used_bases": used_bases, "missing_bases": missing_bases, **(extras or {})},
    }
    try:
        await redis.set(kv_key, json.dumps(payload))
    except Exception as e:
        log.warning(f"[KV] set error {kv_key}: {e}")

    # PG upsert
    details = {
        "direction": direction,
        "strong": strong,
        "used_bases": used_bases,
        "missing_bases": missing_bases,
        "open_time_iso": open_time_iso,
        **(extras or {}),
    }
    try:
        async with pg.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO indicator_marketwatch_values
                  (symbol, timeframe, open_time, kind, state, status, details, version, source, computed_at, updated_at)
                VALUES ($1,$2,$3,'trend',$4,$5,$6,$7,$8,NOW(),NOW())
                ON CONFLICT (symbol, timeframe, open_time, kind)
                DO UPDATE SET
                  state = EXCLUDED.state,
                  status = EXCLUDED.status,
                  details = EXCLUDED.details,
                  version = EXCLUDED.version,
                  source = EXCLUDED.source,
                  updated_at = NOW()
                """,
                symbol, tf, datetime.fromisoformat(open_time_iso),
                state, status, json.dumps(details), version, source
            )
    except Exception as e:
        log.error(f"[PG] upsert error trend {symbol}/{tf}@{open_time_iso}: {e}")


# 🔸 Фиксация gap (если TS-комплект так и не собрался)
async def mark_gap(pg, symbol: str, tf: str, open_time_iso: str, missing_bases: list[str]):
    try:
        async with pg.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO trend_calc_gap (symbol, timeframe, open_time, missing_bases, status, detected_at)
                VALUES ($1,$2,$3,$4,'found',NOW())
                ON CONFLICT (symbol, timeframe, open_time)
                DO UPDATE SET
                  missing_bases = EXCLUDED.missing_bases,
                  status = 'found',
                  attempts = trend_calc_gap.attempts + 1,
                  detected_at = NOW()
                """,
                symbol, tf, datetime.fromisoformat(open_time_iso), json.dumps(missing_bases)
            )
    except Exception as e:
        log.warning(f"[GAP] insert error {symbol}/{tf}@{open_time_iso}: {e}")


# 🔸 Расчёт Trend по ключу (symbol, tf, open_time) — с учётом TS-барьера и дельт
async def compute_trend_for_bar(pg, redis, symbol: str, tf: str, open_iso: str):
    open_ms = iso_to_ms(open_iso)

    # ждём полный комплект в TS (без partial)
    ready = await wait_for_ts_barrier(redis, symbol, tf, open_ms)
    if not ready:
        # выясним, чего не хватает — пробежимся по ключам
        req = {
            "ema21":     f"{TS_IND_PREFIX}:{symbol}:{tf}:ema21",
            "ema50":     f"{TS_IND_PREFIX}:{symbol}:{tf}:ema50",
            "ema200":    f"{TS_IND_PREFIX}:{symbol}:{tf}:ema200",
            "lr50":      f"{TS_IND_PREFIX}:{symbol}:{tf}:lr50_angle",
            "lr100":     f"{TS_IND_PREFIX}:{symbol}:{tf}:lr100_angle",
            "adx_dmi14": f"{TS_IND_PREFIX}:{symbol}:{tf}:adx_dmi14_adx",
            "adx_dmi21": f"{TS_IND_PREFIX}:{symbol}:{tf}:adx_dmi21_adx",
            "close":     f"{BB_TS_PREFIX}:{symbol}:{tf}:c",
        }
        missing = []
        for base, key in req.items():
            val = await ts_get_at(redis, key, open_ms)
            if val is None and base != "close":
                missing.append(base)
        await mark_gap(pg, symbol, tf, open_iso, sorted(missing))
        log.debug(f"MW_TREND GAP {symbol}/{tf}@{open_iso} missing={missing}")
        return

    # все точки есть — грузим пары cur/prev
    inputs = await load_trend_inputs(redis, symbol, tf, open_ms)

    # считаем дельты всегда (до ADX-гарда)
    deltas = weaken_by_deltas(
        tf,
        inputs["adx14"]["cur"], inputs["adx14"]["prev"],
        inputs["adx21"]["cur"], inputs["adx21"]["prev"],
        inputs["ema50"]["cur"], inputs["ema50"]["prev"],
        inputs["close"]["cur"], inputs["close"]["prev"],
        inputs["lr50_angle"]["cur"], inputs["lr50_angle"]["prev"],
        inputs["lr100_angle"]["cur"], inputs["lr100_angle"]["prev"],
    )

    # проверяем ADX guard → если слабый тренд, сразу sideways, но дельты логируем
    _, max_adx = base_strength_now(inputs["adx14"]["cur"], inputs["adx21"]["cur"])
    if max_adx < ADX_SIDEWAYS_LEVEL:
        state = "sideways"
        extras = {
            "deltas": {
                "d_adx": deltas["d_adx"],
                "d_abs_dist_pct": deltas["d_abs_dist_pct"],
                "d_lr50_angle": deltas["d_lr50_angle"],
                "d_lr100_angle": deltas["d_lr100_angle"],
                **deltas["flags"],
            },
            "max_adx": round(max_adx, 2),
        }
        await persist_result(
            pg, redis, symbol, tf, open_iso,
            state=state, direction="sideways", strong=False,
            status="ok", used_bases=sorted(EXPECTED_BASES), missing_bases=[],
            extras=extras, source="live", version=1
        )
        d_adx_str    = "n/a" if deltas["d_adx"] is None else f"{deltas['d_adx']:.2f}"
        d_abs_str    = "n/a" if deltas["d_abs_dist_pct"] is None else f"{deltas['d_abs_dist_pct']:.2f}"
        d_ang50_str  = "n/a" if deltas["d_lr50_angle"] is None else f"{deltas['d_lr50_angle']:.5f}"
        d_ang100_str = "n/a" if deltas["d_lr100_angle"] is None else f"{deltas['d_lr100_angle']:.5f}"
        log.info(
            f"MW_TREND OK {symbol}/{tf}@{open_iso} state=sideways (max_adx={max_adx:.2f}) "
            f"d_adx={d_adx_str} d_abs_dist={d_abs_str} d_ang50={d_ang50_str} d_ang100={d_ang100_str}"
        )
        return

    # направление по «мягкому» голосованию
    direction = infer_direction_soft(
        tf,
        inputs["close"]["cur"],
        inputs["ema21"]["cur"], inputs["ema50"]["cur"], inputs["ema200"]["cur"],
        inputs["lr50_angle"]["cur"], inputs["lr100_angle"]["cur"]
    )

    # сила: базово strong по уровню, затем ослабляем по дельтам
    strong, _ = base_strength_now(inputs["adx14"]["cur"], inputs["adx21"]["cur"])
    deltas = weaken_by_deltas(
        tf,
        inputs["adx14"]["cur"], inputs["adx14"]["prev"],
        inputs["adx21"]["cur"], inputs["adx21"]["prev"],
        inputs["ema50"]["cur"], inputs["ema50"]["prev"],
        inputs["close"]["cur"], inputs["close"]["prev"],
        inputs["lr50_angle"]["cur"], inputs["lr50_angle"]["prev"],
        inputs["lr100_angle"]["cur"], inputs["lr100_angle"]["prev"],
    )
    if strong and deltas["weaken"]:
        strong = False

    state = "sideways" if direction == "sideways" else f"{direction}_{'strong' if strong else 'weak'}"

    extras = {
        "deltas": {
            "d_adx": deltas["d_adx"],
            "d_abs_dist_pct": deltas["d_abs_dist_pct"],
            "d_lr50_angle": deltas["d_lr50_angle"],
            "d_lr100_angle": deltas["d_lr100_angle"],
            **deltas["flags"],
        }
    }

    await persist_result(
        pg, redis, symbol, tf, open_iso,
        state=state, direction=direction, strong=strong,
        status="ok", used_bases=sorted(EXPECTED_BASES), missing_bases=[],
        extras=extras, source="live", version=1
    )

    # лог красиво
    d_adx_str    = "n/a" if deltas["d_adx"] is None else f"{deltas['d_adx']:.2f}"
    d_abs_str    = "n/a" if deltas["d_abs_dist_pct"] is None else f"{deltas['d_abs_dist_pct']:.2f}"
    d_ang50_str  = "n/a" if deltas["d_lr50_angle"] is None else f"{deltas['d_lr50_angle']:.5f}"
    d_ang100_str = "n/a" if deltas["d_lr100_angle"] is None else f"{deltas['d_lr100_angle']:.5f}"
    log.debug(
        f"MW_TREND OK {symbol}/{tf}@{open_iso} state={state} "
        f"d_adx={d_adx_str} d_abs_dist={d_abs_str} d_ang50={d_ang50_str} d_ang100={d_ang100_str}"
    )


# 🔸 Основной воркер: слушает indicator_stream, запускает расчёт с TS-барьером
async def run_indicator_mw_trend(pg, redis):
    log.debug("MW_TREND: воркер запущен")

    # создаём consumer-group на indicator_stream
    try:
        await redis.xgroup_create(STREAM_READY, GROUP, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create error: {e}")

    # семафор на параллельность
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    # уже запущенные вычисления по ключам, чтобы не дублировать
    in_flight = set()

    while True:
        try:
            resp = await redis.xreadgroup(
                groupname=GROUP,
                consumername=CONSUMER,
                streams={STREAM_READY: ">"},
                count=200,
                block=1000
            )
            if not resp:
                continue

            to_ack = []
            tasks = []

            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)

                    try:
                        if data.get("status") != "ready":
                            continue

                        symbol   = data["symbol"]
                        tf       = data.get("timeframe") or data.get("interval")
                        base     = data["indicator"]              # 'ema21'|'lr50'|'adx_dmi14'|...
                        open_iso = data["open_time"]

                        # интересуют только наши базы
                        if base not in EXPECTED_BASES:
                            continue

                        key = (symbol, tf, open_iso)
                        if key in in_flight:
                            continue

                        # запускаем расчёт под семафором
                        async def _run(key_tuple):
                            sym, tff, iso = key_tuple
                            async with sem:
                                try:
                                    await compute_trend_for_bar(pg, redis, sym, tff, iso)
                                except Exception as e:
                                    log.error(f"MW_TREND compute error {sym}/{tff}@{iso}: {e}", exc_info=True)
                                finally:
                                    in_flight.discard(key_tuple)

                        in_flight.add(key)
                        tasks.append(asyncio.create_task(_run(key)))

                    except Exception as e:
                        log.warning(f"MW_TREND message error: {e}", exc_info=True)

            if to_ack:
                try:
                    await redis.xack(STREAM_READY, GROUP, *to_ack)
                except Exception as e:
                    log.warning(f"MW_TREND ack error: {e}")

            if tasks:
                # не ждём все до конца цикла — пусть бегут; но подчистим завершившиеся
                done, _ = await asyncio.wait(tasks, timeout=0, return_when=asyncio.FIRST_COMPLETED)
                for _t in done:
                    pass

        except Exception as e:
            log.error(f"MW_TREND loop error: {e}", exc_info=True)
            await asyncio.sleep(0.5)