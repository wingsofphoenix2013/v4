# indicator_mw_trend.py — воркер расчёта рыночного условия Trend (up/down/sideways + strong/weak) с учётом дельт последнего бара

import asyncio
import json
import logging
from datetime import datetime, timedelta

# 🔸 Константы и настройки
STREAM_READY = "indicator_stream"          # вход: готовность инстансов (из compute_and_store)
GROUP       = "mw_trend_group"
CONSUMER    = "mw_trend_1"

GRACE_SEC   = 90                           # ожидание всех баз для бара
CHECK_TICK  = 1.0                          # период внутреннего таймера (сек)
ANGLE_EPS   = 0.0                          # порог для LR angle (>=0 — up, <=0 — down)
ADX_STRONG  = 25.0                         # порог силы тренда по ADX (max из adx14/21)

# 🔸 Таймшаги TF (мс)
STEP_MS = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}

# 🔸 Ожидаемые базы для Trend (без суффиксов)
EXPECTED_BASES = {"ema21", "ema50", "ema200", "lr50", "lr100", "adx_dmi14", "adx_dmi21"}

# 🔸 Префиксы Redis
TS_IND_PREFIX = "ts_ind"   # ts_ind:{symbol}:{tf}:{param}
BB_TS_PREFIX  = "bb:ts"    # bb:ts:{symbol}:{tf}:c
KV_MW_PREFIX  = "ind_mw"   # ind_mw:{symbol}:{tf}:{kind}

# 🔸 Пороговые дельты (по TF) — «острота» силы
ADX_DROP_EPS        = {"m5": 0.5, "m15": 0.7, "h1": 1.0}    # если max(ADX) уменьшается больше eps → ослабляем
EMA_DIST_DROP_EPS   = {"m5": 0.15, "m15": 0.20, "h1": 0.30} # уменьшение |(Close-EMA50)/EMA50| в п.п. → ослабляем
LR_FLATTEN_ALLOW    = {"m5": 0.0, "m15": 0.0, "h1": 0.0}    # если Δугла <= 0 (не растёт) → ослабляем

# 🔸 Логгер
log = logging.getLogger("MW_TREND")


# 🔸 Утилиты времени/форматов
def iso_to_ms(iso: str) -> int:
    dt = datetime.fromisoformat(iso)
    return int(dt.timestamp() * 1000)

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


# 🔸 Сбор значений из Redis TS на два бара: текущий open_time и предыдущий
async def load_trend_inputs(redis, symbol: str, tf: str, open_ms: int) -> dict:
    # ключи TS (EMA/LR/ADX/Close)
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

    prev_ms = prev_bar_ms(open_ms, tf)

    # внутренняя помощь: прочитать два значения
    async def read_pair(key: str):
        # условия достаточности
        cur = await ts_get_at(redis, key, open_ms)
        prev = await ts_get_at(redis, key, prev_ms)
        return cur, prev

    # параллельный батч чтений
    tasks = {k: read_pair(key) for k, key in keys.items()}
    results = await asyncio.gather(*tasks.values(), return_exceptions=False)

    out = {}
    for (name, _), (cur, prev) in zip(tasks.items(), results):
        out[name] = {"cur": cur, "prev": prev}
    out["open_ms"] = open_ms
    out["prev_ms"] = prev_ms
    return out


# 🔸 Направление: EMA vs цена + LR углы (по текущему бару)
def infer_direction_now(price: float | None,
                        ema21: float | None, ema50: float | None, ema200: float | None,
                        ang50: float | None, ang100: float | None) -> str:
    up_votes = 0
    down_votes = 0

    # условия достаточности
    if price is not None and ema21 is not None:
        if price > ema21: up_votes += 1
        elif price < ema21: down_votes += 1
    if price is not None and ema50 is not None:
        if price > ema50: up_votes += 1
        elif price < ema50: down_votes += 1
    if price is not None and ema200 is not None:
        if price > ema200: up_votes += 1
        elif price < ema200: down_votes += 1

    if ang50 is not None:
        if ang50 > ANGLE_EPS: up_votes += 1
        elif ang50 < -ANGLE_EPS: down_votes += 1
    if ang100 is not None:
        if ang100 > ANGLE_EPS: up_votes += 1
        elif ang100 < -ANGLE_EPS: down_votes += 1

    # решение
    if up_votes >= 3 and up_votes > down_votes:
        return "up"
    if down_votes >= 3 and down_votes > up_votes:
        return "down"
    return "sideways"


# 🔸 Сила: базово по уровню ADX на текущем баре
def base_strength_now(adx14: float | None, adx21: float | None) -> bool:
    vals = [v for v in (adx14, adx21) if v is not None]
    if not vals:
        return False
    return max(vals) >= ADX_STRONG


# 🔸 Коррекция силы по «динамике» (ослабление strong → weak)
def weaken_by_deltas(tf: str,
                     adx14_cur: float | None, adx14_prev: float | None,
                     adx21_cur: float | None, adx21_prev: float | None,
                     ema50_cur: float | None, ema50_prev: float | None,
                     close_cur: float | None, close_prev: float | None,
                     ang50_cur: float | None, ang50_prev: float | None,
                     ang100_cur: float | None, ang100_prev: float | None) -> dict:
    # условия достаточности
    adx_drop_eps = ADX_DROP_EPS.get(tf, 0.7)
    ema_drop_eps = EMA_DIST_DROP_EPS.get(tf, 0.2)
    lr_flat_allow = LR_FLATTEN_ALLOW.get(tf, 0.0)

    # ΔADX (берём максимум из двух длин)
    adx_cur_vals = [v for v in (adx14_cur, adx21_cur) if v is not None]
    adx_prev_vals = [v for v in (adx14_prev, adx21_prev) if v is not None]
    max_adx_cur = max(adx_cur_vals) if adx_cur_vals else None
    max_adx_prev = max(adx_prev_vals) if adx_prev_vals else None
    d_adx = None
    adx_is_falling = False
    if max_adx_cur is not None and max_adx_prev is not None:
        d_adx = max_adx_cur - max_adx_prev
        adx_is_falling = (d_adx <= -adx_drop_eps)

    # Δ|dist to EMA50| (% пункты)
    d_abs_dist = None
    abs_dist_is_shrinking = False
    if (ema50_cur is not None and ema50_cur != 0 and close_cur is not None and
        ema50_prev is not None and ema50_prev != 0 and close_prev is not None):
        dist_cur = abs((close_cur - ema50_cur) / ema50_cur) * 100.0
        dist_prev = abs((close_prev - ema50_prev) / ema50_prev) * 100.0
        d_abs_dist = dist_cur - dist_prev
        abs_dist_is_shrinking = (d_abs_dist <= -ema_drop_eps)

    # Δуглов LR (если не растут → считаем «сглаживание/флэт»)
    d_ang50 = None
    d_ang100 = None
    lr_is_flatten = False
    if ang50_cur is not None and ang50_prev is not None:
        d_ang50 = ang50_cur - ang50_prev
    if ang100_cur is not None and ang100_prev is not None:
        d_ang100 = ang100_cur - ang100_prev
    # если оба определены — используем «и», если один — по одному
    conds = []
    if d_ang50 is not None:
        conds.append(d_ang50 <= lr_flat_allow)
    if d_ang100 is not None:
        conds.append(d_ang100 <= lr_flat_allow)
    if conds:
        lr_is_flatten = all(conds)

    # итоговый флаг «ослабить strong»
    weaken = adx_is_falling or abs_dist_is_shrinking or lr_is_flatten

    # отдадим детали для логирования/деталей
    return {
        "weaken": weaken,
        "d_adx": d_adx,
        "d_abs_dist_pct": d_abs_dist,
        "d_lr50_angle": d_ang50,
        "d_lr100_angle": d_ang100,
        "flags": {
            "adx_is_falling": adx_is_falling,
            "abs_dist_is_shrinking": abs_dist_is_shrinking,
            "lr_is_flatten": lr_is_flatten,
        }
    }


# 🔸 Запись результата в Redis KV и в PostgreSQL
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


# 🔸 Запись пропуска в аудит-таблицу (trend_calc_gap)
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


# 🔸 Основной воркер: агрегирует готовности и считает Trend
async def run_indicator_mw_trend(pg, redis):
    log.debug("MW_TREND: воркер запущен")

    # создать consumer-group на indicator_stream
    try:
        await redis.xgroup_create(STREAM_READY, GROUP, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create error: {e}")

    # in-memory агрегатор по ключу (symbol, timeframe, open_time_iso)
    pending = {}

    # внутренний таймер для таймаутов
    async def check_timeouts():
        # ищем просроченные ключи и закрываем их как partial
        now = datetime.utcnow()
        expired = []
        for k, obj in pending.items():
            if now >= obj["deadline"]:
                expired.append((k, obj))
        for (k, obj) in expired:
            symbol, tf, open_time_iso = k
            missing = sorted(list(obj["expected"] - obj["arrived"]))

            open_ms = iso_to_ms(open_time_iso)
            inputs = await load_trend_inputs(redis, symbol, tf, open_ms)

            # направление по текущему бару
            direction = infer_direction_now(
                inputs["close"]["cur"],
                inputs["ema21"]["cur"], inputs["ema50"]["cur"], inputs["ema200"]["cur"],
                inputs["lr50_angle"]["cur"], inputs["lr100_angle"]["cur"]
            )
            strong = base_strength_now(inputs["adx14"]["cur"], inputs["adx21"]["cur"])

            # коррекция силы по дельтам
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

            state = (
                "sideways" if direction == "sideways"
                else f"{direction}_{'strong' if strong else 'weak'}"
            )

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
                pg, redis, symbol, tf, open_time_iso,
                state=state, direction=direction, strong=strong,
                status="partial", used_bases=sorted(list(obj["arrived"])), missing_bases=missing,
                extras=extras, source="live", version=1
            )
            await mark_gap(pg, symbol, tf, open_time_iso, missing)
            log.info(
                f"MW_TREND PARTIAL {symbol}/{tf}@{open_time_iso} "
                f"arrived={len(obj['arrived'])}/{len(obj['expected'])} state={state}"
            )
            pending.pop(k, None)

    # основной цикл
    while True:
        try:
            # читаем пачкой готовности
            resp = await redis.xreadgroup(
                groupname=GROUP,
                consumername=CONSUMER,
                streams={STREAM_READY: ">"},
                count=200,
                block=int(CHECK_TICK * 1000)
            )

            # проверяем таймауты независимо от приходов
            await check_timeouts()

            if not resp:
                continue

            to_ack = []
            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)

                    try:
                        if data.get("status") != "ready":
                            continue

                        symbol = data["symbol"]
                        tf     = data.get("timeframe") or data.get("interval")
                        base   = data["indicator"]              # 'ema21', 'lr50', 'adx_dmi14', ...
                        open_iso = data["open_time"]

                        # интересуют только нужные базы
                        if base not in EXPECTED_BASES:
                            continue

                        key = (symbol, tf, open_iso)
                        rec = pending.get(key)
                        if rec is None:
                            # создаём запись ожидания
                            rec = {
                                "expected": set(EXPECTED_BASES),
                                "arrived":  set(),
                                "deadline": datetime.utcnow() + timedelta(seconds=GRACE_SEC),
                            }
                            pending[key] = rec

                        # отметить пришедшую базу
                        rec["arrived"].add(base)

                        # если всё готово — считаем немедленно
                        if rec["arrived"] == rec["expected"]:
                            open_ms = iso_to_ms(open_iso)
                            inputs = await load_trend_inputs(redis, symbol, tf, open_ms)

                            # направление по текущему бару
                            direction = infer_direction_now(
                                inputs["close"]["cur"],
                                inputs["ema21"]["cur"], inputs["ema50"]["cur"], inputs["ema200"]["cur"],
                                inputs["lr50_angle"]["cur"], inputs["lr100_angle"]["cur"]
                            )
                            strong = base_strength_now(inputs["adx14"]["cur"], inputs["adx21"]["cur"])

                            # коррекция силы по дельтам
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

                            state = (
                                "sideways" if direction == "sideways"
                                else f"{direction}_{'strong' if strong else 'weak'}"
                            )

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
                                status="ok", used_bases=sorted(list(rec["arrived"])), missing_bases=[],
                                extras=extras, source="live", version=1
                            )

                            # подготовка строк для логов
                            d_adx_str    = f"{deltas['d_adx']:.2f}" if deltas["d_adx"] is not None else "n/a"
                            d_abs_str    = f"{deltas['d_abs_dist_pct']:.2f}" if deltas["d_abs_dist_pct"] is not None else "n/a"
                            d_ang50_str  = f"{deltas['d_lr50_angle']:.5f}" if deltas["d_lr50_angle"] is not None else "n/a"
                            d_ang100_str = f"{deltas['d_lr100_angle']:.5f}" if deltas["d_lr100_angle"] is not None else "n/a"

                            log.info(
                                f"MW_TREND OK {symbol}/{tf}@{open_iso} state={state} "
                                f"d_adx={d_adx_str} d_abs_dist={d_abs_str} "
                                f"d_ang50={d_ang50_str} d_ang100={d_ang100_str}"
                            )

                            pending.pop(key, None)

                    except Exception as e:
                        log.warning(f"MW_TREND message error: {e}", exc_info=True)

            if to_ack:
                try:
                    await redis.xack(STREAM_READY, GROUP, *to_ack)
                except Exception as e:
                    log.warning(f"MW_TREND ack error: {e}")

        except Exception as e:
            log.error(f"MW_TREND loop error: {e}", exc_info=True)
            await asyncio.sleep(0.5)