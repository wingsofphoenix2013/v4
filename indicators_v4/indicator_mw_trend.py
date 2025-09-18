# indicator_mw_trend.py — воркер расчёта рыночного условия Trend (up/down/sideways + strong/weak)

import asyncio
import json
import logging
from datetime import datetime, timedelta

# 🔸 Константы и настройки
STREAM_READY = "indicator_stream"          # вход: готовность инстансов (из compute_and_store)
GROUP       = "mw_trend_group"
CONSUMER    = "mw_trend_1"

GRACE_SEC   = 60                           # окно ожидания всех баз для бара
CHECK_TICK  = 1.0                          # период внутреннего таймера (сек)
ANGLE_EPS   = 0.0                          # порог для LR angle (>=0 — up, <=0 — down)
ADX_STRONG  = 25.0                         # порог силы тренда по ADX (max из adx14/21)

# 🔸 Шаги TF в миллисекундах (для выравнивания и удобства)
STEP_MS = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}

# 🔸 Ожидаемые базы для Trend (без суффиксов)
EXPECTED_BASES = {"ema21", "ema50", "ema200", "lr50", "lr100", "adx_dmi14", "adx_dmi21"}

# 🔸 Префиксы Redis
TS_IND_PREFIX = "ts_ind"   # ts_ind:{symbol}:{tf}:{param}
BB_TS_PREFIX  = "bb:ts"    # bb:ts:{symbol}:{tf}:c
KV_MW_PREFIX  = "ind_mw"   # ind_mw:{symbol}:{tf}:{kind}

# 🔸 Логгер
log = logging.getLogger("MW_TREND")


# 🔸 Вспомогательное: ms с ISO open_time
def iso_to_ms(iso: str) -> int:
    dt = datetime.fromisoformat(iso)
    return int(dt.timestamp() * 1000)


# 🔸 Вспомогательное: чтение единственной точки TS по exact open_time (from=to)
async def ts_get_at(redis, key: str, ts_ms: int):
    try:
        res = await redis.execute_command("TS.RANGE", key, ts_ms, ts_ms)
        if res:
            return float(res[0][1])
    except Exception as e:
        log.warning(f"[TS] read error {key}@{ts_ms}: {e}")
    return None


# 🔸 Определение направления по EMA и LR
def infer_direction(price: float | None,
                    ema21: float | None, ema50: float | None, ema200: float | None,
                    ang50: float | None, ang100: float | None) -> str:
    # голоса EMA: price vs EMA (если цена неизвестна — голос игнорируем)
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

    # голоса LR по углам
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


# 🔸 Определение силы по ADX
def infer_strength(adx14: float | None, adx21: float | None) -> bool:
    vals = [v for v in (adx14, adx21) if v is not None]
    if not vals:
        return False
    return max(vals) >= ADX_STRONG


# 🔸 Сбор значений из Redis TS на конкретный бар
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

    # параллельный батч чтений
    tasks = {k: ts_get_at(redis, key, open_ms) for k, key in keys.items()}
    results = await asyncio.gather(*tasks.values(), return_exceptions=False)
    return dict(zip(tasks.keys(), results))


# 🔸 Запись результата в Redis KV (последнее состояние) и в PostgreSQL (история)
async def persist_result(pg, redis, symbol: str, tf: str, open_time_iso: str,
                         state: str, direction: str, strong: bool,
                         status: str, used_bases: list[str], missing_bases: list[str],
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
        "details": {"used_bases": used_bases, "missing_bases": missing_bases},
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
        # сканируем дедлайны и закрываем просроченные ключи
        now = datetime.utcnow()
        expired = []
        for k, obj in pending.items():
            if now >= obj["deadline"]:
                expired.append((k, obj))
        for (k, obj) in expired:
            symbol, tf, open_time_iso = k
            missing = sorted(list(obj["expected"] - obj["arrived"]))
            # частичный расчёт (или фиксация как partial без расчёта, если данных явно нет)
            open_ms = iso_to_ms(open_time_iso)
            inputs = await load_trend_inputs(redis, symbol, tf, open_ms)

            # направление/сила (что есть)
            direction = infer_direction(
                inputs.get("close"),
                inputs.get("ema21"), inputs.get("ema50"), inputs.get("ema200"),
                inputs.get("lr50_angle"), inputs.get("lr100_angle")
            )
            strong = infer_strength(inputs.get("adx14"), inputs.get("adx21"))
            state = (
                "sideways" if direction == "sideways"
                else f"{direction}_{'strong' if strong else 'weak'}"
            )

            await persist_result(
                pg, redis, symbol, tf, open_time_iso,
                state=state, direction=direction, strong=strong,
                status="partial", used_bases=sorted(list(obj["arrived"])), missing_bases=missing,
                source="live", version=1
            )
            await mark_gap(pg, symbol, tf, open_time_iso, missing)
            log.info(f"MW_TREND PARTIAL {symbol}/{tf}@{open_time_iso} arrived={len(obj['arrived'])}/{len(obj['expected'])} state={state}")
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
                        tf     = data["timeframe"]
                        base   = data["indicator"]  # например: 'ema21', 'lr50', 'adx_dmi14', ...
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

                            direction = infer_direction(
                                inputs.get("close"),
                                inputs.get("ema21"), inputs.get("ema50"), inputs.get("ema200"),
                                inputs.get("lr50_angle"), inputs.get("lr100_angle")
                            )
                            strong = infer_strength(inputs.get("adx14"), inputs.get("adx21"))
                            state = (
                                "sideways" if direction == "sideways"
                                else f"{direction}_{'strong' if strong else 'weak'}"
                            )

                            await persist_result(
                                pg, redis, symbol, tf, open_iso,
                                state=state, direction=direction, strong=strong,
                                status="ok", used_bases=sorted(list(rec["arrived"])), missing_bases=[],
                                source="live", version=1
                            )
                            log.info(f"MW_TREND OK {symbol}/{tf}@{open_iso} state={state}")
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