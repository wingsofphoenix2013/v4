# indicator_mw_states.py — расчёт интегрального состояния тикера (market_state) на основе TREND/VOL/MOM/EXT

import asyncio
import json
import logging
import time
from datetime import datetime

# 🔸 Константы стрима готовых свечей (Bybit PG insert)
CANDLE_STREAM = "bb:pg_candle_inserted"
GROUP = "mw_state_group"
CONSUMER = "mw_state_1"

# 🔸 Ограничения ожидания готовности MW-состояний
STATE_WAIT_FIRST_SEC = 5
STATE_WAIT_RETRIES = 10
STATE_WAIT_RETRY_SEC = 2

# 🔸 Таймфреймы и шаги (для информации; пока используем только для логики TF)
VALID_TF = {"m5", "m15", "h1"}

# 🔸 Префиксы Redis KV для MarketWatch
KV_MW_PREFIX = "ind_mw"  # ind_mw:{symbol}:{tf}:{kind}

# 🔸 Логгер модуля
log = logging.getLogger("MW_STATE")


# 🔸 Утилиты времени / конверсии
def ms_to_iso(ms: int) -> str:
    return datetime.utcfromtimestamp(ms / 1000).isoformat()


# 🔸 Загрузка MW-состояния для (symbol, tf, kind) из KV
async def load_mw_state(redis, symbol: str, tf: str, kind: str):
    """
    Читает KV ind_mw:{symbol}:{tf}:{kind} и возвращает dict с полями:
      - state: str
      - open_time: str (ISO)
      - computed_at: str (ISO) | None
      - details: dict
    Если ключа нет или формат некорректен — возвращает None.
    """
    key = f"{KV_MW_PREFIX}:{symbol}:{tf}:{kind}"
    try:
        raw = await redis.get(key)
        if not raw:
            return None
        data = json.loads(raw)
        state = data.get("state")
        open_time = data.get("open_time")
        if not state or not open_time:
            return None
        return {
            "state": state,
            "open_time": open_time,
            "computed_at": data.get("computed_at"),
            "details": data.get("details") or {},
        }
    except Exception as e:
        log.warning(f"MW_STATE: error reading {key}: {e}")
        return None


# 🔸 Расчёт direction/quality/score по четырём MW-состояниям
def compute_direction_and_quality(trend_state: str,
                                  vol_state: str,
                                  mom_state: str,
                                  ext_state: str) -> tuple[str, str, float, dict]:
    """
    На входе финальные состояния MW:
      - trend_state: 'up_strong'|'up_weak'|'sideways'|'down_weak'|'down_strong'|...
      - vol_state:   'low_squeeze'|'normal'|'expanding'|'high'|...
      - mom_state:   'bull_impulse'|'bear_impulse'|'overbought'|'oversold'|'divergence_flat'|...
      - ext_state:   'overbought_extension'|'oversold_extension'|'pullback_in_uptrend'|'pullback_in_downtrend'|'none'|...

    Возвращает:
      - direction: 'short_only'|'both'|'long_only'
      - quality: 'ok'|'avoid'
      - score: float ∈ [-1.0; +1.0]
      - components: dict для details
    """
    score = 0.0

    # вклад TREND
    if trend_state == "up_strong":
        score += 0.8
    elif trend_state == "up_weak":
        score += 0.4
    elif trend_state == "down_strong":
        score -= 0.8
    elif trend_state == "down_weak":
        score -= 0.4
    elif trend_state == "sideways":
        score += 0.0
    else:
        # незнакомое состояние — считаем как нейтральное
        score += 0.0

    # вклад MOMENTUM
    if mom_state == "bull_impulse":
        score += 0.3
    elif mom_state == "bear_impulse":
        score -= 0.3
    elif mom_state == "overbought" and score > 0:
        score -= 0.3
    elif mom_state == "oversold" and score < 0:
        score += 0.3

    # вклад EXTREMES
    if ext_state == "overbought_extension" and score > 0:
        score -= 0.5
    elif ext_state == "oversold_extension" and score < 0:
        score += 0.5
    elif ext_state == "pullback_in_uptrend":
        score += 0.2
    elif ext_state == "pullback_in_downtrend":
        score -= 0.2

    # мягкая нормализация score
    if score > 1.0:
        score = 1.0
    if score < -1.0:
        score = -1.0

    # quality по VOL + крайностям
    quality = "ok"
    if vol_state == "high" and (
        ext_state in ("overbought_extension", "oversold_extension")
        or mom_state in ("overbought", "oversold")
    ):
        quality = "avoid"

    # маппинг score → direction
    if score >= 0.4:
        direction = "long_only"
    elif score <= -0.4:
        direction = "short_only"
    else:
        direction = "both"

    components = {
        "trend_state": trend_state,
        "volatility_state": vol_state,
        "momentum_state": mom_state,
        "extremes_state": ext_state,
    }

    return direction, quality, score, components


# 🔸 Запись результата market_state в KV и PostgreSQL
async def persist_market_state(pg,
                               redis,
                               symbol: str,
                               tf: str,
                               open_time_iso: str,
                               direction: str,
                               quality: str,
                               score: float,
                               components: dict):
    """
    Пишет:
      - KV ind_mw:{symbol}:{tf}:market_state
      - строку в indicator_marketwatch_values с kind='market_state'
    """
    # KV
    kv_key = f"{KV_MW_PREFIX}:{symbol}:{tf}:market_state"
    payload = {
        "direction": direction,
        "quality": quality,
        "score": round(float(score), 4),
        "open_time": open_time_iso,
        "computed_at": datetime.utcnow().isoformat(),
        "details": components,
    }
    try:
        await redis.set(kv_key, json.dumps(payload))
    except Exception as e:
        log.warning(f"MW_STATE: KV set error {kv_key}: {e}")

    # PG upsert
    details = {
        "direction": direction,
        "quality": quality,
        "score": round(float(score), 4),
        "components": components,
        "open_time_iso": open_time_iso,
    }
    try:
        async with pg.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO indicator_marketwatch_values
                  (symbol, timeframe, open_time, kind, state, status, details, version, source, computed_at, updated_at)
                VALUES ($1,$2,$3,'market_state',$4,'ok',$5,1,'live',NOW(),NOW())
                ON CONFLICT (symbol, timeframe, open_time, kind)
                DO UPDATE SET
                  state   = EXCLUDED.state,
                  status  = EXCLUDED.status,
                  details = EXCLUDED.details,
                  version = EXCLUDED.version,
                  source  = EXCLUDED.source,
                  updated_at = NOW()
                """,
                symbol,
                tf,
                datetime.fromisoformat(open_time_iso),
                direction,
                json.dumps(details),
            )
    except Exception as e:
        log.error(f"MW_STATE: PG upsert error {symbol}/{tf}@{open_time_iso}: {e}")


# 🔸 Фиксация пропуска market_state (опционально, под будущую таблицу gap'ов)
async def mark_market_state_gap(pg,
                                symbol: str,
                                tf: str,
                                open_time_iso: str,
                                missing_kinds: list[str]):
    """
    Пишем пропуск в отдельную таблицу (если она создана в БД) или просто логируем.
    Ожидаем таблицу вида market_state_gap с UNIQUE(symbol,timeframe,open_time).
    """
    try:
        async with pg.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO market_state_gap (symbol, timeframe, open_time, missing_kinds, status, detected_at)
                VALUES ($1,$2,$3,$4,'found',NOW())
                ON CONFLICT (symbol, timeframe, open_time)
                DO UPDATE SET
                  missing_kinds = EXCLUDED.missing_kinds,
                  status = 'found',
                  attempts = market_state_gap.attempts + 1,
                  detected_at = NOW()
                """,
                symbol,
                tf,
                datetime.fromisoformat(open_time_iso),
                json.dumps(missing_kinds),
            )
    except Exception as e:
        # если таблицы нет или другая ошибка — не роняем воркер
        log.warning(f"MW_STATE: gap insert error {symbol}/{tf}@{open_time_iso}: {e}")


# 🔸 Расчёт market_state для конкретного бара (symbol, tf, open_time)
async def compute_market_state_for_bar(pg, redis, symbol: str, tf: str, open_time_iso: str):
    """
    Алгоритм:
      - ждём STATE_WAIT_FIRST_SEC, давая MW-воркерам время отработать;
      - затем до STATE_WAIT_RETRIES раз:
          * читаем KV для 4 kind'ов (trend/volatility/momentum/extremes)
          * если у всех open_time == target → считаем market_state и выходим;
          * иначе ждём STATE_WAIT_RETRY_SEC и повторяем.
      - если так и не собрали полный набор — фиксируем gap и выходим.
    """
    if tf not in VALID_TF:
        log.debug(f"MW_STATE: skip unsupported TF {tf} for {symbol}")
        return

    target_open = open_time_iso

    # первичное ожидание
    await asyncio.sleep(STATE_WAIT_FIRST_SEC)

    attempts = 0
    total_wait_sec = STATE_WAIT_FIRST_SEC

    while True:
        # загрузка текущих MW-состояний
        trend = await load_mw_state(redis, symbol, tf, "trend")
        vol = await load_mw_state(redis, symbol, tf, "volatility")
        mom = await load_mw_state(redis, symbol, tf, "momentum")
        ext = await load_mw_state(redis, symbol, tf, "extremes")

        missing = []

        def match_open(rec, kind_name):
            if rec is None:
                missing.append(kind_name)
                return None
            if rec["open_time"] != target_open:
                missing.append(kind_name)
                return None
            return rec["state"]

        trend_state = match_open(trend, "trend")
        vol_state = match_open(vol, "volatility")
        mom_state = match_open(mom, "momentum")
        ext_state = match_open(ext, "extremes")

        if not missing:
            # все 4 состояния готовы для target_open
            direction, quality, score, components = compute_direction_and_quality(
                trend_state, vol_state, mom_state, ext_state
            )
            await persist_market_state(pg, redis, symbol, tf, target_open, direction, quality, score, components)

            # логируем сводку на INFO
            log.debug(
                f"MW_STATE OK {symbol}/{tf}@{target_open} "
                f"direction={direction} quality={quality} score={score:.3f} "
                f"trend={trend_state} vol={vol_state} mom={mom_state} ext={ext_state}"
            )
            return

        attempts += 1
        if attempts >= STATE_WAIT_RETRIES:
            # не дождались — фиксируем gap и выходим
            await mark_market_state_gap(pg, symbol, tf, target_open, missing)
            log.debug(
                f"MW_STATE GAP {symbol}/{tf}@{target_open} "
                f"missing={','.join(missing)} total_wait_sec={total_wait_sec}"
            )
            return

        await asyncio.sleep(STATE_WAIT_RETRY_SEC)
        total_wait_sec += STATE_WAIT_RETRY_SEC


# 🔸 Основной воркер: слушает bb:pg_candle_inserted и запускает расчёт market_state
async def run_indicator_mw_states(pg, redis):
    log.debug("MW_STATE: воркер запущен")

    # создаём consumer-group на стриме закрытых свечей в PG
    try:
        await redis.xgroup_create(CANDLE_STREAM, GROUP, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"MW_STATE: xgroup_create error: {e}")

    sem = asyncio.Semaphore(30)
    in_flight = set()

    while True:
        try:
            resp = await redis.xreadgroup(
                groupname=GROUP,
                consumername=CONSUMER,
                streams={CANDLE_STREAM: ">"},
                count=200,
                block=1000,
            )
            if not resp:
                continue

            to_ack = []
            tasks = []

            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)
                    try:
                        symbol = data.get("symbol")
                        tf = data.get("interval")
                        ts_raw = data.get("timestamp")

                        # базовая валидация
                        if not symbol or tf not in VALID_TF or not ts_raw:
                            continue

                        try:
                            ts_ms = int(ts_raw)
                        except Exception:
                            continue

                        open_iso = ms_to_iso(ts_ms)
                        key = (symbol, tf, open_iso)

                        if key in in_flight:
                            continue

                        async def _run(key_tuple):
                            sym, tff, iso = key_tuple
                            async with sem:
                                try:
                                    await compute_market_state_for_bar(pg, redis, sym, tff, iso)
                                except Exception as e:
                                    log.error(f"MW_STATE compute error {sym}/{tff}@{iso}: {e}", exc_info=True)
                                finally:
                                    in_flight.discard(key_tuple)

                        in_flight.add(key)
                        tasks.append(asyncio.create_task(_run(key)))

                    except Exception as e:
                        log.warning(f"MW_STATE: message error: {e}", exc_info=True)

            if to_ack:
                try:
                    await redis.xack(CANDLE_STREAM, GROUP, *to_ack)
                except Exception as e:
                    log.warning(f"MW_STATE: ack error: {e}")

            if tasks:
                # подчистим завершившиеся задачи
                done, _ = await asyncio.wait(tasks, timeout=0, return_when=asyncio.FIRST_COMPLETED)
                for _t in done:
                    pass

        except Exception as e:
            log.error(f"MW_STATE loop error: {e}", exc_info=True)
            await asyncio.sleep(0.5)