# indicator_mw_states_back.py — бэкаповый расчёт market_state по истории indicator_marketwatch_values

import asyncio
import json
import logging
from datetime import datetime, timedelta

# 🔸 Импорт правил агрегирования из основного модуля состояний
from indicator_mw_states import compute_direction_and_quality

# 🔸 Константы стрима закрытых свечей (Bybit PG insert)
CANDLE_STREAM = "bb:pg_candle_inserted"
BACK_GROUP = "mw_state_back_group"
BACK_CONSUMER = "mw_state_back_1"

# 🔸 Окно ожидания и бэкапа
BACKFILL_START_DELAY_SEC = 60          # ждать после старта системы
BACKFILL_LOOKBACK_DAYS = 10            # сколько суток назад смотреть
BACKFILL_SKIP_RECENT_HOURS = 1         # не трогаем последние N часов (оставляем live-воркеру)
BACKFILL_BATCH_LIMIT = 50_000          # страховочный лимит строк на один прогон

# 🔸 Поддерживаемые TF
VALID_TF = {"m5", "m15", "h1"}

# 🔸 Логгер модуля
log = logging.getLogger("MW_STATE_BACK")


# 🔸 Конвертация timestamp(ms) → datetime UTC
def ms_to_dt(ms: int) -> datetime:
    return datetime.utcfromtimestamp(ms / 1000)


# 🔸 Ожидание первого сообщения в bb:pg_candle_inserted для синхронизации старта
async def wait_first_candle(redis) -> datetime:
    """
    Ждёт первое сообщение в CANDLE_STREAM через consumer-group BACK_GROUP.
    Возвращает datetime(anchor) по полю timestamp (UTC), который потом
    используется как опорное время для окна бэкапа.
    """
    # создаём consumer-group, если её ещё нет
    try:
        await redis.xgroup_create(CANDLE_STREAM, BACK_GROUP, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"MW_STATE_BACK: xgroup_create error: {e}")

    log.debug("MW_STATE_BACK: ожидание первого сообщения в bb:pg_candle_inserted")

    while True:
        try:
            resp = await redis.xreadgroup(
                groupname=BACK_GROUP,
                consumername=BACK_CONSUMER,
                streams={CANDLE_STREAM: ">"},
                count=1,
                block=10_000,  # 10 секунд
            )
            if not resp:
                continue

            to_ack = []
            anchor_dt = None

            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)
                    ts_raw = data.get("timestamp")
                    try:
                        ts_ms = int(ts_raw)
                        anchor_dt = ms_to_dt(ts_ms)
                    except Exception:
                        # если timestamp кривой — просто игнорируем и ждём следующее
                        anchor_dt = None

            if to_ack:
                try:
                    await redis.xack(CANDLE_STREAM, BACK_GROUP, *to_ack)
                except Exception as e:
                    log.warning(f"MW_STATE_BACK: ack error: {e}")

            if anchor_dt is not None:
                log.info(f"MW_STATE_BACK: получен anchor из bb:pg_candle_inserted → {anchor_dt.isoformat()}")
                return anchor_dt

        except Exception as e:
            log.error(f"MW_STATE_BACK: read error: {e}", exc_info=True)
            await asyncio.sleep(1)


# 🔸 Основная выборка кандидатов для бэкапа из indicator_marketwatch_values
async def fetch_backfill_candidates(pg, start_dt: datetime, end_dt: datetime):
    """
    Ищет бары, для которых есть все 4 MW-состояния (trend/volatility/momentum/extremes),
    но ещё нет kind='market_state'.

    Возвращает список записей вида:
      {
        "symbol": str,
        "timeframe": str,
        "open_time": datetime,
        "trend_state": str,
        "vol_state": str,
        "mom_state": str,
        "ext_state": str,
      }
    """
    log.debug(
        f"MW_STATE_BACK: выборка кандидатов для окна "
        f"{start_dt.isoformat()} .. {end_dt.isoformat()}"
    )

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
              symbol,
              timeframe,
              open_time,
              max(CASE WHEN kind = 'trend'      THEN state END) AS trend_state,
              max(CASE WHEN kind = 'volatility' THEN state END) AS vol_state,
              max(CASE WHEN kind = 'momentum'   THEN state END) AS mom_state,
              max(CASE WHEN kind = 'extremes'   THEN state END) AS ext_state,
              bool_or(kind = 'market_state')    AS has_market_state,
              count(DISTINCT kind)              AS kind_count
            FROM indicator_marketwatch_values
            WHERE
              open_time BETWEEN $1 AND $2
              AND timeframe = ANY($3::text[])
              AND kind IN ('trend','volatility','momentum','extremes','market_state')
            GROUP BY symbol, timeframe, open_time
            HAVING
              bool_or(kind = 'market_state') = false
              AND count(DISTINCT kind) >= 4
            ORDER BY open_time ASC
            LIMIT $4
            """,
            start_dt,
            end_dt,
            list(VALID_TF),
            BACKFILL_BATCH_LIMIT,
        )

    candidates = []
    for r in rows:
        # страховка от NULL-состояний
        if not (r["trend_state"] and r["vol_state"] and r["mom_state"] and r["ext_state"]):
            continue
        candidates.append(
            {
                "symbol": r["symbol"],
                "timeframe": r["timeframe"],
                "open_time": r["open_time"],
                "trend_state": r["trend_state"],
                "vol_state": r["vol_state"],
                "mom_state": r["mom_state"],
                "ext_state": r["ext_state"],
            }
        )

    return candidates


# 🔸 Запись бэкапного market_state в indicator_marketwatch_values
async def write_backfilled_states(pg, records: list[dict]):
    """
    Принимает список предварительно посчитанных market_state-записей и
    пишет их в indicator_marketwatch_values с kind='market_state'.

    details содержит direction, quality, score, components, open_time_iso.
    status ставим 'healed', source = 'backfill'.
    """
    if not records:
        return 0

    async with pg.acquire() as conn:
        async with conn.transaction():
            params = []
            for rec in records:
                symbol = rec["symbol"]
                tf = rec["timeframe"]
                open_time = rec["open_time"]
                direction = rec["direction"]
                details = {
                    "direction": rec["direction"],
                    "quality": rec["quality"],
                    "score": rec["score"],
                    "components": rec["components"],
                    "open_time_iso": rec["open_time_iso"],
                }
                params.append((symbol, tf, open_time, direction, json.dumps(details)))

            await conn.executemany(
                """
                INSERT INTO indicator_marketwatch_values
                  (symbol, timeframe, open_time, kind, state, status, details, version, source, computed_at, updated_at)
                VALUES ($1,$2,$3,'market_state',$4,'healed',$5,1,'backfill',NOW(),NOW())
                ON CONFLICT (symbol, timeframe, open_time, kind)
                DO UPDATE SET
                  state   = EXCLUDED.state,
                  status  = EXCLUDED.status,
                  details = EXCLUDED.details,
                  version = EXCLUDED.version,
                  source  = EXCLUDED.source,
                  updated_at = NOW()
                """,
                params,
            )

    return len(records)


# 🔸 Один прогон бэкапа по окну [start_dt .. end_dt]
async def run_backfill_window(pg, start_dt: datetime, end_dt: datetime):
    # выбираем кандидатов
    candidates = await fetch_backfill_candidates(pg, start_dt, end_dt)
    if not candidates:
        log.info(
            f"MW_STATE_BACK: кандидатов для бэкапа не найдено в окне "
            f"{start_dt.isoformat()} .. {end_dt.isoformat()}"
        )
        return 0, {}

    records_to_write = []
    per_tf = {tf: 0 for tf in VALID_TF}

    for c in candidates:
        symbol = c["symbol"]
        tf = c["timeframe"]
        ot = c["open_time"]
        trend_state = c["trend_state"]
        vol_state = c["vol_state"]
        mom_state = c["mom_state"]
        ext_state = c["ext_state"]

        direction, quality, score, components = compute_direction_and_quality(
            trend_state, vol_state, mom_state, ext_state
        )

        rec = {
            "symbol": symbol,
            "timeframe": tf,
            "open_time": ot,
            "direction": direction,
            "quality": quality,
            "score": round(float(score), 4),
            "components": components,
            "open_time_iso": ot.isoformat(),
        }
        records_to_write.append(rec)
        per_tf[tf] = per_tf.get(tf, 0) + 1

    written = await write_backfilled_states(pg, records_to_write)

    log.info(
        f"MW_STATE_BACK: в окне {start_dt.isoformat()} .. {end_dt.isoformat()} "
        f"найдено кандидатов={len(candidates)}, записано={written} "
        f"(m5={per_tf.get('m5',0)}, m15={per_tf.get('m15',0)}, h1={per_tf.get('h1',0)})"
    )

    return written, per_tf


# 🔸 Основной воркер бэкапа: однократный проход по истории за последние 10 суток
async def run_indicator_mw_states_back(pg, redis):
    log.debug("MW_STATE_BACK: воркер запущен (ожидание старта)")

    # ждём стартовый лаг, чтобы не мешать живой инициализации
    await asyncio.sleep(BACKFILL_START_DELAY_SEC)

    # ждём первое сообщение от feed_bb, чтобы взять опорное время
    anchor_dt = await wait_first_candle(redis)

    # сдвигаем назад на BACKFILL_SKIP_RECENT_HOURS
    effective_now = anchor_dt - timedelta(hours=BACKFILL_SKIP_RECENT_HOURS)
    start_dt = effective_now - timedelta(days=BACKFILL_LOOKBACK_DAYS)
    end_dt = effective_now

    log.info(
        f"MW_STATE_BACK: старт бэкапа market_state для окна "
        f"{start_dt.isoformat()} .. {end_dt.isoformat()} "
        f"(anchor={anchor_dt.isoformat()}, skip_recent_hours={BACKFILL_SKIP_RECENT_HOURS})"
    )

    # один проход по окну; при необходимости можно будет сделать сегментацию по дням
    total_written, per_tf = await run_backfill_window(pg, start_dt, end_dt)

    log.info(
        f"MW_STATE_BACK: бэкап завершён, всего записано={total_written} "
        f"(m5={per_tf.get('m5',0)}, m15={per_tf.get('m15',0)}, h1={per_tf.get('h1',0)})"
    )

    # после этого воркер корректно завершается; если его запускать без run_safe_loop,
    # он отработает один раз и больше не будет перезапускаться.