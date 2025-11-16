# indicator_mw_states_back.py — разовый бэкофилл market_state за последние 12 суток из MW-состояний

import asyncio
import json
import logging
from datetime import datetime, timedelta

# 🔸 Импорт правил агрегирования из основного модуля состояний
from indicator_mw_states import compute_direction_and_quality

# 🔸 Логгер
log = logging.getLogger("MW_STATE_BACK")

# 🔸 Параметры бэкофилла
BACKFILL_LOOKBACK_DAYS = 12             # глубина окна в днях
BACKFILL_SKIP_RECENT_HOURS = 1          # не трогаем последний час (live-воркер сам всё сделает)
BACKFILL_BATCH_LIMIT = 50_000           # размер одной порции бэкофилла

# 🔸 Поддерживаемые TF
VALID_TF = {"m5", "m15", "h1"}


# 🔸 Выборка кандидатов для бэкофилла market_state
async def fetch_backfill_candidates(pg, start_dt: datetime, end_dt: datetime):
    """
    Ищет бары, для которых есть все 4 MW-состояния (trend/volatility/momentum/extremes),
    но ещё нет kind='market_state'.

    Возвращает список dict:
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
              bool_or(kind = 'market_state') = false      -- уже существующие market_state не трогаем
              AND count(DISTINCT kind) >= 4               -- есть все 4 MW-компоненты
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
    status = 'healed', source = 'backfill'.
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
                DO NOTHING
                """,
                params,
            )

    return len(records)


# 🔸 Один проход бэкофилла по окну [start_dt .. end_dt] (одна порция до BACKFILL_BATCH_LIMIT)
async def run_backfill_window(pg, start_dt: datetime, end_dt: datetime):
    candidates = await fetch_backfill_candidates(pg, start_dt, end_dt)
    if not candidates:
        log.info(
            f"MW_STATE_BACK: кандидатов для бэкофилла не найдено в окне "
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


# 🔸 Основной воркер бэкофилла: несколько проходов по окну, пока есть кандидаты
async def run_indicator_mw_states_back(pg, redis):
    log.info("MW_STATE_BACK: воркер запущен (разовый бэкофилл market_state)")

    # задаём окно по времени
    now = datetime.utcnow()
    end_dt = now - timedelta(hours=BACKFILL_SKIP_RECENT_HOURS)
    start_dt = end_dt - timedelta(days=BACKFILL_LOOKBACK_DAYS)

    log.info(
        f"MW_STATE_BACK: старт бэкофилла market_state для окна "
        f"{start_dt.isoformat()} .. {end_dt.isoformat()} "
        f"(now={now.isoformat()}, skip_recent_hours={BACKFILL_SKIP_RECENT_HOURS})"
    )

    total_written = 0
    per_tf_total = {tf: 0 for tf in VALID_TF}

    while True:
        # одна порция до BACKFILL_BATCH_LIMIT
        written, per_tf = await run_backfill_window(pg, start_dt, end_dt)
        total_written += written
        for tf in VALID_TF:
            per_tf_total[tf] = per_tf_total.get(tf, 0) + per_tf.get(tf, 0)

        # если записали меньше лимита — это был последний batch
        if written < BACKFILL_BATCH_LIMIT:
            break

    log.info(
        f"MW_STATE_BACK: бэкофилл завершён, всего записано={total_written} "
        f"(m5={per_tf_total.get('m5',0)}, m15={per_tf_total.get('m15',0)}, h1={per_tf_total.get('h1',0)})"
    )
    # воркер завершается — его можно запускать как одноразовый