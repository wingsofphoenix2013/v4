# indicator_mw_auditor.py — аудит целостности MarketWatch (trend/volatility/momentum/extremes) в indicator_marketwatch_values

import asyncio
import json
import logging
from datetime import datetime, timedelta

# 🔸 Логгер
log = logging.getLogger("IND_MW_AUDITOR")

# 🔸 Таймфреймы и шаги (в минутах)
STEP_MIN = {"m5": 5, "m15": 15, "h1": 60}

# 🔸 Глубина окна проверки (в днях)
MW_AUDIT_WINDOW_DAYS = 13

# 🔸 Стрим закрытых свечей в PG (Bybit feed_bb)
CANDLE_STREAM = "bb:pg_candle_inserted"
GROUP = "mw_audit_group"
CONSUMER = "mw_audit_1"

# 🔸 Ожидаемые kind'ы MarketWatch
EXPECTED_KINDS = ("trend", "volatility", "momentum", "extremes")


# 🔸 Выравнивание времени вниз по сетке TF
def align_start(ts: datetime, step_min: int) -> datetime:
    ts = ts.replace(second=0, microsecond=0)
    rem = ts.minute % step_min
    if rem:
        ts -= timedelta(minutes=rem)
    return ts


# 🔸 Конвертация timestamp(ms) → datetime UTC
def ms_to_dt(ms: int) -> datetime:
    return datetime.utcfromtimestamp(ms / 1000)


# 🔸 Уже записанные MW-kind'ы по окну для symbol/timeframe
async def existing_mw_kinds_in_db(pg, symbol: str, timeframe: str, start_ts: datetime, end_ts: datetime):
    """
    Возвращает dict:
      { open_time: set(kind1, kind2, ...) }
    по таблице indicator_marketwatch_values для заданного symbol/timeframe/окна.
    """
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT open_time, kind
            FROM indicator_marketwatch_values
            WHERE symbol = $1
              AND timeframe = $2
              AND open_time BETWEEN $3 AND $4
              AND kind = ANY($5::text[])
            """,
            symbol,
            timeframe,
            start_ts,
            end_ts,
            list(EXPECTED_KINDS),
        )

    by_time = {}
    for r in rows:
        ot = r["open_time"]
        k = r["kind"]
        by_time.setdefault(ot, set()).add(k)
    return by_time


# 🔸 Массовая фиксация дыр в indicator_mw_gap
async def insert_mw_gaps(pg, gaps):
    """
    gaps: iterable[(symbol, timeframe, open_time, missing_kinds_json)]
    missing_kinds_json — jsonb-строка (например, '["trend","momentum"]')
    """
    if not gaps:
        return 0

    async with pg.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO indicator_mw_gap (symbol, timeframe, open_time, missing_kinds, status, detected_at)
            VALUES ($1, $2, $3, $4::jsonb, 'found', NOW())
            ON CONFLICT (symbol, timeframe, open_time)
            DO UPDATE SET
              missing_kinds = EXCLUDED.missing_kinds,
              status = 'found',
              attempts = indicator_mw_gap.attempts + 1,
              detected_at = NOW()
            """,
            gaps,
        )
    return len(gaps)


# 🔸 Основной воркер аудитора MarketWatch
async def run_indicator_mw_auditor(pg, redis):
    log.debug("IND_MW_AUDITOR: воркер запущен (bb:pg_candle_inserted)")

    # создаём consumer-group для стрима свечей
    try:
        await redis.xgroup_create(CANDLE_STREAM, GROUP, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"IND_MW_AUDITOR: xgroup_create error: {e}")

    while True:
        try:
            # читаем события вставки свечей в PG
            resp = await redis.xreadgroup(
                groupname=GROUP,
                consumername=CONSUMER,
                streams={CANDLE_STREAM: ">"},
                count=100,
                block=2000,
            )
            if not resp:
                continue

            to_ack = []
            latest = {}  # (symbol, timeframe) -> max(open_time_dt)

            # собираем последние open_time на пару (symbol, timeframe)
            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)
                    try:
                        symbol = data.get("symbol")
                        interval = data.get("interval")
                        ts_raw = data.get("timestamp")
                        if not symbol or interval not in STEP_MIN or not ts_raw:
                            continue
                        try:
                            ts_ms = int(ts_raw)
                        except Exception:
                            continue
                        ot = ms_to_dt(ts_ms)
                        key = (symbol, interval)
                        if key not in latest or ot > latest[key]:
                            latest[key] = ot
                    except Exception as e:
                        log.warning(f"IND_MW_AUDITOR: parse candle event error: {e}")

            # обработка агрегированных ключей
            for (symbol, tf), end_dt in latest.items():
                step_min = STEP_MIN[tf]
                step = timedelta(minutes=step_min)

                # выравниваем и сдвигаем НАЗАД на один бар — исключаем самый свежий
                end_dt = end_dt.replace(second=0, microsecond=0)
                audit_end = end_dt - step

                # окно: 14 суток до audit_end
                window = timedelta(days=MW_AUDIT_WINDOW_DAYS)
                audit_start = align_start(audit_end - window, step_min)

                # генерация сетки open_time [audit_start .. audit_end]
                times = []
                t = audit_start
                while t <= audit_end:
                    times.append(t)
                    t += step
                if not times:
                    log.debug(f"IND_MW_AUDITOR: {symbol}/{tf} — пустая сетка времени для окна {audit_start}..{audit_end}")
                    continue

                # что уже есть в БД
                have = await existing_mw_kinds_in_db(pg, symbol, tf, audit_start, audit_end)

                total_found = 0
                gaps = []

                for ot in times:
                    present = have.get(ot, set())
                    missing = [k for k in EXPECTED_KINDS if k not in present]
                    if missing:
                        # сериализуем missing в jsonb
                        missing_json = json.dumps(missing)
                        gaps.append((symbol, tf, ot, missing_json))

                if gaps:
                    inserted = await insert_mw_gaps(pg, gaps)
                    total_found += inserted

                log.debug(
                    f"IND_MW_AUDITOR: [{symbol}] [{tf}] окно {audit_start}..{audit_end} — "
                    f"найдено пропусков баров={total_found}"
                )

            # подтверждаем обработку сообщений стрима
            if to_ack:
                try:
                    await redis.xack(CANDLE_STREAM, GROUP, *to_ack)
                except Exception as e:
                    log.warning(f"IND_MW_AUDITOR: xack error: {e}")

        except Exception as e:
            log.error(f"IND_MW_AUDITOR loop error: {e}", exc_info=True)
            await asyncio.sleep(2)