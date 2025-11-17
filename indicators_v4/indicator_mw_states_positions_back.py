# indicator_mw_states_positions_back.py — разовый бэкофилл market_state в indicator_position_stat для старых позиций

import asyncio
import logging
from datetime import datetime, timedelta

# 🔸 Логгер
log = logging.getLogger("IPS_MW_BACK")

# 🔸 Параметры бэкофилла
POS_LOOKBACK_DAYS = 12              # сколько дней назад берём позиции
POS_SKIP_RECENT_MINUTES = 10        # не трогаем позиции, открытые в последние N минут
POS_BATCH_LIMIT = 500               # максимум позиций за один прогон
POSITION_CONCURRENCY = 10           # максимальное число позиций, обрабатываемых параллельно

# 🔸 Таймшаги TF (в минутах)
STEP_MIN = {"m5": 5, "m15": 15, "h1": 60}

# 🔸 Поддерживаемые TF
VALID_TF = ("m5", "m15", "h1")


# 🔸 Вспомогательные функции времени
def floor_to_bar(dt: datetime, step_min: int) -> datetime:
    """
    Округление времени вниз к началу бара данного ТФ.
    Пример: 05:27 при m5 → 05:25; 05:24:30 при m5 → 05:20.
    """
    dt = dt.replace(second=0, microsecond=0)
    rem = dt.minute % step_min
    if rem:
        dt -= timedelta(minutes=rem)
    return dt


def last_closed_bar_cutoff(dt: datetime, step_min: int) -> datetime:
    """
    Возвращает максимальное open_time бара, который уже ЗАКРЫТ к моменту dt.
    Логика: open_time <= dt - step_min.
    Пример: T_open=05:25, step=5 → cutoff=05:20 → последний закрытый бар m5=05:20.
    """
    return dt - timedelta(minutes=step_min)


# 🔸 Выборка позиций без market_state в indicator_position_stat
async def fetch_pending_positions(pg, start_dt: datetime, end_dt: datetime, limit: int = POS_BATCH_LIMIT):
    """
    Ищет позиции в окне [start_dt .. end_dt], для которых ещё нет НИ одной записи
    с param_type='marketwatch' AND param_base='market_state' в indicator_position_stat.

    Возвращает список dict:
      {"position_uid", "strategy_id", "symbol", "created_at"}
    """
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT p.position_uid,
                   p.strategy_id,
                   p.symbol,
                   p.created_at
            FROM positions_v4 p
            WHERE p.created_at BETWEEN $1 AND $2
              AND p.created_at IS NOT NULL
              AND NOT EXISTS (
                    SELECT 1
                    FROM indicator_position_stat ips
                    WHERE ips.position_uid = p.position_uid
                      AND ips.param_type = 'marketwatch'
                      AND ips.param_base = 'market_state'
                )
            ORDER BY p.created_at ASC
            LIMIT $3
            """,
            start_dt,
            end_dt,
            limit,
        )

    positions = []
    for r in rows:
        positions.append(
            {
                "position_uid": r["position_uid"],
                "strategy_id": r["strategy_id"],
                "symbol": r["symbol"],
                "created_at": r["created_at"],
            }
        )
    return positions


# 🔸 Поиск market_state на момент открытия позиции для symbol/tf
async def load_market_state_for_position(pg, symbol: str, tf: str, created_at: datetime):
    """
    Ищет последнее ЗАКРЫТОЕ market_state для symbol/tf к моменту created_at.

    Логика:
      - cutoff = created_at - шаг_ТФ
      - находим market_state с open_time <= cutoff
      - ORDER BY open_time DESC LIMIT 1

    Возвращает dict {"direction", "quality", "open_time"} или None.
    """
    step_min = STEP_MIN[tf]
    cutoff = last_closed_bar_cutoff(created_at, step_min)

    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT state, details, open_time
            FROM indicator_marketwatch_values
            WHERE symbol = $1
              AND timeframe = $2
              AND kind = 'market_state'
              AND open_time <= $3
            ORDER BY open_time DESC
            LIMIT 1
            """,
            symbol,
            tf,
            cutoff,
        )

    if not row:
        return None

    details = row["details"] or {}
    direction = row["state"]  # state уже равен direction
    quality = details.get("quality", "ok")

    return {
        "direction": direction,
        "quality": quality,
        "open_time": row["open_time"],
    }


# 🔸 Запись двух параметров (direction/quality) в indicator_position_stat
async def write_position_market_state(pg,
                                      position_uid: str,
                                      strategy_id: int,
                                      symbol: str,
                                      tf: str,
                                      open_time_snapshot: datetime,
                                      market_state: dict):
    """
    Пишет две записи в indicator_position_stat:
      - (market_state, direction)
      - (market_state, quality)

    open_time_snapshot — время ОТКРЫТИЯ бара TF, внутри которого открыта позиция
    (а НЕ open_time бара, по которому посчитан market_state).
    """
    direction = market_state["direction"]
    quality = market_state["quality"]

    records = [
        # direction
        (
            position_uid,
            strategy_id,
            symbol,
            tf,
            "marketwatch",   # param_type
            "market_state",  # param_base
            "direction",     # param_name
            None,            # value_num
            direction,       # value_text
            open_time_snapshot,
            "ok",            # status
            None,            # error_code
        ),
        # quality
        (
            position_uid,
            strategy_id,
            symbol,
            tf,
            "marketwatch",
            "market_state",
            "quality",
            None,
            quality,
            open_time_snapshot,
            "ok",
            None,
        ),
    ]

    async with pg.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(
                """
                INSERT INTO indicator_position_stat
                  (position_uid,
                   strategy_id,
                   symbol,
                   timeframe,
                   param_type,
                   param_base,
                   param_name,
                   value_num,
                   value_text,
                   open_time,
                   status,
                   error_code)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                ON CONFLICT (position_uid, timeframe, param_type, param_base, param_name)
                DO NOTHING
                """,
                records,
            )


# 🔸 Обработка одной позиции (по всем TF, последовательно)
async def process_single_position(pg, position: dict):
    """
    position: {"position_uid", "strategy_id", "symbol", "created_at"}

    Для каждой позиции:
      - для TF ∈ {m5,m15,h1}:
          * находим market_state для последнего закрытого бара к моменту created_at
          * считаем open_time_snapshot = бар TF, внутри которого открыта позиция
          * если нашли market_state — пишем direction/quality в indicator_position_stat
    """
    position_uid = position["position_uid"]
    strategy_id = position["strategy_id"]
    symbol = position["symbol"]
    created_at = position["created_at"]

    written_tf = []

    for tf in VALID_TF:
        step_min = STEP_MIN[tf]

        # время открытия бара TF, внутри которого открыта позиция
        open_time_snapshot = floor_to_bar(created_at, step_min)

        # находим market_state на последний закрытый бар перед created_at
        ms = await load_market_state_for_position(pg, symbol, tf, created_at)
        if not ms:
            # нет market_state для этого TF к моменту открытия — пропускаем TF
            continue

        await write_position_market_state(
            pg,
            position_uid,
            strategy_id,
            symbol,
            tf,
            open_time_snapshot,
            ms,
        )
        written_tf.append(tf)

    return written_tf


# 🔸 Разовый воркер бэкофилла по позициям
async def run_indicator_mw_states_positions_back(pg, redis):
    log.info("IPS_MW_BACK: воркер запущен (разовый бэкофилл market_state для позиций)")

    sem = asyncio.Semaphore(POSITION_CONCURRENCY)

    now = datetime.utcnow()
    end_dt = now - timedelta(minutes=POS_SKIP_RECENT_MINUTES)
    start_dt = end_dt - timedelta(days=POS_LOOKBACK_DAYS)

    # выборка позиций, по которым ещё нет market_state-снимков
    positions = await fetch_pending_positions(pg, start_dt, end_dt)
    total = len(positions)

    if not positions:
        log.info(
            f"IPS_MW_BACK: нет позиций для бэкофилла в окне "
            f"{start_dt.isoformat()} .. {end_dt.isoformat()}"
        )
        return

    log.info(
        f"IPS_MW_BACK: найдено позиций для обработки={total} "
        f"в окне {start_dt.isoformat()} .. {end_dt.isoformat()}"
    )

    tasks = []

    for pos in positions:
        async def _run_position(p=pos):
            async with sem:
                try:
                    tfs_written = await process_single_position(pg, p)
                    if tfs_written:
                        log.debug(
                            f"IPS_MW_BACK: позиция {p['position_uid']} "
                            f"{p['symbol']} created_at={p['created_at']} "
                            f"→ записано TF={','.join(tfs_written)}"
                        )
                except Exception as e:
                    log.error(
                        f"IPS_MW_BACK: ошибка при обработке позиции "
                        f"{p['position_uid']} {p['symbol']}: {e}",
                        exc_info=True,
                    )

        tasks.append(asyncio.create_task(_run_position()))

    await asyncio.gather(*tasks, return_exceptions=False)

    log.info(
        f"IPS_MW_BACK: проход завершён, обработано позиций={total}"
    )
    # после этого воркер завершится; при подключении в indicators_v4_main
    # его нужно вызывать БЕЗ run_safe_loop (одноразовый запуск)