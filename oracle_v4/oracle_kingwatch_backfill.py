# 🔸 oracle_kingwatch_backfill.py — KingWatcher backfill: добираем композит по триплету MW на баре открытия (для стратегий king_watcher=true)
#     Обновлено: при апдейте триплета пишем денормализованное "всего сделок у стратегии" в строку.

import os
import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

import infra

# 🔸 Логгер
log = logging.getLogger("ORACLE_KW_BF")


# 🔸 Конфиг backfill'а
BATCH_SIZE           = int(os.getenv("KW_BF_BATCH_SIZE", "500"))
MAX_CONCURRENCY      = int(os.getenv("KW_BF_MAX_CONCURRENCY", "12"))
SHORT_SLEEP_MS       = int(os.getenv("KW_BF_SLEEP_MS", "150"))
START_DELAY_SEC      = int(os.getenv("KW_BF_START_DELAY_SEC", "180"))
RECHECK_INTERVAL_SEC = int(os.getenv("KW_BF_RECHECK_INTERVAL_SEC", "300"))


# 🔸 Таймшаги TF
TF_ORDER = ("m5", "m15", "h1")
TF_STEP_SEC = {"m5": 300, "m15": 900, "h1": 3600}


# 🔸 SQL-кандидаты (позиции к добору)
_CANDIDATES_SQL = """
SELECT p.position_uid
FROM positions_v4 p
JOIN strategies_v4 s ON s.id = p.strategy_id
WHERE p.status = 'closed'
  AND COALESCE(p.king_watcher_checked, false) = false
  AND s.enabled = true
  AND COALESCE(s.archived, false) = false
  AND COALESCE(s.king_watcher, false) = true
ORDER BY p.closed_at NULLS LAST, p.id
LIMIT $1
"""


# 🔸 SQL-подсчёт остатка
_COUNT_SQL = """
SELECT COUNT(*)
FROM positions_v4 p
JOIN strategies_v4 s ON s.id = p.strategy_id
WHERE p.status = 'closed'
  AND COALESCE(p.king_watcher_checked, false) = false
  AND s.enabled = true
  AND COALESCE(s.archived, false) = false
  AND COALESCE(s.king_watcher, false) = true
"""


# 🔸 Утилита: floor к началу бара TF (UTC, NAIVE)
def _floor_to_bar_open(dt_utc: datetime, tf: str) -> datetime:
    """
    Вход/выход: naive-UTC datetime (tzinfo=None).
    Если пришёл aware — приводим к naive UTC.
    """
    if dt_utc.tzinfo is not None:
        dt_utc = dt_utc.astimezone(timezone.utc).replace(tzinfo=None)
    step_sec = TF_STEP_SEC[tf]
    epoch = int(dt_utc.timestamp())  # трактуем как UTC
    floored = (epoch // step_sec) * step_sec
    return datetime.utcfromtimestamp(floored)  # naive UTC


# 🔸 Чтение MW regime_code из indicator_marketwatcher_v4
async def _load_imw_code(symbol: str, tf: str, bar_open: datetime):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        code = await conn.fetchval(
            """
            SELECT regime_code
            FROM indicator_marketwatcher_v4
            WHERE symbol = $1 AND timeframe = $2 AND open_time = $3
            """,
            symbol, tf, bar_open
        )
    return None if code is None else int(code)


# 🔸 Загрузка позиции/стратегии (фильтры KW)
async def _load_pos_and_strat(position_uid: str):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        pos = await conn.fetchrow(
            """
            SELECT p.id, p.position_uid, p.symbol, p.direction, p.strategy_id,
                   p.pnl, p.status, p.created_at,
                   COALESCE(p.king_watcher_checked, false) AS kw_checked
            FROM positions_v4 p
            WHERE p.position_uid = $1
            """,
            position_uid,
        )
        if not pos or pos["status"] != "closed" or bool(pos["kw_checked"]):
            return pos, None, ("skip", "not_applicable")

        strat = await conn.fetchrow(
            """
            SELECT id, enabled, COALESCE(archived, false) AS archived, COALESCE(king_watcher, false) AS kw
            FROM strategies_v4
            WHERE id = $1
            """,
            int(pos["strategy_id"]),
        )
        if (not strat) or (not strat["enabled"]) or bool(strat["archived"]) or (not strat["kw"]):
            return pos, strat, ("skip", "strategy_inactive_or_no_kw")

    return pos, strat, ("ok", "eligible")


# 🔸 Собрать триплет MW-кодов по бару открытия
async def _collect_mw_triplet(symbol: str, created_at_utc: datetime):
    per_tf = {}
    # перебор TF; триплет валиден только при наличии всех трёх
    for tf in TF_ORDER:
        bar_open = _floor_to_bar_open(created_at_utc, tf)
        code = await _load_imw_code(symbol, tf, bar_open)
        if code is None:
            return None
        per_tf[tf] = code
    return f"{per_tf['m5']}-{per_tf['m15']}-{per_tf['h1']}"


# 🔸 Advisory-lock на композитный ключ (class_id=10 для KW)
async def _advisory_xact_lock(conn, key_text: str):
    # используем детерминированный hashtext(text) → int4; два компонента: (class_id, hash)
    await conn.execute(
        "SELECT pg_advisory_xact_lock($1::int4, hashtext($2)::int4)",
        10, key_text
    )


# 🔸 Claim позиции и апдейт агрегата (в одной транзакции) + обновление total + Redis KV
async def _aggregate_with_claim(pos, triplet: str):
    pg = infra.pg_pool
    redis = infra.redis_client

    s_id = int(pos["strategy_id"])
    dir_ = str(pos["direction"])

    pnl_raw = pos["pnl"]
    pnl = Decimal(str(pnl_raw if pnl_raw is not None else "0")).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    is_win = 1 if pnl > Decimal("0") else 0

    async with pg.acquire() as conn:
        async with conn.transaction():
            # claim позиции против гонок с live/другими backfill
            claimed = await conn.fetchrow(
                """
                UPDATE positions_v4
                SET king_watcher_checked = true
                WHERE position_uid = $1
                  AND status = 'closed'
                  AND COALESCE(king_watcher_checked, false) = false
                RETURNING position_uid
                """,
                pos["position_uid"]
            )
            if not claimed:
                return ("claimed_by_other", 0, 0)

            # advisory-lock на агрегатный ключ
            await _advisory_xact_lock(conn, f"{s_id}:{dir_}:{triplet}")

            # предсоздание строки (идемпотентно)
            await conn.execute(
                """
                INSERT INTO positions_kw_stat_comp
                  (strategy_id, direction, status_triplet,
                   closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at,
                   strategy_total_closed_trades)
                VALUES ($1,$2,$3, 0,0,0,0,0,NOW(), 0)
                ON CONFLICT (strategy_id, direction, status_triplet) DO NOTHING
                """,
                s_id, dir_, triplet
            )

            # апдейт агрегата под FOR UPDATE
            row = await conn.fetchrow(
                """
                SELECT closed_trades, won_trades, pnl_sum
                FROM positions_kw_stat_comp
                WHERE strategy_id=$1 AND direction=$2 AND status_triplet=$3
                FOR UPDATE
                """,
                s_id, dir_, triplet
            )
            c0 = int(row["closed_trades"]); w0 = int(row["won_trades"]); s0 = Decimal(str(row["pnl_sum"]))
            c = c0 + 1
            w = w0 + is_win
            s = (s0 + pnl).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
            wr = (Decimal(w) / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
            ap = (s / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

            await conn.execute(
                """
                UPDATE positions_kw_stat_comp
                SET closed_trades=$4, won_trades=$5, pnl_sum=$6, winrate=$7, avg_pnl=$8, updated_at=NOW()
                WHERE strategy_id=$1 AND direction=$2 AND status_triplet=$3
                """,
                s_id, dir_, triplet,
                c, w, str(s), str(wr), str(ap)
            )

            # считаем "всего сделок у стратегии" по направлению (денормализация)
            total_n = await conn.fetchval(
                """
                SELECT COALESCE(SUM(closed_trades), 0)
                FROM positions_kw_stat_comp
                WHERE strategy_id=$1 AND direction=$2
                """,
                s_id, dir_
            )
            await conn.execute(
                """
                UPDATE positions_kw_stat_comp
                SET strategy_total_closed_trades = $4
                WHERE strategy_id=$1 AND direction=$2 AND status_triplet=$3
                """,
                s_id, dir_, triplet, int(total_n)
            )

            # Redis KV публикация
            try:
                await redis.set(
                    f"oracle:kw:comp:{s_id}:{dir_}:{triplet}",
                    f'{{"closed_trades": {c}, "winrate": {float(wr):.4f}}}'
                )
            except Exception:
                log.debug("Redis SET failed (kw comp)")

            return ("aggregated", c, int(total_n))