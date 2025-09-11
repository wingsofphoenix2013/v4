# 🔸 oracle_mw_emastatus_quartet_aggregator.py — MW×EMA(m5) квартеты: батчи + семафор, UPSERT агрегатов, Redis, флаги

# 🔸 Импорты и базовая настройка
import os
import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

import infra

log = logging.getLogger("ORACLE_MW_EMA_Q")

# 🔸 Конфиг
START_DELAY_SEC      = int(os.getenv("MW_EMA_Q_START_DELAY_SEC", "120"))
RECHECK_INTERVAL_SEC = int(os.getenv("MW_EMA_Q_RECHECK_INTERVAL_SEC", "300"))
BATCH_SIZE           = int(os.getenv("MW_EMA_Q_BATCH_SIZE", "500"))
MAX_CONCURRENCY      = int(os.getenv("MW_EMA_Q_MAX_CONCURRENCY", "12"))

# 🔸 Таймфреймы/инстансы
TF_ORDER    = ("m5", "m15", "h1")
TF_STEP_SEC = {"m5": 300, "m15": 900, "h1": 3600}
MW_INST     = {"m5": 1001, "m15": 1002, "h1": 1003}
EMA_LENS    = (9, 21, 50, 100, 200)

# 🔸 SQL: кандидаты и остаток
_CANDIDATES_SQL = """
SELECT p.position_uid
FROM positions_v4 p
JOIN strategies_v4 s ON s.id = p.strategy_id
WHERE p.status = 'closed'
  AND COALESCE(p.mrk_watcher_checked, false) = true
  AND COALESCE(p.emastatus_checked, false) = true
  AND COALESCE(p.mw_emastatus_quartet_checked, false) = false
  AND s.enabled = true
  AND COALESCE(s.market_watcher, false) = true
ORDER BY p.closed_at NULLS LAST, p.id
LIMIT $1
"""

_COUNT_SQL = """
SELECT COUNT(*)
FROM positions_v4 p
JOIN strategies_v4 s ON s.id = p.strategy_id
WHERE p.status = 'closed'
  AND COALESCE(p.mrk_watcher_checked, false) = true
  AND COALESCE(p.emastatus_checked, false) = true
  AND COALESCE(p.mw_emastatus_quartet_checked, false) = false
  AND s.enabled = true
  AND COALESCE(s.market_watcher, false) = true
"""

# 🔸 Утилита: floor к началу бара TF (UTC, NAIVE)
def _floor_to_bar_open(dt_utc: datetime, tf: str) -> datetime:
    if dt_utc.tzinfo is not None:
        dt_utc = dt_utc.astimezone(timezone.utc).replace(tzinfo=None)
    step_sec = TF_STEP_SEC[tf]
    epoch = int(dt_utc.timestamp())
    floored = (epoch // step_sec) * step_sec
    return datetime.utcfromtimestamp(floored)  # naive UTC

# 🔸 Позиция (базовая загрузка)
async def _load_pos(position_uid: str):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        return await conn.fetchrow(
            """
            SELECT p.id, p.position_uid, p.symbol, p.direction, p.strategy_id,
                   p.pnl, p.status, p.created_at,
                   p.mrk_watcher_checked, p.emastatus_checked, p.mw_emastatus_quartet_checked
            FROM positions_v4 p
            WHERE p.position_uid = $1
            """,
            position_uid,
        )

# 🔸 MW-код из PIS (на баре открытия)
async def _load_mw_code_from_pis(uid: str, tf: str, bar_open: datetime):
    pg = infra.pg_pool
    inst = MW_INST[tf]
    async with pg.acquire() as conn:
        code = await conn.fetchval(
            """
            SELECT value_num
            FROM positions_indicators_stat
            WHERE position_uid=$1 AND timeframe=$2
              AND instance_id=$3 AND param_name='mw'
              AND bar_open_time=$4
              AND using_current_bar=false AND is_final=true
            """,
            uid, tf, int(inst), bar_open
        )
    return None if code is None else int(code)

# 🔸 EMA-status(m5) по доступным длинам (последние значения per param)
async def _load_ema_statuses_m5(uid: str):
    pg = infra.pg_pool
    pname_list = [f"ema{n}_status" for n in EMA_LENS]
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (param_name) param_name, value_num
            FROM positions_indicators_stat
            WHERE position_uid=$1
              AND timeframe='m5'
              AND using_current_bar=true
              AND param_name = ANY($2::text[])
            ORDER BY param_name, snapshot_at DESC
            """,
            uid, pname_list
        )
    result = {}
    for r in rows:
        pname = r["param_name"]; val = r["value_num"]
        if val is None:
            continue
        try:
            ema_len = int(pname.replace("ema", "").replace("_status", ""))
            result[ema_len] = int(val)  # 0..4
        except Exception:
            continue
    return result  # {ema_len: status_code}

# 🔸 Построить компоненты квартета: MW-триплет + EMA(m5) статусы
async def _build_quartet_components(pos) -> tuple[str | None, dict[int, int]]:
    created_at = pos["created_at"]
    created_at_utc = created_at.astimezone(timezone.utc).replace(tzinfo=None) if created_at and created_at.tzinfo is not None else created_at

    mw_codes = {}
    if created_at_utc:
        for tf in TF_ORDER:
            code = await _load_mw_code_from_pis(pos["position_uid"], tf, _floor_to_bar_open(created_at_utc, tf))
            if code is not None:
                mw_codes[tf] = int(code)
    mw_triplet = f"{mw_codes['m5']}-{mw_codes['m15']}-{mw_codes['h1']}" if all(tf in mw_codes for tf in TF_ORDER) else None

    ema_statuses_m5 = await _load_ema_statuses_m5(pos["position_uid"])  # {ema_len: 0..4}
    return mw_triplet, ema_statuses_m5

# 🔸 UPSERT квартетов под claim позиции, публикация Redis, флаг
async def _upsert_quartets_with_claim(pos, mw_triplet: str, ema_statuses_m5: dict[int, int]):
    pg = infra.pg_pool
    redis = infra.redis_client

    strategy_id = int(pos["strategy_id"])
    direction   = str(pos["direction"])
    pnl_raw     = pos["pnl"]
    pnl         = Decimal(str(pnl_raw if pnl_raw is not None else "0")).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    is_win      = 1 if pnl > Decimal("0") else 0

    # детерминированный порядок EMA-длин
    ema_items = [(k, ema_statuses_m5[k]) for k in sorted(ema_statuses_m5.keys())]

    async with pg.acquire() as conn:
        async with conn.transaction():
            # claim позиции
            claimed = await conn.fetchrow(
                """
                UPDATE positions_v4
                SET mw_emastatus_quartet_checked = true
                WHERE position_uid = $1
                  AND status = 'closed'
                  AND COALESCE(mw_emastatus_quartet_checked, false) = false
                RETURNING position_uid
                """,
                pos["position_uid"]
            )
            if not claimed:
                return ("claimed_by_other", 0)

            total_updates = 0

            # вставка-обновление агрегатов по каждой длине
            for ema_len, status_code in ema_items:
                # предсоздание
                await conn.execute(
                    """
                    INSERT INTO positions_mw_emastatus_stat_quartet
                      (strategy_id, direction, mw_triplet, ema_len, status_code,
                       closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
                    VALUES ($1,$2,$3,$4,$5, 0,0,0,0,0,NOW())
                    ON CONFLICT (strategy_id, direction, mw_triplet, ema_len, status_code) DO NOTHING
                    """,
                    strategy_id, direction, mw_triplet, int(ema_len), int(status_code)
                )

                # FOR UPDATE → пересчёт
                row = await conn.fetchrow(
                    """
                    SELECT closed_trades, won_trades, pnl_sum
                    FROM positions_mw_emastatus_stat_quartet
                    WHERE strategy_id=$1 AND direction=$2 AND mw_triplet=$3 AND ema_len=$4 AND status_code=$5
                    FOR UPDATE
                    """,
                    strategy_id, direction, mw_triplet, int(ema_len), int(status_code)
                )
                c0 = int(row["closed_trades"]); w0 = int(row["won_trades"]); s0 = Decimal(str(row["pnl_sum"]))
                c = c0 + 1
                w = w0 + is_win
                s = (s0 + pnl).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                wr = (Decimal(w) / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                ap = (s / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

                await conn.execute(
                    """
                    UPDATE positions_mw_emastatus_stat_quartet
                    SET closed_trades=$6, won_trades=$7, pnl_sum=$8, winrate=$9, avg_pnl=$10, updated_at=NOW()
                    WHERE strategy_id=$1 AND direction=$2 AND mw_triplet=$3 AND ema_len=$4 AND status_code=$5
                    """,
                    strategy_id, direction, mw_triplet, int(ema_len), int(status_code),
                    c, w, str(s), str(wr), str(ap)
                )

                # публикация Redis (best-effort)
                try:
                    await redis.set(
                        f"oracle:mw_ema:quartet:{strategy_id}:{direction}:mw:{mw_triplet}:ema{int(ema_len)}:{int(status_code)}",
                        f'{{"closed_trades": {c}, "winrate": {float(wr):.4f}}}'
                    )
                except Exception:
                    log.debug("Redis SET failed (quartet ema_len=%s)", ema_len)

                total_updates += 1

            return ("updated", total_updates)

# 🔸 Обработка одной позиции
async def _process_uid(uid: str):
    try:
        pos = await _load_pos(uid)
        if not pos or pos["status"] != "closed":
            return ("skip", "not_applicable")
        if not (pos["mrk_watcher_checked"] and pos["emastatus_checked"]) or pos["mw_emastatus_quartet_checked"]:
            return ("skip", "flags")

        mw_triplet, ema_statuses_m5 = await _build_quartet_components(pos)

        # если EMA нет вовсе — только флаг (с claim), ничего не пишем в квартеты
        if not ema_statuses_m5:
            pg = infra.pg_pool
            async with pg.acquire() as conn:
                async with conn.transaction():
                    claimed = await conn.fetchrow(
                        """
                        UPDATE positions_v4
                        SET mw_emastatus_quartet_checked = true
                        WHERE position_uid = $1
                          AND status = 'closed'
                          AND COALESCE(mw_emastatus_quartet_checked, false) = false
                        RETURNING position_uid
                        """,
                        uid
                    )
            return ("empty_zero", 1 if claimed else 0)

        # если нет MW-триплета — отложим
        if not mw_triplet:
            return ("partial", "mw_triplet_missing")

        # нормальный путь: claim + upserts
        status, n = await _upsert_quartets_with_claim(pos, mw_triplet, ema_statuses_m5)
        if status == "updated":
            return ("updated", n)
        else:
            return ("claimed", "by_other")
    except Exception as e:
        log.exception("❌ MW×EMA-Q uid=%s error: %s", uid, e)
        return ("error", "exception")

# 🔸 Пакет кандидатов / остаток
async def _fetch_candidates(n: int):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        rows = await conn.fetch(_CANDIDATES_SQL, n)
    return [r["position_uid"] for r in rows]

async def _count_remaining():
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        val = await conn.fetchval(_COUNT_SQL)
    return int(val or 0)

# 🔸 Основной цикл
async def run_oracle_mw_emastatus_quartet_aggregator():
    if START_DELAY_SEC > 0:
        log.debug("⏳ MW×EMA-Q: задержка старта %d сек (batch=%d, conc=%d)", START_DELAY_SEC, BATCH_SIZE, MAX_CONCURRENCY)
        await asyncio.sleep(START_DELAY_SEC)

    gate = asyncio.Semaphore(MAX_CONCURRENCY)

    while True:
        try:
            log.debug("🚀 MW×EMA-Q: старт прохода")
            batch_idx = 0
            tot_upd = tot_part = tot_skip = tot_claim = tot_err = 0
            tot_empty = 0

            while True:
                uids = await _fetch_candidates(BATCH_SIZE)
                if not uids:
                    break

                batch_idx += 1
                upd = part = skip = claim = err = empty = 0
                results = []

                async def worker(one_uid: str):
                    async with gate:
                        res = await _process_uid(one_uid)
                        results.append(res)

                await asyncio.gather(*[asyncio.create_task(worker(u)) for u in uids])

                for status, _ in results:
                    if status == "updated":   upd += 1
                    elif status == "partial": part += 1
                    elif status == "claimed": claim += 1
                    elif status == "skip":    skip += 1
                    elif status == "empty_zero": empty += 1
                    else:                     err  += 1

                tot_upd += upd; tot_part += part; tot_claim += claim; tot_skip += skip; tot_err += err; tot_empty += empty

                remaining = None
                if batch_idx % 5 == 1:
                    try:
                        remaining = await _count_remaining()
                    except Exception:
                        remaining = None

                # лог по батчу (INFO)
                if remaining is None:
                    log.debug("[MW×EMA-Q] batch=%d size=%d updated=%d partial=%d empty_zero=%d claimed=%d skipped=%d errors=%d",
                             batch_idx, len(uids), upd, part, empty, claim, skip, err)
                else:
                    log.debug("[MW×EMA-Q] batch=%d size=%d updated=%d partial=%d empty_zero=%d claimed=%d skipped=%d errors=%d remaining≈%d",
                             batch_idx, len(uids), upd, part, empty, claim, skip, err, remaining)

            # итог по проходу (INFO)
            log.debug("✅ [MW×EMA-Q] pass done: batches=%d updated=%d partial=%d empty_zero=%d claimed=%d skipped=%d errors=%d — next in %ds",
                     batch_idx, tot_upd, tot_part, tot_empty, tot_claim, tot_skip, tot_err, RECHECK_INTERVAL_SEC)

            await asyncio.sleep(RECHECK_INTERVAL_SEC)

        except asyncio.CancelledError:
            log.debug("⏹️ MW×EMA-Q агрегатор остановлен")
            raise
        except Exception as e:
            log.exception("❌ MW×EMA-Q loop error: %s", e)
            await asyncio.sleep(1)