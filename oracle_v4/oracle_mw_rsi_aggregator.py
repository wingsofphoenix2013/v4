# 🔸 oracle_mw_rsi_aggregator.py — Sextet (MW×RSI) агрегатор: скан позиций, сбор MW+RSI триплетов, UPSERT секстета, публикация Redis, флаг mw_rsi_checked

import os
import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

import infra

log = logging.getLogger("ORACLE_MW_RSI")

# 🔸 Конфиг сканера
BATCH_SIZE           = int(os.getenv("MW_RSI_BATCH_SIZE", "500"))
MAX_CONCURRENCY      = int(os.getenv("MW_RSI_MAX_CONCURRENCY", "15"))
START_DELAY_SEC      = int(os.getenv("MW_RSI_START_DELAY_SEC", "120"))
RECHECK_INTERVAL_SEC = int(os.getenv("MW_RSI_RECHECK_INTERVAL_SEC", "300"))  # каждые 5 минут

TF_ORDER = ("m5", "m15", "h1")
TF_STEP_SEC = {"m5": 300, "m15": 900, "h1": 3600}
MW_INSTANCE_BY_TF = {"m5": 1001, "m15": 1002, "h1": 1003}
RSI_PARAM = "rsi14"  # длина RSI фиксирована = 14

# 🔸 Кандидаты: только закрытые, MW включена, оба флага (mw и rsi) уже true, но sextet ещё нет
_CANDIDATES_SQL = """
SELECT p.position_uid
FROM positions_v4 p
JOIN strategies_v4 s ON s.id = p.strategy_id
WHERE p.status = 'closed'
  AND s.enabled = true
  AND COALESCE(s.market_watcher, false) = true
  AND COALESCE(p.mrk_watcher_checked, false) = true
  AND COALESCE(p.rsi_checked, false) = true
  AND COALESCE(p.mw_rsi_checked, false) = false
ORDER BY p.closed_at NULLS LAST, p.id
LIMIT $1
"""

# 🔸 Подсчёт остатка
_COUNT_SQL = """
SELECT COUNT(*)
FROM positions_v4 p
JOIN strategies_v4 s ON s.id = p.strategy_id
WHERE p.status = 'closed'
  AND s.enabled = true
  AND COALESCE(s.market_watcher, false) = true
  AND COALESCE(p.mrk_watcher_checked, false) = true
  AND COALESCE(p.rsi_checked, false) = true
  AND COALESCE(p.mw_rsi_checked, false) = false
"""

# 🔸 Утилита: floor к началу бара TF (UTC, NAIVE)
def _floor_to_bar_open(dt_utc: datetime, tf: str) -> datetime:
    if dt_utc.tzinfo is not None:
        dt_utc = dt_utc.astimezone(timezone.utc).replace(tzinfo=None)
    step_sec = TF_STEP_SEC[tf]
    epoch = int(dt_utc.timestamp())  # трактуем как UTC
    floored = (epoch // step_sec) * step_sec
    return datetime.utcfromtimestamp(floored)  # naive UTC

# 🔸 Загрузка позиции/стратегии (без claim — он на этапе записи секстета)
async def _load_pos(position_uid: str):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        pos = await conn.fetchrow(
            """
            SELECT p.id, p.position_uid, p.symbol, p.direction, p.strategy_id,
                   p.pnl, p.status, p.created_at,
                   p.mrk_watcher_checked, p.rsi_checked, p.mw_rsi_checked
            FROM positions_v4 p
            WHERE p.position_uid = $1
            """,
            position_uid,
        )
    return pos

# 🔸 Прочитать MW-код из PIS по TF (на баре открытия)
async def _load_mw_code_from_pis(uid: str, tf: str, bar_open: datetime):
    pg = infra.pg_pool
    inst = MW_INSTANCE_BY_TF[tf]
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

# 🔸 Прочитать RSI-значение из PIS и забиннить по 5 (0..95, 100→95)
def _rsi_to_bin(x: float) -> int:
    v = max(0.0, min(100.0, float(x)))
    b = int(v // 5) * 5
    return 95 if b == 100 else b

async def _load_rsi_bin_from_pis(uid: str, tf: str):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        val = await conn.fetchval(
            """
            SELECT value_num
            FROM positions_indicators_stat
            WHERE position_uid=$1 AND timeframe=$2
              AND param_name=$3
              AND using_current_bar=true
            ORDER BY snapshot_at DESC
            LIMIT 1
            """,
            uid, tf, RSI_PARAM
        )
    return None if val is None else _rsi_to_bin(float(val))

# 🔸 Собрать MW-триплет и RSI-триплет
async def _build_triplets(pos) -> tuple[str | None, str | None]:
    created_at = pos["created_at"]
    created_at_utc = created_at.astimezone(timezone.utc).replace(tzinfo=None) if created_at.tzinfo is not None else created_at

    # MW по бару открытия
    mw_codes = {}
    for tf in TF_ORDER:
        code = await _load_mw_code_from_pis(pos["position_uid"], tf, _floor_to_bar_open(created_at_utc, tf))
        if code is not None:
            mw_codes[tf] = int(code)
    mw_triplet = None
    if all(tf in mw_codes for tf in TF_ORDER):
        mw_triplet = f"{mw_codes['m5']}-{mw_codes['m15']}-{mw_codes['h1']}"

    # RSI по текущему срезу PIS (using_current_bar=true) — по каждой TF
    rsi_bins = {}
    for tf in TF_ORDER:
        b = await _load_rsi_bin_from_pis(pos["position_uid"], tf)
        if b is not None:
            rsi_bins[tf] = int(b)
    rsi_triplet = None
    if all(tf in rsi_bins for tf in TF_ORDER):
        rsi_triplet = f"{rsi_bins['m5']}-{rsi_bins['m15']}-{rsi_bins['h1']}"

    return mw_triplet, rsi_triplet

# 🔸 UPSERT секстета под claim позиции (исключаем гонки), публикация Redis, выставление mw_rsi_checked
async def _upsert_sextet_with_claim(pos, mw_triplet: str, rsi_triplet: str):
    pg = infra.pg_pool
    redis = infra.redis_client

    strategy_id = int(pos["strategy_id"])
    direction   = str(pos["direction"])
    pnl_raw     = pos["pnl"]
    pnl         = Decimal(str(pnl_raw if pnl_raw is not None else "0")).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    is_win      = 1 if pnl > Decimal("0") else 0

    async with pg.acquire() as conn:
        async with conn.transaction():
            # claim: отмечаем позицию как обработанную по секстетам, если ещё нет
            claimed = await conn.fetchrow(
                """
                UPDATE positions_v4
                SET mw_rsi_checked = true
                WHERE position_uid = $1
                  AND status = 'closed'
                  AND COALESCE(mw_rsi_checked, false) = false
                RETURNING position_uid
                """,
                pos["position_uid"]
            )
            if not claimed:
                return ("claimed_by_other", 0)

            # предсоздание строки секстета
            await conn.execute(
                """
                INSERT INTO positions_rsi_mw_stat_sextet
                  (strategy_id, direction, mw_triplet, rsi_triplet,
                   closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
                VALUES ($1,$2,$3,$4, 0,0,0,0,0,NOW())
                ON CONFLICT (strategy_id, direction, mw_triplet, rsi_triplet) DO NOTHING
                """,
                strategy_id, direction, mw_triplet, rsi_triplet
            )

            # захват и апдейт
            row = await conn.fetchrow(
                """
                SELECT closed_trades, won_trades, pnl_sum
                FROM positions_rsi_mw_stat_sextet
                WHERE strategy_id=$1 AND direction=$2 AND mw_triplet=$3 AND rsi_triplet=$4
                FOR UPDATE
                """,
                strategy_id, direction, mw_triplet, rsi_triplet
            )
            c0 = int(row["closed_trades"]); w0 = int(row["won_trades"]); s0 = Decimal(str(row["pnl_sum"]))
            c = c0 + 1
            w = w0 + is_win
            s = (s0 + pnl).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
            wr = (Decimal(w) / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
            ap = (s / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

            await conn.execute(
                """
                UPDATE positions_rsi_mw_stat_sextet
                SET closed_trades=$5, won_trades=$6, pnl_sum=$7, winrate=$8, avg_pnl=$9, updated_at=NOW()
                WHERE strategy_id=$1 AND direction=$2 AND mw_triplet=$3 AND rsi_triplet=$4
                """,
                strategy_id, direction, mw_triplet, rsi_triplet,
                c, w, str(s), str(wr), str(ap)
            )

            try:
                # 🔸 Redis-ключ по твоему формату
                await redis.set(
                    f"oracle:rsi_mw:comp:{strategy_id}:{direction}:mw:{mw_triplet}:{rsi_triplet}",
                    f'{{"closed_trades": {c}, "winrate": {float(wr):.4f}}}'
                )
            except Exception:
                log.debug("Redis SET failed (sextet)")

            return ("updated", c)

# 🔸 Обработка одного UID
async def _process_uid(uid: str):
    try:
        pos = await _load_pos(uid)
        if not pos or pos["status"] != "closed":
            return ("skip", "not_applicable")
        if not (pos["mrk_watcher_checked"] and pos["rsi_checked"]) or pos["mw_rsi_checked"]:
            return ("skip", "flags")

        mw_triplet, rsi_triplet = await _build_triplets(pos)
        if not mw_triplet or not rsi_triplet:
            return ("partial", f"mw={bool(mw_triplet)} rsi={bool(rsi_triplet)}")

        status, trades = await _upsert_sextet_with_claim(pos, mw_triplet, rsi_triplet)
        if status == "updated":
            win_flag = 1 if (pos["pnl"] is not None and pos["pnl"] > 0) else 0
            log.info("[MW×RSI] uid=%s strat=%s dir=%s mw=%s rsi=%s win=%d",
                     uid, pos["strategy_id"], pos["direction"], mw_triplet, rsi_triplet, win_flag)
            return ("updated", trades)
        else:
            return ("claimed", "by_other")
    except Exception as e:
        log.exception("❌ MW×RSI uid=%s error: %s", uid, e)
        return ("error", "exception")

# 🔸 Пакет кандидатов
async def _fetch_candidates(batch_size: int):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        rows = await conn.fetch(_CANDIDATES_SQL, batch_size)
    return [r["position_uid"] for r in rows]

# 🔸 Остаток
async def _count_remaining():
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        val = await conn.fetchval(_COUNT_SQL)
    return int(val or 0)

# 🔸 Основной цикл
async def run_oracle_mw_rsi_aggregator():
    # стартовая задержка
    if START_DELAY_SEC > 0:
        log.info("⏳ MW×RSI: задержка старта %d сек (batch=%d, conc=%d)", START_DELAY_SEC, BATCH_SIZE, MAX_CONCURRENCY)
        await asyncio.sleep(START_DELAY_SEC)

    gate = asyncio.Semaphore(MAX_CONCURRENCY)

    while True:
        try:
            log.info("🚀 MW×RSI: старт прохода")
            batch_idx = 0
            tot_upd = tot_part = tot_skip = tot_claim = tot_err = 0

            while True:
                uids = await _fetch_candidates(BATCH_SIZE)
                if not uids:
                    break

                batch_idx += 1
                upd = part = skip = claim = err = 0
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
                    else:                     err  += 1

                tot_upd += upd; tot_part += part; tot_claim += claim; tot_skip += skip; tot_err += err

                remaining = None
                if batch_idx % 5 == 1:
                    try:
                        remaining = await _count_remaining()
                    except Exception:
                        remaining = None

                if remaining is None:
                    log.info("[MW×RSI] batch=%d size=%d updated=%d partial=%d claimed=%d skipped=%d errors=%d",
                             batch_idx, len(uids), upd, part, claim, skip, err)
                else:
                    log.info("[MW×RSI] batch=%d size=%d updated=%d partial=%d claimed=%d skipped=%d errors=%d remaining≈%d",
                             batch_idx, len(uids), upd, part, claim, skip, err, remaining)

            log.info("✅ MW×RSI: проход завершён batches=%d updated=%d partial=%d claimed=%d skipped=%d errors=%d — следующий запуск через %ds",
                     batch_idx, tot_upd, tot_part, tot_claim, tot_skip, tot_err, RECHECK_INTERVAL_SEC)

            await asyncio.sleep(RECHECK_INTERVAL_SEC)

        except asyncio.CancelledError:
            log.info("⏹️ MW×RSI агрегатор остановлен")
            raise
        except Exception as e:
            log.exception("❌ MW×RSI loop error: %s", e)
            await asyncio.sleep(1)