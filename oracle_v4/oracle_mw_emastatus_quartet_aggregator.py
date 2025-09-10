# 🔸 oracle_mw_emastatus_quartet_aggregator.py — MW×EMA-status(m5) квартеты:
# скан позиций, проверка что есть MW-триплет и все 5 EMA-status(m5), UPSERT агрегатов по каждой длине, Redis, флаг

import os
import asyncio
import logging
from decimal import Decimal, ROUND_HALF_UP

import infra

log = logging.getLogger("ORACLE_MW_EMASTATUS_Q")

# 🔸 Конфиг сканера
BATCH_SIZE           = int(os.getenv("MW_EMASTATUS_Q_BATCH_SIZE", "500"))
MAX_CONCURRENCY      = int(os.getenv("MW_EMASTATUS_Q_MAX_CONCURRENCY", "15"))
START_DELAY_SEC      = int(os.getenv("MW_EMASTATUS_Q_START_DELAY_SEC", "120"))
RECHECK_INTERVAL_SEC = int(os.getenv("MW_EMASTATUS_Q_RECHECK_INTERVAL_SEC", "300"))  # каждые 5 минут

TF_ORDER = ("m5", "m15", "h1")
MW_INST  = {"m5": 1001, "m15": 1002, "h1": 1003}
EMA_LENS = [9, 21, 50, 100, 200]  # 5 длин на m5

# 🔸 Кандидаты: закрытые, MW и EMA-status готовы, квартет ещё нет
_CANDIDATES_SQL = """
SELECT p.position_uid
FROM positions_v4 p
JOIN strategies_v4 s ON s.id = p.strategy_id
WHERE p.status = 'closed'
  AND s.enabled = true
  AND COALESCE(s.market_watcher, false) = true
  AND COALESCE(p.mrk_watcher_checked, false) = true
  AND COALESCE(p.emastatus_checked, false) = true
  AND COALESCE(p.mw_emastatus_quartet_checked, false) = false
ORDER BY p.closed_at NULLS LAST, p.id
LIMIT $1
"""

_COUNT_SQL = """
SELECT COUNT(*)
FROM positions_v4 p
JOIN strategies_v4 s ON s.id = p.strategy_id
WHERE p.status = 'closed'
  AND s.enabled = true
  AND COALESCE(s.market_watcher, false) = true
  AND COALESCE(p.mrk_watcher_checked, false) = true
  AND COALESCE(p.emastatus_checked, false) = true
  AND COALESCE(p.mw_emastatus_quartet_checked, false) = false
"""

# 🔸 Позиция
async def _load_pos(position_uid: str):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        return await conn.fetchrow(
            """
            SELECT p.id, p.position_uid, p.symbol, p.direction, p.strategy_id,
                   p.pnl, p.status, p.mrk_watcher_checked, p.emastatus_checked, p.mw_emastatus_quartet_checked
            FROM positions_v4 p
            WHERE p.position_uid = $1
            """,
            position_uid,
        )

# 🔸 MW-коды из PIS (final, по инстансам 1001/1002/1003)
async def _load_mw_code_from_pis(uid: str, tf: str):
    pg = infra.pg_pool
    inst = MW_INST[tf]
    async with pg.acquire() as conn:
        code = await conn.fetchval(
            """
            SELECT value_num
            FROM positions_indicators_stat
            WHERE position_uid=$1 AND timeframe=$2
              AND instance_id=$3 AND param_name='mw'
              AND using_current_bar=false AND is_final=true
            ORDER BY snapshot_at DESC
            LIMIT 1
            """,
            uid, tf, int(inst)
        )
    return None if code is None else int(code)

# 🔸 Считать EMA-status на m5 для всех 5 длин (final) → словарь {L: status_code}
async def _load_emastatus_map_m5(uid: str):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT param_name, value_num
            FROM positions_indicators_stat
            WHERE position_uid=$1 AND timeframe='m5'
              AND using_current_bar=false AND is_final=true
              AND param_name IN ('ema9_status','ema21_status','ema50_status','ema100_status','ema200_status')
            """,
            uid
        )
    got = {}
    for r in rows:
        name = r["param_name"]
        v = r["value_num"]
        if v is None:
            continue
        try:
            L = int(name.replace("ema","").replace("_status",""))
            if L in EMA_LENS:
                got[L] = int(v)
        except Exception:
            continue
    return got  # ожидаем все 5 ключей

# 🔸 Собрать MW-триплет и EMA-status карту
async def _build_quartet_components(pos):
    # MW
    mw_codes = {}
    for tf in TF_ORDER:
        code = await _load_mw_code_from_pis(pos["position_uid"], tf)
        if code is not None:
            mw_codes[tf] = int(code)
    mw_triplet = f"{mw_codes['m5']}-{mw_codes['m15']}-{mw_codes['h1']}" if all(tf in mw_codes for tf in TF_ORDER) else None

    # EMA-status m5
    ems_map = await _load_emastatus_map_m5(pos["position_uid"])
    all_five = all(L in ems_map for L in EMA_LENS)

    return mw_triplet, ems_map, all_five

# 🔸 UPSERT квартетов по каждой длине под claim позиции, публикация Redis, флаг (только если есть все 5 длин)
async def _upsert_quartets_with_claim(pos, mw_triplet: str, ems_map: dict):
    pg = infra.pg_pool
    redis = infra.redis_client

    strategy_id = int(pos["strategy_id"])
    direction   = str(pos["direction"])
    pnl_raw     = pos["pnl"]
    pnl         = Decimal(str(pnl_raw if pnl_raw is not None else "0")).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    is_win      = 1 if pnl > Decimal("0") else 0

    async with pg.acquire() as conn:
        async with conn.transaction():
            # claim позиции на квартеты
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

            total_upd = 0
            for L in EMA_LENS:
                code = int(ems_map[L])  # безопасно, тк проверили наличие ранее

                # предсоздать строку
                await conn.execute(
                    """
                    INSERT INTO positions_mw_emastatus_stat_quartet
                      (strategy_id, direction, mw_triplet, ema_len, status_code,
                       closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
                    VALUES ($1,$2,$3,$4,$5, 0,0,0,0,0,NOW())
                    ON CONFLICT (strategy_id, direction, mw_triplet, ema_len, status_code) DO NOTHING
                    """,
                    strategy_id, direction, mw_triplet, int(L), code
                )

                # захват и апдейт
                row = await conn.fetchrow(
                    """
                    SELECT closed_trades, won_trades, pnl_sum
                    FROM positions_mw_emastatus_stat_quartet
                    WHERE strategy_id=$1 AND direction=$2 AND mw_triplet=$3 AND ema_len=$4 AND status_code=$5
                    FOR UPDATE
                    """,
                    strategy_id, direction, mw_triplet, int(L), code
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
                    strategy_id, direction, mw_triplet, int(L), code,
                    c, w, str(s), str(wr), str(ap)
                )
                total_upd += 1

                # Redis
                try:
                    await redis.set(
                        f"oracle:mw_emastatus:quartet:{strategy_id}:{direction}:mw:{mw_triplet}:ema{int(L)}:{code}",
                        f'{{"closed_trades": {c}, "winrate": {float(wr):.4f}}}'
                    )
                except Exception:
                    log.debug("Redis SET failed (quartet)")

            return ("updated", total_upd)

# 🔸 Обработка одной позиции
async def _process_uid(uid: str):
    try:
        pos = await _load_pos(uid)
        if not pos or pos["status"] != "closed":
            return ("skip", "not_applicable")
        if not (pos["mrk_watcher_checked"] and pos["emastatus_checked"]) or pos["mw_emastatus_quartet_checked"]:
            return ("skip", "flags")

        mw_triplet, ems_map, all_five = await _build_quartet_components(pos)
        if (not mw_triplet) or (not all_five):
            # нужно дождаться наличия MW m5/m15/h1 и всех 5 ema{L}_status на m5
            return ("partial", f"mw={bool(mw_triplet)} ema5={len(ems_map)}/5")

        status, total_upd = await _upsert_quartets_with_claim(pos, mw_triplet, ems_map)
        if status == "updated":
            win_flag = 1 if (pos["pnl"] is not None and pos["pnl"] > 0) else 0
            log.debug("[MW×EMAStatus-Q] uid=%s strat=%s dir=%s mw=%s ema_count=%d win=%d",
                     uid, pos["strategy_id"], pos["direction"], mw_triplet, total_upd, win_flag)
            return ("updated", total_upd)
        else:
            return ("claimed", "by_other")

    except Exception as e:
        log.exception("❌ MW×EMAStatus-Q uid=%s error: %s", uid, e)
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
        log.debug("⏳ MW×EMAStatus-Q: задержка старта %d сек (batch=%d, conc=%d)", START_DELAY_SEC, BATCH_SIZE, MAX_CONCURRENCY)
        await asyncio.sleep(START_DELAY_SEC)

    gate = asyncio.Semaphore(MAX_CONCURRENCY)

    while True:
        try:
            log.debug("🚀 MW×EMAStatus-Q: старт прохода")
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
                    log.debug("[MW×EMAStatus-Q] batch=%d size=%d updated=%d partial=%d claimed=%d skipped=%d errors=%d",
                             batch_idx, len(uids), upd, part, claim, skip, err)
                else:
                    log.debug("[MW×EMAStatus-Q] batch=%d size=%d updated=%d partial=%d claimed=%d skipped=%d errors=%d remaining≈%d",
                             batch_idx, len(uids), upd, part, claim, skip, err, remaining)

            log.debug("✅ MW×EMAStatus-Q: проход завершён batches=%d updated=%d partial=%d claimed=%d skipped=%d errors=%d — следующий запуск через %ds",
                     batch_idx, tot_upd, tot_part, tot_claim, tot_skip, tot_err, RECHECK_INTERVAL_SEC)

            await asyncio.sleep(RECHECK_INTERVAL_SEC)

        except asyncio.CancelledError:
            log.debug("⏹️ MW×EMAStatus-Q агрегатор остановлен")
            raise
        except Exception as e:
            log.exception("❌ MW×EMAStatus-Q loop error: %s", e)
            await asyncio.sleep(1)