# 🔸 oracle_mw_backfill.py — MarketWatcher backfill: дописываем PIS (mw) на баре открытия, агрегируем при полном комплекте; транзакционный claim против конфликтов с live

import os
import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

import infra

log = logging.getLogger("ORACLE_MW_BF")

# 🔸 Конфиг backfill'а
BATCH_SIZE           = int(os.getenv("MW_BF_BATCH_SIZE", "500"))
MAX_CONCURRENCY      = int(os.getenv("MW_BF_MAX_CONCURRENCY", "12"))          # допускаем повышенную нагрузку
SHORT_SLEEP_MS       = int(os.getenv("MW_BF_SLEEP_MS", "150"))                # пауза между батчами
START_DELAY_SEC      = int(os.getenv("MW_BF_START_DELAY_SEC", "120"))         # стартовая задержка
RECHECK_INTERVAL_SEC = int(os.getenv("MW_BF_RECHECK_INTERVAL_SEC", "300"))    # 🔁 каждые 5 минут

# 🔸 Константы/маппинги
TF_ORDER = ("m5", "m15", "h1")
TF_STEP_SEC = {"m5": 300, "m15": 900, "h1": 3600}
MW_INSTANCE_BY_TF = {"m5": 1001, "m15": 1002, "h1": 1003}
MW_CODE2STR = {
    0: "FLAT_CONS",
    1: "FLAT_EXP",
    2: "FLAT_DRIFT",
    3: "TREND_UP_ACCEL",
    4: "TREND_UP_STABLE",
    5: "TREND_UP_DECEL",
    6: "TREND_DN_ACCEL",
    7: "TREND_DN_STABLE",
    8: "TREND_DN_DECEL",
}

_CANDIDATES_SQL = """
SELECT p.position_uid
FROM positions_v4 p
JOIN strategies_v4 s ON s.id = p.strategy_id
WHERE p.status = 'closed'
  AND COALESCE(p.mrk_watcher_checked, false) = false
  AND COALESCE(p.mrk_indwatch_checked, false) = false
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
  AND COALESCE(p.mrk_watcher_checked, false) = false
  AND COALESCE(p.mrk_indwatch_checked, false) = false
  AND s.enabled = true
  AND COALESCE(s.market_watcher, false) = true
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

# 🔸 Прочитать regime_code из indicator_marketwatcher_v4
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

# 🔸 Загрузка позиции и стратегии (предварительные проверки)
async def _load_pos_and_strat(position_uid: str):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        pos = await conn.fetchrow(
            """
            SELECT p.id, p.position_uid, p.symbol, p.direction, p.strategy_id,
                   p.pnl, p.status, p.created_at,
                   COALESCE(p.mrk_indwatch_checked, false) AS pis_checked,
                   COALESCE(p.mrk_watcher_checked, false) AS agg_checked
            FROM positions_v4 p
            WHERE p.position_uid = $1
            """,
            position_uid,
        )
        if not pos or pos["status"] != "closed" or pos["agg_checked"]:
            return pos, None, ("skip", "not_applicable")

        strat = await conn.fetchrow(
            """
            SELECT id, enabled, COALESCE(archived, false) AS archived, COALESCE(market_watcher, false) AS mw
            FROM strategies_v4
            WHERE id = $1
            """,
            int(pos["strategy_id"]),
        )
        if (not strat) or (not strat["enabled"]) or bool(strat["archived"]) or (not strat["mw"]):
            return pos, strat, ("skip", "strategy_inactive_or_no_mw")
    return pos, strat, ("ok", "eligible")

# 🔸 Идемпотентно записать отсутствующие PIS (mw) по найденным TF
async def _write_missing_pis_mw(position_uid: str, strategy_id: int, direction: str, symbol: str, created_at_utc: datetime):
    per_tf_found = {}
    rows = []
    for tf in TF_ORDER:
        bar_open = _floor_to_bar_open(created_at_utc, tf)
        code = await _load_imw_code(symbol, tf, bar_open)
        if code is None:
            continue
        instance_id = MW_INSTANCE_BY_TF[tf]
        vstr = MW_CODE2STR.get(code, f"REGIME_{code}")
        rows.append((
            position_uid,
            int(strategy_id),
            direction,
            tf,
            int(instance_id),
            "mw",
            vstr,
            float(code),
            bar_open,
            None,
            None
        ))
        per_tf_found[tf] = code

    if not rows:
        return per_tf_found

    pg = infra.pg_pool
    async with pg.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(
                """
                INSERT INTO positions_indicators_stat
                  (position_uid, strategy_id, direction, timeframe,
                   instance_id, param_name, value_str, value_num,
                   bar_open_time, enabled_at, params_json,
                   using_current_bar, is_final)
                VALUES
                  ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11, false, true)
                ON CONFLICT (position_uid, timeframe, instance_id, param_name, bar_open_time)
                DO NOTHING
                """,
                rows
            )
    return per_tf_found

# 🔸 Проверить наличие всех 3 TF в PIS
async def _three_present_in_pis(position_uid: str, created_at_utc: datetime) -> bool:
    pg = infra.pg_pool
    for tf in TF_ORDER:
        bar_open = _floor_to_bar_open(created_at_utc, tf)
        instance_id = MW_INSTANCE_BY_TF[tf]
        async with pg.acquire() as conn:
            exists = await conn.fetchval(
                """
                SELECT 1
                FROM positions_indicators_stat
                WHERE position_uid=$1 AND timeframe=$2
                  AND instance_id=$3 AND param_name='mw'
                  AND bar_open_time=$4
                """,
                position_uid, tf, int(instance_id), bar_open
            )
        if not exists:
            return False
    return True

# 🔸 Детерминированные ключи агрегатов
def _ordered_agg_keys(strategy_id: int, direction: str, per_tf_codes: dict):
    per_tf_keys = []
    comp_keys = []
    for tf in TF_ORDER:
        if tf in per_tf_codes:
            per_tf_keys.append(("tf", (strategy_id, direction, tf, int(per_tf_codes[tf]))))
    if all(tf in per_tf_codes for tf in TF_ORDER):
        triplet = f"{per_tf_codes['m5']}-{per_tf_codes['m15']}-{per_tf_codes['h1']}"
        comp_keys.append(("comp", (strategy_id, direction, triplet)))
    return per_tf_keys, comp_keys

# 🔸 Агрегация под транзакционным claim'ом позиции (исключает конфликт с live)
async def _aggregate_with_claim(pos, per_tf_codes: dict):
    pg = infra.pg_pool
    redis = infra.redis_client

    strategy_id = int(pos["strategy_id"])
    direction   = str(pos["direction"])
    pnl_raw     = pos["pnl"]
    pnl         = Decimal(str(pnl_raw if pnl_raw is not None else "0")).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    is_win      = 1 if pnl > Decimal("0") else 0

    per_tf_keys, comp_keys = _ordered_agg_keys(strategy_id, direction, per_tf_codes or {})

    async with pg.acquire() as conn:
        async with conn.transaction():
            claimed = await conn.fetchrow(
                """
                UPDATE positions_v4
                SET mrk_watcher_checked = true,
                    mrk_indwatch_checked = true
                WHERE position_uid = $1
                  AND status = 'closed'
                  AND COALESCE(mrk_watcher_checked, false) = false
                RETURNING position_uid
                """,
                pos["position_uid"]
            )
            if not claimed:
                return ("claimed_by_other", 0, 0)

            for _, key in per_tf_keys:
                s_id, dir_, tf, code = key
                await conn.execute(
                    """
                    INSERT INTO positions_mw_stat_tf
                      (strategy_id, direction, timeframe, status_code,
                       closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
                    VALUES ($1,$2,$3,$4, 0,0,0,0,0,NOW())
                    ON CONFLICT (strategy_id,direction,timeframe,status_code) DO NOTHING
                    """,
                    s_id, dir_, tf, code
                )
            for _, key in comp_keys:
                s_id, dir_, triplet = key
                await conn.execute(
                    """
                    INSERT INTO positions_mw_stat_comp
                      (strategy_id, direction, status_triplet,
                       closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
                    VALUES ($1,$2,$3, 0,0,0,0,0,NOW())
                    ON CONFLICT (strategy_id,direction,status_triplet) DO NOTHING
                    """,
                    s_id, dir_, triplet
                )

            updated_tf = 0
            for _, key in per_tf_keys:
                s_id, dir_, tf, code = key
                row = await conn.fetchrow(
                    """
                    SELECT closed_trades, won_trades, pnl_sum
                    FROM positions_mw_stat_tf
                    WHERE strategy_id=$1 AND direction=$2 AND timeframe=$3 AND status_code=$4
                    FOR UPDATE
                    """,
                    s_id, dir_, tf, code
                )
                c0 = int(row["closed_trades"]); w0 = int(row["won_trades"]); s0 = Decimal(str(row["pnl_sum"]))
                c = c0 + 1
                w = w0 + is_win
                s = (s0 + pnl).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                wr = (Decimal(w) / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                ap = (s / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

                await conn.execute(
                    """
                    UPDATE positions_mw_stat_tf
                    SET closed_trades=$5, won_trades=$6, pnl_sum=$7, winrate=$8, avg_pnl=$9, updated_at=NOW()
                    WHERE strategy_id=$1 AND direction=$2 AND timeframe=$3 AND status_code=$4
                    """,
                    s_id, dir_, tf, code,
                    c, w, str(s), str(wr), str(ap)
                )
                updated_tf += 1

                try:
                    await redis.set(
                        f"oracle:mw:tf:{s_id}:{dir_}:{tf}:mw:{code}",
                        f'{{"closed_trades": {c}, "winrate": {float(wr):.4f}}}'
                    )
                except Exception:
                    log.debug("Redis SET failed (per-TF)")

            updated_comp = 0
            for _, key in comp_keys:
                s_id, dir_, triplet = key
                row = await conn.fetchrow(
                    """
                    SELECT closed_trades, won_trades, pnl_sum
                    FROM positions_mw_stat_comp
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
                    UPDATE positions_mw_stat_comp
                    SET closed_trades=$4, won_trades=$5, pnl_sum=$6, winrate=$7, avg_pnl=$8, updated_at=NOW()
                    WHERE strategy_id=$1 AND direction=$2 AND status_triplet=$3
                    """,
                    s_id, dir_, triplet,
                    c, w, str(s), str(wr), str(ap)
                )
                updated_comp += 1

                try:
                    await redis.set(
                        f"oracle:mw:comp:{s_id}:{dir_}:mw:{triplet}",
                        f'{{"closed_trades": {c}, "winrate": {float(wr):.4f}}}'
                    )
                except Exception:
                    log.debug("Redis SET failed (comp)")

            return ("aggregated", updated_tf, updated_comp)

# 🔸 Обработка одного UID
async def _process_uid(uid: str):
    try:
        pos, strat, verdict = await _load_pos_and_strat(uid)
        if not pos:
            return ("skip", "pos_not_found")
        v_code, v_reason = verdict
        if v_code != "ok":
            return ("skip", v_reason)

        created_at = pos["created_at"]  # timestamp (naive UTC по схеме)
        if created_at.tzinfo is not None:
            created_at_utc = created_at.astimezone(timezone.utc).replace(tzinfo=None)
        else:
            created_at_utc = created_at

        per_tf_now = await _write_missing_pis_mw(
            pos["position_uid"], pos["strategy_id"], pos["direction"], pos["symbol"], created_at_utc
        )

        ready = await _three_present_in_pis(pos["position_uid"], created_at_utc)
        if not ready:
            return ("pis_partial", "/".join(sorted(per_tf_now.keys())) if per_tf_now else "-")

        per_tf_codes = {
            tf: await _load_imw_code(pos["symbol"], tf, _floor_to_bar_open(created_at_utc, tf))
            for tf in TF_ORDER
        }

        agg_status, updated_tf, updated_comp = await _aggregate_with_claim(pos, per_tf_codes)
        if agg_status == "aggregated":
            win_flag = 1 if (pos["pnl"] is not None and pos["pnl"] > 0) else 0
            return ("aggregated", f"tf={updated_tf} comp={updated_comp} win={win_flag}")
        else:
            return ("claimed", "by_other")

    except Exception as e:
        log.exception("❌ MW-BF uid=%s error: %s", uid, e)
        return ("error", "exception")

# 🔸 Выборка пачки UID'ов
async def _fetch_candidates(batch_size: int):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        rows = await conn.fetch(_CANDIDATES_SQL, batch_size)
    return [r["position_uid"] for r in rows]

# 🔸 Подсчёт оставшихся (для периодических отчётов)
async def _count_remaining():
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        val = await conn.fetchval(_COUNT_SQL)
    return int(val or 0)

# 🔸 Основной цикл backfill'а
async def run_oracle_mw_backfill():
    if START_DELAY_SEC > 0:
        log.info("⏳ MW-BF: задержка старта %d сек (batch=%d, conc=%d)", START_DELAY_SEC, BATCH_SIZE, MAX_CONCURRENCY)
        await asyncio.sleep(START_DELAY_SEC)

    gate = asyncio.Semaphore(MAX_CONCURRENCY)

    while True:
        try:
            log.info("🚀 MW-BF: старт прохода")
            batch_idx = 0
            total_agg = total_partial = total_skip = total_claim = total_err = 0

            while True:
                uids = await _fetch_candidates(BATCH_SIZE)
                if not uids:
                    break

                batch_idx += 1
                agg = partial = skip = claim = err = 0
                results = []

                async def worker(one_uid: str):
                    async with gate:
                        res = await _process_uid(one_uid)
                        results.append(res)

                await asyncio.gather(*[asyncio.create_task(worker(u)) for u in uids])

                for status, _ in results:
                    if status == "aggregated":
                        agg += 1
                    elif status == "pis_partial":
                        partial += 1
                    elif status == "claimed":
                        claim += 1
                    elif status == "skip":
                        skip += 1
                    else:
                        err += 1

                total_agg += agg
                total_partial += partial
                total_claim += claim
                total_skip += skip
                total_err += err

                remaining = None
                if batch_idx % 5 == 1:
                    try:
                        remaining = await _count_remaining()
                    except Exception:
                        remaining = None

                if remaining is None:
                    log.info("[MW-BF] batch=%d size=%d aggregated=%d partial=%d claimed=%d skipped=%d errors=%d",
                             batch_idx, len(uids), agg, partial, claim, skip, err)
                else:
                    log.info("[MW-BF] batch=%d size=%d aggregated=%d partial=%d claimed=%d skipped=%d errors=%d remaining≈%d",
                             batch_idx, len(uids), agg, partial, claim, skip, err, remaining)

                await asyncio.sleep(SHORT_SLEEP_MS / 1000)

            log.info("✅ MW-BF: проход завершён batches=%d aggregated=%d partial=%d claimed=%d skipped=%d errors=%d — следующий запуск через %ds",
                     batch_idx, total_agg, total_partial, total_claim, total_skip, total_err, RECHECK_INTERVAL_SEC)

            await asyncio.sleep(RECHECK_INTERVAL_SEC)

        except asyncio.CancelledError:
            log.info("⏹️ MW-BF остановлен")
            raise
        except Exception as e:
            log.exception("❌ MW-BF loop error: %s", e)
            await asyncio.sleep(1)