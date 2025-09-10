# 🔸 oracle_mw_aggregator.py — MarketWatcher: запись MW-срезов (PIS) на баре открытия + агрегация при наличии всех трёх TF (naive UTC timestamps)

import os
import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

import infra

log = logging.getLogger("ORACLE_MW_AGG")

# 🔸 Конфиг consumer-группы и чтения
STREAM_NAME   = os.getenv("ORACLE_MW_STREAM",   "signal_log_queue")
GROUP_NAME    = os.getenv("ORACLE_MW_GROUP",    "oracle_mw_aggregator")
CONSUMER_NAME = os.getenv("ORACLE_MW_CONSUMER", "oracle_mw_1")
XREAD_COUNT   = int(os.getenv("ORACLE_MW_COUNT",    "50"))
XREAD_BLOCKMS = int(os.getenv("ORACLE_MW_BLOCK_MS", "1000"))

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


# 🔸 Утилита: floor к началу бара TF (UTC, NAIVE)
def _floor_to_bar_open(dt_utc: datetime, tf: str) -> datetime:
    """
    Принимает datetime в UTC. Возвращает NAIVE UTC datetime (tzinfo=None).
    """
    # приводим к naive UTC
    if dt_utc.tzinfo is not None:
        dt_utc = dt_utc.astimezone(timezone.utc).replace(tzinfo=None)
    step = TF_STEP_SEC[tf]
    epoch = int(dt_utc.timestamp())  # трактуется как UTC для naive datetime
    floored = (epoch // step) * step
    return datetime.utcfromtimestamp(floored)  # naive UTC


# 🔸 Идемпотентно создать consumer-group
async def _ensure_group():
    try:
        await infra.redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, id="$", mkstream=True)
        log.info("✅ Consumer group '%s' создана на '%s'", GROUP_NAME, STREAM_NAME)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info("ℹ️ Consumer group '%s' уже существует", GROUP_NAME)
        else:
            log.exception("❌ Ошибка создания consumer group: %s", e)
            raise


# 🔸 Загрузка позиции и стратегии (проверки флагов)
async def _load_position_and_strategy(position_uid: str):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        async with conn.transaction():
            pos = await conn.fetchrow(
                """
                SELECT p.id, p.position_uid, p.symbol, p.direction, p.strategy_id,
                       p.pnl, p.status, p.created_at,
                       COALESCE(p.mrk_indwatch_checked, false) AS pis_checked,
                       COALESCE(p.mrk_watcher_checked, false) AS agg_checked
                FROM positions_v4 p
                WHERE p.position_uid = $1
                FOR UPDATE
                """,
                position_uid,
            )
            if not pos:
                return None, None, ("skip", "position_not_found")
            if pos["status"] != "closed":
                return pos, None, ("skip", "position_not_closed")
            if pos["agg_checked"]:
                return pos, None, ("skip", "already_aggregated")

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


# 🔸 Прочитать regime_code из indicator_marketwatcher_v4 по (symbol, TF, bar_open_time)
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


#   Сформировать строки для PIS (пишем только найденные TF)
async def _write_pis_mw(position_uid: str, strategy_id: int, direction: str, symbol: str, created_at_utc: datetime):
    """
    Возвращает: dict per_tf_found: {'m5': code?, 'm15': code?, 'h1': code?} — только найденные TF
    """
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
            bar_open,       # bar_open_time (naive UTC)
            None,           # enabled_at
            None            # params_json
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


# 🔸 Проверить: есть ли в PIS все 3 TF для 'mw' на баре открытия
async def _check_all_three_present(position_uid: str, created_at_utc: datetime):
    pg = infra.pg_pool
    per_tf_ok = {}
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
        per_tf_ok[tf] = bool(exists)
    return all(per_tf_ok.values())


# 🔸 Подготовка ключей агрегатов (детерминированный порядок)
def _ordered_keys_for_agg(strategy_id: int, direction: str, per_tf_codes: dict):
    per_tf_keys = []
    comp_keys = []
    for tf in TF_ORDER:
        if tf in per_tf_codes:
            per_tf_keys.append(("tf", (strategy_id, direction, tf, int(per_tf_codes[tf]))))
    if all(tf in per_tf_codes for tf in TF_ORDER):
        triplet = f"{per_tf_codes['m5']}-{per_tf_codes['m15']}-{per_tf_codes['h1']}"
        comp_keys.append(("comp", (strategy_id, direction, triplet)))
    return per_tf_keys, comp_keys


# 🔸 Агрегация MW (per-TF и композит) + публикация Redis (только если есть все 3 TF) + выставление флагов
async def _aggregate_and_mark(pos, per_tf_codes: dict):
    """
    ВАЖНО: вызывать только если в PIS найдены все 3 TF (иначе агрегацию не делаем).
    """
    pg = infra.pg_pool
    redis = infra.redis_client

    strategy_id = int(pos["strategy_id"])
    direction   = str(pos["direction"])
    pnl_raw     = pos["pnl"]
    pnl         = Decimal(str(pnl_raw if pnl_raw is not None else "0")).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    is_win      = 1 if pnl > Decimal("0") else 0

    per_tf_keys, comp_keys = _ordered_keys_for_agg(strategy_id, direction, per_tf_codes or {})

    async with pg.acquire() as conn:
        async with conn.transaction():
            # предсоздание (DO NOTHING)
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

            # апдейты (FOR UPDATE → UPDATE) в фиксированном порядке
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

                try:
                    await redis.set(
                        f"oracle:mw:tf:{s_id}:{dir_}:{tf}:mw:{code}",
                        f'{{"closed_trades": {c}, "winrate": {float(wr):.4f}}}'
                    )
                except Exception:
                    log.debug("Redis SET failed (per-TF)")

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

                try:
                    await redis.set(
                        f"oracle:mw:comp:{s_id}:{dir_}:mw:{triplet}",
                        f'{{"closed_trades": {c}, "winrate": {float(wr):.4f}}}'
                    )
                except Exception:
                    log.debug("Redis SET failed (comp)")

            # флаги: все 3 TF есть → отмечаем PIS-флаг и итоговый флаг агрегации
            await conn.execute(
                "UPDATE positions_v4 SET mrk_indwatch_checked = true, mrk_watcher_checked = true WHERE position_uid = $1",
                pos["position_uid"]
            )


# 🔸 Основной цикл
async def run_oracle_mw_aggregator():
    await _ensure_group()
    log.info("🚀 MW AGG: слушаем '%s' (group=%s, consumer=%s)", STREAM_NAME, GROUP_NAME, CONSUMER_NAME)

    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={STREAM_NAME: ">"},
                count=XREAD_COUNT,
                block=XREAD_BLOCKMS,
            )
            if not resp:
                continue

            to_ack = []
            for _, records in resp:
                for msg_id, data in records:
                    to_ack.append(msg_id)
                    try:
                        status  = data.get("status")
                        pos_uid = data.get("position_uid")
                        if status != "closed":
                            log.debug("[MW AGG] skip msg_id=%s uid=%s reason=status=%s", msg_id, pos_uid, status)
                            continue

                        pos, strat, verdict = await _load_position_and_strategy(pos_uid)
                        v_code, v_reason = verdict
                        if v_code != "ok":
                            log.debug("[MW AGG] uid=%s skip: %s", pos_uid, v_reason)
                            continue

                        created_at = pos["created_at"]
                        created_at_utc = created_at.astimezone(timezone.utc).replace(tzinfo=None) if created_at.tzinfo is not None else created_at

                        per_tf_found = await _write_pis_mw(
                            pos["position_uid"], pos["strategy_id"], pos["direction"], pos["symbol"], created_at_utc
                        )

                        all_three = await _check_all_three_present(pos["position_uid"], created_at_utc)
                        if all_three:
                            per_tf_codes = {
                                tf: await _load_imw_code(pos["symbol"], tf, _floor_to_bar_open(created_at_utc, tf))
                                for tf in TF_ORDER
                            }
                            await _aggregate_and_mark(pos, per_tf_codes)
                            win_flag = 1 if (pos["pnl"] is not None and pos["pnl"] > 0) else 0
                            log.info(
                                "[MW AGG] uid=%s strat=%s dir=%s PIS=%s AGG tf=3 comp=1 win=%d",
                                pos_uid, pos["strategy_id"], pos["direction"],
                                "/".join(sorted(per_tf_found.keys())) if per_tf_found else "-",
                                win_flag
                            )
                        else:
                            log.debug(
                                "[MW AGG] uid=%s partial PIS: present=%s (agg postponed)",
                                pos_uid, "/".join(sorted(per_tf_found.keys())) if per_tf_found else "-"
                            )

                    except Exception as e:
                        log.exception("❌ MW AGG msg error %s: %s", msg_id, e)

            if to_ack:
                await infra.redis_client.xack(STREAM_NAME, GROUP_NAME, *to_ack)

        except asyncio.CancelledError:
            log.info("⏹️ MW агрегатор остановлен")
            raise
        except Exception as e:
            log.exception("❌ XREADGROUP loop error: %s", e)
            await asyncio.sleep(1)