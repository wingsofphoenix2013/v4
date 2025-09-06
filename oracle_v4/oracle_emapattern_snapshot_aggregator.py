# 🔸 oracle_emapattern_snapshot_aggregator.py — EMA-pattern snapshot агрегатор: расчёт в Python, детерминированные апдейты, отметка позиции

import os
import asyncio
import logging
from decimal import Decimal, ROUND_HALF_UP

import infra

log = logging.getLogger("ORACLE_EMAPATTERN_SNAP")

# 🔸 Конфиг consumer-группы и чтения
STREAM_NAME   = os.getenv("ORACLE_EMAPATTERN_STREAM",   "signal_log_queue")
GROUP_NAME    = os.getenv("ORACLE_EMAPATTERN_GROUP",    "oracle_emapattern_snap")
CONSUMER_NAME = os.getenv("ORACLE_EMAPATTERN_CONSUMER", "oracle_emapattern_1")
XREAD_COUNT   = int(os.getenv("ORACLE_EMAPATTERN_COUNT",    "50"))
XREAD_BLOCKMS = int(os.getenv("ORACLE_EMAPATTERN_BLOCK_MS", "1000"))

TF_ORDER = ("m5", "m15", "h1")
PATTERN_INSTANCE_BY_TF = {"m5": 1004, "m15": 1005, "h1": 1006}


# 🔸 Создание consumer-group (идемпотентно)
async def _ensure_group():
    try:
        await infra.redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, id="$", mkstream=True)
        log.debug("✅ Consumer group '%s' создана на '%s'", GROUP_NAME, STREAM_NAME)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug("ℹ️ Consumer group '%s' уже существует", GROUP_NAME)
        else:
            log.exception("❌ Ошибка создания consumer group: %s", e)
            raise


# 🔸 Загрузка позиции и стратегии
async def _load_position_and_strategy(position_uid: str):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        async with conn.transaction():
            pos = await conn.fetchrow(
                """
                SELECT p.id, p.position_uid, p.symbol, p.direction, p.strategy_id,
                       p.pnl, p.status,
                       COALESCE(p.emapattern_checked, false) AS ep_checked
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
            if pos["ep_checked"]:
                return pos, None, ("skip", "already_checked")

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


# 🔸 Загрузка EMA-pattern из PIS (по фиксированным instance_id)
async def _load_pattern_codes(position_uid: str):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT timeframe, instance_id, value_num
            FROM positions_indicators_stat
            WHERE position_uid = $1
              AND using_current_bar = true
              AND param_name = 'emapattern'
              AND timeframe IN ('m5','m15','h1')
              AND instance_id IN (1004,1005,1006)
            """,
            position_uid,
        )
    per_tf = {}
    for r in rows:
        tf = r["timeframe"]
        inst = int(r["instance_id"])
        if PATTERN_INSTANCE_BY_TF.get(tf) != inst:
            continue
        vnum = r["value_num"]
        if vnum is None:
            continue
        # value_num приходит как 1429.0 → приводим к int безопасно
        try:
            pattern_id = int(Decimal(str(vnum)))
        except Exception:
            continue
        per_tf[tf] = pattern_id
    return per_tf  # {'m5': id, 'm15': id, 'h1': id}


# 🔸 Подготовка детерминированного списка ключей агрегатов
def _ordered_keys_from_tf_codes(strategy_id: int, direction: str, per_tf: dict):
    per_tf_keys = []
    comp_keys = []
    for tf in TF_ORDER:
        if tf in per_tf:
            per_tf_keys.append(("tf", (strategy_id, direction, tf, int(per_tf[tf]))))
    if all(tf in per_tf for tf in TF_ORDER):
        triplet = f"{per_tf['m5']}-{per_tf['m15']}-{per_tf['h1']}"
        comp_keys.append(("comp", (strategy_id, direction, triplet)))
    return per_tf_keys, comp_keys


# 🔸 Апдейт агрегатов + Redis + отметка позиции (детерминированно)
async def _update_aggregates_and_mark(pos, per_tf_codes):
    pg = infra.pg_pool
    redis = infra.redis_client

    strategy_id = int(pos["strategy_id"])
    direction   = str(pos["direction"])
    pnl_raw     = pos["pnl"]
    pnl         = Decimal(str(pnl_raw if pnl_raw is not None else "0")).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    is_win      = 1 if pnl > Decimal("0") else 0

    per_tf_keys, comp_keys = _ordered_keys_from_tf_codes(strategy_id, direction, per_tf_codes or {})

    async with pg.acquire() as conn:
        async with conn.transaction():
            # предсоздание строк (DO NOTHING)
            for _, key in per_tf_keys:
                s_id, dir_, tf, pattern_id = key
                await conn.execute(
                    """
                    INSERT INTO positions_emapattern_stat_tf
                      (strategy_id, direction, timeframe, pattern_id,
                       closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
                    VALUES ($1,$2,$3,$4, 0,0,0,0,0,NOW())
                    ON CONFLICT (strategy_id,direction,timeframe,pattern_id) DO NOTHING
                    """,
                    s_id, dir_, tf, pattern_id
                )
            for _, key in comp_keys:
                s_id, dir_, triplet = key
                await conn.execute(
                    """
                    INSERT INTO positions_emapattern_stat_comp
                      (strategy_id, direction, pattern_triplet,
                       closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
                    VALUES ($1,$2,$3, 0,0,0,0,0,NOW())
                    ON CONFLICT (strategy_id,direction,pattern_triplet) DO NOTHING
                    """,
                    s_id, dir_, triplet
                )

            # апдейты в фиксированном порядке
            for _, key in per_tf_keys:
                s_id, dir_, tf, pattern_id = key
                row = await conn.fetchrow(
                    """
                    SELECT closed_trades, won_trades, pnl_sum
                    FROM positions_emapattern_stat_tf
                    WHERE strategy_id=$1 AND direction=$2 AND timeframe=$3 AND pattern_id=$4
                    FOR UPDATE
                    """,
                    s_id, dir_, tf, pattern_id
                )
                c0 = int(row["closed_trades"]); w0 = int(row["won_trades"]); s0 = Decimal(str(row["pnl_sum"]))
                c = c0 + 1
                w = w0 + is_win
                s = (s0 + pnl).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                wr = (Decimal(w) / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                ap = (s / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

                await conn.execute(
                    """
                    UPDATE positions_emapattern_stat_tf
                    SET closed_trades=$5, won_trades=$6, pnl_sum=$7, winrate=$8, avg_pnl=$9, updated_at=NOW()
                    WHERE strategy_id=$1 AND direction=$2 AND timeframe=$3 AND pattern_id=$4
                    """,
                    s_id, dir_, tf, pattern_id,
                    c, w, str(s), str(wr), str(ap)
                )

                try:
                    await redis.set(
                        f"oracle:emapattern:tf:{s_id}:{dir_}:{tf}:{pattern_id}",
                        f'{{"closed_trades": {c}, "winrate": {float(wr):.4f}}}'
                    )
                except Exception:
                    log.debug("Redis SET failed (per-TF)")

            for _, key in comp_keys:
                s_id, dir_, triplet = key
                row = await conn.fetchrow(
                    """
                    SELECT closed_trades, won_trades, pnl_sum
                    FROM positions_emapattern_stat_comp
                    WHERE strategy_id=$1 AND direction=$2 AND pattern_triplet=$3
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
                    UPDATE positions_emapattern_stat_comp
                    SET closed_trades=$4, won_trades=$5, pnl_sum=$6, winrate=$7, avg_pnl=$8, updated_at=NOW()
                    WHERE strategy_id=$1 AND direction=$2 AND pattern_triplet=$3
                    """,
                    s_id, dir_, triplet,
                    c, w, str(s), str(wr), str(ap)
                )

                try:
                    await redis.set(
                        f"oracle:emapattern:comp:{s_id}:{dir_}:{triplet}",
                        f'{{"closed_trades": {c}, "winrate": {float(wr):.4f}}}'
                    )
                except Exception:
                    log.debug("Redis SET failed (comp)")

            # отметка позиции (всегда)
            await conn.execute(
                "UPDATE positions_v4 SET emapattern_checked = true WHERE position_uid = $1",
                pos["position_uid"]
            )


# 🔸 Основной цикл
async def run_oracle_emapattern_snapshot_aggregator():
    await _ensure_group()
    log.debug("🚀 EMAPATTERN SNAP: слушаем '%s' (group=%s, consumer=%s)", STREAM_NAME, GROUP_NAME, CONSUMER_NAME)

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
                            log.debug("[EMAPATTERN SNAP] skip msg_id=%s uid=%s reason=status=%s", msg_id, pos_uid, status)
                            continue

                        log.debug("[EMAPATTERN SNAP] closed position received: uid=%s", pos_uid)

                        pos, strat, verdict = await _load_position_and_strategy(pos_uid)
                        v_code, v_reason = verdict
                        if v_code != "ok":
                            log.debug("[EMAPATTERN SNAP] uid=%s skip: %s", pos_uid, v_reason)
                            continue

                        per_tf_codes = await _load_pattern_codes(pos_uid)

                        updated_tf = 0
                        updated_comp = 0
                        if per_tf_codes:
                            for tf in TF_ORDER:
                                if tf in per_tf_codes:
                                    log.debug(
                                        "[EMAPATTERN SNAP] uid=%s strat=%s dir=%s TF=%s pattern_id=%s",
                                        pos_uid, pos["strategy_id"], pos["direction"], tf, per_tf_codes[tf]
                                    )
                                    updated_tf += 1
                            if all(tf in per_tf_codes for tf in TF_ORDER):
                                triplet = f"{per_tf_codes['m5']}-{per_tf_codes['m15']}-{per_tf_codes['h1']}"
                                log.debug(
                                    "[EMAPATTERN SNAP] uid=%s strat=%s dir=%s triplet=%s",
                                    pos_uid, pos["strategy_id"], pos["direction"], triplet
                                )
                                updated_comp += 1
                        else:
                            log.debug("[EMAPATTERN SNAP] uid=%s no_emapattern_found", pos_uid)

                        await _update_aggregates_and_mark(pos, per_tf_codes)

                        win_flag = 1 if (pos["pnl"] is not None and pos["pnl"] > 0) else 0
                        log.debug(
                            "[EMAPATTERN SNAP] uid=%s strat=%s dir=%s updated_tf=%d updated_comp=%d win=%d",
                            pos_uid, pos["strategy_id"], pos["direction"], updated_tf, updated_comp, win_flag
                        )

                    except Exception as e:
                        log.exception("❌ EMAPATTERN SNAP msg error %s: %s", msg_id, e)

            if to_ack:
                await infra.redis_client.xack(STREAM_NAME, GROUP_NAME, *to_ack)

        except asyncio.CancelledError:
            log.debug("⏹️ EMAPATTERN snapshot агрегатор остановлен")
            raise
        except Exception as e:
            log.exception("❌ XREADGROUP loop error: %s", e)
            await asyncio.sleep(1)