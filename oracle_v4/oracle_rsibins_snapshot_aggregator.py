# oracle_rsibins_snapshot_aggregator.py — RSI-bins snapshot агрегатор: Этап 4 (апдейт таблиц и Redis, отметка позиции)

import os
import asyncio
import logging
from decimal import Decimal, ROUND_HALF_UP

import infra

log = logging.getLogger("ORACLE_RSIBINS_SNAP")

# 🔸 Конфиг consumer-группы и чтения
STREAM_NAME   = os.getenv("ORACLE_RSIBINS_STREAM",   "signal_log_queue")
GROUP_NAME    = os.getenv("ORACLE_RSIBINS_GROUP",    "oracle_rsibins_snap")
CONSUMER_NAME = os.getenv("ORACLE_RSIBINS_CONSUMER", "oracle_rsibins_1")
XREAD_COUNT   = int(os.getenv("ORACLE_RSIBINS_COUNT",    "50"))
XREAD_BLOCKMS = int(os.getenv("ORACLE_RSIBINS_BLOCK_MS", "1000"))

EMA_RSI_LEN = 14  # фиксируем rsi14

# 🔸 Создание consumer-group (идемпотентно)
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

# 🔸 Загрузка позиции и стратегии
async def _load_position_and_strategy(position_uid: str):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        async with conn.transaction():
            pos = await conn.fetchrow("""
                SELECT p.id, p.position_uid, p.symbol, p.direction, p.strategy_id,
                       p.pnl, p.status,
                       COALESCE(p.rsi_checked, false) AS rsi_checked
                FROM positions_v4 p
                WHERE p.position_uid = $1
                FOR UPDATE
            """, position_uid)

            if not pos:
                return None, None, ("skip", "position_not_found")
            if pos["status"] != "closed":
                return pos, None, ("skip", "position_not_closed")
            if pos["rsi_checked"]:
                return pos, None, ("skip", "already_checked")

            strat = await conn.fetchrow("""
                SELECT id, enabled, COALESCE(market_watcher, false) AS mw
                FROM strategies_v4
                WHERE id = $1
            """, int(pos["strategy_id"]))

            if not strat or not strat["enabled"] or not strat["mw"]:
                return pos, strat, ("skip", "strategy_inactive_or_no_mw")

            return pos, strat, ("ok", "eligible")

# 🔸 Загрузка RSI14 из PIS и биннинг
async def _load_rsi_bins(position_uid: str):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        rows = await conn.fetch("""
            SELECT timeframe, value_num
            FROM positions_indicators_stat
            WHERE position_uid = $1
              AND using_current_bar = true
              AND param_name = 'rsi14'
              AND timeframe IN ('m5','m15','h1')
        """, position_uid)

    per_tf_bins = {}
    for r in rows:
        tf = r["timeframe"]
        val = r["value_num"]
        if val is None:
            continue
        v = max(0.0, min(100.0, float(val)))
        bin_val = int(v // 5) * 5
        if bin_val == 100:
            bin_val = 95
        per_tf_bins[tf] = bin_val

    return per_tf_bins

# 🔸 Апдейт таблиц + Redis
async def _update_aggregates(pos, strat, bins):
    pg = infra.pg_pool
    redis = infra.redis_client

    strategy_id = int(pos["strategy_id"])
    direction   = str(pos["direction"])
    pnl         = Decimal(str(pos["pnl"])).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    is_win      = 1 if pnl > Decimal("0") else 0

    async with pg.acquire() as conn:
        async with conn.transaction():
            # per-TF
            for tf, rsi_bin in bins.items():
                stat = await conn.fetchrow("""
                    SELECT closed_trades, won_trades, pnl_sum
                    FROM positions_rsibins_stat_tf
                    WHERE strategy_id=$1 AND direction=$2 AND timeframe=$3 AND rsi_len=$4 AND rsi_bin=$5
                    FOR UPDATE
                """, strategy_id, direction, tf, EMA_RSI_LEN, rsi_bin)

                if stat:
                    c = int(stat["closed_trades"]) + 1
                    w = int(stat["won_trades"]) + is_win
                    s = (Decimal(str(stat["pnl_sum"])) + pnl).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                else:
                    c, w, s = 1, is_win, pnl

                wr = (Decimal(w) / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                ap = (s / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

                await conn.execute("""
                    INSERT INTO positions_rsibins_stat_tf
                      (strategy_id, direction, timeframe, rsi_len, rsi_bin,
                       closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
                    ON CONFLICT (strategy_id,direction,timeframe,rsi_len,rsi_bin)
                    DO UPDATE SET
                      closed_trades=$6, won_trades=$7, pnl_sum=$8, winrate=$9, avg_pnl=$10, updated_at=NOW()
                """, strategy_id, direction, tf, EMA_RSI_LEN, rsi_bin,
                     c, w, str(s), str(wr), str(ap))

                try:
                    await redis.set(
                        f"oracle:rsi:tf:{strategy_id}:{direction}:{tf}:rsi14:{rsi_bin}",
                        f'{{"closed_trades": {c}, "winrate": {float(wr):.4f}}}'
                    )
                except Exception:
                    log.debug("Redis SET failed (per-TF)")

            # comp-триплет
            if all(tf in bins for tf in ("m5","m15","h1")):
                triplet = f"{bins['m5']}-{bins['m15']}-{bins['h1']}"
                stat = await conn.fetchrow("""
                    SELECT closed_trades, won_trades, pnl_sum
                    FROM positions_rsibins_stat_comp
                    WHERE strategy_id=$1 AND direction=$2 AND rsi_len=$3 AND status_triplet=$4
                    FOR UPDATE
                """, strategy_id, direction, EMA_RSI_LEN, triplet)

                if stat:
                    c = int(stat["closed_trades"]) + 1
                    w = int(stat["won_trades"]) + is_win
                    s = (Decimal(str(stat["pnl_sum"])) + pnl).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                else:
                    c, w, s = 1, is_win, pnl

                wr = (Decimal(w) / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                ap = (s / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

                await conn.execute("""
                    INSERT INTO positions_rsibins_stat_comp
                      (strategy_id, direction, rsi_len, status_triplet,
                       closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW())
                    ON CONFLICT (strategy_id,direction,rsi_len,status_triplet)
                    DO UPDATE SET
                      closed_trades=$5, won_trades=$6, pnl_sum=$7, winrate=$8, avg_pnl=$9, updated_at=NOW()
                """, strategy_id, direction, EMA_RSI_LEN, triplet,
                     c, w, str(s), str(wr), str(ap))

                try:
                    await redis.set(
                        f"oracle:rsi:comp:{strategy_id}:{direction}:rsi14:{triplet}",
                        f'{{"closed_trades": {c}, "winrate": {float(wr):.4f}}}'
                    )
                except Exception:
                    log.debug("Redis SET failed (comp)")

            # отметить позицию
            await conn.execute("""
                UPDATE positions_v4 SET rsi_checked=true WHERE position_uid=$1
            """, pos["position_uid"])

# 🔸 Основной цикл
async def run_oracle_rsibins_snapshot_aggregator():
    await _ensure_group()
    log.info("🚀 RSI-BINS SNAP: слушаем '%s' (group=%s, consumer=%s)", STREAM_NAME, GROUP_NAME, CONSUMER_NAME)
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={STREAM_NAME: ">"},
                count=XREAD_COUNT,
                block=XREAD_BLOCKMS
            )
            if not resp:
                continue

            to_ack = []
            for _, records in resp:
                for msg_id, data in records:
                    try:
                        if data.get("status") != "closed":
                            to_ack.append(msg_id)
                            continue

                        pos_uid = data.get("position_uid")
                        log.debug("[RSI-BINS SNAP] closed position received: uid=%s", pos_uid)

                        pos, strat, verdict = await _load_position_and_strategy(pos_uid)
                        v_code, v_reason = verdict

                        if v_code == "ok":
                            bins = await _load_rsi_bins(pos_uid)
                            if not bins:
                                log.info("[RSI-BINS SNAP] skip uid=%s reason=no_rsi14", pos_uid)
                            else:
                                await _update_aggregates(pos, strat, bins)
                                log.info("[RSI-BINS SNAP] updated uid=%s bins=%s", pos_uid, bins)
                        else:
                            log.info("[RSI-BINS SNAP] skip uid=%s reason=%s", pos_uid, v_reason)

                        to_ack.append(msg_id)

                    except Exception as e:
                        to_ack.append(msg_id)
                        log.exception("❌ RSI-BINS SNAP msg error %s: %s", msg_id, e)

            if to_ack:
                await infra.redis_client.xack(STREAM_NAME, GROUP_NAME, *to_ack)

        except asyncio.CancelledError:
            log.info("⏹️ RSI-BINS snapshot агрегатор остановлен")
            raise
        except Exception as e:
            log.exception("❌ XREADGROUP loop error: %s", e)
            await asyncio.sleep(1)