# oracle_adxbins_snapshot_aggregator.py — ADX-bins snapshot агрегатор: Этап 4 (UPSERT таблиц, Redis, adx_checked)

import os
import asyncio
import logging
from decimal import Decimal, ROUND_HALF_UP

import infra

log = logging.getLogger("ORACLE_ADXBINS_SNAP")

# 🔸 Конфиг consumer-группы и чтения
STREAM_NAME   = os.getenv("ORACLE_ADXBINS_STREAM",   "signal_log_queue")
GROUP_NAME    = os.getenv("ORACLE_ADXBINS_GROUP",    "oracle_adxbins_snap")
CONSUMER_NAME = os.getenv("ORACLE_ADXBINS_CONSUMER", "oracle_adxbins_1")
XREAD_COUNT   = int(os.getenv("ORACLE_ADXBINS_COUNT",    "50"))
XREAD_BLOCKMS = int(os.getenv("ORACLE_ADXBINS_BLOCK_MS", "1000"))

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

# 🔸 Этап 2: позиция + стратегия
async def _load_position_and_strategy(position_uid: str):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        async with conn.transaction():
            pos = await conn.fetchrow("""
                SELECT p.id, p.position_uid, p.symbol, p.direction, p.strategy_id,
                       p.pnl, p.status,
                       COALESCE(p.adx_checked, false) AS adx_checked
                FROM positions_v4 p
                WHERE p.position_uid = $1
                FOR UPDATE
            """, position_uid)
            if not pos:
                return None, None, ("skip", "position_not_found")
            if pos["status"] != "closed":
                return pos, None, ("skip", "position_not_closed")
            if pos["adx_checked"]:
                return pos, None, ("skip", "already_checked")

            strat = await conn.fetchrow("""
                SELECT id, enabled, COALESCE(market_watcher, false) AS mw
                FROM strategies_v4
                WHERE id = $1
            """, int(pos["strategy_id"]))
            if not strat or not strat["enabled"] or not strat["mw"]:
                return pos, strat, ("skip", "strategy_inactive_or_no_mw")
            return pos, strat, ("ok", "eligible")

# 🔸 Биннинг ADX
def _bin_adx(value: float) -> int | None:
    try:
        v = max(0.0, min(100.0, float(value)))
        b = int(v // 5) * 5
        if b == 100:
            b = 95
        return b
    except Exception:
        return None

# 🔸 Чтение ADX из PIS и расчёт бинов по TF
async def _load_adx_bins(position_uid: str):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        rows = await conn.fetch("""
            SELECT timeframe, param_name, value_num
            FROM positions_indicators_stat
            WHERE position_uid = $1
              AND using_current_bar = true
              AND param_name IN ('adx_dmi14_adx','adx_dmi28_adx')
              AND timeframe IN ('m5','m15','h1')
        """, position_uid)

    by_tf = { 'm5': None, 'm15': None, 'h1': None }
    for r in rows:
        tf = r["timeframe"]
        name = r["param_name"]
        val = r["value_num"]
        if tf in ("m5","m15") and name == "adx_dmi14_adx":
            by_tf[tf] = val
        elif tf == "h1" and name == "adx_dmi28_adx":
            by_tf[tf] = val

    bins = {}
    for tf, val in by_tf.items():
        if val is None:
            continue
        code = _bin_adx(float(val))
        if code is not None:
            bins[tf] = code
    return bins

# 🔸 UPSERT пер-TF и композита + Redis + отметка adx_checked
async def _update_adx_aggregates(pos, bins):
    pg = infra.pg_pool
    redis = infra.redis_client

    strategy_id = int(pos["strategy_id"])
    direction   = str(pos["direction"])
    pnl         = Decimal(str(pos["pnl"])).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    is_win      = 1 if pnl > Decimal("0") else 0

    async with pg.acquire() as conn:
        async with conn.transaction():
            # per-TF
            for tf, bin_code in bins.items():
                adx_len = 14 if tf in ("m5", "m15") else 28

                stat = await conn.fetchrow("""
                    SELECT closed_trades, won_trades, pnl_sum
                    FROM positions_adxbins_stat_tf
                    WHERE strategy_id=$1 AND direction=$2 AND timeframe=$3
                          AND adx_len=$4 AND bin_code=$5
                    FOR UPDATE
                """, strategy_id, direction, tf, adx_len, bin_code)

                if stat:
                    c = int(stat["closed_trades"]) + 1
                    w = int(stat["won_trades"]) + is_win
                    s = (Decimal(str(stat["pnl_sum"])) + pnl).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                else:
                    c, w, s = 1, is_win, pnl

                wr = (Decimal(w) / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                ap = (s / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

                await conn.execute("""
                    INSERT INTO positions_adxbins_stat_tf
                      (strategy_id, direction, timeframe, adx_len, bin_code,
                       closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
                    ON CONFLICT (strategy_id,direction,timeframe,adx_len,bin_code)
                    DO UPDATE SET
                      closed_trades=$6, won_trades=$7, pnl_sum=$8, winrate=$9, avg_pnl=$10, updated_at=NOW()
                """, strategy_id, direction, tf, adx_len, bin_code,
                     c, w, str(s), str(wr), str(ap))

                # Redis per-TF
                try:
                    await redis.set(
                        f"oracle:adx:tf:{strategy_id}:{direction}:{tf}:adx:{bin_code}",
                        f'{{"closed_trades": {c}, "winrate": {float(wr):.4f}}}'
                    )
                except Exception:
                    log.debug("Redis SET failed (ADX per-TF)")

            # композит: если есть три TF
            if all(tf in bins for tf in ("m5", "m15", "h1")):
                triplet = f"{bins['m5']}-{bins['m15']}-{bins['h1']}"

                stat = await conn.fetchrow("""
                    SELECT closed_trades, won_trades, pnl_sum
                    FROM positions_adxbins_stat_comp
                    WHERE strategy_id=$1 AND direction=$2 AND status_triplet=$3
                    FOR UPDATE
                """, strategy_id, direction, triplet)

                if stat:
                    c = int(stat["closed_trades"]) + 1
                    w = int(stat["won_trades"]) + is_win
                    s = (Decimal(str(stat["pnl_sum"])) + pnl).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                else:
                    c, w, s = 1, is_win, pnl

                wr = (Decimal(w) / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                ap = (s / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

                # ✅ ВАЖНАЯ ПРАВКА: 8 плейсхолдеров + NOW()
                await conn.execute("""
                    INSERT INTO positions_adxbins_stat_comp
                      (strategy_id, direction, status_triplet,
                       closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
                    ON CONFLICT (strategy_id,direction,status_triplet)
                    DO UPDATE SET
                      closed_trades=$4, won_trades=$5, pnl_sum=$6, winrate=$7, avg_pnl=$8, updated_at=NOW()
                """, strategy_id, direction, triplet,
                     c, w, str(s), str(wr), str(ap))

                # Redis comp
                try:
                    await redis.set(
                        f"oracle:adx:comp:{strategy_id}:{direction}:adx:{triplet}",
                        f'{{"closed_trades": {c}, "winrate": {float(wr):.4f}}}'
                    )
                except Exception:
                    log.debug("Redis SET failed (ADX comp)")

            # отметить позицию
            await conn.execute(
                "UPDATE positions_v4 SET adx_checked=true WHERE position_uid=$1",
                pos["position_uid"]
            )
# 🔸 Основной цикл: запись в БД/Redis и отметка adx_checked
async def run_oracle_adxbins_snapshot_aggregator():
    await _ensure_group()
    log.info("🚀 ADX-BINS SNAP: слушаем '%s' (group=%s, consumer=%s)", STREAM_NAME, GROUP_NAME, CONSUMER_NAME)
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
                            to_ack.append(msg_id); continue

                        pos_uid = data.get("position_uid")
                        log.debug("[ADX-BINS SNAP] closed position received: uid=%s", pos_uid)

                        pos, strat, verdict = await _load_position_and_strategy(pos_uid)
                        v_code, v_reason = verdict
                        if v_code != "ok":
                            log.info("[ADX-BINS SNAP] skip uid=%s reason=%s", pos_uid, v_reason)
                            to_ack.append(msg_id); continue

                        bins = await _load_adx_bins(pos_uid)
                        if not bins:
                            log.info("[ADX-BINS SNAP] skip uid=%s reason=no_adx_in_pis", pos_uid)
                        else:
                            await _update_adx_aggregates(pos, bins)
                            log.info("[ADX-BINS SNAP] updated uid=%s bins=%s", pos_uid, bins)

                        to_ack.append(msg_id)

                    except Exception as e:
                        to_ack.append(msg_id)
                        log.exception("❌ ADX-BINS SNAP msg error %s: %s", msg_id, e)

            if to_ack:
                await infra.redis_client.xack(STREAM_NAME, GROUP_NAME, *to_ack)

        except asyncio.CancelledError:
            log.info("⏹️ ADX-BINS snapshot агрегатор остановлен"); raise
        except Exception as e:
            log.exception("❌ XREADGROUP loop error: %s", e)
            await asyncio.sleep(1)