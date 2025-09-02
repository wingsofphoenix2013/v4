# oracle_bbbins_snapshot_aggregator.py — BB-bins snapshot агрегатор: Этап 4 (апдейт таблиц/Redis и bb_checked)

import os
import asyncio
import logging
from decimal import Decimal, ROUND_HALF_UP

import infra

log = logging.getLogger("ORACLE_BBBINS_SNAP")

# 🔸 Конфиг consumer-группы и чтения
STREAM_NAME   = os.getenv("ORACLE_BBBINS_STREAM",   "signal_log_queue")
GROUP_NAME    = os.getenv("ORACLE_BBBINS_GROUP",    "oracle_bbbins_snap")
CONSUMER_NAME = os.getenv("ORACLE_BBBINS_CONSUMER", "oracle_bbbins_1")
XREAD_COUNT   = int(os.getenv("ORACLE_BBBINS_COUNT",    "50"))
XREAD_BLOCKMS = int(os.getenv("ORACLE_BBBINS_BLOCK_MS", "1000"))

BB_LEN = 20
BB_STD = Decimal("2.0")

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

# 🔸 Загрузка позиции и стратегии под FOR UPDATE + базовые проверки (Этап 2)
async def _load_position_and_strategy(position_uid: str):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        async with conn.transaction():
            pos = await conn.fetchrow("""
                SELECT p.id, p.position_uid, p.symbol, p.direction, p.strategy_id,
                       p.entry_price, p.pnl, p.status,
                       COALESCE(p.bb_checked, false) AS bb_checked
                FROM positions_v4 p
                WHERE p.position_uid = $1
                FOR UPDATE
            """, position_uid)

            if not pos:
                return None, None, ("skip", "position_not_found")
            if pos["status"] != "closed":
                return pos, None, ("skip", "position_not_closed")
            if pos["bb_checked"]:
                return pos, None, ("skip", "already_checked")

            strat = await conn.fetchrow("""
                SELECT id, enabled, COALESCE(market_watcher, false) AS mw
                FROM strategies_v4
                WHERE id = $1
            """, int(pos["strategy_id"]))
            if not strat or not strat["enabled"] or not strat["mw"]:
                return pos, strat, ("skip", "strategy_inactive_or_no_mw")

            return pos, strat, ("ok", "eligible")

# 🔸 Биннинг entry_price по 12 корзинам (0..11 сверху вниз) на основе BB20/2.0
def _bin_entry_price(entry: float, lower: float, upper: float):
    try:
        width = float(upper) - float(lower)
        if width <= 0:
            return None
        bucket = width / 6.0  # одинаковая ширина всех 12 корзин
        # верхние корзины (открытый верх)
        if entry >= upper + 2*bucket:
            return 0
        if entry >= upper + 1*bucket:
            return 1
        if entry >= upper:
            return 2
        # внутри канала [lower .. upper)
        if entry >= lower:
            k = int((entry - lower) // bucket)  # 0..5
            if k < 0: k = 0
            if k > 5: k = 5
            return 8 - k  # 3..8 сверху вниз
        # нижние корзины (открытый низ)
        if entry <= lower - 2*bucket:
            return 11
        if entry <= lower - 1*bucket:
            return 10
        # (entry < lower)
        return 9
    except Exception:
        return None

# 🔸 Чтение BB20/2.0 (upper/lower) из PIS и расчёт корзин для m5/m15/h1 (Этап 3)
async def _load_bb_bins(position_uid: str, entry_price: float):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        rows = await conn.fetch("""
            SELECT timeframe, param_name, value_num
            FROM positions_indicators_stat
            WHERE position_uid = $1
              AND using_current_bar = true
              AND param_name IN ('bb20_2_0_upper','bb20_2_0_lower')
              AND timeframe IN ('m5','m15','h1')
        """, position_uid)

    by_tf = { 'm5': {}, 'm15': {}, 'h1': {} }
    for r in rows:
        tf = r["timeframe"]
        name = r["param_name"]
        by_tf[tf][name] = r["value_num"]

    bins = {}
    for tf, vals in by_tf.items():
        upper = vals.get('bb20_2_0_upper')
        lower = vals.get('bb20_2_0_lower')
        if upper is None or lower is None or entry_price is None:
            continue
        code = _bin_entry_price(float(entry_price), float(lower), float(upper))
        if code is not None:
            bins[tf] = code
    return bins

# 🔸 UPSERT пер-TF и композита + Redis + отметка bb_checked
async def _update_bb_aggregates(pos, bins):
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
                stat = await conn.fetchrow("""
                    SELECT closed_trades, won_trades, pnl_sum
                    FROM positions_bbbins_stat_tf
                    WHERE strategy_id=$1 AND direction=$2 AND timeframe=$3
                          AND bb_len=$4 AND bb_std=$5 AND bin_code=$6
                    FOR UPDATE
                """, strategy_id, direction, tf, BB_LEN, str(BB_STD), bin_code)

                if stat:
                    c = int(stat["closed_trades"]) + 1
                    w = int(stat["won_trades"]) + is_win
                    s = (Decimal(str(stat["pnl_sum"])) + pnl).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                else:
                    c, w, s = 1, is_win, pnl

                wr = (Decimal(w) / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                ap = (s / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

                await conn.execute("""
                    INSERT INTO positions_bbbins_stat_tf
                      (strategy_id, direction, timeframe, bb_len, bb_std, bin_code,
                       closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
                    ON CONFLICT (strategy_id,direction,timeframe,bb_len,bb_std,bin_code)
                    DO UPDATE SET
                      closed_trades=$7, won_trades=$8, pnl_sum=$9, winrate=$10, avg_pnl=$11, updated_at=NOW()
                """, strategy_id, direction, tf, BB_LEN, str(BB_STD), bin_code,
                     c, w, str(s), str(wr), str(ap))

                # Redis per-TF
                try:
                    await redis.set(
                        f"oracle:bb:tf:{strategy_id}:{direction}:{tf}:bb20_2_0:{bin_code}",
                        f'{{"closed_trades": {c}, "winrate": {float(wr):.4f}}}'
                    )
                except Exception:
                    log.debug("Redis SET failed (BB per-TF)")

            # композит: если есть три TF
            if all(tf in bins for tf in ("m5","m15","h1")):
                triplet = f"{bins['m5']}-{bins['m15']}-{bins['h1']}"
                stat = await conn.fetchrow("""
                    SELECT closed_trades, won_trades, pnl_sum
                    FROM positions_bbbins_stat_comp
                    WHERE strategy_id=$1 AND direction=$2 AND bb_len=$3 AND bb_std=$4 AND status_triplet=$5
                    FOR UPDATE
                """, strategy_id, direction, BB_LEN, str(BB_STD), triplet)

                if stat:
                    c = int(stat["closed_trades"]) + 1
                    w = int(stat["won_trades"]) + is_win
                    s = (Decimal(str(stat["pnl_sum"])) + pnl).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                else:
                    c, w, s = 1, is_win, pnl

                wr = (Decimal(w) / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                ap = (s / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

                await conn.execute("""
                    INSERT INTO positions_bbbins_stat_comp
                      (strategy_id, direction, bb_len, bb_std, status_triplet,
                       closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
                    ON CONFLICT (strategy_id,direction,bb_len,bb_std,status_triplet)
                    DO UPDATE SET
                      closed_trades=$6, won_trades=$7, pnl_sum=$8, winrate=$9, avg_pnl=$10, updated_at=NOW()
                """, strategy_id, direction, BB_LEN, str(BB_STD), triplet,
                     c, w, str(s), str(wr), str(ap))

                # Redis comp
                try:
                    await redis.set(
                        f"oracle:bb:comp:{strategy_id}:{direction}:bb20_2_0:{triplet}",
                        f'{{"closed_trades": {c}, "winrate": {float(wr):.4f}}}'
                    )
                except Exception:
                    log.debug("Redis SET failed (BB comp)")

            # отметить позицию
            await conn.execute("UPDATE positions_v4 SET bb_checked=true WHERE position_uid=$1", pos["position_uid"])

# 🔸 Основной цикл: теперь пишем в БД/Redis и отмечаем bb_checked
async def run_oracle_bbbins_snapshot_aggregator():
    await _ensure_group()
    log.info("🚀 BB-BINS SNAP: слушаем '%s' (group=%s, consumer=%s)", STREAM_NAME, GROUP_NAME, CONSUMER_NAME)
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
                        log.debug("[BB-BINS SNAP] closed position received: uid=%s", pos_uid)

                        pos, strat, verdict = await _load_position_and_strategy(pos_uid)
                        v_code, v_reason = verdict
                        if v_code != "ok":
                            log.info("[BB-BINS SNAP] skip uid=%s reason=%s", pos_uid, v_reason)
                            to_ack.append(msg_id); continue

                        entry = pos["entry_price"]
                        bins = await _load_bb_bins(pos_uid, float(entry) if entry is not None else None)
                        if not bins:
                            log.info("[BB-BINS SNAP] skip uid=%s reason=no_bb_bounds_or_entry", pos_uid)
                        else:
                            await _update_bb_aggregates(pos, bins)
                            log.info("[BB-BINS SNAP] updated uid=%s bins=%s", pos_uid, bins)

                        to_ack.append(msg_id)

                    except Exception as e:
                        to_ack.append(msg_id)
                        log.exception("❌ BB-BINS SNAP msg error %s: %s", msg_id, e)

            if to_ack:
                await infra.redis_client.xack(STREAM_NAME, GROUP_NAME, *to_ack)

        except asyncio.CancelledError:
            log.info("⏹️ BB-BINS snapshot агрегатор остановлен")
            raise
        except Exception as e:
            log.exception("❌ XREADGROUP loop error: %s", e)
            await asyncio.sleep(1)