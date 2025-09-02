# oracle_dmigap_snapshot_aggregator.py — DMI-GAP snapshot агрегатор (gap-бин + тренд по 3 точкам): онлайн-обработка закрытий

import os
import asyncio
import logging
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime

import infra

log = logging.getLogger("ORACLE_DMIGAP_SNAP")

# 🔸 Конфиг
STREAM_NAME   = os.getenv("ORACLE_DMIGAP_STREAM",   "signal_log_queue")
GROUP_NAME    = os.getenv("ORACLE_DMIGAP_GROUP",    "oracle_dmigap_snap")
CONSUMER_NAME = os.getenv("ORACLE_DMIGAP_CONSUMER", "oracle_dmigap_1")
XREAD_COUNT   = int(os.getenv("ORACLE_DMIGAP_COUNT",    "50"))
XREAD_BLOCKMS = int(os.getenv("ORACLE_DMIGAP_BLOCK_MS", "1000"))
RETRY_SEC     = int(os.getenv("ORACLE_DMIGAP_RETRY_SEC","15"))

# Трендовые пороги (по 3-точечному наклону, пунктов gap/бар)
S0 = float(os.getenv("DMI_GAP_S0", "2.0"))     # нейтральная зона
S1 = float(os.getenv("DMI_GAP_S1", "5.0"))     # значимое изменение
JITTER = float(os.getenv("DMI_GAP_JITTER", "10.0"))  # если пила слишком резкая — считаем stable

# 🔸 Шаги по TF
_STEP_MS = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}

# 🔸 Ключи Redis TS для DMI
def _k_plus(tf_len: int, sym: str, tf: str) -> str:
    return f"ts_ind:{sym}:{tf}:adx_dmi{tf_len}_plus_di"
def _k_minus(tf_len: int, sym: str, tf: str) -> str:
    return f"ts_ind:{sym}:{tf}:adx_dmi{tf_len}_minus_di"

# 🔸 Идемпотентная инициализация consumer-group
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

# 🔸 Чтение точки TS ровно на open_time
async def _ts_get_exact(key: str, ts_ms: int):
    try:
        r = await infra.redis_client.execute_command("TS.RANGE", key, ts_ms, ts_ms)
        if r and int(r[0][0]) == ts_ms:
            return float(r[0][1])
    except Exception as e:
        log.debug("[TSERR] key=%s err=%s", key, e)
    return None

# 🔸 Позиция + стратегия под FOR UPDATE + базовые проверки
async def _load_position_and_strategy(position_uid: str):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        async with conn.transaction():
            pos = await conn.fetchrow("""
                SELECT p.id, p.position_uid, p.symbol, p.direction, p.strategy_id,
                       p.created_at, p.status, p.pnl,
                       COALESCE(p.dmi_gap_checked, false) AS dmi_gap_checked
                FROM positions_v4 p
                WHERE p.position_uid = $1
                FOR UPDATE
            """, position_uid)
            if not pos:
                return None, None, ("skip", "position_not_found")
            if pos["status"] != "closed":
                return pos, None, ("skip", "position_not_closed")
            if pos["dmi_gap_checked"]:
                return pos, None, ("skip", "already_checked")

            strat = await conn.fetchrow("""
                SELECT id, enabled, COALESCE(market_watcher, false) AS mw
                FROM strategies_v4
                WHERE id = $1
            """, int(pos["strategy_id"]))
            if not strat or not strat["enabled"] or not strat["mw"]:
                return pos, strat, ("skip", "strategy_inactive_or_no_mw")

            return pos, strat, ("ok", "eligible")

# 🔸 Gap-бин (клип [-100..100], шаг 5; 100 → 95, −100 остаётся −100)
def _gap_bin(v: float) -> int | None:
    try:
        x = max(-100.0, min(100.0, float(v)))
        b = int(x // 5) * 5
        if b == 100:
            b = 95
        return b
    except Exception:
        return None

# 🔸 Тренд по 3 точкам: slope = (gap_t − gap_{t-2})/2; возврат ∈ {-1,0,+1}
def _gap_trend(gm2: float, gm1: float, gt: float) -> int:
    try:
        slope = (float(gt) - float(gm2)) / 2.0
        d1 = float(gt) - float(gm1)
        d2 = float(gm1) - float(gm2)
        if max(abs(d1), abs(d2)) > JITTER:
            return 0
        if slope >= S1:
            return +1
        if slope <= -S1:
            return -1
        if abs(slope) < S0:
            return 0
        return 0
    except Exception:
        return 0

# 🔸 Загрузка PIS (gap_t по t) + TS (gap_{t-1}, gap_{t-2}) и расчёт бин/тренд по каждому TF
async def _load_dmigap_bins_trends(position_uid: str, symbol: str, created_at) -> tuple[dict, dict]:
    # PIS: t
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        rows = await conn.fetch("""
            SELECT timeframe, param_name, value_num, bar_open_time
            FROM positions_indicators_stat
            WHERE position_uid = $1
              AND using_current_bar = true
              AND param_name IN (
                  'adx_dmi14_plus_di','adx_dmi14_minus_di',
                  'adx_dmi28_plus_di','adx_dmi28_minus_di'
              )
              AND timeframe IN ('m5','m15','h1')
        """, position_uid)

    pis = {'m5': {}, 'm15': {}, 'h1': {}}
    bar_open_ms = {}
    for r in rows:
        tf = r["timeframe"]
        pis[tf][r["param_name"]] = r["value_num"]
        if tf not in bar_open_ms and r["bar_open_time"] is not None:
            bar_open_ms[tf] = int(r["bar_open_time"].timestamp() * 1000)

    bins: dict[str,int] = {}
    trends: dict[str,int] = {}

    # TS: t-1, t-2
    for tf in ("m5","m15","h1"):
        tf_len = 14 if tf in ("m5","m15") else 28
        plus_t = pis[tf].get(f"adx_dmi{tf_len}_plus_di")
        minus_t= pis[tf].get(f"adx_dmi{tf_len}_minus_di")
        if plus_t is None or minus_t is None or tf not in bar_open_ms:
            continue

        step = _STEP_MS[tf]
        t_ms   = bar_open_ms[tf]
        t1_ms  = t_ms - step
        t2_ms  = t_ms - 2*step

        # читаем t-1, t-2 из TS
        plus_t1  = await _ts_get_exact(_k_plus(tf_len, symbol, tf), t1_ms)
        minus_t1 = await _ts_get_exact(_k_minus(tf_len, symbol, tf), t1_ms)
        plus_t2  = await _ts_get_exact(_k_plus(tf_len, symbol, tf), t2_ms)
        minus_t2 = await _ts_get_exact(_k_minus(tf_len, symbol, tf), t2_ms)

        # один retry, если на границе ещё нет данных
        if any(x is None for x in (plus_t1, minus_t1, plus_t2, minus_t2)):
            await asyncio.sleep(RETRY_SEC)
            plus_t1  = plus_t1  if plus_t1  is not None else await _ts_get_exact(_k_plus(tf_len, symbol, tf), t1_ms)
            minus_t1 = minus_t1 if minus_t1 is not None else await _ts_get_exact(_k_minus(tf_len, symbol, tf), t1_ms)
            plus_t2  = plus_t2  if plus_t2  is not None else await _ts_get_exact(_k_plus(tf_len, symbol, tf), t2_ms)
            minus_t2 = minus_t2 if minus_t2 is not None else await _ts_get_exact(_k_minus(tf_len, symbol, tf), t2_ms)

        # gap’ы
        gap_t  = float(plus_t)  - float(minus_t)
        if None in (plus_t1, minus_t1, plus_t2, minus_t2):
            # нет истории — запишем только бин по t, тренд пропустим
            b = _gap_bin(gap_t)
            if b is not None:
                bins[tf] = b
            continue

        gap_t1 = float(plus_t1) - float(minus_t1)
        gap_t2 = float(plus_t2) - float(minus_t2)

        b = _gap_bin(gap_t)
        if b is not None:
            bins[tf] = b
            trends[tf] = _gap_trend(gap_t2, gap_t1, gap_t)

    return bins, trends

# 🔸 UPSERT в таблицы (gap и trend), Redis, отметка dmi_gap_checked
async def _update_dmigap_aggregates(pos, bins: dict, trends: dict):
    pg = infra.pg_pool
    redis = infra.redis_client

    strategy_id = int(pos["strategy_id"])
    direction   = str(pos["direction"])
    pnl         = Decimal(str(pos["pnl"])).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    is_win      = 1 if pnl > Decimal("0") else 0

    async with pg.acquire() as conn:
        async with conn.transaction():
            # per-TF GAP (величина)
            for tf, gap_bin in bins.items():
                dmi_len = 14 if tf in ("m5","m15") else 28
                stat = await conn.fetchrow("""
                    SELECT closed_trades, won_trades, pnl_sum
                    FROM positions_dmigap_stat_tf
                    WHERE strategy_id=$1 AND direction=$2 AND timeframe=$3 AND dmi_len=$4 AND gap_bin=$5
                    FOR UPDATE
                """, strategy_id, direction, tf, dmi_len, gap_bin)

                if stat:
                    c = int(stat["closed_trades"]) + 1
                    w = int(stat["won_trades"]) + is_win
                    s = (Decimal(str(stat["pnl_sum"])) + pnl).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                else:
                    c, w, s = 1, is_win, pnl

                wr = (Decimal(w) / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                ap = (s / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

                await conn.execute("""
                    INSERT INTO positions_dmigap_stat_tf
                      (strategy_id, direction, timeframe, dmi_len, gap_bin,
                       closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
                    ON CONFLICT (strategy_id,direction,timeframe,dmi_len,gap_bin)
                    DO UPDATE SET
                      closed_trades=$6, won_trades=$7, pnl_sum=$8, winrate=$9, avg_pnl=$10, updated_at=NOW()
                """, strategy_id, direction, tf, dmi_len, gap_bin,
                     c, w, str(s), str(wr), str(ap))

                try:
                    await redis.set(
                        f"oracle:dmi_gap:tf:{strategy_id}:{direction}:{tf}:gap:{gap_bin}",
                        f'{{"closed_trades": {c}, "winrate": {float(wr):.4f}}}'
                    )
                except Exception:
                    log.debug("Redis SET failed (dmigap per-TF)")

            # per-TF TREND (динамика)
            for tf, trend_code in trends.items():
                dmi_len = 14 if tf in ("m5","m15") else 28
                stat = await conn.fetchrow("""
                    SELECT closed_trades, won_trades, pnl_sum
                    FROM positions_dmigaptrend_stat_tf
                    WHERE strategy_id=$1 AND direction=$2 AND timeframe=$3 AND dmi_len=$4 AND trend_code=$5
                    FOR UPDATE
                """, strategy_id, direction, tf, dmi_len, trend_code)

                if stat:
                    c = int(stat["closed_trades"]) + 1
                    w = int(stat["won_trades"]) + is_win
                    s = (Decimal(str(stat["pnl_sum"])) + pnl).quantize(Decimal("0.0001"), ROUND_HALF_UP)
                else:
                    c, w, s = 1, is_win, pnl

                wr = (Decimal(w) / Decimal(c)).quantize(Decimal("0.0001"), ROUND_HALF_UP)
                ap = (s / Decimal(c)).quantize(Decimal("0.0001"), ROUND_HALF_UP)

                await conn.execute("""
                    INSERT INTO positions_dmigaptrend_stat_tf
                      (strategy_id, direction, timeframe, dmi_len, trend_code,
                       closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
                    ON CONFLICT (strategy_id,direction,timeframe,dmi_len,trend_code)
                    DO UPDATE SET
                      closed_trades=$6, won_trades=$7, pnl_sum=$8, winrate=$9, avg_pnl=$10, updated_at=NOW()
                """, strategy_id, direction, tf, dmi_len, trend_code,
                     c, w, str(s), str(wr), str(ap))

                try:
                    await redis.set(
                        f"oracle:dmi_gap_trend:tf:{strategy_id}:{direction}:{tf}:trend:{trend_code}",
                        f'{{"closed_trades": {c}, "winrate": {float(wr):.4f}}}'
                    )
                except Exception:
                    log.debug("Redis SET failed (dmigap trend per-TF)")

            # Композиты: если есть все TF
            if all(k in bins for k in ("m5","m15","h1")):
                gap_trip = f"{bins['m5']}-{bins['m15']}-{bins['h1']}"
                stat = await conn.fetchrow("""
                    SELECT closed_trades, won_trades, pnl_sum
                    FROM positions_dmigap_stat_comp
                    WHERE strategy_id=$1 AND direction=$2 AND status_triplet=$3
                    FOR UPDATE
                """, strategy_id, direction, gap_trip)
                if stat:
                    c = int(stat["closed_trades"]) + 1
                    w = int(stat["won_trades"]) + is_win
                    s = (Decimal(str(stat["pnl_sum"])) + pnl).quantize(Decimal("0.0001"), ROUND_HALF_UP)
                else:
                    c, w, s = 1, is_win, pnl
                wr = (Decimal(w)/Decimal(c)).quantize(Decimal("0.0001"), ROUND_HALF_UP)
                ap = (s/Decimal(c)).quantize(Decimal("0.0001"), ROUND_HALF_UP)

                await conn.execute("""
                    INSERT INTO positions_dmigap_stat_comp
                      (strategy_id, direction, status_triplet,
                       closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
                    ON CONFLICT (strategy_id,direction,status_triplet)
                    DO UPDATE SET
                      closed_trades=$4, won_trades=$5, pnl_sum=$6, winrate=$7, avg_pnl=$8, updated_at=NOW()
                """, strategy_id, direction, gap_trip,
                     c, w, str(s), str(wr), str(ap))

                try:
                    await redis.set(
                        f"oracle:dmi_gap:comp:{strategy_id}:{direction}:gap:{gap_trip}",
                        f'{{"closed_trades": {c}, "winrate": {float(wr):.4f}}}'
                    )
                except Exception:
                    log.debug("Redis SET failed (dmigap comp)")

            if all(k in trends for k in ("m5","m15","h1")):
                tr_trip = f"{trends['m5']}-{trends['m15']}-{trends['h1']}"
                stat = await conn.fetchrow("""
                    SELECT closed_trades, won_trades, pnl_sum
                    FROM positions_dmigaptrend_stat_comp
                    WHERE strategy_id=$1 AND direction=$2 AND status_triplet=$3
                    FOR UPDATE
                """, strategy_id, direction, tr_trip)
                if stat:
                    c = int(stat["closed_trades"]) + 1
                    w = int(stat["won_trades"]) + is_win
                    s = (Decimal(str(stat["pnl_sum"])) + pnl).quantize(Decimal("0.0001"), ROUND_HALF_UP)
                else:
                    c, w, s = 1, is_win, pnl
                wr = (Decimal(w)/Decimal(c)).quantize(Decimal("0.0001"), ROUND_HALF_UP)
                ap = (s/Decimal(c)).quantize(Decimal("0.0001"), ROUND_HALF_UP)

                await conn.execute("""
                    INSERT INTO positions_dmigaptrend_stat_comp
                      (strategy_id, direction, status_triplet,
                       closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
                    ON CONFLICT (strategy_id,direction,status_triplet)
                    DO UPDATE SET
                      closed_trades=$4, won_trades=$5, pnl_sum=$6, winrate=$7, avg_pnl=$8, updated_at=NOW()
                """, strategy_id, direction, tr_trip,
                     c, w, str(s), str(wr), str(ap))

                try:
                    await redis.set(
                        f"oracle:dmi_gap_trend:comp:{strategy_id}:{direction}:trend:{tr_trip}",
                        f'{{"closed_trades": {c}, "winrate": {float(wr):.4f}}}'
                    )
                except Exception:
                    log.debug("Redis SET failed (dmigap trend comp)")

            # отметить позицию
            await conn.execute("UPDATE positions_v4 SET dmi_gap_checked=true WHERE position_uid=$1", pos["position_uid"])

# 🔸 Основной цикл: приём закрытий, расчёт gap/тренда, апдейты
async def run_oracle_dmigap_snapshot_aggregator():
    await _ensure_group()
    log.info("🚀 DMI-GAP SNAP: слушаем '%s' (group=%s, consumer=%s)", STREAM_NAME, GROUP_NAME, CONSUMER_NAME)
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
                        log.debug("[DMI-GAP SNAP] closed position received: uid=%s", pos_uid)

                        pos, strat, verdict = await _load_position_and_strategy(pos_uid)
                        v_code, v_reason = verdict
                        if v_code != "ok":
                            log.info("[DMI-GAP SNAP] skip uid=%s reason=%s", pos_uid, v_reason)
                            to_ack.append(msg_id); continue

                        symbol = pos["symbol"]
                        created_at = pos["created_at"] or datetime.utcnow()
                        bins, trends = await _load_dmigap_bins_trends(pos_uid, symbol, created_at)

                        if not bins:
                            log.info("[DMI-GAP SNAP] skip uid=%s reason=no_gap_values", pos_uid)
                        else:
                            await _update_dmigap_aggregates(pos, bins, trends)
                            log.info("[DMI-GAP SNAP] updated uid=%s bins=%s trends=%s", pos_uid, bins, trends)

                        to_ack.append(msg_id)

                    except Exception as e:
                        to_ack.append(msg_id)
                        log.exception("❌ DMI-GAP SNAP msg error %s: %s", msg_id, e)

            if to_ack:
                await infra.redis_client.xack(STREAM_NAME, GROUP_NAME, *to_ack)

        except asyncio.CancelledError:
            log.info("⏹️ DMI-GAP snapshot агрегатор остановлен"); raise
        except Exception as e:
            log.exception("❌ XREADGROUP loop error: %s", e)
            await asyncio.sleep(1)