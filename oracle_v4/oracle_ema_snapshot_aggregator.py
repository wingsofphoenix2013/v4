# 🔸 oracle_ema_snapshot_aggregator.py — EMA snapshot агрегатор: считаем на момент входа, пишем сразу в агрегаты

import os
import json
import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

import infra

STREAM_NAME   = os.getenv("ORACLE_EMASNAP_STREAM", "signal_log_queue")
GROUP_NAME    = os.getenv("ORACLE_EMASNAP_GROUP",  "oracle_ema_snap")
CONSUMER_NAME = os.getenv("ORACLE_EMASNAP_CONSUMER", "oracle_ema_snap_1")
XREAD_COUNT   = int(os.getenv("ORACLE_EMASNAP_COUNT", "50"))
XREAD_BLOCKMS = int(os.getenv("ORACLE_EMASNAP_BLOCK_MS", "1000"))

EMA_LENS = [9, 21, 50, 100, 200]
EPS0 = float(os.getenv("EMA_STATUS_EPS0", "0.05"))  # зона equal
EPS1 = float(os.getenv("EMA_STATUS_EPS1", "0.02"))  # значимое изменение ΔD

STATE_LABELS = {
    0: "below_away",
    1: "below_towards",
    2: "equal",
    3: "above_towards",
    4: "above_away",
}

log = logging.getLogger("ORACLE_EMA_SNAP")

# 🔸 Redis TS keys
def k_close(sym: str, tf: str) -> str:
    return f"ts:{sym}:{tf}:c"

def k_ema(sym: str, tf: str, L: int) -> str:
    return f"ts_ind:{sym}:{tf}:ema{L}"

def k_atr(sym: str, tf: str) -> str:
    return f"ts_ind:{sym}:{tf}:atr14"

def k_bb(sym: str, tf: str, part: str) -> str:
    return f"ts_ind:{sym}:{tf}:bb20_2_0_{part}"

async def _ts_get(redis, key: str, ts_ms: int):
    try:
        r = await redis.execute_command("TS.RANGE", key, ts_ms, ts_ms)
        if r and int(r[0][0]) == ts_ms:
            return float(r[0][1])
    except Exception:
        pass
    return None

def _floor_to_step_utc(dt: datetime, minutes: int) -> datetime:
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    base = dt.replace(second=0, microsecond=0)
    return base.replace(minute=(base.minute // minutes) * minutes)

def _tf_step_minutes(tf: str) -> int:
    return 5 if tf == "m5" else (15 if tf == "m15" else 60)

# 🔸 Классификация на один TF×EMA
def _classify(close_t, close_p, ema_t, ema_p, scale_t, scale_p, eps0, eps1):
    if None in (close_t, close_p, ema_t, ema_p, scale_t, scale_p):
        return None
    if scale_t <= 0.0 or scale_p <= 0.0:
        return None

    nd_t = (close_t - ema_t) / scale_t
    nd_p = (close_p - ema_p) / scale_p
    d_t  = abs(nd_t)
    d_p  = abs(nd_p)
    delta_d = d_t - d_p

    if d_t <= eps0:
        return 2, STATE_LABELS[2]

    above = nd_t > 0.0
    if delta_d >= eps1:
        code = 4 if above else 0
    elif delta_d <= -eps1:
        code = 3 if above else 1
    else:
        code = 3 if above else 1
    return code, STATE_LABELS[code]

# 🔸 Redis-публикация сводок
def key_tf(strategy_id: int, direction: str, tf: str, ema_len: int, state_code: int) -> str:
    return f"oracle:emastat:tf:{strategy_id}:{direction}:{tf}:{ema_len}:{state_code}"

def key_comp(strategy_id: int, direction: str, ema_len: int, triplet: str) -> str:
    return f"oracle:emastat:comp:{strategy_id}:{direction}:{ema_len}:{triplet}"

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

# 🔸 Основной обработчик закрытия позиции
async def _process_closed(position_uid: str):
    pg = infra.pg_pool
    redis = infra.redis_client

    async with pg.acquire() as conn:
        async with conn.transaction():
            # позиция FOR UPDATE
            pos = await conn.fetchrow("""
                SELECT p.id, p.strategy_id, p.symbol, p.direction, p.created_at, p.pnl, p.status,
                       COALESCE(p.emastatus_checked, false) AS checked
                FROM positions_v4 p
                WHERE p.position_uid = $1
                FOR UPDATE
            """, position_uid)
            if not pos or pos["status"] != "closed" or pos["checked"]:
                return False, "skip"

            # стратегия активна & market_watcher=true
            strat = await conn.fetchrow("""
                SELECT id, enabled, COALESCE(market_watcher, false) AS mw
                FROM strategies_v4
                WHERE id = $1
            """, int(pos["strategy_id"]))
            if not strat or not strat["enabled"] or not strat["mw"]:
                return False, "strategy_inactive"

            strategy_id = int(pos["strategy_id"])
            direction   = str(pos["direction"])
            symbol      = str(pos["symbol"])
            pnl         = Decimal(str(pos["pnl"])).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
            is_win      = 1 if pnl > Decimal("0") else 0

            # Берём снапшоты on-demand из positions_indicators_stat (текущий бар входа)
            # для TF m5/m15/h1 на любые bar_open_time из PIS (чтобы точно совпасть с моментом снимка)
            pis_rows = await conn.fetch("""
                SELECT timeframe, param_name, value_num, bar_open_time
                FROM positions_indicators_stat
                WHERE position_uid = $1
                  AND timeframe IN ('m5','m15','h1')
                  AND using_current_bar = true
                  AND param_name IN (
                    'ema9','ema21','ema50','ema100','ema200',
                    'atr14',
                    'bb20_2_0_lower','bb20_2_0_center','bb20_2_0_upper'
                  )
            """, position_uid)

            per_tf = {
                'm5':    {'bar_open': None, 'ema': {}, 'atr': None, 'bb_lo': None, 'bb_up': None},
                'm15':   {'bar_open': None, 'ema': {}, 'atr': None, 'bb_lo': None, 'bb_up': None},
                'h1':    {'bar_open': None, 'ema': {}, 'atr': None, 'bb_lo': None, 'bb_up': None},
            }
            for r in pis_rows:
                tf = r["timeframe"]
                per_tf[tf]['bar_open'] = per_tf[tf]['bar_open'] or r["bar_open_time"]
                name = r["param_name"]
                v = r["value_num"]
                if name.startswith("ema"):
                    try:
                        L = int(name.replace("ema",""))
                        if L in EMA_LENS:
                            per_tf[tf]['ema'][L] = v
                    except:
                        pass
                elif name == "atr14":
                    per_tf[tf]['atr'] = v
                elif name == "bb20_2_0_lower":
                    per_tf[tf]['bb_lo'] = v
                elif name == "bb20_2_0_upper":
                    per_tf[tf]['bb_up'] = v

            # Добираем из TS close и prev/scale_prev/ema_prev
            async def get_ts_point(key, ts_ms): return await _ts_get(redis, key, ts_ms)

            per_tf_status = {L: {} for L in EMA_LENS}  # L -> tf -> state_code
            for tf in ('m5','m15','h1'):
                bar_open_dt = per_tf[tf]['bar_open']
                if not bar_open_dt:
                    continue
                step_min = _tf_step_minutes(tf)
                bar_open_ms = int(bar_open_dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
                prev_open_ms= bar_open_ms - step_min * 60_000

                close_t = await get_ts_point(k_close(symbol, tf), bar_open_ms)
                close_p = await get_ts_point(k_close(symbol, tf), prev_open_ms)

                # scale_t (по PIS): m5/m15 → atr14 (если >0) иначе bb_width; h1 → bb_width
                if tf in ('m5','m15'):
                    atr_t = per_tf[tf]['atr']
                    if atr_t is not None and atr_t > 0.0:
                        scale_t = atr_t
                    else:
                        if per_tf[tf]['bb_up'] is not None and per_tf[tf]['bb_lo'] is not None:
                            scale_t = per_tf[tf]['bb_up'] - per_tf[tf]['bb_lo']
                        else:
                            scale_t = None
                else:
                    if per_tf[tf]['bb_up'] is not None and per_tf[tf]['bb_lo'] is not None:
                        scale_t = per_tf[tf]['bb_up'] - per_tf[tf]['bb_lo']
                    else:
                        scale_t = None

                # scale_prev из TS: atr14 (m5/m15) если >0 иначе bb_width_prev; h1 → bb_width_prev
                if tf in ('m5','m15'):
                    atr_p = await get_ts_point(k_atr(symbol, tf), prev_open_ms)
                    if atr_p is not None and atr_p > 0.0:
                        scale_p = atr_p
                    else:
                        bbu_p = await get_ts_point(k_bb(symbol, tf, 'upper'), prev_open_ms)
                        bbl_p = await get_ts_point(k_bb(symbol, tf, 'lower'), prev_open_ms)
                        scale_p = (bbu_p - bbl_p) if (bbu_p is not None and bbl_p is not None and (bbu_p - bbl_p) > 0.0) else None
                else:
                    bbu_p = await get_ts_point(k_bb(symbol, tf, 'upper'), prev_open_ms)
                    bbl_p = await get_ts_point(k_bb(symbol, tf, 'lower'), prev_open_ms)
                    scale_p = (bbu_p - bbl_p) if (bbu_p is not None and bbl_p is not None and (bbu_p - bbl_p) > 0.0) else None

                if None in (close_t, close_p, scale_t, scale_p):
                    continue

                # для каждой EMA длины
                for L in EMA_LENS:
                    ema_t = per_tf[tf]['ema'].get(L)
                    ema_p = await get_ts_point(k_ema(symbol, tf, L), prev_open_ms)
                    if ema_t is None or ema_p is None:
                        continue
                    cls = _classify(close_t, close_p, ema_t, ema_p, scale_t, scale_p, EPS0, EPS1)
                    if cls is None:
                        continue
                    code, label = cls
                    per_tf_status[L][tf] = code

                    # UPSERT per-TF агрегата
                    # читаем существующие счётчики
                    stat = await conn.fetchrow("""
                        SELECT closed_trades, won_trades, pnl_sum
                        FROM positions_emastatus_stat_tf
                        WHERE strategy_id=$1 AND direction=$2 AND timeframe=$3 AND ema_len=$4 AND state_code=$5
                        FOR UPDATE
                    """, strategy_id, direction, tf, L, code)
                    if stat:
                        c = int(stat["closed_trades"]) + 1
                        w = int(stat["won_trades"]) + is_win
                        s = (Decimal(str(stat["pnl_sum"])) + pnl).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                    else:
                        c, w, s = 1, is_win, pnl
                    wr = (Decimal(w) / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                    ap = (s / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

                    await conn.execute("""
                        INSERT INTO positions_emastatus_stat_tf
                          (strategy_id, direction, timeframe, ema_len, state_code,
                           closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
                        ON CONFLICT (strategy_id, direction, timeframe, ema_len, state_code)
                        DO UPDATE SET
                          closed_trades=$6, won_trades=$7, pnl_sum=$8, winrate=$9, avg_pnl=$10, updated_at=NOW()
                    """, strategy_id, direction, tf, L, code, c, w, str(s), str(wr), str(ap))

                    # Redis per-TF
                    try:
                        await infra.redis_client.set(key_tf(strategy_id, direction, tf, L, code),
                                                     json.dumps({"closed_trades": c, "winrate": float(wr)}))
                    except Exception:
                        log.debug("Redis SET failed (per-TF)")

                # конец TF

            # Композит: по каждой EMA длине, если есть три TF кода
            for L in EMA_LENS:
                if all(tf in per_tf_status[L] for tf in ('m5','m15','h1')):
                    triplet = f"{per_tf_status[L]['m5']}-{per_tf_status[L]['m15']}-{per_tf_status[L]['h1']}"
                    stat = await conn.fetchrow("""
                        SELECT closed_trades, won_trades, pnl_sum
                        FROM positions_emastatus_stat_comp
                        WHERE strategy_id=$1 AND direction=$2 AND ema_len=$3 AND status_triplet=$4
                        FOR UPDATE
                    """, strategy_id, direction, L, triplet)
                    if stat:
                        c = int(stat["closed_trades"]) + 1
                        w = int(stat["won_trades"]) + is_win
                        s = (Decimal(str(stat["pnl_sum"])) + pnl).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                    else:
                        c, w, s = 1, is_win, pnl
                    wr = (Decimal(w) / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                    ap = (s / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

                    await conn.execute("""
                        INSERT INTO positions_emastatus_stat_comp
                          (strategy_id, direction, ema_len, status_triplet,
                           closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW())
                        ON CONFLICT (strategy_id, direction, ema_len, status_triplet)
                        DO UPDATE SET
                          closed_trades=$5, won_trades=$6, pnl_sum=$7, winrate=$8, avg_pnl=$9, updated_at=NOW()
                    """, strategy_id, direction, L, triplet, c, w, str(s), str(wr), str(ap))

                    # Redis comp
                    try:
                        await infra.redis_client.set(key_comp(strategy_id, direction, L, triplet),
                                                     json.dumps({"closed_trades": c, "winrate": float(wr)}))
                    except Exception:
                        log.debug("Redis SET failed (comp)")

            # отметим позицию как учтённую
            await conn.execute("""
                UPDATE positions_v4 SET emastatus_checked = true WHERE position_uid = $1
            """, position_uid)

            return True, "ok"

async def run_oracle_ema_snapshot_aggregator():
    await _ensure_group()
    log.debug("🚀 EMA-SNAP: слушаем '%s' (group=%s, consumer=%s)", STREAM_NAME, GROUP_NAME, CONSUMER_NAME)
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
                        ok, reason = await _process_closed(pos_uid)
                        if not ok and reason != "skip":
                            log.debug("[EMA-SNAP DEFER] pos=%s reason=%s", pos_uid, reason)
                        to_ack.append(msg_id)
                    except Exception as e:
                        to_ack.append(msg_id)
                        log.exception("❌ EMA-SNAP msg error %s: %s", msg_id, e)
            if to_ack:
                await infra.redis_client.xack(STREAM_NAME, GROUP_NAME, *to_ack)
        except asyncio.CancelledError:
            log.debug("⏹️ EMA snapshot агрегатор остановлен"); raise
        except Exception as e:
            log.exception("❌ XREADGROUP loop error: %s", e); await asyncio.sleep(1)