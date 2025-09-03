# 🔸 DMI-GapTrend воркер лаборатории: тренд по 3 точкам (–1/0/+1), проверка по агрегатам, запись результатов

import os
import asyncio
import logging
from typing import Dict, List, Tuple
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime

import laboratory_v4_infra as infra

log = logging.getLogger("LAB_DMIGAPTREND")

# 🔸 Пороговые параметры тренда (совпадают с oracle_dmigap_snapshot_aggregator)
S0     = float(os.getenv("DMI_GAP_S0", "2.0"))      # нейтральная зона
S1     = float(os.getenv("DMI_GAP_S1", "5.0"))      # значимое изменение
JITTER = float(os.getenv("DMI_GAP_JITTER", "10.0")) # шум/пила → считаем 0

# 🔸 Шаги по TF (мс)
_STEP_MS = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}

# 🔸 Ключи Redis TS (как в oracle)
def _k_plus(tf_len: int, sym: str, tf: str) -> str:
    return f"ts_ind:{sym}:{tf}:adx_dmi{tf_len}_plus_di"
def _k_minus(tf_len: int, sym: str, tf: str) -> str:
    return f"ts_ind:{sym}:{tf}:adx_dmi{tf_len}_minus_di"

# внутренние типы кэшей
PerTFKey   = Tuple[str, str, int, int]  # (direction, timeframe, dmi_len, trend_code)
PerTFValue = Tuple[int, float, float]   # (closed_trades, winrate, pnl_sum)
CompKey    = Tuple[str, str]            # (direction, triplet)
CompValue  = Tuple[int, float, float]   # (closed_trades, winrate, pnl_sum)


# 🔸 Чтение точки TS ровно на open_time
async def _ts_get_exact(key: str, ts_ms: int):
    try:
        r = await infra.redis_client.execute_command("TS.RANGE", key, ts_ms, ts_ms)
        if r and int(r[0][0]) == ts_ms:
            return float(r[0][1])
    except Exception as e:
        log.info("[TSERR] key=%s err=%s", key, e)
    return None


# 🔸 Код тренда по 3 точкам: {-1,0,+1}
def _trend_code(gm2: float, gm1: float, gt: float) -> int:
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


# 🔸 Загрузка агрегатов (per-TF и композит) для dmigaptrend
async def load_dmigaptrend_aggregates_for_strategy(strategy_id: int) -> Tuple[Dict[PerTFKey, PerTFValue], Dict[CompKey, CompValue]]:
    per_tf: Dict[PerTFKey, PerTFValue] = {}
    comp  : Dict[CompKey,   CompValue] = {}

    async with infra.pg_pool.acquire() as conn:
        rows_tf = await conn.fetch(
            """
            SELECT direction, timeframe, dmi_len, trend_code, closed_trades, winrate, pnl_sum
            FROM positions_dmigaptrend_stat_tf
            WHERE strategy_id=$1
            """,
            strategy_id,
        )
        for r in rows_tf:
            key: PerTFKey = (r["direction"], r["timeframe"], int(r["dmi_len"]), int(r["trend_code"]))
            per_tf[key] = (int(r["closed_trades"]), float(r["winrate"]), float(r["pnl_sum"]))

        rows_c = await conn.fetch(
            """
            SELECT direction, status_triplet, closed_trades, winrate, pnl_sum
            FROM positions_dmigaptrend_stat_comp
            WHERE strategy_id=$1
            """,
            strategy_id,
        )
        for r in rows_c:
            key: CompKey = (r["direction"], r["status_triplet"])
            comp[key] = (int(r["closed_trades"]), float(r["winrate"]), float(r["pnl_sum"]))

    return per_tf, comp


# 🔸 Счётчик всех закрытых сделок по направлению (для percent)
async def load_total_closed_by_direction(strategy_id: int, cutoff: datetime) -> Dict[str, int]:
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT direction, COUNT(*) AS cnt
            FROM positions_v4
            WHERE strategy_id=$1 AND status='closed' AND closed_at <= $2
            GROUP BY direction
            """,
            strategy_id, cutoff,
        )
    totals = {"long": 0, "short": 0}
    for r in rows:
        if r["direction"] in totals:
            totals[r["direction"]] = int(r["cnt"])
    return totals


# 🔸 Для пачки позиций: собираем direction+symbol из positions_v4, PIS (текущие плюс/минус), TS t-1/t-2 → коды тренда по TF
async def load_dmigaptrend_codes_for_positions(position_uids: List[str]) -> Dict[str, Dict]:
    if not position_uids:
        return {}

    # positions_v4 — direction, symbol
    async with infra.pg_pool.acquire() as conn:
        pos_rows = await conn.fetch(
            """
            SELECT position_uid, direction, symbol
            FROM positions_v4
            WHERE position_uid = ANY($1::text[])
            """,
            position_uids,
        )
    dir_sym: Dict[str, Tuple[str, str]] = {}
    for r in pos_rows:
        dir_sym[r["position_uid"]] = (r["direction"], r["symbol"])

    # PIS — текущие плюс/минус и bar_open_time по TF
    async with infra.pg_pool.acquire() as conn:
        pis_rows = await conn.fetch(
            """
            SELECT position_uid, timeframe, param_name, value_num, bar_open_time
            FROM positions_indicators_stat
            WHERE position_uid = ANY($1::text[])
              AND using_current_bar = true
              AND param_name IN (
                  'adx_dmi14_plus_di','adx_dmi14_minus_di',
                  'adx_dmi28_plus_di','adx_dmi28_minus_di'
              )
              AND timeframe IN ('m5','m15','h1')
            """,
            position_uids,
        )

    # группируем
    cur: Dict[str, Dict[str, Dict[str, float]]] = {}
    bar_ms: Dict[str, Dict[str, int]] = {}
    for r in pis_rows:
        uid, tf, name = r["position_uid"], r["timeframe"], r["param_name"]
        cur.setdefault(uid, {}).setdefault(tf, {})[name] = float(r["value_num"]) if r["value_num"] is not None else None
        if r["bar_open_time"] is not None:
            bar_ms.setdefault(uid, {})[tf] = int(r["bar_open_time"].timestamp() * 1000)

    out: Dict[str, Dict] = {}
    for uid in position_uids:
        if uid not in dir_sym:
            continue
        dire, sym = dir_sym[uid]
        rec = {"direction": dire, "m5": None, "m15": None, "h1": None}

        for tf in ("m5", "m15", "h1"):
            tf_len = 14 if tf in ("m5", "m15") else 28
            tf_cur = cur.get(uid, {}).get(tf, {})
            plus_t = tf_cur.get(f"adx_dmi{tf_len}_plus_di")
            minus_t= tf_cur.get(f"adx_dmi{tf_len}_minus_di")
            t_ms   = bar_ms.get(uid, {}).get(tf)

            if plus_t is None or minus_t is None or t_ms is None:
                continue

            step  = _STEP_MS[tf]
            t1_ms = t_ms - step
            t2_ms = t_ms - 2*step

            plus_t1  = await _ts_get_exact(_k_plus(tf_len, sym, tf), t1_ms)
            minus_t1 = await _ts_get_exact(_k_minus(tf_len, sym, tf), t1_ms)
            plus_t2  = await _ts_get_exact(_k_plus(tf_len, sym, tf), t2_ms)
            minus_t2 = await _ts_get_exact(_k_minus(tf_len, sym, tf), t2_ms)

            # небольшой retry на границе
            if any(x is None for x in (plus_t1, minus_t1, plus_t2, minus_t2)):
                await asyncio.sleep(0)  # yield
                plus_t1  = plus_t1  if plus_t1  is not None else await _ts_get_exact(_k_plus(tf_len, sym, tf), t1_ms)
                minus_t1 = minus_t1 if minus_t1 is not None else await _ts_get_exact(_k_minus(tf_len, sym, tf), t1_ms)
                plus_t2  = plus_t2  if plus_t2  is not None else await _ts_get_exact(_k_plus(tf_len, sym, tf), t2_ms)
                minus_t2 = minus_t2 if minus_t2 is not None else await _ts_get_exact(_k_minus(tf_len, sym, tf), t2_ms)

            if None in (plus_t1, minus_t1, plus_t2, minus_t2):
                continue

            gap_t2 = float(plus_t2) - float(minus_t2)
            gap_t1 = float(plus_t1) - float(minus_t1)
            gap_t  = float(plus_t)  - float(minus_t)

            trend = _trend_code(gap_t2, gap_t1, gap_t)
            rec[tf] = trend

        out[uid] = rec

    return out


# 🔸 Проверка компоненты per-TF (strict >)
def _check_component_tf(direction: str, tf: str, trend_code: int,
                        per_tf: Dict[PerTFKey, PerTFValue],
                        min_trade_type: str, min_trade_value: Decimal, min_winrate: Decimal,
                        totals_by_dir: Dict[str, int]) -> Tuple[bool, str]:
    dmi_len = 14 if tf in ("m5","m15") else 28
    key: PerTFKey = (direction, tf, dmi_len, trend_code)
    stat = per_tf.get(key)
    if not stat:
        return False, f"DMItrend {tf} code={trend_code}: no_bin_stats → filtered"

    closed, wr, _ = stat
    wr_dec = Decimal(str(wr)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    if min_trade_type == "absolute":
        passed_trades = closed > int(min_trade_value)
        trade_note = f"closed={closed} >? min_closed={int(min_trade_value)}"
    else:
        base = totals_by_dir.get(direction, 0)
        need = (Decimal(str(min_trade_value)) * Decimal(base)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        passed_trades = Decimal(closed) > need
        trade_note = f"closed={closed} >? min_closed={str(need)} (dir_total={base})"

    if not passed_trades:
        return False, f"DMItrend {tf} code={trend_code}: {trade_note} → filtered"

    if wr_dec > Decimal(str(min_winrate)):
        return True, f"DMItrend {tf} code={trend_code}: wr={wr_dec} > min_wr={Decimal(str(min_winrate))} → ok"
    else:
        return False, f"DMItrend {tf} code={trend_code}: wr={wr_dec} >? min_wr={Decimal(str(min_winrate))} → filtered"


# 🔸 Проверка композита (strict >)
def _check_component_comp(direction: str, triplet: str,
                          comp: Dict[CompKey, CompValue],
                          min_trade_type: str, min_trade_value: Decimal, min_winrate: Decimal,
                          totals_by_dir: Dict[str, int]) -> Tuple[bool, str]:
    key: CompKey = (direction, triplet)
    stat = comp.get(key)
    if not stat:
        return False, f"DMItrend comp {triplet}: no_triplet_stats → filtered"

    closed, wr, _ = stat
    wr_dec = Decimal(str(wr)).quantize(Decimal("0.0001"), ROUND_HALF_UP)

    if min_trade_type == "absolute":
        passed_trades = closed > int(min_trade_value)
        trade_note = f"closed={closed} >? min_closed={int(min_trade_value)}"
    else:
        base = totals_by_dir.get(direction, 0)
        need = (Decimal(str(min_trade_value)) * Decimal(base)).quantize(Decimal("0.0001"), ROUND_HALF_UP)
        passed_trades = Decimal(closed) > need
        trade_note = f"closed={closed} >? min_closed={str(need)} (dir_total={base})"

    if not passed_trades:
        return False, f"DMItrend comp {triplet}: {trade_note} → filtered"

    if wr_dec > Decimal(str(min_winrate)):
        return True, f"DMItrend comp {triplet}: wr={wr_dec} > min_wr={Decimal(str(min_winrate))} → ok"
    else:
        return False, f"DMItrend comp {triplet}: wr={wr_dec} >? min_wr={Decimal(str(min_winrate))} → filtered"


# 🔸 Принятие решения по пачке позиций + запись результатов
async def process_dmigaptrend_batch(
    lab: dict,
    strategy_id: int,
    run_id: int,
    cutoff: datetime,
    lab_params: List[dict],
    position_uids: List[str],
    per_tf_cache: Dict[PerTFKey, PerTFValue],
    comp_cache: Dict[CompKey, CompValue],
    totals_by_dir: Dict[str, int],
):
    if not position_uids:
        return 0, 0, 0

    min_trade_type  = str(lab["min_trade_type"])
    min_trade_value = Decimal(str(lab["min_trade_value"]))
    min_winrate     = Decimal(str(lab["min_winrate"]))

    # компоненты теста: solo m5→m15→h1, затем comp
    solo = [p for p in lab_params if p["test_name"] == "dmigap_trend" and p["test_type"] == "solo"]
    solo = sorted(solo, key=lambda x: {"m5": 1, "m15": 2, "h1": 3}.get(x.get("test_tf") or "", 99))
    comps = [p for p in lab_params if p["test_name"] == "dmigap_trend" and p["test_type"] == "comp"]
    ordered_components = solo + comps

    # PIS+TS → тренд-коды по TF
    trend_codes = await load_dmigaptrend_codes_for_positions(position_uids)

    rows_to_insert = []
    approved = filtered = skipped = 0

    for uid in position_uids:
        rec = trend_codes.get(uid)
        if not rec or rec.get("direction") not in ("long", "short"):
            rows_to_insert.append((run_id, uid, strategy_id, lab["lab_id"], "skipped_no_data", "DMItrend: no trend codes or direction"))
            skipped += 1
            continue

        direction = rec["direction"]
        m5_code  = rec.get("m5")
        m15_code = rec.get("m15")
        h1_code  = rec.get("h1")

        triplet = None
        if m5_code is not None and m15_code is not None and h1_code is not None:
            triplet = f"{m5_code}-{m15_code}-{h1_code}"

        decision = "approved"
        reason_chain: List[str] = []

        for comp in ordered_components:
            tt = comp["test_type"]
            if tt == "solo":
                tf = comp.get("test_tf")
                if tf not in ("m5", "m15", "h1"):
                    continue
                current = m5_code if tf == "m5" else m15_code if tf == "m15" else h1_code
                if current is None:
                    decision = "skipped_no_data"
                    reason_chain.append(f"DMItrend {tf}: no trend code")
                    break
                ok, note = _check_component_tf(direction, tf, current, per_tf_cache,
                                               min_trade_type, min_trade_value, min_winrate, totals_by_dir)
                reason_chain.append(note)
                if not ok:
                    decision = "filtered"
                    break
            else:
                if not triplet:
                    decision = "skipped_no_data"
                    reason_chain.append("DMItrend comp: triplet not available")
                    break
                ok, note = _check_component_comp(direction, triplet, comp_cache,
                                                 min_trade_type, min_trade_value, min_winrate, totals_by_dir)
                reason_chain.append(note)
                if not ok:
                    decision = "filtered"
                    break

        reason_text = " | ".join(reason_chain) if reason_chain else "ok"

        if decision == "approved":
            approved += 1
        elif decision == "filtered":
            filtered += 1
        else:
            skipped += 1

        rows_to_insert.append((run_id, uid, strategy_id, lab["lab_id"], decision, reason_text))

    # вставка результатов пачкой
    async with infra.pg_pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(
                """
                INSERT INTO laboratory_results_v4
                  (run_id, position_uid, strategy_id, lab_id, test_result, reason, created_at)
                VALUES ($1,$2,$3,$4,$5,$6, NOW())
                ON CONFLICT (run_id, position_uid, lab_id)
                DO UPDATE SET test_result=EXCLUDED.test_result, reason=EXCLUDED.reason, created_at=NOW()
                """,
                rows_to_insert,
            )

    return approved, filtered, skipped