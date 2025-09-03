# 🔸 RSI-воркер лаборатории: загрузка агрегатов, биннинг PIS (rsi14), проверка фильтров, запись результатов

import logging
from typing import Dict, List, Tuple
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime

import laboratory_v4_infra as infra

log = logging.getLogger("LAB_RSI")

# 🔸 Константы RSI (фиксировано)
RSI_LEN = 14

# ключи кэшей
PerTFKey   = Tuple[str, str, int, int]   # (direction, timeframe, rsi_len, rsi_bin)
PerTFValue = Tuple[int, float, float]    # (closed_trades, winrate, pnl_sum)
CompKey    = Tuple[str, int, str]        # (direction, rsi_len, triplet)
CompValue  = Tuple[int, float, float]    # (closed_trades, winrate, pnl_sum)


# 🔸 Биннинг rsi в 0..95 (шаг 5)
def _bin_rsi(val: float) -> int | None:
    try:
        v = max(0.0, min(100.0, float(val)))
        b = int(v // 5) * 5
        if b == 100:
            b = 95
        return b
    except Exception:
        return None


# 🔸 Загрузка агрегатов RSI по стратегии (per-TF и comp)
async def load_rsi_aggregates_for_strategy(strategy_id: int) -> Tuple[Dict[PerTFKey, PerTFValue], Dict[CompKey, CompValue]]:
    per_tf: Dict[PerTFKey, PerTFValue] = {}
    comp:   Dict[CompKey,   CompValue] = {}

    async with infra.pg_pool.acquire() as conn:
        rows_tf = await conn.fetch(
            """
            SELECT direction, timeframe, rsi_len, rsi_bin, closed_trades, winrate, pnl_sum
            FROM positions_rsibins_stat_tf
            WHERE strategy_id=$1
            """,
            strategy_id,
        )
        for r in rows_tf:
            key: PerTFKey = (r["direction"], r["timeframe"], int(r["rsi_len"]), int(r["rsi_bin"]))
            per_tf[key] = (int(r["closed_trades"]), float(r["winrate"]), float(r["pnl_sum"]))

        rows_c = await conn.fetch(
            """
            SELECT direction, rsi_len, status_triplet, closed_trades, winrate, pnl_sum
            FROM positions_rsibins_stat_comp
            WHERE strategy_id=$1
            """,
            strategy_id,
        )
        for r in rows_c:
            key: CompKey = (r["direction"], int(r["rsi_len"]), r["status_triplet"])
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


# 🔸 PIS → бины rsi14 по пачке позиций
async def load_rsi_bins_for_positions(position_uids: List[str]) -> Dict[str, Dict]:
    if not position_uids:
        return {}

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT position_uid, direction, timeframe, value_num
            FROM positions_indicators_stat
            WHERE position_uid = ANY($1::text[])
              AND using_current_bar = true
              AND param_name = 'rsi14'
              AND timeframe IN ('m5','m15','h1')
            """,
            position_uids,
        )

    out: Dict[str, Dict] = {}
    for r in rows:
        uid  = r["position_uid"]
        dire = r["direction"]
        tf   = r["timeframe"]
        val  = r["value_num"]

        rec = out.setdefault(uid, {"direction": dire, "m5": None, "m15": None, "h1": None})
        if val is not None:
            code = _bin_rsi(float(val))
            if code is not None:
                rec[tf] = code

    return out


# 🔸 Проверка компоненты per-TF (strict >)
def _check_component_tf(direction: str, tf: str, rsi_bin: int,
                        per_tf: Dict[PerTFKey, PerTFValue],
                        min_trade_type: str, min_trade_value: Decimal, min_winrate: Decimal,
                        totals_by_dir: Dict[str, int]) -> Tuple[bool, str]:
    key: PerTFKey = (direction, tf, RSI_LEN, rsi_bin)
    stat = per_tf.get(key)
    if not stat:
        return False, f"RSI {tf} bin={rsi_bin}: no_bin_stats → filtered"

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
        return False, f"RSI {tf} bin={rsi_bin}: {trade_note} → filtered"

    if wr_dec > Decimal(str(min_winrate)):
        return True, f"RSI {tf} bin={rsi_bin}: wr={wr_dec} > min_wr={Decimal(str(min_winrate))} → ok"
    else:
        return False, f"RSI {tf} bin={rsi_bin}: wr={wr_dec} >? min_wr={Decimal(str(min_winrate))} → filtered"


# 🔸 Проверка компоненты comp (strict >)
def _check_component_comp(direction: str, triplet: str,
                          comp: Dict[CompKey, CompValue],
                          min_trade_type: str, min_trade_value: Decimal, min_winrate: Decimal,
                          totals_by_dir: Dict[str, int]) -> Tuple[bool, str]:
    key: CompKey = (direction, RSI_LEN, triplet)
    stat = comp.get(key)
    if not stat:
        return False, f"RSI comp {triplet}: no_triplet_stats → filtered"

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
        return False, f"RSI comp {triplet}: {trade_note} → filtered"

    if wr_dec > Decimal(str(min_winrate)):
        return True, f"RSI comp {triplet}: wr={wr_dec} > min_wr={Decimal(str(min_winrate))} → ok"
    else:
        return False, f"RSI comp {triplet}: wr={wr_dec} >? min_wr={Decimal(str(min_winrate))} → filtered"


# 🔸 Принятие решения по пачке позиций + запись результатов
async def process_rsi_batch(
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

    rsi_solo_tfs = [p for p in lab_params if p["test_name"] == "rsi" and p["test_type"] == "solo"]
    rsi_solo_tfs = sorted(rsi_solo_tfs, key=lambda x: {"m5": 1, "m15": 2, "h1": 3}.get(x.get("test_tf") or "", 99))
    rsi_comp     = [p for p in lab_params if p["test_name"] == "rsi" and p["test_type"] == "comp"]
    ordered_components = rsi_solo_tfs + rsi_comp

    pis_bins = await load_rsi_bins_for_positions(position_uids)

    rows_to_insert = []
    approved = filtered = skipped = 0

    for uid in position_uids:
        rec = pis_bins.get(uid)
        if not rec or rec.get("direction") not in ("long", "short"):
            rows_to_insert.append((run_id, uid, strategy_id, lab["lab_id"], "skipped_no_data", "RSI: no PIS rsi14 or direction"))
            skipped += 1
            continue

        direction = rec["direction"]
        m5_bin = rec.get("m5")
        m15_bin = rec.get("m15")
        h1_bin = rec.get("h1")

        triplet = None
        if m5_bin is not None and m15_bin is not None and h1_bin is not None:
            triplet = f"{m5_bin}-{m15_bin}-{h1_bin}"

        decision = "approved"
        reason_chain: List[str] = []

        for comp in ordered_components:
            tt = comp["test_type"]
            if tt == "solo":
                tf = comp.get("test_tf")
                if tf not in ("m5", "m15", "h1"):
                    continue
                current_bin = m5_bin if tf == "m5" else m15_bin if tf == "m15" else h1_bin
                if current_bin is None:
                    decision = "skipped_no_data"
                    reason_chain.append(f"RSI {tf}: no PIS bin")
                    break
                ok, note = _check_component_tf(direction, tf, current_bin, per_tf_cache,
                                               min_trade_type, min_trade_value, min_winrate, totals_by_dir)
                reason_chain.append(note)
                if not ok:
                    decision = "filtered"
                    break
            else:
                if not triplet:
                    decision = "skipped_no_data"
                    reason_chain.append("RSI comp: triplet not available")
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