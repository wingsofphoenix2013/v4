# 🔸 BB-воркер лаборатории: загрузка агрегатов, биннинг PIS/entry, проверка фильтров, запись результатов

import logging
from typing import Dict, List, Tuple
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime

import laboratory_v4_infra as infra

log = logging.getLogger("LAB_BB")

# 🔸 Константы BB (фиксированные)
BB_LEN = 20
BB_STD = Decimal("2.0")

# внутренние типы
PerTFKey   = Tuple[str, str, int, Decimal, int]  # (direction, timeframe, bb_len, bb_std, bin_code)
PerTFValue = Tuple[int, float, float]            # (closed_trades, winrate, pnl_sum)
CompKey    = Tuple[str, int, Decimal, str]       # (direction, bb_len, bb_std, triplet)
CompValue  = Tuple[int, float, float]            # (closed_trades, winrate, pnl_sum)


# 🔸 Биннинг entry_price по 12 корзинам (0..11 сверху вниз) на основе BB20/2.0
def _bin_entry_price(entry: float, lower: float, upper: float):
    try:
        width = float(upper) - float(lower)
        if width <= 0:
            return None
        bucket = width / 6.0  # одинаковая ширина всех 12 корзин
        if entry >= upper + 2*bucket:
            return 0
        if entry >= upper + 1*bucket:
            return 1
        if entry >= upper:
            return 2
        if entry >= lower:
            k = int((entry - lower) // bucket)  # 0..5
            if k < 0: k = 0
            if k > 5: k = 5
            return 8 - k  # 3..8 сверху вниз
        if entry <= lower - 2*bucket:
            return 11
        if entry <= lower - 1*bucket:
            return 10
        return 9
    except Exception:
        return None


# 🔸 Загрузка агрегатов BB по стратегии (per-TF и comp)
async def load_bb_aggregates_for_strategy(strategy_id: int) -> Tuple[Dict[PerTFKey, PerTFValue], Dict[CompKey, CompValue]]:
    per_tf: Dict[PerTFKey, PerTFValue] = {}
    comp:   Dict[CompKey,   CompValue] = {}

    async with infra.pg_pool.acquire() as conn:
        rows_tf = await conn.fetch(
            """
            SELECT direction, timeframe, bb_len, bb_std, bin_code, closed_trades, winrate, pnl_sum
            FROM positions_bbbins_stat_tf
            WHERE strategy_id=$1
            """,
            strategy_id,
        )
        for r in rows_tf:
            key: PerTFKey = (r["direction"], r["timeframe"], int(r["bb_len"]), Decimal(str(r["bb_std"])), int(r["bin_code"]))
            per_tf[key] = (int(r["closed_trades"]), float(r["winrate"]), float(r["pnl_sum"]))

        rows_c = await conn.fetch(
            """
            SELECT direction, bb_len, bb_std, status_triplet, closed_trades, winrate, pnl_sum
            FROM positions_bbbins_stat_comp
            WHERE strategy_id=$1
            """,
            strategy_id,
        )
        for r in rows_c:
            key: CompKey = (r["direction"], int(r["bb_len"]), Decimal(str(r["bb_std"])), r["status_triplet"])
            comp[key] = (int(r["closed_trades"]), float(r["winrate"]), float(r["pnl_sum"]))

    return per_tf, comp


# 🔸 Счётчик всех закрытых сделок стратегии по направлению на момент cutoff (для percent-порога)
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


# 🔸 Для пачки позиций: direction/entry из positions_v4 + BB upper/lower из PIS → бины по TF
async def load_bb_bins_for_positions(position_uids: List[str]) -> Dict[str, Dict]:
    if not position_uids:
        return {}

    # positions_v4: entry_price, direction
    async with infra.pg_pool.acquire() as conn:
        pos_rows = await conn.fetch(
            """
            SELECT position_uid, direction, entry_price
            FROM positions_v4
            WHERE position_uid = ANY($1::text[])
            """,
            position_uids,
        )

    dir_entry: Dict[str, Tuple[str, float | None]] = {}
    for r in pos_rows:
        d = r["direction"]
        e = float(r["entry_price"]) if r["entry_price"] is not None else None
        dir_entry[r["position_uid"]] = (d, e)

    # PIS: BB границы
    async with infra.pg_pool.acquire() as conn:
        pis_rows = await conn.fetch(
            """
            SELECT position_uid, timeframe, param_name, value_num
            FROM positions_indicators_stat
            WHERE position_uid = ANY($1::text[])
              AND using_current_bar = true
              AND param_name IN ('bb20_2_0_upper','bb20_2_0_lower')
              AND timeframe IN ('m5','m15','h1')
            """,
            position_uids,
        )

    bounds: Dict[str, Dict[str, Dict[str, float]]] = {}
    for r in pis_rows:
        uid = r["position_uid"]; tf = r["timeframe"]; name = r["param_name"]
        rec_tf = bounds.setdefault(uid, {}).setdefault(tf, {})
        rec_tf[name] = float(r["value_num"]) if r["value_num"] is not None else None

    out: Dict[str, Dict] = {}
    for uid in position_uids:
        d_e = dir_entry.get(uid)
        if not d_e:
            continue
        dire, entry = d_e
        rec = {"direction": dire, "m5": None, "m15": None, "h1": None}
        bb = bounds.get(uid, {})
        for tf in ("m5", "m15", "h1"):
            tfb = bb.get(tf, {})
            upper = tfb.get("bb20_2_0_upper")
            lower = tfb.get("bb20_2_0_lower")
            if entry is None or upper is None or lower is None:
                continue
            code = _bin_entry_price(entry, lower, upper)
            if code is not None:
                rec[tf] = code
        out[uid] = rec

    return out


# 🔸 Проверка компоненты per-TF (strict > пороги)
def _check_component_tf(direction: str, tf: str, bin_code: int,
                        per_tf: Dict[PerTFKey, PerTFValue],
                        min_trade_type: str, min_trade_value: Decimal, min_winrate: Decimal,
                        totals_by_dir: Dict[str, int]) -> Tuple[bool, str]:
    key: PerTFKey = (direction, tf, BB_LEN, BB_STD, bin_code)
    stat = per_tf.get(key)
    if not stat:
        return False, f"BB {tf} bin={bin_code}: no_bin_stats → filtered"

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
        return False, f"BB {tf} bin={bin_code}: {trade_note} → filtered"

    if wr_dec > Decimal(str(min_winrate)):
        return True, f"BB {tf} bin={bin_code}: wr={wr_dec} > min_wr={Decimal(str(min_winrate))} → ok"
    else:
        return False, f"BB {tf} bin={bin_code}: wr={wr_dec} >? min_wr={Decimal(str(min_winrate))} → filtered"


# 🔸 Проверка компоненты comp (strict > пороги)
def _check_component_comp(direction: str, triplet: str,
                          comp: Dict[CompKey, CompValue],
                          min_trade_type: str, min_trade_value: Decimal, min_winrate: Decimal,
                          totals_by_dir: Dict[str, int]) -> Tuple[bool, str]:
    key: CompKey = (direction, BB_LEN, BB_STD, triplet)
    stat = comp.get(key)
    if not stat:
        return False, f"BB comp {triplet}: no_triplet_stats → filtered"

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
        return False, f"BB comp {triplet}: {trade_note} → filtered"

    if wr_dec > Decimal(str(min_winrate)):
        return True, f"BB comp {triplet}: wr={wr_dec} > min_wr={Decimal(str(min_winrate))} → ok"
    else:
        return False, f"BB comp {triplet}: wr={wr_dec} >? min_wr={Decimal(str(min_winrate))} → filtered"


# 🔸 Принятие решения по пачке позиций + запись результатов
async def process_bb_batch(
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

    # компоненты для BB: solo m5→m15→h1, затем comp
    bb_solo_tfs = [p for p in lab_params if p["test_name"] == "bb" and p["test_type"] == "solo"]
    bb_solo_tfs = sorted(bb_solo_tfs, key=lambda x: {"m5": 1, "m15": 2, "h1": 3}.get(x.get("test_tf") or "", 99))
    bb_comp     = [p for p in lab_params if p["test_name"] == "bb" and p["test_type"] == "comp"]
    ordered_components = bb_solo_tfs + bb_comp

    # PIS + entry → бины
    pis_bins = await load_bb_bins_for_positions(position_uids)

    rows_to_insert = []
    approved = filtered = skipped = 0

    for uid in position_uids:
        rec = pis_bins.get(uid)
        if not rec or rec.get("direction") not in ("long", "short"):
            rows_to_insert.append((run_id, uid, strategy_id, lab["lab_id"], "skipped_no_data", "BB: no PIS/entry or direction"))
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
                    reason_chain.append(f"BB {tf}: no PIS bin")
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
                    reason_chain.append("BB comp: triplet not available")
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