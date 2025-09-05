# 🔸 EMAstatus-воркер лаборатории: загрузка агрегатов, чтение PIS (ema{L}_status), проверка порогов, запись результатов

import logging
from typing import Dict, List, Tuple, Optional
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime

import laboratory_v4_infra as infra

log = logging.getLogger("LAB_EMASTATUS")

# ключи кэшей
PerTFKey   = Tuple[str, str, int, int]   # (direction, timeframe, ema_len, status_code)
PerTFValue = Tuple[int, float, float]    # (closed_trades, winrate, pnl_sum)
CompKey    = Tuple[str, int, str]        # (direction, ema_len, triplet)
CompValue  = Tuple[int, float, float]    # (closed_trades, winrate, pnl_sum)


# 🔸 Вытащить ema_len из параметров теста (ожидается одна длина на тест)
def _extract_ema_len(lab_params: List[dict]) -> Optional[int]:
    for p in lab_params:
        if p.get("test_name") == "emastatus":
            spec = p.get("param_spec") or {}
            val = spec.get("ema_len")
            if val is not None:
                try:
                    return int(val)
                except Exception:
                    pass
    return None


# 🔸 Загрузка агрегатов EMAstatus по стратегии (per-TF и comp)
async def load_emastatus_aggregates_for_strategy(strategy_id: int) -> Tuple[Dict[PerTFKey, PerTFValue], Dict[CompKey, CompValue]]:
    per_tf: Dict[PerTFKey, PerTFValue] = {}
    comp:   Dict[CompKey,   CompValue] = {}

    async with infra.pg_pool.acquire() as conn:
        rows_tf = await conn.fetch(
            """
            SELECT direction, timeframe, ema_len, status_code, closed_trades, winrate, pnl_sum
            FROM positions_emastatus_stat_tf
            WHERE strategy_id=$1
            """,
            strategy_id,
        )
        for r in rows_tf:
            key: PerTFKey = (r["direction"], r["timeframe"], int(r["ema_len"]), int(r["status_code"]))
            per_tf[key] = (int(r["closed_trades"]), float(r["winrate"]), float(r["pnl_sum"]))

        rows_c = await conn.fetch(
            """
            SELECT direction, ema_len, status_triplet, closed_trades, winrate, pnl_sum
            FROM positions_emastatus_stat_comp
            WHERE strategy_id=$1
            """,
            strategy_id,
        )
        for r in rows_c:
            key: CompKey = (r["direction"], int(r["ema_len"]), r["status_triplet"])
            comp[key] = (int(r["closed_trades"]), float(r["winrate"]), float(r["pnl_sum"]))

    return per_tf, comp


# 🔸 Счётчик всех закрытых сделок стратегии по направлению (для percent-порога)
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


# 🔸 PIS → коды ema{L}_status по пачке позиций (m5/m15/h1)
async def load_emastatus_codes_for_positions(position_uids: List[str], ema_len: int) -> Dict[str, Dict]:
    if not position_uids:
        return {}

    param_name = f"ema{ema_len}_status"

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT position_uid, direction, timeframe, value_num
            FROM positions_indicators_stat
            WHERE position_uid = ANY($1::text[])
              AND using_current_bar = true
              AND param_name = $2
              AND timeframe IN ('m5','m15','h1')
            """,
            position_uids, param_name,
        )

    out: Dict[str, Dict] = {}
    for r in rows:
        uid  = r["position_uid"]
        dire = r["direction"]
        tf   = r["timeframe"]
        val  = r["value_num"]

        rec = out.setdefault(uid, {"direction": dire, "m5": None, "m15": None, "h1": None})
        if val is not None:
            try:
                rec[tf] = int(val)
            except Exception:
                rec[tf] = None

    return out


# 🔸 Проверка компоненты per-TF (strict >)
def _check_component_tf(direction: str, tf: str, ema_len: int, status_code: int,
                        per_tf: Dict[PerTFKey, PerTFValue],
                        min_trade_type: str, min_trade_value: Decimal, min_winrate: Decimal,
                        totals_by_dir: Dict[str, int]) -> Tuple[bool, str]:
    key: PerTFKey = (direction, tf, ema_len, status_code)
    stat = per_tf.get(key)
    if not stat:
        return False, f"EMAstatus(L={ema_len}) {tf} code={status_code}: no_bin_stats → filtered"

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
        return False, f"EMAstatus(L={ema_len}) {tf} code={status_code}: {trade_note} → filtered"

    if wr_dec > Decimal(str(min_winrate)):
        return True, f"EMAstatus(L={ema_len}) {tf} code={status_code}: wr={wr_dec} > min_wr={Decimal(str(min_winrate))} → ok"
    else:
        return False, f"EMAstatus(L={ema_len}) {tf} code={status_code}: wr={wr_dec} >? min_wr={Decimal(str(min_winrate))} → filtered"


# 🔸 Проверка компоненты comp (strict >)
def _check_component_comp(direction: str, ema_len: int, triplet: str,
                          comp: Dict[CompKey, CompValue],
                          min_trade_type: str, min_trade_value: Decimal, min_winrate: Decimal,
                          totals_by_dir: Dict[str, int]) -> Tuple[bool, str]:
    key: CompKey = (direction, ema_len, triplet)
    stat = comp.get(key)
    if not stat:
        return False, f"EMAstatus(L={ema_len}) comp {triplet}: no_triplet_stats → filtered"

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
        return False, f"EMAstatus(L={ema_len}) comp {triplet}: {trade_note} → filtered"

    if wr_dec > Decimal(str(min_winrate)):
        return True, f"EMAstatus(L={ema_len}) comp {triplet}: wr={wr_dec} > min_wr={Decimal(str(min_winrate))} → ok"
    else:
        return False, f"EMAstatus(L={ema_len}) comp {triplet}: wr={wr_dec} >? min_wr={Decimal(str(min_winrate))} → filtered"


# 🔸 Принятие решения по пачке позиций + запись результатов
async def process_emastatus_batch(
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

    ema_len = _extract_ema_len(lab_params)
    if ema_len is None:
        log.error("EMAstatus: не найден ema_len в param_spec теста lab_id=%s", lab.get("lab_id"))
        return 0, 0, 0

    min_trade_type  = str(lab["min_trade_type"])
    min_trade_value = Decimal(str(lab["min_trade_value"]))
    min_winrate     = Decimal(str(lab["min_winrate"]))

    ema_solo_tfs = [p for p in lab_params if p["test_name"] == "emastatus" and p["test_type"] == "solo"]
    ema_solo_tfs = sorted(ema_solo_tfs, key=lambda x: {"m5": 1, "m15": 2, "h1": 3}.get(x.get("test_tf") or "", 99))
    ema_comp     = [p for p in lab_params if p["test_name"] == "emastatus" and p["test_type"] == "comp"]
    ordered_components = ema_solo_tfs + ema_comp

    pis_codes = await load_emastatus_codes_for_positions(position_uids, ema_len)

    rows_to_insert = []
    approved = filtered = skipped = 0

    for uid in position_uids:
        rec = pis_codes.get(uid)
        if not rec or rec.get("direction") not in ("long", "short"):
            rows_to_insert.append((run_id, uid, strategy_id, lab["lab_id"], "skipped_no_data",
                                   f"EMAstatus(L={ema_len}): no PIS status or direction"))
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
                    reason_chain.append(f"EMAstatus(L={ema_len}) {tf}: no PIS status")
                    break
                ok, note = _check_component_tf(direction, tf, ema_len, current, per_tf_cache,
                                               min_trade_type, min_trade_value, min_winrate, totals_by_dir)
                reason_chain.append(note)
                if not ok:
                    decision = "filtered"
                    break
            else:
                if not triplet:
                    decision = "skipped_no_data"
                    reason_chain.append(f"EMAstatus(L={ema_len}) comp: triplet not available")
                    break
                ok, note = _check_component_comp(direction, ema_len, triplet, comp_cache,
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