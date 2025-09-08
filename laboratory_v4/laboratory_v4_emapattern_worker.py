# 🔸 EMA Pattern-воркер лаборатории: загрузка агрегатов, PIS, проверки solo/comp, запись результатов

import logging
from typing import Dict, List, Tuple
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime

import laboratory_v4_infra as infra

log = logging.getLogger("LAB_EMAPATTERN")

# 🔸 Внутренние типы кэшей
PerTFKey   = Tuple[str, str, int]      # (direction, timeframe, pattern_id)
PerTFValue = Tuple[int, float, float]  # (closed_trades, winrate, pnl_sum)
CompKey    = Tuple[str, str]           # (direction, pattern_triplet)
CompValue  = Tuple[int, float, float]  # (closed_trades, winrate, pnl_sum)

# 🔸 Константы PIS по инстансам (зафиксированы)
_EMA_PATTERN_INSTANCE = {"m5": 1004, "m15": 1005, "h1": 1006}
_PARAM_NAME = "emapattern"


# 🔸 Загрузка агрегатов EMA Pattern по стратегии (per-TF и comp)
async def load_emapattern_aggregates_for_strategy(strategy_id: int) -> Tuple[Dict[PerTFKey, PerTFValue], Dict[CompKey, CompValue]]:
    per_tf: Dict[PerTFKey, PerTFValue] = {}
    comp:   Dict[CompKey,   CompValue] = {}

    async with infra.pg_pool.acquire() as conn:
        rows_tf = await conn.fetch(
            """
            SELECT direction, timeframe, pattern_id, closed_trades, winrate, pnl_sum
            FROM positions_emapattern_stat_tf
            WHERE strategy_id = $1
            """,
            strategy_id,
        )
        for r in rows_tf:
            key: PerTFKey = (r["direction"], r["timeframe"], int(r["pattern_id"]))
            per_tf[key] = (int(r["closed_trades"]), float(r["winrate"]), float(r["pnl_sum"]))

        rows_c = await conn.fetch(
            """
            SELECT direction, pattern_triplet, closed_trades, winrate, pnl_sum
            FROM positions_emapattern_stat_comp
            WHERE strategy_id = $1
            """,
            strategy_id,
        )
        for r in rows_c:
            key: CompKey = (r["direction"], r["pattern_triplet"])
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


# 🔸 Загрузка PIS-паттернов по пачке позиций; возвращает pattern_id по каждому TF и направление сделки
async def load_emapattern_for_positions(position_uids: List[str]) -> Dict[str, Dict]:
    if not position_uids:
        return {}

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT position_uid, direction, timeframe, value_num, instance_id
            FROM positions_indicators_stat
            WHERE position_uid = ANY($1::text[])
              AND using_current_bar = true
              AND param_name = $2
              AND timeframe IN ('m5','m15','h1')
            """,
            position_uids, _PARAM_NAME,
        )

    out: Dict[str, Dict] = {}
    for r in rows:
        uid  = r["position_uid"]
        dire = r["direction"]
        tf   = r["timeframe"]
        inst = int(r["instance_id"] or 0)
        pid  = r["value_num"]

        # фильтруем по ожидаемому instance_id на всякий случай
        if tf not in _EMA_PATTERN_INSTANCE or inst != _EMA_PATTERN_INSTANCE[tf]:
            continue

        rec = out.setdefault(uid, {"direction": dire, "m5": None, "m15": None, "h1": None})
        if pid is not None:
            try:
                rec[tf] = int(pid)
            except Exception:
                # если value_num не приводится к int — пропускаем
                pass

    return out


# 🔸 Проверка одной компоненты per-TF (строгое сравнение > порогов)
def _check_component_tf(direction: str, tf: str, pattern_id: int,
                        per_tf: Dict[PerTFKey, PerTFValue],
                        min_trade_type: str, min_trade_value: Decimal, min_winrate: Decimal,
                        totals_by_dir: Dict[str, int]) -> Tuple[bool, str]:
    key: PerTFKey = (direction, tf, pattern_id)
    stat = per_tf.get(key)

    if not stat:
        return False, f"EMA {tf} pattern_id={pattern_id}: no_tf_stats → filtered"

    closed, wr, _ = stat
    wr_dec = Decimal(str(wr)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    # trade-count threshold
    if min_trade_type == "absolute":
        passed_trades = closed > int(min_trade_value)
        trade_note = f"closed={closed} >? min_closed={int(min_trade_value)}"
    else:
        base = totals_by_dir.get(direction, 0)
        need = (Decimal(str(min_trade_value)) * Decimal(base)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        passed_trades = Decimal(closed) > need
        trade_note = f"closed={closed} >? min_closed={str(need)} (dir_total={base})"

    if not passed_trades:
        return False, f"EMA {tf} pattern_id={pattern_id}: {trade_note} → filtered"

    # winrate threshold (strict >)
    if wr_dec > Decimal(str(min_winrate)):
        return True, f"EMA {tf} pattern_id={pattern_id}: wr={wr_dec} > min_wr={Decimal(str(min_winrate))} → ok"
    else:
        return False, f"EMA {tf} pattern_id={pattern_id}: wr={wr_dec} >? min_wr={Decimal(str(min_winrate))} → filtered"


# 🔸 Проверка композита (строгое сравнение > порогов)
def _check_component_comp(direction: str, triplet: str,
                          comp: Dict[CompKey, CompValue],
                          min_trade_type: str, min_trade_value: Decimal, min_winrate: Decimal,
                          totals_by_dir: Dict[str, int]) -> Tuple[bool, str]:
    key: CompKey = (direction, triplet)
    stat = comp.get(key)
    if not stat:
        return False, f"EMA comp {triplet}: no_triplet_stats → filtered"

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
        return False, f"EMA comp {triplet}: {trade_note} → filtered"

    if wr_dec > Decimal(str(min_winrate)):
        return True, f"EMA comp {triplet}: wr={wr_dec} > min_wr={Decimal(str(min_winrate))} → ok"
    else:
        return False, f"EMA comp {triplet}: wr={wr_dec} >? min_wr={Decimal(str(min_winrate))} → filtered"


# 🔸 Принятие решения по пачке позиций + запись результатов
async def process_emapattern_batch(
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
        return 0, 0, 0  # (approved, filtered, skipped)

    # Пороговые параметры теста (глобальные для теста)
    min_trade_type  = str(lab["min_trade_type"])
    min_trade_value = Decimal(str(lab["min_trade_value"]))
    min_winrate     = Decimal(str(lab["min_winrate"]))

    # Компоненты внутри сущности EMApattern:
    # solo: m5 → m15 → h1; comp в конце
    ema_solo_tfs = [p for p in lab_params if p["test_name"] == "emapattern" and p["test_type"] == "solo"]
    ema_solo_tfs = sorted(ema_solo_tfs, key=lambda x: {"m5": 1, "m15": 2, "h1": 3}.get(x.get("test_tf") or "", 99))
    ema_comp     = [p for p in lab_params if p["test_name"] == "emapattern" and p["test_type"] == "comp"]
    ordered_components = ema_solo_tfs + ema_comp

    # PIS → pattern_id по TF
    pis_patterns = await load_emapattern_for_positions(position_uids)

    # Готовим batch вставку результатов
    rows_to_insert = []
    approved = filtered = skipped = 0

    for uid in position_uids:
        rec = pis_patterns.get(uid)
        if not rec or rec.get("direction") not in ("long", "short"):
            rows_to_insert.append((
                run_id, uid, strategy_id, lab["lab_id"], "skipped_no_data", "EMA: no PIS pattern or direction"
            ))
            skipped += 1
            continue

        direction = rec["direction"]
        m5_pid  = rec.get("m5")
        m15_pid = rec.get("m15")
        h1_pid  = rec.get("h1")

        # строим triplet, если понадобится
        triplet = None
        if m5_pid is not None and m15_pid is not None and h1_pid is not None:
            triplet = f"{m5_pid}-{m15_pid}-{h1_pid}"

        # Каскадная проверка по ordered_components
        decision = "approved"
        reason_chain: List[str] = []

        for comp in ordered_components:
            tt = comp["test_type"]
            if tt == "solo":
                tf = comp.get("test_tf")
                if tf not in ("m5", "m15", "h1"):
                    continue
                current_pid = m5_pid if tf == "m5" else m15_pid if tf == "m15" else h1_pid
                if current_pid is None:
                    decision = "skipped_no_data"
                    reason_chain.append(f"EMA {tf}: no PIS pattern_id")
                    break
                ok, note = _check_component_tf(direction, tf, current_pid, per_tf_cache,
                                               min_trade_type, min_trade_value, min_winrate, totals_by_dir)
                reason_chain.append(note)
                if not ok:
                    decision = "filtered"
                    break
            else:
                if not triplet:
                    decision = "skipped_no_data"
                    reason_chain.append("EMA comp: triplet not available")
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

        rows_to_insert.append((
            run_id, uid, strategy_id, lab["lab_id"], decision, reason_text
        ))

    # Вставка пачки результатов
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