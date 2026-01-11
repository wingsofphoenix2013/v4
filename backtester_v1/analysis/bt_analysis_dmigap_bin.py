# bt_analysis_dmigap_bin.py — анализатор распределения позиций по биннам DMI-gap (+DI − −DI) (адаптивные границы q6 + запись в bt_analysis_bin_dict_adaptive)

import logging
import json
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal, InvalidOperation, ROUND_DOWN

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_DMIGAP_BIN")

# 🔸 Единая точность для адаптивных биннов (источник истины)
Q6 = Decimal("0.000001")


# 🔸 Публичная точка входа анализатора DMI-gap/bin (линейные адаптивные бины по диапазону + запись границ в bt_analysis_bin_dict_adaptive)
async def run_dmigap_bin_analysis(
    analysis: Dict[str, Any],
    analysis_ctx: Dict[str, Any],
    pg,
    redis,  # оставляем для совместимости сигнатур, здесь не используется
) -> Dict[str, Any]:
    analysis_id = analysis.get("id")
    family_key = str(analysis.get("family_key") or "").strip()
    analysis_key = str(analysis.get("key") or "").strip()
    name = analysis.get("name")

    params = analysis.get("params") or {}
    scenario_id = analysis_ctx.get("scenario_id")
    signal_id = analysis_ctx.get("signal_id")

    # run-aware поля
    run_id = analysis_ctx.get("run_id")
    run_finished_at = analysis_ctx.get("run_finished_at")

    # базовые параметры анализатора
    tf = _get_str_param(params, "tf", default="m5")                      # TF из raw_stat["tf"][tf]
    base_param_name = _get_str_param(params, "param_name", "adx_dmi14")  # например adx_dmi14

    if analysis_id is None or scenario_id is None or signal_id is None or run_id is None:
        log.debug(
            "BT_ANALYSIS_DMIGAP_BIN: нет обязательных идентификаторов (analysis_id=%s, scenario_id=%s, signal_id=%s) — анализ пропущен",
            analysis_id,
            scenario_id,
            signal_id,
        )
        return {
            "rows": [],
            "summary": {
                "positions_total": 0,
                "positions_used": 0,
                "positions_skipped": 0,
                "skipped_reason": "missing_ids",
            },
        }

    log.debug(
        "BT_ANALYSIS_DMIGAP_BIN: старт анализа id=%s (family=%s, key=%s, name=%s) "
        "для scenario_id=%s, signal_id=%s, tf=%s, base_param_name=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        tf,
        base_param_name,
    )

    # загружаем позиции окна run (status=closed + postproc=true) — строго в границах window_from/window_to
    window_from = analysis_ctx.get("window_from")
    window_to = analysis_ctx.get("window_to")

    positions = await _load_positions_for_analysis(pg, int(scenario_id), int(signal_id), window_from, window_to)
    if not positions:
        log.debug(
            "BT_ANALYSIS_DMIGAP_BIN: нет позиций для анализа id=%s (family=%s, key=%s, name=%s), scenario_id=%s, signal_id=%s",
            analysis_id,
            family_key,
            analysis_key,
            name,
            scenario_id,
            signal_id,
        )
        return {
            "rows": [],
            "summary": {
                "positions_total": 0,
                "positions_used": 0,
                "positions_skipped": 0,
            },
        }

    positions_total = len(positions)
    valid_values: List[Decimal] = []
    valid_positions: List[Dict[str, Any]] = []

    # первый проход: считаем dmi_gap (q6) для каждой позиции
    for p in positions:
        raw_stat = p["raw_stat"]
        dmi_gap = _extract_dmigap_from_raw_stat(raw_stat, tf, base_param_name)
        if dmi_gap is None:
            p["dmi_gap"] = None
            continue

        dmi_gap_q = _q6(dmi_gap)
        p["dmi_gap"] = dmi_gap_q
        valid_values.append(dmi_gap_q)
        valid_positions.append(p)

    if not valid_positions:
        log.debug(
            "BT_ANALYSIS_DMIGAP_BIN: нет валидных значений DMI-gap для анализа id=%s (family=%s, key=%s, name=%s), "
            "scenario_id=%s, signal_id=%s — positions_total=%s",
            analysis_id,
            family_key,
            analysis_key,
            name,
            scenario_id,
            signal_id,
            positions_total,
        )
        return {
            "rows": [],
            "summary": {
                "positions_total": positions_total,
                "positions_used": 0,
                "positions_skipped": positions_total,
            },
        }

    min_gap = min(valid_values)
    max_gap = max(valid_values)

    # строим линейные бины по диапазону [min_gap .. max_gap] (в q6)
    bins_count = 10
    bins = _build_dmigap_bins(
        min_val=min_gap,
        max_val=max_gap,
        bins_count=bins_count,
        tf=tf,
    )

    # записываем адаптивный словарь биннов в БД (на каждый проход)
    # source_finished_at — единый по run: bt_signal_backfill_runs.finished_at
    source_finished_at = run_finished_at if isinstance(run_finished_at, datetime) else datetime.utcnow()
    try:
        inserted_rows = await _store_adaptive_bins(
            pg=pg,
            run_id=int(run_id),
            analysis_id=int(analysis_id),
            scenario_id=int(scenario_id),
            signal_id=int(signal_id),
            tf=tf,
            bins=bins,
            source_finished_at=source_finished_at,
        )
        log.debug(
            "BT_ANALYSIS_DMIGAP_BIN: записан bt_analysis_bin_dict_adaptive — analysis_id=%s, scenario_id=%s, signal_id=%s, tf=%s, rows=%s, min_gap=%s, max_gap=%s",
            analysis_id,
            scenario_id,
            signal_id,
            tf,
            inserted_rows,
            str(min_gap),
            str(max_gap),
        )
    except Exception as e:
        log.error(
            "BT_ANALYSIS_DMIGAP_BIN: ошибка записи bt_analysis_bin_dict_adaptive для analysis_id=%s, scenario_id=%s, signal_id=%s: %s",
            analysis_id,
            scenario_id,
            signal_id,
            e,
            exc_info=True,
        )

    rows: List[Dict[str, Any]] = []
    positions_used = 0
    positions_skipped = positions_total - len(valid_positions)

    # второй проход: раскладываем позиции по бинам (с q6-значением)
    for p in valid_positions:
        position_uid = p["position_uid"]
        direction = p["direction"]
        pnl_abs = p["pnl_abs"]
        dmi_gap = p.get("dmi_gap")

        if dmi_gap is None:
            positions_skipped += 1
            continue

        v = _q6(dmi_gap)

        # клипуем значение в [min_gap, max_gap] на всякий случай
        if v < min_gap:
            v = min_gap
        if v > max_gap:
            v = max_gap

        bin_name = _assign_bin(bins, v)
        if bin_name is None:
            positions_skipped += 1
            continue

        rows.append(
            {
                "position_uid": position_uid,
                "timeframe": tf,
                "direction": direction,
                "bin_name": bin_name,
                "value": v,        # DMI-gap (+DI - -DI) (q6)
                "pnl_abs": pnl_abs,
            }
        )
        positions_used += 1

    log.debug(
        "BT_ANALYSIS_DMIGAP_BIN: summary id=%s (family=%s, key=%s, name=%s), scenario_id=%s, signal_id=%s — "
        "positions_total=%s, valid=%s, used=%s, skipped=%s, rows=%s, min_gap=%s, max_gap=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        positions_total,
        len(valid_positions),
        positions_used,
        positions_skipped,
        len(rows),
        str(min_gap),
        str(max_gap),
    )

    return {
        "rows": rows,
        "summary": {
            "positions_total": positions_total,
            "positions_used": positions_used,
            "positions_skipped": positions_skipped,
            "min_gap": str(min_gap),
            "max_gap": str(max_gap),
            "source_finished_at": source_finished_at.isoformat(),
        },
    }


# 🔸 Загрузка позиций сценария/сигнала с postproc=true
async def _load_positions_for_analysis(
    pg,
    scenario_id: int,
    signal_id: int,
    window_from: Optional[Any],
    window_to: Optional[Any],
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        # условия достаточности
        if window_from is None or window_to is None:
            return []

        rows = await conn.fetch(
            """
            SELECT
                position_uid,
                timeframe,
                direction,
                pnl_abs,
                raw_stat
            FROM bt_scenario_positions_v2
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND status      = 'closed'
              AND postproc_v2 = true
              AND entry_time BETWEEN $3 AND $4
            ORDER BY entry_time
            """,
            scenario_id,
            signal_id,
            window_from,
            window_to,
        )

    positions: List[Dict[str, Any]] = []
    for r in rows:
        raw = r["raw_stat"]

        # приводим jsonb к dict, если он пришёл строкой
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                raw = None

        positions.append(
            {
                "position_uid": r["position_uid"],
                "timeframe": r["timeframe"],
                "direction": r["direction"],
                "pnl_abs": _safe_decimal(r["pnl_abs"]),
                "raw_stat": raw,
            }
        )

    log.debug(
        "BT_ANALYSIS_DMIGAP_BIN: загружено позиций для анализа scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(positions),
    )
    return positions


# 🔸 Извлечение DMI-gap (+DI − −DI) из raw_stat по TF и базовому имени (например, 'adx_dmi14')
def _extract_dmigap_from_raw_stat(
    raw_stat: Any,
    tf: str,
    base_param_name: str,
) -> Optional[Decimal]:
    # если raw_stat пришёл строкой из jsonb — парсим
    if isinstance(raw_stat, str):
        try:
            raw_stat = json.loads(raw_stat)
        except Exception:
            return None

    if not isinstance(raw_stat, dict):
        return None

    tf_block = (raw_stat.get("tf") or {}).get(tf)
    if not isinstance(tf_block, dict):
        return None

    indicators = tf_block.get("indicators") or {}
    dmi_family = indicators.get("adx_dmi") or {}
    if not isinstance(dmi_family, dict):
        return None

    plus_key = f"{base_param_name}_plus_di"
    minus_key = f"{base_param_name}_minus_di"

    plus_val = dmi_family.get(plus_key)
    minus_val = dmi_family.get(minus_key)

    if plus_val is None or minus_val is None:
        return None

    plus_dec = _safe_decimal(plus_val)
    minus_dec = _safe_decimal(minus_val)

    return plus_dec - minus_dec


# 🔸 Построение линейных биннов по диапазону DMI-gap (bin_name в формате TF_BIN_N, границы q6)
def _build_dmigap_bins(
    min_val: Decimal,
    max_val: Decimal,
    bins_count: int = 10,
    tf: str = "m5",
) -> List[Dict[str, Any]]:
    bins: List[Dict[str, Any]] = []
    tf_up = str(tf or "").strip().upper()

    min_q = _q6(min_val)
    max_q = _q6(max_val)

    # вырожденный диапазон — все бины одинаковые
    if max_q <= min_q:
        for i in range(bins_count):
            bins.append(
                {
                    "bin_order": i,
                    "bin_name": f"{tf_up}_bin_{i}",
                    "min": min_q,
                    "max": max_q,
                    "to_inclusive": (i == bins_count - 1),
                }
            )
        return bins

    total_range = max_q - min_q
    step = _q6(total_range / Decimal(bins_count))

    # если шаг “схлопнулся” до 0 из-за q6 — считаем диапазон вырожденным
    if step <= Decimal("0"):
        for i in range(bins_count):
            bins.append(
                {
                    "bin_order": i,
                    "bin_name": f"{tf_up}_bin_{i}",
                    "min": min_q,
                    "max": max_q,
                    "to_inclusive": (i == bins_count - 1),
                }
            )
        return bins

    # первые bins_count-1 бинов [min, max)
    for i in range(bins_count - 1):
        lo = _q6(min_q + step * Decimal(i))
        hi = _q6(min_q + step * Decimal(i + 1))
        bins.append(
            {
                "bin_order": i,
                "bin_name": f"{tf_up}_bin_{i}",
                "min": lo,
                "max": hi,
                "to_inclusive": False,
            }
        )

    # последний бин [min_last, max_q] включительно
    lo_last = _q6(min_q + step * Decimal(bins_count - 1))
    bins.append(
        {
            "bin_order": bins_count - 1,
            "bin_name": f"{tf_up}_bin_{bins_count - 1}",
            "min": lo_last,
            "max": max_q,
            "to_inclusive": True,
        }
    )

    return bins


# 🔸 Определение имени бина для значения DMI-gap (q6-границы)
def _assign_bin(
    bins: List[Dict[str, Any]],
    value: Decimal,
) -> Optional[str]:
    if not bins:
        return None

    last_index = len(bins) - 1
    v = _q6(value)

    for idx, b in enumerate(bins):
        name = b.get("bin_name")
        lo = b.get("min")
        hi = b.get("max")
        to_inclusive = bool(b.get("to_inclusive"))

        if lo is None or hi is None or name is None:
            continue

        lo_q = _q6(lo)
        hi_q = _q6(hi)

        if to_inclusive or idx == last_index:
            if lo_q <= v <= hi_q:
                return str(name)
        else:
            if lo_q <= v < hi_q:
                return str(name)

    return None


# 🔸 Запись адаптивных биннов в bt_analysis_bin_dict_adaptive (дублирование под long/short, границы q6)
async def _store_adaptive_bins(
    pg,
    run_id: int,
    analysis_id: int,
    scenario_id: int,
    signal_id: int,
    tf: str,
    bins: List[Dict[str, Any]],
    source_finished_at: datetime,
) -> int:
    if not bins:
        return 0

    to_insert: List[Tuple[Any, ...]] = []
    tf_l = str(tf or "").strip().lower()

    for direction in ("long", "short"):
        for b in bins:
            bin_order = int(b.get("bin_order") or 0)
            bin_name = str(b.get("bin_name") or "")
            val_from = _q6(b.get("min"))
            val_to = _q6(b.get("max"))
            to_inclusive = bool(b.get("to_inclusive"))

            to_insert.append(
                (
                    run_id,
                    analysis_id,
                    scenario_id,
                    signal_id,
                    direction,
                    tf_l,
                    "bins",
                    bin_order,
                    bin_name,
                    val_from,
                    val_to,
                    to_inclusive,
                    source_finished_at,
                )
            )

    async with pg.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO bt_analysis_bin_dict_adaptive (
                run_id,
                analysis_id,
                scenario_id,
                signal_id,
                direction,
                timeframe,
                bin_type,
                bin_order,
                bin_name,
                val_from,
                val_to,
                to_inclusive,
                source_finished_at,
                created_at
            )
            VALUES (
                $1, $2, $3, $4,
                $5, $6, $7,
                $8, $9, $10, $11,
                $12, $13,
                now()
            )
            ON CONFLICT (run_id, analysis_id, scenario_id, signal_id, direction, timeframe, bin_type, bin_order)
            DO UPDATE SET
                bin_name           = EXCLUDED.bin_name,
                val_from           = EXCLUDED.val_from,
                val_to             = EXCLUDED.val_to,
                to_inclusive       = EXCLUDED.to_inclusive,
                source_finished_at = EXCLUDED.source_finished_at,
                updated_at         = now()
            """,
            to_insert,
        )

    return len(to_insert)


# 🔸 q6 квантизация (ROUND_DOWN) — источник истины
def _q6(value: Any) -> Decimal:
    try:
        if isinstance(value, Decimal):
            d = value
        else:
            d = Decimal(str(value))
        return d.quantize(Q6, rounding=ROUND_DOWN)
    except Exception:
        return Decimal("0").quantize(Q6, rounding=ROUND_DOWN)


# 🔸 Вспомогательная функция: безопасное чтение str-параметра
def _get_str_param(params: Dict[str, Any], name: str, default: str) -> str:
    cfg = params.get(name)
    if cfg is None:
        return default

    raw = cfg.get("value")
    if raw is None:
        return default

    return str(raw).strip()


# 🔸 Вспомогательная функция: безопасное приведение к Decimal
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")