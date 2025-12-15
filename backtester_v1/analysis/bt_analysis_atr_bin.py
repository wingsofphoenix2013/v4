# bt_analysis_atr_bin.py — анализатор распределения позиций по биннам ATR в процентах от цены входа (адаптивные границы + запись в bt_analysis_bin_dict_adaptive)

import logging
import json
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal, InvalidOperation, ROUND_DOWN

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_ATR_BIN")


# 🔸 Публичная точка входа анализатора ATR/bin (ATR% от цены входа, линейные бины по диапазону + запись границ в bt_analysis_bin_dict_adaptive)
async def run_atr_bin_analysis(
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

    # базовые параметры анализатора
    tf = _get_str_param(params, "tf", default="m5")                 # TF из raw_stat["tf"][tf]
    atr_param_name = _get_str_param(params, "param_name", "atr14")  # например atr14

    if analysis_id is None or scenario_id is None or signal_id is None:
        log.info(
            "BT_ANALYSIS_ATR_BIN: нет обязательных идентификаторов (analysis_id=%s, scenario_id=%s, signal_id=%s) — анализ пропущен",
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
        "BT_ANALYSIS_ATR_BIN: старт анализа id=%s (family=%s, key=%s, name=%s) "
        "для scenario_id=%s, signal_id=%s, tf=%s, atr_param_name=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        tf,
        atr_param_name,
    )

    # загружаем позиции данного сценария/сигнала, прошедшие постпроцессинг (есть raw_stat и entry_price)
    positions = await _load_positions_for_analysis(pg, int(scenario_id), int(signal_id))
    if not positions:
        log.info(
            "BT_ANALYSIS_ATR_BIN: нет позиций для анализа id=%s (family=%s, key=%s, name=%s), scenario_id=%s, signal_id=%s",
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

    # первый проход: считаем atr_pct для каждой позиции
    for p in positions:
        raw_stat = p["raw_stat"]
        entry_price: Decimal = p["entry_price"]

        if entry_price <= Decimal("0"):
            p["atr_pct"] = None
            continue

        atr_val = _extract_atr_from_raw_stat(raw_stat, tf, atr_param_name)
        if atr_val is None or atr_val < Decimal("0"):
            p["atr_pct"] = None
            continue

        try:
            atr_pct = (atr_val / entry_price) * Decimal("100")
        except Exception:
            p["atr_pct"] = None
            continue

        p["atr_pct"] = atr_pct
        valid_values.append(atr_pct)
        valid_positions.append(p)

    if not valid_positions:
        log.info(
            "BT_ANALYSIS_ATR_BIN: нет валидных значений ATR%% для анализа id=%s (family=%s, key=%s, name=%s), "
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

    min_atr_pct = min(valid_values)
    max_atr_pct = max(valid_values)

    # строим линейные бины по диапазону [min_atr_pct .. max_atr_pct]
    bins_count = 10
    bins = _build_atr_bins(
        min_val=min_atr_pct,
        max_val=max_atr_pct,
        bins_count=bins_count,
        tf=tf,
    )

    # записываем адаптивный словарь биннов в БД (на каждый проход)
    source_finished_at = datetime.utcnow()
    try:
        inserted_rows = await _store_adaptive_bins(
            pg=pg,
            analysis_id=int(analysis_id),
            scenario_id=int(scenario_id),
            signal_id=int(signal_id),
            tf=tf,
            bins=bins,
            source_finished_at=source_finished_at,
        )
        log.info(
            "BT_ANALYSIS_ATR_BIN: записан bt_analysis_bin_dict_adaptive — analysis_id=%s, scenario_id=%s, signal_id=%s, tf=%s, rows=%s, min_atr_pct=%s, max_atr_pct=%s",
            analysis_id,
            scenario_id,
            signal_id,
            tf,
            inserted_rows,
            str(min_atr_pct),
            str(max_atr_pct),
        )
    except Exception as e:
        log.error(
            "BT_ANALYSIS_ATR_BIN: ошибка записи bt_analysis_bin_dict_adaptive для analysis_id=%s, scenario_id=%s, signal_id=%s: %s",
            analysis_id,
            scenario_id,
            signal_id,
            e,
            exc_info=True,
        )

    rows: List[Dict[str, Any]] = []
    positions_used = 0
    positions_skipped = positions_total - len(valid_positions)

    # второй проход: раскладываем позиции по бинам
    for p in valid_positions:
        position_uid = p["position_uid"]
        direction = p["direction"]
        pnl_abs = p["pnl_abs"]
        atr_pct = p.get("atr_pct")

        if atr_pct is None:
            positions_skipped += 1
            continue

        # клипуем значение в [min_atr_pct, max_atr_pct] на всякий случай
        if atr_pct < min_atr_pct:
            atr_pct = min_atr_pct
        if atr_pct > max_atr_pct:
            atr_pct = max_atr_pct

        bin_name = _assign_bin(bins, atr_pct)
        if bin_name is None:
            positions_skipped += 1
            continue

        rows.append(
            {
                "position_uid": position_uid,
                "timeframe": tf,
                "direction": direction,
                "bin_name": bin_name,
                "value": atr_pct,   # ATR в процентах от цены входа
                "pnl_abs": pnl_abs,
            }
        )
        positions_used += 1

    log.info(
        "BT_ANALYSIS_ATR_BIN: summary id=%s (family=%s, key=%s, name=%s), scenario_id=%s, signal_id=%s — "
        "positions_total=%s, valid=%s, used=%s, skipped=%s, rows=%s, min_atr_pct=%s, max_atr_pct=%s",
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
        str(min_atr_pct),
        str(max_atr_pct),
    )

    return {
        "rows": rows,
        "summary": {
            "positions_total": positions_total,
            "positions_used": positions_used,
            "positions_skipped": positions_skipped,
            "min_atr_pct": str(min_atr_pct),
            "max_atr_pct": str(max_atr_pct),
            "source_finished_at": source_finished_at.isoformat(),
        },
    }


# 🔸 Загрузка позиций сценария/сигнала с postproc=true
async def _load_positions_for_analysis(
    pg,
    scenario_id: int,
    signal_id: int,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                position_uid,
                timeframe,
                direction,
                pnl_abs,
                raw_stat,
                entry_price
            FROM bt_scenario_positions
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND postproc    = true
            ORDER BY entry_time
            """,
            scenario_id,
            signal_id,
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
                "entry_price": _safe_decimal(r["entry_price"]),
                "raw_stat": raw,
            }
        )

    log.debug(
        "BT_ANALYSIS_ATR_BIN: загружено позиций для анализа scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(positions),
    )
    return positions


# 🔸 Извлечение значения ATR из raw_stat по TF и имени параметра (например, 'atr14')
def _extract_atr_from_raw_stat(
    raw_stat: Any,
    tf: str,
    atr_param_name: str,
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
    atr_family = indicators.get("atr") or {}
    if not isinstance(atr_family, dict):
        return None

    value = atr_family.get(atr_param_name)
    if value is None:
        return None

    return _safe_decimal(value)


# 🔸 Построение линейных бинов по диапазону ATR% (bin_name в формате TF_BIN_N)
def _build_atr_bins(
    min_val: Decimal,
    max_val: Decimal,
    bins_count: int = 10,
    tf: str = "m5",
) -> List[Dict[str, Any]]:
    bins: List[Dict[str, Any]] = []
    tf_up = str(tf or "").strip().upper()

    # вырожденный диапазон — все бины одинаковые
    if max_val <= min_val:
        for i in range(bins_count):
            bins.append(
                {
                    "bin_order": i,
                    "bin_name": f"{tf_up}_bin_{i}",
                    "min": min_val,
                    "max": max_val,
                    "to_inclusive": (i == bins_count - 1),
                }
            )
        return bins

    total_range = max_val - min_val
    step = total_range / Decimal(bins_count)

    # первые bins_count-1 бинов [min, max)
    for i in range(bins_count - 1):
        lo = min_val + step * Decimal(i)
        hi = min_val + step * Decimal(i + 1)
        bins.append(
            {
                "bin_order": i,
                "bin_name": f"{tf_up}_bin_{i}",
                "min": lo,
                "max": hi,
                "to_inclusive": False,
            }
        )

    # последний бин [min_last, max_val] включительно
    lo_last = min_val + step * Decimal(bins_count - 1)
    bins.append(
        {
            "bin_order": bins_count - 1,
            "bin_name": f"{tf_up}_bin_{bins_count - 1}",
            "min": lo_last,
            "max": max_val,
            "to_inclusive": True,
        }
    )

    return bins


# 🔸 Определение имени бина для значения ATR%
def _assign_bin(
    bins: List[Dict[str, Any]],
    value: Decimal,
) -> Optional[str]:
    if not bins:
        return None

    last_index = len(bins) - 1

    for idx, b in enumerate(bins):
        name = b.get("bin_name")
        lo = b.get("min")
        hi = b.get("max")
        to_inclusive = bool(b.get("to_inclusive"))

        if lo is None or hi is None or name is None:
            continue

        # обычный бин: [min, max)
        # inclusive бин: [min, max]
        if to_inclusive or idx == last_index:
            if lo <= value <= hi:
                return str(name)
        else:
            if lo <= value < hi:
                return str(name)

    return None


# 🔸 Запись адаптивных биннов в bt_analysis_bin_dict_adaptive (дублирование под long/short)
async def _store_adaptive_bins(
    pg,
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
            val_from = b.get("min")
            val_to = b.get("max")
            to_inclusive = bool(b.get("to_inclusive"))

            to_insert.append(
                (
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
                $1, $2, $3,
                $4, $5, $6,
                $7, $8, $9, $10,
                $11, $12,
                now()
            )
            ON CONFLICT ON CONSTRAINT bt_analysis_bin_dict_adaptive_uniq_order
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