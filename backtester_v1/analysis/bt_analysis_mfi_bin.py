# bt_analysis_mfi_bin.py — анализатор распределения позиций по биннам MFI (биннинг берётся из bt_analysis_bins_dict)

import logging
import json
from typing import Dict, Any, List, Optional
from decimal import Decimal, InvalidOperation, ROUND_DOWN

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_MFI_BIN")


# 🔸 Публичная точка входа анализатора MFI/bin
async def run_mfi_bin_analysis(
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
    tf = _get_str_param(params, "tf", default="m5")                  # TF из raw_stat["tf"][tf]
    mfi_param_name = _get_str_param(params, "param_name", "mfi14")   # например mfi14 / mfi21

    # загружаем конфигурацию биннов из словаря
    if analysis_id is None:
        log.debug(
            "BT_ANALYSIS_MFI_BIN: analysis_id отсутствует (family=%s, key=%s, name=%s), "
            "scenario_id=%s, signal_id=%s — анализ пропущен",
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
                "skipped_reason": "no_analysis_id",
            },
        }

    bins_by_dir = await _load_bins_dict_for_analysis(pg, int(analysis_id), tf)
    if not bins_by_dir:
        log.debug(
            "BT_ANALYSIS_MFI_BIN: нет биннов в bt_analysis_bins_dict для analysis_id=%s, tf=%s "
            "(family=%s, key=%s, name=%s), scenario_id=%s, signal_id=%s — анализ пропущен",
            analysis_id,
            tf,
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
                "skipped_reason": "no_bins_dict",
            },
        }

    log.debug(
        "BT_ANALYSIS_MFI_BIN: старт анализа id=%s (family=%s, key=%s, name=%s) "
        "для scenario_id=%s, signal_id=%s, tf=%s, mfi_param_name=%s, bins_loaded=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        tf,
        mfi_param_name,
        {d: len(b) for d, b in bins_by_dir.items()},
    )

    # загружаем позиции окна run (status=closed + postproc=true) — строго в границах window_from/window_to
    window_from = analysis_ctx.get("window_from")
    window_to = analysis_ctx.get("window_to")

    positions = await _load_positions_for_analysis(pg, scenario_id, signal_id, window_from, window_to)
    if not positions:
        log.debug(
            "BT_ANALYSIS_MFI_BIN: нет позиций для анализа id=%s (family=%s, key=%s, name=%s), "
            "scenario_id=%s, signal_id=%s",
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

    rows: List[Dict[str, Any]] = []
    positions_total = 0
    positions_used = 0
    positions_skipped = 0

    for p in positions:
        positions_total += 1

        position_uid = p["position_uid"]
        direction = str(p["direction"] or "").strip().lower()
        pnl_abs = p["pnl_abs"]
        raw_stat = p["raw_stat"]

        # бины зависят от направления
        bins = bins_by_dir.get(direction)
        if not bins:
            positions_skipped += 1
            continue

        # извлекаем значение MFI из raw_stat по TF и param_name
        mfi_dec = _extract_mfi_from_raw_stat(raw_stat, tf, mfi_param_name)
        if mfi_dec is None:
            positions_skipped += 1
            continue

        # клипуем MFI в диапазон [0, 100]
        if mfi_dec < Decimal("0"):
            mfi_dec = Decimal("0")
        if mfi_dec > Decimal("100"):
            mfi_dec = Decimal("100")

        # квантизация до разумной точности (4 знака после запятой)
        mfi_dec = _q_decimal(mfi_dec)

        bin_name = _assign_bin(bins, mfi_dec)
        if bin_name is None:
            positions_skipped += 1
            continue

        rows.append(
            {
                "position_uid": position_uid,
                "timeframe": tf,
                "direction": direction,
                "bin_name": bin_name,
                "value": mfi_dec,   # Decimal -> numeric без хвостов
                "pnl_abs": pnl_abs, # уже Decimal
            }
        )
        positions_used += 1

    log.debug(
        "BT_ANALYSIS_MFI_BIN: анализатор id=%s (family=%s, key=%s, name=%s), scenario_id=%s, signal_id=%s — "
        "tf=%s, mfi_param_name=%s, позиций всего=%s, использовано=%s, пропущено=%s, строк_в_результате=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        tf,
        mfi_param_name,
        positions_total,
        positions_used,
        positions_skipped,
        len(rows),
    )

    return {
        "rows": rows,
        "summary": {
            "positions_total": positions_total,
            "positions_used": positions_used,
            "positions_skipped": positions_skipped,
        },
    }


# 🔸 Загрузка биннов из bt_analysis_bins_dict для analysis_id + tf (группировка по direction)
async def _load_bins_dict_for_analysis(
    pg,
    analysis_id: int,
    timeframe: str,
) -> Dict[str, List[Dict[str, Any]]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                direction,
                bin_order,
                bin_name,
                val_from,
                val_to,
                to_inclusive
            FROM bt_analysis_bins_dict
            WHERE analysis_id = $1
              AND timeframe   = $2
              AND bin_type    = 'bins'
            ORDER BY direction, bin_order
            """,
            analysis_id,
            timeframe,
        )

    if not rows:
        return {}

    out: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        direction = str(r["direction"] or "").strip().lower()
        if not direction:
            continue

        out.setdefault(direction, []).append(
            {
                "name": str(r["bin_name"]),
                "min": _safe_decimal(r["val_from"]),
                "max": _safe_decimal(r["val_to"]),
                "to_inclusive": bool(r["to_inclusive"]),
            }
        )

    return out


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
        "BT_ANALYSIS_MFI_BIN: загружено позиций для анализа scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(positions),
    )
    return positions


# 🔸 Извлечение значения MFI из raw_stat по TF и имени параметра (например, 'mfi14')
def _extract_mfi_from_raw_stat(
    raw_stat: Any,
    tf: str,
    mfi_param_name: str,
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
    mfi_family = indicators.get("mfi") or {}
    if not isinstance(mfi_family, dict):
        return None

    value = mfi_family.get(mfi_param_name)
    if value is None:
        return None

    return _safe_decimal(value)


# 🔸 Определение имени бина для значения MFI (границы из bt_analysis_bins_dict)
def _assign_bin(
    bins: List[Dict[str, Any]],
    value: Decimal,
) -> Optional[str]:
    if not bins:
        return None

    for b in bins:
        name = b.get("name")
        lo = b.get("min")
        hi = b.get("max")
        to_inclusive = bool(b.get("to_inclusive"))

        if lo is None or hi is None or name is None:
            continue

        # обычный бин: [min, max)
        # inclusive бин: [min, max]
        if to_inclusive:
            if lo <= value <= hi:
                return str(name)
        else:
            if lo <= value < hi:
                return str(name)

    return None


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


# 🔸 Вспомогательная функция: квантизация Decimal до 4 знаков
def _q_decimal(value: Decimal) -> Decimal:
    # 4 знака после запятой, округление вниз для предсказуемости
    return value.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)