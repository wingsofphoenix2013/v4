# bt_analysis_atr_sl.py — анализатор распределения позиций по биннам отношения SL к ATR

import logging
import json
from typing import Dict, Any, List, Optional
from decimal import Decimal, InvalidOperation, ROUND_DOWN

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_ATR_SL")

# 🔸 Маленький эпсилон для нормализации ATR
EPSILON = Decimal("0.0000001")


# 🔸 Публичная точка входа анализатора ATR/SL
async def run_atr_sl_analysis(
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
    base_param_name = _get_str_param(params, "param_name", "atr14")  # например atr14

    log.debug(
        "BT_ANALYSIS_ATR_SL: старт анализа id=%s (family=%s, key=%s, name=%s) "
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

    # загружаем позиции данного сценария/сигнала, прошедшие постпроцессинг (есть raw_stat)
    positions = await _load_positions_for_analysis(pg, scenario_id, signal_id)
    if not positions:
        log.debug(
            "BT_ANALYSIS_ATR_SL: нет позиций для анализа id=%s, scenario_id=%s, signal_id=%s",
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
            },
        }

    positions_total = len(positions)
    valid_positions: List[Dict[str, Any]] = []
    valid_ratios: List[Decimal] = []

    # первый проход: считаем ratio SL/ATR для каждой позиции
    for p in positions:
        raw_stat = p["raw_stat"]
        entry_price = p["entry_price"]
        sl_price = p["sl_price"]

        ratio = _compute_ratio_sl_atr(raw_stat, tf, base_param_name, entry_price, sl_price)
        if ratio is None:
            p["ratio"] = None
            continue

        p["ratio"] = ratio
        valid_positions.append(p)
        valid_ratios.append(ratio)

    if not valid_positions:
        log.info(
            "BT_ANALYSIS_ATR_SL: анализатор id=%s (family=%s, key=%s, name=%s) "
            "для scenario_id=%s, signal_id=%s — нет валидных значений SL/ATR для анализа "
            "(positions_total=%s)",
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

    # пытаемся загрузить бины из параметров; если не заданы — строим линейные по диапазону [min_ratio..max_ratio]
    bins = _load_bins_from_params(params)
    if not bins:
        min_ratio = min(valid_ratios)
        max_ratio = max(valid_ratios)
        bins = _build_ratio_bins(min_ratio, max_ratio, bins_count=10)

    rows: List[Dict[str, Any]] = []
    positions_used = 0
    positions_skipped = positions_total - len(valid_positions)

    # второй проход: раскладываем позиции по бинам
    for p in valid_positions:
        position_uid = p["position_uid"]
        direction = p["direction"]
        pnl_abs = p["pnl_abs"]
        ratio = p.get("ratio")

        if ratio is None:
            positions_skipped += 1
            continue

        bin_name = _assign_bin(bins, ratio)
        if bin_name is None:
            positions_skipped += 1
            continue

        ratio_q = _q_decimal(ratio)

        rows.append(
            {
                "position_uid": position_uid,
                "timeframe": tf,
                "direction": direction,
                "bin_name": bin_name,
                "value": ratio_q,   # SL_distance / ATR
                "pnl_abs": pnl_abs,
            }
        )
        positions_used += 1

    log.info(
        "BT_ANALYSIS_ATR_SL: анализатор id=%s (family=%s, key=%s, name=%s), "
        "scenario_id=%s, signal_id=%s — позиций всего=%s, валидных=%s, использовано=%s, "
        "пропущено=%s, строк_в_результате=%s, min_ratio=%s, max_ratio=%s",
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
        str(min(valid_ratios)),
        str(max(valid_ratios)),
    )

    return {
        "rows": rows,
        "summary": {
            "positions_total": positions_total,
            "positions_used": positions_used,
            "positions_skipped": positions_skipped,
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
                entry_price,
                sl_price
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
                "raw_stat": raw,
                "entry_price": _safe_decimal(r["entry_price"]),
                "sl_price": _safe_decimal(r["sl_price"]),
            }
        )

    log.debug(
        "BT_ANALYSIS_ATR_SL: загружено позиций для анализа scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(positions),
    )
    return positions


# 🔸 Расчёт ratio SL/ATR для одной позиции
def _compute_ratio_sl_atr(
    raw_stat: Any,
    tf: str,
    base_param_name: str,
    entry_price: Decimal,
    sl_price: Decimal,
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

    atr_val = atr_family.get(base_param_name)
    if atr_val is None:
        return None

    atr_dec = _safe_decimal(atr_val)
    if atr_dec <= Decimal("0"):
        return None

    # расстояние до стоп-лосса
    sl_distance = (entry_price - sl_price).copy_abs()
    if sl_distance <= Decimal("0"):
        return None

    ratio = sl_distance / max(atr_dec, EPSILON)
    return ratio


# 🔸 Загрузка конфигурации биннов из параметров анализатора
def _load_bins_from_params(params: Dict[str, Any]) -> List[Dict[str, Decimal]]:
    bins_cfg = params.get("bins")
    if not bins_cfg:
        return []

    raw = bins_cfg.get("value")
    if not raw:
        return []

    try:
        data = json.loads(raw)
    except Exception:
        log.warning(
            "BT_ANALYSIS_ATR_SL: не удалось распарсить JSON в параметре 'bins', используется дефолтная схема"
        )
        return []

    bins: List[Dict[str, Decimal]] = []
    for item in data:
        # ожидаемый формат элемента: {"name": "lo-hi", "min": 0.0, "max": 1.0}
        if not isinstance(item, dict):
            continue

        name = item.get("name")
        min_v = item.get("min")
        max_v = item.get("max")

        if name is None or min_v is None or max_v is None:
            continue

        min_d = _safe_decimal(min_v)
        max_d = _safe_decimal(max_v)

        bins.append(
            {
                "name": str(name),
                "min": min_d,
                "max": max_d,
            }
        )

    # если после парсинга бины пустые — вернём пустой список, выше подставится динамическая схема
    return bins


# 🔸 Построение линейных бинов по диапазону ratio
def _build_ratio_bins(
    min_val: Decimal,
    max_val: Decimal,
    bins_count: int = 10,
) -> List[Dict[str, Decimal]]:
    bins: List[Dict[str, Decimal]] = []

    # вырожденный диапазон — все бины одинаковые
    if max_val <= min_val:
        for i in range(bins_count):
            name = f"bin_{i}"
            bins.append(
                {
                    "name": name,
                    "min": min_val,
                    "max": max_val,
                }
            )
        return bins

    total_range = max_val - min_val
    step = total_range / Decimal(bins_count)

    # первые bins_count-1 бинов [min, max)
    for i in range(bins_count - 1):
        lo = min_val + step * Decimal(i)
        hi = min_val + step * Decimal(i + 1)
        name = f"bin_{i}"
        bins.append(
            {
                "name": name,
                "min": lo,
                "max": hi,
            }
        )

    # последний бин [min_last, max_val] включительно
    lo_last = min_val + step * Decimal(bins_count - 1)
    bins.append(
        {
            "name": f"bin_{bins_count - 1}",
            "min": lo_last,
            "max": max_val,
        }
    )

    return bins


# 🔸 Определение имени бина для значения ratio
def _assign_bin(
    bins: List[Dict[str, Decimal]],
    value: Decimal,
) -> Optional[str]:
    # все бины кроме последнего: [min, max)
    # последний бин: [min, max] (включая верхнюю границу)
    if not bins:
        return None

    last_index = len(bins) - 1

    for idx, b in enumerate(bins):
        name = b.get("name")
        lo = b.get("min")
        hi = b.get("max")

        if lo is None or hi is None or name is None:
            continue

        if idx < last_index:
            if lo <= value < hi:
                return str(name)
        else:
            # последний бин включительно по верхней границе
            if lo <= value <= hi:
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