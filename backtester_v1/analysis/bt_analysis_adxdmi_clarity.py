# bt_analysis_adxdmi_clarity.py — анализатор распределения позиций по биннам ADX/DMI clarity

import logging
import json
from typing import Dict, Any, List, Optional
from decimal import Decimal, InvalidOperation, ROUND_DOWN

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_ADXDMI_CLARITY")

# 🔸 Маленький эпсилон для нормализации суммы DMI
EPSILON = Decimal("0.000001")


# 🔸 Публичная точка входа анализатора ADX/DMI clarity
async def run_adxdmi_clarity_analysis(
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
    tf = _get_str_param(params, "tf", default="m5")                    # TF из raw_stat["tf"][tf]
    base_param_name = _get_str_param(params, "param_name", "adx_dmi14")  # например adx_dmi14 / adx_dmi21

    # загружаем конфигурацию биннов (если не задана — используем дефолт 10 бинов по [0,1])
    bins = _load_bins_from_params(params)
    if not bins:
        bins = _default_clarity_bins()

    log.debug(
        "BT_ANALYSIS_ADXDMI_CLARITY: старт анализа id=%s (family=%s, key=%s, name=%s) "
        "для scenario_id=%s, signal_id=%s, tf=%s, base_param_name=%s, bins=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        tf,
        base_param_name,
        bins,
    )

    # загружаем позиции данного сценария/сигнала, прошедшие постпроцессинг (есть raw_stat)
    positions = await _load_positions_for_analysis(pg, scenario_id, signal_id)
    if not positions:
        log.debug(
            "BT_ANALYSIS_ADXDMI_CLARITY: нет позиций для анализа id=%s, scenario_id=%s, signal_id=%s",
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

    rows: List[Dict[str, Any]] = []
    positions_total = 0
    positions_used = 0
    positions_skipped = 0

    for p in positions:
        positions_total += 1

        position_uid = p["position_uid"]
        direction = p["direction"]
        pnl_abs = p["pnl_abs"]
        raw_stat = p["raw_stat"]

        # извлекаем значение clarity из raw_stat по TF и базовому имени
        clarity = _extract_clarity_from_raw_stat(raw_stat, tf, base_param_name)
        if clarity is None:
            positions_skipped += 1
            continue

        # клипуем clarity в [0, 1]
        if clarity < Decimal("0"):
            clarity = Decimal("0")
        if clarity > Decimal("1"):
            clarity = Decimal("1")

        # квантизация до разумной точности (4 знака после запятой)
        clarity = _q_decimal(clarity)

        bin_name = _assign_bin(bins, clarity)
        if bin_name is None:
            positions_skipped += 1
            continue

        rows.append(
            {
                "position_uid": position_uid,
                "timeframe": tf,
                "direction": direction,
                "bin_name": bin_name,
                "value": clarity,   # Decimal -> numeric без лишних хвостов
                "pnl_abs": pnl_abs, # уже Decimal
            }
        )
        positions_used += 1

    log.info(
        "BT_ANALYSIS_ADXDMI_CLARITY: анализатор id=%s (family=%s, key=%s, name=%s), "
        "scenario_id=%s, signal_id=%s — позиций всего=%s, использовано=%s, пропущено=%s, строк_в_результате=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
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
                raw_stat
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
            }
        )

    log.debug(
        "BT_ANALYSIS_ADXDMI_CLARITY: загружено позиций для анализа scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(positions),
    )
    return positions


# 🔸 Извлечение clarity из raw_stat по TF и базовому имени (например, 'adx_dmi14')
def _extract_clarity_from_raw_stat(
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

    # сумма модулей +DI и -DI
    denom = plus_dec.copy_abs() + minus_dec.copy_abs()

    # если сумма слишком мала — считаем, что направление не определено (позицию пропускаем)
    if denom <= EPSILON:
        return None

    gap = plus_dec - minus_dec
    clarity = gap.copy_abs() / denom

    return clarity


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
            "BT_ANALYSIS_ADXDMI_CLARITY: не удалось распарсить JSON в параметре 'bins', используется дефолтная схема"
        )
        return []

    bins: List[Dict[str, Decimal]] = []
    for item in data:
        # ожидаемый формат элемента: {"name": "0.0-0.1", "min": 0.0, "max": 0.1}
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

    # если после парсинга бины пустые — вернём пустой список, выше подставится дефолт
    return bins


# 🔸 Дефолтные бины clarity: [0.0,0.1), [0.1,0.2), ..., [0.9,1.0]
def _default_clarity_bins() -> List[Dict[str, Decimal]]:
    bins: List[Dict[str, Decimal]] = []
    step = Decimal("0.1")

    # первые 9 бинов [0.0,0.1), [0.1,0.2), ..., [0.8,0.9)
    for i in range(9):
        lo = step * Decimal(i)
        hi = step * Decimal(i + 1)
        name = f"{lo:.1f}-{hi:.1f}"
        bins.append(
            {
                "name": name,
                "min": lo,
                "max": hi,
            }
        )

    # последний бин [0.9,1.0]
    bins.append(
        {
            "name": "0.9-1.0",
            "min": Decimal("0.9"),
            "max": Decimal("1.0"),
        }
    )

    return bins


# 🔸 Определение имени бина для значения clarity
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