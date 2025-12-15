# bt_analysis_mfi_bin.py — анализатор распределения позиций по биннам MFI

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

    # загружаем конфигурацию биннов
    bins = _load_bins_from_params(params)
    if not bins:
        # если по какой-то причине не удалось собрать бины — используем дефолт 0-10, 10-20, ... 90-100
        bins = _default_mfi_bins()

    log.debug(
        "BT_ANALYSIS_MFI_BIN: старт анализа id=%s (family=%s, key=%s, name=%s) "
        "для scenario_id=%s, signal_id=%s, tf=%s, mfi_param_name=%s, bins=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        tf,
        mfi_param_name,
        bins,
    )

    # загружаем позиции данного сценария/сигнала, прошедшие постпроцессинг (есть raw_stat)
    positions = await _load_positions_for_analysis(pg, scenario_id, signal_id)
    if not positions:
        log.debug(
            "BT_ANALYSIS_MFI_BIN: нет позиций для анализа id=%s, scenario_id=%s, signal_id=%s",
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
            # если по какой-то причине не нашли бин — считаем позицию пропущенной
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
        "BT_ANALYSIS_MFI_BIN: анализатор id=%s (family=%s, key=%s, name=%s), "
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
        log.warning("BT_ANALYSIS_MFI_BIN: не удалось распарсить JSON в параметре 'bins', используется дефолтная схема")
        return []

    bins: List[Dict[str, Decimal]] = []
    for item in data:
        # ожидаемый формат элемента: {"name": "0-10", "min": 0, "max": 10}
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


# 🔸 Дефолтные бины MFI: 0-10, 10-20, ..., 90-100
def _default_mfi_bins() -> List[Dict[str, Decimal]]:
    bins: List[Dict[str, Decimal]] = []
    step = Decimal("10")

    # первые 9 бинов [0,10), [10,20), ..., [80,90)
    for i in range(9):
        lo = step * Decimal(i)
        hi = step * Decimal(i + 1)
        name = f"{int(lo)}-{int(hi)}"
        bins.append(
            {
                "name": name,
                "min": lo,
                "max": hi,
            }
        )

    # последний бин [90,100]
    bins.append(
        {
            "name": "90-100",
            "min": Decimal("90"),
            "max": Decimal("100"),
        }
    )

    return bins


# 🔸 Определение имени бина для значения MFI
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