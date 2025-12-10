# bt_analysis_ema_dist.py — анализатор распределения позиций по биннам отклонения цены от EMA

import logging
import json
from typing import Dict, Any, List, Optional
from decimal import Decimal, InvalidOperation, ROUND_DOWN

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_EMA_DIST")

# 🔸 Параметры диапазона отклонений (в долях, т.е. 0.01 = 1%)
MIN_DIST = Decimal("-0.05")  # -5%
MAX_DIST = Decimal("0.05")   # +5%


# 🔸 Публичная точка входа анализатора EMA-distance
async def run_ema_dist_analysis(
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
    ema_param_name = _get_str_param(params, "param_name", "ema50")  # например ema9 / ema21 / ema50 / ema100 / ema200

    # загружаем конфигурацию биннов (если не задана — используем дефолтную схему)
    bins = _load_bins_from_params(params)
    if not bins:
        bins = _default_ema_dist_bins()

    log.debug(
        "BT_ANALYSIS_EMA_DIST: старт анализа id=%s (family=%s, key=%s, name=%s) "
        "для scenario_id=%s, signal_id=%s, tf=%s, ema_param_name=%s, bins=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        tf,
        ema_param_name,
        bins,
    )

    # загружаем позиции данного сценария/сигнала, прошедшие постпроцессинг (есть raw_stat)
    positions = await _load_positions_for_analysis(pg, scenario_id, signal_id)
    if not positions:
        log.debug(
            "BT_ANALYSIS_EMA_DIST: нет позиций для анализа id=%s, scenario_id=%s, signal_id=%s",
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
        entry_price = p["entry_price"]

        # извлекаем dist = (entry_price - ema) / ema из raw_stat по TF и имени параметра
        dist = _extract_ema_dist_from_raw_stat(raw_stat, tf, ema_param_name, entry_price)
        if dist is None:
            positions_skipped += 1
            continue

        # клипуем dist в [MIN_DIST, MAX_DIST]
        if dist < MIN_DIST:
            dist = MIN_DIST
        if dist > MAX_DIST:
            dist = MAX_DIST

        # квантизация до 4 знаков после запятой
        dist_q = _q_decimal(dist)

        bin_name = _assign_bin(bins, dist_q)
        if bin_name is None:
            positions_skipped += 1
            continue

        rows.append(
            {
                "position_uid": position_uid,
                "timeframe": tf,
                "direction": direction,
                "bin_name": bin_name,
                "value": dist_q,   # Decimal в долях (0.01 = 1%)
                "pnl_abs": pnl_abs,
            }
        )
        positions_used += 1

    log.info(
        "BT_ANALYSIS_EMA_DIST: анализатор id=%s (family=%s, key=%s, name=%s), "
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
                "raw_stat": raw,
                "entry_price": _safe_decimal(r["entry_price"]),
            }
        )

    log.debug(
        "BT_ANALYSIS_EMA_DIST: загружено позиций для анализа scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(positions),
    )
    return positions


# 🔸 Извлечение dist = (entry_price - ema) / ema из raw_stat
def _extract_ema_dist_from_raw_stat(
    raw_stat: Any,
    tf: str,
    ema_param_name: str,
    entry_price: Decimal,
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
    ema_family = indicators.get("ema") or {}
    if not isinstance(ema_family, dict):
        return None

    ema_val = ema_family.get(ema_param_name)
    if ema_val is None:
        return None

    ema_dec = _safe_decimal(ema_val)
    if ema_dec <= Decimal("0"):
        return None

    # относительное отклонение: (entry - ema) / ema
    dist = (entry_price - ema_dec) / ema_dec
    return dist


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
            "BT_ANALYSIS_EMA_DIST: не удалось распарсить JSON в параметре 'bins', используется дефолтная схема"
        )
        return []

    bins: List[Dict[str, Decimal]] = []
    for item in data:
        # ожидаемый формат элемента: {"name": "lo-hi", "min": -0.05, "max": -0.03}
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


# 🔸 Дефолтные бины EMA-distance в диапазоне [-5%, +5%]
def _default_ema_dist_bins() -> List[Dict[str, Decimal]]:
    bins: List[Dict[str, Decimal]] = []

    # границы: -5%, -3%, -1%, +1%, +3%, +5%
    boundaries = [
        Decimal("-0.05"),
        Decimal("-0.03"),
        Decimal("-0.01"),
        Decimal("0.01"),
        Decimal("0.03"),
        Decimal("0.05"),
    ]

    # бины:
    # [-0.05, -0.03)
    bins.append(
        {
            "name": "-0.05--0.03",
            "min": boundaries[0],
            "max": boundaries[1],
        }
    )
    # [-0.03, -0.01)
    bins.append(
        {
            "name": "-0.03--0.01",
            "min": boundaries[1],
            "max": boundaries[2],
        }
    )
    # [-0.01, 0.01)
    bins.append(
        {
            "name": "-0.01-0.01",
            "min": boundaries[2],
            "max": boundaries[3],
        }
    )
    # [0.01, 0.03)
    bins.append(
        {
            "name": "0.01-0.03",
            "min": boundaries[3],
            "max": boundaries[4],
        }
    )
    # [0.03, 0.05] (последний бин включает верхнюю границу)
    bins.append(
        {
            "name": "0.03-0.05",
            "min": boundaries[4],
            "max": boundaries[5],
        }
    )

    return bins


# 🔸 Определение имени бина для значения dist
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