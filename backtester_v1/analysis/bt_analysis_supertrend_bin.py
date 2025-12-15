# bt_analysis_supertrend_bin.py — анализатор согласованности позиций с Supertrend по трём ТФ

import logging
import json
from typing import Dict, Any, List, Optional
from decimal import Decimal, InvalidOperation

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_SUPERTREND_BIN")


# 🔸 Публичная точка входа анализатора Supertrend/bin
async def run_supertrend_bin_analysis(
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
    # param_name — имя параметра тренда супертренда, по умолчанию supertrend10_3_0_trend
    st_param_name = _get_str_param(params, "param_name", default="supertrend10_3_0_trend")

    # фиксированный порядок TF: m5, m15, h1
    tf_order = ["m5", "m15", "h1"]

    log.debug(
        "BT_ANALYSIS_SUPERTREND_BIN: старт анализа id=%s (family=%s, key=%s, name=%s) "
        "для scenario_id=%s, signal_id=%s, st_param_name=%s, tf_order=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        st_param_name,
        tf_order,
    )

    # загружаем позиции данного сценария/сигнала, прошедшие постпроцессинг (есть raw_stat)
    positions = await _load_positions_for_analysis(pg, scenario_id, signal_id)
    if not positions:
        log.debug(
            "BT_ANALYSIS_SUPERTREND_BIN: нет позиций для анализа id=%s, scenario_id=%s, signal_id=%s",
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

        # извлекаем бинарный вектор согласованности по трём ТФ
        bits = _build_supertrend_bits_vector(
            raw_stat=raw_stat,
            tf_order=tf_order,
            st_param_name=st_param_name,
            direction=direction,
        )
        if bits is None:
            # если не удалось построить вектор (нет данных, непонятное направление и т.п.) — скипаем позицию
            positions_skipped += 1
            continue

        # формируем имя бина, например "1-0-1"
        bin_name = "-".join(bits)

        # числовой код бина: "1-0-1" -> 101 (numeric)
        value_numeric = _bin_name_to_numeric(bin_name)

        rows.append(
            {
                "position_uid": position_uid,
                # timeframe: используем специальную метку, обозначающую, что бин основан на комбинации TF
                "timeframe": "mtf",
                "direction": direction,
                "bin_name": bin_name,
                "value": value_numeric,
                "pnl_abs": pnl_abs,
            }
        )
        positions_used += 1

    log.debug(
        "BT_ANALYSIS_SUPERTREND_BIN: анализатор id=%s (family=%s, key=%s, name=%s), "
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
                "direction": r["direction"],
                "pnl_abs": _safe_decimal(r["pnl_abs"]),
                "raw_stat": raw,
            }
        )

    log.debug(
        "BT_ANALYSIS_SUPERTREND_BIN: загружено позиций для анализа scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(positions),
    )
    return positions


# 🔸 Построение бинарного вектора согласованности по Supertrend для позиции
def _build_supertrend_bits_vector(
    raw_stat: Any,
    tf_order: List[str],
    st_param_name: str,
    direction: str,
) -> Optional[List[str]]:
    # если raw_stat пришёл строкой из jsonb — парсим
    if isinstance(raw_stat, str):
        try:
            raw_stat = json.loads(raw_stat)
        except Exception:
            return None

    if not isinstance(raw_stat, dict):
        return None

    tf_block = raw_stat.get("tf") or {}
    if not isinstance(tf_block, dict):
        return None

    dir_norm = str(direction or "").strip().lower()
    if dir_norm not in ("long", "short"):
        # если направление неизвестно — позицию пропускаем
        return None

    bits: List[str] = []

    for tf in tf_order:
        tf_info = tf_block.get(tf)
        if not isinstance(tf_info, dict):
            return None

        indicators = tf_info.get("indicators") or {}
        if not isinstance(indicators, dict):
            return None

        st_family = indicators.get("supertrend") or {}
        if not isinstance(st_family, dict):
            return None

        trend_raw = st_family.get(st_param_name)
        if trend_raw is None:
            return None

        trend = _safe_decimal(trend_raw)
        # нормализуем знак тренда: >0 -> +1, <0 -> -1, 0 считаем бессмысленным
        if trend > 0:
            st_sign = 1
        elif trend < 0:
            st_sign = -1
        else:
            return None

        # условия согласованности:
        # long  + supertrend +1 -> 1, иначе 0
        # short + supertrend -1 -> 1, иначе 0
        if dir_norm == "long":
            bit = "1" if st_sign == 1 else "0"
        else:  # dir_norm == "short"
            bit = "1" if st_sign == -1 else "0"

        bits.append(bit)

    if len(bits) != len(tf_order):
        return None

    return bits


# 🔸 Преобразование имени бина вида "1-0-1" в числовой код 101 (numeric)
def _bin_name_to_numeric(bin_name: str) -> Decimal:
    # убираем дефисы и интерпретируем как целое число
    digits = bin_name.replace("-", "")
    if not digits:
        return Decimal("0")
    try:
        return Decimal(digits)
    except (InvalidOperation, ValueError):
        return Decimal("0")


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