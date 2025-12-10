# bt_analysis_ema_stack.py — анализатор распределения позиций по режимам EMA-веера (stack regime)

import logging
import json
from typing import Dict, Any, List, Optional
from decimal import Decimal, InvalidOperation, ROUND_DOWN

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_EMA_STACK")

# 🔸 Порог ширины веера для flat / trend режимов (в долях)
WIDTH_FLAT = Decimal("0.005")   # <0.5% считаем сжатием / флэтом
WIDTH_TREND = Decimal("0.010")  # >=1% считаем достаточной шириной тренда

# 🔸 Относительный порог для сравнения EMA (чтобы не срабатывать на микроскопический шум)
EMA_TOL_REL = Decimal("0.0001")  # 0.01% от средней цены


# 🔸 Публичная точка входа анализатора EMA stack regime
async def run_ema_stack_analysis(
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

    # базовый параметр: TF, по которому берём EMA
    tf = _get_str_param(params, "tf", default="m5")  # raw_stat["tf"][tf]["indicators"]["ema"]

    log.debug(
        "BT_ANALYSIS_EMA_STACK: старт анализа id=%s (family=%s, key=%s, name=%s) "
        "для scenario_id=%s, signal_id=%s, tf=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        tf,
    )

    # загружаем позиции данного сценария/сигнала, прошедшие постпроцессинг (есть raw_stat)
    positions = await _load_positions_for_analysis(pg, scenario_id, signal_id)
    if not positions:
        log.debug(
            "BT_ANALYSIS_EMA_STACK: нет позиций для анализа id=%s, scenario_id=%s, signal_id=%s",
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

    # счёт по режимам для логирования
    regimes_count: Dict[str, int] = {
        "strong_bull": 0,
        "weak_bull": 0,
        "flat": 0,
        "weak_bear": 0,
        "strong_bear": 0,
    }

    for p in positions:
        positions_total += 1

        position_uid = p["position_uid"]
        direction = p["direction"]
        pnl_abs = p["pnl_abs"]
        raw_stat = p["raw_stat"]

        # считаем stack_score и width_rel по EMA-вееру
        regime_info = _extract_ema_regime_from_raw_stat(raw_stat, tf)
        if regime_info is None:
            positions_skipped += 1
            continue

        regime_name = regime_info["regime"]
        stack_score = regime_info["stack_score"]

        # квантизация stack_score до 4 знаков
        stack_q = _q_decimal(stack_score)

        rows.append(
            {
                "position_uid": position_uid,
                "timeframe": tf,
                "direction": direction,
                "bin_name": regime_name,  # один из strong_bull/weak_bull/flat/weak_bear/strong_bear
                "value": stack_q,         # stack_score в диапазоне [-1, 1]
                "pnl_abs": pnl_abs,
            }
        )
        positions_used += 1
        regimes_count[regime_name] = regimes_count.get(regime_name, 0) + 1

    log.info(
        "BT_ANALYSIS_EMA_STACK: анализатор id=%s (family=%s, key=%s, name=%s), "
        "scenario_id=%s, signal_id=%s — позиций всего=%s, использовано=%s, пропущено=%s, "
        "режимы={strong_bull=%s, weak_bull=%s, flat=%s, weak_bear=%s, strong_bear=%s}",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        positions_total,
        positions_used,
        positions_skipped,
        regimes_count.get("strong_bull", 0),
        regimes_count.get("weak_bull", 0),
        regimes_count.get("flat", 0),
        regimes_count.get("weak_bear", 0),
        regimes_count.get("strong_bear", 0),
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
        "BT_ANALYSIS_EMA_STACK: загружено позиций для анализа scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(positions),
    )
    return positions


# 🔸 Расчёт режима EMA-веера (stack_score и width_rel) из raw_stat
def _extract_ema_regime_from_raw_stat(
    raw_stat: Any,
    tf: str,
) -> Optional[Dict[str, Any]]:
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

    # набор EMA, которые нас интересуют
    ema_keys = ["ema9", "ema21", "ema50", "ema100", "ema200"]

    ema_vals: Dict[str, Decimal] = {}
    for k in ema_keys:
        val = ema_family.get(k)
        if val is None:
            continue
        ema_vals[k] = _safe_decimal(val)

    # нужно хотя бы две EMA, чтобы говорить о порядке и ширине
    if len(ema_vals) < 2:
        return None

    # ширина веера
    vals_list = list(ema_vals.values())
    ema_min = min(vals_list)
    ema_max = max(vals_list)
    ema_mid = ema_vals.get("ema50") or ((ema_min + ema_max) / Decimal("2"))

    if ema_mid <= Decimal("0"):
        return None

    width_rel = (ema_max - ema_min) / ema_mid

    # расчёт stack_score по парам (short, long)
    pairs = [
        ("ema9", "ema21"),
        ("ema21", "ema50"),
        ("ema50", "ema100"),
        ("ema100", "ema200"),
    ]

    tol = ema_mid * EMA_TOL_REL
    score_sum = Decimal("0")
    pair_count = 0

    for short_key, long_key in pairs:
        if short_key not in ema_vals or long_key not in ema_vals:
            continue

        short_v = ema_vals[short_key]
        long_v = ema_vals[long_key]

        diff = short_v - long_v

        # условия оценки пары
        if diff > tol:
            score_sum += Decimal("1")   # бычий вклад
        elif diff < -tol:
            score_sum += Decimal("-1")  # медвежий вклад
        else:
            # слишком близко — считаем нейтральным вкладом
            score_sum += Decimal("0")

        pair_count += 1

    if pair_count == 0:
        return None

    stack_score = score_sum / Decimal(pair_count)

    # определяем режим по stack_score и width_rel
    regime = _classify_regime(stack_score, width_rel)

    return {
        "stack_score": stack_score,
        "width_rel": width_rel,
        "regime": regime,
    }


# 🔸 Классификация режима EMA-веера
def _classify_regime(
    stack_score: Decimal,
    width_rel: Decimal,
) -> str:
    # flat условие: либо веер сжат, либо нет явного доминирования
    if width_rel < WIDTH_FLAT or stack_score.copy_abs() < Decimal("0.25"):
        return "flat"

    # сильный бычий
    if stack_score >= Decimal("0.75") and width_rel >= WIDTH_TREND:
        return "strong_bull"

    # слабый бычий
    if stack_score >= Decimal("0.25"):
        return "weak_bull"

    # сильный медвежий
    if stack_score <= Decimal("-0.75") and width_rel >= WIDTH_TREND:
        return "strong_bear"

    # слабый медвежий
    if stack_score <= Decimal("-0.25"):
        return "weak_bear"

    # на всякий случай fallback
    return "flat"


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