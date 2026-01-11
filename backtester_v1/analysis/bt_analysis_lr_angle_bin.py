# bt_analysis_lr_angle_bin.py — анализатор распределения позиций по биннам угла LR (адаптивные границы q6 + запись в bt_analysis_bin_dict_adaptive)

import logging
import json
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal, InvalidOperation, ROUND_DOWN

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_LR_ANGLE_BIN")

# 🔸 Единая точность для адаптивных биннов (источник истины)
Q6 = Decimal("0.000001")


# 🔸 Публичная точка входа анализатора LR/angle_bin (адаптивные бины по углу + запись границ в bt_analysis_bin_dict_adaptive)
async def run_lr_angle_bin_analysis(
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
    tf = _get_str_param(params, "tf", default="m5")  # TF из raw_stat["tf"][tf]
    angle_param_name = _get_str_param(params, "param_name", "lr50_angle")  # например lr50_angle

    if analysis_id is None or scenario_id is None or signal_id is None or run_id is None:
        log.debug(
            "BT_ANALYSIS_LR_ANGLE_BIN: нет обязательных идентификаторов (analysis_id=%s, scenario_id=%s, signal_id=%s) — анализ пропущен",
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
        "BT_ANALYSIS_LR_ANGLE_BIN: старт анализа id=%s (family=%s, key=%s, name=%s) "
        "для scenario_id=%s, signal_id=%s, tf=%s, angle_param_name=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        tf,
        angle_param_name,
    )

    # загружаем позиции окна run (status=closed + postproc=true) — строго в границах window_from/window_to
    window_from = analysis_ctx.get("window_from")
    window_to = analysis_ctx.get("window_to")

    positions = await _load_positions_for_analysis(pg, int(scenario_id), int(signal_id), window_from, window_to)
    if not positions:
        log.debug(
            "BT_ANALYSIS_LR_ANGLE_BIN: нет позиций для анализа id=%s (family=%s, key=%s, name=%s), scenario_id=%s, signal_id=%s",
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

    # первый проход: собираем углы по всем позициям, находим min/max (в q6)
    positions_total = len(positions)
    valid_angles: List[Decimal] = []

    for p in positions:
        raw_stat = p["raw_stat"]
        angle = _extract_angle_from_raw_stat(raw_stat, tf, angle_param_name)
        if angle is None:
            p["angle"] = None
            continue

        angle_q = _q6(angle)
        p["angle"] = angle_q
        valid_angles.append(angle_q)

    if not valid_angles:
        log.debug(
            "BT_ANALYSIS_LR_ANGLE_BIN: нет валидных углов LR для анализа id=%s (family=%s, key=%s, name=%s), "
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

    min_angle = min(valid_angles)
    max_angle = max(valid_angles)

    # строим адаптивные бины по углу в диапазоне [min_angle .. max_angle] (в q6)
    bins = _build_angle_bins(
        min_angle=min_angle,
        max_angle=max_angle,
        bins_count=10,
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
            "BT_ANALYSIS_LR_ANGLE_BIN: записан bt_analysis_bin_dict_adaptive — analysis_id=%s, scenario_id=%s, signal_id=%s, tf=%s, rows=%s, min_angle=%s, max_angle=%s",
            analysis_id,
            scenario_id,
            signal_id,
            tf,
            inserted_rows,
            str(min_angle),
            str(max_angle),
        )
    except Exception as e:
        log.error(
            "BT_ANALYSIS_LR_ANGLE_BIN: ошибка записи bt_analysis_bin_dict_adaptive для analysis_id=%s, scenario_id=%s, signal_id=%s: %s",
            analysis_id,
            scenario_id,
            signal_id,
            e,
            exc_info=True,
        )

    rows: List[Dict[str, Any]] = []
    positions_used = 0
    positions_skipped = 0

    # второй проход: раскладываем позиции по биннам (с q6-значением)
    for p in positions:
        position_uid = p["position_uid"]
        direction = p["direction"]
        pnl_abs = p["pnl_abs"]
        angle = p.get("angle")

        if angle is None:
            positions_skipped += 1
            continue

        angle_q = _q6(angle)

        # клипуем угол в диапазон [min_angle, max_angle] (в q6)
        if angle_q < min_angle:
            angle_q = min_angle
        if angle_q > max_angle:
            angle_q = max_angle

        bin_name = _assign_bin(bins, angle_q)
        if bin_name is None:
            positions_skipped += 1
            continue

        rows.append(
            {
                "position_uid": position_uid,
                "timeframe": tf,
                "direction": direction,
                "bin_name": bin_name,
                "value": angle_q,   # значение угла LR (q6)
                "pnl_abs": pnl_abs,
            }
        )
        positions_used += 1

    log.debug(
        "BT_ANALYSIS_LR_ANGLE_BIN: summary id=%s (family=%s, key=%s, name=%s), scenario_id=%s, signal_id=%s — "
        "positions_total=%s, used=%s, skipped=%s, rows=%s, min_angle=%s, max_angle=%s",
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
        str(min_angle),
        str(max_angle),
    )

    return {
        "rows": rows,
        "summary": {
            "positions_total": positions_total,
            "positions_used": positions_used,
            "positions_skipped": positions_skipped,
            "min_angle": str(min_angle),
            "max_angle": str(max_angle),
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
        "BT_ANALYSIS_LR_ANGLE_BIN: загружено позиций для анализа scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(positions),
    )
    return positions


# 🔸 Извлечение значения угла LR из raw_stat по TF и имени параметра (например, 'lr50_angle')
def _extract_angle_from_raw_stat(
    raw_stat: Any,
    tf: str,
    angle_param_name: str,
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
    lr_family = indicators.get("lr") or {}
    if not isinstance(lr_family, dict):
        return None

    value = lr_family.get(angle_param_name)
    if value is None:
        return None

    return _safe_decimal(value)


# 🔸 Построение адаптивных бинов по углу LR (bin_name в формате TF_BIN_N, границы q6)
def _build_angle_bins(
    min_angle: Decimal,
    max_angle: Decimal,
    bins_count: int = 10,
    tf: str = "m5",
) -> List[Dict[str, Any]]:
    bins: List[Dict[str, Any]] = []
    tf_up = str(tf or "").strip().upper()

    min_q = _q6(min_angle)
    max_q = _q6(max_angle)

    # если диапазон вырожденный — делаем bins_count одинаковых бинов
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


# 🔸 Определение имени бина для значения угла (по q6-границам)
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

        # обычный бин: [min, max)
        # inclusive бин: [min, max]
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