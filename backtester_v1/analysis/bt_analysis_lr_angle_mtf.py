# bt_analysis_lr_angle_mtf.py — анализатор распределения позиций по комбинациям углов LR (H1/M15 зоны через bt_analysis_bins_dict + квантиль m5 в bt_analysis_bin_dict_adaptive)

import logging
import json
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal, InvalidOperation, ROUND_DOWN

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_LR_ANGLE_MTF")

# 🔸 Константы квантилей и дефолтов
ANGLE_QUANTILES = 5
DEFAULT_MIN_SHARE = Decimal("0.01")
DEFAULT_LR_PREFIX = "lr50"

# 🔸 Единая точность для углов/границ квантилей (источник истины)
Q6 = Decimal("0.000001")

# 🔸 Маппинг зон -> индекс 0..4 (SD/MD/FLAT/MU/SU)
ZONE_TO_BIN_IDX = {
    "SD": 0,
    "MD": 1,
    "FLAT": 2,
    "MU": 3,
    "SU": 4,
}


# 🔸 Публичная точка входа анализатора LR/angle MTF (h1 + m15 + квантиль m5) через словари
async def run_lr_angle_mtf_analysis(
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

    # параметры анализатора
    lr_prefix = _get_str_param(params, "lr_prefix", DEFAULT_LR_PREFIX)
    min_share = _get_decimal_param(params, "min_share", DEFAULT_MIN_SHARE)

    if analysis_id is None or scenario_id is None or signal_id is None or run_id is None:
        log.debug(
            "BT_ANALYSIS_LR_ANGLE_MTF: анализ пропущен (нет обязательных id) analysis_id=%s, scenario_id=%s, signal_id=%s",
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
        "BT_ANALYSIS_LR_ANGLE_MTF: старт анализа id=%s (family=%s, key=%s, name=%s) "
        "для scenario_id=%s, signal_id=%s, lr_prefix=%s, min_share=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        lr_prefix,
        min_share,
    )

    # загружаем компонентные бины H1/M15 из статического словаря (индексные 0..4)
    bins_h1_by_dir = await _load_bins_dict_for_analysis(pg, int(analysis_id), "h1")
    bins_m15_by_dir = await _load_bins_dict_for_analysis(pg, int(analysis_id), "m15")

    # условия достаточности словаря
    if not bins_h1_by_dir or not bins_m15_by_dir:
        log.debug(
            "BT_ANALYSIS_LR_ANGLE_MTF: анализ пропущен (нет биннов H1/M15 в bt_analysis_bins_dict) "
            "analysis_id=%s, scenario_id=%s, signal_id=%s, bins_h1=%s, bins_m15=%s",
            analysis_id,
            scenario_id,
            signal_id,
            bool(bins_h1_by_dir),
            bool(bins_m15_by_dir),
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

    # загружаем позиции окна run (status=closed + postproc=true) — строго в границах window_from/window_to
    window_from = analysis_ctx.get("window_from")
    window_to = analysis_ctx.get("window_to")

    positions = await _load_positions_for_analysis(pg, int(scenario_id), int(signal_id), window_from, window_to)
    if not positions:
        log.debug(
            "BT_ANALYSIS_LR_ANGLE_MTF: нет позиций для анализа analysis_id=%s, scenario_id=%s, signal_id=%s",
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

    # первый проход: считаем зоны по h1/m15 + угол m5, группируем по (H1_bin, M15_bin)
    positions_total = 0
    positions_skipped = 0
    positions_used_base = 0

    grouped: Dict[Tuple[int, int, str, str], List[Dict[str, Any]]] = {}
    # ключ: (h1_idx, m15_idx, h1_bin_name, m15_bin_name)

    for p in positions:
        positions_total += 1

        position_uid = p["position_uid"]
        direction = str(p["direction"] or "").strip().lower()
        pnl_abs = p["pnl_abs"]
        raw_stat = p["raw_stat"]

        # зоны LR по углу для h1 и m15
        z_h1 = _extract_lr_zone_code(raw_stat, "h1", lr_prefix)
        z_m15 = _extract_lr_zone_code(raw_stat, "m15", lr_prefix)
        # угол LR для m5 (q6)
        angle_m5 = _extract_lr_angle_q6(raw_stat, "m5", lr_prefix)

        # если чего-то нет — позицию пропускаем
        if z_h1 is None or z_m15 is None or angle_m5 is None:
            positions_skipped += 1
            continue

        h1_idx = ZONE_TO_BIN_IDX.get(z_h1)
        m15_idx = ZONE_TO_BIN_IDX.get(z_m15)
        if h1_idx is None or m15_idx is None:
            positions_skipped += 1
            continue

        # нормализуем имена H1/M15 через словарь (для данного direction)
        bins_h1 = bins_h1_by_dir.get(direction)
        bins_m15 = bins_m15_by_dir.get(direction)
        if not bins_h1 or not bins_m15:
            positions_skipped += 1
            continue

        h1_bin_name = _assign_bin(bins_h1, Decimal(h1_idx))
        m15_bin_name = _assign_bin(bins_m15, Decimal(m15_idx))
        if h1_bin_name is None or m15_bin_name is None:
            positions_skipped += 1
            continue

        key = (int(h1_idx), int(m15_idx), str(h1_bin_name), str(m15_bin_name))
        grouped.setdefault(key, []).append(
            {
                "position_uid": position_uid,
                "direction": direction,
                "pnl_abs": pnl_abs,
                "angle_m5": angle_m5,  # Decimal q6
            }
        )
        positions_used_base += 1

    # total_for_share = количество валидных позиций (не skipped)
    total_for_share = Decimal(positions_used_base) if positions_used_base > 0 else Decimal(0)

    rows: List[Dict[str, Any]] = []
    adaptive_to_store: List[Tuple[int, int, int, int, Decimal, Decimal, bool]] = []
    # формат: (h1_idx, m15_idx, q_idx, bin_order, val_from, val_to, to_inclusive)

    # source_finished_at — единый по run: bt_signal_backfill_runs.finished_at
    source_finished_at = run_finished_at if isinstance(run_finished_at, datetime) else datetime.utcnow()

    # второй проход: внутри каждой группы (H1_bin|M15_bin) делаем квантильную разбивку по углу m5
    for (h1_idx, m15_idx, h1_bin, m15_bin), plist in grouped.items():
        group_n = len(plist)
        if total_for_share <= 0:
            continue

        share = Decimal(group_n) / total_for_share

        # если группа меньше min_share или слишком маленькая по количеству — хвост: M5_0
        if share < min_share or group_n <= ANGLE_QUANTILES:
            bin_name = f"{h1_bin}|{m15_bin}|M5_0"
            for rec in plist:
                rows.append(
                    {
                        "position_uid": rec["position_uid"],
                        "timeframe": "mtf",
                        "direction": rec["direction"],
                        "bin_name": bin_name,
                        "value": rec["angle_m5"],
                        "pnl_abs": rec["pnl_abs"],
                    }
                )
            continue

        # иначе делим на квантиль по angle_m5 с учётом направления
        sortable: List[Tuple[Decimal, Dict[str, Any]]] = []
        for rec in plist:
            angle = rec.get("angle_m5")
            if angle is None:
                continue

            angle_q = _q6(angle)
            direction = str(rec.get("direction") or "").strip().lower()

            # для шорта инвертируем знак, чтобы "хорошие/плохие" углы были сопоставимыми
            if direction == "short":
                sort_key = _q6(-angle_q)
            else:
                sort_key = angle_q

            sortable.append((sort_key, rec))

        if not sortable:
            continue

        sortable.sort(key=lambda x: x[0])
        n = len(sortable)

        # собираем min/max по каждому квантилю (в шкале sort_key)
        q_minmax: Dict[int, Dict[str, Decimal]] = {}

        for idx, (sort_key, rec) in enumerate(sortable):
            # NTILE(Q): q_idx = floor(i * Q / n) + 1
            q_idx = (idx * ANGLE_QUANTILES) // n + 1
            if q_idx < 1:
                q_idx = 1
            if q_idx > ANGLE_QUANTILES:
                q_idx = ANGLE_QUANTILES

            # обновляем min/max квантиля по sort_key
            mm = q_minmax.get(q_idx)
            if mm is None:
                q_minmax[q_idx] = {"min": _q6(sort_key), "max": _q6(sort_key)}
            else:
                if sort_key < mm["min"]:
                    mm["min"] = _q6(sort_key)
                if sort_key > mm["max"]:
                    mm["max"] = _q6(sort_key)

            bin_name = f"{h1_bin}|{m15_bin}|M5_Q{q_idx}"
            rows.append(
                {
                    "position_uid": rec["position_uid"],
                    "timeframe": "mtf",
                    "direction": rec["direction"],
                    "bin_name": bin_name,
                    "value": rec["angle_m5"],
                    "pnl_abs": rec["pnl_abs"],
                }
            )

        # записываем квантили в adaptive dict (для группы H1/M15)
        for q_idx in range(1, ANGLE_QUANTILES + 1):
            mm = q_minmax.get(q_idx)
            if not mm:
                continue

            # уникальный bin_order по группе
            bin_order = int(h1_idx) * 100 + int(m15_idx) * 10 + int(q_idx)
            adaptive_to_store.append(
                (
                    int(h1_idx),
                    int(m15_idx),
                    int(q_idx),
                    int(bin_order),
                    _q6(mm["min"]),
                    _q6(mm["max"]),
                    (q_idx == ANGLE_QUANTILES),
                )
            )

    # записываем адаптивные квантили в bt_analysis_bin_dict_adaptive (timeframe='mtf')
    adaptive_inserted = 0
    try:
        adaptive_inserted = await _store_adaptive_quantiles(
            pg=pg,
            run_id=int(run_id),
            analysis_id=int(analysis_id),
            scenario_id=int(scenario_id),
            signal_id=int(signal_id),
            records=adaptive_to_store,
            source_finished_at=source_finished_at,
        )
    except Exception as e:
        log.error(
            "BT_ANALYSIS_LR_ANGLE_MTF: ошибка записи bt_analysis_bin_dict_adaptive для analysis_id=%s, scenario_id=%s, signal_id=%s: %s",
            analysis_id,
            scenario_id,
            signal_id,
            e,
            exc_info=True,
        )

    log.debug(
        "BT_ANALYSIS_LR_ANGLE_MTF: завершено analysis_id=%s (family=%s, key=%s, name=%s), scenario_id=%s, signal_id=%s — "
        "lr_prefix=%s, min_share=%s, pos_total=%s, used=%s, skipped=%s, groups=%s, rows=%s, adaptive_quantiles=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        lr_prefix,
        min_share,
        positions_total,
        positions_used_base,
        positions_skipped,
        len(grouped),
        len(rows),
        adaptive_inserted,
    )

    return {
        "rows": rows,
        "summary": {
            "positions_total": positions_total,
            "positions_used": positions_used_base,
            "positions_skipped": positions_skipped,
            "adaptive_quantiles_rows": adaptive_inserted,
            "source_finished_at": source_finished_at.isoformat(),
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


# 🔸 Запись адаптивных квантилей в bt_analysis_bin_dict_adaptive (timeframe='mtf', bin_type='quantiles')
async def _store_adaptive_quantiles(
    pg,
    run_id: int,
    analysis_id: int,
    scenario_id: int,
    signal_id: int,
    records: List[Tuple[int, int, int, int, Decimal, Decimal, bool]],
    source_finished_at: datetime,
) -> int:
    if not records:
        return 0

    to_insert: List[Tuple[Any, ...]] = []

    for h1_idx, m15_idx, q_idx, bin_order, v_from, v_to, to_inclusive in records:
        h1_bin_name = f"H1_bin_{int(h1_idx)}"
        m15_bin_name = f"M15_bin_{int(m15_idx)}"
        bin_name = f"{h1_bin_name}|{m15_bin_name}|M5_Q{int(q_idx)}"

        # пишем для обеих сторон (как и в других adaptive воркерах) — значения уже нормализованы sort_key
        for direction in ("long", "short"):
            to_insert.append(
                (
                    run_id,
                    analysis_id,
                    scenario_id,
                    signal_id,
                    direction,
                    "mtf",
                    "quantiles",
                    int(bin_order),
                    bin_name,
                    _q6(v_from),
                    _q6(v_to),
                    bool(to_inclusive),
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
                direction,
                pnl_abs,
                raw_stat
            FROM bt_scenario_positions
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND status      = 'closed'
              AND postproc    = true
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
                "direction": r["direction"],
                "pnl_abs": _safe_decimal(r["pnl_abs"]),
                "raw_stat": raw,
            }
        )

    log.debug(
        "BT_ANALYSIS_LR_ANGLE_MTF: загружено позиций для анализа scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(positions),
    )
    return positions


# 🔸 Извлечение зоны LR-угла (SD/MD/FLAT/MU/SU) для заданного TF (m15 или h1)
def _extract_lr_zone_code(
    raw_stat: Any,
    tf: str,
    lr_prefix: str,
) -> Optional[str]:
    if raw_stat is None:
        return None

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
    if not isinstance(indicators, dict):
        return None

    lr_family = indicators.get("lr") or {}
    if not isinstance(lr_family, dict):
        return None

    key = f"{lr_prefix}_angle"
    value = lr_family.get(key)
    if value is None:
        return None

    angle = _safe_decimal(value)
    return _angle_to_zone(angle)


# 🔸 Извлечение угла LR для заданного TF (m5) в q6
def _extract_lr_angle_q6(
    raw_stat: Any,
    tf: str,
    lr_prefix: str,
) -> Optional[Decimal]:
    if raw_stat is None:
        return None

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
    if not isinstance(indicators, dict):
        return None

    lr_family = indicators.get("lr") or {}
    if not isinstance(lr_family, dict):
        return None

    key = f"{lr_prefix}_angle"
    value = lr_family.get(key)
    if value is None:
        return None

    return _q6(_safe_decimal(value))


# 🔸 Преобразование угла в короткий код зоны
def _angle_to_zone(angle: Decimal) -> Optional[str]:
    try:
        val = float(angle)
    except (TypeError, InvalidOperation, ValueError):
        return None

    if val <= -0.10:
        return "SD"   # strong_down
    if -0.10 < val < -0.02:
        return "MD"   # mild_down
    if -0.02 <= val <= 0.02:
        return "FLAT"
    if 0.02 < val < 0.10:
        return "MU"   # mild_up
    if val >= 0.10:
        return "SU"   # strong_up
    return None


# 🔸 Определение имени бина для значения (по границам из bt_analysis_bins_dict)
def _assign_bin(
    bins: List[Dict[str, Any]],
    value: Decimal,
) -> Optional[str]:
    if not bins:
        return None

    last_index = len(bins) - 1
    v = _safe_decimal(value)

    for idx, b in enumerate(bins):
        name = b.get("name")
        lo = b.get("min")
        hi = b.get("max")
        to_inclusive = bool(b.get("to_inclusive"))

        if lo is None or hi is None or name is None:
            continue

        lo_d = _safe_decimal(lo)
        hi_d = _safe_decimal(hi)

        # обычный бин: [min, max)
        # inclusive бин: [min, max]
        if to_inclusive or idx == last_index:
            if lo_d <= v <= hi_d:
                return str(name)
        else:
            if lo_d <= v < hi_d:
                return str(name)

    return None


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


# 🔸 Вспомогательная функция: безопасное приведение к Decimal
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
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


# 🔸 Вспомогательная функция: безопасное чтение Decimal-параметра
def _get_decimal_param(params: Dict[str, Any], name: str, default: Decimal) -> Decimal:
    cfg = params.get(name)
    if cfg is None:
        return default

    raw = cfg.get("value")
    if raw is None:
        return default

    try:
        return Decimal(str(raw))
    except (InvalidOperation, TypeError, ValueError):
        return default