# bt_analysis_mfi_mtf.py — анализатор распределения позиций по MTF-корзинкам MFI (h1 + m15 + m5)

import logging
import json
from typing import Dict, Any, List, Optional
from decimal import Decimal, InvalidOperation

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_MFI_MTF")

# 🔸 Дефолтные параметры анализатора
DEFAULT_MIN_SHARE = Decimal("0.01")
DEFAULT_LENGTH = 14


# 🔸 Публичная точка входа анализатора MFI MTF (h1 + m15 + m5)
async def run_mfi_mtf_analysis(
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

    # параметры анализатора
    min_share = _get_decimal_param(params, "min_share", DEFAULT_MIN_SHARE)
    length = _get_int_param(params, "length", DEFAULT_LENGTH)

    log.debug(
        "BT_ANALYSIS_MFI_MTF: старт анализа id=%s (family=%s, key=%s, name=%s) "
        "для scenario_id=%s, signal_id=%s, min_share=%s, length=%s, params=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        min_share,
        length,
        params,
    )

    # загружаем позиции данного сценария/сигнала, прошедшие постпроцессинг (postproc=true)
    positions = await _load_positions_for_analysis(pg, scenario_id, signal_id)
    if not positions:
        log.debug(
            "BT_ANALYSIS_MFI_MTF: нет позиций для анализа id=%s, scenario_id=%s, signal_id=%s",
            analysis_id,
            scenario_id,
            signal_id,
        )
        summary = {
            "positions_total": 0,
            "positions_used": 0,
            "positions_skipped": 0,
        }
        return {
            "rows": [],
            "summary": summary,
        }

    positions_total = 0
    positions_skipped = 0

    # сначала собираем все бин-коды по h1/m15/m5 для каждой позиции
    # H1: bin_0..bin_4, M15: bin_0..bin_4, M5: bin_0..bin_4
    base_list: List[Dict[str, Any]] = []

    for p in positions:
        positions_total += 1

        position_uid = p["position_uid"]
        direction = p["direction"]
        pnl_abs = p["pnl_abs"]
        raw_stat = p["raw_stat"]

        mfi_h1 = _extract_mfi_value(raw_stat, "h1", length)
        mfi_m15 = _extract_mfi_value(raw_stat, "m15", length)
        mfi_m5 = _extract_mfi_value(raw_stat, "m5", length)

        # если не удалось прочитать все три значения — пропускаем позицию
        if mfi_h1 is None or mfi_m15 is None or mfi_m5 is None:
            positions_skipped += 1
            continue

        h_bin = _value_to_bin(mfi_h1)
        m15_bin = _value_to_bin(mfi_m15)
        m5_bin = _value_to_bin(mfi_m5)
        if h_bin is None or m15_bin is None or m5_bin is None:
            positions_skipped += 1
            continue

        base_list.append(
            {
                "position_uid": position_uid,
                "direction": direction,
                "pnl_abs": pnl_abs,
                "h_bin": h_bin,      # например "bin_2"
                "m15_bin": m15_bin,  # например "bin_3"
                "m5_bin": m5_bin,    # например "bin_1"
            }
        )

    positions_used = len(base_list)

    if positions_used == 0:
        log.debug(
            "BT_ANALYSIS_MFI_MTF: после фильтрации нет позиций для анализа scenario_id=%s, signal_id=%s",
            scenario_id,
            signal_id,
        )
        summary = {
            "positions_total": positions_total,
            "positions_used": 0,
            "positions_skipped": positions_skipped,
        }
        return {
            "rows": [],
            "summary": summary,
        }

    total_for_share = Decimal(positions_used)

    # группируем по H1-бинам
    by_h1: Dict[str, List[Dict[str, Any]]] = {}
    for rec in base_list:
        h_bin = rec["h_bin"]
        by_h1.setdefault(h_bin, []).append(rec)

    rows: List[Dict[str, Any]] = []

    # проходим по H1-бинам и последовательно применяем min_share (H1 → M15 → M5)
    for h_bin, group_h in by_h1.items():
        group_n_h = len(group_h)
        share_h = Decimal(group_n_h) / total_for_share

        # если доля по H1 < min_share — все позиции этого бина идут в агрегированный хвост:
        # H1_bin_X|M15_0|M5_0
        if share_h < min_share:
            for rec in group_h:
                bin_name = f"H1_{h_bin}|M15_0|M5_0"
                rows.append(
                    {
                        "position_uid": rec["position_uid"],
                        "timeframe": "mtf",
                        "direction": rec["direction"],
                        "bin_name": bin_name,
                        "value": 0,
                        "pnl_abs": rec["pnl_abs"],
                    }
                )
            continue

        # внутри этого H1-бина — фильтр по M15
        by_m15: Dict[str, List[Dict[str, Any]]] = {}
        for rec in group_h:
            m15_bin = rec["m15_bin"]
            by_m15.setdefault(m15_bin, []).append(rec)

        for m15_bin, group_m15 in by_m15.items():
            group_n_m15 = len(group_m15)
            share_m15 = Decimal(group_n_m15) / total_for_share

            # если доля по (H1, M15) < min_share — кладём в:
            # H1_bin_X|M15_bin_Y|M5_0
            if share_m15 < min_share:
                for rec in group_m15:
                    bin_name = f"H1_{h_bin}|M15_{m15_bin}|M5_0"
                    rows.append(
                        {
                            "position_uid": rec["position_uid"],
                            "timeframe": "mtf",
                            "direction": rec["direction"],
                            "bin_name": bin_name,
                            "value": 0,
                            "pnl_abs": rec["pnl_abs"],
                        }
                    )
                continue

            # иначе — полноценная MTF-матрица: H1_bin_X|M15_bin_Y|M5_bin_Z
            for rec in group_m15:
                m5_bin = rec["m5_bin"]
                bin_name = f"H1_{h_bin}|M15_{m15_bin}|M5_{m5_bin}"
                rows.append(
                    {
                        "position_uid": rec["position_uid"],
                        "timeframe": "mtf",
                        "direction": rec["direction"],
                        "bin_name": bin_name,
                        "value": 0,
                        "pnl_abs": rec["pnl_abs"],
                    }
                )

    log.info(
        "BT_ANALYSIS_MFI_MTF: анализатор id=%s (family=%s, key=%s, name=%s), "
        "scenario_id=%s, signal_id=%s, length=%s, min_share=%s — "
        "позиций всего=%s, использовано=%s, пропущено=%s, строк_в_результате=%s, H1-бинов=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        length,
        min_share,
        positions_total,
        positions_used,
        positions_skipped,
        len(rows),
        len(by_h1),
    )

    summary = {
        "positions_total": positions_total,
        "positions_used": positions_used,
        "positions_skipped": positions_skipped,
    }

    return {
        "rows": rows,
        "summary": summary,
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
        "BT_ANALYSIS_MFI_MTF: загружено позиций для анализа scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(positions),
    )
    return positions


# 🔸 Извлечение значения MFI для заданного TF и длины
def _extract_mfi_value(
    raw_stat: Any,
    tf: str,
    length: int,
) -> Optional[float]:
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

    mfi_family = indicators.get("mfi") or {}
    if not isinstance(mfi_family, dict):
        return None

    mfi_key = f"mfi{length}"
    value = mfi_family.get(mfi_key)
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# 🔸 Разбиение значения 0–100 по биннам bin_0..bin_4 (шаг 20)
def _value_to_bin(value: float) -> Optional[str]:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None

    idx = int(v // 20)
    if idx < 0:
        idx = 0
    if idx > 4:
        idx = 4

    return f"bin_{idx}"


# 🔸 Вспомогательная функция: безопасное приведение к Decimal
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


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


# 🔸 Вспомогательная функция: безопасное чтение int-параметра
def _get_int_param(params: Dict[str, Any], name: str, default: int) -> int:
    cfg = params.get(name)
    if cfg is None:
        return default

    raw = cfg.get("value")
    if raw is None:
        return default

    try:
        return int(str(raw))
    except (TypeError, ValueError):
        return default