# 🔸 laboratory_auditor_macd.py — логика идеи macd_hist для ветки аудитора (MACD Histogram Regime)

# 🔸 Импорты
import logging
import json
from typing import Tuple, Dict, Any, Optional

import laboratory_infra as infra
from laboratory_auditor_config import BestIdeaRecord, ThresholdsRecord  # thresholds не используются, но оставляем для совместимости

# 🔸 Логгер
log = logging.getLogger("LAB_AUDITOR_MACD")

# 🔸 Маски фаз для режимов (бин 1..5 → фаза) — как в auditor_macd
# 1: flat_zero, 2: early_impulse, 3: strong_impulse, 4: late_impulse, 5: counter/noisy
MASK_BINS: Dict[str, set] = {
    "any":  {1, 2, 3, 4, 5},
    "low":  {1, 2},          # flat / early
    "mid":  {2, 3, 4},       # early / strong / late
    "high": {4, 5},          # late / adverse
}
MASK_QBOUNDS: Dict[str, Tuple[Optional[int], Optional[int]]] = {
    # численные границы условные, используются только как метаданные
    "any":  (None, None),
    "low":  (0, 40),
    "mid":  (20, 80),
    "high": (60, 100),
}

PHASE_LABELS: Dict[int, str] = {
    1: "flat_zero",
    2: "early_impulse",
    3: "strong_impulse",
    4: "late_impulse",
    5: "counter_noisy",
}


# 🔸 Вспомогательные функции чтения live-значений

async def _get_live_pack_macd(symbol: str, timeframe: str, base: str = "macd12") -> Optional[Dict[str, Any]]:
    """
    Читаем live PACK MACD:
      ключ: pack_live:macd:{symbol}:{tf}:{base}

    Ожидаемый формат (по доке PACK):
      {
        "base": "macd12",
        "pack": {
          "open_time": "...",
          "mode": "bull|bear",
          "cross": "...",
          "zero_side": "above_zero|near_zero|below_zero",
          "hist_bucket_low_pct": "0.10",
          "hist_trend_strict": "rising|falling|stable",
          "hist_trend_smooth": "rising|falling|stable",
          ...
        }
      }
    """
    sym = symbol.upper().strip()
    tf = timeframe.lower().strip()
    key = f"pack_live:macd:{sym}:{tf}:{base}"
    try:
        js = await infra.redis_client.get(key)
        if not js:
            log.debug("LAB_AUDITOR_MACD: live MACD pack не найден (key=%s)", key)
            return None
        obj = json.loads(js)
        if not isinstance(obj, dict):
            return None
        return obj
    except Exception:
        log.exception("❌ LAB_AUDITOR_MACD: ошибка чтения/парсинга PACK (key=%s)", key)
        return None


def _get_field_from_pack(obj: Dict[str, Any], field: str) -> Any:
    """
    Универсальный доставатель поля из PACK:
      - сначала obj[field]
      - затем obj['pack'][field]
      - затем obj['features'][field]
    """
    if not isinstance(obj, dict):
        return None
    if field in obj:
        return obj.get(field)
    pack = obj.get("pack")
    if isinstance(pack, dict) and field in pack:
        return pack.get(field)
    features = obj.get("features")
    if isinstance(features, dict) and field in features:
        return features.get(field)
    return None


def _to_float_or_none(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _compute_phase_bin_from_live(obj: Dict[str, Any], direction: str) -> Optional[Tuple[int, Dict[str, Any]]]:
    """
    Перенос логики _compute_phase_bin_from_snapshot из auditor_macd
    на live PACK.
    Возвращает (phase_bin, metrics_dict) или None, если данных недостаточно.
    """
    mode = str(_get_field_from_pack(obj, "mode") or "").strip().lower()
    zero_side = str(_get_field_from_pack(obj, "zero_side") or "").strip().lower()
    hist_trend = str(
        _get_field_from_pack(obj, "hist_trend_smooth")
        or _get_field_from_pack(obj, "hist_trend_strict")
        or ""
    ).strip().lower()
    hb = _to_float_or_none(_get_field_from_pack(obj, "hist_bucket_low_pct"))

    if hb is None:
        return None

    # в auditor_macd hb хранится как доля (0..1), переводим в проценты 0..100
    size_pct = float(hb) * 100.0

    # уровни размера гистограммы
    small = size_pct <= 20.0
    medium = 20.0 < size_pct <= 60.0
    large = size_pct > 60.0

    dir_norm = direction.lower().strip()
    align = False
    if mode == "bull" and dir_norm == "long":
        align = True
    if mode == "bear" and dir_norm == "short":
        align = True

    # 1. flat_zero: около нуля и слабый сигнал
    if zero_side == "near_zero" and small:
        phase_bin = 1
    # 2. early_impulse: умеренная гистограмма, растёт в сторону сделки
    elif align and medium and hist_trend == "rising":
        phase_bin = 2
    # 3. strong_impulse: большая гистограмма, растёт или стабильна в сторону сделки
    elif align and large and hist_trend in ("rising", "stable"):
        phase_bin = 3
    # 4. late_impulse: большая гистограмма, но уже падает
    elif align and large and hist_trend == "falling":
        phase_bin = 4
    # 5. всё остальное — контр-сигнал или шум
    else:
        phase_bin = 5

    metrics = {
        "mode": mode,
        "zero_side": zero_side,
        "hist_trend": hist_trend,
        "hist_bucket_low_pct": hb,
        "hist_size_pct": size_pct,
        "aligned_with_direction": bool(align),
    }
    return phase_bin, metrics


def _mode_for_tf(config: Dict[str, Any], timeframe: str) -> str:
    # режим TF берём из config_json: m5_mode / m15_mode / h1_mode; default='any'
    tf = timeframe.lower().strip()
    key = f"{tf}_mode"
    mode = str(config.get(key, "any") or "any").lower().strip()
    if mode not in ("low", "mid", "high", "any"):
        mode = "any"
    return mode


# 🔸 Публичный интерфейс идеи macd_hist для ветки аудитора

async def evaluate_macd_hist(
    strategy_id: int,
    client_strategy_id: Optional[int],
    symbol: str,
    direction: str,
    timeframe: str,
    best: BestIdeaRecord,
    thresholds: Optional[ThresholdsRecord],  # не используется для этой идеи
    redis_client,  # оставлено для совместимости интерфейса, используем infra.redis_client
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Оценка идеи macd_hist по одному TF:
      - читаем live PACK MACD,
      - вычисляем фазовый бин (1..5),
      - сверяем бин с режимом TF (low/mid/high/any) через MASK_BINS.

    Возвращает:
      allow_tf: bool
      reason_tf: str
      tf_details: dict (пойдёт в tf_results)
    """
    sym = symbol.upper().strip()
    tf = timeframe.lower().strip()
    dir_norm = direction.lower().strip()

    # читаем live MACD pack (предполагаем базу macd12)
    pack_obj = await _get_live_pack_macd(sym, tf, base="macd12")
    if pack_obj is None:
        reason = "no_live_data_macd"
        tf_details = {
            "auditor": {
                "idea_key": best.idea_key,
                "variant": best.variant_key,
                "primary_window": best.primary_window,
                "source_run_id": best.source_run_id,
                "strategy_id": strategy_id,
                "client_strategy_id": client_strategy_id,
                "symbol": sym,
                "direction": dir_norm,
                "timeframe": tf,
                "mode": None,
                "metrics": None,
                "thresholds": None,
                "allow": False,
                "reason": reason,
            }
        }
        return False, reason, tf_details

    phase_res = _compute_phase_bin_from_live(pack_obj, dir_norm)
    if phase_res is None:
        reason = "no_live_data_macd"
        tf_details = {
            "auditor": {
                "idea_key": best.idea_key,
                "variant": best.variant_key,
                "primary_window": best.primary_window,
                "source_run_id": best.source_run_id,
                "strategy_id": strategy_id,
                "client_strategy_id": client_strategy_id,
                "symbol": sym,
                "direction": dir_norm,
                "timeframe": tf,
                "mode": None,
                "metrics": None,
                "thresholds": None,
                "allow": False,
                "reason": reason,
            }
        }
        return False, reason, tf_details

    phase_bin, metrics_live = phase_res
    phase_label = PHASE_LABELS.get(phase_bin, f"B{phase_bin}")

    # режим TF из config_json
    mode = _mode_for_tf(best.config_json, tf)
    allowed_bins = MASK_BINS.get(mode, MASK_BINS["any"])

    passed = (phase_bin in allowed_bins)
    reason = "ok" if passed else "idea_filter_reject"

    q_low, q_high = MASK_QBOUNDS.get(mode, (None, None))

    tf_details = {
        "auditor": {
            "idea_key": best.idea_key,
            "variant": best.variant_key,
            "primary_window": best.primary_window,
            "source_run_id": best.source_run_id,
            "strategy_id": strategy_id,
            "client_strategy_id": client_strategy_id,
            "symbol": sym,
            "direction": dir_norm,
            "timeframe": tf,
            "mode": mode,
            "metrics": {
                "phase_bin": phase_bin,
                "phase_label": phase_label,
                "mode": metrics_live.get("mode"),
                "zero_side": metrics_live.get("zero_side"),
                "hist_trend": metrics_live.get("hist_trend"),
                "hist_bucket_low_pct": metrics_live.get("hist_bucket_low_pct"),
                "hist_size_pct": metrics_live.get("hist_size_pct"),
                "aligned_with_direction": metrics_live.get("aligned_with_direction"),
            },
            "thresholds": {
                "q_low": q_low,
                "q_high": q_high,
                "allowed_bins": sorted(list(allowed_bins)),
                "n_samples": None,  # фиксированная шкала фаз, thresholds не используются
            },
            "allow": bool(passed),
            "reason": reason,
        }
    }

    return passed, reason, tf_details