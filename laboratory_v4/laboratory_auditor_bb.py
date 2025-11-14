# 🔸 laboratory_auditor_bb.py — логика идеи bb_squeeze для ветки аудитора (BB Squeeze & Expansion Regime)

# 🔸 Импорты
import logging
import json
from typing import Tuple, Dict, Any, Optional

import laboratory_infra as infra
from laboratory_auditor_config import BestIdeaRecord, ThresholdsRecord  # thresholds не используются, но оставляем для совместимости

# 🔸 Логгер
log = logging.getLogger("LAB_AUDITOR_BB")

# 🔸 Маски фаз для режимов (бин 1..5 → фаза) — как в auditor_bb
# 1: squeeze, 2: pre_breakout, 3: breakout, 4: expanded, 5: noisy
MASK_BINS: Dict[str, set] = {
    "any":  {1, 2, 3, 4, 5},
    "low":  {1, 2},          # squeeze / pre_breakout
    "mid":  {2, 3, 4},       # pre_breakout / breakout / expanded
    "high": {4, 5},          # expanded / noisy
}
MASK_QBOUNDS: Dict[str, Tuple[Optional[int], Optional[int]]] = {
    # границы условные, только для метаданных
    "any":  (None, None),
    "low":  (0, 40),
    "mid":  (20, 80),
    "high": (60, 100),
}

PHASE_LABELS: Dict[int, str] = {
    1: "squeeze",
    2: "pre_breakout",
    3: "breakout",
    4: "expanded",
    5: "noisy",
}


# 🔸 Вспомогательные функции чтения live-значений

async def _get_live_pack_bb(symbol: str, timeframe: str, base: str = "bb20_2_0") -> Optional[Dict[str, Any]]:
    """
    Читаем live PACK BB:
      ключ: pack_live:bb:{symbol}:{tf}:{base}
    Ожидаемый формат:
      {
        "base": "...",
        "pack": {
          "bucket": <0..11>,
          "bucket_delta": "up_1|down_1|...",
          "bw_trend_strict": "expanding|contracting|stable",
          "bw_trend_smooth": "expanding|contracting|stable",
          ...
        }
      }
    """
    sym = symbol.upper().strip()
    tf = timeframe.lower().strip()
    key = f"pack_live:bb:{sym}:{tf}:{base}"
    try:
        js = await infra.redis_client.get(key)
        if not js:
            log.debug("LAB_AUDITOR_BB: live BB pack не найден (key=%s)", key)
            return None
        obj = json.loads(js)
        if not isinstance(obj, dict):
            return None
        return obj
    except Exception:
        log.exception("❌ LAB_AUDITOR_BB: ошибка чтения/парсинга PACK (key=%s)", key)
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


def _compute_phase_bin_from_live(obj: Dict[str, Any]) -> Optional[int]:
    """
    Перенос логики _compute_phase_bin_from_snapshot из auditor_bb
    на live PACK.
    """
    # bucket 0..11
    bucket_raw = _get_field_from_pack(obj, "bucket")
    bucket = _to_float_or_none(bucket_raw)
    if bucket is None:
        return None
    try:
        b = int(round(float(bucket)))
    except Exception:
        return None
    b = max(0, min(b, 11))

    # bw_trend: сначала smooth, потом strict
    bw_trend = _get_field_from_pack(obj, "bw_trend_smooth")
    if not isinstance(bw_trend, str) or not bw_trend:
        bw_trend = _get_field_from_pack(obj, "bw_trend_strict")
    bw_trend = (bw_trend or "").strip().lower()

    bucket_delta = _get_field_from_pack(obj, "bucket_delta")
    bucket_delta = (bucket_delta or "").strip().lower()

    # уровни bucket: low / mid / high
    if b <= 3:
        level = "low"
    elif b <= 7:
        level = "mid"
    else:
        level = "high"

    is_up = bucket_delta.startswith("up")
    is_down = bucket_delta.startswith("down")

    # squeeze: low bucket + contracting
    if level == "low" and bw_trend == "contracting":
        return 1  # squeeze

    # pre_breakout: low bucket + stable
    if level == "low" and bw_trend == "stable":
        return 2  # pre_breakout

    # breakout: любое повышение ширины + expanding
    if is_up and bw_trend == "expanding":
        return 3  # breakout

    # expanded: high bucket + expanding
    if level == "high" and bw_trend == "expanding":
        return 4  # expanded

    # всё остальное — noisy
    return 5


def _mode_for_tf(config: Dict[str, Any], timeframe: str) -> str:
    # режим TF берём из config_json: m5_mode / m15_mode / h1_mode; default='any'
    tf = timeframe.lower().strip()
    key = f"{tf}_mode"
    mode = str(config.get(key, "any") or "any").lower().strip()
    if mode not in ("low", "mid", "high", "any"):
        mode = "any"
    return mode


# 🔸 Публичный интерфейс идеи bb_squeeze для ветки аудитора

async def evaluate_bb_squeeze(
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
    Оценка идеи bb_squeeze по одному TF:
      - читаем live PACK BB,
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

    # читаем live BB pack (предполагаем базу bb20_2_0)
    pack_obj = await _get_live_pack_bb(sym, tf, base="bb20_2_0")
    if pack_obj is None:
        reason = "no_live_data_bb"
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

    phase_bin = _compute_phase_bin_from_live(pack_obj)
    if phase_bin is None:
        reason = "no_live_data_bb"
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