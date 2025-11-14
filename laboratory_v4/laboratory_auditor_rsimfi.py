# 🔸 laboratory_auditor_rsimfi.py — логика идеи rsimfi_energy для ветки аудитора (RSI/MFI energy regime)

# 🔸 Импорты
import logging
from typing import Tuple, Dict, Any, Optional, List

import laboratory_infra as infra
from laboratory_auditor_config import BestIdeaRecord, ThresholdsRecord  # thresholds не используются, но оставляем для совместимости

# 🔸 Логгер
log = logging.getLogger("LAB_AUDITOR_RSIMFI")

# 🔸 Маски бинов и «квантили» (совпадают с auditor_rsimfi)
MASK_BINS: Dict[str, set] = {
    "any":  {1, 2, 3, 4, 5},
    "low":  {1, 2},        # преимущественно 0..40
    "mid":  {2, 3, 4},     # примерно 20..80
    "high": {4, 5},        # преимущественно 60..100
}
MASK_QBOUNDS: Dict[str, Tuple[Optional[int], Optional[int]]] = {
    "any":  (None, None),
    "low":  (0, 40),
    "mid":  (20, 80),
    "high": (60, 100),
}


# 🔸 Вспомогательные функции чтения live-значений

async def _get_live_float(key: str) -> Optional[float]:
    # чтение и парсинг float из Redis
    try:
        js = await infra.redis_client.get(key)
        if js is None:
            return None
        s = str(js).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        log.exception("❌ LAB_AUDITOR_RSIMFI: ошибка чтения/парсинга Redis-ключа (%s)", key)
        return None


async def _get_live_rsimfi_energy(symbol: str, timeframe: str) -> Optional[Dict[str, float]]:
    """
    Строим energy из live RSI/MFI по текущему бару:
      - берём rsi14 и mfi14 из ind_live:{symbol}:{tf}:rsi14/mfi14
      - дискретизируем 0..100 шагом 5 → bucket_low (0..95)
      - energy = max(rsi_bucket, mfi_bucket)
    """
    sym = symbol.upper().strip()
    tf = timeframe.lower().strip()

    rsi_key = f"ind_live:{sym}:{tf}:rsi14"
    mfi_key = f"ind_live:{sym}:{tf}:mfi14"

    rsi = await _get_live_float(rsi_key)
    mfi = await _get_live_float(mfi_key)

    if rsi is None and mfi is None:
        log.debug(
            "LAB_AUDITOR_RSIMFI: нет live-данных RSI/MFI (symbol=%s, tf=%s, rsi=%s, mfi=%s)",
            sym, tf, rsi, mfi
        )
        return None

    def make_bucket_from_raw(x: Optional[float]) -> Optional[int]:
        if x is None:
            return None
        try:
            v = float(x)
        except Exception:
            return None
        # дискретизация: 0..100 шагом 5
        b = int(v // 5) * 5
        b = max(0, min(b, 95))
        return b

    buckets: List[int] = []
    rsi_bucket = make_bucket_from_raw(rsi)
    mfi_bucket = make_bucket_from_raw(mfi)

    if rsi_bucket is not None:
        buckets.append(rsi_bucket)
    if mfi_bucket is not None:
        buckets.append(mfi_bucket)

    if not buckets:
        log.debug(
            "LAB_AUDITOR_RSIMFI: не удалось нормализовать RSI/MFI в bucket (symbol=%s, tf=%s, rsi=%s, mfi=%s)",
            sym, tf, rsi, mfi
        )
        return None

    energy = float(max(buckets))  # 0..95
    return {
        "energy": energy,
        "rsi": float(rsi) if rsi is not None else None,
        "mfi": float(mfi) if mfi is not None else None,
    }


def _assign_energy_bin(x: float) -> int:
    """
    Бины по energy в шкале 0..100; точно как в auditor_rsimfi:
      0..20  → B1
      20..40 → B2
      40..60 → B3
      60..80 → B4
      80..100→ B5
    """
    if x <= 20.0:
        return 1
    elif x <= 40.0:
        return 2
    elif x <= 60.0:
        return 3
    elif x <= 80.0:
        return 4
    else:
        return 5


def _mode_for_tf(config: Dict[str, Any], timeframe: str) -> str:
    # режим TF берём из config_json: m5_mode / m15_mode / h1_mode; default='any'
    tf = timeframe.lower().strip()
    key = f"{tf}_mode"
    mode = str(config.get(key, "any") or "any").lower().strip()
    if mode not in ("low", "mid", "high", "any"):
        mode = "any"
    return mode


# 🔸 Публичный интерфейс идеи rsimfi_energy для ветки аудитора

async def evaluate_rsimfi_energy(
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
    Оценка идеи rsimfi_energy по одному TF:
      energy ∈ [0..100] на основе RSI/MFI (bucket_low),
      bin_index = _assign_energy_bin(energy),
      mask: allowed_bins = MASK_BINS[mode] по config_json.

    Возвращает:
      allow_tf: bool
      reason_tf: str
      tf_details: dict (пойдёт в tf_results)
    """
    sym = symbol.upper().strip()
    tf = timeframe.lower().strip()
    dir_norm = direction.lower().strip()

    # читаем live energy
    live = await _get_live_rsimfi_energy(sym, tf)
    if live is None:
        reason = "no_live_data_rsimfi"
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

    energy = live["energy"]        # 0..95
    rsi = live["rsi"]
    mfi = live["mfi"]

    # бин по фиксированным границам
    bin_index = _assign_energy_bin(energy)

    # режим TF из config_json
    mode = _mode_for_tf(best.config_json, tf)
    allowed_bins = MASK_BINS.get(mode, MASK_BINS["any"])

    passed = (bin_index in allowed_bins)
    reason = "ok" if passed else "idea_filter_reject"

    # q_low/q_high по режиму (для логов/диагностики)
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
                "energy": energy,
                "bin_index": bin_index,
                "rsi": rsi,
                "mfi": mfi,
            },
            "thresholds": {
                "q_low": q_low,
                "q_high": q_high,
                "allowed_bins": sorted(list(allowed_bins)),
                "n_samples": None,   # для rsimfi_thresholds нет, используем фиксированную шкалу
            },
            "allow": bool(passed),
            "reason": reason,
        }
    }

    return passed, reason, tf_details