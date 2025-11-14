# 🔸 laboratory_auditor_ema2150_spread.py — логика идеи emacross_2150_spread для ветки аудитора (спред EMA21/EMA50)

# 🔸 Импорты
import logging
from typing import Tuple, Dict, Any, Optional

import laboratory_infra as infra
from laboratory_auditor_config import BestIdeaRecord, ThresholdsRecord

# 🔸 Логгер
log = logging.getLogger("LAB_AUDITOR_EMA2150")


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
        log.exception("❌ LAB_AUDITOR_EMA2150: ошибка чтения/парсинга Redis-ключа (%s)", key)
        return None


async def _get_live_spread(symbol: str, timeframe: str) -> Optional[Dict[str, float]]:
    # spread = |EMA21 - EMA50| / ATR14
    sym = symbol.upper().strip()
    tf = timeframe.lower().strip()

    ema21_key = f"ind_live:{sym}:{tf}:ema21"
    ema50_key = f"ind_live:{sym}:{tf}:ema50"
    atr14_key = f"ind_live:{sym}:{tf}:atr14"

    ema21 = await _get_live_float(ema21_key)
    ema50 = await _get_live_float(ema50_key)
    atr14 = await _get_live_float(atr14_key)

    if ema21 is None or ema50 is None or atr14 is None:
        log.debug(
            "LAB_AUDITOR_EMA2150: нет live-данных для spread (symbol=%s, tf=%s, ema21=%s, ema50=%s, atr14=%s)",
            sym, tf, ema21, ema50, atr14
        )
        return None

    if atr14 <= 0.0:
        log.debug(
            "LAB_AUDITOR_EMA2150: atr14<=0, spread не вычислить (symbol=%s, tf=%s, atr14=%.8f)",
            sym, tf, atr14
        )
        return None

    spread = abs(ema21 - ema50) / atr14
    return {"spread": spread, "ema21": ema21, "ema50": ema50, "atr14": atr14}


def _mode_for_tf(config: Dict[str, Any], timeframe: str) -> str:
    # режим TF берём из config_json: m5_mode / m15_mode / h1_mode; default='any'
    tf = timeframe.lower().strip()
    key = f"{tf}_mode"
    mode = str(config.get(key, "any") or "any").lower().strip()
    if mode not in ("low", "mid", "high", "any"):
        mode = "any"
    return mode


def _check_spread_against_mode(spread: float, thr: ThresholdsRecord, mode: str) -> bool:
    # проверка spread против thresholds по режиму
    # по той же схеме, что и для emacross_cs:
    #   mid  → spread ∈ [q20, q80]
    #   high → spread ≥ q60
    #   low  → spread ≤ q60
    #   any  → всегда True
    if mode == "any":
        return True
    if mode == "mid":
        return (spread >= thr.q20) and (spread <= thr.q80)
    if mode == "high":
        return spread >= thr.q60
    if mode == "low":
        return spread <= thr.q60
    # неизвестный режим трактуем как any
    return True


# 🔸 Публичный интерфейс идеи emacross_2150_spread для ветки аудитора

async def evaluate_emacross_2150_spread(
    strategy_id: int,
    client_strategy_id: Optional[int],
    symbol: str,
    direction: str,
    timeframe: str,
    best: BestIdeaRecord,
    thresholds: ThresholdsRecord,
    redis_client,  # оставлено для совместимости интерфейса, используем infra.redis_client
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Оценка идеи emacross_2150_spread по одному TF:
      spread = |EMA21 - EMA50| / ATR14

    Возвращает:
      allow_tf: bool
      reason_tf: str
      tf_details: dict (пойдёт в tf_results)
    """
    sym = symbol.upper().strip()
    tf = timeframe.lower().strip()
    dir_norm = direction.lower().strip()

    # читаем live spread
    live = await _get_live_spread(sym, tf)
    if live is None:
        reason = "no_live_data_emacross_2150"
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
                "thresholds": {
                    "q20": thresholds.q20,
                    "q40": thresholds.q40,
                    "q60": thresholds.q60,
                    "q80": thresholds.q80,
                    "n_samples": thresholds.n_samples,
                },
                "allow": False,
                "reason": reason,
            }
        }
        return False, reason, tf_details

    spread = live["spread"]
    ema21 = live["ema21"]
    ema50 = live["ema50"]
    atr14 = live["atr14"]

    # проверяем, есть ли вообще выборка для thresholds
    if thresholds.n_samples <= 0:
        reason = "insufficient_threshold_samples"
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
                "metrics": {
                    "spread": spread,
                    "ema21": ema21,
                    "ema50": ema50,
                    "atr14": atr14,
                },
                "thresholds": {
                    "q20": thresholds.q20,
                    "q40": thresholds.q40,
                    "q60": thresholds.q60,
                    "q80": thresholds.q80,
                    "n_samples": thresholds.n_samples,
                },
                "allow": False,
                "reason": reason,
            }
        }
        return False, reason, tf_details

    # режим для TF из config_json
    mode = _mode_for_tf(best.config_json, tf)

    # проверка spread против порогов
    passed = _check_spread_against_mode(spread, thresholds, mode)
    if passed:
        reason = "ok"
    else:
        reason = "idea_filter_reject"

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
                "spread": spread,
                "ema21": ema21,
                "ema50": ema50,
                "atr14": atr14,
            },
            "thresholds": {
                "q20": thresholds.q20,
                "q40": thresholds.q40,
                "q60": thresholds.q60,
                "q80": thresholds.q80,
                "n_samples": thresholds.n_samples,
            },
            "allow": bool(passed),
            "reason": reason,
        }
    }

    return passed, reason, tf_details