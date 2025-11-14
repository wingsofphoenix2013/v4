# 🔸 laboratory_auditor_emacross.py — логика идеи emacross_cs для ветки аудитора (EMA9/EMA21 cross strength)

# 🔸 Импорты
import logging
from typing import Tuple, Dict, Any, Optional

import laboratory_infra as infra
from laboratory_auditor_config import BestIdeaRecord, ThresholdsRecord

# 🔸 Логгер
log = logging.getLogger("LAB_AUDITOR_EMACROSS")


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
        log.exception("❌ LAB_AUDITOR_EMACROSS: ошибка чтения/парсинга Redis-ключа (%s)", key)
        return None


async def _get_live_cs(symbol: str, timeframe: str) -> Optional[Dict[str, float]]:
    # собираем EMA9, EMA21 и ATR14 из ind_live:{symbol}:{tf}:*
    sym = symbol.upper().strip()
    tf = timeframe.lower().strip()

    ema9_key = f"ind_live:{sym}:{tf}:ema9"
    ema21_key = f"ind_live:{sym}:{tf}:ema21"
    atr14_key = f"ind_live:{sym}:{tf}:atr14"

    ema9 = await _get_live_float(ema9_key)
    ema21 = await _get_live_float(ema21_key)
    atr14 = await _get_live_float(atr14_key)

    if ema9 is None or ema21 is None or atr14 is None:
        log.debug(
            "LAB_AUDITOR_EMACROSS: нет live-данных для cs (symbol=%s, tf=%s, ema9=%s, ema21=%s, atr14=%s)",
            sym, tf, ema9, ema21, atr14
        )
        return None

    if atr14 <= 0.0:
        log.debug(
            "LAB_AUDITOR_EMACROSS: atr14<=0, cs не вычислить (symbol=%s, tf=%s, atr14=%.8f)",
            sym, tf, atr14
        )
        return None

    cs = abs(ema9 - ema21) / atr14
    return {"cs": cs, "ema9": ema9, "ema21": ema21, "atr14": atr14}


def _mode_for_tf(config: Dict[str, Any], timeframe: str) -> str:
    # режим TF берём из config_json: m5_mode / m15_mode / h1_mode; default='any'
    tf = timeframe.lower().strip()
    key = f"{tf}_mode"
    mode = str(config.get(key, "any") or "any").lower().strip()
    if mode not in ("low", "mid", "high", "any"):
        mode = "any"
    return mode


def _check_cs_against_mode(cs: float, thr: ThresholdsRecord, mode: str) -> bool:
    # проверка cs против thresholds по режиму
    # по дополнению к доку:
    #   mid  → cs ∈ [q20, q80]
    #   high → cs ≥ q60
    #   low  → cs ≤ q60
    #   any  → всегда True
    if mode == "any":
        return True
    if mode == "mid":
        return (cs >= thr.q20) and (cs <= thr.q80)
    if mode == "high":
        return cs >= thr.q60
    if mode == "low":
        return cs <= thr.q60
    # неизвестный режим трактуем как any
    return True


# 🔸 Публичный интерфейс идеи emacross_cs для ветки аудитора

async def evaluate_emacross_cs(
    strategy_id: int,
    client_strategy_id: Optional[int],
    symbol: str,
    direction: str,
    timeframe: str,
    best: BestIdeaRecord,
    thresholds: ThresholdsRecord,
    redis_client,  # не используется напрямую, но оставляем для совместимости интерфейса
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Оценка идеи emacross_cs по одному TF:
      cs = |EMA9 - EMA21| / ATR14

    Возвращает:
      allow_tf: bool
      reason_tf: str
      tf_details: dict (пойдёт в tf_results)
    """
    sym = symbol.upper().strip()
    tf = timeframe.lower().strip()
    dir_norm = direction.lower().strip()

    # читаем live cs
    live = await _get_live_cs(sym, tf)
    if live is None:
        reason = "no_live_data_emacross"
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

    cs = live["cs"]
    ema9 = live["ema9"]
    ema21 = live["ema21"]
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
                    "cs": cs,
                    "ema9": ema9,
                    "ema21": ema21,
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

    # проверка cs против порогов
    passed = _check_cs_against_mode(cs, thresholds, mode)
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
                "cs": cs,
                "ema9": ema9,
                "ema21": ema21,
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