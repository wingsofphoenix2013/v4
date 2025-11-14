# 🔸 laboratory_auditor_atrreg.py — логика идеи atr_pct_regime для ветки аудитора (ATR% волатильность)

# 🔸 Импорты
import logging
from typing import Tuple, Dict, Any, Optional

import laboratory_infra as infra
from laboratory_auditor_config import BestIdeaRecord, ThresholdsRecord

# 🔸 Логгер
log = logging.getLogger("LAB_AUDITOR_ATRREG")


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
        log.exception("❌ LAB_AUDITOR_ATRREG: ошибка чтения/парсинга Redis-ключа (%s)", key)
        return None


async def _get_live_atr_pct(symbol: str, timeframe: str) -> Optional[Dict[str, float]]:
    # собираем ATR14 и markPrice для atr_pct = ATR14 / markPrice * 100
    sym = symbol.upper().strip()
    tf = timeframe.lower().strip()

    atr14_key = f"ind_live:{sym}:{tf}:atr14"
    price_key = f"bb:price:{sym}"

    atr14 = await _get_live_float(atr14_key)
    mark_price = await _get_live_float(price_key)

    if atr14 is None or mark_price is None:
        log.debug(
            "LAB_AUDITOR_ATRREG: нет live-данных для atr_pct (symbol=%s, tf=%s, atr14=%s, mark_price=%s)",
            sym, tf, atr14, mark_price
        )
        return None

    if mark_price <= 0.0:
        log.debug(
            "LAB_AUDITOR_ATRREG: mark_price<=0, atr_pct не вычислить (symbol=%s, tf=%s, mark_price=%.8f)",
            sym, tf, mark_price
        )
        return None

    atr_pct = atr14 / mark_price * 100.0
    return {"atr_pct": atr_pct, "atr14": atr14, "mark_price": mark_price}


def _mode_for_tf(config: Dict[str, Any], timeframe: str) -> str:
    # режим TF берём из config_json: m5_mode / m15_mode / h1_mode; default='any'
    tf = timeframe.lower().strip()
    key = f"{tf}_mode"
    mode = str(config.get(key, "any") or "any").lower().strip()
    if mode not in ("low", "mid", "high", "any"):
        mode = "any"
    return mode


def _check_atr_pct_against_mode(atr_pct: float, thr: ThresholdsRecord, mode: str) -> bool:
    # проверка atr_pct против thresholds по режиму
    # по аналогии с emacross_cs:
    #   mid  → atr_pct ∈ [q20, q80]
    #   high → atr_pct ≥ q60
    #   low  → atr_pct ≤ q60
    #   any  → всегда True
    if mode == "any":
        return True
    if mode == "mid":
        return (atr_pct >= thr.q20) and (atr_pct <= thr.q80)
    if mode == "high":
        return atr_pct >= thr.q60
    if mode == "low":
        return atr_pct <= thr.q60
    # неизвестный режим трактуем как any
    return True


# 🔸 Публичный интерфейс идеи atr_pct_regime для ветки аудитора

async def evaluate_atr_pct_regime(
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
    Оценка идеи atr_pct_regime по одному TF:
      atr_pct = (ATR14 / markPrice) * 100%

    Возвращает:
      allow_tf: bool
      reason_tf: str
      tf_details: dict (пойдёт в tf_results)
    """
    sym = symbol.upper().strip()
    tf = timeframe.lower().strip()
    dir_norm = direction.lower().strip()

    # читаем live atr_pct
    live = await _get_live_atr_pct(sym, tf)
    if live is None:
        reason = "no_live_data_atrreg"
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

    atr_pct = live["atr_pct"]
    atr14 = live["atr14"]
    mark_price = live["mark_price"]

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
                    "atr_pct": atr_pct,
                    "atr14": atr14,
                    "mark_price": mark_price,
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

    # проверка atr_pct против порогов
    passed = _check_atr_pct_against_mode(atr_pct, thresholds, mode)
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
                "atr_pct": atr_pct,
                "atr14": atr14,
                "mark_price": mark_price,
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