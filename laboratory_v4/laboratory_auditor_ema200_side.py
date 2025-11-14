# 🔸 laboratory_auditor_ema200_side.py — логика идеи ema200_side для ветки аудитора (сторона/расстояние до EMA200)

# 🔸 Импорты
import logging
from typing import Tuple, Dict, Any, Optional

import laboratory_infra as infra
from laboratory_auditor_config import BestIdeaRecord, ThresholdsRecord

# 🔸 Логгер
log = logging.getLogger("LAB_AUDITOR_EMA200_SIDE")


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
        log.exception("❌ LAB_AUDITOR_EMA200_SIDE: ошибка чтения/парсинга Redis-ключа (%s)", key)
        return None


async def _get_live_ema200_side_and_dist(symbol: str, timeframe: str, direction: str) -> Optional[Dict[str, Any]]:
    # side/dist считаем по текущему бару:
    #   side:
    #     long : aligned  if price >= ema200 else opposite
    #     short: aligned  if price <= ema200 else opposite
    #     ema200 is None → side="equal"
    #   dist = abs(price - ema200) / ATR14
    sym = symbol.upper().strip()
    tf = timeframe.lower().strip()
    dir_norm = direction.lower().strip()

    atr14_key = f"ind_live:{sym}:{tf}:atr14"
    ema200_key = f"ind_live:{sym}:{tf}:ema200"
    price_key = f"bb:price:{sym}"

    atr14 = await _get_live_float(atr14_key)
    ema200 = await _get_live_float(ema200_key)
    mark_price = await _get_live_float(price_key)

    if atr14 is None or mark_price is None:
        log.debug(
            "LAB_AUDITOR_EMA200_SIDE: нет live-данных (symbol=%s, tf=%s, atr14=%s, mark_price=%s)",
            sym, tf, atr14, mark_price
        )
        return None

    if mark_price <= 0.0 or atr14 <= 0.0:
        log.debug(
            "LAB_AUDITOR_EMA200_SIDE: некорректные atr/price (symbol=%s, tf=%s, atr14=%.8f, mark_price=%.8f)",
            sym, tf, atr14 or 0.0, mark_price or 0.0
        )
        return None

    if ema200 is None:
        side = "equal"
        dist = 0.0
    else:
        if dir_norm == "long":
            side = "aligned" if mark_price >= ema200 else "opposite"
        else:
            side = "aligned" if mark_price <= ema200 else "opposite"
        dist = abs(mark_price - ema200) / atr14

    return {
        "side": side,
        "dist": dist,
        "ema200": ema200,
        "atr14": atr14,
        "mark_price": mark_price,
    }


def _side_mode_for_tf(config: Dict[str, Any], timeframe: str) -> str:
    # режим стороны TF: m5_side / m15_side / h1_side; default='any'
    tf = timeframe.lower().strip()
    key = f"{tf}_side"
    mode = str(config.get(key, "any") or "any").lower().strip()
    if mode not in ("aligned", "opposite", "equal", "any"):
        mode = "any"
    return mode


def _dist_mode_for_tf(config: Dict[str, Any], timeframe: str) -> str:
    # режим расстояния TF: m5_dist_mode / m15_dist_mode / h1_dist_mode; default='any'
    tf = timeframe.lower().strip()
    key = f"{tf}_dist_mode"
    mode = str(config.get(key, "any") or "any").lower().strip()
    if mode not in ("low", "mid", "high", "any"):
        mode = "any"
    return mode


def _check_side(side: str, mode_side: str) -> bool:
    # проверка стороны относительно режима
    if mode_side == "any":
        return True
    if mode_side == "aligned":
        return side == "aligned"
    if mode_side == "opposite":
        return side == "opposite"
    if mode_side == "equal":
        return side == "equal"
    # неизвестный режим — считаем как any
    return True


def _check_dist_against_mode(dist: float, thr: ThresholdsRecord, mode_dist: str) -> bool:
    # проверка dist против thresholds по режиму
    # по аналогии с emacross_cs:
    #   mid  → dist ∈ [q20, q80]
    #   high → dist ≥ q60
    #   low  → dist ≤ q60
    #   any  → всегда True
    if mode_dist == "any":
        return True
    if mode_dist == "mid":
        return (dist >= thr.q20) and (dist <= thr.q80)
    if mode_dist == "high":
        return dist >= thr.q60
    if mode_dist == "low":
        return dist <= thr.q60
    # неизвестный режим трактуем как any
    return True


# 🔸 Публичный интерфейс идеи ema200_side для ветки аудитора

async def evaluate_ema200_side(
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
    Оценка идеи ema200_side по одному TF:
      side: aligned/opposite/equal (по отношению к EMA200 и направлению сделки)
      dist: |price - EMA200| / ATR14

    Возвращает:
      allow_tf: bool
      reason_tf: str
      tf_details: dict (пойдёт в tf_results)
    """
    sym = symbol.upper().strip()
    tf = timeframe.lower().strip()
    dir_norm = direction.lower().strip()

    # читаем live side/dist
    live = await _get_live_ema200_side_and_dist(sym, tf, dir_norm)
    if live is None:
        reason = "no_live_data_ema200_side"
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
                "mode_side": None,
                "mode_dist": None,
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

    side = live["side"]
    dist = live["dist"]
    ema200 = live["ema200"]
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
                "mode_side": None,
                "mode_dist": None,
                "metrics": {
                    "side": side,
                    "dist": dist,
                    "ema200": ema200,
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

    # режимы для TF из config_json
    mode_side = _side_mode_for_tf(best.config_json, tf)
    mode_dist = _dist_mode_for_tf(best.config_json, tf)

    # проверка стороны
    side_ok = _check_side(side, mode_side)
    # проверка расстояния
    dist_ok = _check_dist_against_mode(dist, thresholds, mode_dist)

    passed = side_ok and dist_ok
    if not side_ok:
        reason = "ema200_side_mismatch"
    elif not dist_ok:
        reason = "idea_filter_reject"
    else:
        reason = "ok"

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
            "mode_side": mode_side,
            "mode_dist": mode_dist,
            "metrics": {
                "side": side,
                "dist": dist,
                "ema200": ema200,
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