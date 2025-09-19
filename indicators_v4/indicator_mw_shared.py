# indicator_mw_shared.py — общий слой правил для MarketWatch (гистерезис + dwell) и доступ к прошлому состоянию

import json
from datetime import datetime

# 🔸 Константы порогов (начинаем с Trend; остальные блоки добавим на следующих шагах)
TREND_ADX_SIDEWAYS_IN  = 12.0  # вход во флет: max(ADX) < IN
TREND_ADX_SIDEWAYS_OUT = 14.0  # выход из флэта: max(ADX) ≥ OUT

# 🔸 Минимальная длительность состояния (в барах) по TF
TREND_MIN_STREAK = {"m5": 2, "m15": 1, "h1": 1}

# 🔸 Имя KV ключа MarketWatch
def kv_key(kind: str, symbol: str, tf: str) -> str:
    return f"ind_mw:{symbol}:{tf}:{kind}"


# 🔸 Загрузить предыдущее состояние из KV (state, streak)
async def load_prev_state(redis, kind: str, symbol: str, tf: str) -> tuple[str | None, int]:
    """
    Читает KV ind_mw:{symbol}:{tf}:{kind}.
    Возвращает (prev_state | None, prev_streak:int).
    """
    try:
        key = kv_key(kind, symbol, tf)
        raw = await redis.get(key)
        if not raw:
            return None, 0
        data = json.loads(raw)
        prev_state = data.get("state")
        details = data.get("details") or {}
        prev_streak = int(details.get("streak") or 0)
        return prev_state, prev_streak
    except Exception:
        return None, 0


# 🔸 Пороговая конфигурация Trend для TF
def trend_thresholds(tf: str) -> dict:
    return {
        "adx_in": TREND_ADX_SIDEWAYS_IN,
        "adx_out": TREND_ADX_SIDEWAYS_OUT,
        "min_streak": TREND_MIN_STREAK.get(tf, 2),
    }


# 🔸 Применить гистерезис и dwell для Trend
def apply_trend_hysteresis_and_dwell(
    prev_state: str | None,
    raw_state: str,
    features: dict,     # ожидается {"max_adx": float}
    thresholds: dict,   # из trend_thresholds(tf)
    prev_streak: int,
) -> tuple[str, int]:
    """
    Правила:
      - Гистерезис по ADX для входа/выхода из 'sideways':
          вход во флет:  max_adx <  IN
          выход из флэт: max_adx ≥ OUT
      - Dwell-time: не менять state, пока не выдержан минимум баров (min_streak),
        кроме случая когда raw_state == prev_state (тогда просто наращиваем streak).
    """
    max_adx = features.get("max_adx")
    adx_in  = thresholds["adx_in"]
    adx_out = thresholds["adx_out"]
    min_streak = thresholds["min_streak"]

    # если нет прошлого — принимаем raw немедленно
    if prev_state is None:
        return raw_state, 1

    prev_is_sideways = (prev_state == "sideways")
    raw_is_sideways  = (raw_state == "sideways")
    candidate = raw_state

    # гистерезис: контролируем переходы в/из флэта
    if raw_is_sideways and not prev_is_sideways:
        if max_adx is not None and max_adx >= adx_in:
            candidate = prev_state  # ещё рано во флет
    elif (not raw_is_sideways) and prev_is_sideways:
        if max_adx is not None and max_adx < adx_out:
            candidate = "sideways"  # ещё рано выходить из флэта

    # если кандидат совпал с прошлым — растим streak
    if candidate == prev_state:
        return prev_state, prev_streak + 1

    # иначе хотим изменить state — проверяем dwell
    if prev_streak + 1 < min_streak:
        # удерживаем прошлый state до минимума
        return prev_state, prev_streak + 1

    # минимум выполнен — переключаемся
    return candidate, 1