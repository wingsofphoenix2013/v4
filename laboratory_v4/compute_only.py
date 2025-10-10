# compute_only.py — чистый расчёт индикаторов (без записи в Redis/БД): snapshot-значения в формате v4

# 🔸 Импорты
import logging
import asyncio
import math
from typing import Dict

# 🔸 Индикаторы (локальные копии из laboratory_v4/indicators)
from indicators import ema, atr, lr, mfi, rsi, adx_dmi, macd, bb, kama

# 🔸 Логгер
log = logging.getLogger("SNAPSHOT")

# 🔸 Сопоставление имён индикаторов с функциями
INDICATOR_DISPATCH: Dict[str, callable] = {
    "ema": ema.compute,
    "atr": atr.compute,
    "lr": lr.compute,
    "mfi": mfi.compute,
    "rsi": rsi.compute,
    "adx_dmi": adx_dmi.compute,
    "macd": macd.compute,
    "bb": bb.compute,
    "kama": kama.compute,
}

# 🔸 Вспомогательное: проверка на конечное число
def _is_finite_number(x) -> bool:
    try:
        return x is not None and isinstance(x, (int, float)) and math.isfinite(float(x))
    except Exception:
        return False

# 🔸 Чистый расчёт (как в v4): округление, канонизация имён, строковый вывод по precision
def compute_snapshot_values(instance: dict, symbol: str, df, precision: int) -> Dict[str, str]:
    """
    instance = {
        "indicator": <str>,
        "params":    <dict>,   # строки/числа как в indicator_parameters_v4
        "timeframe": <str>,
        ...
    }
    Возвращает {param_name: str(value)} c форматированием как в v4:
      - ключи *_angle округляются до 5 знаков и выводятся с '.5f';
      - остальные до <precision> знаков и выводятся с f".{precision}f";
      - базовое имя: macd{fast} | {indicator}{length} | {indicator};
      - 'value' → base; иные → base_param (если не начинаются с base_).
    """
    indicator = instance.get("indicator")
    params = instance.get("params") or {}

    # получить функцию расчёта
    compute_fn = INDICATOR_DISPATCH.get(str(indicator))
    if compute_fn is None:
        log.warning(f"⛔ Неизвестный индикатор: {indicator}")
        return {}

    # страхуем precision (0..12 разумный диапазон)
    try:
        precision = int(precision)
    except Exception:
        precision = 8
    precision = max(0, min(precision, 12))

    # выполнить расчёт
    try:
        raw = compute_fn(df, params)
    except Exception as e:
        log.error(f"Ошибка расчёта {indicator}: {e}")
        return {}

    if not isinstance(raw, dict) or not raw:
        return {}

    # округление + фильтрация (аналогично v4)
    rounded: Dict[str, float] = {}
    for k, v in raw.items():
        if not _is_finite_number(v):
            continue
        try:
            fv = float(v)
            if "angle" in str(k):
                rounded[k] = round(fv, 5)
            else:
                rounded[k] = round(fv, precision)
        except Exception as e:
            log.warning(f"[{indicator}] {symbol}: ошибка округления {k}={v} → {e}")

    if not rounded:
        return {}

    # построение base (как в v4)
    try:
        if indicator == "macd":
            base = f"{indicator}{params['fast']}"
        elif "length" in params:
            base = f"{indicator}{params['length']}"
        else:
            base = str(indicator)
    except Exception:
        # на случай кривых params — fallback
        base = str(indicator)

    # приведение имён параметров и форматирование в строки
    out: Dict[str, str] = {}
    for param, value in rounded.items():
        # каноническое имя
        if str(param).startswith(f"{base}_") or str(param) == base:
            param_name = str(param)
        else:
            param_name = base if str(param) == "value" else f"{base}_{param}"

        # формат значения
        if "angle" in param_name:
            out[param_name] = f"{float(value):.5f}"
        else:
            out[param_name] = f"{float(value):.{precision}f}"

    return out

# 🔸 Асинхронная обёртка: расчёт в пуле потоков (не блокирует event loop)
async def compute_snapshot_values_async(instance: dict, symbol: str, df, precision: int) -> Dict[str, str]:
    return await asyncio.to_thread(compute_snapshot_values, instance, symbol, df, precision)