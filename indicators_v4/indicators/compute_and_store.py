# 🔸 indicators/compute_and_store.py

import logging
import asyncio
import math
from datetime import datetime

# импорт индикаторов оставляем как было
from indicators import ema, atr, lr, mfi, rsi, adx_dmi, macd, bb, kama

# 🔸 Сопоставление имён индикаторов с функциями
INDICATOR_DISPATCH = {
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

def _is_finite_number(x) -> bool:
    try:
        return x is not None and isinstance(x, (int, float)) and math.isfinite(float(x))
    except Exception:
        return False

# 🔸 Расчёт и обработка результата одного расчётного экземпляра
async def compute_and_store(instance_id, instance, symbol, df, ts, pg, redis, precision):
    log = logging.getLogger("CALC")
    log.debug(f"[TRACE] compute_and_store received precision={precision} for {symbol} (instance_id={instance_id})")

    indicator = instance["indicator"]
    timeframe = instance["timeframe"]
    params = instance["params"]
    stream = instance["stream_publish"]

    compute_fn = INDICATOR_DISPATCH.get(indicator)
    if compute_fn is None:
        log.warning(f"⛔ Неизвестный индикатор: {indicator}")
        return

    try:
        raw_result = compute_fn(df, params)
        # округление
        result = {}
        for k, v in raw_result.items():
            if not _is_finite_number(v):
                log.debug(f"[SKIP] {indicator} {symbol}/{timeframe} → {k} is non-finite ({v})")
                continue
            if "angle" in k:
                result[k] = round(float(v), 5)
            else:
                result[k] = round(float(v), precision)
    except Exception as e:
        log.error(f"Ошибка расчёта {indicator} id={instance_id}: {e}")
        return

    if not result:
        log.debug(f"[SKIP] {indicator} {symbol}/{timeframe} → пустой результат после фильтрации")
        return

    log.debug(f"✅ {indicator.upper()} id={instance_id} {symbol}/{timeframe} → {result}")

    # 🔸 Базовое имя (label)
    if indicator == "macd":
        base = f"{indicator}{params['fast']}"
    elif "length" in params:
        base = f"{indicator}{params['length']}"
    else:
        base = indicator

    tasks = []
    # UTC-naive ISO без таймзоны
    open_time_iso = datetime.utcfromtimestamp(int(ts) / 1000).isoformat()

    for param, value in result.items():
        if param.startswith(f"{base}_") or param == base:
            param_name = param
        else:
            param_name = f"{base}_{param}" if param != "value" else base

        # Форматирование значения в строку по precision
        if "angle" in param_name:
            str_value = f"{value:.5f}"
        else:
            str_value = f"{value:.{precision}f}"

        # Redis KV
        redis_key = f"ind:{symbol}:{timeframe}:{param_name}"
        tasks.append(redis.set(redis_key, str_value))

        # Redis TS
        ts_key = f"ts_ind:{symbol}:{timeframe}:{param_name}"
        ts_add = redis.execute_command(
            "TS.ADD", ts_key, int(ts), str_value,
            "RETENTION", 604800000,  # 7 дней
            "DUPLICATE_POLICY", "last"
        )
        if asyncio.iscoroutine(ts_add):
            tasks.append(ts_add)

        # Redis Stream (core)
        tasks.append(redis.xadd("indicator_stream_core", {
            "symbol": symbol,
            "interval": timeframe,
            "instance_id": str(instance_id),
            "open_time": open_time_iso,
            "param_name": param_name,
            "value": str_value,
            "precision": str(precision)
        }))

    # Redis Stream (готовность)
    if stream:
        tasks.append(redis.xadd("indicator_stream", {
            "symbol": symbol,
            "indicator": base,
            "timeframe": timeframe,
            "open_time": open_time_iso,
            "status": "ready"
        }))

    await asyncio.gather(*tasks, return_exceptions=True)

# 🔸 Генерация ожидаемых имён параметров для индикатора
def get_expected_param_names(indicator: str, params: dict) -> list[str]:
    if indicator == "macd":
        base = f"macd{params['fast']}"
        return [f"{base}_macd", f"{base}_macd_signal", f"{base}_macd_hist"]

    elif indicator == "bb":
        length = params["length"]
        std_raw = round(float(params["std"]), 2)
        std_str = str(std_raw).replace(".", "_")
        base = f"bb{length}_{std_str}"
        return [f"{base}_center", f"{base}_upper", f"{base}_lower"]

    elif indicator == "adx_dmi":
        base = f"adx_dmi{params['length']}"
        return [f"{base}_adx", f"{base}_plus_di", f"{base}_minus_di"]

    elif indicator == "lr":
        base = f"lr{params['length']}"
        return [f"{base}_angle", f"{base}_center", f"{base}_upper", f"{base}_lower"]

    elif indicator in ("rsi", "mfi", "ema", "kama", "atr"):
        return [f"{indicator}{params['length']}"]

    else:
        return [indicator]