# 🔸 indicators/compute_and_store.py

import logging
import pandas as pd
import asyncio
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
        result = {}
        for k, v in raw_result.items():
            if "angle" in k:
                result[k] = round(v, 5)
            else:
                result[k] = round(v, precision)
    except Exception as e:
        log.error(f"Ошибка расчёта {indicator} id={instance_id}: {e}")
        return

    log.debug(f"✅ {indicator.upper()} id={instance_id} {symbol}/{timeframe} → {result}")

    # 🔸 Построение базового имени (label)
    if indicator == "macd":
        base = f"{indicator}{params['fast']}"
    elif "length" in params:
        base = f"{indicator}{params['length']}"
    else:
        base = indicator
        
    tasks = []
    open_time_iso = pd.to_datetime(ts, unit="ms").isoformat()

    for param, value in result.items():
        if param.startswith(f"{base}_") or param == base:
            param_name = param
        else:
            param_name = f"{base}_{param}" if param != "value" else base

        # 🔸 Форматирование строкового значения строго по precision
        if "angle" in param:
            str_value = f"{value:.5f}"
        else:
            str_value = f"{value:.{precision}f}"

        # Redis key
        redis_key = f"ind:{symbol}:{timeframe}:{param_name}"
        log.debug(f"SET {redis_key} = {str_value}")
        tasks.append(redis.set(redis_key, str_value))

        # Redis TS
        ts_key = f"ts_ind:{symbol}:{timeframe}:{param_name}"
        log.debug(f"TS.ADD {ts_key} {ts} {str_value}")
        ts_add = redis.execute_command(
            "TS.ADD", ts_key, ts, str_value,
            "RETENTION", 604800000,
            "DUPLICATE_POLICY", "last"
        )
        if asyncio.iscoroutine(ts_add):
            tasks.append(ts_add)
        else:
            log.warning(f"TS.ADD не вернул coroutine для {ts_key}")

        # Redis Stream (core)
        log.debug(f"XADD indicator_stream_core: {param_name}={str_value}")
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
        log.debug(f"XADD indicator_stream: {base} ready for {symbol}/{timeframe}")
        tasks.append(redis.xadd("indicator_stream", {
            "symbol": symbol,
            "indicator": base,
            "timeframe": timeframe,
            "open_time": open_time_iso,
            "status": "ready"
        }))

    await asyncio.gather(*tasks, return_exceptions=True)