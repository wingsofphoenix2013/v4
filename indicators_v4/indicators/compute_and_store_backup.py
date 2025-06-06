# 🔸 indicators/compute_and_store.py

import logging
import pandas as pd
import asyncio
from indicators import ema, atr, lr, mfi, rsi

# 🔸 Сопоставление имён индикаторов с функциями
INDICATOR_DISPATCH = {
    "ema": ema.compute,
    "atr": atr.compute,
    "lr": lr.compute,
    "mfi": mfi.compute,
    "rsi": rsi.compute,
}

# 🔸 Расчёт и обработка результата одного расчётного экземпляра
async def compute_and_store(instance_id, instance, symbol, df, ts, pg, redis, precision):
    log = logging.getLogger("CALC")
    log.info(f"[TRACE] compute_and_store received precision={precision} for {symbol} (instance_id={instance_id})")
    
    indicator = instance["indicator"]
    timeframe = instance["timeframe"]
    params = instance["params"]
    stream = instance["stream_publish"]

    compute_fn = INDICATOR_DISPATCH.get(indicator)
    if compute_fn is None:
        log.warning(f"⛔ Неизвестный индикатор: {indicator}")
        return

    try:
        raw_result = compute_fn(df, params)  # {'value': float, ...}
        result = {k: round(v, precision) for k, v in raw_result.items()}
    except Exception as e:
        log.error(f"Ошибка расчёта {indicator} id={instance_id}: {e}")
        return

    log.info(f"✅ {indicator.upper()} id={instance_id} {symbol}/{timeframe} → {result}")

    # 🔸 Построение базового имени (label)
    if "length" in params:
        base = f"{indicator}{params['length']}"
    else:
        base = indicator

    tasks = []
    open_time_iso = pd.to_datetime(ts, unit="ms").isoformat()

    for param, value in result.items():
        param_name = f"{base}_{param}" if param != "value" else base

        # Redis key для быстрого доступа
        redis_key = f"ind:{symbol}:{timeframe}:{param_name}"
        log.debug(f"SET {redis_key} = {value}")
        tasks.append(redis.set(redis_key, str(value)))

        # Redis TS для истории
        ts_key = f"ts_ind:{symbol}:{timeframe}:{param_name}"
        log.debug(f"TS.ADD {ts_key} {ts} {value}")
        ts_add = redis.execute_command(
            "TS.ADD", ts_key, ts, str(value),
            "RETENTION", 604800000,
            "DUPLICATE_POLICY", "last"
        )
        if asyncio.iscoroutine(ts_add):
            tasks.append(ts_add)
        else:
            log.warning(f"TS.ADD не вернул coroutine для {ts_key}")

        # Stream для core_io (по одному значению)
        log.debug(f"XADD indicator_stream_core: {param_name}={value}")
        tasks.append(redis.xadd("indicator_stream_core", {
            "symbol": symbol,
            "interval": timeframe,
            "instance_id": str(instance_id),
            "open_time": open_time_iso,
            "param_name": param_name,
            "value": str(value),
            "precision": str(precision)
        }))

    # Stream для сигнала "готово" (по расчёту)
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