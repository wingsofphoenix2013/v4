# 🔸 indicators/compute_and_store.py

import logging
from indicators import ema  # пока только ema

# 🔸 Сопоставление имён индикаторов с функциями
INDICATOR_DISPATCH = {
    "ema": ema.compute,
}

# 🔸 Расчёт и обработка результата одного расчётного экземпляра
async def compute_and_store(instance_id, instance, symbol, df, ts, pg, redis, precision):
    log = logging.getLogger("CALC")

    indicator = instance["indicator"]
    timeframe = instance["timeframe"]
    params = instance["params"]
    stream = instance["stream_publish"]

    compute_fn = INDICATOR_DISPATCH.get(indicator)
    if compute_fn is None:
        log.warning(f"⛔ Неизвестный индикатор: {indicator}")
        return

    try:
        result = compute_fn(df, params)  # {'value': float, ...}
        result = {k: round(v, precision) for k, v in result.items()}
    except Exception as e:
        log.error(f"Ошибка расчёта {indicator} id={instance_id}: {e}")
        return

    log.info(f"✅ {indicator.upper()} id={instance_id} {symbol}/{timeframe} → {result}")

    # 🔸 Построение базового имени (label)
    if "length" in params:
        base = f"{indicator}{params['length']}"
    else:
        base = indicator

    # 🔸 Сохранение результатов в Redis
    for param, value in result.items():
        param_name = f"{base}_{param}" if param != "value" else base
        redis_key = f"ind:{symbol}:{timeframe}:{param_name}"
        await redis.set(redis_key, str(value))

    # 🔸 В будущем: сохранение в PG и публикация в Stream