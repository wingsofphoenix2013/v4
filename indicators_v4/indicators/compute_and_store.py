# 🔸 indicators/compute_and_store.py (заглушечная версия для отладки precision)

import logging

# 🔸 Заглушка расчёта и записи
async def compute_and_store(instance_id, instance, symbol, df, ts, pg, redis, precision):
    log = logging.getLogger("CALC")
    log.info(f"[TRACE] compute_and_store received precision={precision} for {symbol} (instance_id={instance_id})")
    return