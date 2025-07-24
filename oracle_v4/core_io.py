# core_io.py

import logging
from datetime import datetime
import infra

log = logging.getLogger("CORE_IO")


# 🔸 Сохранение результата во флаговую таблицу
async def save_flag(symbol: str, open_time: str, flag_type: str, flag_value: str):
    query = """
        INSERT INTO oracle_flags_v4 (symbol, open_time, flag_type, flag_value)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT DO NOTHING
    """
    open_dt = datetime.fromisoformat(open_time.replace("Z", ""))

    async with infra.pg_pool.acquire() as conn:
        await conn.execute(query, symbol, open_dt, flag_type, flag_value)
        log.info(f"💾 Сохранён флаг {flag_type}={flag_value} для {symbol} @ {open_time}")