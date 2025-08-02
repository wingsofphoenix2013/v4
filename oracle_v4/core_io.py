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
        log.debug(f"💾 Сохранён флаг {flag_type}={flag_value} для {symbol} @ {open_time}")
        
# 🔸 Сохранение фазы close vs EMA в oracle_ema_v4
async def save_ema_phase(symbol: str, interval: str, open_time: str, ema_period: int, phase: str):
    query = """
        INSERT INTO oracle_ema_v4 (symbol, interval, open_time, ema_period, phase)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT DO NOTHING
    """
    open_dt = datetime.fromisoformat(open_time.replace("Z", ""))

    async with infra.pg_pool.acquire() as conn:
        await conn.execute(query, symbol, interval, open_dt, ema_period, phase)
        log.debug(f"💾 Сохранена фаза EMA {ema_period} для {symbol} @ {interval} → {phase}")
        
# 🔸 Сохранение EMA snapshot в БД
async def save_snapshot(symbol: str, interval: str, open_time: str, ordering: str):
    query = """
        INSERT INTO oracle_ema_snapshot_v4 (symbol, interval, open_time, ordering)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT DO NOTHING
    """
    open_dt = datetime.fromisoformat(open_time.replace("Z", ""))

    async with infra.pg_pool.acquire() as conn:
        await conn.execute(query, symbol, interval, open_dt, ordering)
        log.debug(f"💾 Сохранён EMA snapshot: {symbol} | {interval} | {open_time}")
        
# 🔸 Получение ID snapshot-а из словаря oracle_emasnapshot_dict
async def get_snapshot_id(ordering: str) -> int:
    query = "SELECT id FROM oracle_emasnapshot_dict WHERE ordering = $1"
    async with infra.pg_pool.acquire() as conn:
        row = await conn.fetchrow(query, ordering)
        if row:
            return row["id"]
        else:
            raise ValueError(f"❌ ordering не найден в словаре: {ordering}")