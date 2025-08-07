import asyncio
import logging

import infra

log = logging.getLogger("VOTER_ANALYZER")

# 🔸 Максимальное количество позиций за один проход
BATCH_SIZE = 100

# 🔸 Получение списка необработанных закрытых позиций по стратегиям из strategy_voting_list
async def fetch_positions_to_evaluate():
    query = """
        SELECT p.id, p.log_uid, p.strategy_id, p.symbol, p.tf, p.pnl
        FROM positions_v4 p
        JOIN strategy_voting_list svl ON p.strategy_id = svl.strategy_id
        WHERE p.status = 'closed'
          AND p.voter_checked = false
          AND p.log_uid IS NOT NULL
        ORDER BY p.closed_at
        LIMIT $1
    """
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(query, BATCH_SIZE)
        log.info(f"🔸 Найдено позиций для оценки: {len(rows)}")
        return rows

# 🔸 Основной запуск анализатора
async def run_voter_analyzer():
    positions = await fetch_positions_to_evaluate()
    for pos in positions:
        log.info(
            f"🔸 Обработка позиции #{pos['id']} | strategy_id={pos['strategy_id']} | "
            f"log_uid={pos['log_uid']} | pnl={pos['pnl']}"
        )