import asyncio
import logging

import infra

log = logging.getLogger("VOTER_ANALYZER")

# 🔸 Максимальное количество позиций за один проход
BATCH_SIZE = 100

# 🔸 Получение списка необработанных закрытых позиций по стратегиям из strategy_voting_list
async def fetch_positions_to_evaluate():
    query = """
        SELECT p.id, p.log_uid, p.strategy_id, p.symbol, p.pnl
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

# 🔸 Классификация одного голосования по сравнению с позицией
async def analyze_position(pos):
    log_uid = pos["log_uid"]
    strategy_id = pos["strategy_id"]
    symbol = pos["symbol"]
    pnl = pos["pnl"]
    position_result = "win" if pnl > 0 else "loss"
    position_present = True  # всегда True, так как позиция уже передана

    query = """
        SELECT model, decision
        FROM strategy_voting_log
        WHERE log_uid = $1
    """
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(query, log_uid)

    if not rows:
        # Позиция есть, но голосования не было
        classification = f"missed_{position_result}"
        log.info(
            f"🔸 {log_uid} | strategy_id={strategy_id} | model=— | decision=— | "
            f"position={position_result} | → классификация: {classification.upper()}"
        )
        return

    for row in rows:
        model = row["model"]
        decision = row["decision"]

        if decision == "open":
            if position_result == "win":
                classification = "TP"
            else:
                classification = "FP"
        elif decision == "reject":
            if position_result == "win":
                classification = "FN"
            else:
                classification = "TN"
        else:
            classification = "invalid"

        log.info(
            f"🔸 {log_uid} | strategy_id={strategy_id} | model={model} | decision={decision} | "
            f"position={position_result} | → классификация: {classification}"
        )

# 🔸 Основной запуск анализатора
async def run_voter_analyzer():
    positions = await fetch_positions_to_evaluate()
    for pos in positions:
        log.debug(
            f"🔸 Обработка позиции #{pos['id']} | strategy_id={pos['strategy_id']} | "
            f"log_uid={pos['log_uid']} | pnl={pos['pnl']}"
        )
        await analyze_position(pos)