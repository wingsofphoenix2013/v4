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
    position_present = True

    query = """
        SELECT model, decision, tf
        FROM strategy_voting_log
        WHERE log_uid = $1
    """

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(query, log_uid)

        if not rows:
            # 🔸 Голосование не запускалось вовсе
            classification = f"missed_{position_result}"
            await conn.execute(
                """
                INSERT INTO strategy_voting_models (
                    log_uid, model, strategy_id, symbol, tf,
                    model_decision, position_present,
                    position_result, classification
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (log_uid, model) DO NOTHING
                """,
                log_uid, '-', strategy_id, symbol, '-',
                None, True, position_result, classification
            )
            log.info(
                f"🔸 {log_uid} | strategy_id={strategy_id} | model=— | decision=— | "
                f"position={position_result} | → классификация: {classification.upper()}"
            )

        else:
            for row in rows:
                model = row["model"]
                decision = row["decision"]
                tf = row["tf"]

                if decision == "open":
                    classification = "TP" if position_result == "win" else "FP"
                elif decision == "reject":
                    classification = "FN" if position_result == "win" else "TN"
                else:
                    classification = "invalid"

                await conn.execute(
                    """
                    INSERT INTO strategy_voting_models (
                        log_uid, model, strategy_id, symbol, tf,
                        model_decision, position_present,
                        position_result, classification
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (log_uid, model) DO NOTHING
                    """,
                    log_uid, model, strategy_id, symbol, tf,
                    decision, True, position_result, classification
                )

                log.info(
                    f"🔸 {log_uid} | strategy_id={strategy_id} | model={model} | decision={decision} | "
                    f"position={position_result} | tf={tf} | → классификация: {classification}"
                )

        # 🔸 Пометить позицию как обработанную
        await conn.execute(
            "UPDATE positions_v4 SET voter_checked = true WHERE id = $1",
            pos["id"]
        )
        log.debug(f"🔸 Позиция #{pos['id']} помечена как обработанная")
# 🔸 Основной запуск анализатора
async def run_voter_analyzer():
    positions = await fetch_positions_to_evaluate()
    for pos in positions:
        log.debug(
            f"🔸 Обработка позиции #{pos['id']} | strategy_id={pos['strategy_id']} | "
            f"log_uid={pos['log_uid']} | pnl={pos['pnl']}"
        )
        await analyze_position(pos)