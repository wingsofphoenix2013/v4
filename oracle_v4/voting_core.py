# voting_core.py

import logging
import asyncpg
import json
import infra

log = logging.getLogger("VOTING_CORE")


# 🔸 Сохранение результата голосования в strategy_voting_log
async def save_voting_result(
    log_uid: str,
    strategy_id: int,
    direction: str,
    tf: str,
    symbol: str,
    model: str,
    total_score: float,
    decision: str,
    veto_applied: bool | None,
    votes: list[dict]
):
    try:
        async with infra.pg_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO strategy_voting_log (
                    log_uid,
                    strategy_id,
                    direction,
                    tf,
                    symbol,
                    model,
                    total_score,
                    decision,
                    veto_applied,
                    votes
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """, log_uid, strategy_id, direction, tf, symbol,
                 model, total_score, decision, veto_applied,
                 json.dumps(votes))

        log.debug(f"[DB] Голосование log_uid={log_uid} model={model} → {decision.upper()}")

    except asyncpg.UniqueViolationError:
        log.warning(f"[DB] Дубликат log_uid={log_uid}, model={model} — пропуск")

    except Exception:
        log.exception(f"❌ Ошибка записи в БД: log_uid={log_uid}, model={model}")