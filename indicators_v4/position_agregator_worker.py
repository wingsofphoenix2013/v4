# position_agregator_worker.py — воркер агрегации позиций (шаг 1: чтение и фильтрация закрытий)

import asyncio
import logging
import json
from datetime import datetime

log = logging.getLogger("IND_AGG")

STREAM   = "positions_update_stream"
GROUP    = "indicators_agg_group"
CONSUMER = "ind_agg_1"

READ_COUNT = 50
READ_BLOCK_MS = 2000

RETRY_MAX = 3
RETRY_BACKOFF = [0.5, 1.0, 2.0]


# 🔸 Универсальный парсер ISO-строк (поддерживает 'Z' и смещения)
def _parse_iso(dt: str) -> datetime:
    s = dt.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


# 🔸 Загрузка позиции по uid из positions_v4
async def _fetch_position(pg, position_uid: str):
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                position_uid,
                strategy_id,
                symbol,
                direction,
                status,
                created_at,
                closed_at,
                pnl,
                audited
            FROM positions_v4
            WHERE position_uid = $1
            """,
            position_uid,
        )
        return row


# 🔸 Загрузка позиции с короткими ретраями (на случай гонки записи)
async def _fetch_position_with_retry(pg, position_uid: str):
    for attempt, delay in enumerate([0.0] + RETRY_BACKOFF):
        if delay:
            await asyncio.sleep(delay)
        row = await _fetch_position(pg, position_uid)
        if row:
            return row
        log.info(f"[RETRY] position {position_uid} not found (attempt {attempt+1}/{RETRY_MAX+1})")
        if attempt >= RETRY_MAX:
            break
    return None


# 🔸 Основной воркер: пока только чтение стрима и фильтрация событий закрытия
async def run_position_aggregator_worker(pg, redis):
    try:
        await redis.xgroup_create(STREAM, GROUP, id="$", mkstream=True)
        log.info(f"Группа {GROUP} создана для {STREAM}")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info(f"Группа {GROUP} уже существует")
        else:
            log.exception("Ошибка создания consumer group")
            return

    while True:
        try:
            resp = await redis.xreadgroup(
                groupname=GROUP,
                consumername=CONSUMER,
                streams={STREAM: ">"},
                count=READ_COUNT,
                block=READ_BLOCK_MS,
            )
            if not resp:
                continue

            to_ack = []
            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)
                    try:
                        payload = data.get("data")
                        if not payload:
                            log.info("[SKIP] empty 'data' in message")
                            continue

                        try:
                            event = json.loads(payload)
                        except Exception:
                            log.warning("[SKIP] bad JSON in 'data'")
                            continue

                        if event.get("event_type") != "closed":
                            continue

                        uid = event.get("position_uid")
                        if not uid:
                            log.warning("[SKIP] closed event without position_uid")
                            continue

                        row = await _fetch_position_with_retry(pg, uid)
                        if not row:
                            log.warning(f"[SKIP] position not found in DB: uid={uid}")
                            continue

                        status = row["status"]
                        pnl = row["pnl"]
                        audited = row["audited"]
                        strategy_id = row["strategy_id"]
                        closed_at = row["closed_at"]

                        if status != "closed":
                            log.info(f"[SKIP] uid={uid} status={status} (not closed)")
                            continue
                        if pnl is None:
                            log.info(f"[SKIP] uid={uid} pnl is NULL")
                            continue
                        if audited:
                            log.info(f"[SKIP] uid={uid} already audited")
                            continue

                        log.info(f"[READY] uid={uid} strategy={strategy_id} pnl={pnl} closed_at={closed_at} → готово к агрегации RSI")

                    except Exception:
                        log.exception("Ошибка обработки сообщения positions_update_stream")

            if to_ack:
                await redis.xack(STREAM, GROUP, *to_ack)

        except Exception as e:
            log.error(f"Ошибка в цикле IND_AGG: {e}", exc_info=True)
            await asyncio.sleep(2)