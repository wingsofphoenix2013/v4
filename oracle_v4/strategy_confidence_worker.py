# strategy_confidence_worker.py

import asyncio
import logging
import json

import infra

log = logging.getLogger("STRATEGY_CONFIDENCE_WORKER")

STREAM_NAME = "emasnapshot:ratings:commands"


# 🔸 Основной воркер
async def run_strategy_confidence_worker():
    redis = infra.redis_client

    try:
        stream_info = await redis.xinfo_stream(STREAM_NAME)
        last_id = stream_info["last-generated-id"]
    except Exception as e:
        log.warning(f"⚠️ Не удалось получить last ID из stream: {e}")
        last_id = "$"

    log.info(f"📡 Подписка на Redis Stream: {STREAM_NAME}")

    while True:
        try:
            response = await redis.xread(
                streams={STREAM_NAME: last_id},
                count=50,
                block=1000
            )
            for stream, messages in response:
                for msg_id, msg_data in messages:
                    parsed = {k: v for k, v in msg_data.items()}
                    asyncio.create_task(handle_message(parsed))
                    last_id = msg_id
        except Exception:
            log.exception("❌ Ошибка чтения из Redis Stream")
            await asyncio.sleep(1)


# 🔸 Обработка одного сообщения (только логика парсинга и выборки)
async def handle_message(msg: dict):
    table = msg.get("table")
    strategies_raw = msg.get("strategies")

    if not table or not strategies_raw:
        log.warning(f"⚠️ Неверное сообщение: {msg}")
        return

    try:
        strategy_ids = json.loads(strategies_raw)
        assert isinstance(strategy_ids, list)
    except Exception:
        log.warning(f"⚠️ Не удалось распарсить список стратегий: {strategies_raw}")
        return

    log.info(f"📩 Принято сообщение: table = {table}, strategies = {strategy_ids}")

    async with infra.pg_pool.acquire() as conn:
        for strategy_id in strategy_ids:
            rows = await conn.fetch(f"""
                SELECT strategy_id, direction, num_trades, num_wins
                FROM {table}
                WHERE strategy_id = $1
            """, strategy_id)

            log.info(f"🧮 strategy_id={strategy_id} | строк: {len(rows)}")

            for r in rows[:3]:
                log.debug(f"    • {dict(r)}")