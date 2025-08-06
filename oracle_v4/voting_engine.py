# voting_engine.py

import asyncio
import logging
import json

import infra

log = logging.getLogger("VOTING_ENGINE")

REQUEST_STREAM = "strategy_voting_request"


# 🔸 Основной воркер
async def run_voting_engine():
    redis = infra.redis_client

    try:
        stream_info = await redis.xinfo_stream(REQUEST_STREAM)
        last_id = stream_info["last-generated-id"]
    except Exception:
        last_id = "$"

    log.info(f"📡 Подписка на Redis Stream: {REQUEST_STREAM} с last_id = {last_id}")

    while True:
        try:
            response = await redis.xread(
                streams={REQUEST_STREAM: last_id},
                count=50,
                block=1000
            )
            for stream, messages in response:
                for msg_id, msg_data in messages:
                    parsed = {k: v for k, v in msg_data.items()}
                    asyncio.create_task(handle_voting_request(parsed))
                    last_id = msg_id
        except Exception:
            log.exception("❌ Ошибка чтения из Redis Stream")
            await asyncio.sleep(1)


# 🔸 Обработка запроса (ШАГ 1 — только подготовка)
async def handle_voting_request(msg: dict):
    try:
        strategy_id = int(msg["strategy_id"])
        direction = msg["direction"]
        tf = msg["tf"]
        symbol = msg["symbol"]
        log_uid = msg["log_uid"]

        log.info(f"📥 log_uid={log_uid} | strategy={strategy_id} | dir={direction} | tf={tf} | symbol={symbol}")

        redis = infra.redis_client

        # 🔹 Чтение snapshot:<symbol>:<tf>
        snapshot_key = f"snapshot:{symbol}:{tf}"
        val = await redis.get(snapshot_key)

        if not val:
            log.warning(f"⛔ Не найден ключ {snapshot_key}")
            return

        try:
            data = json.loads(val)
            snapshot_id = data["snapshot_id"]
            pattern_id = data["pattern_id"]
            log.info(f"🔍 Найдено: snapshot_id={snapshot_id}, pattern_id={pattern_id}")
        except Exception:
            log.warning(f"❌ Ошибка парсинга значения snapshot:{symbol}:{tf}")
            return

    except Exception:
        log.exception("❌ Ошибка обработки запроса голосования")