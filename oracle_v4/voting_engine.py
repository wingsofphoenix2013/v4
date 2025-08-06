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

# 🔸 Обработка запроса голосования (ШАГ 2 — по всем ТФ)
async def handle_voting_request(msg: dict):
    try:
        strategy_id = int(msg["strategy_id"])
        direction = msg["direction"]
        tf_trigger = msg["tf"]
        symbol = msg["symbol"]
        log_uid = msg["log_uid"]

        log.info(f"📥 log_uid={log_uid} | strategy={strategy_id} | dir={direction} | tf={tf_trigger} | symbol={symbol}")

        redis = infra.redis_client
        snapshots = {}

        for tf in ["m5", "m15", "h1"]:
            key = f"snapshot:{symbol}:{tf}"
            val = await redis.get(key)

            if not val:
                log.warning(f"⛔ Не найден ключ {key}")
                continue

            try:
                data = json.loads(val)
                snapshot_id = data["snapshot_id"]
                pattern_id = data["pattern_id"]

                snapshots[tf] = {
                    "snapshot_id": snapshot_id,
                    "pattern_id": pattern_id
                }

                log.info(f"🔍 {tf}: snapshot_id={snapshot_id}, pattern_id={pattern_id}")

            except Exception:
                log.warning(f"❌ Ошибка парсинга значения {key}")
                continue

        if not snapshots:
            log.warning(f"⚠️ log_uid={log_uid} → ни одного объекта не найдено")
            return

        # пока только логика получения объектов, дальше будет голосование
        log.debug(f"📦 Готово к голосованию: {snapshots}")

    except Exception:
        log.exception("❌ Ошибка обработки запроса голосования")