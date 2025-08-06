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

# 🔸 Обработка запроса голосования (ШАГ 3 — получаем доверие)
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

        # 🔹 Теперь получаем confidence и winrate
        votes_raw = {}

        for tf, obj in snapshots.items():
            for obj_type in ["snapshot", "pattern"]:
                object_id = obj[f"{obj_type}_id"] if f"{obj_type}_id" in obj else obj.get(f"{obj_type}_id")
                if object_id is None:
                    continue

                conf_key = f"confidence:{strategy_id}:{direction}:{tf}:{obj_type}:{object_id}"
                raw = await redis.get(conf_key)

                if not raw:
                    log.warning(f"⛔ Нет confidence ключа: {conf_key}")
                    continue

                try:
                    conf = json.loads(raw)
                    winrate = conf.get("winrate")
                    confidence = conf.get("confidence_raw")

                    source_key = f"{obj_type}_{tf}"
                    votes_raw[source_key] = {
                        "object_id": object_id,
                        "winrate": winrate,
                        "confidence_raw": confidence
                    }

                    log.info(f"📦 {source_key}: winrate={winrate:.3f}, conf={confidence:.3f}")

                except Exception:
                    log.warning(f"❌ Ошибка парсинга JSON: {conf_key}")
                    continue

        if not votes_raw:
            log.warning(f"⚠️ log_uid={log_uid} → нет данных для голосования")
            return

        log.debug(f"📋 Итоговый набор данных для голосования: {json.dumps(votes_raw, indent=2)}")

        # (дальше — следующий шаг: голосование)

    except Exception:
        log.exception("❌ Ошибка обработки запроса голосования")