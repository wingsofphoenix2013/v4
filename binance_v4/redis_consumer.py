# redis_consumer.py

import json
import logging
from infra import infra
from binance_worker import handle_open_position  # 🔸 обработка сигналов на открытие

log = logging.getLogger("REDIS_CONSUMER")

REDIS_STREAM_KEY = "binance_open_stream"
CONSUMER_GROUP = "binance_open_group"
CONSUMER_NAME = "binance_open_worker"


# 🔸 Инициализация консюмер-группы Redis
async def init_redis_stream_group():
    try:
        await infra.redis_client.xgroup_create(REDIS_STREAM_KEY, CONSUMER_GROUP, id="$", mkstream=True)
        log.info(f"✅ Redis stream group {CONSUMER_GROUP} создан")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info(f"ℹ️ Redis group {CONSUMER_GROUP} уже существует")
        else:
            raise


# 🔸 Основной цикл чтения и маршрутизации
async def run_redis_consumer():
    await init_redis_stream_group()

    while True:
        try:
            entries = await infra.redis_client.xread_group(
                group_name=CONSUMER_GROUP,
                consumer_name=CONSUMER_NAME,
                streams={REDIS_STREAM_KEY: '>'},
                count=10,
                block=5000
            )

            for stream_key, messages in entries:
                for message_id, fields in messages:
                    try:
                        payload = json.loads(fields["data"])
                        event_type = payload.get("event_type")

                        if event_type == "opened":
                            await handle_open_position(payload)

                        await infra.redis_client.xack(REDIS_STREAM_KEY, CONSUMER_GROUP, message_id)

                    except Exception:
                        log.exception(f"❌ Ошибка обработки сообщения {message_id}")
        except Exception:
            log.exception("❌ Ошибка чтения из Redis Stream")