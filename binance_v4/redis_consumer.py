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
        # Попытка удалить старую группу (если существует)
        await infra.redis_client.xgroup_destroy(REDIS_STREAM_KEY, CONSUMER_GROUP)
        log.info(f"♻️ Redis group {CONSUMER_GROUP} удалена перед пересозданием")
    except Exception as e:
        log.warning(f"⚠️ Не удалось удалить Redis группу (возможно, не существует): {e}")

    try:
        # Создание новой группы, начиная с новых сообщений
        await infra.redis_client.xgroup_create(
            name=REDIS_STREAM_KEY,
            groupname=CONSUMER_GROUP,
            id="$",
            mkstream=True
        )
        log.info(f"✅ Redis stream group {CONSUMER_GROUP} создан с id='$' (только новые сообщения)")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info(f"ℹ️ Redis group {CONSUMER_GROUP} уже существует (одновременный запуск?)")
        else:
            log.exception("❌ Ошибка при создании Redis группы")
            raise

# 🔸 Основной цикл чтения и маршрутизации
async def run_redis_consumer():
    await init_redis_stream_group()

    while True:
        try:
            entries = await infra.redis_client.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=CONSUMER_NAME,
                streams={REDIS_STREAM_KEY: '>'},
                count=10,
                block=5000
            )

            for stream_key, messages in entries:
                for message_id, fields in messages:
                    try:
                        raw_data = fields.get("data")
                        if not raw_data:
                            log.warning(f"⚠️ Пустое поле 'data' в сообщении {message_id}")
                            await infra.redis_client.xack(REDIS_STREAM_KEY, CONSUMER_GROUP, message_id)
                            continue

                        payload = json.loads(raw_data)
                        event_type = payload.get("event_type")

                        if event_type == "opened":
                            await handle_open_position(payload)

                        await infra.redis_client.xack(REDIS_STREAM_KEY, CONSUMER_GROUP, message_id)

                    except Exception:
                        log.exception(f"❌ Ошибка обработки сообщения {message_id}")

        except Exception:
            log.exception("❌ Ошибка чтения из Redis Stream")