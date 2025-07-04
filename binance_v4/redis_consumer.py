# redis_consumer.py

import asyncio
import logging
import json

from infra import infra
from strategy_registry import is_strategy_binance_enabled

log = logging.getLogger("REDIS_CONSUMER")

STREAMS = {
    "binance_open_stream": "binance_open_group",
    "binance_update_stream": "binance_update_group"
}

CONSUMER_NAME = "binance_worker_1"


# 🔹 Инициализация всех групп
async def ensure_consumer_groups():
    for stream, group in STREAMS.items():
        try:
            await infra.redis_client.xgroup_create(
                name=stream,
                groupname=group,
                id="$",
                mkstream=True
            )
            log.info(f"📦 Создана группа {group} на потоке {stream}")
        except Exception as e:
            if "BUSYGROUP" in str(e):
                log.info(f"ℹ️ Группа {group} уже существует")
            else:
                log.exception(f"❌ Ошибка при создании группы для {stream}")


# 🔹 Основной цикл
async def run_redis_consumer():
    await ensure_consumer_groups()

    while True:
        try:
            entries = await infra.redis_client.xreadgroup(
                groupname=list(STREAMS.values())[0],
                consumername=CONSUMER_NAME,
                streams={name: ">" for name in STREAMS.keys()},
                count=10,
                block=1000
            )

            for stream_name, records in entries:
                for record_id, data in records:
                    payload = data.get("data")
                    group = STREAMS[stream_name]

                    if not payload:
                        log.warning(f"⚠️ Нет поля 'data' в сообщении из {stream_name}")
                        await infra.redis_client.xack(stream_name, group, record_id)
                        continue

                    try:
                        event = json.loads(payload)
                        strategy_id = event.get("strategy_id")
                    except Exception:
                        log.warning(f"⚠️ Невозможно распарсить JSON из {stream_name}: {payload}")
                        await infra.redis_client.xack(stream_name, group, record_id)
                        continue

                    if not strategy_id:
                        log.warning(f"⚠️ Нет strategy_id в сообщении: {event}")
                    elif not is_strategy_binance_enabled(strategy_id):
                        log.info(f"⏭️ [{stream_name}] Пропущено: стратегия {strategy_id} не включена для Binance")
                    else:
                        log.info(f"✅ [{stream_name}] Принято сообщение для стратегии {strategy_id}: {event}")

                    await infra.redis_client.xack(stream_name, group, record_id)

        except Exception:
            log.exception("❌ Ошибка в run_redis_consumer")
            await asyncio.sleep(2)