# signal_processor.py

import asyncio
import json
import logging

from infra import infra

log = logging.getLogger("SIGNAL_PROCESSOR")

# 🔸 Название Redis-стрима для входящих сигналов
STRATEGY_INPUT_STREAM = "strategy_input_stream"

# 🔸 Основной цикл обработки сигналов
async def run_signal_loop(strategy_registry):
    log.info("🚦 [SIGNAL_PROCESSOR] Запуск цикла обработки сигналов")

    redis = infra.redis_client
    last_id = "$"  # 🔸 начинаем с конца стрима

    while True:
        try:
            # 🔸 Чтение сигналов из Redis (без групп)
            response = await redis.xread(
                streams={STRATEGY_INPUT_STREAM: last_id},
                count=10,
                block=1000
            )

            if not response:
                continue

            for stream_name, messages in response:
                for msg_id, msg_data in messages:
                    last_id = msg_id
                    raw = msg_data.get("data")
                    if not raw:
                        continue
                    try:
                        signal = json.loads(raw)
                        log.info(f"📩 Получен сигнал: strategy={signal.get('strategy_id')}, symbol={signal.get('symbol')}, direction={signal.get('direction')}")
                    except Exception as e:
                        log.warning(f"⚠️ Ошибка парсинга сигнала: {e}")

        except Exception as e:
            log.exception("❌ Ошибка при чтении из Redis — повтор через 5 секунд")
            await asyncio.sleep(5)