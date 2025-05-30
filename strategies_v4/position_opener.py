# position_opener.py

import logging
import json
import asyncio

from infra import infra

log = logging.getLogger("POSITION_OPENER")

# 🔸 Основная функция открытия позиции (заглушка)
async def open_position(signal: dict, strategy_obj, context: dict) -> dict:
    symbol = signal.get("symbol")
    direction = signal.get("direction")
    strategy_id = signal.get("strategy_id")

    log.info(f"📥 [OPEN_POSITION] Открытие позиции: strategy={strategy_id}, symbol={symbol}, direction={direction}")
    # Здесь позже будет: расчёт размера, регистрация позиции, публикация события

    return {"status": "opened (mock)"}

# 🔸 Слушатель потока strategy_opener_stream
async def run_position_opener_loop():
    log.info("🧭 [POSITION_OPENER] Запуск слушателя strategy_opener_stream")

    redis = infra.redis_client
    last_id = "$"

    while True:
        try:
            response = await redis.xread(
                streams={"strategy_opener_stream": last_id},
                count=10,
                block=1000
            )

            if not response:
                continue

            for stream_name, messages in response:
                for msg_id, msg_data in messages:
                    last_id = msg_id
                    try:
                        payload = json.loads(msg_data["data"])
                        await open_position(payload, None, {"redis": redis})
                    except Exception as e:
                        log.warning(f"⚠️ [POSITION_OPENER] Ошибка обработки команды: {e}")

        except Exception:
            log.exception("❌ [POSITION_OPENER] Ошибка при чтении из strategy_opener_stream")
            await asyncio.sleep(5)