# rule_processor.py

import asyncio
import logging
from datetime import datetime

from infra import infra
from rule_loader import RULE_INSTANCES

log = logging.getLogger("RULE_PROC")

# 🔸 Асинхронный воркер обработки потока готовности индикаторов
async def run_rule_processor():
    redis = infra.redis_client
    stream = "indicator_stream"
    last_id = "$"  # читаем только новые события

    log.info("[RULE_PROCESSOR] ▶️ Запуск воркера. Источник: indicator_stream")

    while True:
        try:
            response = await redis.xread(
                streams={stream: last_id},
                count=100,
                block=1000  # 1 секунда
            )
            if not response:
                continue

            for _, messages in response:
                for msg_id, data in messages:
                    last_id = msg_id
                    await handle_ready_event(data)

        except Exception:
            log.exception("[RULE_PROCESSOR] ❌ Ошибка чтения потока")
            await asyncio.sleep(1)


# 🔸 Обработка одного события готовности индикаторов
async def handle_ready_event(data: dict):
    try:
        symbol = data["symbol"]
        tf = data["timeframe"]
        open_time = datetime.fromisoformat(data["open_time"].replace("Z", "+00:00"))
    except Exception:
        log.warning(f"[RULE_PROCESSOR] ⚠️ Невалидные данные из потока: {data}")
        return

    for (rule_name, rule_symbol, rule_tf), rule in RULE_INSTANCES.items():
        if rule_symbol != symbol or rule_tf != tf:
            continue

        try:
            log.debug(f"[RULE_PROCESSOR] 🔍 {rule_name} → {symbol}/{tf}")
            result = await rule.update(open_time)
            if result:
                log.info(f"[RULE_PROCESSOR] ✅ Сигнал {result.direction.upper()} по {symbol}/{tf}")
        except Exception:
            log.exception(f"[RULE_PROCESSOR] ❌ Ошибка в update() правила {rule_name}")