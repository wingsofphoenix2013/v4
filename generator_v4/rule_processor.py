# rule_processor.py

import asyncio
import logging
from collections import defaultdict
from datetime import datetime

from infra import infra, ENABLED_TICKERS
from rule_loader import RULE_INSTANCES

log = logging.getLogger("GEN")

# 🔸 Буфер значений индикаторов:
# Ключ: (symbol, timeframe, open_time) → {param: value}
INDICATOR_BUFFER = defaultdict(dict)

# 🔸 Асинхронный воркер обработки индикаторов
async def run_rule_processor():
    redis = infra.redis_client
    stream = "indicator_stream"
    last_id = "$"  # читаем только новые события

    log.info("[RULE_PROCESSOR] Запуск обработки потока индикаторов")

    while True:
        try:
            response = await redis.xread(
                streams={stream: last_id},
                count=100,
                block=1000  # 1 сек
            )
            if not response:
                continue

            for _, messages in response:
                for msg_id, data in messages:
                    last_id = msg_id
                    await process_indicator_message(data)

        except Exception:
            log.exception("[RULE_PROCESSOR] ❌ Ошибка чтения из потока")
            await asyncio.sleep(1)
# 🔸 Обработка одного сообщения из потока индикаторов
async def process_indicator_message(data: dict):
    try:
        symbol = data["symbol"]
        tf = data["interval"]
        open_time = datetime.fromisoformat(data["open_time"])
        param = data["param"]
        value = float(data["value"])
    except Exception:
        log.warning(f"[RULE_PROCESSOR] ❌ Некорректные данные: {data}")
        return

    key = (symbol, tf, open_time)
    INDICATOR_BUFFER[key][param] = value

    # Найдём правило для этой пары
    for (rule_name, rule_symbol, rule_tf), rule in RULE_INSTANCES.items():
        if rule_symbol != symbol or rule_tf != tf:
            continue

        required = set(rule.required_indicators())
        current = set(INDICATOR_BUFFER[key].keys())

        if not required.issubset(current):
            return  # ещё не все параметры пришли

        indicator_values = INDICATOR_BUFFER.pop(key)

        try:
            result = await rule.update(open_time, indicator_values)
            if result:
                log.info(f"[RULE_PROCESSOR] ✅ Сигнал {result.direction.upper()} → {symbol}/{tf}")
                # сюда будет отправка в signals_stream
        except Exception:
            log.exception(f"[RULE_PROCESSOR] ❌ Ошибка выполнения update() для {rule_name} {symbol}/{tf}")