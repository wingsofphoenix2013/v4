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
            log.info(f"[RULE_PROCESSOR] 🔍 {rule_name} → {symbol}/{tf}")
            result = await rule.update(open_time)
            if result:
                log.info(f"[RULE_PROCESSOR] ✅ Сигнал {result.direction.upper()} по {symbol}/{tf}")
                await publish_signal(result, open_time, symbol)
        except Exception:
            log.exception(f"[RULE_PROCESSOR] ❌ Ошибка в update() правила {rule_name}")


# 🔸 Публикация сигнала в Redis Stream signals_stream
async def publish_signal(result: SignalResult, open_time: datetime, symbol: str):
    redis = infra.redis_client
    now = datetime.utcnow().isoformat()

    try:
        config = next(s for s in infra.SIGNAL_CONFIGS if s["id"] == result.signal_id)
        message = config["long_phrase"] if result.direction == "long" else config["short_phrase"]
    except StopIteration:
        log.info(f"[RULE_PROCESSOR] ⚠️ Не найдена фраза для signal_id={result.signal_id}")
        return

    payload = {
        "symbol": symbol,
        "message": message,
        "bar_time": open_time.isoformat(),
        "sent_at": now,
        "received_at": now,
        "source": "generator"
    }

    await redis.xadd("signals_stream", payload)
    log.info(f"[RULE_PROCESSOR] 📤 Сигнал опубликован в signals_stream → {symbol} {message}")