# rule_processor.py

import asyncio
import logging
import json
from datetime import datetime
from infra import infra, SIGNAL_CONFIGS
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

    # 🔸 Защита от повторной обработки через Redis-лок
    LOCK_TTL = {
        "m1": 45,
        "m5": 275,
        "m15": 875,
    }

    key = f"gen_lock:{symbol}:{tf}:{open_time.isoformat()}"
    ttl = LOCK_TTL.get(tf, 600)

    was_set = await infra.redis_client.set(key, "1", ex=ttl, nx=True)
    if not was_set:
        log.info(f"[RULE_PROCESSOR] ⏩ Пропущен повторный вызов для {symbol}/{tf} @ {open_time}")
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
                await enqueue_log_to_stream(
                    symbol=symbol,
                    timeframe=tf,
                    open_time=open_time,
                    rule=rule_name,
                    status="success",
                    signal_id=result.signal_id,
                    direction=result.direction,
                    reason=result.reason,
                    details=result.details,
                )
            else:
                await enqueue_log_to_stream(
                    symbol=symbol,
                    timeframe=tf,
                    open_time=open_time,
                    rule=rule_name,
                    status="skipped",
                    reason="Пересечения не произошло"
                )

        except Exception as e:
            log.exception(f"[RULE_PROCESSOR] ❌ Ошибка в update() правила {rule_name}")
            await enqueue_log_to_stream(
                symbol=symbol,
                timeframe=tf,
                open_time=open_time,
                rule=rule_name,
                status="error",
                reason="Ошибка в update()",
                details={"exception": str(e)}
            )
# 🔸 Публикация сигнала в Redis Stream signals_stream
async def publish_signal(result, open_time: datetime, symbol: str):
    redis = infra.redis_client
    now = datetime.utcnow().isoformat()

    try:
        config = next(s for s in SIGNAL_CONFIGS if s["id"] == result.signal_id)
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


# 🔸 Публикация лога генерации сигнала в Redis Stream generator_log_stream
async def enqueue_log_to_stream(
    symbol: str,
    timeframe: str,
    open_time: datetime,
    rule: str,
    status: str,
    signal_id: int = None,
    direction: str = "",
    reason: str = "",
    details: dict = None
):
    redis = infra.redis_client

    payload = {
        "symbol": symbol,
        "timeframe": timeframe,
        "open_time": open_time.isoformat(),
        "rule": rule,
        "status": status,
        "signal_id": str(signal_id) if signal_id is not None else "",
        "direction": direction or "",
        "reason": reason or "",
        "details": json.dumps(details or {}),
    }

    await redis.xadd("generator_log_stream", payload)
    log.info(f"[RULE_PROCESSOR] 🪵 Лог генерации отправлен → {symbol}/{timeframe} {status}")