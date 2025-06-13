# position_opener.py

import json
import logging
import asyncio

from infra import infra
from position_state_loader import position_registry

# 🔸 Логгер модуля
log = logging.getLogger("POSITION_OPENER")

# 🔸 Открытие позиции (заглушка)
async def open_position(signal: dict, context: dict):
    strategy_id = signal["strategy_id"]
    symbol = signal["symbol"]
    direction = signal["direction"]

    log.info(f"📥 Получен сигнал на открытие: {symbol} {direction} (strategy {strategy_id})")
    # TODO: реализация расчётов и открытия
    return

# 🔸 Воркер: слушает Redis Stream strategy_opener_stream
async def run_position_opener_loop():
    stream = "strategy_opener_stream"
    group = "position_opener"
    consumer = "opener_1"
    redis = infra.redis_client

    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
        log.info(f"📡 Группа {group} создана для {stream}")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info(f"ℹ️ Группа {group} уже существует")
        else:
            log.exception("❌ Ошибка создания Consumer Group")
            return

    log.info(f"📥 Подписка на открытия: {stream} → {group}")

    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream: ">"},
                count=50,
                block=1000
            )

            for _, records in entries:
                for record_id, data in records:
                    try:
                        payload = json.loads(data["data"])
                        await open_position(payload, context={"redis": redis})
                        await redis.xack(stream, group, record_id)
                    except Exception:
                        log.exception("❌ Ошибка обработки команды на открытие позиции")
        except Exception:
            log.exception("❌ Ошибка чтения из потока открытия позиции")
            await asyncio.sleep(2)