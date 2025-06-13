# position_opener.py

import asyncio
import logging
import uuid
import json
from datetime import datetime
from dataclasses import dataclass, asdict

from infra import infra, get_price, get_indicator
from config_loader import config
from position_state_loader import position_registry, PositionState, Target

log = logging.getLogger("POSITION_OPENER")

@dataclass
class PositionCalculation:
    entry_price: float
    quantity: float
    planned_risk: float
    tp_targets: list
    sl_target: dict
    route: str
    log_uid: str

# 🔹 Расчёт параметров позиции, TP и SL
async def calculate_position_size(data: dict):
    # TODO: реализовать расчёт всех параметров позиции
    return "skip", "not implemented"

# 🔹 Открытие позиции и публикация события
async def open_position(calc_result: PositionCalculation, signal_data: dict):
    # TODO: регистрация позиции и публикация события в Redis
    pass

# 🔹 Логгирование skip-события в Redis Stream
async def publish_skip_reason(log_uid: str, strategy_id: int, reason: str):
    try:
        record = {
            "log_uid": log_uid,
            "strategy_id": str(strategy_id),
            "status": "skip",
            "note": reason,
            "position_uid": "",
            "logged_at": datetime.utcnow().isoformat()
        }
        await infra.redis_client.xadd("signal_log_queue", record)
        log.info(f"⚠️ [SKIP] strategy_id={strategy_id} log_uid={log_uid} reason=\"{reason}\"")
    except Exception:
        log.exception("❌ Ошибка при записи skip-события в Redis")

# 🔹 Основной воркер
async def run_position_opener_loop():
    stream = "strategy_opener_stream"
    group = "position_opener_group"
    consumer = "position_opener_1"
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

    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream: ">"},
                count=10,
                block=1000
            )
            if not entries:
                continue

            for _, records in entries:
                for record_id, data in records:
                    # Декодирование ключей и значений
                    data = {
                        k.decode() if isinstance(k, bytes) else k:
                        v.decode() if isinstance(v, bytes) else v
                        for k, v in data.items()
                    }

                    strategy_id = int(data["strategy_id"])
                    log_uid = data["log_uid"]

                    result = await calculate_position_size(data)
                    if isinstance(result, tuple) and result[0] == "skip":
                        reason = result[1]
                        await publish_skip_reason(log_uid, strategy_id, reason)
                        await redis.xack(stream, group, record_id)
                        continue

                    # TODO: реализация open_position и дальнейшая обработка

        except Exception:
            log.exception("❌ Ошибка в основном цикле position_opener_loop")
            await asyncio.sleep(5)