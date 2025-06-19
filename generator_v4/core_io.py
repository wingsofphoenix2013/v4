# core_io.py

import asyncio
import logging
import json
from datetime import datetime
from infra import infra

log = logging.getLogger("GEN_IO")

# 🔸 Воркер обработки логов генерации сигналов
async def run_core_io():
    redis = infra.redis_client
    pg = infra.pg_pool
    stream = "generator_log_stream"
    last_id = "$"

    log.debug("[CORE_IO] ▶️ Запуск воркера логирования генерации")

    while True:
        try:
            response = await redis.xread(
                streams={stream: last_id},
                count=100,
                block=1000
            )
            if not response:
                continue

            for _, messages in response:
                for msg_id, data in messages:
                    last_id = msg_id
                    await process_log_entry(data)

        except Exception:
            log.exception("[CORE_IO] ❌ Ошибка чтения логов генерации")
            await asyncio.sleep(1)


# 🔸 Запись одной строки в generator_logs_v4
async def process_log_entry(data: dict):
    try:
        symbol = data["symbol"]
        timeframe = data["timeframe"]
        open_time = datetime.fromisoformat(data["open_time"])
        rule = data["rule"]
        status = data["status"]
        signal_id = int(data["signal_id"]) if data.get("signal_id") else None
        direction = data.get("direction") or None
        reason = data.get("reason") or None
        details = data.get("details") or "{}"  # 👈 оставляем строкой

        query = """
            INSERT INTO generator_logs_v4 (
                symbol, timeframe, open_time, rule, status,
                signal_id, direction, reason, details
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """

        await infra.pg_pool.execute(
            query,
            symbol, timeframe, open_time, rule, status,
            signal_id, direction, reason, details  # ✅ передаём str
        )

        log.debug(f"[GEN_IO] ✅ Лог записан: {symbol}/{timeframe} {status}")

    except Exception:
        log.exception("[GEN_IO] ❌ Ошибка обработки и записи лога")