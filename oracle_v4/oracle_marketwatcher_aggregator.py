# 🔸 oracle_marketwatcher_aggregator.py — Этап 1: чтение закрытий из stream (без учёта)

import os
import json
import asyncio
import logging
from datetime import datetime

import infra

# 🔸 Константы стрима/группы
STREAM_NAME   = os.getenv("ORACLE_MW_STREAM", "signal_log_queue")
GROUP_NAME    = os.getenv("ORACLE_MW_GROUP",  "oracle_mw")
CONSUMER_NAME = os.getenv("ORACLE_MW_CONSUMER","oracle_mw_1")
XREAD_COUNT   = int(os.getenv("ORACLE_MW_COUNT", "50"))
XREAD_BLOCKMS = int(os.getenv("ORACLE_MW_BLOCK_MS", "1000"))

log = logging.getLogger("ORACLE_MW_AGG")


# 🔸 Создание consumer-group (идемпотентно)
async def _ensure_group():
    try:
        await infra.redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, id="$", mkstream=True)
        log.info("✅ Consumer group '%s' создана на '%s'", GROUP_NAME, STREAM_NAME)
    except Exception as e:
        # BUSYGROUP означает, что группа уже есть — это нормально
        if "BUSYGROUP" in str(e):
            log.info("ℹ️ Consumer group '%s' уже существует", GROUP_NAME)
        else:
            log.exception("❌ Ошибка создания consumer group: %s", e)
            raise


# 🔸 Этап 1: читаем stream, фильтруем закрытия, логируем
async def run_oracle_marketwatcher_aggregator():
    await _ensure_group()
    log.info("🚀 Этап 1: слушаем stream '%s' (group=%s, consumer=%s)", STREAM_NAME, GROUP_NAME, CONSUMER_NAME)

    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={STREAM_NAME: ">"},
                count=XREAD_COUNT,
                block=XREAD_BLOCKMS
            )
            if not resp:
                continue

            to_ack = []
            for _, messages in resp:
                for msg_id, data in messages:
                    try:
                        # ожидаемые поля от продюсера:
                        # "status": "closed", "position_uid", "strategy_id", "log_uid", "note", "logged_at"
                        status = data.get("status")
                        if status != "closed":
                            to_ack.append(msg_id)
                            continue

                        strategy_id = data.get("strategy_id")
                        position_uid = data.get("position_uid")
                        log_uid = data.get("log_uid")
                        note = data.get("note")
                        logged_at = data.get("logged_at")

                        log.info("[STAGE1] closed-event: pos=%s strat=%s log=%s at=%s note=%s",
                                 position_uid, strategy_id, log_uid, logged_at, note)

                        # Этап 1: только логируем и ACK — ни БД, ни Redis-ключей
                        to_ack.append(msg_id)

                    except Exception as e:
                        # на ошибке тоже ACK, чтобы не зависнуть (повторно получим из PEL только если упадём сами)
                        to_ack.append(msg_id)
                        log.exception("❌ Ошибка парсинга сообщения %s: %s", msg_id, e)

            if to_ack:
                await infra.redis_client.xack(STREAM_NAME, GROUP_NAME, *to_ack)

        except asyncio.CancelledError:
            log.info("⏹️ Аггрегатор остановлен")
            raise
        except Exception as e:
            log.exception("❌ Ошибка XREADGROUP: %s", e)
            await asyncio.sleep(1)