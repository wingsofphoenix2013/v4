# 🔸 oracle_indicatorwatcher_aggregator.py — Этап 1: чтение закрытий для индикаторных агрегатов (без расчётов)

import os
import json
import asyncio
import logging

import infra

# 🔸 Константы стрима/группы
STREAM_NAME   = os.getenv("ORACLE_IND_MW_STREAM", "signal_log_queue")
GROUP_NAME    = os.getenv("ORACLE_IND_MW_GROUP",  "oracle_indmw")
CONSUMER_NAME = os.getenv("ORACLE_IND_MW_CONSUMER", "oracle_indmw_1")
XREAD_COUNT   = int(os.getenv("ORACLE_IND_MW_COUNT", "50"))
XREAD_BLOCKMS = int(os.getenv("ORACLE_IND_MW_BLOCK_MS", "1000"))

log = logging.getLogger("ORACLE_IND_MW_AGG")


# 🔸 Создание consumer-group (идемпотентно)
async def _ensure_group():
    try:
        await infra.redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, id="$", mkstream=True)
        log.info("✅ Consumer group '%s' создана на '%s'", GROUP_NAME, STREAM_NAME)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info("ℹ️ Consumer group '%s' уже существует", GROUP_NAME)
        else:
            log.exception("❌ Ошибка создания consumer group: %s", e)
            raise


# 🔸 Этап 1: слушаем закрытия, пока только логируем
async def run_oracle_indicatorwatcher_aggregator():
    await _ensure_group()
    log.info("🚀 Этап 1 (IND-MW): слушаем stream '%s' (group=%s, consumer=%s)",
             STREAM_NAME, GROUP_NAME, CONSUMER_NAME)

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
                        status = data.get("status")
                        if status != "closed":
                            to_ack.append(msg_id)
                            continue

                        position_uid = data.get("position_uid")
                        strategy_id = data.get("strategy_id")

                        log.info("[IND-STAGE1] closed-event: pos=%s strat=%s",
                                 position_uid, strategy_id)

                        # пока только лог, без обработки
                        to_ack.append(msg_id)

                    except Exception as e:
                        to_ack.append(msg_id)
                        log.exception("❌ Ошибка обработки сообщения %s: %s", msg_id, e)

            if to_ack:
                await infra.redis_client.xack(STREAM_NAME, GROUP_NAME, *to_ack)

        except asyncio.CancelledError:
            log.info("⏹️ Индикаторный агрегатор остановлен")
            raise
        except Exception as e:
            log.exception("❌ Ошибка XREADGROUP: %s", e)
            await asyncio.sleep(1)