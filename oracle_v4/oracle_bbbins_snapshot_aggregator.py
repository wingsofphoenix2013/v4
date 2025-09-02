# oracle_bbbins_snapshot_aggregator.py — BB-bins snapshot агрегатор: Этап 1 (каркас, приём закрытий из Stream)

import os
import asyncio
import logging

import infra

log = logging.getLogger("ORACLE_BBBINS_SNAP")

# 🔸 Конфиг consumer-группы и чтения
STREAM_NAME   = os.getenv("ORACLE_BBBINS_STREAM",   "signal_log_queue")
GROUP_NAME    = os.getenv("ORACLE_BBBINS_GROUP",    "oracle_bbbins_snap")
CONSUMER_NAME = os.getenv("ORACLE_BBBINS_CONSUMER", "oracle_bbbins_1")
XREAD_COUNT   = int(os.getenv("ORACLE_BBBINS_COUNT",    "50"))
XREAD_BLOCKMS = int(os.getenv("ORACLE_BBBINS_BLOCK_MS", "1000"))

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

# 🔸 Основной цикл: читаем только закрытия и логируем их получение (Этап 1)
async def run_oracle_bbbins_snapshot_aggregator():
    await _ensure_group()
    log.info("🚀 BB-BINS SNAP: слушаем '%s' (group=%s, consumer=%s)", STREAM_NAME, GROUP_NAME, CONSUMER_NAME)
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
            for _, records in resp:
                for msg_id, data in records:
                    try:
                        if data.get("status") != "closed":
                            to_ack.append(msg_id)
                            continue
                        pos_uid = data.get("position_uid")
                        log.info("[BB-BINS SNAP] closed position received: uid=%s", pos_uid)
                        to_ack.append(msg_id)
                    except Exception as e:
                        to_ack.append(msg_id)
                        log.exception("❌ BB-BINS SNAP msg error %s: %s", msg_id, e)

            if to_ack:
                await infra.redis_client.xack(STREAM_NAME, GROUP_NAME, *to_ack)

        except asyncio.CancelledError:
            log.info("⏹️ BB-BINS snapshot агрегатор остановлен"); raise
        except Exception as e:
            log.exception("❌ XREADGROUP loop error: %s", e); await asyncio.sleep(1)