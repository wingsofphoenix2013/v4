# 🔸 oracle_emastatus_snapshot_aggregator.py — EMA-status snapshot агрегатор: Этап 1 (только чтение стрима закрытий)

import os
import asyncio
import logging

import infra

log = logging.getLogger("ORACLE_EMASTATUS_SNAP")

# 🔸 Конфиг consumer-группы и чтения
STREAM_NAME   = os.getenv("ORACLE_EMASTATUS_STREAM",   "signal_log_queue")
GROUP_NAME    = os.getenv("ORACLE_EMASTATUS_GROUP",    "oracle_emastatus_snap")
CONSUMER_NAME = os.getenv("ORACLE_EMASTATUS_CONSUMER", "oracle_emastatus_1")
XREAD_COUNT   = int(os.getenv("ORACLE_EMASTATUS_COUNT",    "50"))
XREAD_BLOCKMS = int(os.getenv("ORACLE_EMASTATUS_BLOCK_MS", "1000"))


# 🔸 Создание consumer-group (идемпотентно)
async def _ensure_group():
    try:
        await infra.redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, id="$", mkstream=True)
        log.debug("✅ Consumer group '%s' создана на '%s'", GROUP_NAME, STREAM_NAME)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug("ℹ️ Consumer group '%s' уже существует", GROUP_NAME)
        else:
            log.exception("❌ Ошибка создания consumer group: %s", e)
            raise


# 🔸 Основной цикл (Этап 1 — только слушаем закрытия)
async def run_oracle_emastatus_snapshot_aggregator():
    await _ensure_group()
    log.debug("🚀 EMA-STATUS SNAP: слушаем '%s' (group=%s, consumer=%s)", STREAM_NAME, GROUP_NAME, CONSUMER_NAME)

    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={STREAM_NAME: ">"},
                count=XREAD_COUNT,
                block=XREAD_BLOCKMS,
            )
            if not resp:
                continue

            to_ack = []
            for _, records in resp:
                for msg_id, data in records:
                    to_ack.append(msg_id)
                    try:
                        status = data.get("status")
                        pos_uid = data.get("position_uid")
                        strategy_id = data.get("strategy_id")
                        direction = data.get("direction")
                        symbol = data.get("symbol")

                        if status != "closed":
                            log.debug("[EMA-STATUS SNAP] skip msg_id=%s uid=%s reason=status=%s", msg_id, pos_uid, status)
                            continue

                        # Этап 1: только фиксация факта закрытия (без чтения БД/обновления агрегатов)
                        log.info(
                            "[EMA-STATUS SNAP] closed position received: uid=%s sym=%s strat=%s dir=%s",
                            pos_uid, symbol, strategy_id, direction
                        )

                        # Этап 2/3 будут добавлены позже:
                        # - рассчёт агрегата (без апдейта БД)
                        # - апдейт таблиц/Redis и отметка позиции

                    except Exception as e:
                        log.exception("❌ EMA-STATUS SNAP msg error %s: %s", msg_id, e)

            if to_ack:
                await infra.redis_client.xack(STREAM_NAME, GROUP_NAME, *to_ack)

        except asyncio.CancelledError:
            log.debug("⏹️ EMA-STATUS snapshot агрегатор остановлен")
            raise
        except Exception as e:
            log.exception("❌ XREADGROUP loop error: %s", e)
            await asyncio.sleep(1)