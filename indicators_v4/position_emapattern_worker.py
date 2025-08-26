# position_emapattern_worker.py — слушатель закрытий позиций для EMA-паттернов (этап 2: логирование)

import asyncio
import logging

log = logging.getLogger("IND_EMA_PATTERN_DICT")

STREAM   = "signal_log_queue"
GROUP    = "ema_pattern_aggr_group"
CONSUMER = "ema_aggr_1"


# 🔸 Инициализация consumer group для стрима
async def _ensure_group(redis):
    try:
        await redis.xgroup_create(STREAM, GROUP, id="$", mkstream=True)
        log.info(f"Создана consumer group {GROUP} для {STREAM}")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug(f"Consumer group {GROUP} уже существует")
        else:
            log.exception("Ошибка создания consumer group")
            raise


# 🔸 Точка входа воркера: читаем закрытия и логируем
async def run_position_emapattern_worker(pg, redis):
    await _ensure_group(redis)

    while True:
        try:
            resp = await redis.xreadgroup(
                groupname=GROUP,
                consumername=CONSUMER,
                streams={STREAM: ">"},
                count=50,
                block=2000
            )
            if not resp:
                continue

            to_ack = []
            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)
                    try:
                        status = data.get("status")
                        if status != "closed":
                            continue

                        position_uid = data.get("position_uid")
                        strategy_id  = data.get("strategy_id")
                        direction    = data.get("direction")  # может не приходить — ок
                        logged_at    = data.get("logged_at")

                        # логируем факт получения закрытия; дальнейшие этапы добавим потом
                        log.info(f"[CLOSED] position_uid={position_uid} strategy_id={strategy_id} direction={direction} logged_at={logged_at}")

                    except Exception:
                        log.exception("Ошибка обработки события из stream signal_log_queue")

            if to_ack:
                await redis.xack(STREAM, GROUP, *to_ack)

        except Exception as e:
            log.error(f"Ошибка в цикле IND_EMA_PATTERN_DICT: {e}", exc_info=True)
            await asyncio.sleep(2)