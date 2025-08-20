# position_snapshot_worker.py — чтение positions_open_stream (своя группа), подготовка к снятию среза

import asyncio
import logging
from datetime import datetime

log = logging.getLogger("IND_POS_SNAPSHOT")

STREAM = "positions_open_stream"
GROUP  = "indicators_position_group"
CONSUMER = "ind_pos_1"

# 🔸 Основной воркер: читаем открытия позиций, пока только логируем payload
async def run_position_snapshot_worker(redis):
    try:
        await redis.xgroup_create(STREAM, GROUP, id="$", mkstream=True)
        log.info(f"Группа {GROUP} создана для {STREAM}")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info(f"Группа {GROUP} уже существует")
        else:
            log.exception("Ошибка создания consumer group")
            return

    while True:
        try:
            resp = await redis.xreadgroup(
                groupname=GROUP,
                consumername=CONSUMER,
                streams={STREAM: ">"},
                count=20,
                block=2000
            )

            if not resp:
                continue

            to_ack = []
            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)
                    try:
                        # данные приходят как строки — используем .get
                        position_uid = data.get("position_uid")
                        symbol = data.get("symbol")
                        strategy_id = data.get("strategy_id")
                        direction = data.get("direction")
                        created_at = data.get("created_at")

                        log.info(f"[OPENED] uid={position_uid} {symbol} "
                                 f"strategy={strategy_id} dir={direction} created_at={created_at}")

                    except Exception:
                        log.exception("Ошибка разбора сообщения positions_open_stream")

            if to_ack:
                await redis.xack(STREAM, GROUP, *to_ack)

        except Exception as e:
            log.error(f"Ошибка в цикле IND_POS_SNAPSHOT: {e}", exc_info=True)
            await asyncio.sleep(2)