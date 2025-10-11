# m5_sequencer.py — секвенсор m5: LIVE → пауза → MW → пауза (использует L1 между шагами)

# 🔸 Импорты
import asyncio
import logging

from live_indicators_m5 import live_m5_pass, INITIAL_DELAY_SEC, SLEEP_BETWEEN_CYCLES_SEC
from live_mw_m5 import mw_m5_pass


# 🔸 Логгер
log = logging.getLogger("SEQ_M5")


# 🔸 Бесконечный секвенсор
async def run_m5_sequencer(pg,
                           redis,
                           get_instances_by_tf,
                           get_precision,
                           get_active_symbols,
                           live_cache):
    log.debug("SEQ_M5: запуск секвенсора (LIVE → пауза → MW → пауза)")
    await asyncio.sleep(INITIAL_DELAY_SEC)

    while True:
        # LIVE m5
        await live_m5_pass(
            redis,
            get_instances_by_tf,
            get_precision,
            get_active_symbols,
            live_cache=live_cache,
        )
        await asyncio.sleep(SLEEP_BETWEEN_CYCLES_SEC)

        # MW m5
        await mw_m5_pass(
            redis,
            get_active_symbols,
            get_precision,
            live_cache,
        )
        await asyncio.sleep(SLEEP_BETWEEN_CYCLES_SEC)