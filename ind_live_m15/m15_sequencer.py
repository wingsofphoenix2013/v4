# m15_sequencer.py — секвенсор m15: LIVE → пауза → MW → пауза → PACK → пауза (использует L1 между шагами)

# 🔸 Импорты
import asyncio
import logging

from live_indicators_m15 import live_m15_pass, INITIAL_DELAY_SEC, SLEEP_BETWEEN_CYCLES_SEC
from live_mw_m15 import mw_m15_pass
from live_pack_m15 import pack_m15_pass

# 🔸 Логгер
log = logging.getLogger("SEQ_M15")

# 🔸 Бесконечный секвенсор
async def run_m15_sequencer(pg,
                           redis,
                           get_instances_by_tf,
                           get_precision,
                           get_active_symbols,
                           live_cache):
    log.debug("SEQ_M15: запуск секвенсора (LIVE → пауза → MW → пауза → PACK → пауза)")
    await asyncio.sleep(INITIAL_DELAY_SEC)

    while True:
        # LIVE m15
        await live_m15_pass(
            redis,
            get_instances_by_tf,
            get_precision,
            get_active_symbols,
            live_cache=live_cache,
        )
        await asyncio.sleep(SLEEP_BETWEEN_CYCLES_SEC)

        # MW m15
        await mw_m15_pass(
            redis,
            get_active_symbols,
            get_precision,
            live_cache,
        )
        await asyncio.sleep(SLEEP_BETWEEN_CYCLES_SEC)

        # PACK m15
        await pack_m15_pass(
            redis,
            get_instances_by_tf,
            get_active_symbols,
            get_precision,
            live_cache,
        )
        await asyncio.sleep(SLEEP_BETWEEN_CYCLES_SEC)