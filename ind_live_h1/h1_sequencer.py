# h1_sequencer.py — секвенсор h1: LIVE → пауза → MW → пауза → PACK → пауза (использует L1 между шагами)

# 🔸 Импорты
import asyncio
import logging

from live_indicators_h1 import live_h1_pass, INITIAL_DELAY_SEC, SLEEP_BETWEEN_CYCLES_SEC
from live_mw_h1 import mw_h1_pass
from live_pack_h1 import pack_h1_pass

# 🔸 Логгер
log = logging.getLogger("SEQ_H1")

# 🔸 Бесконечный секвенсор
async def run_h1_sequencer(pg,
                           redis,
                           get_instances_by_tf,
                           get_precision,
                           get_active_symbols,
                           live_cache):
    log.debug("SEQ_H1: запуск секвенсора (LIVE → пауза → MW → пауза → PACK → пауза)")
    await asyncio.sleep(INITIAL_DELAY_SEC)

    while True:
        # LIVE h1
        await live_h1_pass(
            redis,
            get_instances_by_tf,
            get_precision,
            get_active_symbols,
            live_cache=live_cache,
        )
        await asyncio.sleep(SLEEP_BETWEEN_CYCLES_SEC)

        # MW h1
        await mw_h1_pass(
            redis,
            get_active_symbols,
            get_precision,
            live_cache,
        )
        await asyncio.sleep(SLEEP_BETWEEN_CYCLES_SEC)

        # PACK h1
        await pack_h1_pass(
            redis,
            get_instances_by_tf,
            get_active_symbols,
            get_precision,
            live_cache,
        )
        await asyncio.sleep(SLEEP_BETWEEN_CYCLES_SEC)