# indicators_ema_status_live.py — ежеминутный каркас on-demand EMA-status по текущему бару (Этап 1)

import os
import asyncio
import logging
from datetime import datetime

log = logging.getLogger("EMA_STATUS_LIVE")

# 🔸 Конфиг
INTERVAL_SEC = int(os.getenv("EMA_STATUS_LIVE_INTERVAL_SEC", "60"))
REQUIRED_TFS = ("m5", "m15", "h1")

# 🔸 Основной воркер (Этап 1: только таймер и план обхода)
async def run_indicators_ema_status_live(pg, redis, get_instances_by_tf, get_precision, get_active_symbols):
    """
    Этап 1:
      - Каждые INTERVAL_SEC секунд стартует тик.
      - Собираем список активных символов и трёх ТФ.
      - Логируем планируемые задания (без выполнения расчётов).
    При переходе к Этапу 2 эти логи будут понижены до DEBUG.
    """
    while True:
        try:
            tick_iso = datetime.utcnow().isoformat()
            symbols = list(get_active_symbols() or [])
            planned = 0

            log.info("[TICK] start @ %s, symbols=%d", tick_iso, len(symbols))

            for sym in symbols:
                # (на следующих этапах сюда добавим ограничители и фактические задания)
                for tf in REQUIRED_TFS:
                    log.info("[PLAN] symbol=%s tf=%s", sym, tf)
                    planned += 1

            log.info("[TICK] end, planned=%d", planned)

        except Exception as e:
            log.error("loop error: %s", e, exc_info=True)

        await asyncio.sleep(INTERVAL_SEC)