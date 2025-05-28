# config_loader.py

import logging
log = logging.getLogger("CONFIG_LOADER")

# Заглушка для инициализации in-memory состояния
async def init_config_state():
    log.info("⚙️ [CONFIG_LOADER] Состояние конфигурации инициализировано (заглушка)")

# Заглушка для слушателя событий Pub/Sub
async def config_event_listener():
    log.info("📡 [CONFIG_LOADER] Слушатель Pub/Sub запущен (заглушка)")
    while True:
        await asyncio.sleep(10)