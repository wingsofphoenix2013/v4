# laboratory_v4_main.py — Laboratory v4: базовый сервис, загрузка активных стратегий и тикеров

import asyncio
import logging

from laboratory_v4_infra import setup_logging, init_pg_pool, init_redis_client, run_safe_loop
from laboratory_v4_config import LAB_START_DELAY_SEC, LAB_REFRESH_SEC
from laboratory_v4_repo import load_active_strategies, load_active_tickers
from lab_seeder_adx import seed as seed_adx

log = logging.getLogger("LAB_MAIN")

# 🔸 Глобальные кеши
active_strategies = []   # list[dict]: {id, enabled, market_watcher}
active_tickers    = []   # list[dict]: {symbol, precision_price}

# 🔸 Периодический рефреш кешей (лог — только сводка)
async def refresher(pg):
    if LAB_START_DELAY_SEC > 0:
        log.info(f"⏳ Задержка старта {LAB_START_DELAY_SEC} с")
        await asyncio.sleep(LAB_START_DELAY_SEC)

    while True:
        try:
            strategies = await load_active_strategies(pg)
            tickers    = await load_active_tickers(pg)

            active_strategies.clear()
            active_strategies.extend(strategies)

            active_tickers.clear()
            active_tickers.extend(tickers)

            log.info(f"✅ Загружено стратегий: {len(active_strategies)} | тикеров: {len(active_tickers)}")
        except Exception as e:
            log.error(f"❌ Ошибка обновления кешей: {e}", exc_info=True)

        await asyncio.sleep(LAB_REFRESH_SEC)

# 🔸 Точка входа Laboratory v4
async def main():
    setup_logging()
    log.info("📦 Laboratory v4: инициализация соединений")
    pg    = await init_pg_pool()
    _redis = await init_redis_client()  # подключение держим живым; на будущее пригодится

    # 🔸 один раз прогоняем сидер ADX (идемпотентно)
    try:
        log.info("🧩 ADX seeder: запуск")
        await seed_adx(pg)
        log.info("🧩 ADX seeder: завершён")
    except Exception as e:
        log.error(f"❌ ADX seeder error: {e}", exc_info=True)

    # здесь позже добавим планировщики/раннеры тестов
    await asyncio.gather(
        run_safe_loop(lambda: refresher(pg), "CONFIG_LOADER"),
        # r

if __name__ == "__main__":
    asyncio.run(main())