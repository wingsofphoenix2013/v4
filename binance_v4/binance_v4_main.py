# binance_v4_main.py

import asyncio
import logging

from infra import setup_logging, setup_pg, setup_redis_client, setup_binance_client, setup_binance_ws_client
from redis_consumer import run_redis_consumer
from strategy_registry import load_binance_enabled_strategies, run_binance_strategy_watcher, load_symbol_precisions
from binance_ws_v4 import run_binance_ws_listener

# 🔸 Обёртка с автоперезапуском для воркеров
async def run_safe_loop(coro_factory, label: str):
    log = logging.getLogger(label)
    while True:
        try:
            log.info(f"🔄 Стартует цикл: {label}")
            await coro_factory()
        except Exception as e:
            log.exception(f"❌ Ошибка в цикле {label}: {e}")
        await asyncio.sleep(5)

# 🔸 Главная точка входа
async def main():
    setup_logging()
    await setup_pg()
    await setup_redis_client()
    await setup_binance_client()
    await setup_binance_ws_client()
    await load_binance_enabled_strategies()
    await load_symbol_precisions()

    await asyncio.gather(
        run_safe_loop(run_redis_consumer, label="REDIS_CONSUMER"),
        run_safe_loop(run_binance_strategy_watcher, label="STRATEGY_WATCHER"),
        run_safe_loop(run_binance_ws_listener, label="BINANCE_WS")
    )

if __name__ == "__main__":
    asyncio.run(main())