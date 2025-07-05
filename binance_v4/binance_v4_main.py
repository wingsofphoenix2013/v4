# binance_v4_main.py

import asyncio
import logging

from infra import setup_logging, setup_pg, setup_redis_client, setup_binance_client
from redis_consumer import run_redis_consumer
from strategy_registry import load_binance_enabled_strategies, run_binance_strategy_watcher
from binance_worker import handle_opened


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


# 🔸 Отложенный запуск тестовой покупки через 2 минуты
async def run_test_order():
    await asyncio.sleep(120)

    test_event = {
        "strategy_id": "999",
        "symbol": "BTCUSDT",
        "direction": "long",
        "quantity": "0.001",
        "event_type": "opened"
    }

    await handle_opened(test_event)


# 🔸 Главная точка входа
async def main():
    setup_logging()
    await setup_pg()
    await setup_redis_client()
    await setup_binance_client()
    await load_binance_enabled_strategies()

    # 🔸 Фоновая задача: отложенный тестовый ордер
    asyncio.create_task(run_test_order())

    await asyncio.gather(
        run_safe_loop(run_redis_consumer, label="REDIS_CONSUMER"),
        run_safe_loop(run_binance_strategy_watcher, label="STRATEGY_WATCHER"),
    )


if __name__ == "__main__":
    asyncio.run(main())