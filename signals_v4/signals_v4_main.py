import asyncio
import logging
import json
import infra

from infra import (
    setup_logging,
    init_pg_pool,
    init_redis_client,
    ENABLED_TICKERS,
    ENABLED_SIGNALS,
    ENABLED_STRATEGIES
)

from processor import process_signal
log = logging.getLogger("SIGNALS_COORDINATOR")

# 🔸 Обёртка безопасного запуска задач
async def run_safe_loop(coro_factory, name: str):
    logger = logging.getLogger(name)
    while True:
        try:
            logger.info(f"Запуск задачи: {name}")
            await coro_factory()
        except Exception as e:
            logger.exception(f"Ошибка в задаче {name}: {e}")
        await asyncio.sleep(1)

# 🔸 Загрузка разрешённых тикеров из БД
async def load_enabled_tickers():
    log = logging.getLogger("STATE_LOADER")
    ENABLED_TICKERS.clear()
    async with infra.PG_POOL.acquire() as conn:
        rows = await conn.fetch("""
            SELECT symbol
            FROM tickers_v4
            WHERE status = 'enabled' AND tradepermission = 'enabled'
        """)
        for row in rows:
            ENABLED_TICKERS[row["symbol"]] = True
    log.info(f"Загружено {len(ENABLED_TICKERS)} активных тикеров")

# 🔸 Загрузка активных сигналов из БД
async def load_enabled_signals():
    log = logging.getLogger("STATE_LOADER")
    ENABLED_SIGNALS.clear()
    async with infra.PG_POOL.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, long_phrase, short_phrase
            FROM signals_v4
            WHERE enabled = true
        """)
        for row in rows:
            ENABLED_SIGNALS[row["id"]] = {
                "long": row["long_phrase"],
                "short": row["short_phrase"]
            }
    log.info(f"Загружено {len(ENABLED_SIGNALS)} сигналов")

# 🔸 Загрузка активных стратегий из БД
async def load_enabled_strategies():
    log = logging.getLogger("STATE_LOADER")
    ENABLED_STRATEGIES.clear()
    async with infra.PG_POOL.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, signal_id, allow_open, reverse
            FROM strategies_v4
            WHERE enabled = true AND archived = false
        """)
        for row in rows:
            ENABLED_STRATEGIES[row["id"]] = {
                "signal_id": row["signal_id"],
                "allow_open": row["allow_open"],
                "reverse": row["reverse"]
            }
    log.info(f"Загружено {len(ENABLED_STRATEGIES)} стратегий")

# 🔸 Загрузка начальных справочников из БД
async def load_initial_state():
    log = logging.getLogger("STATE_LOADER")
    log.info("Загрузка активных тикеров, сигналов и стратегий...")

    await load_enabled_tickers()
    await load_enabled_signals()
    await load_enabled_strategies()
# 🔸 Обработка события активации/деактивации тикера
async def handle_ticker_event(data: dict):
    log = logging.getLogger("PUBSUB_WATCHER")
    symbol = data.get("symbol")
    action = data.get("action")

    if not symbol or action not in {"enabled", "disabled"}:
        log.warning(f"Игнорировано событие тикера: {data}")
        return

    if action == "enabled":
        async with infra.PG_POOL.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT symbol
                FROM tickers_v4
                WHERE symbol = $1 AND status = 'enabled' AND tradepermission = 'enabled'
            """, symbol)
            if row:
                ENABLED_TICKERS[symbol] = True
                log.info(f"Тикер {symbol} добавлен в ENABLED_TICKERS")
            else:
                log.warning(f"Тикер {symbol} не прошёл фильтр enabled/tradepermission")
    else:
        if symbol in ENABLED_TICKERS:
            ENABLED_TICKERS.pop(symbol, None)
            log.info(f"Тикер {symbol} удалён из ENABLED_TICKERS")
# 🔸 Обработка события активации/деактивации сигнала
async def handle_signal_event(data: dict):
    log = logging.getLogger("PUBSUB_WATCHER")
    signal_id = data.get("id")
    action = data.get("action")

    if not signal_id or action not in {"true", "false"}:
        log.warning(f"Игнорировано событие сигнала: {data}")
        return

    if action == "true":
        async with infra.PG_POOL.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT id, long_phrase, short_phrase
                FROM signals_v4
                WHERE id = $1 AND enabled = true
            """, signal_id)
            if row:
                ENABLED_SIGNALS[row["id"]] = {
                    "long": row["long_phrase"],
                    "short": row["short_phrase"]
                }
                log.info(f"Сигнал {row['id']} добавлен в ENABLED_SIGNALS")
            else:
                log.warning(f"Сигнал {signal_id} не прошёл фильтр enabled")
    else:
        if signal_id in ENABLED_SIGNALS:
            ENABLED_SIGNALS.pop(signal_id, None)
            log.info(f"Сигнал {signal_id} удалён из ENABLED_SIGNALS")
# 🔸 Обработка события активации/деактивации стратегии
async def handle_strategy_event(data: dict):
    log = logging.getLogger("PUBSUB_WATCHER")
    strategy_id = data.get("id")
    action = data.get("action")

    if not strategy_id or action not in {"true", "false"}:
        log.warning(f"Игнорировано событие стратегии: {data}")
        return

    if action == "true":
        async with infra.PG_POOL.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT id, signal_id, allow_open, reverse
                FROM strategies_v4
                WHERE id = $1 AND enabled = true AND archived = false
            """, strategy_id)
            if row:
                ENABLED_STRATEGIES[row["id"]] = {
                    "signal_id": row["signal_id"],
                    "allow_open": row["allow_open"],
                    "reverse": row["reverse"]
                }
                log.info(f"Стратегия {row['id']} добавлена в ENABLED_STRATEGIES")
            else:
                log.warning(f"Стратегия {strategy_id} не прошла фильтр enabled/archived")
    else:
        if strategy_id in ENABLED_STRATEGIES:
            ENABLED_STRATEGIES.pop(strategy_id, None)
            log.info(f"Стратегия {strategy_id} удалена из ENABLED_STRATEGIES")
# 🔸 Подписка на Pub/Sub обновления
async def subscribe_and_watch_pubsub():
    log = logging.getLogger("PUBSUB_WATCHER")
    pubsub = infra.REDIS.pubsub()
    await pubsub.subscribe("tickers_v4_events", "signals_v4_events", "strategies_v4_events")
    log.info("Подписка на каналы Pub/Sub активна")

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue

        try:
            data = json.loads(message["data"])
            channel = message["channel"]

            if channel == "tickers_v4_events":
                await handle_ticker_event(data)
            elif channel == "signals_v4_events":
                await handle_signal_event(data)
            elif channel == "strategies_v4_events":
                await handle_strategy_event(data)

        except Exception as e:
            log.exception(f"Ошибка при обработке Pub/Sub события: {e}")
            
# 🔸 Чтение сигналов из Redis Stream и передача в обработку
async def read_and_process_signals():
    log = logging.getLogger("SIGNAL_STREAM_READER")
    redis = infra.REDIS
    group = "signal_processor"
    consumer = "worker-1"
    stream = "signals_stream"

    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
        log.info(f"Группа {group} создана для {stream}")
    except Exception:
        pass  # группа уже существует

    while True:
        try:
            messages = await redis.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream: ">"},
                count=100,
                block=3000  # мс
            )
            if messages:
                for _, entries in messages:
                    for entry in entries:
                        log.debug(f"Входящий сигнал: {dict(entry[1])}")
                    await asyncio.gather(*[
                        process_signal(dict(entry[1])) for entry in entries
                    ])
                    for entry_id, _ in entries:
                        await redis.xack(stream, group, entry_id)
        except Exception as e:
            log.exception(f"Ошибка при чтении из Redis Stream: {e}")
            await asyncio.sleep(1)

# 🔸 Основной запуск
async def main():
    setup_logging()
    log.info("Инициализация signals_v4")

    await init_pg_pool()
    await init_redis_client()
    log.info("Подключения Redis и PostgreSQL установлены")

    await load_initial_state()

    await asyncio.gather(
        run_safe_loop(subscribe_and_watch_pubsub, "PUBSUB_WATCHER"),
        run_safe_loop(read_and_process_signals, "SIGNAL_STREAM_READER")
    )

if __name__ == "__main__":
    asyncio.run(main())