# signals_v4_main.py — координатор обработки сигналов v4 + live-обновление кеша стратегий по applied

# 🔸 Импорты
import asyncio
import logging
import json
import infra
from time import perf_counter

from infra import (
    setup_logging,
    init_pg_pool,
    init_redis_client,
    ENABLED_TICKERS,
    ENABLED_SIGNALS,
    ENABLED_STRATEGIES,
)

from processor import process_signal
from core_io import run_core_io

# 🔸 Константы стримов/CG
STRATEGY_STATE_STREAM = "strategy_state_stream"   # сюда прилетает action="applied" после reload_strategy()
STATE_CG = "signal_strategy_state_cg"
STATE_CONSUMER = "state-worker-1"

SIGNAL_STREAM = "signals_stream"
SIGNAL_CG = "signal_processor"
SIGNAL_CONSUMER = "worker-1"

# 🔸 Логгер
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
        rows = await conn.fetch(
            """
            SELECT symbol
            FROM tickers_bb
            WHERE status = 'enabled' AND tradepermission = 'enabled'
            """
        )
        for row in rows:
            ENABLED_TICKERS[row["symbol"]] = True
    log.info(f"Загружено {len(ENABLED_TICKERS)} активных тикеров")


# 🔸 Загрузка активных сигналов из БД
async def load_enabled_signals():
    log = logging.getLogger("STATE_LOADER")
    ENABLED_SIGNALS.clear()
    async with infra.PG_POOL.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, long_phrase, short_phrase
            FROM signals_v4
            WHERE enabled = true
            """
        )
        for row in rows:
            ENABLED_SIGNALS[row["id"]] = {
                "long": row["long_phrase"],
                "short": row["short_phrase"],
            }
    log.info(f"Загружено {len(ENABLED_SIGNALS)} сигналов")


# 🔸 Загрузка активных стратегий из БД
async def load_enabled_strategies():
    log = logging.getLogger("STATE_LOADER")
    ENABLED_STRATEGIES.clear()
    async with infra.PG_POOL.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, signal_id, allow_open, reverse
            FROM strategies_v4
            WHERE enabled = true AND archived = false
            """
        )
        for row in rows:
            ENABLED_STRATEGIES[row["id"]] = {
                "signal_id": row["signal_id"],
                "allow_open": row["allow_open"],
                "reverse": row["reverse"],
            }
    log.info(f"Загружено {len(ENABLED_STRATEGIES)} стратегий")


# 🔸 Загрузка начальных справочников из БД
async def load_initial_state():
    log = logging.getLogger("STATE_LOADER")
    log.info("Загрузка активных тикеров, сигналов и стратегий...")
    await load_enabled_tickers()
    await load_enabled_signals()
    await load_enabled_strategies()


# 🔸 Обновление кеша по одной стратегии (по sid)
async def reload_strategy_into_cache(strategy_id: int):
    log = logging.getLogger("STATE_LOADER")
    async with infra.PG_POOL.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, signal_id, allow_open, reverse
            FROM strategies_v4
            WHERE id = $1 AND enabled = true AND archived = false
            """,
            strategy_id,
        )

    # если стратегия прошла фильтр — обновить; иначе удалить из кеша
    if row:
        ENABLED_STRATEGIES[row["id"]] = {
            "signal_id": row["signal_id"],
            "allow_open": row["allow_open"],
            "reverse": row["reverse"],
        }
        log.info("Кеш стратегий обновлён: id=%s (signal_id=%s, allow_open=%s, reverse=%s)",
                 row["id"], row["signal_id"], row["allow_open"], row["reverse"])
    else:
        if strategy_id in ENABLED_STRATEGIES:
            ENABLED_STRATEGIES.pop(strategy_id, None)
            log.info("Стратегия удалена из кеша: id=%s", strategy_id)
        else:
            log.info("Стратегия не активна / отсутствует: id=%s (кеш без изменений)", strategy_id)


# 🔸 Обработка события активации/деактивации тикера (Pub/Sub)
async def handle_ticker_event(data: dict):
    log = logging.getLogger("PUBSUB_WATCHER")
    symbol = data.get("symbol")
    action = data.get("action")

    if not symbol or action not in {"enabled", "disabled"}:
        log.warning(f"Игнорировано событие тикера: {data}")
        return

    if action == "enabled":
        async with infra.PG_POOL.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT symbol
                FROM tickers_bb
                WHERE symbol = $1 AND status = 'enabled' AND tradepermission = 'enabled'
                """,
                symbol,
            )
            if row:
                ENABLED_TICKERS[symbol] = True
                log.info("Тикер %s добавлен в ENABLED_TICKERS", symbol)
            else:
                log.warning("Тикер %s не прошёл фильтр enabled/tradepermission", symbol)
    else:
        if symbol in ENABLED_TICKERS:
            ENABLED_TICKERS.pop(symbol, None)
            log.info("Тикер %s удалён из ENABLED_TICKERS", symbol)


# 🔸 Обработка события активации/деактивации сигнала (Pub/Sub)
async def handle_signal_event(data: dict):
    log = logging.getLogger("PUBSUB_WATCHER")
    signal_id = data.get("id")
    action = data.get("action")

    if not signal_id or action not in {"true", "false"}:
        log.warning(f"Игнорировано событие сигнала: {data}")
        return

    if action == "true":
        async with infra.PG_POOL.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, long_phrase, short_phrase
                FROM signals_v4
                WHERE id = $1 AND enabled = true
                """,
                signal_id,
            )
            if row:
                ENABLED_SIGNALS[row["id"]] = {
                    "long": row["long_phrase"],
                    "short": row["short_phrase"],
                }
                log.info("Сигнал %s добавлен в ENABLED_SIGNALS", row["id"])
            else:
                log.warning("Сигнал %s не прошёл фильтр enabled", signal_id)
    else:
        if signal_id in ENABLED_SIGNALS:
            ENABLED_SIGNALS.pop(signal_id, None)
            log.info("Сигнал %s удалён из ENABLED_SIGNALS", signal_id)


# 🔸 Обработка события активации/деактивации стратегии (Pub/Sub)
async def handle_strategy_event(data: dict):
    log = logging.getLogger("PUBSUB_WATCHER")
    strategy_id = data.get("id")
    action = data.get("action")

    if not strategy_id or action not in {"true", "false"}:
        log.warning(f"Игнорировано событие стратегии: {data}")
        return

    if action == "true":
        await reload_strategy_into_cache(int(strategy_id))
    else:
        if strategy_id in ENABLED_STRATEGIES:
            ENABLED_STRATEGIES.pop(strategy_id, None)
            log.info("Стратегия %s удалена из ENABLED_STRATEGIES", strategy_id)


# 🔸 Подписка на Pub/Sub обновления
async def subscribe_and_watch_pubsub():
    log = logging.getLogger("PUBSUB_WATCHER")
    pubsub = infra.REDIS.pubsub()
    await pubsub.subscribe("tickers_bb_events", "signals_v4_events", "strategies_v4_events")
    log.info("Подписка на каналы Pub/Sub активна")

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue
        try:
            data = json.loads(message["data"])
            channel = message["channel"]

            if channel == "tickers_bb_events":
                await handle_ticker_event(data)
            elif channel == "signals_v4_events":
                await handle_signal_event(data)
            elif channel == "strategies_v4_events":
                await handle_strategy_event(data)
        except Exception as e:
            log.exception(f"Ошибка при обработке Pub/Sub события: {e}")


# 🔸 Чтение подтверждений applied из strategy_state_stream (двухфазный протокол)
async def read_strategy_state_stream():
    log = logging.getLogger("STRATEGY_STATE_READER")
    redis = infra.REDIS

    try:
        await redis.xgroup_create(STRATEGY_STATE_STREAM, STATE_CG, id="$", mkstream=True)
        log.info("Группа %s создана для %s", STATE_CG, STRATEGY_STATE_STREAM)
    except Exception:
        # группа уже существует
        pass

    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=STATE_CG,
                consumername=STATE_CONSUMER,
                streams={STRATEGY_STATE_STREAM: ">"},
                count=200,
                block=1000,  # мс
            )
            if not entries:
                continue

            for _, records in entries:
                for entry_id, data in records:
                    try:
                        # data: {'type': 'strategy', 'action': 'applied', 'id': '<sid>'}
                        if data.get("type") == "strategy" and data.get("action") == "applied":
                            sid_raw = data.get("id")
                            sid = int(sid_raw) if sid_raw is not None else None
                            if sid is not None:
                                await reload_strategy_into_cache(sid)
                        await redis.xack(STRATEGY_STATE_STREAM, STATE_CG, entry_id)
                    except Exception:
                        log.exception("Ошибка разбора записи state/applied (id=%s)", entry_id)
        except Exception as e:
            log.exception("Ошибка чтения из state-stream: %s", e)
            await asyncio.sleep(1)


# 🔸 Чтение сигналов из Redis Stream и передача в обработку
async def read_and_process_signals():
    log = logging.getLogger("SIGNAL_STREAM_READER")
    redis = infra.REDIS

    try:
        await redis.xgroup_create(SIGNAL_STREAM, SIGNAL_CG, id="$", mkstream=True)
        log.info("Группа %s создана для %s", SIGNAL_CG, SIGNAL_STREAM)
    except Exception:
        # группа уже существует
        pass

    while True:
        try:
            messages = await redis.xreadgroup(
                groupname=SIGNAL_CG,
                consumername=SIGNAL_CONSUMER,
                streams={SIGNAL_STREAM: ">"},
                count=100,
                block=500,  # мс
            )
            if messages:
                for _, entries in messages:
                    for entry in entries:
                        log.debug("Входящий сигнал: %s", dict(entry[1]))
                    await asyncio.gather(*[process_signal(dict(entry[1])) for entry in entries])
                    for entry_id, _ in entries:
                        await redis.xack(SIGNAL_STREAM, SIGNAL_CG, entry_id)
        except Exception as e:
            log.exception("Ошибка при чтении из Redis Stream: %s", e)
            await asyncio.sleep(1)


# 🔸 Основной запуск
async def main():
    setup_logging()
    print("signals_v4: старт main()")
    import sys
    sys.stdout.flush()

    log = logging.getLogger("SIGNALS_COORDINATOR")

    try:
        # подключение к БД и Redis
        t0 = perf_counter()
        await init_pg_pool()
        await init_redis_client()
        t1 = perf_counter()
        log.info("Подключения Redis и PostgreSQL установлены за %.2f сек", t1 - t0)

        # проверка соединений
        try:
            pong = await infra.REDIS.ping()
            log.info("Redis ответил: %s", pong)
        except Exception as e:
            log.warning("Redis недоступен: %s", e)

        try:
            async with infra.PG_POOL.acquire() as conn:
                await conn.execute("SELECT 1")
            log.info("PostgreSQL соединение проверено")
        except Exception as e:
            log.warning("PostgreSQL недоступен: %s", e)

        # загрузка справочников
        t2 = perf_counter()
        await load_initial_state()
        t3 = perf_counter()
        log.info("Справочники загружены за %.2f сек", t3 - t2)

        # запуск компонентов
        await asyncio.gather(
            run_safe_loop(subscribe_and_watch_pubsub, "PUBSUB_WATCHER"),
            run_safe_loop(read_strategy_state_stream, "STRATEGY_STATE_READER"),
            run_safe_loop(read_and_process_signals, "SIGNAL_STREAM_READER"),
            run_safe_loop(run_core_io, "CORE_IO"),
        )

    except Exception as fatal:
        log.exception("FATAL: исключение в main(): %s", fatal)
        await asyncio.sleep(5)


# 🔸 Точка входа
if __name__ == "__main__":
    asyncio.run(main())