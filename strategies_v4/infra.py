# infra.py

import os
import time
import logging
import asyncio
import asyncpg
import redis.asyncio as aioredis

# 🔸 Глобальное состояние
class Infra:
    pg_pool: asyncpg.Pool = None
    redis_client: aioredis.Redis = None

infra = Infra()

# 🔸 Константы
SIGNAL_STREAM = "signals_stream"
EVENT_STREAM = "strategy_events"
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"

# 🔸 Кеши markprice и индикаторов
_price_cache: dict[str, float] = {}
_price_ts: dict[str, float] = {}

_indicator_cache: dict[tuple[str, str, str], float] = {}

# 🔸 Логирование
def setup_logging():
    level = logging.DEBUG if DEBUG_MODE else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

# 🔸 PostgreSQL
async def setup_pg():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("❌ DATABASE_URL не задан")

    pool = await asyncpg.create_pool(db_url)
    await pool.execute("SELECT 1")
    infra.pg_pool = pool
    logging.getLogger("INFRA").info("🛢️ Подключение к PostgreSQL установлено")

# 🔸 Redis
async def setup_redis_client():
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", 6379))
    password = os.getenv("REDIS_PASSWORD")
    use_tls = os.getenv("REDIS_USE_TLS", "false").lower() == "true"

    protocol = "rediss" if use_tls else "redis"
    redis_url = f"{protocol}://{host}:{port}"

    client = aioredis.from_url(
        redis_url,
        password=password,
        decode_responses=True
    )

    await client.ping()
    infra.redis_client = client
    logging.getLogger("INFRA").info("📡 Подключение к Redis установлено")

# 🔸 Загрузка индикаторов (bulk)
async def load_indicators(symbol: str, params: list[str], timeframe: str) -> dict:
    redis = infra.redis_client
    result = {}

    if not redis:
        raise RuntimeError("❌ Redis клиент не инициализирован")

    keys = [f"ind:{symbol}:{timeframe}:{param}" for param in params]
    values = await redis.mget(*keys)

    for param, value in zip(params, values):
        result[param] = float(value) if value is not None else None

    return result

# 🔸 Получение цены (markprice) с TTL = 1 секунда
async def get_price(symbol: str) -> float | None:
    now = time.monotonic()
    if symbol in _price_cache and now - _price_ts[symbol] < 1.0:
        return _price_cache[symbol]

    raw = await infra.redis_client.get(f"price:{symbol}")
    if raw:
        try:
            price = float(raw)
            _price_cache[symbol] = price
            _price_ts[symbol] = now
            return price
        except ValueError:
            logging.getLogger("INFRA").warning(f"⚠️ Неверный формат цены: {raw}")
    return None

# 🔸 Получение индикатора из кеша
async def get_indicator(symbol: str, tf: str, param: str) -> float | None:
    return _indicator_cache.get((symbol, tf, param))

# 🔸 Подписка на поток готовых индикаторов
async def listen_indicator_stream():
    stream = "indicator_stream_core"
    group = "infra_cache"
    consumer = "worker_1"
    redis = infra.redis_client
    log = logging.getLogger("INFRA")

    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
        log.debug(f"📡 Группа {group} создана для {stream}")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug(f"ℹ️ Группа {group} уже существует")
        else:
            log.exception("❌ Ошибка создания Consumer Group")
            return

    log.debug(f"📥 Подписка на индикаторы: {stream} → {group}")

    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream: ">"},
                count=100,
                block=1000
            )
            for _, records in entries:
                for record_id, data in records:
                    try:
                        symbol = data["symbol"]
                        tf = data["interval"]
                        param = data["param_name"]
                        value = float(data["value"])

                        _indicator_cache[(symbol, tf, param)] = value
                        await redis.xack(stream, group, record_id)
                    except Exception:
                        log.exception("❌ Ошибка при обработке записи индикатора")
        except Exception:
            log.exception("❌ Ошибка в потоке подписки индикаторов")
            await asyncio.sleep(2)
# 🔸 Инициализация кеша индикаторов из Redis (по ключам ind:*)
log = logging.getLogger("INFRA")

async def init_indicator_cache_via_redis():
    redis = infra.redis_client
    if not redis:
        raise RuntimeError("❌ Redis клиент не инициализирован")

    log.info("🔍 Начало инициализации кеша индикаторов из Redis")
    cursor = 0
    key_groups: dict[tuple[str, str], list[str]] = {}

    while True:
        cursor, keys = await redis.scan(cursor=cursor, match="ind:*", count=500)
        for key in keys:
            parts = key.split(":")
            if len(parts) != 4:
                continue
            _, symbol, interval, param = parts
            key_groups.setdefault((symbol, interval), []).append(param)

        if cursor == 0:
            break

    for (symbol, interval), params in key_groups.items():
        try:
            result = await load_indicators(symbol, params, interval)
            for param, value in result.items():
                if value is not None:
                    _indicator_cache[(symbol, interval, param)] = value
            log.info(f"✅ Кеш загружен: {symbol} {interval} ({len(params)} параметров)")
        except Exception:
            log.exception(f"❌ Ошибка при инициализации кеша для {symbol}-{interval}")

    log.info("✅ Инициализация кеша индикаторов завершена")