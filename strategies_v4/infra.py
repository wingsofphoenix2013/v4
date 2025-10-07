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

    raw = await infra.redis_client.get(f"bb:price:{symbol}")  # ← новый ключ Bybit
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

    log.debug("🔍 Начало инициализации кеша индикаторов из Redis")
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
            log.debug(f"✅ Кеш загружен: {symbol} {interval} ({len(params)} параметров)")
        except Exception:
            log.exception(f"❌ Ошибка при инициализации кеша для {symbol}-{interval}")

    log.debug("✅ Инициализация кеша индикаторов завершена")

# 🔸 LAB: распределённый семафор в Redis для ограничения одновременных запросов в лабораторию

# ключ по умолчанию (можно переопределить env)
_LAB_SEMA_KEY = os.getenv("LAB_SEMA_KEY", "lab:sema:labreq")
_LAB_SEMA_LIMIT = int(os.getenv("LAB_SEMA_LIMIT", "200"))
_LAB_SEMA_TTL = int(os.getenv("LAB_SEMA_TTL", "120"))  # чуть больше дедлайна лаборатории

# Lua-скрипты
_LAB_SEMA_ACQ_SCRIPT = """
-- KEYS[1] = key, ARGV[1] = limit, ARGV[2] = ttl_sec, ARGV[3] = holder
local cur = redis.call('SCARD', KEYS[1])
local limit = tonumber(ARGV[1])
if cur >= limit then return 0 end
redis.call('SADD', KEYS[1], ARGV[3])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return 1
"""

_LAB_SEMA_REL_SCRIPT = """
-- KEYS[1] = key, ARGV[1] = holder
redis.call('SREM', KEYS[1], ARGV[1])
return 1
"""

_lab_sema_acq_sha = None
_lab_sema_rel_sha = None

async def _lab_sema_ensure_loaded():
    global _lab_sema_acq_sha, _lab_sema_rel_sha
    if _lab_sema_acq_sha and _lab_sema_rel_sha:
        return
    rc = infra.redis_client
    _lab_sema_acq_sha = await rc.script_load(_LAB_SEMA_ACQ_SCRIPT)
    _lab_sema_rel_sha = await rc.script_load(_LAB_SEMA_REL_SCRIPT)

# 🔸 Захват слота (true/false)
async def lab_sema_acquire(holder: str, *, limit: int | None = None, ttl: int | None = None, key: str | None = None) -> bool:
    await _lab_sema_ensure_loaded()
    rc = infra.redis_client
    k = key or _LAB_SEMA_KEY
    lim = limit if limit is not None else _LAB_SEMA_LIMIT
    t = ttl if ttl is not None else _LAB_SEMA_TTL
    try:
        res = await rc.evalsha(_lab_sema_acq_sha, 1, k, lim, t, holder)
        return bool(res)
    except aioredis.ResponseError as e:
        # fallback, если скрипт выгрузился
        if "NOSCRIPT" in str(e):
            await _lab_sema_ensure_loaded()
            res = await rc.evalsha(_lab_sema_acq_sha, 1, k, lim, t, holder)
            return bool(res)
        raise

# 🔸 Освобождение слота (безопасно вызывать в finally)
async def lab_sema_release(holder: str, *, key: str | None = None) -> None:
    await _lab_sema_ensure_loaded()
    rc = infra.redis_client
    k = key or _LAB_SEMA_KEY
    try:
        await rc.evalsha(_lab_sema_rel_sha, 1, k, holder)
    except aioredis.ResponseError as e:
        if "NOSCRIPT" in str(e):
            await _lab_sema_ensure_loaded()
            await rc.evalsha(_lab_sema_rel_sha, 1, k, holder)