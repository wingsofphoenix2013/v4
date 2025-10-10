# laboratory_infra.py — инфраструктура лабораторного сервиса (логи, PG/Redis, безопасный запуск воркеров)

# 🔸 Импорты
import asyncio
import logging
import asyncpg
import redis.asyncio as aioredis

# 🔸 Параметры подключения (без ENV): задаются прямо в файле
LAB_DB_URL: str = "postgresql://user:password@hostname:5432/database"   # ← укажи свой DSN
LAB_REDIS_URL: str = "redis://localhost:6379/0"                          # ← укажи свой Redis URL
LAB_DEBUG_MODE: bool = False                                             # True → DEBUG, False → INFO

# 🔸 Настройка централизованного логирования
def setup_logging():
    level = logging.DEBUG if LAB_DEBUG_MODE else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger("LAB_INFRA")
    log.info("Логирование инициализировано (level=%s)", "DEBUG" if LAB_DEBUG_MODE else "INFO")

# 🔸 Подключение к PostgreSQL (read-only по договорённости)
async def init_pg_pool():
    log = logging.getLogger("LAB_INFRA")
    pool = await asyncpg.create_pool(LAB_DB_URL)
    # простая проверка соединения
    async with pool.acquire() as conn:
        await conn.fetch("SELECT 1")
    log.info("PG: пул соединений создан")
    return pool

# 🔸 Подключение к Redis (async + health-check)
async def init_redis_client():
    log = logging.getLogger("LAB_INFRA")
    client = aioredis.from_url(
        LAB_REDIS_URL,
        decode_responses=True,
        encoding="utf-8",
        socket_connect_timeout=3,
        socket_keepalive=True,
    )
    # health-check с короткими ретраями
    for attempt in range(3):
        try:
            pong = await client.ping()
            if pong:
                log.info("Redis: подключение установлено, PING ok")
                return client
        except Exception as e:
            if attempt == 2:
                log.error("Redis: не удалось подключиться после 3 попыток: %s", e, exc_info=True)
                raise
            # задержка 1s, затем 2s
            await asyncio.sleep(1 + attempt)

# 🔸 Безопасный запуск фонового воркера (бесконечный перезапуск при ошибках)
async def run_safe_loop(coro_fn, name: str, retry_delay: int = 5):
    log = logging.getLogger(name)
    while True:
        try:
            log.info("Запуск воркера")
            await coro_fn()
        except asyncio.CancelledError:
            # корректное завершение
            log.info("Воркер остановлен (cancelled)")
            raise
        except Exception as e:
            log.error("Ошибка: %s", e, exc_info=True)
            log.info("Перезапуск через %d секунд...", retry_delay)
            await asyncio.sleep(retry_delay)