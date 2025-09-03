# 🔸 Инфраструктура лаборатории: логирование, PG/Redis, семафор, Redis-локи, статусы/прогресс ранa

import os
import json
import logging
import asyncio
import asyncpg
import redis.asyncio as aioredis
from contextlib import asynccontextmanager

# 🔸 Глобальные переменные и настройки
pg_pool = None
redis_client = None
log = logging.getLogger("LAB_INFRA")

DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
MAX_CONCURRENCY = int(os.getenv("LAB_MAX_CONCURRENCY", "20"))
POSITIONS_BATCH = int(os.getenv("LAB_POSITIONS_BATCH", "1000"))
FINISH_STREAM = os.getenv("LAB_FINISH_STREAM", "lab_results_stream")
LOCK_TTL_SEC = int(os.getenv("LAB_LOCK_TTL_SEC", "600"))

# 🔸 Семафор параллелизма (на процесс)
concurrency_sem = asyncio.Semaphore(MAX_CONCURRENCY)


# 🔸 Настройка логирования
def setup_logging():
    level = logging.DEBUG if DEBUG_MODE else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log.debug(
        "Логирование настроено (DEBUG_MODE=%s, MAX_CONCURRENCY=%s, BATCH=%s)",
        DEBUG_MODE, MAX_CONCURRENCY, POSITIONS_BATCH
    )


# 🔸 Инициализация подключения к PostgreSQL
async def setup_pg():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL не задан")
    pool = await asyncpg.create_pool(dsn=dsn, min_size=4, max_size=40, timeout=30.0)
    async with pool.acquire() as conn:
        await conn.execute("SELECT 1")
    globals()["pg_pool"] = pool
    log.info("Подключение к PostgreSQL установлено")


# 🔸 Инициализация подключения к Redis
async def setup_redis_client():
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", 6379))
    password = os.getenv("REDIS_PASSWORD")
    use_tls = os.getenv("REDIS_USE_TLS", "false").lower() == "true"
    proto = "rediss" if use_tls else "redis"
    client = aioredis.from_url(f"{proto}://{host}:{port}", password=password, decode_responses=True)
    await client.ping()
    globals()["redis_client"] = client
    log.info("Подключение к Redis установлено")


# 🔸 Redis-лок (эксклюзив на (lab_id, strategy_id))
@asynccontextmanager
async def redis_lock(key: str, ttl_sec: int = LOCK_TTL_SEC):
    token = os.urandom(8).hex()
    ok = await redis_client.set(key, token, nx=True, ex=ttl_sec)
    if not ok:
        raise RuntimeError(f"lock_busy:{key}")
    try:
        yield
    finally:
        try:
            cur = await redis_client.get(key)
            if cur == token:
                await redis_client.delete(key)
        except Exception:
            log.debug("Не удалось освободить лок (key=%s)", key)


# 🔸 Создание записи о ранe (status=queued; started_at выставит БД)
async def create_run(lab_id: int, strategy_id: int) -> int:
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO laboratory_runs_v4 (lab_id, strategy_id, status, progress_json)
            VALUES ($1, $2, 'queued', '{}')
            RETURNING id
            """,
            lab_id, strategy_id,
        )
        return int(row["id"])


# 🔸 Перевод рана в статус running
async def mark_run_started(run_id: int):
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "UPDATE laboratory_runs_v4 SET status='running' WHERE id=$1",
            run_id,
        )


# 🔸 Перевод рана в статус done
async def mark_run_finished(run_id: int):
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "UPDATE laboratory_runs_v4 SET status='done', finished_at=NOW() WHERE id=$1",
            run_id,
        )


# 🔸 Перевод рана в статус failed (c reason в progress_json)
async def mark_run_failed(run_id: int, reason: str | None = None):
    reason = reason or "unknown"
    async with pg_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE laboratory_runs_v4
            SET status='failed',
                finished_at=NOW(),
                progress_json = jsonb_set(COALESCE(progress_json,'{}'::jsonb), '{fail_reason}', to_jsonb($2::text), true)
            WHERE id=$1
            """,
            run_id, reason,
        )


# 🔸 Обновление прогресса (progress_json)
async def update_progress_json(run_id: int, data: dict):
    payload = json.dumps(data, ensure_ascii=False)
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "UPDATE laboratory_runs_v4 SET progress_json=$2 WHERE id=$1",
            run_id, payload,
        )


# 🔸 Отправка сигнала о завершении рана
async def send_finish_signal(lab_id: int, strategy_id: int, run_id: int):
    try:
        await redis_client.xadd(
            FINISH_STREAM,
            {"lab_id": str(lab_id), "strategy_id": str(strategy_id), "run_id": str(run_id)},
        )
    except Exception:
        log.exception("Ошибка XADD finish-сигнала (lab_id=%s, strategy_id=%s, run_id=%s)", lab_id, strategy_id, run_id)


# 🔸 Обёртка автоперезапуска фоновых задач (в стиле oracle_v4)
async def run_safe_loop(coro, label: str):
    while True:
        try:
            log.info("[%s] 🚀 Запуск задачи", label)
            await coro()
        except asyncio.CancelledError:
            log.info("[%s] ⏹️ Остановлено по сигналу", label)
            raise
        except Exception:
            log.exception("[%s] ❌ Упал с ошибкой — перезапуск через 5 секунд", label)
            await asyncio.sleep(5)