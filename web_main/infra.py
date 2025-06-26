# infra.py — глобальная инфраструктура: конфигурация, подключения, логирование

import os
import logging
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

import asyncpg
import redis.asyncio as aioredis
from fastapi.templating import Jinja2Templates

# 🔸 Переменные окружения
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_USE_TLS = os.getenv("REDIS_USE_TLS", "false").lower() == "true"
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"

# 🔸 Централизованная настройка логирования
def setup_logging():
    """
    Централизованная настройка логирования.
    DEBUG_MODE=True → debug/info/warning/error
    DEBUG_MODE=False → info/warning/error
    """
    level = logging.DEBUG if DEBUG_MODE else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

# 🔸 Подключение к PostgreSQL (асинхронный пул)
pg_pool: asyncpg.Pool = None

async def init_pg_pool():
    return await asyncpg.create_pool(DATABASE_URL)

# 🔸 Подключение к Redis
def init_redis_client():
    protocol = "rediss" if REDIS_USE_TLS else "redis"
    return aioredis.from_url(
        f"{protocol}://{REDIS_HOST}:{REDIS_PORT}",
        password=REDIS_PASSWORD,
        decode_responses=True
    )

# 🔸 FastAPI шаблоны
templates = Jinja2Templates(directory="templates")

# 🔸 Временная зона и фильтрация по локальному времени (Киев)
KYIV_TZ = ZoneInfo("Europe/Kyiv")

def get_kyiv_day_bounds(days_ago: int = 0) -> tuple[datetime, datetime]:
    """
    Возвращает границы суток по Киеву в naive-UTC формате (для SQL через asyncpg).
    days_ago = 0 → сегодня, 1 → вчера и т.д.
    """
    now_kyiv = datetime.now(KYIV_TZ)
    target_day = now_kyiv.date() - timedelta(days=days_ago)

    start_kyiv = datetime.combine(target_day, time.min, tzinfo=KYIV_TZ)
    end_kyiv = datetime.combine(target_day, time.max, tzinfo=KYIV_TZ)

    return (
        start_kyiv.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
        end_kyiv.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    )

def get_kyiv_range_backwards(days: int) -> tuple[datetime, datetime]:
    """
    Возвращает диапазон последних N суток по Киеву — в naive-UTC формате (для SQL).
    """
    now_kyiv = datetime.now(KYIV_TZ)
    start_kyiv = now_kyiv - timedelta(days=days)

    return (
        start_kyiv.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
        now_kyiv.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    )