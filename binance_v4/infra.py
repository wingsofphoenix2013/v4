# infra.py

import os
import logging
import asyncpg
import redis.asyncio as aioredis
from binance.um_futures import UMFutures
from binance.error import ClientError


# 🔸 Глобальное состояние
class Infra:
    pg_pool: asyncpg.Pool = None
    redis_client: aioredis.Redis = None
    binance_client: UMFutures = None


infra = Infra()


# 🔸 Константы (если понадобятся)
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"


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


# 🔸 Binance Testnet (официальный коннектор)
async def setup_binance_client():
    log = logging.getLogger("INFRA")

    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    testnet_url = "https://testnet.binancefuture.com"

    if not api_key or not api_secret:
        raise RuntimeError("❌ BINANCE_API_KEY или BINANCE_API_SECRET не заданы")

    try:
        client = UMFutures(key=api_key, secret=api_secret, base_url=testnet_url)
        infra.binance_client = client
        log.info("🔑 Binance (UMFutures) инициализирован для Testnet")

        # 🔸 Проверка доступности API (без авторизации)
        try:
            server_time = client.time()
            log.info(f"📡 Binance Testnet доступен. Время сервера: {server_time['serverTime']}")
        except Exception as e:
            log.exception("❌ Ошибка при /time — Testnet API недоступен")

        # 🔸 Проверка авторизации
        try:
            acc_info = client.account()
            log.info(f"✅ Авторизация успешна. Общий баланс: {acc_info.get('totalWalletBalance', '?')}")
        except ClientError as e:
            log.error(f"❌ Ошибка авторизации Binance: {e.error_message}")
        except Exception:
            log.exception("❌ Неизвестная ошибка авторизации Binance")

    except Exception as e:
        log.exception("❌ Ошибка инициализации Binance клиента")
        raise