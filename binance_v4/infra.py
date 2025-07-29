# infra.py

import os
import logging
import asyncio
import asyncpg
import redis.asyncio as aioredis
import aiohttp
from typing import Callable, Any
from binance.um_futures import UMFutures
from binance.error import ClientError


# 🔸 Глобальное состояние
class Infra:
    pg_pool: asyncpg.Pool = None
    redis_client: aioredis.Redis = None
    binance_client: UMFutures = None
    binance_ws_listen_key: str = None
    binance_ws_client: aiohttp.ClientWebSocketResponse = None
    binance_ws_session: aiohttp.ClientSession = None
    inflight_positions: dict[str, dict] = {}
    
infra = Infra()

# 🔸 Константы
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

# 🔸 Binance UMFutures: инициализация клиента (только MAINNET)
async def setup_binance_client():
    log = logging.getLogger("INFRA")

    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        raise RuntimeError("❌ BINANCE_API_KEY или BINANCE_API_SECRET не заданы")

    try:
        client = UMFutures(key=api_key, secret=api_secret)
        infra.binance_client = client

        log.info("🔑 Binance (UMFutures) инициализирован для MAINNET")

        # 🔸 Проверка доступности API
        try:
            server_time = client.time()
            log.info(f"📡 Binance MAINNET доступен. Время сервера: {server_time['serverTime']}")
        except Exception:
            log.exception("❌ Ошибка при /time — Binance MAINNET API недоступен")

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

# 🔸 Получение нового listenKey через клиент UMFutures

async def get_binance_listen_key() -> str:
    log = logging.getLogger("INFRA")

    try:
        listen_key = await run_in_thread(infra.binance_client.new_listen_key)
        log.info(f"🧾 listenKey получен: {listen_key}")
        global binance_listen_key
        binance_listen_key = listen_key
        return listen_key
    except Exception as e:
        log.exception(f"❌ Ошибка при получении listenKey через клиент: {e}")
        raise

# 🔸 Продление listenKey каждые 30 минут
async def keep_alive_binance_listen_key():
    log = logging.getLogger("INFRA")
    api_key = os.getenv("BINANCE_API_KEY")
    url = "https://fapi.binance.com/fapi/v1/listenKey"

    while True:
        if binance_listen_key is None:
            log.warning("⚠️ Нет актуального listenKey — пропускаем продление")
        else:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.put(
                        url,
                        headers={"X-MBX-APIKEY": api_key},
                        params={"listenKey": binance_listen_key}
                    ) as resp:
                        if resp.status == 200:
                            log.debug("🔄 listenKey успешно продлён")
                        else:
                            text = await resp.text()
                            log.warning(f"⚠️ Ошибка продления listenKey: HTTP {resp.status} — {text}")
            except Exception as e:
                log.warning(f"⚠️ Исключение при продлении listenKey: {e}")

        await asyncio.sleep(30 * 60)

# 🔸 Выполнение синхронной функции в отдельном потоке
async def run_in_thread(func: Callable[..., Any], *args, **kwargs) -> Any:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))