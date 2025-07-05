# infra.py

import os
import logging
import asyncio
import asyncpg
import redis.asyncio as aioredis
import aiohttp
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

        # 🔸 Проверка доступности API
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

# 🔸 Binance WebSocket (User Data Stream)
async def setup_binance_ws_client():
    log = logging.getLogger("INFRA")
    client = infra.binance_client

    try:
        # Получаем listenKey через REST
        listen_key_resp = client.new_listen_key()
        listen_key = listen_key_resp["listenKey"]
        infra.binance_ws_listen_key = listen_key

        # Устанавливаем WebSocket-соединение
        url = f"wss://fstream.binance.com/ws/{listen_key}"
        session = aiohttp.ClientSession()
        ws = await session.ws_connect(url)
        infra.binance_ws_session = session
        infra.binance_ws_client = ws

        log.info("🔌 Binance WebSocket подключён")

        # Запускаем фоновую задачу продления listenKey
        asyncio.create_task(_keep_alive_binance_listen_key(listen_key))

        # Запускаем фоновую задачу переподключения через 23 часа
        asyncio.create_task(_restart_binance_ws_after_timeout(23 * 60 * 60))

    except Exception:
        log.exception("❌ Ошибка при подключении к Binance WebSocket")


# 🔸 Продление listenKey каждые 30 минут
async def _keep_alive_binance_listen_key(listen_key: str):
    log = logging.getLogger("INFRA")
    api_key = os.getenv("BINANCE_API_KEY")
    url = "https://testnet.binancefuture.com/fapi/v1/listenKey"

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.put(
                    url,
                    headers={"X-MBX-APIKEY": api_key},
                    params={"listenKey": listen_key}
                ) as resp:
                    if resp.status == 200:
                        log.debug("🔄 Binance listenKey обновлён")
                    else:
                        text = await resp.text()
                        log.warning(f"⚠️ Ошибка продления listenKey: HTTP {resp.status} — {text}")
        except Exception as e:
            log.warning(f"⚠️ Не удалось обновить listenKey: {e}")
        await asyncio.sleep(30 * 60)


# 🔸 Переподключение к WebSocket после 23 часов
async def _restart_binance_ws_after_timeout(delay_seconds: int):
    log = logging.getLogger("INFRA")
    await asyncio.sleep(delay_seconds)

    try:
        log.info("♻️ Переподключение к Binance WebSocket (таймер 24ч)")

        # Закрываем старые соединения
        if infra.binance_ws_client is not None:
            await infra.binance_ws_client.close()
            infra.binance_ws_client = None

        if infra.binance_ws_session is not None:
            await infra.binance_ws_session.close()
            infra.binance_ws_session = None

        # Повторное подключение
        await setup_binance_ws_client()

    except Exception:
        log.exception("❌ Ошибка при переподключении к Binance WebSocket")