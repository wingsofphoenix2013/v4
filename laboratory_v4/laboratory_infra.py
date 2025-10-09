# laboratory_infra.py — инфраструктура laboratory_v4: логирование, PG/Redis, кэши конфигов и WL, шторки/локи (две версии WL)

# 🔸 Импорты
import os
import logging
from typing import Dict, List, Any, Tuple

import asyncpg
import redis.asyncio as aioredis
import asyncio

# 🔸 Константы
DEFAULT_VERSION = "v1"

# 🔸 Глобальные переменные подключений
pg_pool: asyncpg.Pool | None = None
redis_client: aioredis.Redis | None = None

# 🔸 Кэш конфигов (полный срез строк)
enabled_tickers: Dict[str, Dict[str, Any]] = {}       # {symbol -> row_dict (*все поля*)}
enabled_strategies: Dict[int, Dict[str, Any]] = {}    # {strategy_id -> row_dict (*все поля*)}

# 🔸 Кэши whitelist по стратегиям (версионные)
#     Структура: pack_wl_by_strategy_ver[sid][version] -> {"rows": [...], "meta": {...}}
#                mw_wl_by_strategy_ver[sid][version]   -> {"rows": [...], "meta": {...}}
pack_wl_by_strategy_ver: Dict[int, Dict[str, Dict[str, Any]]] = {}
mw_wl_by_strategy_ver: Dict[int, Dict[str, Dict[str, Any]]] = {}

# 🔸 Шторки (готовность данных) и локи (защита обновления) по (strategy_id, version)
_pack_ready_events: Dict[Tuple[int, str], asyncio.Event] = {}
_mw_ready_events: Dict[Tuple[int, str], asyncio.Event] = {}
_pack_update_locks: Dict[Tuple[int, str], asyncio.Lock] = {}
_mw_update_locks: Dict[Tuple[int, str], asyncio.Lock] = {}

# 🔸 Параметры окружения
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
REDIS_USE_TLS = os.getenv("REDIS_USE_TLS", "false").lower() == "true"

# 🔸 Логгер
log = logging.getLogger("LAB_INFRA")


# 🔸 Внутренние утилиты нормализации
def _norm_version(version: str | None) -> str:
    v = (version or DEFAULT_VERSION).strip().lower()
    return v or DEFAULT_VERSION


def _k(sid: int, version: str | None) -> Tuple[int, str]:
    return int(sid), _norm_version(version)


# 🔸 Настройка логирования
def setup_logging():
    level = logging.DEBUG if DEBUG_MODE else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log.debug("Логирование настроено (DEBUG_MODE=%s)", DEBUG_MODE)


# 🔸 Инициализация подключения к PostgreSQL
async def setup_pg():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("❌ DATABASE_URL не задан")

    pool = await asyncpg.create_pool(
        dsn=db_url,
        min_size=2,
        max_size=10,
        timeout=30.0,
    )
    # быстрый health-check
    async with pool.acquire() as conn:
        await conn.execute("SELECT 1")

    globals()["pg_pool"] = pool
    log.info("🛢️ Подключение к PostgreSQL установлено")


# 🔸 Инициализация подключения к Redis
async def setup_redis_client():
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", 6379))
    password = os.getenv("REDIS_PASSWORD")

    protocol = "rediss" if REDIS_USE_TLS else "redis"
    redis_url = f"{protocol}://{host}:{port}"

    client = aioredis.from_url(
        redis_url,
        password=password,
        decode_responses=True,  # строки на вход/выход
    )

    # health-check
    await client.ping()

    globals()["redis_client"] = client
    log.info("📡 Подключение к Redis установлено")


# 🔸 Атомарная замена кэша тикеров (полный срез; ключ — symbol)
def set_enabled_tickers(new_dict: Dict[str, Dict[str, Any]]):
    global enabled_tickers
    enabled_tickers = new_dict or {}
    log.info("✅ Кэш тикеров обновлён (всего: %d)", len(enabled_tickers))


# 🔸 Атомарная замена кэша стратегий (полный срез; ключ — id)
def set_enabled_strategies(new_dict: Dict[int, Dict[str, Any]]):
    global enabled_strategies
    enabled_strategies = new_dict or {}
    log.info("✅ Кэш стратегий (enabled=true) обновлён (всего: %d)", len(enabled_strategies))


# 🔸 Обеспечение наличия шторок и локов (PACK)
def _ensure_pack_sync_primitives(sid: int, version: str):
    k = _k(sid, version)
    if k not in _pack_ready_events:
        _pack_ready_events[k] = asyncio.Event()
        _pack_ready_events[k].set()  # по умолчанию «готово», пока не начнётся обновление
    if k not in _pack_update_locks:
        _pack_update_locks[k] = asyncio.Lock()


# 🔸 Обеспечение наличия шторок и локов (MW)
def _ensure_mw_sync_primitives(sid: int, version: str):
    k = _k(sid, version)
    if k not in _mw_ready_events:
        _mw_ready_events[k] = asyncio.Event()
        _mw_ready_events[k].set()
    if k not in _mw_update_locks:
        _mw_update_locks[k] = asyncio.Lock()


# 🔸 Старт/финиш обновления PACK для стратегии (шторка+лок)
async def start_pack_update(sid: int, version: str):
    _ensure_pack_sync_primitives(sid, version)
    k = _k(sid, version)
    # опускаем шторку — читатели будут ждать свежие данные
    _pack_ready_events[k].clear()
    await _pack_update_locks[k].acquire()
    log.info("🔧 PACK обновление начато (strategy_id=%s, ver=%s)", sid, _norm_version(version))


def finish_pack_update(sid: int, version: str):
    k = _k(sid, version)
    # атомарная публикация нового среза уже должна быть выполнена до вызова
    if k in _pack_update_locks and _pack_update_locks[k].locked():
        _pack_update_locks[k].release()
    if k in _pack_ready_events:
        _pack_ready_events[k].set()
    log.info("✅ PACK обновление завершено (strategy_id=%s, ver=%s)", sid, _norm_version(version))


# 🔸 Старт/финиш обновления MW для стратегии (шторка+лок)
async def start_mw_update(sid: int, version: str):
    _ensure_mw_sync_primitives(sid, version)
    k = _k(sid, version)
    _mw_ready_events[k].clear()
    await _mw_update_locks[k].acquire()
    log.info("🔧 MW обновление начато (strategy_id=%s, ver=%s)", sid, _norm_version(version))


def finish_mw_update(sid: int, version: str):
    k = _k(sid, version)
    if k in _mw_update_locks and _mw_update_locks[k].locked():
        _mw_update_locks[k].release()
    if k in _mw_ready_events:
        _mw_ready_events[k].set()
    log.info("✅ MW обновление завершено (strategy_id=%s, ver=%s)", sid, _norm_version(version))


# 🔸 Ожидание готовности данных (читатели) — PACK
async def wait_pack_ready(sid: int, version: str, timeout_sec: float | None = 5.0) -> bool:
    _ensure_pack_sync_primitives(sid, version)
    try:
        await asyncio.wait_for(_pack_ready_events[_k(sid, version)].wait(), timeout=timeout_sec)
        return True
    except asyncio.TimeoutError:
        log.info("⏳ PACK ожидание свежих данных истекло (strategy_id=%s, ver=%s, timeout=%.1fs)",
                 sid, _norm_version(version), timeout_sec or -1)
        return False


# 🔸 Ожидание готовности данных (читатели) — MW
async def wait_mw_ready(sid: int, version: str, timeout_sec: float | None = 5.0) -> bool:
    _ensure_mw_sync_primitives(sid, version)
    try:
        await asyncio.wait_for(_mw_ready_events[_k(sid, version)].wait(), timeout=timeout_sec)
        return True
    except asyncio.TimeoutError:
        log.info("⏳ MW ожидание свежих данных истекло (strategy_id=%s, ver=%s, timeout=%.1fs)",
                 sid, _norm_version(version), timeout_sec or -1)
        return False


# 🔸 Атомарная замена WL-среза (PACK) по стратегии и версии
def set_pack_whitelist_for_strategy(
    sid: int,
    rows: List[Dict[str, Any]] | None,
    meta: Dict[str, Any] | None,
    version: str,
):
    ver = _norm_version(version)
    d = pack_wl_by_strategy_ver.setdefault(int(sid), {})
    d[ver] = {
        "rows": rows or [],
        "meta": {**(meta or {}), "version": ver},
    }
    log.info("📦 PACK WL обновлён (strategy_id=%s, ver=%s, rows=%d)", sid, ver, len(d[ver]["rows"]))


# 🔸 Очистка WL-среза (PACK) по стратегии и версии
def clear_pack_whitelist_for_strategy(sid: int, version: str):
    ver = _norm_version(version)
    d = pack_wl_by_strategy_ver.setdefault(int(sid), {})
    d[ver] = {
        "rows": [],
        "meta": {"note": "cleared", "version": ver},
    }
    log.info("🧹 PACK WL очищён (strategy_id=%s, ver=%s)", sid, ver)


# 🔸 Атомарная замена WL-среза (MW) по стратегии и версии
def set_mw_whitelist_for_strategy(
    sid: int,
    rows: List[Dict[str, Any]] | None,
    meta: Dict[str, Any] | None,
    version: str,
):
    ver = _norm_version(version)
    d = mw_wl_by_strategy_ver.setdefault(int(sid), {})
    d[ver] = {
        "rows": rows or [],
        "meta": {**(meta or {}), "version": ver},
    }
    log.info("📦 MW WL обновлён (strategy_id=%s, ver=%s, rows=%d)", sid, ver, len(d[ver]["rows"]))


# 🔸 Очистка WL-среза (MW) по стратегии и версии
def clear_mw_whitelist_for_strategy(sid: int, version: str):
    ver = _norm_version(version)
    d = mw_wl_by_strategy_ver.setdefault(int(sid), {})
    d[ver] = {
        "rows": [],
        "meta": {"note": "cleared", "version": ver},
    }
    log.info("🧹 MW WL очищён (strategy_id=%s, ver=%s)", sid, ver)