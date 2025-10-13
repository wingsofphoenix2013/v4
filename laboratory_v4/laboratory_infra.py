# 🔸 laboratory_infra.py — инфраструктура laboratory_v4: логирование, PG/Redis, кэши конфигурации

# 🔸 Импорты
import os
import logging
import asyncpg
import redis.asyncio as aioredis
from typing import Dict, Set, Tuple

# 🔸 Глобальные подключения
pg_pool = None
redis_client = None

# 🔸 Кэши laboratory_v4
# тикеры: symbol -> row_dict
lab_tickers: Dict[str, dict] = {}
# стратегии: id -> row_dict
lab_strategies: Dict[int, dict] = {}

# MW whitelist: версия -> {(sid, timeframe, direction) -> {(agg_base, agg_state), ...}}
lab_mw_wl: Dict[str, Dict[Tuple[int, str, str], Set[Tuple[str, str]]]] = {"v1": {}, "v2": {}}

# PACK lists:
#   whitelist: версия -> {(sid, timeframe, direction) -> {(pack_base, agg_key, agg_value), ...}}
#   blacklist: версия -> {(sid, timeframe, direction) -> {(pack_base, agg_key, agg_value), ...}}
lab_pack_wl: Dict[str, Dict[Tuple[int, str, str], Set[Tuple[str, str, str]]]] = {"v1": {}, "v2": {}}
lab_pack_bl: Dict[str, Dict[Tuple[int, str, str], Set[Tuple[str, str, str]]]] = {"v1": {}, "v2": {}}

# 🔸 Переменные окружения
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"

# 🔸 Логгер
log = logging.getLogger("LAB_INFRA")


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
    # health-check
    async with pool.acquire() as conn:
        await conn.execute("SELECT 1")

    globals()["pg_pool"] = pool
    log.debug("🛢️ Подключение к PostgreSQL установлено")


# 🔸 Инициализация подключения к Redis
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
        decode_responses=True,  # строки на вход/выход
    )

    # health-check
    await client.ping()

    globals()["redis_client"] = client
    log.debug("📡 Подключение к Redis установлено")


# 🔸 Кэши: установки целиком

def set_lab_tickers(new_dict: Dict[str, dict]):
    """
    Полная замена кэша тикеров.
    """
    global lab_tickers
    lab_tickers = new_dict or {}
    log.debug("Кэш тикеров обновлён (%d)", len(lab_tickers))


def set_lab_strategies(new_dict: Dict[int, dict]):
    """
    Полная замена кэша стратегий.
    """
    global lab_strategies
    lab_strategies = new_dict or {}
    log.debug("Кэш стратегий обновлён (%d)", len(lab_strategies))


def replace_mw_whitelist(version: str, new_map: Dict[Tuple[int, str, str], Set[Tuple[str, str]]]):
    """
    Полная замена WL MW для указанной версии ('v1'|'v2').
    """
    v = str(version or "").lower()
    if v not in lab_mw_wl:
        lab_mw_wl[v] = {}
    lab_mw_wl[v] = new_map or {}
    log.debug("MW WL[%s] обновлён: срезов=%d", v, len(lab_mw_wl[v]))


def replace_pack_list(list_tag: str, version: str, new_map: Dict[Tuple[int, str, str], Set[Tuple[str, str, str]]]):
    """
    Полная замена PACK WL/BL для указанной версии ('v1'|'v2') и списка ('whitelist'|'blacklist').
    """
    v = str(version or "").lower()
    lt = str(list_tag or "").lower()
    if lt == "whitelist":
        if v not in lab_pack_wl:
            lab_pack_wl[v] = {}
        lab_pack_wl[v] = new_map or {}
        log.debug("PACK WL[%s] обновлён: срезов=%d", v, len(lab_pack_wl[v]))
    elif lt == "blacklist":
        if v not in lab_pack_bl:
            lab_pack_bl[v] = {}
        lab_pack_bl[v] = new_map or {}
        log.debug("PACK BL[%s] обновлён: срезов=%d", v, len(lab_pack_bl[v]))


# 🔸 Кэши: точечные обновления по стратегии

def update_mw_whitelist_for_strategy(version: str, strategy_id: int, slice_map: Dict[Tuple[str, str], Set[Tuple[str, str]]]):
    """
    Обновить MW WL для конкретной стратегии и версии:
      slice_map: {(timeframe, direction) -> {(agg_base, agg_state), ...}}
    """
    v = str(version or "").lower()
    if v not in lab_mw_wl:
        lab_mw_wl[v] = {}

    sid = int(strategy_id)

    # удаляем прежние ключи с этим strategy_id
    keys_to_del = [k for k in list(lab_mw_wl[v].keys()) if k[0] == sid]
    for k in keys_to_del:
        lab_mw_wl[v].pop(k, None)

    # добавляем новые срезы
    for (tf, direction), states in (slice_map or {}).items():
        lab_mw_wl[v][(sid, str(tf), str(direction))] = set(states or set())

    # метрики после обновления по sid
    total_slices = 0
    total_entries = 0
    per_tf_entries = {"m5": 0, "m15": 0, "h1": 0}
    for (k_sid, tf, _dir), states in lab_mw_wl[v].items():
        if k_sid != sid:
            continue
        total_slices += 1
        cnt = len(states)
        total_entries += cnt
        if tf in per_tf_entries:
            per_tf_entries[tf] += cnt

    # результатирующий лог по стратегии
    log.debug(
        "LAB: MW WL updated — sid=%s version=%s slices=%d entries=%d (m5=%d m15=%d h1=%d)",
        sid, v, total_slices, total_entries,
        per_tf_entries["m5"], per_tf_entries["m15"], per_tf_entries["h1"]
    )


def update_pack_list_for_strategy(list_tag: str, version: str, strategy_id: int, slice_map: Dict[Tuple[str, str], Set[Tuple[str, str, str]]]):
    """
    Обновить PACK WL/BL для конкретной стратегии и версии:
      slice_map: {(timeframe, direction) -> {(pack_base, agg_key, agg_value), ...}}
    """
    lt = str(list_tag or "").lower()
    v = str(version or "").lower()
    target = lab_pack_wl if lt == "whitelist" else lab_pack_bl
    if v not in target:
        target[v] = {}

    sid = int(strategy_id)

    # удаляем прежние ключи с этим strategy_id
    keys_to_del = [k for k in list(target[v].keys()) if k[0] == sid]
    for k in keys_to_del:
        target[v].pop(k, None)

    # добавляем новые срезы
    for (tf, direction), states in (slice_map or {}).items():
        target[v][(sid, str(tf), str(direction))] = set(states or set())

    # метрики после обновления по sid
    total_slices = 0
    total_entries = 0
    per_tf_entries = {"m5": 0, "m15": 0, "h1": 0}
    for (k_sid, tf, _dir), states in target[v].items():
        if k_sid != sid:
            continue
        total_slices += 1
        cnt = len(states)
        total_entries += cnt
        if tf in per_tf_entries:
            per_tf_entries[tf] += cnt

    # результатирующий лог по стратегии
    log.debug(
        "LAB: PACK %s updated — sid=%s version=%s slices=%d entries=%d (m5=%d m15=%d h1=%d)",
        lt.upper(), sid, v, total_slices, total_entries,
        per_tf_entries["m5"], per_tf_entries["m15"], per_tf_entries["h1"]
    )