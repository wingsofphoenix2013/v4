# 🔸 laboratory_infra.py — инфраструктура laboratory_v4: логирование, PG/Redis, кэши конфигурации (+winrate карты для WL/BL, активные BL-пороги)

# 🔸 Импорты
import os
import logging
import asyncpg
import redis.asyncio as aioredis
from typing import Dict, Set, Tuple, Optional, Any

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
# MW WL winrates: версия -> {(sid, tf, dir) -> {(agg_base, agg_state) -> wr}}
lab_mw_wl_wr: Dict[str, Dict[Tuple[int, str, str], Dict[Tuple[str, str], float]]] = {"v1": {}, "v2": {}}

# PACK lists:
#   whitelist: версия -> {(sid, tf, dir) -> {(pack_base, agg_key, agg_value), ...}}
#   blacklist: версия -> {(sid, tf, dir) -> {(pack_base, agg_key, agg_value), ...}}
lab_pack_wl: Dict[str, Dict[Tuple[int, str, str], Set[Tuple[str, str, str]]]] = {"v1": {}, "v2": {}}
lab_pack_bl: Dict[str, Dict[Tuple[int, str, str], Set[Tuple[str, str, str]]]] = {"v1": {}, "v2": {}}
# PACK WL/BL winrates: версия -> {(sid, tf, dir) -> {(pack_base, agg_key, agg_value) -> wr}}
lab_pack_wl_wr: Dict[str, Dict[Tuple[int, str, str], Dict[Tuple[str, str, str], float]]] = {"v1": {}, "v2": {}}
lab_pack_bl_wr: Dict[str, Dict[Tuple[int, str, str], Dict[Tuple[str, str, str], float]]] = {"v1": {}, "v2": {}}

# 🔸 Активные пороги BL (для быстрого применения фильтров без БД)
# ключ: (master_sid, version, decision_mode, direction, tf)
# значение: {"threshold": int, "best_roi": float, "roi_base": float, "positions_total": int,
#            "deposit_used": float, "computed_at": "ISO8601"}
lab_bl_active: Dict[Tuple[int, str, str, str, str], Dict[str, Any]] = {}

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
    log.info("🛢️ Подключение к PostgreSQL установлено")


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
    log.info("📡 Подключение к Redis установлено")


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


def replace_mw_whitelist(
    version: str,
    new_map: Dict[Tuple[int, str, str], Set[Tuple[str, str]]],
    wr_map: Optional[Dict[Tuple[int, str, str], Dict[Tuple[str, str], float]]] = None,
):
    """
    Полная замена WL MW для указанной версии ('v1'|'v2') + (опционально) карта winrate.
    """
    v = str(version or "").lower()
    if v not in lab_mw_wl:
        lab_mw_wl[v] = {}
    if v not in lab_mw_wl_wr:
        lab_mw_wl_wr[v] = {}

    lab_mw_wl[v] = new_map or {}
    if wr_map is not None:
        lab_mw_wl_wr[v] = wr_map or {}

    log.debug("MW WL[%s] обновлён: срезов=%d", v, len(lab_mw_wl[v]))


def replace_pack_list(
    list_tag: str,
    version: str,
    new_map: Dict[Tuple[int, str, str], Set[Tuple[str, str, str]]],
    wr_map: Optional[Dict[Tuple[int, str, str], Dict[Tuple[str, str, str], float]]] = None,
):
    """
    Полная замена PACK WL/BL для указанной версии ('v1'|'v2') и списка ('whitelist'|'blacklist') + (опционально) карта winrate.
    """
    v = str(version or "").lower()
    lt = str(list_tag or "").lower()

    target = lab_pack_wl if lt == "whitelist" else lab_pack_bl
    target_wr = lab_pack_wl_wr if lt == "whitelist" else lab_pack_bl_wr

    if v not in target:
        target[v] = {}
    if v not in target_wr:
        target_wr[v] = {}

    target[v] = new_map or {}
    if wr_map is not None:
        target_wr[v] = wr_map or {}

    log.debug("PACK %s[%s] обновлён: срезов=%d", lt.upper(), v, len(target[v]))


# 🔸 Кэши: точечные обновления по стратегии

def update_mw_whitelist_for_strategy(
    version: str,
    strategy_id: int,
    slice_map: Dict[Tuple[str, str], Set[Tuple[str, str]]],
    wr_map: Optional[Dict[Tuple[int, str, str], Dict[Tuple[str, str], float]]] = None,
):
    """
    Обновить MW WL для конкретной стратегии и версии:
      slice_map: {(timeframe, direction) -> {(agg_base, agg_state), ...}}
      wr_map: {(sid, timeframe, direction) -> {(agg_base, agg_state) -> winrate}}
    """
    v = str(version or "").lower()
    if v not in lab_mw_wl:
        lab_mw_wl[v] = {}
    if v not in lab_mw_wl_wr:
        lab_mw_wl_wr[v] = {}

    sid = int(strategy_id)

    # удаляем прежние ключи по sid
    for k in [k for k in list(lab_mw_wl[v].keys()) if k[0] == sid]:
        lab_mw_wl[v].pop(k, None)
    for k in [k for k in list(lab_mw_wl_wr[v].keys()) if k[0] == sid]:
        lab_mw_wl_wr[v].pop(k, None)

    # добавляем новые срезы
    for (tf, direction), states in (slice_map or {}).items():
        lab_mw_wl[v][(sid, str(tf), str(direction))] = set(states or set())

    # добавляем winrate-карты
    if wr_map:
        for sid_tf_dir, m in wr_map.items():
            lab_mw_wl_wr[v][sid_tf_dir] = dict(m or {})

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

    log.debug(
        "LAB: MW WL updated — sid=%s version=%s slices=%d entries=%d (m5=%d m15=%d h1=%d)",
        sid, v, total_slices, total_entries,
        per_tf_entries["m5"], per_tf_entries["m15"], per_tf_entries["h1"]
    )


def update_pack_list_for_strategy(
    list_tag: str,
    version: str,
    strategy_id: int,
    slice_map: Dict[Tuple[str, str], Set[Tuple[str, str, str]]],
    wr_map: Optional[Dict[Tuple[int, str, str], Dict[Tuple[str, str, str], float]]] = None,
):
    """
    Обновить PACK WL/BL для конкретной стратегии и версии:
      slice_map: {(timeframe, direction) -> {(pack_base, agg_key, agg_value), ...}}
      wr_map: {(sid, timeframe, direction) -> {(pack_base, agg_key, agg_value) -> winrate}}
    """
    lt = str(list_tag or "").lower()
    v = str(version or "").lower()

    target = lab_pack_wl if lt == "whitelist" else lab_pack_bl
    target_wr = lab_pack_wl_wr if lt == "whitelist" else lab_pack_bl_wr

    if v not in target:
        target[v] = {}
    if v not in target_wr:
        target_wr[v] = {}

    sid = int(strategy_id)

    # удаляем прежние ключи по sid
    for k in [k for k in list(target[v].keys()) if k[0] == sid]:
        target[v].pop(k, None)
    for k in [k for k in list(target_wr[v].keys()) if k[0] == sid]:
        target_wr[v].pop(k, None)

    # добавляем новые срезы
    for (tf, direction), states in (slice_map or {}).items():
        target[v][(sid, str(tf), str(direction))] = set(states or set())

    # добавляем winrate-карты
    if wr_map:
        for sid_tf_dir, m in wr_map.items():
            target_wr[v][sid_tf_dir] = dict(m or {})

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

    log.debug(
        "LAB: PACK %s updated — sid=%s version=%s slices=%d entries=%d (m5=%d m15=%d h1=%d)",
        lt.upper(), sid, v, total_slices, total_entries,
        per_tf_entries["m5"], per_tf_entries["m15"], per_tf_entries["h1"]
    )


# 🔸 BL Active: массовая установка, точечный upsert и быстрый доступ к порогу

def set_bl_active_bulk(new_map: Dict[Tuple[int, str, str, str, str], Dict[str, Any]]):
    """
    Полная замена in-memory кэша активных порогов BL.
    new_map: {(master_sid, version, decision_mode, direction, tf) -> {...см. lab_bl_active value...}}
    """
    global lab_bl_active
    lab_bl_active = new_map or {}
    log.info("LAB: BL active cache replaced (records=%d)", len(lab_bl_active))


def upsert_bl_active(
    master_sid: int,
    version: str,
    decision_mode: str,
    direction: str,
    tf: str,
    threshold: int,
    *,
    best_roi: float = 0.0,
    roi_base: float = 0.0,
    positions_total: int = 0,
    deposit_used: float = 0.0,
    computed_at: Optional[str] = None,
):
    """
    Точечное обновление записи активного порога BL в памяти (после UPSERT в БД).
    """
    key = (int(master_sid), str(version), str(decision_mode), str(direction), str(tf))
    rec = {
        "threshold": int(threshold),
        "best_roi": float(best_roi),
        "roi_base": float(roi_base),
        "positions_total": int(positions_total),
        "deposit_used": float(deposit_used),
        "computed_at": computed_at or "",
    }
    lab_bl_active[key] = rec
    log.debug("LAB: BL active upsert %s -> T=%s ROI=%.6f (base=%.6f, n=%d)", key, rec["threshold"], rec["best_roi"], rec["roi_base"], rec["positions_total"])


def get_bl_threshold(
    master_sid: int,
    version: str,
    decision_mode: str,
    direction: str,
    tf: str,
    default: int = 0,
) -> int:
    """
    Быстрый доступ к активному порогу BL. Если записи нет — возвращает default (по договорённости, 0).
    """
    key = (int(master_sid), str(version), str(decision_mode), str(direction), str(tf))
    rec = lab_bl_active.get(key)
    if not rec:
        return int(default)
    return int(rec.get("threshold", default))