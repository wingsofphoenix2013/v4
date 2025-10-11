# laboratory_config.py — стартовая загрузка кешей laboratory_v4 + подписчики Pub/Sub/Streams (тикеры, индикаторы, готовность свечей) + in-memory live cache

# 🔸 Импорты
import asyncio
import json
import logging
from datetime import datetime

# 🔸 Логгер
log = logging.getLogger("LAB_CONFIG")

# 🔸 Кеши модуля (in-memory, общий доступ)
lab_tickers: dict[str, dict] = {}                # symbol -> {"precision_price": int, "status": str, "tradepermission": str}
lab_indicators: dict[int, dict] = {}             # instance_id -> {"indicator": str, "timeframe": str, "stream_publish": bool, "enabled_at": datetime|None, "params": dict}
lab_last_bar: dict[tuple[str, str], int] = {}    # (symbol, tf) -> last open_time (ms)

# 🔸 In-memory live cache от IND-тика (на один бар для пары (symbol, tf))
# ключ = (symbol, tf, open_ms); значение: {"by_param": {param_name -> "строка-значение"}, "ts": float_monotonic}
live_cache: dict[tuple[str, str, int], dict] = {}


# 🔸 Вспомогательные геттеры кешей
def get_active_symbols() -> list[str]:
    return list(lab_tickers.keys())

def get_precision(symbol: str) -> int | None:
    info = lab_tickers.get(symbol)
    return int(info["precision_price"]) if info and info.get("precision_price") is not None else None

def get_instances_by_tf(tf: str) -> list[dict]:
    return [
        {
            "id": iid,
            "indicator": inst["indicator"],
            "timeframe": inst["timeframe"],
            "enabled_at": inst.get("enabled_at"),
            "params": inst.get("params", {}),
            "stream_publish": bool(inst.get("stream_publish", False)),
        }
        for iid, inst in lab_indicators.items()
        if inst.get("timeframe") == tf
    ]

def get_last_bar(symbol: str, tf: str) -> int | None:
    return lab_last_bar.get((symbol, tf))

def get_cache_stats() -> dict:
    return {
        "symbols": len(lab_tickers),
        "indicators": len(lab_indicators),
        "last_bars": len(lab_last_bar),
    }


# 🔸 Live-cache API (используется IND для записи, MW/другие — для чтения)
def put_live_values(symbol: str, tf: str, open_ms: int, values: dict[str, str]) -> None:
    """
    Обновить live-значения IND для (symbol, tf, open_ms).
    values — канонические ключи параметров (как в compute_snapshot_values), значения — строковые.
    """
    key = (symbol, tf, int(open_ms))
    entry = live_cache.get(key)
    if entry is None:
        entry = {"by_param": {}, "ts": asyncio.get_running_loop().time() if asyncio.get_running_loop() else 0.0}
        live_cache[key] = entry
    by_param = entry.get("by_param") or {}
    by_param.update(values)
    entry["by_param"] = by_param
    # метку времени обновим для простого GC
    try:
        entry["ts"] = asyncio.get_running_loop().time()
    except Exception:
        pass

def get_live_values(symbol: str, tf: str, open_ms: int) -> dict[str, str] | None:
    """
    Вернуть словарь by_param для (symbol, tf, open_ms) или None, если нет кеша.
    """
    entry = live_cache.get((symbol, tf, int(open_ms)))
    if not entry:
        return None
    return entry.get("by_param") or None

def gc_live_cache(max_age_sec: float = 30.0) -> int:
    """
    Простейший GC: удалить записи live_cache старше max_age_sec по ts.
    Возвращает число удалённых записей. Вызывать по желанию (не критично).
    """
    try:
        now = asyncio.get_running_loop().time()
    except Exception:
        # если нет loop (теоретически), ничего не делаем
        return 0
    removed = 0
    stale_keys = []
    for k, entry in live_cache.items():
        ts = entry.get("ts")
        if ts is None or (now - float(ts) > max_age_sec):
            stale_keys.append(k)
    for k in stale_keys:
        live_cache.pop(k, None)
        removed += 1
    if removed:
        log.debug("LIVE_CACHE GC: removed=%d", removed)
    return removed


# 🔸 Стартовая загрузка кешей (тикеры + индикаторы)
async def bootstrap_caches(pg, redis, tf_set: tuple[str, ...] = ("m5", "m15", "h1")):
    # загрузка активных тикеров
    async with pg.acquire() as conn:
        rows = await conn.fetch("""
            SELECT symbol, precision_price, status, tradepermission
            FROM tickers_bb
            WHERE status = 'enabled' AND tradepermission = 'enabled'
        """)
    added_tickers = 0
    for r in rows:
        sym = r["symbol"]
        lab_tickers[sym] = {
            "precision_price": int(r["precision_price"]) if r["precision_price"] is not None else None,
            "status": r["status"],
            "tradepermission": r["tradepermission"],
        }
        added_tickers += 1

    # загрузка инстансов индикаторов (+ параметры), только по заданным TF
    async with pg.acquire() as conn:
        inst_rows = await conn.fetch("""
            SELECT id, indicator, timeframe, stream_publish, enabled_at, enabled
            FROM indicator_instances_v4
            WHERE enabled = true
        """)
        for inst in inst_rows:
            tf = inst["timeframe"]
            if tf not in tf_set:
                continue
            iid = int(inst["id"])
            params_rows = await conn.fetch("""
                SELECT param, value
                FROM indicator_parameters_v4
                WHERE instance_id = $1
            """, iid)
            params = {p["param"]: p["value"] for p in params_rows}
            lab_indicators[iid] = {
                "indicator": inst["indicator"],
                "timeframe": tf,
                "stream_publish": bool(inst["stream_publish"]),
                "enabled_at": inst["enabled_at"],
                "params": params,
            }

    log.info("LAB INIT (bootstrap): tickers=%d indicators=%d", added_tickers, len(lab_indicators))


# 🔸 Помощники для обновлений кешей из БД
async def _reload_ticker(pg, symbol: str) -> bool:
    async with pg.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT symbol, precision_price, status, tradepermission
            FROM tickers_bb
            WHERE symbol = $1 AND status = 'enabled' AND tradepermission = 'enabled'
        """, symbol)
    if row:
        lab_tickers[symbol] = {
            "precision_price": int(row["precision_price"]) if row["precision_price"] is not None else None,
            "status": row["status"],
            "tradepermission": row["tradepermission"],
        }
        return True
    else:
        lab_tickers.pop(symbol, None)
        return False

async def _reload_indicator_instance(pg, iid: int, tf_set: tuple[str, ...]) -> bool:
    async with pg.acquire() as conn:
        inst = await conn.fetchrow("""
            SELECT id, indicator, timeframe, stream_publish, enabled_at, enabled
            FROM indicator_instances_v4
            WHERE id = $1
        """, iid)
        if not inst:
            lab_indicators.pop(iid, None)
            return False
        if not inst["enabled"] or inst["timeframe"] not in tf_set:
            lab_indicators.pop(iid, None)
            return False
        params_rows = await conn.fetch("""
            SELECT param, value
            FROM indicator_parameters_v4
            WHERE instance_id = $1
        """, iid)
        params = {p["param"]: p["value"] for p in params_rows}

    lab_indicators[iid] = {
        "indicator": inst["indicator"],
        "timeframe": inst["timeframe"],
        "stream_publish": bool(inst["stream_publish"]),
        "enabled_at": inst["enabled_at"],
        "params": params,
    }
    return True


# 🔸 Подписчик Pub/Sub: тикеры (tickers_v4_events)
async def run_watch_tickers_events(pg, redis, channel: str, initial_delay: float = 0.0):
    if initial_delay > 0:
        await asyncio.sleep(initial_delay)
    log.info("LAB TICKERS: подписка на канал %s", channel)

    pubsub = redis.pubsub()
    await pubsub.subscribe(channel)

    upd = rem = 0
    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue
        try:
            data = json.loads(msg["data"])
        except Exception:
            log.warning("LAB TICKERS: некорректный JSON: %r", msg.get("data"))
            continue

        sym = data.get("symbol")
        ev_type = data.get("type")
        action = data.get("action")
        if not sym or ev_type != "status" or action not in ("enabled", "disabled"):
            continue

        try:
            if action == "enabled":
                ok = await _reload_ticker(pg, sym)
                if ok:
                    upd += 1
            else:
                lab_tickers.pop(sym, None)
                rem += 1
        except Exception as e:
            log.warning("LAB TICKERS: ошибка обработки события %s: %s", sym, e)

        if (upd + rem) % 50 == 0:
            log.info("LAB TICKERS: updated=%d removed=%d active=%d", upd, rem, len(lab_tickers))


# 🔸 Подписчик Pub/Sub: индикаторы (indicators_v4_events)
async def run_watch_indicators_events(pg, redis, channel: str, initial_delay: float = 0.0, tf_set: tuple[str, ...] = ("m5","m15","h1")):
    if initial_delay > 0:
        await asyncio.sleep(initial_delay)
    log.info("LAB IND: подписка на канал %s", channel)

    pubsub = redis.pubsub()
    await pubsub.subscribe(channel)

    added = removed = updated = 0
    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue
        try:
            data = json.loads(msg["data"])
        except Exception:
            log.warning("LAB IND: некорректный JSON: %r", msg.get("data"))
            continue

        iid = data.get("id")
        field = data.get("type")
        action = data.get("action")
        if iid is None or field not in ("enabled", "stream_publish"):
            continue

        iid = int(iid)
        try:
            if field == "enabled":
                if action == "true":
                    ok = await _reload_indicator_instance(pg, iid, tf_set)
                    if ok:
                        added += 1
                else:
                    if lab_indicators.pop(iid, None) is not None:
                        removed += 1
            elif field == "stream_publish" and iid in lab_indicators:
                lab_indicators[iid]["stream_publish"] = (action == "true")
                updated += 1
        except Exception as e:
            log.warning("LAB IND: ошибка обработки %s: %s", iid, e)

        total = len(lab_indicators)
        if (added + removed + updated) % 50 == 0:
            log.info("LAB IND: added=%d removed=%d updated=%d total=%d", added, removed, updated, total)


# 🔸 Подписчик Pub/Sub: готовность свечей (bb:ohlcv_channel) → обновление lab_last_bar
async def run_watch_ohlcv_ready_channel(redis, channel: str, initial_delay: float = 0.0):
    if initial_delay > 0:
        await asyncio.sleep(initial_delay)
    log.info("LAB OHLCV (channel): подписка на канал %s", channel)

    pubsub = redis.pubsub()
    await pubsub.subscribe(channel)

    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue
        try:
            data = json.loads(msg["data"])
        except Exception:
            log.warning("LAB OHLCV: некорректный JSON: %r", msg.get("data"))
            continue

        symbol = data.get("symbol")
        interval = data.get("interval") or data.get("timeframe")
        ts = data.get("timestamp") or data.get("open_time_ms") or data.get("open_time")
        if not symbol or not interval or ts is None:
            continue

        try:
            open_ms = int(ts)
        except Exception:
            try:
                open_ms = int(datetime.fromisoformat(str(ts)).timestamp() * 1000)
            except Exception:
                continue

        lab_last_bar[(symbol, interval)] = open_ms
        open_iso = datetime.utcfromtimestamp(open_ms / 1000).isoformat()
        log.info("LAB OHLCV: set last_bar %s/%s@%s", symbol, interval, open_iso)