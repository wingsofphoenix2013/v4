# ema_snapshot_worker.py

import asyncio
import logging
import json
from datetime import datetime

import infra
from core_io import save_snapshot, get_snapshot_id, get_pattern_id


log = logging.getLogger("EMA_SNAPSHOT_WORKER")

VALID_EMAS = {"ema9", "ema21", "ema50", "ema100", "ema200"}
VALID_INTERVALS = {"m5", "m15", "h1"}

# Ожидания по (symbol, interval, open_time)
pending_snapshots = {}
snapshot_dict = {}

EPSILON = 0.0005  # 0.05%

# 🔸 Функция сортировки по периоду: EMA9 < EMA21 < ... < PRICE
def sort_key(x):
    if x == "PRICE":
        return 999
    return int(x.replace("EMA", ""))

# 🔸 Группировка значений с учётом слипания и стабильной сортировкой внутри групп
def group_by_proximity(items: list[tuple[str, float]], eps=EPSILON) -> list[str]:
    sorted_items = sorted(items, key=lambda x: -x[1])
    result = []
    group = [sorted_items[0][0]]
    ref_value = sorted_items[0][1]

    for name, value in sorted_items[1:]:
        delta = abs(value - ref_value) / max(value, ref_value)
        if delta < eps:
            group.append(name)
        else:
            result.append("=".join(sorted(group, key=sort_key)))
            group = [name]
            ref_value = value
    result.append("=".join(sorted(group, key=sort_key)))
    return result
    
# 🔸 Загрузка словаря снапшотов из БД
async def load_snapshot_dict():
    global snapshot_dict

    query = """
        SELECT id, ordering, pattern_id
        FROM oracle_emasnapshot_dict
    """

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(query)

    snapshot_dict = {
        row["ordering"]: (row["id"], row["pattern_id"])
        for row in rows
    }

    log.info(f"📚 Загружено вариантов снапшотов: {len(snapshot_dict)}")
    
# 🔸 Построение, логирование и сохранение snapshot с публикацией в Redis
async def build_snapshot(symbol: str, interval: str, open_time: str):
    redis = infra.redis_client

    try:
        target_dt = datetime.fromisoformat(open_time.replace("Z", ""))
        target_ts = int(target_dt.timestamp() * 1000)

        close_key = f"ts:{symbol}:{interval}:c"
        close_series = await redis.ts().range(close_key, target_ts, target_ts)
        if not close_series:
            log.warning(f"⚠️ Не найден close для {symbol} {interval} {open_time}")
            return

        close_value = float(close_series[0][1])
        items = [("PRICE", close_value)]

        for ema_name in ["ema9", "ema21", "ema50", "ema100", "ema200"]:
            ema_key = f"ts_ind:{symbol}:{interval}:{ema_name}"
            ema_series = await redis.ts().range(ema_key, target_ts, target_ts)
            if not ema_series:
                log.warning(f"⚠️ Не найден {ema_name} для {symbol} {interval} {open_time}")
                return
            ema_value = float(ema_series[0][1])
            items.append((ema_name.upper(), ema_value))

        log.debug(f"📋 Значения EMA и PRICE для {symbol} | {interval} | {open_time}:")
        for name, value in sorted(items, key=lambda x: -x[1]):
            log.debug(f"    • {name:<6} = {value}")

        ordered = group_by_proximity(items)
        snapshot_str = " > ".join(ordered)

        log.debug(f"📸 EMA SNAPSHOT: {symbol} | {interval} | {open_time}")
        log.debug(f"    ➤ {snapshot_str}")

        # 🔹 Поиск snapshot_id и pattern_id через словарь
        if snapshot_str not in snapshot_dict:
            log.warning(f"❌ Не найден snapshot в словаре: {snapshot_str}")
            return

        snapshot_id, pattern_id = snapshot_dict[snapshot_str]

        # 💾 Сохраняем в БД
        await save_snapshot(symbol, interval, open_time, snapshot_str)

        # 📡 Публикуем в Redis
        snapshot_key = f"snapshot:{symbol}:{interval}"
        ttl_by_interval = {
            "m5": 360,
            "m15": 960,
            "h1": 3720,
        }
        ttl = ttl_by_interval.get(interval, 360)

        await redis.set(snapshot_key, json.dumps({
            "snapshot_id": snapshot_id,
            "pattern_id": pattern_id
        }), ex=ttl)

        log.debug(f"📡 Redis {snapshot_key} = snapshot_id:{snapshot_id}, pattern_id:{pattern_id} (TTL={ttl})")

    except Exception as e:
        log.exception(f"❌ Ошибка при формировании snapshot: {symbol} | {interval} | {open_time} → {e}")
# 🔸 Обработка одного сообщения из Redis Stream
async def handle_ema_snapshot_message(message: dict):
    symbol = message.get("symbol")
    interval = message.get("timeframe")
    indicator = message.get("indicator")
    open_time = message.get("open_time")
    status = message.get("status")

    if not all([symbol, interval, indicator, open_time, status]):
        return

    if indicator not in VALID_EMAS:
        return
    if interval not in VALID_INTERVALS:
        return
    if status != "ready":
        return

    key = (symbol, interval, open_time)

    # Обновляем set полученных индикаторов
    if key not in pending_snapshots:
        pending_snapshots[key] = set()
    pending_snapshots[key].add(indicator)

    # Если собраны все 5
    if pending_snapshots[key] == VALID_EMAS:
        await build_snapshot(symbol, interval, open_time)
        del pending_snapshots[key]

# 🔸 Основной воркер
async def run_ema_snapshot_worker():
    redis = infra.redis_client
    stream_name = "indicator_stream"

    # 🔹 Загрузка словаря снапшотов из БД
    await load_snapshot_dict()

    try:
        stream_info = await redis.xinfo_stream(stream_name)
        last_id = stream_info["last-generated-id"]
    except Exception as e:
        log.warning(f"⚠️ Не удалось получить last ID из stream: {e}")
        last_id = "$"

    log.info(f"📡 Подписка на Redis Stream: {stream_name} (EMA SNAPSHOT) с last_id = {last_id}")

    while True:
        try:
            response = await redis.xread(
                streams={stream_name: last_id},
                count=50,
                block=1000
            )
            for stream, messages in response:
                for msg_id, msg_data in messages:
                    parsed = {k: v for k, v in msg_data.items()}
                    asyncio.create_task(handle_ema_snapshot_message(parsed))
                    last_id = msg_id
        except Exception:
            log.exception("❌ Ошибка чтения из indicator_stream")
            await asyncio.sleep(1)