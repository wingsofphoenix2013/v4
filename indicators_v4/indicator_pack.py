# indicator_pack.py — оркестратор расчёта и публикации обогащённых состояний (ind_pack)

# 🔸 Базовые импорты
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

# 🔸 Импорт pack-воркеров (предусмотрено расширение)
from packs.rsi_bin import RsiBinPack
from packs.mfi_bin import MfiBinPack
from packs.adx_bin import AdxBinPack
from packs.bb_band_bin import BbBandBinPack
from packs.lr_band_bin import LrBandBinPack
from packs.lr_angle_bin import LrAngleBinPack

# 🔸 Константы Redis
INDICATOR_STREAM = "indicator_stream"          # входной стрим готовности индикаторов
IND_PACK_PREFIX = "ind_pack"                   # префикс ключей результата
IND_PACK_GROUP = "ind_pack_group_v4"           # consumer-group для indicator_stream
IND_PACK_CONSUMER = "ind_pack_consumer_1"      # consumer name

# 🔸 Константы стрима обновления adaptive-словаря
POSTPROC_STREAM_KEY = "bt:analysis:postproc_ready"
POSTPROC_GROUP = "ind_pack_postproc_group_v4"
POSTPROC_CONSUMER = "ind_pack_postproc_1"

# 🔸 Константы Redis TS (feed_bb + indicators_v4)
BB_TS_PREFIX = "bb:ts"                         # bb:ts:{symbol}:{tf}:{field}
IND_TS_PREFIX = "ts_ind"                       # ts_ind:{symbol}:{tf}:{param_name}

# 🔸 Константы БД
PACK_INSTANCES_TABLE = "indicator_pack_instances_v4"
ANALYSIS_INSTANCES_TABLE = "bt_analysis_instances"
ANALYSIS_PARAMETERS_TABLE = "bt_analysis_parameters"
BINS_DICT_TABLE = "bt_analysis_bins_dict"
ADAPTIVE_BINS_TABLE = "bt_analysis_bin_dict_adaptive"
BB_TICKERS_TABLE = "tickers_bb"

# 🔸 Параметры чтения и параллельной обработки stream
STREAM_READ_COUNT = 500          # сколько сообщений читать за раз
STREAM_BLOCK_MS = 2000           # блокировка XREADGROUP (мс)
MAX_PARALLEL_MESSAGES = 200      # сколько сообщений обрабатывать параллельно

# 🔸 Параметры холодного старта (bootstrap)
BOOTSTRAP_MAX_PARALLEL = 300     # сколько тикеров/паков обрабатывать параллельно при старте

# 🔸 TTL по TF
TTL_BY_TF_SEC = {
    "m5": 120,      # 2 минуты
    "m15": 960,     # 16 минут
    "h1": 3660,     # 61 минута
}

# 🔸 Реестр доступных pack-воркеров (key берём из bt_analysis_instances.key)
PACK_WORKERS = {
    "rsi_bin": RsiBinPack,
    "mfi_bin": MfiBinPack,
    "adx_bin": AdxBinPack,
    "bb_band_bin": BbBandBinPack,
    "lr_band_bin": LrBandBinPack,
    "lr_angle_bin": LrAngleBinPack,
}

# 🔸 Глобальный реестр pack-инстансов, готовых к работе
pack_registry: dict[tuple[str, str], list["PackRuntime"]] = {}
# key: (timeframe, indicator_from_stream) -> list[PackRuntime]

# 🔸 Кеш adaptive-словаря: (analysis_id, scenario_id, signal_id, tf, direction) -> [BinRule...]
adaptive_bins_cache: dict[tuple[int, int, int, str, str], list["BinRule"]] = {}

# 🔸 Индекс используемых пар (scenario_id, signal_id) -> set(analysis_id)
adaptive_pairs_index: dict[tuple[int, int], set[int]] = {}

# 🔸 Быстрый set для проверки "интересна ли пара" в стриме postproc_ready
adaptive_pairs_set: set[tuple[int, int]] = set()

# 🔸 Лок для обновления adaptive-кеша
adaptive_lock = asyncio.Lock()


@dataclass(frozen=True)
class BinRule:
    direction: str
    timeframe: str
    bin_type: str
    bin_order: int
    bin_name: str
    val_from: float | None
    val_to: float | None
    to_inclusive: bool


@dataclass
class PackRuntime:
    analysis_id: int
    analysis_key: str
    analysis_name: str
    family_key: str
    timeframe: str
    source_param_name: str
    bins_policy: dict[str, Any] | None
    bins_source: str                       # "static" | "adaptive"
    adaptive_pairs: list[tuple[int, int]]  # [(scenario_id, signal_id), ...] если adaptive
    bins_by_direction: dict[str, list[BinRule]]  # используется для static
    ttl_sec: int
    worker: Any


# 🔸 Определение источника бинов из bins_policy
def get_bins_source(bins_policy: dict[str, Any] | None, timeframe: str) -> str:
    # дефолт — static
    if not bins_policy:
        return "static"

    try:
        # форма 1) {"default":"static","by_tf":{"m5":"adaptive",...}}
        if "by_tf" in bins_policy:
            by_tf = bins_policy.get("by_tf") or {}
            return str(by_tf.get(timeframe) or bins_policy.get("default") or "static")

        # форма 2) {"default":"adaptive"} или {"m5":"adaptive",...}
        return str(bins_policy.get(timeframe) or bins_policy.get("default") or "static")
    except Exception:
        return "static"


# 🔸 Разбор списка пар для adaptive из bins_policy
def get_adaptive_pairs(bins_policy: dict[str, Any] | None) -> list[tuple[int, int]]:
    if not isinstance(bins_policy, dict):
        return []

    raw = bins_policy.get("pairs")
    if not isinstance(raw, list):
        return []

    out: list[tuple[int, int]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        try:
            scenario_id = int(item.get("scenario_id"))
            signal_id = int(item.get("signal_id"))
            out.append((scenario_id, signal_id))
        except Exception:
            continue

    # уникализация, сохраняя порядок
    seen = set()
    uniq: list[tuple[int, int]] = []
    for p in out:
        if p in seen:
            continue
        seen.add(p)
        uniq.append(p)
    return uniq


# 🔸 Приведение param_name к indicator_stream.indicator (base)
def get_stream_indicator_key(family_key: str, param_name: str) -> str:
    # если нет '_' — совпадает как есть
    if "_" not in param_name:
        return param_name

    # adx_dmi{len}_adx -> adx_dmi{len}
    if family_key == "adx_dmi":
        return param_name.rsplit("_", 1)[0]

    # bb20_2_0_upper -> bb20, macd12_macd_hist -> macd12, lr50_angle -> lr50, supertrend10_3_0_trend -> supertrend10
    return param_name.split("_", 1)[0]


# 🔸 Парсинг open_time ISO (UTC-naive) -> ts_ms
def parse_open_time_to_ts_ms(open_time: str | None) -> int | None:
    if not open_time:
        return None
    try:
        dt = datetime.fromisoformat(str(open_time))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


# 🔸 Redis TS helpers
async def ts_get(redis, key: str) -> tuple[int, str] | None:
    try:
        res = await redis.execute_command("TS.GET", key)
        if not res:
            return None
        ts_ms, value = res
        return int(ts_ms), str(value)
    except Exception:
        return None


async def ts_get_value_at(redis, key: str, ts_ms: int) -> str | None:
    try:
        res = await redis.execute_command("TS.RANGE", key, int(ts_ms), int(ts_ms))
        if not res:
            return None
        _, value = res[-1]
        return str(value)
    except Exception:
        return None


# 🔸 Загрузка включённых pack-инстансов
async def load_enabled_packs(pg) -> list[dict[str, Any]]:
    log = logging.getLogger("PACK_INIT")

    async with pg.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT id, analysis_id, enabled, bins_policy, enabled_at
            FROM {PACK_INSTANCES_TABLE}
            WHERE enabled = true
        """)

    packs: list[dict[str, Any]] = []
    for r in rows:
        packs.append({
            "id": int(r["id"]),
            "analysis_id": int(r["analysis_id"]),
            "bins_policy": r["bins_policy"],  # jsonb -> dict (asyncpg)
            "enabled_at": r["enabled_at"],
        })

    log.debug(f"PACK_INIT: включённых pack-инстансов загружено: {len(packs)}")
    return packs


# 🔸 Загрузка метаданных анализаторов
async def load_analysis_instances(pg, analysis_ids: list[int]) -> dict[int, dict[str, Any]]:
    log = logging.getLogger("PACK_INIT")
    if not analysis_ids:
        return {}

    async with pg.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT id, family_key, "key", "name", enabled
            FROM {ANALYSIS_INSTANCES_TABLE}
            WHERE id = ANY($1::int[])
        """, analysis_ids)

    out: dict[int, dict[str, Any]] = {}
    for r in rows:
        out[int(r["id"])] = {
            "family_key": str(r["family_key"]),
            "key": str(r["key"]),
            "name": str(r["name"]),
            "enabled": bool(r["enabled"]),
        }

    log.debug(f"PACK_INIT: analysis-инстансов загружено: {len(out)}")
    return out


# 🔸 Загрузка параметров анализаторов (нужны tf и param_name)
async def load_analysis_parameters(pg, analysis_ids: list[int]) -> dict[int, dict[str, str]]:
    log = logging.getLogger("PACK_INIT")
    if not analysis_ids:
        return {}

    async with pg.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT analysis_id, param_name, param_value
            FROM {ANALYSIS_PARAMETERS_TABLE}
            WHERE analysis_id = ANY($1::int[])
        """, analysis_ids)

    params: dict[int, dict[str, str]] = {}
    for r in rows:
        aid = int(r["analysis_id"])
        pname = str(r["param_name"])
        pval = str(r["param_value"])
        params.setdefault(aid, {})[pname] = pval

    log.debug(f"PACK_INIT: параметров анализаторов загружено: {len(params)}")
    return params


# 🔸 Загрузка статичного словаря бинов (bt_analysis_bins_dict)
async def load_static_bins_dict(pg, analysis_ids: list[int]) -> dict[int, dict[str, dict[str, list[BinRule]]]]:
    log = logging.getLogger("PACK_INIT")
    if not analysis_ids:
        return {}

    async with pg.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT analysis_id, direction, timeframe, bin_type, bin_order, bin_name,
                   val_from, val_to, to_inclusive
            FROM {BINS_DICT_TABLE}
            WHERE analysis_id = ANY($1::int[])
              AND bin_type = 'bins'
        """, analysis_ids)

    out: dict[int, dict[str, dict[str, list[BinRule]]]] = {}
    for r in rows:
        aid = int(r["analysis_id"])
        direction = str(r["direction"])
        timeframe = str(r["timeframe"])
        rule = BinRule(
            direction=direction,
            timeframe=timeframe,
            bin_type=str(r["bin_type"]),
            bin_order=int(r["bin_order"]),
            bin_name=str(r["bin_name"]),
            val_from=float(r["val_from"]) if r["val_from"] is not None else None,
            val_to=float(r["val_to"]) if r["val_to"] is not None else None,
            to_inclusive=bool(r["to_inclusive"]),
        )
        out.setdefault(aid, {}).setdefault(timeframe, {}).setdefault(direction, []).append(rule)

    for aid in out:
        for tf in out[aid]:
            for direction in out[aid][tf]:
                out[aid][tf][direction].sort(key=lambda x: x.bin_order)

    log.debug("PACK_INIT: static bins dict загружен")
    return out


# 🔸 Загрузка adaptive-словаря для одной пары (scenario_id, signal_id)
async def load_adaptive_bins_for_pair(pg, analysis_ids: list[int], scenario_id: int, signal_id: int) -> dict[tuple[int, str, str], list[BinRule]]:
    # возвращает: (analysis_id, timeframe, direction) -> rules[]
    if not analysis_ids:
        return {}

    async with pg.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT analysis_id, direction, timeframe, bin_type, bin_order, bin_name,
                   val_from, val_to, to_inclusive
            FROM {ADAPTIVE_BINS_TABLE}
            WHERE analysis_id = ANY($1::int[])
              AND scenario_id = $2
              AND signal_id   = $3
              AND bin_type    = 'bins'
            ORDER BY analysis_id, timeframe, direction, bin_order
        """, analysis_ids, scenario_id, signal_id)

    out: dict[tuple[int, str, str], list[BinRule]] = {}
    for r in rows:
        aid = int(r["analysis_id"])
        tf = str(r["timeframe"])
        direction = str(r["direction"])
        rule = BinRule(
            direction=direction,
            timeframe=tf,
            bin_type=str(r["bin_type"]),
            bin_order=int(r["bin_order"]),
            bin_name=str(r["bin_name"]),
            val_from=float(r["val_from"]) if r["val_from"] is not None else None,
            val_to=float(r["val_to"]) if r["val_to"] is not None else None,
            to_inclusive=bool(r["to_inclusive"]),
        )
        out.setdefault((aid, tf, direction), []).append(rule)

    for k in out:
        out[k].sort(key=lambda x: x.bin_order)

    return out


# 🔸 Построение реестра pack-воркеров
def build_pack_registry(
    packs: list[dict[str, Any]],
    analysis_meta: dict[int, dict[str, Any]],
    analysis_params: dict[int, dict[str, str]],
    static_bins_dict: dict[int, dict[str, dict[str, list[BinRule]]]],
) -> dict[tuple[str, str], list[PackRuntime]]:
    log = logging.getLogger("PACK_INIT")

    registry: dict[tuple[str, str], list[PackRuntime]] = {}

    for pack in packs:
        analysis_id = int(pack["analysis_id"])
        meta = analysis_meta.get(analysis_id)
        params = analysis_params.get(analysis_id, {})

        if not meta or not bool(meta.get("enabled", True)):
            continue

        analysis_key = str(meta["key"])
        analysis_name = str(meta["name"])
        family_key = str(meta["family_key"])

        timeframe = params.get("tf")
        source_param_name = params.get("param_name")
        if not timeframe or not source_param_name:
            continue

        bins_policy = pack.get("bins_policy")
        bins_source = get_bins_source(bins_policy, timeframe)

        worker_cls = PACK_WORKERS.get(analysis_key)
        if worker_cls is None:
            log.warning(f"PACK_INIT: analysis_id={analysis_id} пропущен: воркер для key='{analysis_key}' не найден")
            continue

        ttl_sec = int(TTL_BY_TF_SEC.get(timeframe, 60))

        adaptive_pairs: list[tuple[int, int]] = []
        bins_by_direction: dict[str, list[BinRule]] = {"long": [], "short": []}

        if bins_source == "adaptive":
            adaptive_pairs = get_adaptive_pairs(bins_policy)
            if not adaptive_pairs:
                log.warning(f"PACK_INIT: analysis_id={analysis_id} ({analysis_key}) bins_source=adaptive, но pairs пустой — пропущен")
                continue

        else:
            bins_tf = static_bins_dict.get(analysis_id, {}).get(timeframe, {})
            bins_by_direction = {
                "long": bins_tf.get("long", []),
                "short": bins_tf.get("short", []),
            }

        runtime = PackRuntime(
            analysis_id=analysis_id,
            analysis_key=analysis_key,
            analysis_name=analysis_name,
            family_key=family_key,
            timeframe=timeframe,
            source_param_name=source_param_name,
            bins_policy=bins_policy,
            bins_source=bins_source,
            adaptive_pairs=adaptive_pairs,
            bins_by_direction=bins_by_direction,
            ttl_sec=ttl_sec,
            worker=worker_cls(),
        )

        stream_indicator = get_stream_indicator_key(family_key, source_param_name)
        registry.setdefault((timeframe, stream_indicator), []).append(runtime)

    log.debug(f"PACK_INIT: registry построен — match_keys={len(registry)}")
    return registry


# 🔸 Публикация ключей результата
async def publish_pack_state_static(redis, analysis_id: int, direction: str, symbol: str, timeframe: str, bin_name: str, ttl_sec: int):
    key = f"{IND_PACK_PREFIX}:{analysis_id}:{direction}:{symbol}:{timeframe}"
    await redis.set(key, bin_name, ex=ttl_sec)


async def publish_pack_state_adaptive(redis, analysis_id: int, scenario_id: int, signal_id: int, direction: str, symbol: str, timeframe: str, bin_name: str, ttl_sec: int):
    key = f"{IND_PACK_PREFIX}:{analysis_id}:{scenario_id}:{signal_id}:{direction}:{symbol}:{timeframe}"
    await redis.set(key, bin_name, ex=ttl_sec)


# 🔸 Сбор value для BB bands (upper/lower из indicators KV, close из feed TS)
async def build_bb_band_value(redis, symbol: str, timeframe: str, bb_prefix: str, ts_ms: int | None) -> dict[str, str] | None:
    upper_key = f"ind:{symbol}:{timeframe}:{bb_prefix}_upper"
    lower_key = f"ind:{symbol}:{timeframe}:{bb_prefix}_lower"

    upper_val = await redis.get(upper_key)
    lower_val = await redis.get(lower_key)
    if upper_val is None or lower_val is None:
        return None

    if ts_ms is None:
        return None

    close_key = f"{BB_TS_PREFIX}:{symbol}:{timeframe}:c"
    close_val = await ts_get_value_at(redis, close_key, ts_ms)
    if close_val is None:
        return None

    return {"price": close_val, "upper": upper_val, "lower": lower_val}


# 🔸 Сбор value для LR bands (upper/lower из indicators KV, close из feed TS)
async def build_lr_band_value(redis, symbol: str, timeframe: str, lr_prefix: str, ts_ms: int | None) -> dict[str, str] | None:
    upper_key = f"ind:{symbol}:{timeframe}:{lr_prefix}_upper"
    lower_key = f"ind:{symbol}:{timeframe}:{lr_prefix}_lower"

    upper_val = await redis.get(upper_key)
    lower_val = await redis.get(lower_key)
    if upper_val is None or lower_val is None:
        return None

    if ts_ms is None:
        return None

    close_key = f"{BB_TS_PREFIX}:{symbol}:{timeframe}:c"
    close_val = await ts_get_value_at(redis, close_key, ts_ms)
    if close_val is None:
        return None

    return {"price": close_val, "upper": upper_val, "lower": lower_val}


# 🔸 Получение adaptive-правил из кеша
async def get_adaptive_rules(analysis_id: int, scenario_id: int, signal_id: int, timeframe: str, direction: str) -> list[BinRule]:
    async with adaptive_lock:
        return adaptive_bins_cache.get((analysis_id, scenario_id, signal_id, timeframe, direction), [])


# 🔸 Обработка одного события indicator_stream (status=ready)
async def handle_indicator_ready(redis, msg: dict[str, str]) -> None:
    log = logging.getLogger("PACK_SET")

    symbol = msg.get("symbol")
    timeframe = msg.get("timeframe")
    indicator_key = msg.get("indicator")
    status = msg.get("status")
    open_time = msg.get("open_time")

    # условия достаточности
    if status != "ready" or not symbol or not timeframe or not indicator_key:
        return

    runtimes = pack_registry.get((timeframe, indicator_key))
    if not runtimes:
        return

    ts_ms = parse_open_time_to_ts_ms(open_time)

    for rt in runtimes:
        # value для воркера
        if rt.analysis_key == "bb_band_bin":
            value = await build_bb_band_value(redis, symbol, rt.timeframe, rt.source_param_name, ts_ms)
            if value is None:
                continue
        elif rt.analysis_key == "lr_band_bin":
            value = await build_lr_band_value(redis, symbol, rt.timeframe, rt.source_param_name, ts_ms)
            if value is None:
                continue
        else:
            raw_key = f"ind:{symbol}:{rt.timeframe}:{rt.source_param_name}"
            raw_value = await redis.get(raw_key)
            if raw_value is None:
                continue
            try:
                value = float(raw_value)
            except Exception:
                continue

        # static vs adaptive
        if rt.bins_source == "adaptive":
            for (scenario_id, signal_id) in rt.adaptive_pairs:
                for direction in ("long", "short"):
                    rules = await get_adaptive_rules(rt.analysis_id, scenario_id, signal_id, rt.timeframe, direction)
                    if not rules:
                        continue

                    bin_name = rt.worker.bin_value(value=value, rules=rules)
                    if not bin_name:
                        continue

                    await publish_pack_state_adaptive(
                        redis=redis,
                        analysis_id=rt.analysis_id,
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                        direction=direction,
                        symbol=symbol,
                        timeframe=rt.timeframe,
                        bin_name=bin_name,
                        ttl_sec=rt.ttl_sec,
                    )

                    log.debug(
                        f"analysis_id={rt.analysis_id} scenario_id={scenario_id} signal_id={signal_id} "
                        f"symbol={symbol} tf={rt.timeframe} direction={direction} "
                        f"bin_name={bin_name} open_time={open_time} ttl={rt.ttl_sec}"
                    )

        else:
            publish_tasks = []
            published_items: list[tuple[str, str]] = []

            for direction in ("long", "short"):
                rules = rt.bins_by_direction.get(direction) or []
                if not rules:
                    continue

                bin_name = rt.worker.bin_value(value=value, rules=rules)
                if not bin_name:
                    continue

                publish_tasks.append(
                    publish_pack_state_static(
                        redis=redis,
                        analysis_id=rt.analysis_id,
                        direction=direction,
                        symbol=symbol,
                        timeframe=rt.timeframe,
                        bin_name=bin_name,
                        ttl_sec=rt.ttl_sec,
                    )
                )
                published_items.append((direction, bin_name))

            if publish_tasks:
                await asyncio.gather(*publish_tasks, return_exceptions=True)
                for direction, bin_name in published_items:
                    log.debug(
                        f"analysis_id={rt.analysis_id} symbol={symbol} tf={rt.timeframe} "
                        f"direction={direction} bin_name={bin_name} open_time={open_time} ttl={rt.ttl_sec}"
                    )


# 🔸 Создание consumer-group (общий хелпер)
async def ensure_stream_group(redis, stream: str, group: str):
    log = logging.getLogger("PACK_STREAM")
    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create error for {stream}/{group}: {e}")


# 🔸 Подписка на indicator_stream (параллельно)
async def watch_indicator_stream(redis):
    log = logging.getLogger("PACK_STREAM")

    sem = asyncio.Semaphore(MAX_PARALLEL_MESSAGES)

    async def _process_one(data: dict) -> None:
        async with sem:
            msg = {
                "symbol": data.get("symbol"),
                "timeframe": data.get("timeframe"),
                "indicator": data.get("indicator"),
                "open_time": data.get("open_time"),
                "status": data.get("status"),
            }
            await handle_indicator_ready(redis, msg)

    while True:
        try:
            resp = await redis.xreadgroup(
                IND_PACK_GROUP,
                IND_PACK_CONSUMER,
                streams={INDICATOR_STREAM: ">"},
                count=STREAM_READ_COUNT,
                block=STREAM_BLOCK_MS,
            )

            if not resp:
                continue

            flat: list[tuple[str, dict]] = []
            for _, messages in resp:
                for msg_id, data in messages:
                    flat.append((msg_id, data))

            if not flat:
                continue

            to_ack = [msg_id for msg_id, _ in flat]

            tasks = [asyncio.create_task(_process_one(data)) for _, data in flat]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for r in results:
                if isinstance(r, Exception):
                    log.warning(f"PACK_STREAM: message processing error: {r}", exc_info=True)

            await redis.xack(INDICATOR_STREAM, IND_PACK_GROUP, *to_ack)

        except Exception as e:
            log.error(f"PACK_STREAM loop error: {e}", exc_info=True)
            await asyncio.sleep(2)


# 🔸 Подписка на bt:analysis:postproc_ready и точечный reload adaptive-cache
async def watch_postproc_ready(pg, redis):
    log = logging.getLogger("PACK_ADAPTIVE")

    sem = asyncio.Semaphore(50)

    async def _reload_pair(scenario_id: int, signal_id: int):
        async with sem:
            pair = (scenario_id, signal_id)
            analysis_ids = sorted(list(adaptive_pairs_index.get(pair, set())))
            if not analysis_ids:
                return

            loaded = await load_adaptive_bins_for_pair(pg, analysis_ids, scenario_id, signal_id)

            async with adaptive_lock:
                # очистить старые значения для пары (только нужные analysis_id)
                keys_to_del = [
                    k for k in adaptive_bins_cache.keys()
                    if k[1] == scenario_id and k[2] == signal_id and k[0] in analysis_ids
                ]
                for k in keys_to_del:
                    adaptive_bins_cache.pop(k, None)

                # записать новые
                for (aid, tf, direction), rules in loaded.items():
                    adaptive_bins_cache[(aid, scenario_id, signal_id, tf, direction)] = rules

            log.info(f"PACK_ADAPTIVE: обновлён adaptive dict для scenario_id={scenario_id}, signal_id={signal_id}, analysis_ids={analysis_ids}")

    while True:
        try:
            resp = await redis.xreadgroup(
                POSTPROC_GROUP,
                POSTPROC_CONSUMER,
                streams={POSTPROC_STREAM_KEY: ">"},
                count=200,
                block=2000,
            )

            if not resp:
                continue

            to_ack = []

            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)

                    try:
                        scenario_id = int(data.get("scenario_id"))
                        signal_id = int(data.get("signal_id"))
                    except Exception:
                        continue

                    pair = (scenario_id, signal_id)

                    # если пара не используется в live — просто игнор
                    if pair not in adaptive_pairs_set:
                        continue

                    # перезагрузка только для интересной пары
                    asyncio.create_task(_reload_pair(scenario_id, signal_id))

            if to_ack:
                await redis.xack(POSTPROC_STREAM_KEY, POSTPROC_GROUP, *to_ack)

        except Exception as e:
            log.error(f"PACK_ADAPTIVE loop error: {e}", exc_info=True)
            await asyncio.sleep(2)


# 🔸 Загрузка активных тикеров (для bootstrap)
async def load_active_symbols(pg) -> list[str]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT symbol
            FROM {BB_TICKERS_TABLE}
            WHERE status = 'enabled' AND tradepermission = 'enabled'
        """)

    symbols: list[str] = []
    for r in rows:
        sym = r["symbol"]
        if sym:
            symbols.append(str(sym))
    return symbols


# 🔸 Холодный старт: пересчитать текущее состояние (без ожидания next ready)
async def bootstrap_current_state(pg, redis):
    log = logging.getLogger("PACK_BOOT")

    runtimes: list[PackRuntime] = []
    for lst in pack_registry.values():
        runtimes.extend(lst)

    if not runtimes:
        log.debug("PACK_BOOT: нет активных pack-инстансов, bootstrap пропущен")
        return

    symbols = await load_active_symbols(pg)
    if not symbols:
        log.debug("PACK_BOOT: нет активных тикеров, bootstrap пропущен")
        return

    sem = asyncio.Semaphore(BOOTSTRAP_MAX_PARALLEL)

    async def _process_one(symbol: str, rt: PackRuntime):
        async with sem:
            # value для воркера
            if rt.analysis_key in ("bb_band_bin", "lr_band_bin"):
                prefix = rt.source_param_name

                upper_ts_key = f"{IND_TS_PREFIX}:{symbol}:{rt.timeframe}:{prefix}_upper"
                upper = await ts_get(redis, upper_ts_key)
                if not upper:
                    return
                ts_ms, upper_val = upper

                lower_ts_key = f"{IND_TS_PREFIX}:{symbol}:{rt.timeframe}:{prefix}_lower"
                lower = await ts_get(redis, lower_ts_key)
                if not lower:
                    return

                lower_ts, lower_val = lower
                if lower_ts != ts_ms:
                    lower_at = await ts_get_value_at(redis, lower_ts_key, ts_ms)
                    if lower_at is None:
                        return
                    lower_val = lower_at

                close_key = f"{BB_TS_PREFIX}:{symbol}:{rt.timeframe}:c"
                close_val = await ts_get_value_at(redis, close_key, ts_ms)
                if close_val is None:
                    return

                value: Any = {"price": close_val, "upper": upper_val, "lower": lower_val}

            else:
                raw_key = f"ind:{symbol}:{rt.timeframe}:{rt.source_param_name}"
                raw_value = await redis.get(raw_key)
                if raw_value is None:
                    return
                try:
                    value = float(raw_value)
                except Exception:
                    return

            # публикация
            if rt.bins_source == "adaptive":
                for (scenario_id, signal_id) in rt.adaptive_pairs:
                    for direction in ("long", "short"):
                        rules = await get_adaptive_rules(rt.analysis_id, scenario_id, signal_id, rt.timeframe, direction)
                        if not rules:
                            continue

                        bin_name = rt.worker.bin_value(value=value, rules=rules)
                        if not bin_name:
                            continue

                        await publish_pack_state_adaptive(
                            redis=redis,
                            analysis_id=rt.analysis_id,
                            scenario_id=scenario_id,
                            signal_id=signal_id,
                            direction=direction,
                            symbol=symbol,
                            timeframe=rt.timeframe,
                            bin_name=bin_name,
                            ttl_sec=rt.ttl_sec,
                        )

                        log.debug(
                            f"analysis_id={rt.analysis_id} scenario_id={scenario_id} signal_id={signal_id} "
                            f"symbol={symbol} tf={rt.timeframe} direction={direction} "
                            f"bin_name={bin_name} open_time=startup ttl={rt.ttl_sec}"
                        )

            else:
                publish_tasks = []
                published_items: list[tuple[str, str]] = []

                for direction in ("long", "short"):
                    rules = rt.bins_by_direction.get(direction) or []
                    if not rules:
                        continue

                    bin_name = rt.worker.bin_value(value=value, rules=rules)
                    if not bin_name:
                        continue

                    publish_tasks.append(
                        publish_pack_state_static(
                            redis=redis,
                            analysis_id=rt.analysis_id,
                            direction=direction,
                            symbol=symbol,
                            timeframe=rt.timeframe,
                            bin_name=bin_name,
                            ttl_sec=rt.ttl_sec,
                        )
                    )
                    published_items.append((direction, bin_name))

                if publish_tasks:
                    await asyncio.gather(*publish_tasks, return_exceptions=True)
                    for direction, bin_name in published_items:
                        log.debug(
                            f"analysis_id={rt.analysis_id} symbol={symbol} tf={rt.timeframe} "
                            f"direction={direction} bin_name={bin_name} open_time=startup ttl={rt.ttl_sec}"
                        )

    tasks = []
    for rt in runtimes:
        for symbol in symbols:
            tasks.append(asyncio.create_task(_process_one(symbol, rt)))

    await asyncio.gather(*tasks, return_exceptions=True)
    log.debug(f"PACK_BOOT: bootstrap завершён — packs={len(runtimes)}, symbols={len(symbols)}")


# 🔸 Инициализация кэша и реестра pack-воркеров
async def init_pack_runtime(pg):
    global pack_registry, adaptive_pairs_index, adaptive_pairs_set

    log = logging.getLogger("PACK_INIT")

    packs = await load_enabled_packs(pg)
    analysis_ids = sorted({int(p["analysis_id"]) for p in packs})

    analysis_meta = await load_analysis_instances(pg, analysis_ids)
    analysis_params = await load_analysis_parameters(pg, analysis_ids)
    static_bins_dict = await load_static_bins_dict(pg, analysis_ids)

    pack_registry = build_pack_registry(
        packs=packs,
        analysis_meta=analysis_meta,
        analysis_params=analysis_params,
        static_bins_dict=static_bins_dict,
    )

    # построить индекс используемых пар для adaptive
    adaptive_pairs_index = {}
    adaptive_pairs_set = set()

    all_runtimes: list[PackRuntime] = []
    for lst in pack_registry.values():
        all_runtimes.extend(lst)

    for rt in all_runtimes:
        if rt.bins_source != "adaptive":
            continue
        for pair in rt.adaptive_pairs:
            adaptive_pairs_set.add(pair)
            adaptive_pairs_index.setdefault(pair, set()).add(rt.analysis_id)

    log.info(f"PACK_INIT: adaptive pairs configured: {len(adaptive_pairs_set)}")

    # первичная загрузка adaptive-кеша для нужных пар
    for (scenario_id, signal_id) in sorted(list(adaptive_pairs_set)):
        analysis_list = sorted(list(adaptive_pairs_index.get((scenario_id, signal_id), set())))
        if not analysis_list:
            continue

        loaded = await load_adaptive_bins_for_pair(pg, analysis_list, scenario_id, signal_id)
        async with adaptive_lock:
            for (aid, tf, direction), rules in loaded.items():
                adaptive_bins_cache[(aid, scenario_id, signal_id, tf, direction)] = rules

        log.info(f"PACK_INIT: adaptive dict loaded for scenario_id={scenario_id}, signal_id={signal_id}, analysis_ids={analysis_list}")


# 🔸 Внешняя точка входа (запускается через indicators_v4_main.py и run_safe_loop)
async def run_indicator_pack(pg, redis):
    # загрузить конфиг и кеши
    await init_pack_runtime(pg)

    # создать группы заранее
    await ensure_stream_group(redis, INDICATOR_STREAM, IND_PACK_GROUP)
    await ensure_stream_group(redis, POSTPROC_STREAM_KEY, POSTPROC_GROUP)

    # холодный старт
    await bootstrap_current_state(pg, redis)

    # основная работа
    await asyncio.gather(
        watch_indicator_stream(redis),
        watch_postproc_ready(pg, redis),
    )