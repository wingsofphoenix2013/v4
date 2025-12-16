# indicator_pack.py — оркестратор расчёта и публикации обогащённых состояний (ind_pack)

# 🔸 Базовые импорты
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

# 🔸 Импорт pack-воркеров (предусмотрено расширение)
from packs.rsi_bin import RsiBinPack


# 🔸 Константы Redis
INDICATOR_STREAM = "indicator_stream"          # входной стрим готовности индикаторов
IND_PACK_PREFIX = "ind_pack"                   # префикс ключей результата (как договорились)
IND_PACK_GROUP = "ind_pack_group_v4"           # consumer-group для indicator_stream
IND_PACK_CONSUMER = "ind_pack_consumer_1"      # consumer name

# 🔸 Константы БД
PACK_INSTANCES_TABLE = "indicator_pack_instances_v4"
ANALYSIS_INSTANCES_TABLE = "bt_analysis_instances"
ANALYSIS_PARAMETERS_TABLE = "bt_analysis_parameters"
BINS_DICT_TABLE = "bt_analysis_bins_dict"

# 🔸 Параметры чтения и параллельной обработки stream
STREAM_READ_COUNT = 500          # сколько сообщений читать за раз
STREAM_BLOCK_MS = 2000           # блокировка XREADGROUP (мс)
MAX_PARALLEL_MESSAGES = 200      # сколько сообщений обрабатывать параллельно
LOG_EVERY_EVENTS = 5000          # редкий суммарный лог по прогрессу

# 🔸 TTL по TF (как договорились)
TTL_BY_TF_SEC = {
    "m5": 120,        # 1 минута
    "m15": 960,      # 16 минут
    "h1": 3660,      # 61 минута
}

# 🔸 Реестр доступных pack-воркеров (key берём из bt_analysis_instances.key)
PACK_WORKERS = {
    "rsi_bin": RsiBinPack,
}

# 🔸 Глобальный реестр pack-инстансов, готовых к работе
pack_registry: dict[tuple[str, str], list["PackRuntime"]] = {}
# key: (timeframe, indicator_from_stream) -> list[PackRuntime]


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
    timeframe: str
    source_param_name: str
    bins_policy: dict[str, Any] | None
    bins_by_direction: dict[str, list[BinRule]]
    ttl_sec: int
    worker: Any


# 🔸 Определение источника бинов из bins_policy (пока поддерживаем только static)
def get_bins_source(bins_policy: dict[str, Any] | None, timeframe: str) -> str:
    # дефолт — static
    if not bins_policy:
        return "static"

    try:
        # форма 1) {"default":"static","by_tf":{"m5":"adaptive",...}}
        if "by_tf" in bins_policy:
            by_tf = bins_policy.get("by_tf") or {}
            return str(by_tf.get(timeframe) or bins_policy.get("default") or "static")

        # форма 2) {"m5":"adaptive","m15":"static","h1":"static"} или {"default":"static"}
        return str(bins_policy.get(timeframe) or bins_policy.get("default") or "static")
    except Exception:
        return "static"


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

    log.info(f"PACK_INIT: включённых pack-инстансов загружено: {len(packs)}")
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

    log.info(f"PACK_INIT: analysis-инстансов загружено: {len(out)}")
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

    # суммарный лог по полноте (tf/param_name)
    ok = 0
    missing = 0
    for aid in analysis_ids:
        p = params.get(aid, {})
        if "tf" in p and "param_name" in p:
            ok += 1
        else:
            missing += 1

    log.info(f"PACK_INIT: параметров анализаторов (tf+param_name) OK={ok}, missing={missing}")
    return params


# 🔸 Загрузка статичного словаря бинов (bt_analysis_bins_dict)
async def load_bins_dict(pg, analysis_ids: list[int]) -> dict[int, dict[str, dict[str, list[BinRule]]]]:
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

    # структура: analysis_id -> timeframe -> direction -> [BinRule...]
    out: dict[int, dict[str, dict[str, list[BinRule]]]] = {}
    total_rules = 0

    for r in rows:
        aid = int(r["analysis_id"])
        direction = str(r["direction"])
        timeframe = str(r["timeframe"])
        bin_type = str(r["bin_type"])

        rule = BinRule(
            direction=direction,
            timeframe=timeframe,
            bin_type=bin_type,
            bin_order=int(r["bin_order"]),
            bin_name=str(r["bin_name"]),
            val_from=float(r["val_from"]) if r["val_from"] is not None else None,
            val_to=float(r["val_to"]) if r["val_to"] is not None else None,
            to_inclusive=bool(r["to_inclusive"]),
        )

        out.setdefault(aid, {}).setdefault(timeframe, {}).setdefault(direction, []).append(rule)
        total_rules += 1

    # сортировка по bin_order для корректного прохода
    for aid in out:
        for tf in out[aid]:
            for direction in out[aid][tf]:
                out[aid][tf][direction].sort(key=lambda x: x.bin_order)

    log.info(f"PACK_INIT: правил бинов (static) загружено: {total_rules}")
    return out


# 🔸 Построение реестра pack-воркеров (match по indicator_stream.indicator + timeframe)
def build_pack_registry(
    packs: list[dict[str, Any]],
    analysis_meta: dict[int, dict[str, Any]],
    analysis_params: dict[int, dict[str, str]],
    bins_dict: dict[int, dict[str, dict[str, list[BinRule]]]],
) -> dict[tuple[str, str], list[PackRuntime]]:
    log = logging.getLogger("PACK_INIT")

    registry: dict[tuple[str, str], list[PackRuntime]] = {}

    active = 0
    skipped = 0
    no_bins = 0
    not_supported = 0

    for pack in packs:
        analysis_id = int(pack["analysis_id"])
        meta = analysis_meta.get(analysis_id)
        params = analysis_params.get(analysis_id, {})

        if not meta:
            skipped += 1
            log.warning(f"PACK_INIT: analysis_id={analysis_id} пропущен: нет записи в bt_analysis_instances")
            continue

        if not bool(meta.get("enabled", True)):
            skipped += 1
            log.warning(f"PACK_INIT: analysis_id={analysis_id} пропущен: bt_analysis_instances.enabled=false")
            continue

        analysis_key = str(meta["key"])
        analysis_name = str(meta["name"])

        timeframe = params.get("tf")
        source_param_name = params.get("param_name")

        if not timeframe or not source_param_name:
            skipped += 1
            log.warning(
                f"PACK_INIT: analysis_id={analysis_id} ({analysis_key}) пропущен: "
                f"нет tf/param_name в bt_analysis_parameters"
            )
            continue

        ttl_sec = int(TTL_BY_TF_SEC.get(timeframe, 60))

        bins_policy = pack.get("bins_policy")
        bins_source = get_bins_source(bins_policy, timeframe)

        # пока поддерживаем только static
        if bins_source != "static":
            not_supported += 1
            log.warning(
                f"PACK_INIT: analysis_id={analysis_id} ({analysis_key}) пропущен: "
                f"bins_source={bins_source} пока не поддержан (только static)"
            )
            continue

        worker_cls = PACK_WORKERS.get(analysis_key)
        if worker_cls is None:
            skipped += 1
            log.warning(f"PACK_INIT: analysis_id={analysis_id} пропущен: воркер для key='{analysis_key}' не найден")
            continue

        # собрать правила бинов
        bins_tf = bins_dict.get(analysis_id, {}).get(timeframe, {})
        if not bins_tf:
            no_bins += 1
            # не пропускаем — пак может жить, просто не будет публиковать до появления правил
            log.warning(
                f"PACK_INIT: analysis_id={analysis_id} ({analysis_key}) — нет bin-правил "
                f"в bt_analysis_bins_dict для tf={timeframe}"
            )

        bins_by_direction = {
            "long": bins_tf.get("long", []),
            "short": bins_tf.get("short", []),
        }

        runtime = PackRuntime(
            analysis_id=analysis_id,
            analysis_key=analysis_key,
            analysis_name=analysis_name,
            timeframe=timeframe,
            source_param_name=source_param_name,
            bins_policy=bins_policy,
            bins_by_direction=bins_by_direction,
            ttl_sec=ttl_sec,
            worker=worker_cls(),
        )

        # матчимся по indicator_stream.indicator == source_param_name
        registry.setdefault((timeframe, source_param_name), []).append(runtime)
        active += 1

    log.info(
        f"PACK_INIT: registry построен — active={active}, skipped={skipped}, "
        f"no_bins={no_bins}, not_supported={not_supported}, routes={len(registry)}"
    )
    return registry


# 🔸 Публикация результата pack в Redis (ключ ind_pack:analysis_id:direction:symbol:tf)
async def publish_pack_state(redis, analysis_id: int, direction: str, symbol: str, timeframe: str, bin_name: str, ttl_sec: int):
    key = f"{IND_PACK_PREFIX}:{analysis_id}:{direction}:{symbol}:{timeframe}"
    await redis.set(key, bin_name, ex=ttl_sec)


# 🔸 Обработка одного события indicator_stream (status=ready)
async def handle_indicator_ready(redis, msg: dict[str, str]) -> tuple[int, int, int, int]:
    symbol = msg.get("symbol")
    timeframe = msg.get("timeframe")
    indicator_key = msg.get("indicator")
    status = msg.get("status")

    # условия достаточности
    if status != "ready" or not symbol or not timeframe or not indicator_key:
        return (0, 0, 0, 0)

    runtimes = pack_registry.get((timeframe, indicator_key))
    if not runtimes:
        return (1, 0, 0, 0)

    computed = 0
    published = 0
    misses = 0

    for rt in runtimes:
        # получить raw значение из Redis KV индикаторов
        raw_key = f"ind:{symbol}:{rt.timeframe}:{rt.source_param_name}"
        raw_value = await redis.get(raw_key)

        # если нет значения — пропускаем
        if raw_value is None:
            misses += 1
            continue

        try:
            value = float(raw_value)
        except Exception:
            misses += 1
            continue

        # считаем бины (long/short) и публикуем два ключа
        tasks = []
        for direction in ("long", "short"):
            rules = rt.bins_by_direction.get(direction) or []
            # если правил нет — нечего публиковать
            if not rules:
                continue

            bin_name = rt.worker.bin_value(value=value, rules=rules)
            if not bin_name:
                # значение не попало ни в один бин
                continue

            tasks.append(
                publish_pack_state(
                    redis=redis,
                    analysis_id=rt.analysis_id,
                    direction=direction,
                    symbol=symbol,
                    timeframe=rt.timeframe,
                    bin_name=bin_name,
                    ttl_sec=rt.ttl_sec,
                )
            )

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            published += len(tasks)

        computed += 1

    return (1, computed, published, misses)

# 🔸 Подписка на indicator_stream и оркестрация pack-обработки (параллельно)
async def watch_indicator_stream(redis):
    log = logging.getLogger("PACK_STREAM")

    # создать consumer-group
    try:
        await redis.xgroup_create(INDICATOR_STREAM, IND_PACK_GROUP, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create error: {e}")

    # суммарные счётчики
    total_events = 0
    total_matched = 0
    total_computed = 0
    total_published = 0
    total_misses = 0
    total_errors = 0

    sem = asyncio.Semaphore(MAX_PARALLEL_MESSAGES)

    async def _process_one(data: dict) -> tuple[int, int, int, int, int]:
        # ограничение параллелизма
        async with sem:
            try:
                msg = {
                    "symbol": data.get("symbol"),
                    "timeframe": data.get("timeframe"),
                    "indicator": data.get("indicator"),
                    "open_time": data.get("open_time"),
                    "status": data.get("status"),
                }

                matched, computed, published, misses = await handle_indicator_ready(redis, msg)
                return (1, matched, computed, published, misses)
            except Exception as e:
                log.warning(f"PACK_STREAM: обработка сообщения упала: {e}", exc_info=True)
                return (0, 0, 0, 0, 0)

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
                # нет новых сообщений — молчим (как ты и хочешь)
                continue

            # расплющим в единый список
            flat: list[tuple[str, dict]] = []
            for _, messages in resp:
                for msg_id, data in messages:
                    flat.append((msg_id, data))

            if not flat:
                continue

            to_ack = [msg_id for msg_id, _ in flat]

            # параллельная обработка пачки
            tasks = [asyncio.create_task(_process_one(data)) for _, data in flat]
            results = await asyncio.gather(*tasks, return_exceptions=False)

            # агрегация результатов пачки
            batch_events = len(flat)
            batch_matched = 0
            batch_computed = 0
            batch_published = 0
            batch_misses = 0
            batch_errors = 0

            for ok, matched, computed, published, misses in results:
                if ok == 0:
                    batch_errors += 1
                    continue
                batch_matched += matched
                batch_computed += computed
                batch_published += published
                batch_misses += misses

            # ack пачкой
            await redis.xack(INDICATOR_STREAM, IND_PACK_GROUP, *to_ack)

            # обновить totals
            total_events += batch_events
            total_matched += batch_matched
            total_computed += batch_computed
            total_published += batch_published
            total_misses += batch_misses
            total_errors += batch_errors

            # логируем только по делу:
            # - если реально что-то публиковали
            # - или если были ошибки
            # - или редкий прогресс-лог по объёму
            if batch_published > 0 or batch_errors > 0 or (total_events % LOG_EVERY_EVENTS == 0):
                log.info(
                    f"PACK_STREAM: batch_events={batch_events}, batch_published={batch_published}, "
                    f"events={total_events}, matched={total_matched}, computed={total_computed}, "
                    f"published={total_published}, misses={total_misses}, errors={total_errors}"
                )

        except Exception as e:
            total_errors += 1
            log.error(f"PACK_STREAM loop error: {e}", exc_info=True)
            await asyncio.sleep(2)

# 🔸 Инициализация кэша и реестра pack-воркеров
async def init_pack_runtime(pg):
    global pack_registry
    log = logging.getLogger("PACK_INIT")

    packs = await load_enabled_packs(pg)
    analysis_ids = sorted({int(p["analysis_id"]) for p in packs})

    analysis_meta = await load_analysis_instances(pg, analysis_ids)
    analysis_params = await load_analysis_parameters(pg, analysis_ids)
    bins_dict = await load_bins_dict(pg, analysis_ids)

    pack_registry = build_pack_registry(
        packs=packs,
        analysis_meta=analysis_meta,
        analysis_params=analysis_params,
        bins_dict=bins_dict,
    )

    # итоговая сводка
    total_routes = sum(len(v) for v in pack_registry.values())
    log.info(f"PACK_INIT: pack_registry готов — routes_total={total_routes}, match_keys={len(pack_registry)}")


# 🔸 Внешняя точка входа (запускается через indicators_v4_main.py и run_safe_loop)
async def run_indicator_pack(pg, redis):
    await init_pack_runtime(pg)
    await watch_indicator_stream(redis)