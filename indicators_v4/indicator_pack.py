# indicator_pack.py — оркестратор расчёта и публикации обогащённых состояний (ind_pack), включая MTF «свежесть» и labels-cache

# 🔸 Базовые импорты
import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

# 🔸 Импорт pack-воркеров (предусмотрено расширение)
from packs.rsi_bin import RsiBinPack
from packs.mfi_bin import MfiBinPack
from packs.adx_bin import AdxBinPack
from packs.bb_band_bin import BbBandBinPack
from packs.lr_band_bin import LrBandBinPack
from packs.lr_angle_bin import LrAngleBinPack
from packs.atr_bin import AtrBinPack
from packs.dmigap_bin import DmiGapBinPack
from packs.rsi_mtf import RsiMtfPack
from packs.mfi_mtf import MfiMtfPack
from packs.rsimfi_mtf import RsiMfiMtfPack
from packs.supertrend_mtf import SupertrendMtfPack
from packs.lr_mtf import LrMtfPack
from packs.bb_mtf import BbMtfPack
from packs.lr_angle_mtf import LrAngleMtfPack

# 🔸 Константы Redis (индикаторы)
INDICATOR_STREAM = "indicator_stream"          # входной стрим готовности индикаторов
IND_PACK_PREFIX = "ind_pack"                   # префикс ключей результата
IND_PACK_GROUP = "ind_pack_group_v4"           # consumer-group для indicator_stream
IND_PACK_CONSUMER = "ind_pack_consumer_1"      # consumer name

# 🔸 Константы Redis (постпроцессинг бектеста → сигнал обновления словарей)
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
BINS_LABELS_TABLE = "bt_analysis_bins_labels"
BB_TICKERS_TABLE = "tickers_bb"

# 🔸 Параметры чтения и параллельной обработки stream
STREAM_READ_COUNT = 500          # сколько сообщений читать за раз
STREAM_BLOCK_MS = 2000           # блокировка XREADGROUP (мс)
MAX_PARALLEL_MESSAGES = 200      # сколько сообщений обрабатывать параллельно

# 🔸 Параметры холодного старта (bootstrap)
BOOTSTRAP_MAX_PARALLEL = 300     # сколько тикеров/паков обрабатывать параллельно при старте

# 🔸 Retry для «свежих» значений MTF (по TS на стыках TF)
MTF_RETRY_TOTAL_SEC = 60         # максимум ожидания «свежего» TF
MTF_RETRY_STEP_SEC = 5           # период опроса TS

# 🔸 Таймшаги TF (ms) — в системе везде open_time (начало бара)
TF_STEP_MS = {
    "m5": 300_000,
    "m15": 900_000,
    "h1": 3_600_000,
}

# 🔸 TTL по TF
TTL_BY_TF_SEC = {
    "m5": 120,      # 2 минуты
    "m15": 960,     # 16 минут
    "h1": 3660,     # 61 минута
    "mtf": 120,     # MTF-результат живёт как m5-состояние
}

# 🔸 Реестр доступных pack-воркеров (key берём из bt_analysis_instances.key)
PACK_WORKERS = {
    "rsi_bin": RsiBinPack,
    "mfi_bin": MfiBinPack,
    "adx_bin": AdxBinPack,
    "bb_band_bin": BbBandBinPack,
    "lr_band_bin": LrBandBinPack,
    "lr_angle_bin": LrAngleBinPack,
    "atr_bin": AtrBinPack,
    "dmigap_bin": DmiGapBinPack,
    "rsi_mtf": RsiMtfPack,
    "mfi_mtf": MfiMtfPack,
    "rsimfi_mtf": RsiMfiMtfPack,
    "supertrend_mtf": SupertrendMtfPack,
    "lr_mtf": LrMtfPack,
    "bb_mtf": BbMtfPack,
    "lr_angle_mtf": LrAngleMtfPack,
}

# 🔸 Глобальный реестр pack-инстансов, готовых к работе
pack_registry: dict[tuple[str, str], list["PackRuntime"]] = {}
# key: (timeframe_from_stream, indicator_from_stream) -> list[PackRuntime]

# 🔸 Кеш adaptive-словаря (bins): (analysis_id, scenario_id, signal_id, tf, direction) -> [BinRule...]
adaptive_bins_cache: dict[tuple[int, int, int, str, str], list["BinRule"]] = {}

# 🔸 Кеш adaptive-словаря (quantiles): (analysis_id, scenario_id, signal_id, tf, direction) -> [BinRule...]
adaptive_quantiles_cache: dict[tuple[int, int, int, str, str], list["BinRule"]] = {}

# 🔸 Индекс используемых пар (scenario_id, signal_id) -> set(analysis_id) для bins
adaptive_pairs_index: dict[tuple[int, int], set[int]] = {}

# 🔸 Индекс используемых пар (scenario_id, signal_id) -> set(analysis_id) для quantiles
adaptive_quantiles_pairs_index: dict[tuple[int, int], set[int]] = {}

# 🔸 Быстрый set для проверки "интересна ли пара" в стриме postproc_ready (bins)
adaptive_pairs_set: set[tuple[int, int]] = set()

# 🔸 Быстрый set для проверки "интересна ли пара" в стриме postproc_ready (quantiles)
adaptive_quantiles_pairs_set: set[tuple[int, int]] = set()

# 🔸 Лок для обновления adaptive-кеша
adaptive_lock = asyncio.Lock()

# 🔸 Labels cache: (scenario_id, signal_id, direction, analysis_id, indicator_param, timeframe) -> set(bin_name)
labels_bins_cache: dict[tuple[int, int, str, int, str, str], set[str]] = {}

# 🔸 Индекс: (scenario_id, signal_id) -> set(LabelsContext)
labels_pairs_index: dict[tuple[int, int], set["LabelsContext"]] = {}

# 🔸 Быстрый set для проверки "интересна ли пара" (labels)
labels_pairs_set: set[tuple[int, int]] = set()

# 🔸 Лок для обновления labels-кеша
labels_lock = asyncio.Lock()


# 🔸 Models
@dataclass(frozen=True)
class BinRule:
    direction: str
    timeframe: str
    bin_type: str
    bin_order: int
    bin_name: str
    val_from: str | None
    val_to: str | None
    to_inclusive: bool


@dataclass(frozen=True)
class LabelsContext:
    analysis_id: int
    indicator_param: str
    timeframe: str  # например "mtf"


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

    # 🔸 MTF-конфиг (для mtf-паков)
    is_mtf: bool = False
    mtf_pairs: list[tuple[int, int]] | None = None
    mtf_trigger_tf: str | None = None
    mtf_component_tfs: list[str] | None = None
    mtf_component_params: dict[str, Any] | None = None          # tf -> param_name OR tf -> {name:param_name}
    mtf_bins_static: dict[str, dict[str, list[BinRule]]] | None = None  # tf/bins_tf -> direction -> rules
    mtf_bins_tf: str = "components"                             # "components" | "mtf"
    mtf_clip_0_100: bool = True                                 # для supertrend должен быть False

    mtf_required_bins_tfs: list[str] | None = None              # какие TF требуют static bins (для lr_mtf: ["h1","m15"])
    mtf_quantiles_key: str | None = None                        # ключ rules_by_tf для quantiles (для lr_mtf: "quantiles")
    mtf_needs_price: bool = False                               # нужен ли price (для lr_mtf: True)
    mtf_price_tf: str = "m5"                                    # откуда брать price (tf)
    mtf_price_field: str = "c"                                  # поле цены (обычно 'c')

# 🔸 Определение источника бинов из bins_policy
def get_bins_source(bins_policy: dict[str, Any] | None, timeframe: str) -> str:
    # дефолт — static
    if not isinstance(bins_policy, dict):
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


# 🔸 Разбор списка пар из bins_policy
def get_pairs(bins_policy: dict[str, Any] | None) -> list[tuple[int, int]]:
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
    pname = str(param_name or "").strip()
    if not pname:
        return ""

    # если нет '_' — совпадает как есть
    if "_" not in pname:
        return pname

    # adx_dmi: base = adx_dmi{len}; параметры могут быть:
    # - adx_dmi14            -> adx_dmi14
    # - adx_dmi14_adx        -> adx_dmi14
    # - adx_dmi14_plus_di    -> adx_dmi14
    # - adx_dmi14_minus_di   -> adx_dmi14
    if family_key == "adx_dmi":
        if pname.endswith("_plus_di") or pname.endswith("_minus_di") or pname.endswith("_adx"):
            return pname.rsplit("_", 1)[0]
        return pname

    # bb20_2_0_upper -> bb20, macd12_macd_hist -> macd12, lr50_angle -> lr50, supertrend10_3_0_trend -> supertrend10
    return pname.split("_", 1)[0]

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


# 🔸 Decimal helpers
def safe_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def clip_0_100(value: Decimal) -> Decimal:
    if value < Decimal("0"):
        return Decimal("0")
    if value > Decimal("100"):
        return Decimal("100")
    return value


# 🔸 Labels cache helpers (без model_id)
def labels_cache_key(
    scenario_id: int,
    signal_id: int,
    direction: str,
    analysis_id: int,
    indicator_param: str,
    timeframe: str,
) -> tuple[int, int, str, int, str, str]:
    return (
        int(scenario_id),
        int(signal_id),
        str(direction),
        int(analysis_id),
        str(indicator_param),
        str(timeframe),
    )


def labels_has_bin(
    scenario_id: int,
    signal_id: int,
    direction: str,
    analysis_id: int,
    indicator_param: str,
    timeframe: str,
    bin_name: str,
) -> bool:
    key = labels_cache_key(scenario_id, signal_id, direction, analysis_id, indicator_param, timeframe)
    s = labels_bins_cache.get(key)
    if not s:
        return False
    return str(bin_name) in s


# 🔸 MTF helpers: styk TF (open_time + step) и ожидание «свежих» значений из TS
def is_tf_boundary(ts_ms: int, tf: str) -> bool:
    step = TF_STEP_MS.get(tf)
    if not step:
        return False
    return (int(ts_ms) % int(step)) == 0


def calc_close_boundary_ts_ms(open_ts_ms: int, tf: str) -> int:
    # в терминах open_time: граница закрытия бара — следующий open_time
    step = TF_STEP_MS.get(tf)
    if not step:
        return int(open_ts_ms)
    return int(open_ts_ms) + int(step)


def just_closed_open_time(boundary_ts_ms: int, tf: str) -> int:
    # boundary — open_time следующего бара, значит закрывшийся бар начинается в boundary - step
    return int(boundary_ts_ms) - int(TF_STEP_MS[tf])


async def get_kv_decimal(redis, symbol: str, tf: str, param_name: str) -> Decimal | None:
    # читаем последнее значение (KV)
    key = f"ind:{symbol}:{tf}:{param_name}"
    raw = await redis.get(key)
    return safe_decimal(raw)


async def get_ts_decimal_with_retry(redis, symbol: str, tf: str, param_name: str, ts_ms: int) -> Decimal | None:
    # читаем точку TS с ретраями до MTF_RETRY_TOTAL_SEC
    key = f"{IND_TS_PREFIX}:{symbol}:{tf}:{param_name}"

    waited = 0
    while waited <= MTF_RETRY_TOTAL_SEC:
        raw = await ts_get_value_at(redis, key, ts_ms)
        d = safe_decimal(raw)
        if d is not None:
            return d

        # таймаут достигнут
        if waited >= MTF_RETRY_TOTAL_SEC:
            break

        await asyncio.sleep(MTF_RETRY_STEP_SEC)
        waited += MTF_RETRY_STEP_SEC

    return None


async def get_mtf_value_decimal(redis, symbol: str, trigger_open_ts_ms: int, target_tf: str, param_name: str) -> Decimal | None:
    # m5 — событие ready уже гарантирует актуальность значения для этого open_time
    if target_tf == "m5":
        return await get_kv_decimal(redis, symbol, "m5", param_name)

    # граница закрытия m5-бара
    boundary = calc_close_boundary_ts_ms(trigger_open_ts_ms, "m5")

    # если boundary не является границей target_tf — target_tf не пересчитывается сейчас, KV безопасен
    if not is_tf_boundary(boundary, target_tf):
        return await get_kv_decimal(redis, symbol, target_tf, param_name)

    # styk TF: нужен «свежий» бар target_tf, который только что закрылся на boundary
    target_open = just_closed_open_time(boundary, target_tf)
    return await get_ts_decimal_with_retry(redis, symbol, target_tf, param_name, target_open)


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
    parsed_json = 0
    for r in rows:
        policy = r["bins_policy"]

        # если jsonb пришёл строкой — парсим
        if isinstance(policy, str):
            try:
                policy = json.loads(policy)
                parsed_json += 1
            except Exception:
                policy = None

        packs.append(
            {
                "id": int(r["id"]),
                "analysis_id": int(r["analysis_id"]),
                "bins_policy": policy,
                "enabled_at": r["enabled_at"],
            }
        )

    log.info(f"PACK_INIT: включённых pack-инстансов загружено: {len(packs)} (bins_policy parsed_from_str={parsed_json})")
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

    log.info(f"PACK_INIT: bt_analysis_instances загружено: {len(out)}")
    return out


# 🔸 Загрузка параметров анализаторов (нужны tf и param_name; tf может быть выведен из *_mtf)
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

    ok = 0
    missing = 0
    for aid in analysis_ids:
        p = params.get(aid, {})
        # условия достаточности (tf допускаем пустым для *_mtf)
        if p.get("param_name"):
            ok += 1
        else:
            missing += 1

    log.info(f"PACK_INIT: bt_analysis_parameters (param_name) OK={ok}, missing={missing}")
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
    total_rules = 0

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
            val_from=str(r["val_from"]) if r["val_from"] is not None else None,
            val_to=str(r["val_to"]) if r["val_to"] is not None else None,
            to_inclusive=bool(r["to_inclusive"]),
        )

        out.setdefault(aid, {}).setdefault(timeframe, {}).setdefault(direction, []).append(rule)
        total_rules += 1

    for aid in out:
        for tf in out[aid]:
            for direction in out[aid][tf]:
                out[aid][tf][direction].sort(key=lambda x: x.bin_order)

    log.info(f"PACK_INIT: static bins загружено: rules={total_rules}")
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
            val_from=str(r["val_from"]) if r["val_from"] is not None else None,
            val_to=str(r["val_to"]) if r["val_to"] is not None else None,
            to_inclusive=bool(r["to_inclusive"]),
        )
        out.setdefault((aid, tf, direction), []).append(rule)

    for k in out:
        out[k].sort(key=lambda x: x.bin_order)

    return out

# 🔸 Загрузка adaptive-словаря (quantiles) для одной пары (scenario_id, signal_id)
async def load_adaptive_quantiles_for_pair(pg, analysis_ids: list[int], scenario_id: int, signal_id: int) -> dict[tuple[int, str, str], list[BinRule]]:
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
              AND bin_type    = 'quantiles'
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
            val_from=str(r["val_from"]) if r["val_from"] is not None else None,
            val_to=str(r["val_to"]) if r["val_to"] is not None else None,
            to_inclusive=bool(r["to_inclusive"]),
        )
        out.setdefault((aid, tf, direction), []).append(rule)

    for k in out:
        out[k].sort(key=lambda x: x.bin_order)

    return out

# 🔸 Загрузка labels (bin_name set) для одной пары (scenario_id, signal_id) и набора контекстов (model_id игнорируется)
async def load_labels_bins_for_pair(pg, scenario_id: int, signal_id: int, contexts: list[LabelsContext]) -> dict[tuple[int, int, str, int, str, str], set[str]]:
    # возвращает: labels_cache_key -> set(bin_name)
    if not contexts:
        return {}

    analysis_ids = sorted({c.analysis_id for c in contexts})
    indicator_params = sorted({c.indicator_param for c in contexts})
    timeframes = sorted({c.timeframe for c in contexts})

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                scenario_id,
                signal_id,
                direction,
                analysis_id,
                indicator_param,
                timeframe,
                bin_name
            FROM {BINS_LABELS_TABLE}
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND analysis_id = ANY($3::int[])
              AND indicator_param = ANY($4::text[])
              AND timeframe   = ANY($5::text[])
            """,
            int(scenario_id),
            int(signal_id),
            analysis_ids,
            indicator_params,
            timeframes,
        )

    out: dict[tuple[int, int, str, int, str, str], set[str]] = {}

    # условия достаточности
    if not rows:
        return out

    ctx_set = {(c.analysis_id, c.indicator_param, c.timeframe) for c in contexts}

    for r in rows:
        try:
            aid = int(r["analysis_id"])
            ip = str(r["indicator_param"])
            tf = str(r["timeframe"])
            if (aid, ip, tf) not in ctx_set:
                continue

            direction = str(r["direction"] or "")
            bin_name = str(r["bin_name"] or "")
            if not direction or not bin_name:
                continue

            k = labels_cache_key(int(scenario_id), int(signal_id), direction, aid, ip, tf)
            out.setdefault(k, set()).add(bin_name)
        except Exception:
            continue

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
    runtimes_total = 0
    runtimes_static = 0
    runtimes_adaptive = 0
    runtimes_mtf = 0

    for pack in packs:
        analysis_id = int(pack["analysis_id"])
        meta = analysis_meta.get(analysis_id)
        params = analysis_params.get(analysis_id, {})

        if not meta:
            log.warning(f"PACK_INIT: analysis_id={analysis_id} пропущен: нет записи в bt_analysis_instances")
            continue

        if not bool(meta.get("enabled", True)):
            log.warning(f"PACK_INIT: analysis_id={analysis_id} пропущен: bt_analysis_instances.enabled=false")
            continue

        analysis_key = str(meta["key"])
        analysis_name = str(meta["name"])
        family_key = str(meta["family_key"])

        source_param_name = str(params.get("param_name") or "").strip()
        timeframe = str(params.get("tf") or "").strip()

        # MTF: если tf отсутствует, но param_name заканчивается на "_mtf" — считаем tf="mtf"
        if not timeframe and source_param_name.lower().endswith("_mtf"):
            timeframe = "mtf"

        # MTF: если tf отсутствует, но key анализатора заканчивается на "_mtf" — считаем tf="mtf"
        if not timeframe and analysis_key.lower().endswith("_mtf"):
            timeframe = "mtf"

        # условия достаточности
        if not timeframe or not source_param_name:
            log.warning(f"PACK_INIT: analysis_id={analysis_id} ({analysis_key}) пропущен: нет tf/param_name")
            continue

        bins_policy = pack.get("bins_policy")
        bins_source = get_bins_source(bins_policy, timeframe)

        worker_cls = PACK_WORKERS.get(analysis_key)
        if worker_cls is None:
            log.warning(f"PACK_INIT: analysis_id={analysis_id} пропущен: воркер для key='{analysis_key}' не найден")
            continue

        # 🔸 MTF (через отдельный воркер)
        is_mtf = (timeframe.lower() == "mtf") or source_param_name.lower().endswith("_mtf")
        if is_mtf:
            runtimes_mtf += 1

            pairs = get_pairs(bins_policy)
            if not pairs:
                log.warning(f"PACK_INIT: analysis_id={analysis_id} ({analysis_key}) mtf: bins_policy.pairs пустой — пропущен")
                continue

            worker = worker_cls()

            # перед mtf_config: применить параметры анализатора (если воркер поддерживает)
            if hasattr(worker, "configure"):
                try:
                    worker.configure(params)
                except Exception:
                    pass

            cfg = worker.mtf_config(source_param_name) if hasattr(worker, "mtf_config") else {}

            trigger_tf = str(cfg.get("trigger_tf") or "m5")
            component_tfs = list(cfg.get("component_tfs") or [])
            component_param = str(cfg.get("component_param") or "")

            # условия достаточности
            if not component_tfs or not component_param:
                log.warning(f"PACK_INIT: analysis_id={analysis_id} ({analysis_key}) mtf: некорректный mtf_config()")
                continue

            # component_params может быть dict tf->param_name, иначе одинаковый param_name для всех TF
            component_params: dict[str, Any] = {}
            comp_params_cfg = cfg.get("component_params")

            if isinstance(comp_params_cfg, dict):
                for tf in component_tfs:
                    v = comp_params_cfg.get(tf)

                    # допускаем либо строку (один param_name), либо dict (несколько param_name)
                    if isinstance(v, dict):
                        component_params[str(tf)] = {str(k): str(val) for k, val in v.items() if k and val}
                    elif v:
                        component_params[str(tf)] = str(v)

            # дефолт: один и тот же param_name для всех TF
            for tf in component_tfs:
                component_params.setdefault(str(tf), component_param)

            # bins_tf и клипование значений (для supertrend клиповать нельзя)
            bins_tf_key = str(cfg.get("bins_tf") or "").strip().lower()
            if not bins_tf_key:
                bins_tf_key = "components"

            clip_cfg = cfg.get("clip_0_100")
            mtf_clip_0_100 = True if clip_cfg is None else bool(clip_cfg)

            # bins берём либо по компонентным TF, либо из timeframe='mtf'
            mtf_bins_static: dict[str, dict[str, list[BinRule]]] = {}

            if bins_tf_key == "mtf":
                bins_mtf = static_bins_dict.get(analysis_id, {}).get("mtf", {})
                mtf_bins_static["mtf"] = {
                    "long": bins_mtf.get("long", []),
                    "short": bins_mtf.get("short", []),
                }
            else:
                for tf in component_tfs:
                    bins_tf = static_bins_dict.get(analysis_id, {}).get(str(tf), {})
                    mtf_bins_static[str(tf)] = {
                        "long": bins_tf.get("long", []),
                        "short": bins_tf.get("short", []),
                    }

            ttl_sec = int(TTL_BY_TF_SEC.get("mtf", TTL_BY_TF_SEC.get("m5", 120)))

            required_bins_tfs = cfg.get("required_bins_tfs")
            if not isinstance(required_bins_tfs, list) or not required_bins_tfs:
                required_bins_tfs = component_tfs

            quantiles_key = cfg.get("quantiles_key")
            if quantiles_key is not None:
                quantiles_key = str(quantiles_key).strip() or None

            needs_price = bool(cfg.get("needs_price", False))
            price_tf = str(cfg.get("price_tf") or "m5").strip()
            price_field = str(cfg.get("price_field") or "c").strip()

            runtime = PackRuntime(
                analysis_id=analysis_id,
                analysis_key=analysis_key,
                analysis_name=analysis_name,
                family_key=family_key,
                timeframe="mtf",
                source_param_name=source_param_name,
                bins_policy=bins_policy,
                bins_source="static",
                adaptive_pairs=[],
                bins_by_direction={"long": [], "short": []},
                ttl_sec=ttl_sec,
                worker=worker,
                is_mtf=True,
                mtf_pairs=pairs,
                mtf_trigger_tf=trigger_tf,
                mtf_component_tfs=component_tfs,
                mtf_component_params=component_params,
                mtf_bins_static=mtf_bins_static,
                mtf_bins_tf=bins_tf_key,
                mtf_clip_0_100=mtf_clip_0_100,
                mtf_required_bins_tfs=[str(x) for x in required_bins_tfs],
                mtf_quantiles_key=quantiles_key,
                mtf_needs_price=needs_price,
                mtf_price_tf=price_tf,
                mtf_price_field=price_field,
            )

            # триггеримся по base индикатора на trigger_tf
            stream_indicator = get_stream_indicator_key(family_key, component_param)
            registry.setdefault((trigger_tf, stream_indicator), []).append(runtime)
            runtimes_total += 1
            continue

        # 🔸 Single-TF (как было)
        ttl_sec = int(TTL_BY_TF_SEC.get(timeframe, 60))

        adaptive_pairs: list[tuple[int, int]] = []
        bins_by_direction: dict[str, list[BinRule]] = {"long": [], "short": []}

        if bins_source == "adaptive":
            adaptive_pairs = get_pairs(bins_policy)
            if not adaptive_pairs:
                log.warning(f"PACK_INIT: analysis_id={analysis_id} ({analysis_key}) bins_source=adaptive, но pairs пустой — пропущен")
                continue
            runtimes_adaptive += 1
        else:
            bins_tf = static_bins_dict.get(analysis_id, {}).get(timeframe, {})
            bins_by_direction = {
                "long": bins_tf.get("long", []),
                "short": bins_tf.get("short", []),
            }
            runtimes_static += 1

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
        runtimes_total += 1

    log.info(
        "PACK_INIT: registry построен — match_keys=%s, runtimes_total=%s, static=%s, adaptive=%s, mtf=%s",
        len(registry),
        runtimes_total,
        runtimes_static,
        runtimes_adaptive,
        runtimes_mtf,
    )
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


# 🔸 Сбор value для ATR% bins (atr из indicators KV, close из feed TS)
async def build_atr_pct_value(redis, symbol: str, timeframe: str, atr_param_name: str, ts_ms: int | None) -> dict[str, str] | None:
    # atr (KV индикаторов)
    atr_key = f"ind:{symbol}:{timeframe}:{atr_param_name}"
    atr_val = await redis.get(atr_key)
    if atr_val is None:
        return None

    # close по нужному ts_ms (Redis TS фида)
    if ts_ms is None:
        return None

    close_key = f"{BB_TS_PREFIX}:{symbol}:{timeframe}:c"
    close_val = await ts_get_value_at(redis, close_key, ts_ms)
    if close_val is None:
        return None

    return {"atr": atr_val, "price": close_val}


# 🔸 Сбор value для DMI-gap bins (plus/minus из indicators KV)
async def build_dmigap_value(redis, symbol: str, timeframe: str, base_param_name: str) -> dict[str, str] | None:
    plus_key = f"ind:{symbol}:{timeframe}:{base_param_name}_plus_di"
    minus_key = f"ind:{symbol}:{timeframe}:{base_param_name}_minus_di"

    plus_val = await redis.get(plus_key)
    minus_val = await redis.get(minus_key)

    if plus_val is None or minus_val is None:
        return None

    return {"plus": plus_val, "minus": minus_val}


# 🔸 Получение adaptive-правил из кеша
def get_adaptive_rules(analysis_id: int, scenario_id: int, signal_id: int, timeframe: str, direction: str) -> list[BinRule]:
    return adaptive_bins_cache.get((analysis_id, scenario_id, signal_id, timeframe, direction), [])

# 🔸 Обработка MTF-пака: получить values_by_tf + подобрать канонический bin через labels
async def handle_mtf_pack(redis, rt: PackRuntime, symbol: str, trigger_open_ts_ms: int, trigger_tf: str, trigger_indicator: str) -> None:
    log = logging.getLogger("PACK_MTF")

    # условия достаточности
    if not rt.mtf_pairs or not rt.mtf_component_tfs or not rt.mtf_component_params or not rt.mtf_bins_static:
        return

    # собираем значения по TF:
    # - если spec строка: одно значение на TF (как rsi_mtf / mfi_mtf / supertrend_mtf)
    # - если spec dict: несколько значений на TF (как rsimfi_mtf: rsi+mfi, lr_mtf: upper/lower)
    tasks = []
    meta: list[tuple[str, str | None]] = []  # (tf, sub_name|None)

    for tf in rt.mtf_component_tfs:
        spec = rt.mtf_component_params.get(tf)

        # один параметр на TF
        if isinstance(spec, str):
            param = str(spec or "").strip()
            if not param:
                tasks.append(asyncio.create_task(asyncio.sleep(0, result=None)))
                meta.append((str(tf), None))
            else:
                tasks.append(asyncio.create_task(get_mtf_value_decimal(redis, symbol, trigger_open_ts_ms, str(tf), param)))
                meta.append((str(tf), None))

        # несколько параметров на TF (dict)
        elif isinstance(spec, dict):
            for name, param_name in spec.items():
                pname = str(param_name or "").strip()
                if not pname:
                    tasks.append(asyncio.create_task(asyncio.sleep(0, result=None)))
                    meta.append((str(tf), str(name)))
                    continue

                # для m5 в multi-param режиме читаем TS по open_time m5 (ждём второй индикатор/параметр, если он чуть позже)
                if str(tf) == "m5":
                    tasks.append(asyncio.create_task(get_ts_decimal_with_retry(redis, symbol, "m5", pname, int(trigger_open_ts_ms))))
                else:
                    tasks.append(asyncio.create_task(get_mtf_value_decimal(redis, symbol, trigger_open_ts_ms, str(tf), pname)))

                meta.append((str(tf), str(name)))

        else:
            tasks.append(asyncio.create_task(asyncio.sleep(0, result=None)))
            meta.append((str(tf), None))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    values_by_tf: dict[str, Any] = {}
    errors = 0

    for (tf, name), r in zip(meta, results):
        if isinstance(r, Exception):
            errors += 1
            continue
        if r is None:
            continue

        # клип 0..100 — безопасно для RSI/MFI; для supertrend/lr клип отключён
        v = clip_0_100(r) if rt.mtf_clip_0_100 else r

        if name is None:
            values_by_tf[str(tf)] = v
        else:
            block = values_by_tf.get(str(tf))
            if not isinstance(block, dict):
                block = {}
                values_by_tf[str(tf)] = block
            block[str(name)] = v

    # проверка полноты: для каждого TF должны быть все требуемые значения
    missing = False
    for tf in rt.mtf_component_tfs:
        spec = rt.mtf_component_params.get(tf)

        if isinstance(spec, str):
            if str(tf) not in values_by_tf:
                missing = True
                break
        elif isinstance(spec, dict):
            block = values_by_tf.get(str(tf))
            if not isinstance(block, dict):
                missing = True
                break
            for name in spec.keys():
                if str(name) not in block:
                    missing = True
                    break
            if missing:
                break
        else:
            missing = True
            break

    # если не собрали все TF — skip
    if errors or missing:
        boundary = calc_close_boundary_ts_ms(trigger_open_ts_ms, "m5")
        styk_m15 = is_tf_boundary(boundary, "m15")
        styk_h1 = is_tf_boundary(boundary, "h1")
        if styk_m15 or styk_h1:
            log.warning(
                "PACK_MTF: missing values after retry — skip (symbol=%s, trigger=%s/%s, analysis_id=%s, open_time=%s, boundary=%s, styk_m15=%s, styk_h1=%s)",
                symbol,
                trigger_tf,
                trigger_indicator,
                rt.analysis_id,
                trigger_open_ts_ms,
                boundary,
                styk_m15,
                styk_h1,
            )
        return

    # price (close) для воркеров, которые требуют price (lr_mtf)
    if rt.mtf_needs_price:
        price_key = f"{BB_TS_PREFIX}:{symbol}:{rt.mtf_price_tf}:{rt.mtf_price_field}"
        raw_price = await ts_get_value_at(redis, price_key, int(trigger_open_ts_ms))
        price_d = safe_decimal(raw_price)
        if price_d is None:
            return
        values_by_tf["price"] = price_d

    published = 0
    skipped = 0

    # считаем для каждой пары и каждого направления
    for (scenario_id, signal_id) in rt.mtf_pairs:
        for direction in ("long", "short"):
            rules_by_tf: dict[str, list[Any]] = {}

            # supertrend_mtf: правила лежат в timeframe='mtf'
            if str(rt.mtf_bins_tf) == "mtf":
                rules_by_tf["mtf"] = (rt.mtf_bins_static.get("mtf", {}) or {}).get(direction, []) or []
                if not rules_by_tf["mtf"]:
                    skipped += 1
                    continue

            # обычные MTF (rsi/mfi/rsimfi): правила по компонентным TF
            else:
                required_tfs = rt.mtf_required_bins_tfs or rt.mtf_component_tfs or []
                for tf in required_tfs:
                    rules_by_tf[str(tf)] = (rt.mtf_bins_static.get(str(tf), {}) or {}).get(direction, []) or []

                # условия достаточности static правил
                if any(not rules_by_tf.get(str(tf)) for tf in required_tfs):
                    skipped += 1
                    continue

            # quantiles rules (lr_mtf): берём из adaptive_quantiles_cache по (analysis_id, pair, "mtf", direction)
            if rt.mtf_quantiles_key:
                q_rules = adaptive_quantiles_cache.get(
                    (int(rt.analysis_id), int(scenario_id), int(signal_id), "mtf", str(direction)),
                    [],
                )
                if not q_rules:
                    skipped += 1
                    continue
                rules_by_tf[str(rt.mtf_quantiles_key)] = q_rules

            # кандидаты bin_name (full → схлопывания)
            try:
                try:
                    candidates = rt.worker.bin_candidates(values_by_tf=values_by_tf, rules_by_tf=rules_by_tf, direction=direction)
                except TypeError:
                    candidates = rt.worker.bin_candidates(values_by_tf=values_by_tf, rules_by_tf=rules_by_tf)
            except Exception:
                skipped += 1
                continue

            if not candidates:
                skipped += 1
                continue

            # выбрать первый кандидат, который существует в labels-cache
            chosen = None
            for cand in candidates:
                if labels_has_bin(
                    scenario_id=int(scenario_id),
                    signal_id=int(signal_id),
                    direction=str(direction),
                    analysis_id=int(rt.analysis_id),
                    indicator_param=str(rt.source_param_name),
                    timeframe="mtf",
                    bin_name=str(cand),
                ):
                    chosen = str(cand)
                    break

            if not chosen:
                skipped += 1
                continue

            # публикуем как pair-key (как adaptive), но timeframe="mtf"
            await publish_pack_state_adaptive(
                redis=redis,
                analysis_id=int(rt.analysis_id),
                scenario_id=int(scenario_id),
                signal_id=int(signal_id),
                direction=str(direction),
                symbol=symbol,
                timeframe="mtf",
                bin_name=chosen,
                ttl_sec=int(rt.ttl_sec),
            )
            published += 1

    # суммирующий лог на одно ready-событие
    if published or skipped:
        log.debug(
            "PACK_MTF: done (symbol=%s, trigger=%s/%s, analysis_id=%s, published=%s, skipped=%s)",
            symbol,
            trigger_tf,
            trigger_indicator,
            rt.analysis_id,
            published,
            skipped,
        )

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
        # MTF паки
        if rt.is_mtf:
            # условия достаточности
            if ts_ms is None:
                continue

            await handle_mtf_pack(
                redis=redis,
                rt=rt,
                symbol=symbol,
                trigger_open_ts_ms=int(ts_ms),
                trigger_tf=str(timeframe),
                trigger_indicator=str(indicator_key),
            )
            continue

        # value для воркера (single-TF)
        if rt.analysis_key == "bb_band_bin":
            value = await build_bb_band_value(redis, symbol, rt.timeframe, rt.source_param_name, ts_ms)
            if value is None:
                continue

        elif rt.analysis_key == "lr_band_bin":
            value = await build_lr_band_value(redis, symbol, rt.timeframe, rt.source_param_name, ts_ms)
            if value is None:
                continue

        elif rt.analysis_key == "atr_bin":
            value = await build_atr_pct_value(redis, symbol, rt.timeframe, rt.source_param_name, ts_ms)
            if value is None:
                continue

        elif rt.analysis_key == "dmigap_bin":
            value = await build_dmigap_value(redis, symbol, rt.timeframe, rt.source_param_name)
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
            publish_tasks = []

            for (scenario_id, signal_id) in rt.adaptive_pairs:
                for direction in ("long", "short"):
                    rules = get_adaptive_rules(rt.analysis_id, scenario_id, signal_id, rt.timeframe, direction)
                    if not rules:
                        continue

                    bin_name = rt.worker.bin_value(value=value, rules=rules)
                    if not bin_name:
                        continue

                    publish_tasks.append(
                        publish_pack_state_adaptive(
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
                    )

            if publish_tasks:
                await asyncio.gather(*publish_tasks, return_exceptions=True)

        else:
            publish_tasks = []
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

            if publish_tasks:
                await asyncio.gather(*publish_tasks, return_exceptions=True)


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
        # ограничение параллелизма
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

            # логируем только ошибки обработки (если были)
            errors = 0
            for r in results:
                if isinstance(r, Exception):
                    errors += 1
                    log.warning(f"PACK_STREAM: message processing error: {r}", exc_info=True)

            await redis.xack(INDICATOR_STREAM, IND_PACK_GROUP, *to_ack)

            # суммирующий лог по батчу
            if errors:
                log.info("PACK_STREAM: batch processed=%s, errors=%s", len(flat), errors)

        except Exception as e:
            log.error(f"PACK_STREAM loop error: {e}", exc_info=True)
            await asyncio.sleep(2)


# 🔸 Подписка на bt:analysis:postproc_ready и точечный reload adaptive-cache + labels-cache
async def watch_postproc_ready(pg, redis):
    log = logging.getLogger("PACK_POSTPROC")

    sem = asyncio.Semaphore(50)

    async def _reload_pair(scenario_id: int, signal_id: int):
        async with sem:
            pair = (scenario_id, signal_id)

            # adaptive reload
            analysis_ids = sorted(list(adaptive_pairs_index.get(pair, set())))
            if analysis_ids:
                loaded = await load_adaptive_bins_for_pair(pg, analysis_ids, scenario_id, signal_id)

                async with adaptive_lock:
                    # удалить старые ключи пары (только нужные analysis_id)
                    keys_to_del = [
                        k for k in list(adaptive_bins_cache.keys())
                        if k[1] == scenario_id and k[2] == signal_id and k[0] in analysis_ids
                    ]
                    for k in keys_to_del:
                        adaptive_bins_cache.pop(k, None)

                    # записать новые
                    loaded_rules = 0
                    for (aid, tf, direction), rules in loaded.items():
                        adaptive_bins_cache[(aid, scenario_id, signal_id, tf, direction)] = rules
                        loaded_rules += len(rules)

                log.info(
                    "PACK_ADAPTIVE: updated (scenario_id=%s, signal_id=%s, analysis_ids=%s, rules_loaded=%s)",
                    scenario_id,
                    signal_id,
                    analysis_ids,
                    loaded_rules,
                )

            # adaptive quantiles reload
            q_analysis_ids = sorted(list(adaptive_quantiles_pairs_index.get(pair, set())))
            if q_analysis_ids:
                loaded_q = await load_adaptive_quantiles_for_pair(pg, q_analysis_ids, scenario_id, signal_id)

                async with adaptive_lock:
                    keys_to_del = [
                        k for k in list(adaptive_quantiles_cache.keys())
                        if k[1] == scenario_id and k[2] == signal_id and k[0] in q_analysis_ids
                    ]
                    for k in keys_to_del:
                        adaptive_quantiles_cache.pop(k, None)

                    loaded_rules = 0
                    for (aid, tf, direction), rules in loaded_q.items():
                        adaptive_quantiles_cache[(aid, scenario_id, signal_id, tf, direction)] = rules
                        loaded_rules += len(rules)

                log.info(
                    "PACK_ADAPTIVE_QUANTILES: updated (scenario_id=%s, signal_id=%s, analysis_ids=%s, rules_loaded=%s)",
                    scenario_id,
                    signal_id,
                    q_analysis_ids,
                    loaded_rules,
                )

            # labels reload (без model_id)
            contexts = sorted(
                list(labels_pairs_index.get(pair, set())),
                key=lambda x: (x.analysis_id, x.indicator_param, x.timeframe),
            )
            if contexts:
                loaded_bins = await load_labels_bins_for_pair(pg, scenario_id, signal_id, contexts)

                async with labels_lock:
                    # удалить старые ключи пары и контекстов
                    ctx_set = {(c.analysis_id, c.indicator_param, c.timeframe) for c in contexts}
                    keys_to_del = [
                        k for k in list(labels_bins_cache.keys())
                        if k[0] == scenario_id and k[1] == signal_id and (k[3], k[4], k[5]) in ctx_set
                    ]
                    for k in keys_to_del:
                        labels_bins_cache.pop(k, None)

                    # записать новые
                    bins_loaded = 0
                    for k, s in loaded_bins.items():
                        labels_bins_cache[k] = s
                        bins_loaded += len(s)

                log.info(
                    "PACK_LABELS: updated (scenario_id=%s, signal_id=%s, ctx=%s, bins_loaded=%s)",
                    scenario_id,
                    signal_id,
                    len(contexts),
                    bins_loaded,
                )

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

                    # если пара не используется ни в adaptive, ни в labels — игнор
                    if pair not in adaptive_pairs_set and pair not in labels_pairs_set and pair not in adaptive_quantiles_pairs_set:
                        continue

                    # точечный reload по паре
                    asyncio.create_task(_reload_pair(scenario_id, signal_id))

            if to_ack:
                await redis.xack(POSTPROC_STREAM_KEY, POSTPROC_GROUP, *to_ack)

        except Exception as e:
            log.error(f"PACK_POSTPROC loop error: {e}", exc_info=True)
            await asyncio.sleep(2)


# 🔸 Загрузка активных тикеров (для bootstrap)
async def load_active_symbols(pg) -> list[str]:
    log = logging.getLogger("PACK_BOOT")

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

    log.info(f"PACK_BOOT: активных тикеров загружено: {len(symbols)}")
    return symbols


# 🔸 Холодный старт: пересчитать текущее состояние (без ожидания next ready)
async def bootstrap_current_state(pg, redis):
    log = logging.getLogger("PACK_BOOT")

    runtimes: list[PackRuntime] = []
    for lst in pack_registry.values():
        runtimes.extend(lst)

    if not runtimes:
        log.info("PACK_BOOT: нет активных pack-инстансов, bootstrap пропущен")
        return

    symbols = await load_active_symbols(pg)
    if not symbols:
        log.info("PACK_BOOT: нет активных тикеров, bootstrap пропущен")
        return

    sem = asyncio.Semaphore(BOOTSTRAP_MAX_PARALLEL)

    async def _process_one(symbol: str, rt: PackRuntime):
        async with sem:
            # MTF bootstrap осознанно пропускаем (требует trigger m5 + styk-логика + labels)
            if rt.is_mtf:
                return

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

            elif rt.analysis_key == "atr_bin":
                atr_ts_key = f"{IND_TS_PREFIX}:{symbol}:{rt.timeframe}:{rt.source_param_name}"
                atr = await ts_get(redis, atr_ts_key)
                if not atr:
                    return
                ts_ms, atr_val = atr

                close_key = f"{BB_TS_PREFIX}:{symbol}:{rt.timeframe}:c"
                close_val = await ts_get_value_at(redis, close_key, ts_ms)
                if close_val is None:
                    return

                value = {"atr": atr_val, "price": close_val}

            elif rt.analysis_key == "dmigap_bin":
                base = rt.source_param_name  # например adx_dmi14

                plus_ts_key = f"{IND_TS_PREFIX}:{symbol}:{rt.timeframe}:{base}_plus_di"
                plus = await ts_get(redis, plus_ts_key)
                if not plus:
                    return
                ts_ms, plus_val = plus

                minus_ts_key = f"{IND_TS_PREFIX}:{symbol}:{rt.timeframe}:{base}_minus_di"
                minus = await ts_get(redis, minus_ts_key)
                if not minus:
                    return

                minus_ts, minus_val = minus
                if minus_ts != ts_ms:
                    minus_at = await ts_get_value_at(redis, minus_ts_key, ts_ms)
                    if minus_at is None:
                        return
                    minus_val = minus_at

                value = {"plus": plus_val, "minus": minus_val}

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
                publish_tasks = []

                for (scenario_id, signal_id) in rt.adaptive_pairs:
                    for direction in ("long", "short"):
                        rules = get_adaptive_rules(rt.analysis_id, scenario_id, signal_id, rt.timeframe, direction)
                        if not rules:
                            continue

                        bin_name = rt.worker.bin_value(value=value, rules=rules)
                        if not bin_name:
                            continue

                        publish_tasks.append(
                            publish_pack_state_adaptive(
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
                        )

                if publish_tasks:
                    await asyncio.gather(*publish_tasks, return_exceptions=True)

            else:
                publish_tasks = []

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

                if publish_tasks:
                    await asyncio.gather(*publish_tasks, return_exceptions=True)

    tasks = []
    for rt in runtimes:
        for symbol in symbols:
            tasks.append(asyncio.create_task(_process_one(symbol, rt)))

    await asyncio.gather(*tasks, return_exceptions=True)
    log.info(f"PACK_BOOT: bootstrap завершён — packs={len(runtimes)}, symbols={len(symbols)}")

# 🔸 Инициализация кэша и реестра pack-воркеров
async def init_pack_runtime(pg):
    global pack_registry, adaptive_pairs_index, adaptive_pairs_set, adaptive_quantiles_pairs_index, adaptive_quantiles_pairs_set, labels_pairs_index, labels_pairs_set

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

    # adaptive index
    adaptive_pairs_index = {}
    adaptive_pairs_set = set()

    adaptive_quantiles_pairs_index = {}
    adaptive_quantiles_pairs_set = set()

    # labels index
    labels_pairs_index = {}
    labels_pairs_set = set()

    all_runtimes: list[PackRuntime] = []
    for lst in pack_registry.values():
        all_runtimes.extend(lst)

    adaptive_runtimes = 0
    labels_runtimes = 0

    for rt in all_runtimes:
        # adaptive (bins)
        if rt.bins_source == "adaptive":
            adaptive_runtimes += 1
            for pair in rt.adaptive_pairs:
                adaptive_pairs_set.add(pair)
                adaptive_pairs_index.setdefault(pair, set()).add(int(rt.analysis_id))

        # quantiles (для lr_mtf и любых mtf, кто просит quantiles_key)
        if rt.is_mtf and rt.mtf_pairs and rt.mtf_quantiles_key:
            for pair in rt.mtf_pairs:
                adaptive_quantiles_pairs_set.add(pair)
                adaptive_quantiles_pairs_index.setdefault(pair, set()).add(int(rt.analysis_id))

        # labels (для всех mtf паков)
        if rt.is_mtf and rt.mtf_pairs:
            labels_runtimes += 1
            for pair in rt.mtf_pairs:
                labels_pairs_set.add(pair)
                ctx = LabelsContext(
                    analysis_id=int(rt.analysis_id),
                    indicator_param=str(rt.source_param_name),
                    timeframe="mtf",
                )
                labels_pairs_index.setdefault(pair, set()).add(ctx)

    log.info(f"PACK_INIT: adaptive pairs configured: {len(adaptive_pairs_set)} (adaptive_runtimes={adaptive_runtimes})")
    log.info(f"PACK_INIT: labels pairs configured: {len(labels_pairs_set)} (labels_runtimes={labels_runtimes})")

    # первичная загрузка adaptive-кеша (bins)
    loaded_pairs = 0
    loaded_rules_total = 0

    for (scenario_id, signal_id) in sorted(list(adaptive_pairs_set)):
        analysis_list = sorted(list(adaptive_pairs_index.get((scenario_id, signal_id), set())))
        if not analysis_list:
            continue

        loaded = await load_adaptive_bins_for_pair(pg, analysis_list, scenario_id, signal_id)

        async with adaptive_lock:
            rules_loaded = 0
            for (aid, tf, direction), rules in loaded.items():
                adaptive_bins_cache[(aid, scenario_id, signal_id, tf, direction)] = rules
                rules_loaded += len(rules)

        loaded_pairs += 1
        loaded_rules_total += rules_loaded

        log.info(
            "PACK_INIT: adaptive dict loaded for scenario_id=%s, signal_id=%s, analysis_ids=%s, rules_loaded=%s",
            scenario_id,
            signal_id,
            analysis_list,
            rules_loaded,
        )

    if adaptive_pairs_set:
        log.info(f"PACK_INIT: adaptive cache ready — pairs_loaded={loaded_pairs}, rules_total={loaded_rules_total}")

    # первичная загрузка adaptive-quantiles кеша (bin_type='quantiles')
    loaded_q_pairs = 0
    loaded_q_rules_total = 0

    for (scenario_id, signal_id) in sorted(list(adaptive_quantiles_pairs_set)):
        analysis_list = sorted(list(adaptive_quantiles_pairs_index.get((scenario_id, signal_id), set())))
        if not analysis_list:
            continue

        loaded = await load_adaptive_quantiles_for_pair(pg, analysis_list, scenario_id, signal_id)

        async with adaptive_lock:
            rules_loaded = 0
            for (aid, tf, direction), rules in loaded.items():
                adaptive_quantiles_cache[(aid, scenario_id, signal_id, tf, direction)] = rules
                rules_loaded += len(rules)
            loaded_q_rules_total += rules_loaded

        loaded_q_pairs += 1
        log.info(
            "PACK_INIT: adaptive quantiles loaded for scenario_id=%s, signal_id=%s, analysis_ids=%s, rules_loaded=%s",
            scenario_id,
            signal_id,
            analysis_list,
            rules_loaded,
        )

    if adaptive_quantiles_pairs_set:
        log.info("PACK_INIT: adaptive quantiles cache ready — pairs_loaded=%s, rules_total=%s", loaded_q_pairs, loaded_q_rules_total)

    # первичная загрузка labels-кеша
    loaded_pairs_lbl = 0
    loaded_bins_total = 0

    for (scenario_id, signal_id) in sorted(list(labels_pairs_set)):
        contexts = sorted(
            list(labels_pairs_index.get((scenario_id, signal_id), set())),
            key=lambda x: (x.analysis_id, x.indicator_param, x.timeframe),
        )
        if not contexts:
            continue

        loaded = await load_labels_bins_for_pair(pg, scenario_id, signal_id, contexts)

        async with labels_lock:
            for k, s in loaded.items():
                labels_bins_cache[k] = s
                loaded_bins_total += len(s)

        loaded_pairs_lbl += 1
        log.info(
            "PACK_INIT: labels cache loaded for scenario_id=%s, signal_id=%s, ctx=%s, keys=%s",
            scenario_id,
            signal_id,
            len(contexts),
            len(loaded),
        )

    if labels_pairs_set:
        log.info(f"PACK_INIT: labels cache ready — pairs_loaded={loaded_pairs_lbl}, bins_total={loaded_bins_total}")

# 🔸 Внешняя точка входа (запускается через indicators_v4_main.py и run_safe_loop)
async def run_indicator_pack(pg, redis):
    # первичная загрузка
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