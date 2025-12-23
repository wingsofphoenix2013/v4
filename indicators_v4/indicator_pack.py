# indicator_pack.py — оркестратор расчёта и публикации ind_pack (JSON Contract v1): static/adaptive/MTF + кеши rules/labels

# 🔸 Базовые импорты
import asyncio
import json
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

# 🔸 Imports: packs_config (models + contract)
from packs_config.models import BinRule, LabelsContext, PackRuntime
from packs_config.contract import (
    pack_ok,
    pack_fail,
    short_error_str,
    build_fail_details_base,
    parse_open_time_to_open_ts_ms,
)

# 🔸 Импорт pack-воркеров
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

# 🔸 Ограничения диагностики (не заливаем Redis)
MAX_CANDIDATES_IN_DETAILS = 5
MAX_ERROR_STR_LEN = 400

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

# 🔸 Кеши правил
adaptive_bins_cache: dict[tuple[int, int, int, str, str], list["BinRule"]] = {}
adaptive_quantiles_cache: dict[tuple[int, int, int, str, str], list["BinRule"]] = {}
labels_bins_cache: dict[tuple[int, int, str, int, str, str], set[str]] = {}

# 🔸 Индексы и быстрые множества для reload по парам
adaptive_pairs_index: dict[tuple[int, int], set[int]] = {}
adaptive_pairs_set: set[tuple[int, int]] = set()

adaptive_quantiles_pairs_index: dict[tuple[int, int], set[int]] = {}
adaptive_quantiles_pairs_set: set[tuple[int, int]] = set()

labels_pairs_index: dict[tuple[int, int], set["LabelsContext"]] = {}
labels_pairs_set: set[tuple[int, int]] = set()

# 🔸 Locks и флаги готовности кешей
adaptive_lock = asyncio.Lock()
labels_lock = asyncio.Lock()

caches_ready = {
    "registry": False,
    "adaptive_bins": False,
    "quantiles": False,
    "labels": False,
}

# 🔸 Статусы перезагрузки пар
reloading_pairs_bins: set[tuple[int, int]] = set()
reloading_pairs_quantiles: set[tuple[int, int]] = set()
reloading_pairs_labels: set[tuple[int, int]] = set()

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


# 🔸 MTF boundary helpers
def is_tf_boundary(ts_ms: int, tf: str) -> bool:
    step = TF_STEP_MS.get(tf)
    if not step:
        return False
    return (int(ts_ms) % int(step)) == 0


def calc_close_boundary_ts_ms(open_ts_ms: int, tf: str) -> int:
    step = TF_STEP_MS.get(tf)
    if not step:
        return int(open_ts_ms)
    return int(open_ts_ms) + int(step)


def just_closed_open_time(boundary_ts_ms: int, tf: str) -> int:
    return int(boundary_ts_ms) - int(TF_STEP_MS[tf])


# 🔸 KV/TS indicator getters for MTF
async def get_kv_decimal(redis, symbol: str, tf: str, param_name: str) -> tuple[Decimal | None, str | None]:
    key = f"ind:{symbol}:{tf}:{param_name}"
    raw = await redis.get(key)
    if raw is None:
        return None, None
    return safe_decimal(raw), str(raw)


async def get_ts_decimal_with_retry(redis, symbol: str, tf: str, param_name: str, open_ts_ms: int) -> tuple[Decimal | None, str | None, int]:
    key = f"{IND_TS_PREFIX}:{symbol}:{tf}:{param_name}"
    waited = 0
    raw = None

    while waited <= MTF_RETRY_TOTAL_SEC:
        raw = await ts_get_value_at(redis, key, int(open_ts_ms))
        d = safe_decimal(raw)
        if d is not None:
            return d, (str(raw) if raw is not None else None), waited

        # таймаут достигнут
        if waited >= MTF_RETRY_TOTAL_SEC:
            break

        await asyncio.sleep(MTF_RETRY_STEP_SEC)
        waited += MTF_RETRY_STEP_SEC

    return None, (str(raw) if raw is not None else None), waited


async def get_mtf_value_decimal(redis, symbol: str, trigger_open_ts_ms: int, target_tf: str, param_name: str) -> tuple[Decimal | None, str | None, dict[str, Any]]:
    meta: dict[str, Any] = {"styk": False, "waited_sec": 0, "target_tf": str(target_tf)}

    # m5 — событие ready уже гарантирует актуальность значения для этого open_time
    if target_tf == "m5":
        d, raw = await get_kv_decimal(redis, symbol, "m5", param_name)
        return d, raw, meta

    # граница закрытия m5-бара
    boundary = calc_close_boundary_ts_ms(trigger_open_ts_ms, "m5")

    # если boundary не является границей target_tf — target_tf не пересчитывается сейчас, KV безопасен
    if not is_tf_boundary(boundary, target_tf):
        d, raw = await get_kv_decimal(redis, symbol, target_tf, param_name)
        return d, raw, meta

    # styk TF: нужен «свежий» бар target_tf, который только что закрылся на boundary
    meta["styk"] = True
    meta["boundary_open_ts_ms"] = int(boundary)

    target_open = just_closed_open_time(boundary, target_tf)
    d, raw, waited = await get_ts_decimal_with_retry(redis, symbol, target_tf, param_name, target_open)

    meta["target_open_ts_ms"] = int(target_open)
    meta["waited_sec"] = int(waited)
    return d, raw, meta


# 🔸 Helpers: labels cache key + contains
def labels_cache_key(
    scenario_id: int,
    signal_id: int,
    direction: str,
    analysis_id: int,
    indicator_param: str,
    timeframe: str,
) -> tuple[int, int, str, int, str, str]:
    return (int(scenario_id), int(signal_id), str(direction), int(analysis_id), str(indicator_param), str(timeframe))


def labels_has_bin(
    scenario_id: int,
    signal_id: int,
    direction: str,
    analysis_id: int,
    indicator_param: str,
    timeframe: str,
    bin_name: str,
) -> bool:
    s = labels_bins_cache.get(labels_cache_key(scenario_id, signal_id, direction, analysis_id, indicator_param, timeframe))
    if not s:
        return False
    return str(bin_name) in s


# 🔸 Helpers: bins_policy parsing
def get_bins_source(bins_policy: dict[str, Any] | None, timeframe: str) -> str:
    if not isinstance(bins_policy, dict):
        return "static"
    try:
        if "by_tf" in bins_policy:
            by_tf = bins_policy.get("by_tf") or {}
            return str(by_tf.get(timeframe) or bins_policy.get("default") or "static")
        return str(bins_policy.get(timeframe) or bins_policy.get("default") or "static")
    except Exception:
        return "static"


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
            out.append((int(item.get("scenario_id")), int(item.get("signal_id"))))
        except Exception:
            continue

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


# 🔸 Publish helpers (JSON)
async def publish_static(redis, analysis_id: int, direction: str, symbol: str, timeframe: str, payload_json: str, ttl_sec: int):
    key = f"{IND_PACK_PREFIX}:{analysis_id}:{direction}:{symbol}:{timeframe}"
    await redis.set(key, payload_json, ex=int(ttl_sec))


async def publish_pair(redis, analysis_id: int, scenario_id: int, signal_id: int, direction: str, symbol: str, timeframe: str, payload_json: str, ttl_sec: int):
    key = f"{IND_PACK_PREFIX}:{analysis_id}:{scenario_id}:{signal_id}:{direction}:{symbol}:{timeframe}"
    await redis.set(key, payload_json, ex=int(ttl_sec))


# 🔸 Value builders (single-TF)
async def build_bb_band_value(redis, symbol: str, timeframe: str, bb_prefix: str, open_ts_ms: int | None) -> tuple[dict[str, str] | None, list[Any]]:
    missing: list[Any] = []

    upper_key = f"ind:{symbol}:{timeframe}:{bb_prefix}_upper"
    lower_key = f"ind:{symbol}:{timeframe}:{bb_prefix}_lower"

    upper_val = await redis.get(upper_key)
    lower_val = await redis.get(lower_key)

    if upper_val is None:
        missing.append(f"{bb_prefix}_upper")
    if lower_val is None:
        missing.append(f"{bb_prefix}_lower")

    if open_ts_ms is None:
        missing.append({"tf": timeframe, "field": "c", "source": "bb:ts", "open_ts_ms": None})
        return None, missing

    close_key = f"{BB_TS_PREFIX}:{symbol}:{timeframe}:c"
    close_val = await ts_get_value_at(redis, close_key, int(open_ts_ms))
    if close_val is None:
        missing.append({"tf": timeframe, "field": "c", "source": "bb:ts", "open_ts_ms": int(open_ts_ms)})

    if missing:
        return None, missing

    return {"price": str(close_val), "upper": str(upper_val), "lower": str(lower_val)}, []


async def build_lr_band_value(redis, symbol: str, timeframe: str, lr_prefix: str, open_ts_ms: int | None) -> tuple[dict[str, str] | None, list[Any]]:
    missing: list[Any] = []

    upper_key = f"ind:{symbol}:{timeframe}:{lr_prefix}_upper"
    lower_key = f"ind:{symbol}:{timeframe}:{lr_prefix}_lower"

    upper_val = await redis.get(upper_key)
    lower_val = await redis.get(lower_key)

    if upper_val is None:
        missing.append(f"{lr_prefix}_upper")
    if lower_val is None:
        missing.append(f"{lr_prefix}_lower")

    if open_ts_ms is None:
        missing.append({"tf": timeframe, "field": "c", "source": "bb:ts", "open_ts_ms": None})
        return None, missing

    close_key = f"{BB_TS_PREFIX}:{symbol}:{timeframe}:c"
    close_val = await ts_get_value_at(redis, close_key, int(open_ts_ms))
    if close_val is None:
        missing.append({"tf": timeframe, "field": "c", "source": "bb:ts", "open_ts_ms": int(open_ts_ms)})

    if missing:
        return None, missing

    return {"price": str(close_val), "upper": str(upper_val), "lower": str(lower_val)}, []


async def build_atr_pct_value(redis, symbol: str, timeframe: str, atr_param_name: str, open_ts_ms: int | None) -> tuple[dict[str, str] | None, list[Any]]:
    missing: list[Any] = []

    atr_key = f"ind:{symbol}:{timeframe}:{atr_param_name}"
    atr_val = await redis.get(atr_key)
    if atr_val is None:
        missing.append(str(atr_param_name))

    if open_ts_ms is None:
        missing.append({"tf": timeframe, "field": "c", "source": "bb:ts", "open_ts_ms": None})
        return None, missing

    close_key = f"{BB_TS_PREFIX}:{symbol}:{timeframe}:c"
    close_val = await ts_get_value_at(redis, close_key, int(open_ts_ms))
    if close_val is None:
        missing.append({"tf": timeframe, "field": "c", "source": "bb:ts", "open_ts_ms": int(open_ts_ms)})

    if missing:
        return None, missing

    return {"atr": str(atr_val), "price": str(close_val)}, []


async def build_dmigap_value(redis, symbol: str, timeframe: str, base_param_name: str) -> tuple[dict[str, str] | None, list[Any]]:
    missing: list[Any] = []

    plus_key = f"ind:{symbol}:{timeframe}:{base_param_name}_plus_di"
    minus_key = f"ind:{symbol}:{timeframe}:{base_param_name}_minus_di"

    plus_val = await redis.get(plus_key)
    minus_val = await redis.get(minus_key)

    if plus_val is None:
        missing.append(f"{base_param_name}_plus_di")
    if minus_val is None:
        missing.append(f"{base_param_name}_minus_di")

    if missing:
        return None, missing

    return {"plus": str(plus_val), "minus": str(minus_val)}, []


# 🔸 DB loaders: packs / analyzers / params / rules / labels
async def load_enabled_packs(pg) -> list[dict[str, Any]]:
    log = logging.getLogger("PACK_INIT")
    async with pg.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT id, analysis_id, bins_policy, enabled_at
            FROM {PACK_INSTANCES_TABLE}
            WHERE enabled = true
        """)
    packs: list[dict[str, Any]] = []
    parsed = 0
    for r in rows:
        policy = r["bins_policy"]
        if isinstance(policy, str):
            try:
                policy = json.loads(policy)
                parsed += 1
            except Exception:
                policy = None
        packs.append({"id": int(r["id"]), "analysis_id": int(r["analysis_id"]), "bins_policy": policy, "enabled_at": r["enabled_at"]})
    log.info("PACK_INIT: включённых pack-инстансов загружено: %s (bins_policy parsed_from_str=%s)", len(packs), parsed)
    return packs


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
    log.info("PACK_INIT: bt_analysis_instances загружено: %s", len(out))
    return out


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
        params.setdefault(aid, {})[str(r["param_name"])] = str(r["param_value"])
    ok = 0
    missing = 0
    for aid in analysis_ids:
        if (params.get(aid) or {}).get("param_name"):
            ok += 1
        else:
            missing += 1
    log.info("PACK_INIT: bt_analysis_parameters (param_name) OK=%s, missing=%s", ok, missing)
    return params


async def load_static_bins_dict(pg, analysis_ids: list[int]) -> dict[int, dict[str, dict[str, list[BinRule]]]]:
    log = logging.getLogger("PACK_INIT")
    if not analysis_ids:
        return {}
    async with pg.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT analysis_id, direction, timeframe, bin_type, bin_order, bin_name, val_from, val_to, to_inclusive
            FROM {BINS_DICT_TABLE}
            WHERE analysis_id = ANY($1::int[])
              AND bin_type = 'bins'
        """, analysis_ids)
    out: dict[int, dict[str, dict[str, list[BinRule]]]] = {}
    total = 0
    for r in rows:
        aid = int(r["analysis_id"])
        direction = str(r["direction"])
        tf = str(r["timeframe"])
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
        out.setdefault(aid, {}).setdefault(tf, {}).setdefault(direction, []).append(rule)
        total += 1
    for aid in out:
        for tf in out[aid]:
            for direction in out[aid][tf]:
                out[aid][tf][direction].sort(key=lambda x: x.bin_order)
    log.info("PACK_INIT: static bins загружено: rules=%s", total)
    return out


async def load_adaptive_bins_for_pair(pg, analysis_ids: list[int], scenario_id: int, signal_id: int, bin_type: str) -> dict[tuple[int, str, str], list[BinRule]]:
    if not analysis_ids:
        return {}
    async with pg.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT analysis_id, direction, timeframe, bin_type, bin_order, bin_name, val_from, val_to, to_inclusive
            FROM {ADAPTIVE_BINS_TABLE}
            WHERE analysis_id = ANY($1::int[])
              AND scenario_id = $2
              AND signal_id   = $3
              AND bin_type    = $4
            ORDER BY analysis_id, timeframe, direction, bin_order
        """, analysis_ids, int(scenario_id), int(signal_id), str(bin_type))
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


async def load_labels_bins_for_pair(pg, scenario_id: int, signal_id: int, contexts: list[LabelsContext]) -> dict[tuple[int, int, str, int, str, str], set[str]]:
    if not contexts:
        return {}
    analysis_ids = sorted({c.analysis_id for c in contexts})
    indicator_params = sorted({c.indicator_param for c in contexts})
    timeframes = sorted({c.timeframe for c in contexts})

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT scenario_id, signal_id, direction, analysis_id, indicator_param, timeframe, bin_name
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


# 🔸 Get adaptive rules
def get_adaptive_rules(analysis_id: int, scenario_id: int, signal_id: int, timeframe: str, direction: str) -> list[BinRule]:
    return adaptive_bins_cache.get((analysis_id, scenario_id, signal_id, timeframe, direction), [])


# 🔸 Registry builder
def build_pack_registry(
    packs: list[dict[str, Any]],
    analysis_meta: dict[int, dict[str, Any]],
    analysis_params: dict[int, dict[str, str]],
    static_bins_dict: dict[int, dict[str, dict[str, list[BinRule]]]],
) -> dict[tuple[str, str], list[PackRuntime]]:
    log = logging.getLogger("PACK_INIT")

    registry: dict[tuple[str, str], list[PackRuntime]] = {}
    total = 0
    cnt_static = 0
    cnt_adaptive = 0
    cnt_mtf = 0

    for pack in packs:
        analysis_id = int(pack["analysis_id"])
        meta = analysis_meta.get(analysis_id)
        params = analysis_params.get(analysis_id, {})

        if not meta:
            log.warning("PACK_INIT: analysis_id=%s пропущен: нет записи в bt_analysis_instances", analysis_id)
            continue
        if not bool(meta.get("enabled", True)):
            log.warning("PACK_INIT: analysis_id=%s пропущен: bt_analysis_instances.enabled=false", analysis_id)
            continue

        analysis_key = str(meta["key"])
        analysis_name = str(meta["name"])
        family_key = str(meta["family_key"])

        source_param_name = str(params.get("param_name") or "").strip()
        tf = str(params.get("tf") or "").strip()

        # MTF: допускаем tf отсутствует
        if not tf and (source_param_name.lower().endswith("_mtf") or analysis_key.lower().endswith("_mtf")):
            tf = "mtf"

        # условия достаточности
        if not tf or not source_param_name:
            log.warning("PACK_INIT: analysis_id=%s (%s) пропущен: нет tf/param_name", analysis_id, analysis_key)
            continue

        worker_cls = PACK_WORKERS.get(analysis_key)
        if worker_cls is None:
            log.warning("PACK_INIT: analysis_id=%s пропущен: воркер для key='%s' не найден", analysis_id, analysis_key)
            continue

        bins_policy = pack.get("bins_policy")
        bins_source = get_bins_source(bins_policy, tf)

        # MTF runtime
        is_mtf = (tf.lower() == "mtf") or source_param_name.lower().endswith("_mtf")
        if is_mtf:
            cnt_mtf += 1

            pairs = get_pairs(bins_policy)
            if not pairs:
                log.warning("PACK_INIT: analysis_id=%s (%s) mtf: bins_policy.pairs пустой — пропущен", analysis_id, analysis_key)
                continue

            worker = worker_cls()
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
                log.warning("PACK_INIT: analysis_id=%s (%s) mtf: некорректный mtf_config()", analysis_id, analysis_key)
                continue

            component_params: dict[str, Any] = {}
            comp_params_cfg = cfg.get("component_params")

            if isinstance(comp_params_cfg, dict):
                for ctf in component_tfs:
                    v = comp_params_cfg.get(ctf)
                    if isinstance(v, dict):
                        component_params[str(ctf)] = {str(k): str(val) for k, val in v.items() if k and val}
                    elif v:
                        component_params[str(ctf)] = str(v)

            for ctf in component_tfs:
                component_params.setdefault(str(ctf), component_param)

            bins_tf_key = str(cfg.get("bins_tf") or "").strip().lower() or "components"
            mtf_clip_0_100 = True if cfg.get("clip_0_100") is None else bool(cfg.get("clip_0_100"))

            mtf_bins_static: dict[str, dict[str, list[BinRule]]] = {}
            if bins_tf_key == "mtf":
                bins_mtf = static_bins_dict.get(analysis_id, {}).get("mtf", {})
                mtf_bins_static["mtf"] = {"long": bins_mtf.get("long", []), "short": bins_mtf.get("short", [])}
            else:
                for ctf in component_tfs:
                    bins_tf = static_bins_dict.get(analysis_id, {}).get(str(ctf), {})
                    mtf_bins_static[str(ctf)] = {"long": bins_tf.get("long", []), "short": bins_tf.get("short", [])}

            required_bins_tfs = cfg.get("required_bins_tfs")
            if not isinstance(required_bins_tfs, list) or not required_bins_tfs:
                required_bins_tfs = component_tfs

            quantiles_key = cfg.get("quantiles_key")
            quantiles_key = str(quantiles_key).strip() if quantiles_key is not None else None
            if quantiles_key == "":
                quantiles_key = None

            needs_price = bool(cfg.get("needs_price", False))
            price_tf = str(cfg.get("price_tf") or "m5").strip()
            price_field = str(cfg.get("price_field") or "c").strip()

            ttl_sec = int(TTL_BY_TF_SEC.get("mtf", TTL_BY_TF_SEC["m5"]))

            rt = PackRuntime(
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
                mtf_component_tfs=[str(x) for x in component_tfs],
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

            stream_indicator = get_stream_indicator_key(family_key, component_param)
            registry.setdefault((trigger_tf, stream_indicator), []).append(rt)
            total += 1
            continue

        # Single-TF runtime
        ttl_sec = int(TTL_BY_TF_SEC.get(tf, 60))
        adaptive_pairs: list[tuple[int, int]] = []
        bins_by_direction: dict[str, list[BinRule]] = {"long": [], "short": []}

        if bins_source == "adaptive":
            adaptive_pairs = get_pairs(bins_policy)
            if not adaptive_pairs:
                log.warning("PACK_INIT: analysis_id=%s (%s) adaptive: pairs пустой — пропущен", analysis_id, analysis_key)
                continue
            cnt_adaptive += 1
        else:
            bins_tf = static_bins_dict.get(analysis_id, {}).get(tf, {})
            bins_by_direction = {"long": bins_tf.get("long", []), "short": bins_tf.get("short", [])}
            cnt_static += 1

        rt = PackRuntime(
            analysis_id=analysis_id,
            analysis_key=analysis_key,
            analysis_name=analysis_name,
            family_key=family_key,
            timeframe=tf,
            source_param_name=source_param_name,
            bins_policy=bins_policy,
            bins_source=bins_source,
            adaptive_pairs=adaptive_pairs,
            bins_by_direction=bins_by_direction,
            ttl_sec=ttl_sec,
            worker=worker_cls(),
        )

        stream_indicator = get_stream_indicator_key(family_key, source_param_name)
        registry.setdefault((tf, stream_indicator), []).append(rt)
        total += 1

    log.info(
        "PACK_INIT: registry построен — match_keys=%s, runtimes_total=%s, static=%s, adaptive=%s, mtf=%s",
        len(registry),
        total,
        cnt_static,
        cnt_adaptive,
        cnt_mtf,
    )
    return registry


# 🔸 Consumer-group helper
async def ensure_stream_group(redis, stream: str, group: str):
    log = logging.getLogger("PACK_STREAM")
    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning("xgroup_create error for %s/%s: %s", stream, group, e)


# 🔸 Cache init + indexes build
async def init_pack_runtime(pg):
    global pack_registry, adaptive_pairs_index, adaptive_pairs_set, adaptive_quantiles_pairs_index, adaptive_quantiles_pairs_set, labels_pairs_index, labels_pairs_set

    log = logging.getLogger("PACK_INIT")

    caches_ready["registry"] = False
    caches_ready["adaptive_bins"] = False
    caches_ready["quantiles"] = False
    caches_ready["labels"] = False

    # очистка кешей и флагов reload на старте
    adaptive_bins_cache.clear()
    adaptive_quantiles_cache.clear()
    labels_bins_cache.clear()

    reloading_pairs_bins.clear()
    reloading_pairs_quantiles.clear()
    reloading_pairs_labels.clear()

    packs = await load_enabled_packs(pg)
    analysis_ids = sorted({int(p["analysis_id"]) for p in packs})

    analysis_meta = await load_analysis_instances(pg, analysis_ids)
    analysis_params = await load_analysis_parameters(pg, analysis_ids)
    static_bins_dict = await load_static_bins_dict(pg, analysis_ids)

    pack_registry = build_pack_registry(packs, analysis_meta, analysis_params, static_bins_dict)
    caches_ready["registry"] = True

    # reset indices
    adaptive_pairs_index = {}
    adaptive_pairs_set = set()

    adaptive_quantiles_pairs_index = {}
    adaptive_quantiles_pairs_set = set()

    labels_pairs_index = {}
    labels_pairs_set = set()

    all_runtimes: list[PackRuntime] = []
    for lst in pack_registry.values():
        all_runtimes.extend(lst)

    # build indices
    adaptive_runtimes = 0
    mtf_runtimes = 0

    for rt in all_runtimes:
        if rt.bins_source == "adaptive":
            adaptive_runtimes += 1
            for pair in rt.adaptive_pairs:
                adaptive_pairs_set.add(pair)
                adaptive_pairs_index.setdefault(pair, set()).add(int(rt.analysis_id))

        if rt.is_mtf and rt.mtf_pairs:
            mtf_runtimes += 1
            for pair in rt.mtf_pairs:
                labels_pairs_set.add(pair)
                ctx = LabelsContext(analysis_id=int(rt.analysis_id), indicator_param=str(rt.source_param_name), timeframe="mtf")
                labels_pairs_index.setdefault(pair, set()).add(ctx)

        if rt.is_mtf and rt.mtf_pairs and rt.mtf_quantiles_key:
            for pair in rt.mtf_pairs:
                adaptive_quantiles_pairs_set.add(pair)
                adaptive_quantiles_pairs_index.setdefault(pair, set()).add(int(rt.analysis_id))

    log.info("PACK_INIT: adaptive pairs configured: %s (adaptive_runtimes=%s)", len(adaptive_pairs_set), adaptive_runtimes)
    log.info("PACK_INIT: labels pairs configured: %s (mtf_runtimes=%s)", len(labels_pairs_set), mtf_runtimes)

    # первичная загрузка adaptive bins cache
    loaded_pairs = 0
    loaded_rules_total = 0

    for (scenario_id, signal_id) in sorted(list(adaptive_pairs_set)):
        analysis_list = sorted(list(adaptive_pairs_index.get((scenario_id, signal_id), set())))
        if not analysis_list:
            continue
        loaded = await load_adaptive_bins_for_pair(pg, analysis_list, scenario_id, signal_id, "bins")

        async with adaptive_lock:
            rules_loaded = 0
            for (aid, tf, direction), rules in loaded.items():
                adaptive_bins_cache[(aid, scenario_id, signal_id, tf, direction)] = rules
                rules_loaded += len(rules)

        loaded_pairs += 1
        loaded_rules_total += rules_loaded

    caches_ready["adaptive_bins"] = True
    log.info("PACK_INIT: adaptive cache ready — pairs_loaded=%s, rules_total=%s", loaded_pairs, loaded_rules_total)

    # первичная загрузка quantiles cache
    loaded_q_pairs = 0
    loaded_q_rules_total = 0

    for (scenario_id, signal_id) in sorted(list(adaptive_quantiles_pairs_set)):
        analysis_list = sorted(list(adaptive_quantiles_pairs_index.get((scenario_id, signal_id), set())))
        if not analysis_list:
            continue
        loaded = await load_adaptive_bins_for_pair(pg, analysis_list, scenario_id, signal_id, "quantiles")

        async with adaptive_lock:
            rules_loaded = 0
            for (aid, tf, direction), rules in loaded.items():
                adaptive_quantiles_cache[(aid, scenario_id, signal_id, tf, direction)] = rules
                rules_loaded += len(rules)

        loaded_q_pairs += 1
        loaded_q_rules_total += rules_loaded

    caches_ready["quantiles"] = True
    log.info("PACK_INIT: adaptive quantiles cache ready — pairs_loaded=%s, rules_total=%s", loaded_q_pairs, loaded_q_rules_total)

    # первичная загрузка labels cache
    loaded_lbl_pairs = 0
    loaded_lbl_keys = 0

    for (scenario_id, signal_id) in sorted(list(labels_pairs_set)):
        contexts = sorted(list(labels_pairs_index.get((scenario_id, signal_id), set())), key=lambda x: (x.analysis_id, x.indicator_param, x.timeframe))
        if not contexts:
            continue

        loaded = await load_labels_bins_for_pair(pg, scenario_id, signal_id, contexts)

        async with labels_lock:
            for k, s in loaded.items():
                labels_bins_cache[k] = s

        loaded_lbl_pairs += 1
        loaded_lbl_keys += len(loaded)

    caches_ready["labels"] = True
    log.info("PACK_INIT: labels cache ready — pairs_loaded=%s, keys=%s", loaded_lbl_pairs, loaded_lbl_keys)


# 🔸 Reload on postproc_ready
async def watch_postproc_ready(pg, redis):
    log = logging.getLogger("PACK_POSTPROC")
    sem = asyncio.Semaphore(50)

    async def _reload_pair(scenario_id: int, signal_id: int):
        async with sem:
            pair = (int(scenario_id), int(signal_id))

            reloading_pairs_bins.add(pair)
            reloading_pairs_quantiles.add(pair)
            reloading_pairs_labels.add(pair)

            try:
                # bins reload
                analysis_ids = sorted(list(adaptive_pairs_index.get(pair, set())))
                if analysis_ids:
                    loaded = await load_adaptive_bins_for_pair(pg, analysis_ids, scenario_id, signal_id, "bins")

                    async with adaptive_lock:
                        to_del = [k for k in list(adaptive_bins_cache.keys()) if k[1] == scenario_id and k[2] == signal_id and k[0] in analysis_ids]
                        for k in to_del:
                            adaptive_bins_cache.pop(k, None)

                        loaded_rules = 0
                        for (aid, tf, direction), rules in loaded.items():
                            adaptive_bins_cache[(aid, scenario_id, signal_id, tf, direction)] = rules
                            loaded_rules += len(rules)

                    log.info("PACK_ADAPTIVE: updated (scenario_id=%s, signal_id=%s, analysis_ids=%s, rules_loaded=%s)", scenario_id, signal_id, analysis_ids, loaded_rules)

                # quantiles reload
                q_analysis_ids = sorted(list(adaptive_quantiles_pairs_index.get(pair, set())))
                if q_analysis_ids:
                    loaded_q = await load_adaptive_bins_for_pair(pg, q_analysis_ids, scenario_id, signal_id, "quantiles")

                    async with adaptive_lock:
                        to_del = [k for k in list(adaptive_quantiles_cache.keys()) if k[1] == scenario_id and k[2] == signal_id and k[0] in q_analysis_ids]
                        for k in to_del:
                            adaptive_quantiles_cache.pop(k, None)

                        loaded_rules = 0
                        for (aid, tf, direction), rules in loaded_q.items():
                            adaptive_quantiles_cache[(aid, scenario_id, signal_id, tf, direction)] = rules
                            loaded_rules += len(rules)

                    log.info("PACK_ADAPTIVE_QUANTILES: updated (scenario_id=%s, signal_id=%s, analysis_ids=%s, rules_loaded=%s)", scenario_id, signal_id, q_analysis_ids, loaded_rules)

                # labels reload
                contexts = sorted(list(labels_pairs_index.get(pair, set())), key=lambda x: (x.analysis_id, x.indicator_param, x.timeframe))
                if contexts:
                    loaded_bins = await load_labels_bins_for_pair(pg, scenario_id, signal_id, contexts)

                    async with labels_lock:
                        ctx_set = {(c.analysis_id, c.indicator_param, c.timeframe) for c in contexts}
                        to_del = [k for k in list(labels_bins_cache.keys()) if k[0] == scenario_id and k[1] == signal_id and (k[3], k[4], k[5]) in ctx_set]
                        for k in to_del:
                            labels_bins_cache.pop(k, None)

                        bins_loaded = 0
                        for k, s in loaded_bins.items():
                            labels_bins_cache[k] = s
                            bins_loaded += len(s)

                    log.info("PACK_LABELS: updated (scenario_id=%s, signal_id=%s, ctx=%s, bins_loaded=%s)", scenario_id, signal_id, len(contexts), bins_loaded)

            finally:
                reloading_pairs_bins.discard(pair)
                reloading_pairs_quantiles.discard(pair)
                reloading_pairs_labels.discard(pair)

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
            scheduled = 0
            ignored = 0

            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)
                    try:
                        scenario_id = int(data.get("scenario_id"))
                        signal_id = int(data.get("signal_id"))
                    except Exception:
                        ignored += 1
                        continue

                    pair = (scenario_id, signal_id)
                    if pair not in adaptive_pairs_set and pair not in labels_pairs_set and pair not in adaptive_quantiles_pairs_set:
                        ignored += 1
                        continue

                    asyncio.create_task(_reload_pair(scenario_id, signal_id))
                    scheduled += 1

            if to_ack:
                await redis.xack(POSTPROC_STREAM_KEY, POSTPROC_GROUP, *to_ack)

            if scheduled or ignored:
                log.info("PACK_POSTPROC: batch handled (scheduled=%s, ignored=%s, ack=%s)", scheduled, ignored, len(to_ack))

        except Exception as e:
            log.error("PACK_POSTPROC loop error: %s", e, exc_info=True)
            await asyncio.sleep(2)


# 🔸 Handle MTF runtime: always publish for all pairs + dirs
async def handle_mtf_pack_publish_all(redis, rt: PackRuntime, symbol: str, trigger: dict[str, Any], open_ts_ms: int | None) -> tuple[int, int]:
    log = logging.getLogger("PACK_MTF")

    # условия достаточности runtime
    if not rt.mtf_pairs or not rt.mtf_component_tfs or not rt.mtf_component_params or not rt.mtf_bins_static:
        return 0, 0

    published_ok = 0
    published_fail = 0

    # invalid_trigger_event (open_time unparsable) — можем публиковать, т.к. key однозначен
    if open_ts_ms is None:
        for (scenario_id, signal_id) in rt.mtf_pairs:
            for direction in ("long", "short"):
                details = build_fail_details_base(
                    analysis_id=int(rt.analysis_id),
                    symbol=str(symbol),
                    direction=str(direction),
                    timeframe="mtf",
                    pair={"scenario_id": int(scenario_id), "signal_id": int(signal_id)},
                    trigger=trigger,
                    open_ts_ms=None,
                )
                details["missing_fields"] = ["open_time"]
                details["parse"] = {"open_time": trigger.get("open_time"), "open_ts_ms": None}
                payload = pack_fail("invalid_trigger_event", details)
                await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
                published_fail += 1
        return published_ok, published_fail

    # rules_not_loaded_yet при старте/перезагрузке кешей
    if not caches_ready.get("labels", False) or (rt.mtf_quantiles_key and not caches_ready.get("quantiles", False)):
        need = []
        if not caches_ready.get("labels", False):
            need.append("labels")
        if rt.mtf_quantiles_key and not caches_ready.get("quantiles", False):
            need.append("quantiles")
        for (scenario_id, signal_id) in rt.mtf_pairs:
            for direction in ("long", "short"):
                details = build_fail_details_base(
                    analysis_id=int(rt.analysis_id),
                    symbol=str(symbol),
                    direction=str(direction),
                    timeframe="mtf",
                    pair={"scenario_id": int(scenario_id), "signal_id": int(signal_id)},
                    trigger=trigger,
                    open_ts_ms=int(open_ts_ms),
                )
                details["need"] = need
                details["retry"] = {"recommended": True, "after_sec": int(MTF_RETRY_STEP_SEC)}
                payload = pack_fail("rules_not_loaded_yet", details)
                await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
                published_fail += 1
        return published_ok, published_fail

    # собираем значения по TF
    tasks = []
    meta: list[tuple[str, str | None, str]] = []  # (tf, sub_name|None, param_name)

    for tf in rt.mtf_component_tfs:
        spec = rt.mtf_component_params.get(tf)

        # один параметр на TF
        if isinstance(spec, str):
            pname = str(spec or "").strip()
            tasks.append(asyncio.create_task(get_mtf_value_decimal(redis, str(symbol), int(open_ts_ms), str(tf), pname) if pname else asyncio.sleep(0, result=(None, None, {"styk": False, "waited_sec": 0}))))
            meta.append((str(tf), None, pname))

        # несколько параметров на TF (dict)
        elif isinstance(spec, dict):
            for name, param_name in spec.items():
                pname = str(param_name or "").strip()
                if not pname:
                    tasks.append(asyncio.create_task(asyncio.sleep(0, result=(None, None, {"styk": False, "waited_sec": 0}))))
                    meta.append((str(tf), str(name), ""))
                    continue

                # для m5 в multi-param режиме читаем TS по open_time m5
                if str(tf) == "m5":
                    tasks.append(asyncio.create_task(get_ts_decimal_with_retry(redis, str(symbol), "m5", pname, int(open_ts_ms))))
                    meta.append((str(tf), str(name), pname))
                else:
                    tasks.append(asyncio.create_task(get_mtf_value_decimal(redis, str(symbol), int(open_ts_ms), str(tf), pname)))
                    meta.append((str(tf), str(name), pname))

        else:
            tasks.append(asyncio.create_task(asyncio.sleep(0, result=(None, None, {"styk": False, "waited_sec": 0}))))
            meta.append((str(tf), None, ""))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    values_by_tf: dict[str, Any] = {}
    missing_items: list[Any] = []
    invalid_items: list[Any] = []
    styk_m15 = False
    styk_h1 = False
    waited_total = 0

    for (tf, name, param), r in zip(meta, results):
        if isinstance(r, Exception):
            missing_items.append({"tf": tf, "param": param or None, "error": short_error_str(r)})
            continue

        # нормализуем формат return
        d_val: Decimal | None = None
        raw_val: str | None = None
        meta_info: dict[str, Any] = {"styk": False, "waited_sec": 0}

        # get_mtf_value_decimal -> (Decimal|None, raw|None, meta)
        if isinstance(r, tuple) and len(r) == 3 and isinstance(r[2], dict):
            d_val = r[0]
            raw_val = r[1]
            meta_info = r[2] or meta_info

        # get_ts_decimal_with_retry -> (Decimal|None, raw|None, waited)
        elif isinstance(r, tuple) and len(r) == 3 and isinstance(r[2], int):
            d_val = r[0]
            raw_val = r[1]
            meta_info = {"styk": True, "waited_sec": int(r[2])}

        if meta_info.get("styk"):
            if str(tf) == "m15":
                styk_m15 = True
            if str(tf) == "h1":
                styk_h1 = True
        waited_total += int(meta_info.get("waited_sec") or 0)

        # значение отсутствует
        if d_val is None:
            missing_items.append({"tf": tf, "param": param or None, "name": name})
            continue

        # invalid_input_value: raw exists but not parseable
        if raw_val is not None and safe_decimal(raw_val) is None:
            invalid_items.append({"tf": tf, "param": param or None, "raw": str(raw_val), "kind": "mtf_component_value"})
            continue

        v = clip_0_100(d_val) if rt.mtf_clip_0_100 else d_val
        if name is None:
            values_by_tf[str(tf)] = v
        else:
            block = values_by_tf.get(str(tf))
            if not isinstance(block, dict):
                block = {}
                values_by_tf[str(tf)] = block
            block[str(name)] = v

    # проверка полноты по конфигу
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
            for nm in spec.keys():
                if str(nm) not in block:
                    missing = True
                    break
            if missing:
                break
        else:
            missing = True
            break

    if missing or missing_items or invalid_items:
        # invalid_input_value имеет приоритет над “missing component values”
        if invalid_items:
            reason = "invalid_input_value"
        else:
            reason = "mtf_boundary_wait" if (styk_m15 or styk_h1) else "mtf_missing_component_values"

        for (scenario_id, signal_id) in rt.mtf_pairs:
            for direction in ("long", "short"):
                details = build_fail_details_base(
                    analysis_id=int(rt.analysis_id),
                    symbol=str(symbol),
                    direction=str(direction),
                    timeframe="mtf",
                    pair={"scenario_id": int(scenario_id), "signal_id": int(signal_id)},
                    trigger=trigger,
                    open_ts_ms=int(open_ts_ms),
                )

                if invalid_items:
                    details["kind"] = "mtf_component_value"
                    details["input"] = invalid_items
                else:
                    details["missing"] = missing_items

                details["styk"] = {"m15": bool(styk_m15), "h1": bool(styk_h1), "waited_sec": int(waited_total)}
                details["retry"] = {"recommended": True, "after_sec": int(MTF_RETRY_STEP_SEC)}

                payload = pack_fail(reason, details)
                await publish_pair(...)
                published_fail += 1

        if styk_m15 or styk_h1:
            log.info(
                "PACK_MTF: boundary wait → fail=%s (symbol=%s, analysis_id=%s, open_ts_ms=%s, styk_m15=%s, styk_h1=%s)",
                published_fail,
                symbol,
                rt.analysis_id,
                open_ts_ms,
                styk_m15,
                styk_h1,
            )
        return published_ok, published_fail

    # price for lr_mtf
    if rt.mtf_needs_price:
        price_key = f"{BB_TS_PREFIX}:{symbol}:{rt.mtf_price_tf}:{rt.mtf_price_field}"
        raw_price = await ts_get_value_at(redis, price_key, int(open_ts_ms))
        price_d = safe_decimal(raw_price)
        if price_d is None:
            for (scenario_id, signal_id) in rt.mtf_pairs:
                for direction in ("long", "short"):
                    details = build_fail_details_base(
                        analysis_id=int(rt.analysis_id),
                        symbol=str(symbol),
                        direction=str(direction),
                        timeframe="mtf",
                        pair={"scenario_id": int(scenario_id), "signal_id": int(signal_id)},
                        trigger=trigger,
                        open_ts_ms=int(open_ts_ms),
                    )
                    if raw_price is None:
                        details["missing"] = [{"tf": str(rt.mtf_price_tf), "field": str(rt.mtf_price_field), "source": "bb:ts", "open_ts_ms": int(open_ts_ms)}]
                        details["retry"] = {"recommended": True, "after_sec": int(MTF_RETRY_STEP_SEC)}
                        payload = pack_fail("missing_inputs", details)
                    else:
                        details["kind"] = "price_value"
                        details["input"] = {"tf": str(rt.mtf_price_tf), "param": f"bb:{rt.mtf_price_field}", "raw": str(raw_price)}
                        details["retry"] = {"recommended": False, "after_sec": int(MTF_RETRY_STEP_SEC)}
                        payload = pack_fail("invalid_input_value", details)

                    await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
                    published_fail += 1
            return published_ok, published_fail

        values_by_tf["price"] = price_d

    # for each pair and direction compute and publish
    for (scenario_id, signal_id) in rt.mtf_pairs:
        pair_key = (int(scenario_id), int(signal_id))

        # during reload — rules_not_loaded_yet (priority)
        pair_reloading = (pair_key in reloading_pairs_labels) or (pair_key in reloading_pairs_quantiles)
        if pair_reloading:
            for direction in ("long", "short"):
                details = build_fail_details_base(
                    analysis_id=int(rt.analysis_id),
                    symbol=str(symbol),
                    direction=str(direction),
                    timeframe="mtf",
                    pair={"scenario_id": int(scenario_id), "signal_id": int(signal_id)},
                    trigger=trigger,
                    open_ts_ms=int(open_ts_ms),
                )
                details["need"] = ["labels", "quantiles"] if rt.mtf_quantiles_key else ["labels"]
                details["retry"] = {"recommended": True, "after_sec": int(MTF_RETRY_STEP_SEC)}
                payload = pack_fail("rules_not_loaded_yet", details)
                await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
                published_fail += 1
            continue

        for direction in ("long", "short"):
            if direction not in ("long", "short"):
                details = build_fail_details_base(
                    analysis_id=int(rt.analysis_id),
                    symbol=str(symbol),
                    direction=str(direction),
                    timeframe="mtf",
                    pair={"scenario_id": int(scenario_id), "signal_id": int(signal_id)},
                    trigger=trigger,
                    open_ts_ms=int(open_ts_ms),
                )
                payload = pack_fail("invalid_direction", details)
                await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
                published_fail += 1
                continue

            rules_by_tf: dict[str, list[Any]] = {}

            # static bins rules
            if str(rt.mtf_bins_tf) == "mtf":
                rules_by_tf["mtf"] = (rt.mtf_bins_static.get("mtf", {}) or {}).get(direction, []) or []
                if not rules_by_tf["mtf"]:
                    details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), "mtf", {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, int(open_ts_ms))
                    details["expected"] = {"bin_type": "bins", "tf": "mtf", "source": "static"}
                    payload = pack_fail("no_rules_static", details)
                    await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
                    published_fail += 1
                    continue
            else:
                required_tfs = rt.mtf_required_bins_tfs or rt.mtf_component_tfs or []
                missing_rules_tfs: list[str] = []
                for tf in required_tfs:
                    rules = (rt.mtf_bins_static.get(str(tf), {}) or {}).get(direction, []) or []
                    rules_by_tf[str(tf)] = rules
                    if not rules:
                        missing_rules_tfs.append(str(tf))
                if missing_rules_tfs:
                    details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), "mtf", {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, int(open_ts_ms))
                    details["expected"] = {"bin_type": "bins", "tf": missing_rules_tfs, "source": "static"}
                    payload = pack_fail("no_rules_static", details)
                    await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
                    published_fail += 1
                    continue

            # quantiles rules
            if rt.mtf_quantiles_key:
                q_rules = adaptive_quantiles_cache.get((int(rt.analysis_id), int(scenario_id), int(signal_id), "mtf", str(direction)), [])
                if not q_rules:
                    details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), "mtf", {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, int(open_ts_ms))
                    details["expected"] = {"bin_type": "quantiles", "tf": "mtf", "source": "adaptive"}
                    payload = pack_fail("no_quantiles_rules", details)
                    await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
                    published_fail += 1
                    continue
                rules_by_tf[str(rt.mtf_quantiles_key)] = q_rules

            # candidates
            try:
                try:
                    candidates = rt.worker.bin_candidates(values_by_tf=values_by_tf, rules_by_tf=rules_by_tf, direction=str(direction))
                except TypeError:
                    candidates = rt.worker.bin_candidates(values_by_tf=values_by_tf, rules_by_tf=rules_by_tf)
            except Exception as e:
                details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), "mtf", {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, int(open_ts_ms))
                details["where"] = "handle_mtf_pack/bin_candidates"
                details["error"] = short_error_str(e)
                payload = pack_fail("internal_error", details)
                await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
                published_fail += 1
                continue

            if not candidates:
                details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), "mtf", {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, int(open_ts_ms))
                payload = pack_fail("no_candidates", details)
                await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
                published_fail += 1
                continue

            # choose by labels cache
            chosen = None
            cand_list = [str(x) for x in list(candidates)[:MAX_CANDIDATES_IN_DETAILS]]
            for cand in cand_list:
                if labels_has_bin(int(scenario_id), int(signal_id), str(direction), int(rt.analysis_id), str(rt.source_param_name), "mtf", cand):
                    chosen = cand
                    break

            if not chosen:
                details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), "mtf", {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, int(open_ts_ms))
                details["candidates"] = cand_list
                details["labels_ctx"] = {
                    "analysis_id": int(rt.analysis_id),
                    "indicator_param": str(rt.source_param_name),
                    "timeframe": "mtf",
                    "scenario_id": int(scenario_id),
                    "signal_id": int(signal_id),
                }
                payload = pack_fail("no_labels_match", details)
                await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
                published_fail += 1
                continue

            await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", pack_ok(chosen), int(rt.ttl_sec))
            published_ok += 1

    if published_ok or published_fail:
        log.debug("PACK_MTF: done (symbol=%s, analysis_id=%s, open_ts_ms=%s, ok=%s, fail=%s)", symbol, rt.analysis_id, open_ts_ms, published_ok, published_fail)

    return published_ok, published_fail


# 🔸 Handle indicator_stream message: publish always for expected keys of matched runtimes
async def handle_indicator_event(redis, msg: dict[str, Any]) -> dict[str, int]:
    log = logging.getLogger("PACK_SET")

    symbol = msg.get("symbol")
    timeframe = msg.get("timeframe")
    indicator_key = msg.get("indicator")
    status = msg.get("status")
    open_time = msg.get("open_time")

    # invalid_trigger_event: если нельзя построить ключи (нет symbol/timeframe/indicator) — в Redis не пишем
    if not symbol or not timeframe or not indicator_key:
        log.debug("PACK_SET: invalid trigger (missing fields) %s", {k: msg.get(k) for k in ("symbol", "timeframe", "indicator", "status", "open_time")})
        return {"ok": 0, "fail": 0, "runtimes": 0}

    runtimes = pack_registry.get((str(timeframe), str(indicator_key)))
    if not runtimes:
        return {"ok": 0, "fail": 0, "runtimes": 0}

    open_ts_ms = parse_open_time_to_open_ts_ms(str(open_time) if open_time is not None else None)

    trigger = {
        "indicator": str(indicator_key),
        "timeframe": str(timeframe),
        "open_time": (str(open_time) if open_time is not None else None),
        "status": (str(status) if status is not None else None),
    }

    published_ok = 0
    published_fail = 0

    # status != ready → not_ready_retrying для всех ожидаемых ключей
    if status != "ready":
        for rt in runtimes:
            if rt.is_mtf and rt.mtf_pairs:
                for (scenario_id, signal_id) in rt.mtf_pairs:
                    for direction in ("long", "short"):
                        details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), "mtf", {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, open_ts_ms)
                        details["retry"] = {"recommended": True, "after_sec": 5}
                        payload = pack_fail("not_ready_retrying", details)
                        await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
                        published_fail += 1

            elif rt.bins_source == "adaptive" and rt.adaptive_pairs:
                for (scenario_id, signal_id) in rt.adaptive_pairs:
                    for direction in ("long", "short"):
                        details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), str(rt.timeframe), {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, open_ts_ms)
                        details["retry"] = {"recommended": True, "after_sec": 5}
                        payload = pack_fail("not_ready_retrying", details)
                        await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                        published_fail += 1

            else:
                for direction in ("long", "short"):
                    details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), str(rt.timeframe), None, trigger, open_ts_ms)
                    details["retry"] = {"recommended": True, "after_sec": 5}
                    payload = pack_fail("not_ready_retrying", details)
                    await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                    published_fail += 1

        return {"ok": published_ok, "fail": published_fail, "runtimes": len(runtimes)}

    # ready: process runtimes
    for rt in runtimes:
        try:
            # MTF
            if rt.is_mtf:
                ok_n, fail_n = await handle_mtf_pack_publish_all(redis, rt, str(symbol), trigger, open_ts_ms)
                published_ok += ok_n
                published_fail += fail_n
                continue

            # single TF: если open_time не парсится, то для TS-зависимых паков это invalid_trigger_event
            if open_ts_ms is None and rt.analysis_key in ("bb_band_bin", "lr_band_bin", "atr_bin"):
                if rt.bins_source == "adaptive" and rt.adaptive_pairs:
                    for (scenario_id, signal_id) in rt.adaptive_pairs:
                        for direction in ("long", "short"):
                            base = build_fail_details_base(
                                int(rt.analysis_id),
                                str(symbol),
                                str(direction),
                                str(rt.timeframe),
                                {"scenario_id": int(scenario_id), "signal_id": int(signal_id)},
                                trigger,
                                None,
                            )
                            base["missing_fields"] = ["open_time"]
                            base["parse"] = {"open_time": trigger.get("open_time"), "open_ts_ms": None}
                            base["retry"] = {"recommended": False, "after_sec": 0}
                            payload = pack_fail("invalid_trigger_event", base)

                            await publish_pair(
                                redis,
                                int(rt.analysis_id),
                                int(scenario_id),
                                int(signal_id),
                                str(direction),
                                str(symbol),
                                str(rt.timeframe),
                                payload,
                                int(rt.ttl_sec),
                            )
                            published_fail += 1
                else:
                    for direction in ("long", "short"):
                        base = build_fail_details_base(
                            int(rt.analysis_id),
                            str(symbol),
                            str(direction),
                            str(rt.timeframe),
                            None,
                            trigger,
                            None,
                        )
                        base["missing_fields"] = ["open_time"]
                        base["parse"] = {"open_time": trigger.get("open_time"), "open_ts_ms": None}
                        base["retry"] = {"recommended": False, "after_sec": 0}
                        payload = pack_fail("invalid_trigger_event", base)

                        await publish_static(
                            redis,
                            int(rt.analysis_id),
                            str(direction),
                            str(symbol),
                            str(rt.timeframe),
                            payload,
                            int(rt.ttl_sec),
                        )
                        published_fail += 1

                continue

            # single TF: read value
            value: Any = None
            missing: list[Any] = []
            invalid_info: dict[str, Any] | None = None

            if rt.analysis_key == "bb_band_bin":
                value, missing = await build_bb_band_value(
                    redis,
                    str(symbol),
                    str(rt.timeframe),
                    str(rt.source_param_name),
                    open_ts_ms,
                )
            elif rt.analysis_key == "lr_band_bin":
                value, missing = await build_lr_band_value(
                    redis,
                    str(symbol),
                    str(rt.timeframe),
                    str(rt.source_param_name),
                    open_ts_ms,
                )
            elif rt.analysis_key == "atr_bin":
                value, missing = await build_atr_pct_value(
                    redis,
                    str(symbol),
                    str(rt.timeframe),
                    str(rt.source_param_name),
                    open_ts_ms,
                )
            elif rt.analysis_key == "dmigap_bin":
                value, missing = await build_dmigap_value(
                    redis,
                    str(symbol),
                    str(rt.timeframe),
                    str(rt.source_param_name),
                )
            else:
                raw_key = f"ind:{symbol}:{rt.timeframe}:{rt.source_param_name}"
                raw_value = await redis.get(raw_key)
                if raw_value is None:
                    missing = [str(rt.source_param_name)]
                else:
                    try:
                        f = float(raw_value)
                        if not math.isfinite(f):
                            raise ValueError("NaN/inf")
                        value = f
                    except Exception:
                        invalid_info = {
                            "tf": str(rt.timeframe),
                            "param": str(rt.source_param_name),
                            "raw": str(raw_value),
                        }

            # publish for adaptive / static
            if rt.bins_source == "adaptive":
                # pairs guaranteed by registry (otherwise runtime skipped)
                for (scenario_id, signal_id) in rt.adaptive_pairs:
                    pair_key = (int(scenario_id), int(signal_id))
                    pair_reloading = pair_key in reloading_pairs_bins

                    for direction in ("long", "short"):
                        base = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), str(rt.timeframe), {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, open_ts_ms)

                        # invalid input
                        if invalid_info is not None:
                            base["kind"] = "single_value"
                            base["input"] = invalid_info
                            base["retry"] = {"recommended": True, "after_sec": 5}
                            payload = pack_fail("invalid_input_value", base)
                            await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                            published_fail += 1
                            continue

                        # missing inputs
                        if value is None:
                            base["missing"] = missing
                            base["retry"] = {"recommended": True, "after_sec": 5}
                            payload = pack_fail("missing_inputs", base)
                            await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                            published_fail += 1
                            continue

                        # rules not loaded yet / reload
                        if not caches_ready.get("adaptive_bins", False) or pair_reloading:
                            base["need"] = ["adaptive_bins"]
                            base["retry"] = {"recommended": True, "after_sec": 5}
                            payload = pack_fail("rules_not_loaded_yet", base)
                            await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                            published_fail += 1
                            continue

                        rules = get_adaptive_rules(int(rt.analysis_id), int(scenario_id), int(signal_id), str(rt.timeframe), str(direction))
                        if not rules:
                            base["expected"] = {"bin_type": "bins", "tf": str(rt.timeframe), "source": "adaptive", "direction": str(direction)}
                            payload = pack_fail("no_rules_adaptive", base)
                            await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                            published_fail += 1
                            continue

                        try:
                            bin_name = rt.worker.bin_value(value=value, rules=rules)
                        except Exception as e:
                            base["where"] = "handle_indicator_event/adaptive/bin_value"
                            base["error"] = short_error_str(e)
                            payload = pack_fail("internal_error", base)
                            await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                            published_fail += 1
                            continue

                        if not bin_name:
                            payload = pack_fail("no_candidates", base)
                            await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                            published_fail += 1
                            continue

                        await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), str(rt.timeframe), pack_ok(str(bin_name)), int(rt.ttl_sec))
                        published_ok += 1

            else:
                for direction in ("long", "short"):
                    base = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), str(rt.timeframe), None, trigger, open_ts_ms)

                    if invalid_info is not None:
                        base["kind"] = "single_value"
                        base["input"] = invalid_info
                        base["retry"] = {"recommended": True, "after_sec": 5}
                        payload = pack_fail("invalid_input_value", base)
                        await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                        published_fail += 1
                        continue

                    if value is None:
                        base["missing"] = missing
                        base["retry"] = {"recommended": True, "after_sec": 5}
                        payload = pack_fail("missing_inputs", base)
                        await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                        published_fail += 1
                        continue

                    rules = rt.bins_by_direction.get(str(direction)) or []
                    if not rules:
                        base["expected"] = {"bin_type": "bins", "tf": str(rt.timeframe), "source": "static", "direction": str(direction)}
                        payload = pack_fail("no_rules_static", base)
                        await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                        published_fail += 1
                        continue

                    try:
                        bin_name = rt.worker.bin_value(value=value, rules=rules)
                    except Exception as e:
                        base["where"] = "handle_indicator_event/static/bin_value"
                        base["error"] = short_error_str(e)
                        payload = pack_fail("internal_error", base)
                        await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                        published_fail += 1
                        continue

                    if not bin_name:
                        payload = pack_fail("no_candidates", base)
                        await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                        published_fail += 1
                        continue

                    await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), pack_ok(str(bin_name)), int(rt.ttl_sec))
                    published_ok += 1

        except Exception as e:
            # internal_error: если runtime сматчился — publish fail на ожидаемые ключи runtime
            if rt.is_mtf and rt.mtf_pairs:
                for (scenario_id, signal_id) in rt.mtf_pairs:
                    for direction in ("long", "short"):
                        details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), "mtf", {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, open_ts_ms)
                        details["where"] = "handle_indicator_event/mtf/runtime"
                        details["error"] = short_error_str(e)
                        payload = pack_fail("internal_error", details)
                        await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
                        published_fail += 1
            elif rt.bins_source == "adaptive" and rt.adaptive_pairs:
                for (scenario_id, signal_id) in rt.adaptive_pairs:
                    for direction in ("long", "short"):
                        details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), str(rt.timeframe), {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, open_ts_ms)
                        details["where"] = "handle_indicator_event/adaptive/runtime"
                        details["error"] = short_error_str(e)
                        payload = pack_fail("internal_error", details)
                        await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                        published_fail += 1
            else:
                for direction in ("long", "short"):
                    details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), str(rt.timeframe), None, trigger, open_ts_ms)
                    details["where"] = "handle_indicator_event/static/runtime"
                    details["error"] = short_error_str(e)
                    payload = pack_fail("internal_error", details)
                    await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                    published_fail += 1

            log.warning("PACK_SET: runtime error (analysis_id=%s): %s", rt.analysis_id, e, exc_info=True)

    return {"ok": published_ok, "fail": published_fail, "runtimes": len(runtimes)}


# 🔸 Watch indicator_stream (parallel)
async def watch_indicator_stream(redis):
    log = logging.getLogger("PACK_STREAM")
    sem = asyncio.Semaphore(MAX_PARALLEL_MESSAGES)

    async def _process_one(data: dict) -> dict[str, int]:
        async with sem:
            msg = {
                "symbol": data.get("symbol"),
                "timeframe": data.get("timeframe"),
                "indicator": data.get("indicator"),
                "open_time": data.get("open_time"),
                "status": data.get("status"),
            }
            return await handle_indicator_event(redis, msg)

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

            batch_ok = 0
            batch_fail = 0
            batch_runtimes = 0
            errors = 0

            for r in results:
                if isinstance(r, Exception):
                    errors += 1
                    continue
                batch_ok += int(r.get("ok", 0))
                batch_fail += int(r.get("fail", 0))
                batch_runtimes += int(r.get("runtimes", 0))

            await redis.xack(INDICATOR_STREAM, IND_PACK_GROUP, *to_ack)

            # суммирующий лог по батчу
            log.info(
                "PACK_STREAM: batch done (msgs=%s, runtimes_total=%s, ok=%s, fail=%s, errors=%s)",
                len(flat),
                batch_runtimes,
                batch_ok,
                batch_fail,
                errors,
            )

        except Exception as e:
            log.error("PACK_STREAM loop error: %s", e, exc_info=True)
            await asyncio.sleep(2)


# 🔸 Bootstrap helpers
async def load_active_symbols(pg) -> list[str]:
    log = logging.getLogger("PACK_BOOT")
    async with pg.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT symbol
            FROM {BB_TICKERS_TABLE}
            WHERE status = 'enabled' AND tradepermission = 'enabled'
        """)
    symbols = [str(r["symbol"]) for r in rows if r.get("symbol")]
    log.info("PACK_BOOT: активных тикеров загружено: %s", len(symbols))
    return symbols


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
    ok_sum = 0
    fail_sum = 0

    async def _process_one(symbol: str, rt: PackRuntime):
        nonlocal ok_sum, fail_sum
        async with sem:
            # MTF bootstrap осознанно пропускаем
            if rt.is_mtf:
                return

            trigger = {"indicator": "bootstrap", "timeframe": str(rt.timeframe), "open_time": None, "status": "bootstrap"}
            open_ts_ms = None

            # извлекаем value по логике исходного bootstrap (упрощённо)
            value: Any = None
            missing: list[Any] = []
            invalid_info: dict[str, Any] | None = None

            try:
                if rt.analysis_key == "bb_band_bin":
                    prefix = rt.source_param_name
                    upper = await ts_get(redis, f"{IND_TS_PREFIX}:{symbol}:{rt.timeframe}:{prefix}_upper")
                    lower = await ts_get(redis, f"{IND_TS_PREFIX}:{symbol}:{rt.timeframe}:{prefix}_lower")
                    if not upper:
                        missing.append(f"{prefix}_upper")
                    if not lower:
                        missing.append(f"{prefix}_lower")
                    if upper and lower:
                        open_ts_ms = int(upper[0])
                        close_val = await ts_get_value_at(redis, f"{BB_TS_PREFIX}:{symbol}:{rt.timeframe}:c", open_ts_ms)
                        if close_val is None:
                            missing.append({"tf": rt.timeframe, "field": "c", "source": "bb:ts", "open_ts_ms": int(open_ts_ms)})
                        else:
                            value = {"price": str(close_val), "upper": str(upper[1]), "lower": str(lower[1])}

                elif rt.analysis_key == "lr_band_bin":
                    prefix = rt.source_param_name
                    upper = await ts_get(redis, f"{IND_TS_PREFIX}:{symbol}:{rt.timeframe}:{prefix}_upper")
                    lower = await ts_get(redis, f"{IND_TS_PREFIX}:{symbol}:{rt.timeframe}:{prefix}_lower")
                    if not upper:
                        missing.append(f"{prefix}_upper")
                    if not lower:
                        missing.append(f"{prefix}_lower")
                    if upper and lower:
                        open_ts_ms = int(upper[0])
                        close_val = await ts_get_value_at(redis, f"{BB_TS_PREFIX}:{symbol}:{rt.timeframe}:c", open_ts_ms)
                        if close_val is None:
                            missing.append({"tf": rt.timeframe, "field": "c", "source": "bb:ts", "open_ts_ms": int(open_ts_ms)})
                        else:
                            value = {"price": str(close_val), "upper": str(upper[1]), "lower": str(lower[1])}

                elif rt.analysis_key == "atr_bin":
                    atr = await ts_get(redis, f"{IND_TS_PREFIX}:{symbol}:{rt.timeframe}:{rt.source_param_name}")
                    if not atr:
                        missing.append(str(rt.source_param_name))
                    else:
                        open_ts_ms = int(atr[0])
                        close_val = await ts_get_value_at(redis, f"{BB_TS_PREFIX}:{symbol}:{rt.timeframe}:c", open_ts_ms)
                        if close_val is None:
                            missing.append({"tf": rt.timeframe, "field": "c", "source": "bb:ts", "open_ts_ms": int(open_ts_ms)})
                        else:
                            value = {"atr": str(atr[1]), "price": str(close_val)}

                elif rt.analysis_key == "dmigap_bin":
                    base = rt.source_param_name
                    plus = await ts_get(redis, f"{IND_TS_PREFIX}:{symbol}:{rt.timeframe}:{base}_plus_di")
                    minus = await ts_get(redis, f"{IND_TS_PREFIX}:{symbol}:{rt.timeframe}:{base}_minus_di")
                    if not plus:
                        missing.append(f"{base}_plus_di")
                    if not minus:
                        missing.append(f"{base}_minus_di")
                    if plus and minus:
                        value = {"plus": str(plus[1]), "minus": str(minus[1])}

                else:
                    raw = await redis.get(f"ind:{symbol}:{rt.timeframe}:{rt.source_param_name}")
                    if raw is None:
                        missing.append(str(rt.source_param_name))
                    else:
                        try:
                            f = float(raw)
                            if not math.isfinite(f):
                                raise ValueError("NaN/inf")
                            value = f
                        except Exception:
                            invalid_info = {"tf": str(rt.timeframe), "param": str(rt.source_param_name), "raw": str(raw)}

            except Exception as e:
                invalid_info = {"tf": str(rt.timeframe), "param": str(rt.source_param_name), "raw": short_error_str(e)}

            # публикуем только static в bootstrap (как было)
            if rt.bins_source != "static":
                return

            for direction in ("long", "short"):
                if invalid_info is not None:
                    details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), str(rt.timeframe), None, trigger, open_ts_ms)
                    details["kind"] = "single_value"
                    details["input"] = invalid_info
                    details["retry"] = {"recommended": True, "after_sec": 5}
                    await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), pack_fail("invalid_input_value", details), int(rt.ttl_sec))
                    fail_sum += 1
                    continue

                if value is None:
                    details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), str(rt.timeframe), None, trigger, open_ts_ms)
                    details["missing"] = missing
                    details["retry"] = {"recommended": True, "after_sec": 5}
                    await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), pack_fail("missing_inputs", details), int(rt.ttl_sec))
                    fail_sum += 1
                    continue

                rules = rt.bins_by_direction.get(str(direction)) or []
                if not rules:
                    details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), str(rt.timeframe), None, trigger, open_ts_ms)
                    details["expected"] = {"bin_type": "bins", "tf": str(rt.timeframe), "source": "static", "direction": str(direction)}
                    await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), pack_fail("no_rules_static", details), int(rt.ttl_sec))
                    fail_sum += 1
                    continue

                try:
                    bin_name = rt.worker.bin_value(value=value, rules=rules)
                except Exception as e:
                    details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), str(rt.timeframe), None, trigger, open_ts_ms)
                    details["where"] = "bootstrap/bin_value"
                    details["error"] = short_error_str(e)
                    await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), pack_fail("internal_error", details), int(rt.ttl_sec))
                    fail_sum += 1
                    continue

                if not bin_name:
                    details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), str(rt.timeframe), None, trigger, open_ts_ms)
                    await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), pack_fail("no_candidates", details), int(rt.ttl_sec))
                    fail_sum += 1
                    continue

                await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), pack_ok(str(bin_name)), int(rt.ttl_sec))
                ok_sum += 1

    tasks = [asyncio.create_task(_process_one(sym, rt)) for sym in symbols for rt in runtimes]
    await asyncio.gather(*tasks, return_exceptions=True)

    log.info("PACK_BOOT: bootstrap done — packs=%s, symbols=%s, ok=%s, fail=%s", len(runtimes), len(symbols), ok_sum, fail_sum)


# 🔸 run worker
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
