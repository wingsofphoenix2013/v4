# indicators/compute_and_store.py — расчёт индикаторов + подготовка/запись в Redis KV/TS/Stream (core=batched per instance, ready) + snapshot API

# 🔸 Импорты и зависимости
import logging
import asyncio
import math
import json
from datetime import datetime

# 🔸 Импорт индикаторов
from indicators import ema, atr, lr, mfi, rsi, adx_dmi, macd, bb, kama, supertrend

# 🔸 Сопоставление имён индикаторов с функциями
INDICATOR_DISPATCH = {
    "ema": ema.compute,
    "atr": atr.compute,
    "lr": lr.compute,
    "mfi": mfi.compute,
    "rsi": rsi.compute,
    "adx_dmi": adx_dmi.compute,
    "macd": macd.compute,
    "bb": bb.compute,
    "kama": kama.compute,
    "supertrend": supertrend.compute,
}

# 🔸 Константы Redis (потоки)
INDICATOR_STREAM_CORE = "indicator_stream_core"   # core: 1 msg per instance (values_json)
INDICATOR_STREAM_READY = "indicator_stream"       # ready: 1 msg per instance (как было)


# 🔸 Валидация чисел
def _is_finite_number(x) -> bool:
    try:
        return x is not None and isinstance(x, (int, float)) and math.isfinite(float(x))
    except Exception:
        return False


# 🔸 Построение базового имени (base) — как в системе v4
def _build_base(indicator: str, params: dict) -> str:
    if indicator == "macd":
        return f"{indicator}{params['fast']}"
    if "length" in params:
        return f"{indicator}{params['length']}"
    return indicator


# 🔸 Уникальный идентификатор серии Supertrend (для indicator_stream ready)
def _build_supertrend_source_param_name(params: dict) -> str:
    length = params["length"]
    mult_raw = round(float(params["mult"]), 2)
    mult_str = str(mult_raw).replace(".", "_")
    return f"supertrend{length}_{mult_str}"


# 🔸 Форматирование значения в строку по precision
def _format_value_str(value: float, precision: int, is_angle: bool) -> str:
    if is_angle:
        return f"{value:.5f}"
    return f"{value:.{precision}f}"


# 🔸 Приведение имён параметров (контракт param_name v4)
def _normalize_param_name(base: str, raw_param: str) -> str:
    if raw_param.startswith(f"{base}_") or raw_param == base:
        return raw_param
    return f"{base}_{raw_param}" if raw_param != "value" else base


# 🔸 Подготовка ready payload (indicator_stream)
def _build_ready_payload(indicator: str, base: str, timeframe: str, symbol: str, open_time_iso: str, params: dict) -> dict[str, str]:
    payload = {
        "symbol": symbol,
        "indicator": base,
        "timeframe": timeframe,
        "open_time": open_time_iso,
        "status": "ready",
    }

    # отдельная ветка: Supertrend → добавляем уникальный идентификатор серии (length+mult)
    if indicator == "supertrend":
        payload["source_param_name"] = _build_supertrend_source_param_name(params)

    return payload


# 🔸 Чистый расчёт индикатора (compute-only): out[param_name] = str_value
def compute_indicator_values(instance: dict, symbol: str, df, precision: int) -> dict[str, str]:
    log = logging.getLogger("CALC")

    indicator = instance["indicator"]
    params = instance["params"]

    compute_fn = INDICATOR_DISPATCH.get(indicator)
    if compute_fn is None:
        log.warning("⛔ Неизвестный индикатор: %s", indicator)
        return {}

    # расчёт индикатора
    try:
        raw_result = compute_fn(df, params)
    except Exception as e:
        log.error("Ошибка расчёта %s: %s", indicator, e, exc_info=True)
        return {}

    if not raw_result:
        return {}

    # базовое имя
    base = _build_base(indicator, params)

    # округление + фильтрация + нормализация имён
    out: dict[str, str] = {}
    for k, v in raw_result.items():
        if not _is_finite_number(v):
            continue

        is_angle = "angle" in str(k)
        if is_angle:
            val = round(float(v), 5)
        else:
            val = round(float(v), precision)

        param_name = _normalize_param_name(base, str(k))
        out[param_name] = _format_value_str(val, precision, is_angle)

    if not out:
        log.debug("[SKIP] %s %s → пустой результат после фильтрации", indicator, symbol)

    return out


# 🔸 Добавление команд записи индикатора в Redis pipeline (без execute)
def append_writes_to_pipeline(
    *,
    pipe,
    kv_map: dict[str, str],
    instance_id: int,
    instance: dict,
    symbol: str,
    timeframe: str,
    ts: int,
    open_time_iso: str,
    base: str,
    values: dict[str, str],
    precision: int,
) -> dict[str, int]:
    indicator = instance["indicator"]
    params = instance["params"]
    stream_publish = bool(instance.get("stream_publish", False))

    added_core = 0
    added_kv = 0
    added_ts = 0
    added_ready = 0

    # KV + TS — по одному на параметр (контракт не меняем)
    for param_name, str_value in values.items():
        # Redis KV (через общий MSET)
        redis_key = f"ind:{symbol}:{timeframe}:{param_name}"
        kv_map[redis_key] = str_value
        added_kv += 1

        # Redis TS
        ts_key = f"ts_ind:{symbol}:{timeframe}:{param_name}"
        pipe.execute_command(
            "TS.ADD", ts_key, int(ts), str_value,
            "RETENTION", 1209600000,  # 14 дней
            "DUPLICATE_POLICY", "last"
        )
        added_ts += 1

    # core stream — 1 сообщение на instance (values_json)
    if values:
        core_payload = {
            "symbol": symbol,
            "interval": timeframe,
            "instance_id": str(instance_id),
            "open_time": open_time_iso,
            "precision": str(int(precision)),
            "values_json": json.dumps(values, ensure_ascii=False, separators=(",", ":")),
        }
        pipe.xadd(INDICATOR_STREAM_CORE, core_payload)
        added_core += 1

    # ready stream — как было: 1 сообщение на instance (если включено)
    if stream_publish:
        pipe.xadd(INDICATOR_STREAM_READY, _build_ready_payload(indicator, base, timeframe, symbol, open_time_iso, params))
        added_ready += 1

    return {
        "core": added_core,
        "kv": added_kv,
        "ts": added_ts,
        "ready": added_ready,
    }


# 🔸 Расчёт и запись одного расчётного экземпляра (совместимо со старым вызовом)
async def compute_and_store(instance_id, instance, symbol, df, ts, pg, redis, precision, *, log_info: bool = False):
    log = logging.getLogger("CALC")

    indicator = instance["indicator"]
    timeframe = instance["timeframe"]
    params = instance["params"]

    # время бара (UTC-naive ISO)
    open_time_iso = datetime.utcfromtimestamp(int(ts) / 1000).isoformat()

    # расчёт (compute-only)
    values = compute_indicator_values(instance, symbol, df, int(precision))
    if not values:
        return

    # базовое имя (для ready payload)
    base = _build_base(indicator, params)

    # подготовка пачки команд (pipeline)
    pipe = redis.pipeline(transaction=False)
    kv_map: dict[str, str] = {}

    counts = append_writes_to_pipeline(
        pipe=pipe,
        kv_map=kv_map,
        instance_id=int(instance_id),
        instance=instance,
        symbol=symbol,
        timeframe=timeframe,
        ts=int(ts),
        open_time_iso=open_time_iso,
        base=base,
        values=values,
        precision=int(precision),
    )

    # MSET — одной командой после наполнения kv_map
    if kv_map:
        pipe.mset(kv_map)

    # выполнение пачки
    results = await pipe.execute(raise_on_error=False)

    # суммирование ошибок
    errors = 0
    for r in results:
        if isinstance(r, Exception):
            errors += 1

    # суммирующий лог (по желанию + всегда при ошибках)
    if errors > 0:
        log.debug(
            "CALC: errors=%s (symbol=%s, tf=%s, indicator=%s, base=%s, params=%s, core=%s, kv=%s, ts=%s, ready=%s)",
            errors,
            symbol,
            timeframe,
            indicator,
            base,
            params,
            counts["core"],
            counts["kv"],
            counts["ts"],
            counts["ready"],
        )
    elif log_info:
        log.debug(
            "CALC: done (symbol=%s, tf=%s, indicator=%s, base=%s, params=%s, core=%s, kv=%s, ts=%s, ready=%s)",
            symbol,
            timeframe,
            indicator,
            base,
            params,
            counts["core"],
            counts["kv"],
            counts["ts"],
            counts["ready"],
        )
    else:
        log.debug(
            "CALC: done (symbol=%s, tf=%s, indicator=%s, base=%s, params=%s, core=%s, kv=%s, ts=%s, ready=%s, errors=%s)",
            symbol,
            timeframe,
            indicator,
            base,
            params,
            counts["core"],
            counts["kv"],
            counts["ts"],
            counts["ready"],
            errors,
        )


# 🔸 Генерация ожидаемых имён параметров для индикатора
def get_expected_param_names(indicator: str, params: dict) -> list[str]:
    if indicator == "macd":
        base = f"macd{params['fast']}"
        return [f"{base}_macd", f"{base}_macd_signal", f"{base}_macd_hist"]

    elif indicator == "bb":
        length = params["length"]
        std_raw = round(float(params["std"]), 2)
        std_str = str(std_raw).replace(".", "_")
        base = f"bb{length}_{std_str}"
        return [f"{base}_center", f"{base}_upper", f"{base}_lower"]

    elif indicator == "adx_dmi":
        base = f"adx_dmi{params['length']}"
        return [f"{base}_adx", f"{base}_plus_di", f"{base}_minus_di"]

    elif indicator == "lr":
        base = f"lr{params['length']}"
        return [f"{base}_angle", f"{base}_center", f"{base}_upper", f"{base}_lower"]

    elif indicator in ("rsi", "mfi", "ema", "kama", "atr"):
        return [f"{indicator}{params['length']}"]

    elif indicator == "supertrend":
        # имена в стиле supertrend{length}_{mult}_...
        length = params["length"]
        mult_raw = round(float(params["mult"]), 2)
        mult_str = str(mult_raw).replace(".", "_")
        base = f"supertrend{length}_{mult_str}"
        return [base, f"{base}_trend"]

    else:
        return [indicator]


# 🔸 Чистый расчёт индикатора (без записи в Redis/PG/стримы)
def compute_snapshot_values(instance: dict, symbol: str, df, precision: int) -> dict[str, str]:
    log = logging.getLogger("SNAPSHOT")

    indicator = instance["indicator"]
    params = instance["params"]

    compute_fn = INDICATOR_DISPATCH.get(indicator)
    if compute_fn is None:
        log.warning("⛔ Неизвестный индикатор: %s", indicator)
        return {}

    # расчёт индикатора
    try:
        raw = compute_fn(df, params)
    except Exception as e:
        log.error("Ошибка расчёта %s: %s", indicator, e, exc_info=True)
        return {}

    # округление + фильтрация нечисловых значений
    rounded = {}
    for k, v in raw.items():
        try:
            if v is None or not isinstance(v, (int, float)) or not math.isfinite(float(v)):
                continue

            if "angle" in str(k):
                val = round(float(v), 5)
                rounded[str(k)] = f"{val:.5f}"
            else:
                val = round(float(v), precision)
                rounded[str(k)] = f"{val:.{precision}f}"

        except Exception as e:
            log.warning("[%s] %s: ошибка округления %s=%s → %s", indicator, symbol, k, v, e)

    if not rounded:
        return {}

    # базовое имя (как в compute_indicator_values)
    base = _build_base(indicator, params)

    # приведение имён параметров
    out: dict[str, str] = {}
    for param, value in rounded.items():
        out[_normalize_param_name(base, param)] = value

    return out


# 🔸 Асинхронная обёртка: выполнить sync-расчёт в пуле потоков
async def compute_snapshot_values_async(instance: dict, symbol: str, df, precision: int) -> dict[str, str]:
    return await asyncio.to_thread(compute_snapshot_values, instance, symbol, df, precision)