# indicators/compute_and_store.py — расчёт индикаторов + запись в Redis KV/TS/Stream (core + ready)

import logging
import asyncio
import math
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
INDICATOR_STREAM_CORE = "indicator_stream_core"
INDICATOR_STREAM_READY = "indicator_stream"


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


# 🔸 Расчёт и обработка результата одного расчётного экземпляра (pipeline + MSET)
async def compute_and_store(instance_id, instance, symbol, df, ts, pg, redis, precision):
    log = logging.getLogger("CALC")
    log.debug("[TRACE] compute_and_store received precision=%s for %s (instance_id=%s)", precision, symbol, instance_id)

    indicator = instance["indicator"]
    timeframe = instance["timeframe"]
    params = instance["params"]
    stream = instance["stream_publish"]

    compute_fn = INDICATOR_DISPATCH.get(indicator)
    if compute_fn is None:
        log.warning("⛔ Неизвестный индикатор: %s", indicator)
        return

    # расчёт индикатора
    try:
        raw_result = compute_fn(df, params)

        # округление + фильтрация
        result = {}
        for k, v in raw_result.items():
            if not _is_finite_number(v):
                log.debug("[SKIP] %s %s/%s → %s is non-finite (%s)", indicator, symbol, timeframe, k, v)
                continue

            # особая точность для angle
            if "angle" in k:
                result[k] = round(float(v), 5)
            else:
                result[k] = round(float(v), precision)

    except Exception as e:
        log.error("Ошибка расчёта %s id=%s: %s", indicator, instance_id, e, exc_info=True)
        return

    if not result:
        log.debug("[SKIP] %s %s/%s → пустой результат после фильтрации", indicator, symbol, timeframe)
        return

    log.debug("✅ %s id=%s %s/%s → %s", indicator.upper(), instance_id, symbol, timeframe, result)

    # 🔸 Базовое имя (label)
    base = _build_base(indicator, params)

    # 🔸 Время бара (UTC-naive ISO без таймзоны)
    open_time_iso = datetime.utcfromtimestamp(int(ts) / 1000).isoformat()

    # 🔸 Подготовка пачки команд (pipeline)
    pipe = redis.pipeline(transaction=False)

    # подготовка mset для KV
    kv_map = {}

    added_core = 0
    added_kv = 0
    added_ts = 0

    for param, value in result.items():
        # приведение имён параметров (контракт param_name v4)
        if param.startswith(f"{base}_") or param == base:
            param_name = param
        else:
            param_name = f"{base}_{param}" if param != "value" else base

        # форматирование значения в строку по precision
        if "angle" in param_name:
            str_value = f"{value:.5f}"
        else:
            str_value = f"{value:.{precision}f}"

        # Redis KV (выполним через MSET одной командой)
        redis_key = f"ind:{symbol}:{timeframe}:{param_name}"
        kv_map[redis_key] = str_value
        added_kv += 1

        # Redis TS (как было: TS.ADD с retention + duplicate_policy)
        ts_key = f"ts_ind:{symbol}:{timeframe}:{param_name}"
        pipe.execute_command(
            "TS.ADD", ts_key, int(ts), str_value,
            "RETENTION", 1209600000,  # 14 дней
            "DUPLICATE_POLICY", "last"
        )
        added_ts += 1

        # Redis Stream (core) — по одному сообщению на параметр
        stream_precision = 5 if "angle" in param_name else precision
        pipe.xadd(INDICATOR_STREAM_CORE, {
            "symbol": symbol,
            "interval": timeframe,
            "instance_id": str(instance_id),
            "open_time": open_time_iso,
            "param_name": param_name,
            "value": str_value,
            "precision": str(stream_precision),
        })
        added_core += 1

    # mset после подготовки всех ключей
    if kv_map:
        pipe.mset(kv_map)

    # Redis Stream (готовность)
    if stream:
        ready_payload = {
            "symbol": symbol,
            "indicator": base,
            "timeframe": timeframe,
            "open_time": open_time_iso,
            "status": "ready",
        }

        # отдельная ветка: Supertrend → добавляем уникальный идентификатор серии (length+mult)
        if indicator == "supertrend":
            ready_payload["source_param_name"] = _build_supertrend_source_param_name(params)

        pipe.xadd(INDICATOR_STREAM_READY, ready_payload)

    # выполнение пачки
    results = await pipe.execute(raise_on_error=False)

    # суммирование ошибок (как раньше через return_exceptions=True)
    errors = 0
    for r in results:
        if isinstance(r, Exception):
            errors += 1

    # 🔸 Суммирующий лог (чтобы не шуметь — только если публиковали ready)
    if stream:
        extra = ""
        if indicator == "supertrend":
            extra = f", source_param_name={_build_supertrend_source_param_name(params)}"

        log.debug(
            "CALC: done (symbol=%s, tf=%s, indicator=%s, base=%s%s, params=%s, core=%s, kv=%s, ts=%s, errors=%s)",
            symbol,
            timeframe,
            indicator,
            base,
            extra,
            params,
            added_core,
            added_kv,
            added_ts,
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
            if "angle" in k:
                val = round(float(v), 5)
                rounded[k] = f"{val:.5f}"
            else:
                val = round(float(v), precision)
                rounded[k] = f"{val:.{precision}f}"
        except Exception as e:
            log.warning("[%s] %s: ошибка округления %s=%s → %s", indicator, symbol, k, v, e)

    if not rounded:
        return {}

    # 🔸 Построение базового имени (base), как в compute_and_store
    base = _build_base(indicator, params)

    # 🔸 Приведение имён параметров
    out: dict[str, str] = {}
    for param, value in rounded.items():
        if param.startswith(f"{base}_") or param == base:
            param_name = param
        else:
            param_name = f"{base}_{param}" if param != "value" else base
        out[param_name] = value

    return out


# 🔸 Асинхронная обёртка: выполнить sync-расчёт в пуле потоков
async def compute_snapshot_values_async(instance: dict, symbol: str, df, precision: int) -> dict[str, str]:
    return await asyncio.to_thread(compute_snapshot_values, instance, symbol, df, precision)