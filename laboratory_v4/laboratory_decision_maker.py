# laboratory_decision_maker.py — обработчик запросов решений (allow/deny) для стратегий: чтение из Stream, MW→(PACK), ответ и аудит

import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Внутренние импорты
import laboratory_infra as infra

# 🔸 Логгер
log = logging.getLogger("LAB_DECISION")

# 🔸 Константы потоков и шлюзов
DECISION_REQ_STREAM = "laboratory:decision_request"
DECISION_RESP_STREAM = "laboratory:decision_response"

GATEWAY_REQ_STREAM = "indicator_gateway_request"
# Публичные KV ключи gateway (см. спецификацию indicator_gateway)
# Для MW: <indicator>_pack:{symbol}:{tf}:{base}  (base == indicator)
# Для PACK: см. map PACK_PUBLIC_PREFIX ниже

# 🔸 Параметры производительности (согласованные)
XREAD_BLOCK_MS = 2000
XREAD_COUNT = 50
MAX_IN_FLIGHT_DECISIONS = 32
MAX_CONCURRENT_GATEWAY_CALLS = 32
COALESCE_TTL_SEC = 3
SAFETY_DEADLINE_MS = 60_000  # общий «потолок» на запрос

# 🔸 Поддерживаемые TF и порядок обработки
TF_ORDER = ("m5", "m15", "h1")

# 🔸 Префиксы публичного кэша для PACK
PACK_PUBLIC_PREFIX = {
    "bb": "bbpos_pack",
    "lr": "lrpos_pack",
    "atr": "atr_pack",
    "adx_dmi": "adx_dmi_pack",
    "macd": "macd_pack",
    # по умолчанию: f"{indicator}_pack"
}

# 🔸 Семафоры для конкуренции
_decisions_sem = asyncio.Semaphore(MAX_IN_FLIGHT_DECISIONS)
_gateway_sem = asyncio.Semaphore(MAX_CONCURRENT_GATEWAY_CALLS)

# 🔸 Коалесценс (in-process) — ключ → Future (живёт COALESCE_TTL_SEC)
_coalesce: Dict[str, Tuple[float, asyncio.Future]] = {}


# 🔸 Вспомогательные парсеры и нормализация
def _parse_timeframes(tf_str: str) -> List[str]:
    items = [x.strip().lower() for x in (tf_str or "").split(",") if x.strip()]
    # порядок фиксируем: m5 → m15 → h1; удаляем дубликаты
    seen = set()
    ordered = []
    for tf in TF_ORDER:
        if tf in items and tf not in seen:
            seen.add(tf)
            ordered.append(tf)
    return ordered


def _parse_pack_base(base: str) -> Tuple[str, Dict[str, Any]]:
    """
    Возвращает (indicator, params) по строке pack_base.
    Примеры:
      'ema21' -> ('ema', {'length': 21})
      'macd12' -> ('macd', {'fast': 12})
      'bb20_2_0' -> ('bb', {'length': 20, 'std': 2.0})
      'adx_dmi14' -> ('adx_dmi', {'length': 14})
      'lr100' -> ('lr', {'length': 100})
      'atr14' -> ('atr', {'length': 14})
      'rsi14' -> ('rsi', {'length': 14})
      'mfi14' -> ('mfi', {'length': 14})
    """
    s = base.strip().lower()
    if s.startswith("bb"):
        # bb{length}_{std_with_underscore}
        rest = s[2:]
        parts = rest.split("_", 2)
        L = int(parts[0])
        # std может быть "2_0" → 2.0
        std = float(parts[1].replace("_", ".", 1)) if len(parts) > 1 else 2.0
        return "bb", {"length": L, "std": std}
    if s.startswith("macd"):
        F = int(s[4:])
        return "macd", {"fast": F}
    if s.startswith("adx_dmi"):
        L = int(s[7:])
        return "adx_dmi", {"length": L}
    if s.startswith("ema"):
        L = int(s[3:])
        return "ema", {"length": L}
    if s.startswith("rsi"):
        L = int(s[3:])
        return "rsi", {"length": L}
    if s.startswith("mfi"):
        L = int(s[3:])
        return "mfi", {"length": L}
    if s.startswith("lr"):
        L = int(s[2:])
        return "lr", {"length": L}
    if s.startswith("atr"):
        L = int(s[3:])
        return "atr", {"length": L}
    # fallback — как есть
    return s, {}


def _public_pack_key(indicator: str, symbol: str, tf: str, base: str) -> str:
    pref = PACK_PUBLIC_PREFIX.get(indicator, f"{indicator}_pack")
    return f"{pref}:{symbol}:{tf}:{base}"


def _public_mw_key(kind: str, symbol: str, tf: str) -> str:
    # Для MW base == indicator (trend/volatility/momentum/extremes)
    return f"{kind}_pack:{symbol}:{tf}:{kind}"


def _json_or_none(s: Optional[str]) -> Optional[dict]:
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return None


def _now_monotonic_ms() -> int:
    return int(time.monotonic() * 1000)


# 🔸 Чтение публичного кэша gateway (MGET пачкой)
async def _mget_json(keys: List[str]) -> Dict[str, Optional[dict]]:
    if not keys:
        return {}
    # pipeline MGET
    values = await infra.redis_client.mget(*keys)
    out: Dict[str, Optional[dict]] = {}
    for k, v in zip(keys, values):
        out[k] = _json_or_none(v)
    return out


# 🔸 Отправка запроса в indicator_gateway + ожидание появления публичного ключа
async def _ensure_pack_available(
    symbol: str,
    tf: str,
    indicator: str,
    base: str,
    gw_params: Dict[str, Any],
    precision: int,
    deadline_ms: int,
) -> Optional[dict]:
    """
    Возвращает pack-json из публичного KV, гарантируя его наличие:
      1) пробует public KV,
      2) при отсутствии — XADD в gateway, затем ждёт появления public KV.
    Учитывает коалесценс и лимит конкуренции _gateway_sem.
    """
    key = _public_pack_key(indicator, symbol, tf, base)
    # быстрый чек кэша
    cached = await infra.redis_client.get(key)
    if cached:
        obj = _json_or_none(cached)
        if obj:
            return obj

    # коалесценс: один полёт на ключ
    co_key = f"COAL::pack::{key}"
    now = _now_monotonic_ms()
    future: Optional[asyncio.Future] = None
    rec = _coalesce.get(co_key)
    if rec and now < rec[0]:
        future = rec[1]
    if future:
        log.info("[PACK] ⏳ Коалесценс ожидание существующего запроса key=%s", key)
        try:
            return await asyncio.wait_for(future, timeout=max(0.1, (deadline_ms - _now_monotonic_ms()) / 1000))
        except Exception:
            return None

    # создаём новую future и публикуем в коалесценс
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    _coalesce[co_key] = (now + COALESCE_TTL_SEC * 1000, future)

    # защищаем вызов шлюза общим семафором
    async with _gateway_sem:
        try:
            # соберём поля запроса
            req = {
                "symbol": symbol,
                "timeframe": tf,
                "indicator": indicator,
                "mode": "pack",
            }
            # параметры длин и std
            if indicator in ("ema", "rsi", "mfi", "lr", "atr", "adx_dmi"):
                L = int(gw_params.get("length", 0))
                if L:
                    req["length"] = str(L)
            elif indicator == "macd":
                F = int(gw_params.get("fast", 0))
                if F:
                    req["length"] = str(F)  # для gateway length трактуется как fast
            elif indicator == "bb":
                L = int(gw_params.get("length", 0))
                S = float(gw_params.get("std", 2.0))
                req["length"] = str(L)
                req["std"] = f"{S:.2f}"

            # XADD в gateway
            # ответ будем ждать появлением публичного ключа (gateway кладёт public KV)
            req_id = await infra.redis_client.xadd(GATEWAY_REQ_STREAM, req)
            log.info("[PACK] 📤 GW запрос отправлен ind=%s base=%s req_id=%s key=%s", indicator, base, req_id, key)

            # ждём появления public KV до дедлайна
            poll_sleep = 0.1
            while _now_monotonic_ms() < deadline_ms:
                cached = await infra.redis_client.get(key)
                if cached:
                    obj = _json_or_none(cached)
                    if obj:
                        if not future.done():
                            future.set_result(obj)
                        return obj
                await asyncio.sleep(poll_sleep)
                # слегка растянем интервал до 200мс
                if poll_sleep < 0.2:
                    poll_sleep = 0.2

            log.info("[PACK] ⛔ Истёк дедлайн ожидания public KV ind=%s base=%s", indicator, base)
            if not future.done():
                future.set_result(None)
            return None

        except Exception:
            log.exception("[PACK] ❌ Ошибка при запросе в gateway (ind=%s base=%s)", indicator, base)
            if not future.done():
                future.set_result(None)
            return None
        finally:
            # чистка устаревших записей коалесценса
            now2 = _now_monotonic_ms()
            for ck, (exp, fut) in list(_coalesce.items()):
                if now2 > exp or (fut.done() and _json_or_none(fut.result()) is None):
                    _coalesce.pop(ck, None)


# 🔸 Получение MW состояний (только требуемые базы)
async def _get_mw_states(
    symbol: str,
    tf: str,
    bases: List[str],
    precision: int,
    deadline_ms: int,
) -> Dict[str, Optional[str]]:
    """
    Возвращает словарь {base -> state} по MW, используя cache-first и gateway.
    base ∈ {'trend','volatility','extremes','momentum'}
    """
    out: Dict[str, Optional[str]] = {}
    # сначала пробуем публичные ключи пачкой
    keys = [_public_mw_key(b, symbol, tf) for b in bases]
    kv = await _mget_json(keys)
    # пройдёмся по базам
    for base in bases:
        state: Optional[str] = None
        # попытка из кэша
        k = _public_mw_key(base, symbol, tf)
        obj = kv.get(k)
        if obj and isinstance(obj, dict):
            pack = obj.get("pack") or {}
            st = pack.get("state")
            if isinstance(st, str) and st:
                state = st

        # при необходимости — запрос в gateway
        if state is None:
            obj = await _ensure_pack_available(
                symbol=symbol,
                tf=tf,
                indicator=base,  # base == indicator for MW
                base=base,
                gw_params={},
                precision=precision,
                deadline_ms=deadline_ms,
            )
            if obj:
                pack = obj.get("pack") or {}
                st = pack.get("state")
                if isinstance(st, str) and st:
                    state = st

        out[base] = state
        log.info("[MW] 🧩 %s %s/%s state=%s", base, symbol, tf, state)

    return out


# 🔸 Построение «фактов» для MW и матчинг с whitelist
def _mw_match_and_required_confirmation(
    mw_rows: List[Dict[str, Any]],
    states: Dict[str, Optional[str]],
) -> Tuple[bool, Optional[int]]:
    """
    Возвращает (matched, required_confirmation):
      - matched == False → нет совпадений
      - matched == True  → required_confirmation ∈ {0,1,2}, где:
          0 → TF проходит сразу,
          1/2 → требуется PACK-подтверждение (минимально необходимое среди совпадений).
    Логика по договорённости: winrate игнорируем; рассматриваем все совпадения.
    """
    if not mw_rows:
        return False, None

    matched_confirmations: List[int] = []

    for r in mw_rows:
        agg_base = (r.get("agg_base") or "").strip().lower()
        agg_state = (r.get("agg_state") or "").strip().lower()
        if not agg_base or not agg_state:
            continue

        # определим порядок баз из agg_base
        bases = agg_base.split("_")
        # соберём ключ факта
        if len(bases) == 1:
            base = bases[0]
            cur_state = states.get(base)
            if not cur_state:
                continue
            fact = cur_state.strip().lower()
        else:
            parts = []
            ok = True
            for b in bases:
                st = states.get(b)
                if not st:
                    ok = False
                    break
                parts.append(f"{b}:{st.strip().lower()}")
            if not ok:
                continue
            fact = "|".join(parts)

        if fact == agg_state:
            # совпадение — забираем confirmation
            conf = r.get("confirmation")
            try:
                conf_i = int(conf)
            except Exception:
                continue
            matched_confirmations.append(conf_i)

    if not matched_confirmations:
        return False, None

    # если есть нулевой — проход сразу
    if any(c == 0 for c in matched_confirmations):
        return True, 0

    # иначе требуется минимально возможное подтверждение (1 или 2)
    req = min([c for c in matched_confirmations if c in (1, 2)], default=None)
    if req is None:
        # теоретически не должно быть, но на всякий
        return False, None
    return True, req


# 🔸 Получение PACK фактов по нужным base (cache-first + gateway)
async def _get_pack_objects_for_bases(
    symbol: str,
    tf: str,
    bases: List[str],
    precision: int,
    deadline_ms: int,
) -> Dict[str, Optional[dict]]:
    """
    Возвращает {pack_base -> pack_json_or_None}, где pack_json — объект как в публичном KV gateway.
    """
    results: Dict[str, Optional[dict]] = {}
    # сначала одна MGET пачкой
    keys = []
    for base in bases:
        ind, _params = _parse_pack_base(base)
        keys.append(_public_pack_key(ind, symbol, tf, base))
    got = await _mget_json(keys)

    # теперь догоним недостающие через gateway
    for base in bases:
        ind, params = _parse_pack_base(base)
        k = _public_pack_key(ind, symbol, tf, base)
        obj = got.get(k)
        if obj is None:
            obj = await _ensure_pack_available(
                symbol=symbol,
                tf=tf,
                indicator=ind,
                base=base,
                gw_params=params,
                precision=precision,
                deadline_ms=deadline_ms,
            )
        results[base] = obj
        log.info("[PACK] 📦 base=%s %s/%s present=%s", base, symbol, tf, bool(obj))

    return results


# 🔸 Матчинг PACK: blacklist → whitelist count
def _pack_bl_wl_stats(
    pack_rows: List[Dict[str, Any]],
    pack_objs: Dict[str, Optional[dict]],
) -> Tuple[bool, int]:
    """
    Возвращает (blacklist_hit, whitelist_hits_count) на текущем TF.
    Сравнение строгое по (agg_key, agg_value) для соответствующего pack_base.
    """
    bl_hit = False
    wl_hits = 0

    for r in pack_rows:
        base = (r.get("pack_base") or "").strip().lower()
        if not base:
            continue
        pack_obj = pack_objs.get(base)
        if not pack_obj:
            continue  # pack не получен → не считаем совпадением

        pack = pack_obj.get("pack") or {}
        list_type = (r.get("list") or "").strip().lower()  # whitelist | blacklist
        agg_key = (r.get("agg_key") or "").strip().lower()
        agg_val = (r.get("agg_value") or "").strip().lower()
        if not agg_key or not agg_val:
            continue

        keys = [k.strip() for k in agg_key.split("|") if k.strip()]
        # формируем "fact" → field:value|... в том же порядке
        parts = []
        ok = True
        for k in keys:
            v = pack.get(k)
            if v is None:
                ok = False
                break
            parts.append(f"{k}:{str(v).strip().lower()}")
        if not ok:
            continue
        fact = "|".join(parts)

        if fact == agg_val:
            if list_type == "blacklist":
                bl_hit = True
            elif list_type == "whitelist":
                wl_hits += 1

    return bl_hit, wl_hits


# 🔸 Обработка одного TF (MW → PACK при необходимости)
async def _process_tf(
    sid: int,
    symbol: str,
    direction: str,
    tf: str,
    trace: bool,
    deadline_ms: int,
    telemetry: Dict[str, int],
) -> Tuple[bool, Dict[str, Any]]:
    """
    Возвращает (tf_ok, trace_obj)
    """
    tf_trace: Dict[str, Any] = {"tf": tf}

    # 1) Подготовка WL по TF
    mw_rows_all = (infra.mw_wl_by_strategy.get(sid) or {}).get("rows", [])
    pack_rows_all = (infra.pack_wl_by_strategy.get(sid) or {}).get("rows", [])

    mw_rows = [r for r in mw_rows_all if (r.get("timeframe") == tf and r.get("direction") == direction)]
    pack_rows = [r for r in pack_rows_all if (r.get("timeframe") == tf and r.get("direction") == direction)]

    log.info("[TF:%s] 🔎 WL срезы: MW=%d PACK=%d (sid=%s %s %s)", tf, len(mw_rows), len(pack_rows), sid, symbol, direction)

    # если MW пуст — это «нет совпадений» (по договорённости)
    if not mw_rows:
        tf_trace["mw"] = {"matched": False}
        log.info("[TF:%s] ❌ MW: нет строк в WL — отказ", tf)
        return False, tf_trace

    # 2) MW: какие базы нужно спросить
    needed_bases: List[str] = []
    for r in mw_rows:
        base = (r.get("agg_base") or "").strip().lower()
        if not base:
            continue
        for b in base.split("_"):
            if b in ("trend", "volatility", "extremes", "momentum") and b not in needed_bases:
                needed_bases.append(b)

    # 3) Получаем MW состояния
    precision = int(infra.enabled_tickers.get(symbol, {}).get("precision_price", 7))
    states = await _get_mw_states(symbol, tf, needed_bases, precision, deadline_ms)

    # 4) Матчинг MW → required_confirmation
    matched, required_conf = _mw_match_and_required_confirmation(mw_rows, states)
    if trace:
        tf_trace["mw"] = {"matched": matched}
        if matched:
            tf_trace["mw"]["confirmation"] = required_conf
    if not matched:
        log.info("[TF:%s] ❌ MW: совпадений нет — отказ", tf)
        return False, tf_trace

    if required_conf == 0:
        log.info("[TF:%s] ✅ MW: confirmation=0 — TF пройден без PACK", tf)
        return True, tf_trace

    # 5) PACK: нужен confirmation 1 или 2
    # сначала — собрать список уникальных pack_base из WL (и BL/WL)
    bases: List[str] = []
    for r in pack_rows:
        base = (r.get("pack_base") or "").strip().lower()
        if base and base not in bases:
            bases.append(base)

    # если по PACK вообще нет строк — значит подтверждать нечем → отказ
    if not bases:
        tf_trace["pack"] = {"bl_hits": 0, "wl_hits": 0, "required": required_conf}
        log.info("[TF:%s] ❌ PACK: WL пуст — подтверждений нет (need=%s)", tf, required_conf)
        return False, tf_trace

    # 6) Получаем PACK объекты по нужным base (cache-first/gateway)
    pack_objs = await _get_pack_objects_for_bases(symbol, tf, bases, precision, deadline_ms)

    # 7) Чёрный список → Белые совпадения
    bl_hit, wl_hits = _pack_bl_wl_stats(pack_rows, pack_objs)
    if trace:
        tf_trace["pack"] = {"bl_hits": int(bl_hit), "wl_hits": wl_hits, "required": required_conf}

    if bl_hit:
        log.info("[TF:%s] ❌ PACK: blacklist hit — отказ", tf)
        return False, tf_trace

    if wl_hits >= (required_conf or 0):
        log.info("[TF:%s] ✅ PACK: подтверждений достаточно (need=%s got=%s)", tf, required_conf, wl_hits)
        return True, tf_trace

    log.info("[TF:%s] ❌ PACK: подтверждений недостаточно (need=%s got=%s)", tf, required_conf, wl_hits)
    return False, tf_trace


# 🔸 Сохранение результата в БД (best-effort, после ответа)
async def _persist_decision(
    req_id: str,
    log_uid: str,
    strategy_id: int,
    symbol: str,
    direction: str,
    tfr_req: str,
    tfr_proc: str,
    allow: bool,
    reason: Optional[str],
    tf_results_json: Optional[str],
    received_at_dt: datetime,
    finished_at_dt: datetime,
    duration_ms: int,
    cache_hits: int,
    gateway_requests: int,
):
    query = """
    INSERT INTO public.signal_laboratory_entries
    (req_id, log_uid, strategy_id, direction, symbol,
     timeframes_requested, timeframes_processed, protocol_version,
     allow, reason, tf_results, errors,
     received_at, finished_at, duration_ms, cache_hits, gateway_requests)
    VALUES ($1,$2,$3,$4,$5,
            $6,$7,'v1',
            $8,$9, COALESCE($10::jsonb, NULL), NULL,
            $11,$12,$13,$14,$15)
    ON CONFLICT (log_uid, strategy_id) DO UPDATE
      SET req_id=$1, direction=$4, symbol=$5,
          timeframes_requested=$6, timeframes_processed=$7,
          allow=$8, reason=$9, tf_results=COALESCE($10::jsonb, signal_laboratory_entries.tf_results),
          finished_at=$12, duration_ms=$13, cache_hits=$14, gateway_requests=$15
    """
    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            query,
            req_id, log_uid, strategy_id, direction, symbol,
            tfr_req, tfr_proc,
            allow, reason, tf_results_json,
            received_at_dt, finished_at_dt, duration_ms, cache_hits, gateway_requests
        )
    log.info("[AUDIT] 💾 Сохранено решение log_uid=%s sid=%s allow=%s", log_uid, strategy_id, allow)


# 🔸 Обработка одного запроса из Stream
async def _process_request(msg_id: str, fields: Dict[str, str]):
    async with _decisions_sem:
        t0 = _now_monotonic_ms()
        received_at_dt = datetime.utcnow()  # ✅ datetime для PG

        # базовая валидация и парсинг
        log_uid = fields.get("log_uid") or ""
        strategy_id_s = fields.get("strategy_id") or ""
        direction = (fields.get("direction") or "").strip().lower()
        symbol = (fields.get("symbol") or "").strip().upper()
        tfs_raw = fields.get("timeframes") or ""
        trace_flag = (fields.get("trace") or "false").lower() == "true"
        deadline_ms_req = None
        try:
            if "deadline_ms" in fields:
                deadline_ms_req = int(fields["deadline_ms"])
        except Exception:
            deadline_ms_req = None

        if not log_uid or not strategy_id_s.isdigit() or direction not in ("long", "short") or not symbol or not tfs_raw:
            await infra.redis_client.xadd(DECISION_RESP_STREAM, {
                "req_id": msg_id, "status": "error", "error": "bad_request", "message": "missing or invalid fields"
            })
            log.info("[REQ] ❌ bad_request fields=%s", fields)
            return

        sid = int(strategy_id_s)
        tfs = _parse_timeframes(tfs_raw)
        if not tfs:
            await infra.redis_client.xadd(DECISION_RESP_STREAM, {
                "req_id": msg_id, "status": "error", "error": "bad_request", "message": "timeframes invalid"
            })
            log.info("[REQ] ❌ bad_request timeframes=%s", tfs_raw)
            return

        # проверки активности символа и стратегии
        if symbol not in infra.enabled_tickers:
            await infra.redis_client.xadd(DECISION_RESP_STREAM, {
                "req_id": msg_id, "status": "error", "error": "symbol_not_active", "message": f"{symbol}"
            })
            log.info("[REQ] ❌ symbol_not_active %s", symbol)
            return

        if sid not in infra.enabled_strategies:
            await infra.redis_client.xadd(DECISION_RESP_STREAM, {
                "req_id": msg_id, "status": "error", "error": "strategy_not_enabled", "message": f"{sid}"
            })
            log.info("[REQ] ❌ strategy_not_enabled %s", sid)
            return

        log.info("[REQ] 📥 log_uid=%s sid=%s %s %s tfs=%s", log_uid, sid, symbol, direction, ",".join(tfs))

        # ожидаем «шторки» WL (коротко), чтобы работать со свежими
        await infra.wait_mw_ready(sid, timeout_sec=5.0)
        await infra.wait_pack_ready(sid, timeout_sec=5.0)

        # общий дедлайн
        deadline_ms = t0 + (deadline_ms_req or SAFETY_DEADLINE_MS)

        # телеметрия
        telemetry = {"cache_hits": 0, "gateway_requests": 0}
        tf_results: List[Dict[str, Any]] = []
        allow = True
        reason: Optional[str] = None

        # обработка TF последовательно, с коротким замыканием
        for tf in tfs:
            tf_ok, tf_trace = await _process_tf(
                sid=sid,
                symbol=symbol,
                direction=direction,
                tf=tf,
                trace=trace_flag,
                deadline_ms=deadline_ms,
                telemetry=telemetry,
            )
            if trace_flag:
                tf_results.append(tf_trace)

            if not tf_ok:
                allow = False
                # проставим reason для человека
                if "mw" in tf_trace and not tf_trace["mw"].get("matched", True):
                    reason = f"mw_no_match@{tf}"
                elif "pack" in tf_trace and tf_trace["pack"].get("bl_hits", 0) > 0:
                    reason = f"pack_blacklist_hit@{tf}"
                elif "pack" in tf_trace:
                    need = tf_trace["pack"].get("required")
                    got = tf_trace["pack"].get("wl_hits")
                    reason = f"pack_not_enough_confirm@{tf}: need={need} got={got}"
                else:
                    reason = f"deny@{tf}"
                log.info("[TF:%s] ⛔ Останов по причине: %s", tf, reason)
                break
            else:
                log.info("[TF:%s] ✅ TF пройден", tf)

        # ответ стратегии
        finished_at_dt = datetime.utcnow()  # ✅ datetime для PG
        duration_ms = _now_monotonic_ms() - t0
        resp = {
            "req_id": msg_id,
            "status": "ok",
            "allow": "true" if allow else "false",
            "log_uid": log_uid,
            "strategy_id": str(sid),
            "symbol": symbol,
            "direction": direction,
            "timeframes": ",".join(tfs),
        }
        if not allow and reason:
            resp["reason"] = reason
        if trace_flag:
            try:
                resp["tf_results"] = json.dumps(tf_results, ensure_ascii=False)
            except Exception:
                pass

        await infra.redis_client.xadd(DECISION_RESP_STREAM, resp)
        log.info("[RESP] 📤 log_uid=%s sid=%s allow=%s dur=%dms", log_uid, sid, allow, duration_ms)

        # после ответа — best effort запись в БД
        try:
            tf_results_json = json.dumps(tf_results, ensure_ascii=False) if trace_flag else None
            await _persist_decision(
                req_id=msg_id,
                log_uid=log_uid,
                strategy_id=sid,
                symbol=symbol,
                direction=direction,
                tfr_req=tfs_raw,
                tfr_proc=",".join(tfs),
                allow=allow,
                reason=reason,
                tf_results_json=tf_results_json,
                received_at_dt=received_at_dt,     # ✅ datetime
                finished_at_dt=finished_at_dt,     # ✅ datetime
                duration_ms=duration_ms,
                cache_hits=telemetry.get("cache_hits", 0),
                gateway_requests=telemetry.get("gateway_requests", 0),
            )
        except Exception:
            log.exception("[AUDIT] ❌ Ошибка записи аудита log_uid=%s sid=%s", log_uid, sid)


# 🔸 Основной слушатель decision_request
async def run_laboratory_decision_maker():
    """
    Слушает laboratory:decision_request и формирует ответы в laboratory:decision_response.
    Обрабатывает только НОВЫЕ сообщения (старт с хвоста '$').
    """
    log.info("🛰️ LAB_DECISION слушатель запущен (BLOCK=%d COUNT=%d MAX=%d)",
             XREAD_BLOCK_MS, XREAD_COUNT, MAX_IN_FLIGHT_DECISIONS)

    last_id = "$"  # только новые, без истории
    redis = infra.redis_client

    while True:
        try:
            resp = await redis.xread(
                streams={DECISION_REQ_STREAM: last_id},
                count=XREAD_COUNT,
                block=XREAD_BLOCK_MS
            )
            if not resp:
                continue

            # resp: List[ (stream_name, [(msg_id, fields), ...]) ]
            for _, messages in resp:
                for msg_id, fields in messages:
                    last_id = msg_id  # двигаем хвост
                    asyncio.create_task(_process_request(msg_id, fields))

        except asyncio.CancelledError:
            log.info("⏹️ LAB_DECISION остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ LAB_DECISION ошибка в основном цикле")
            await asyncio.sleep(1.0)