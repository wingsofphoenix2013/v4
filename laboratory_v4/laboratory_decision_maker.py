# laboratory_decision_maker.py — обработчик решений (allow/deny): шторка по (gate_sid, symbol) c очередью, MW→(PACK параллельно), динамика blacklist, ответ и аудит (с client_strategy_id)

import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Инфраструктура
import laboratory_infra as infra

# 🔸 Логгер
log = logging.getLogger("LAB_DECISION")

# 🔸 Потоки и шлюз
DECISION_REQ_STREAM = "laboratory:decision_request"
DECISION_RESP_STREAM = "laboratory:decision_response"
GATEWAY_REQ_STREAM = "indicator_gateway_request"
DECISION_FILLER_STREAM = "laboratory_decision_filler"

# 🔸 Параметры производительности
XREAD_BLOCK_MS = 2000
XREAD_COUNT = 50
MAX_IN_FLIGHT_DECISIONS = 32
MAX_CONCURRENT_GATEWAY_CALLS = 32
COALESCE_TTL_SEC = 3
SAFETY_DEADLINE_MS = 60_000  # общий потолок на обработку одного запроса

# 🔸 Порядок TF
TF_ORDER = ("m5", "m15", "h1")

# 🔸 Публичные префиксы PACK-кэша
PACK_PUBLIC_PREFIX = {
    "bb": "bbpos_pack",
    "lr": "lrpos_pack",
    "atr": "atr_pack",
    "adx_dmi": "adx_dmi_pack",
    "macd": "macd_pack",
    # по умолчанию: f"{indicator}_pack"
}

# 🔸 Семафоры конкуренции
_decisions_sem = asyncio.Semaphore(MAX_IN_FLIGHT_DECISIONS)
_gateway_sem = asyncio.Semaphore(MAX_CONCURRENT_GATEWAY_CALLS)

# 🔸 Коалесценс (in-process) — key -> (expire_ms, future)
_coalesce: Dict[str, Tuple[float, asyncio.Future]] = {}


# 🔸 Парсинг и нормализация
def _parse_timeframes(tf_str: str) -> List[str]:
    items = [x.strip().lower() for x in (tf_str or "").split(",") if x.strip()]
    seen, ordered = set(), []
    for tf in TF_ORDER:
        if tf in items and tf not in seen:
            seen.add(tf)
            ordered.append(tf)
    return ordered


def _parse_pack_base(base: str) -> Tuple[str, Dict[str, Any]]:
    s = base.strip().lower()
    if s.startswith("bb"):
        rest = s[2:]
        parts = rest.split("_", 2)
        L = int(parts[0])
        std = float(parts[1].replace("_", ".", 1)) if len(parts) > 1 else 2.0
        return "bb", {"length": L, "std": std}
    if s.startswith("macd"):
        return "macd", {"fast": int(s[4:])}
    if s.startswith("adx_dmi"):
        return "adx_dmi", {"length": int(s[7:])}
    if s.startswith("ema"):
        return "ema", {"length": int(s[3:])}
    if s.startswith("rsi"):
        return "rsi", {"length": int(s[3:])}
    if s.startswith("mfi"):
        return "mfi", {"length": int(s[3:])}
    if s.startswith("lr"):
        return "lr", {"length": int(s[2:])}
    if s.startswith("atr"):
        return "atr", {"length": int(s[3:])}
    return s, {}


def _public_pack_key(indicator: str, symbol: str, tf: str, base: str) -> str:
    pref = PACK_PUBLIC_PREFIX.get(indicator, f"{indicator}_pack")
    return f"{pref}:{symbol}:{tf}:{base}"


def _public_mw_key(kind: str, symbol: str, tf: str) -> str:
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


# 🔸 Ключи шторки/очереди
def _gate_key(gate_sid: int, symbol: str) -> str:
    return f"lab:gate:{gate_sid}:{symbol}"


def _queue_key(gate_sid: int, symbol: str) -> str:
    return f"lab:qids:{gate_sid}:{symbol}"


def _qfields_key(req_id: str) -> str:
    return f"lab:qfields:{req_id}"


# 🔸 MGET JSON пачкой
async def _mget_json(keys: List[str]) -> Dict[str, Optional[dict]]:
    if not keys:
        return {}
    values = await infra.redis_client.mget(*keys)
    return {k: _json_or_none(v) for k, v in zip(keys, values)}


# 🔸 Гарантия наличия PACK через gateway (cache-first + ожидание public KV)
async def _ensure_pack_available(
    symbol: str,
    tf: str,
    indicator: str,
    base: str,
    gw_params: Dict[str, Any],
    precision: int,
    deadline_ms: int,
) -> Optional[dict]:
    key = _public_pack_key(indicator, symbol, tf, base)
    cached = await infra.redis_client.get(key)
    if cached:
        obj = _json_or_none(cached)
        if obj:
            return obj

    co_key = f"COAL::pack::{key}"
    now = _now_monotonic_ms()
    rec = _coalesce.get(co_key)
    if rec and now < rec[0]:
        fut = rec[1]
        log.debug("[PACK] ⏳ Коалесценс ожидание существующего запроса key=%s", key)
        try:
            return await asyncio.wait_for(fut, timeout=max(0.1, (deadline_ms - _now_monotonic_ms()) / 1000))
        except Exception:
            return None

    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    _coalesce[co_key] = (now + COALESCE_TTL_SEC * 1000, fut)

    async with _gateway_sem:
        try:
            req = {"symbol": symbol, "timeframe": tf, "indicator": indicator, "mode": "pack"}
            if indicator in ("ema", "rsi", "mfi", "lr", "atr", "adx_dmi"):
                L = int(gw_params.get("length", 0))
                if L:
                    req["length"] = str(L)
            elif indicator == "macd":
                F = int(gw_params.get("fast", 0))
                if F:
                    req["length"] = str(F)
            elif indicator == "bb":
                L = int(gw_params.get("length", 0))
                S = float(gw_params.get("std", 2.0))
                req["length"] = str(L)
                req["std"] = f"{S:.2f}"

            req_id = await infra.redis_client.xadd(GATEWAY_REQ_STREAM, req)
            log.debug("[PACK] 📤 GW запрос отправлен ind=%s base=%s req_id=%s key=%s", indicator, base, req_id, key)

            poll_sleep = 0.1
            while _now_monotonic_ms() < deadline_ms:
                cached = await infra.redis_client.get(key)
                if cached:
                    obj = _json_or_none(cached)
                    if obj:
                        if not fut.done():
                            fut.set_result(obj)
                        return obj
                await asyncio.sleep(poll_sleep)
                if poll_sleep < 0.2:
                    poll_sleep = 0.2

            log.debug("[PACK] ⛔ Истёк дедлайн ожидания public KV ind=%s base=%s", indicator, base)
            if not fut.done():
                fut.set_result(None)
            return None

        except Exception:
            log.exception("[PACK] ❌ Ошибка при запросе в gateway (ind=%s base=%s)", indicator, base)
            if not fut.done():
                fut.set_result(None)
            return None
        finally:
            now2 = _now_monotonic_ms()
            for ck, (exp, f) in list(_coalesce.items()):
                if now2 > exp or (f.done() and _json_or_none(f.result()) is None):
                    _coalesce.pop(ck, None)


# 🔸 Получение MW-состояний (только нужные базы)
async def _get_mw_states(
    symbol: str,
    tf: str,
    bases: List[str],
    precision: int,
    deadline_ms: int,
) -> Dict[str, Optional[str]]:
    out: Dict[str, Optional[str]] = {}
    keys = [_public_mw_key(b, symbol, tf) for b in bases]
    kv = await _mget_json(keys)

    for base in bases:
        state: Optional[str] = None
        k = _public_mw_key(base, symbol, tf)
        obj = kv.get(k)
        if obj and isinstance(obj, dict):
            st = (obj.get("pack") or {}).get("state")
            if isinstance(st, str) and st:
                state = st

        if state is None:
            obj = await _ensure_pack_available(symbol, tf, base, base, {}, precision, deadline_ms)
            if obj:
                st = (obj.get("pack") or {}).get("state")
                if isinstance(st, str) and st:
                    state = st

        out[base] = state
        log.debug("[MW] 🧩 %s %s/%s state=%s", base, symbol, tf, state)

    return out


# 🔸 Матчинг MW → required_confirmation (winrate не используем, учитываем все совпадения)
def _mw_match_and_required_confirmation(
    mw_rows: List[Dict[str, Any]],
    states: Dict[str, Optional[str]],
) -> Tuple[bool, Optional[int]]:
    if not mw_rows:
        return False, None
    matched_confs: List[int] = []

    for r in mw_rows:
        agg_base = (r.get("agg_base") or "").strip().lower()
        agg_state = (r.get("agg_state") or "").strip().lower()
        if not agg_base or not agg_state:
            continue

        bases = agg_base.split("_")
        if len(bases) == 1:
            base = bases[0]
            st = states.get(base)
            if not st:
                continue
            fact = st.strip().lower()
        else:
            parts, ok = [], True
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
            try:
                matched_confs.append(int(r.get("confirmation")))
            except Exception:
                continue

    if not matched_confs:
        return False, None
    if any(c == 0 for c in matched_confs):
        return True, 0
    req = min([c for c in matched_confs if c in (1, 2)], default=None)
    if req is None:
        return False, None
    return True, req


# 🔸 Параллельный сбор PACK объектов по нужным base
async def _get_pack_objects_for_bases(
    symbol: str, tf: str, bases: List[str], precision: int, deadline_ms: int
) -> Dict[str, Optional[dict]]:
    results: Dict[str, Optional[dict]] = {}
    keys, meta = [], []

    for base in bases:
        ind, params = _parse_pack_base(base)
        meta.append((base, ind, params))
        keys.append(_public_pack_key(ind, symbol, tf, base))

    got = await _mget_json(keys)

    tasks: List[asyncio.Task] = []
    wanted: List[Tuple[str, str, Dict[str, Any]]] = []

    for (base, ind, params) in meta:
        k = _public_pack_key(ind, symbol, tf, base)
        obj = got.get(k)
        if obj is not None:
            results[base] = obj
            log.debug("[PACK] 📦 base=%s %s/%s present=%s", base, symbol, tf, bool(obj))
        else:
            wanted.append((base, ind, params))
            tasks.append(asyncio.create_task(_ensure_pack_available(
                symbol=symbol, tf=tf, indicator=ind, base=base,
                gw_params=params, precision=precision, deadline_ms=deadline_ms
            )))

    if tasks:
        fetched = await asyncio.gather(*tasks, return_exceptions=False)
        for (base, _ind, _params), obj in zip(wanted, fetched):
            results[base] = obj
            log.debug("[PACK] 📦 base=%s %s/%s present=%s", base, symbol, tf, bool(obj))

    return results


# 🔸 Динамический учёт blacklist: детали + winrate
def _pack_bl_wl_stats_with_details(
    pack_rows: List[Dict[str, Any]],
    pack_objs: Dict[str, Optional[dict]],
) -> Tuple[int, int, List[Dict[str, Any]], List[Dict[str, Any]], List[float]]:
    """
    Возвращает:
      bl_hits, wl_hits, bl_details[], wl_details[], bl_winrates[]
    детали: {id, pack_base, agg_key, agg_value, winrate?}
    """
    bl_hits = 0
    wl_hits = 0
    bl_details: List[Dict[str, Any]] = []
    wl_details: List[Dict[str, Any]] = []
    bl_winrates: List[float] = []

    for r in pack_rows:
        base = (r.get("pack_base") or "").strip().lower()
        if not base:
            continue
        pack_obj = pack_objs.get(base)
        if not pack_obj:
            continue

        pack = pack_obj.get("pack") or {}
        list_type = (r.get("list") or "").strip().lower()  # whitelist | blacklist
        agg_key = (r.get("agg_key") or "").strip().lower()
        agg_val = (r.get("agg_value") or "").strip().lower()
        if not agg_key or not agg_val:
            continue

        keys = [k.strip() for k in agg_key.split("|") if k.strip()]
        parts, ok = [], True
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
            det = {
                "id": int(r.get("id")) if r.get("id") is not None else None,
                "pack_base": base,
                "agg_key": agg_key,
                "agg_value": agg_val,
            }
            if list_type == "blacklist":
                bl_hits += 1
                try:
                    w = float(r.get("winrate"))
                except Exception:
                    w = None
                if w is not None:
                    bl_winrates.append(w)
                    det["winrate"] = w
                bl_details.append(det)
            elif list_type == "whitelist":
                wl_hits += 1
                try:
                    w = float(r.get("winrate"))
                except Exception:
                    w = None
                if w is not None:
                    det["winrate"] = w
                wl_details.append(det)

    return bl_hits, wl_hits, bl_details, wl_details, bl_winrates

# 🔸 Обработка одного TF (MW-first; опционально — fallback на PACK по WL; BL не используется в решении)
async def _process_tf(
    sid: int,
    symbol: str,
    direction: str,
    tf: str,
    trace: bool,
    deadline_ms: int,
    telemetry: Dict[str, int],
    use_pack_fallback: bool = False,   # переключатель сценария
) -> Tuple[bool, Dict[str, Any]]:
    """
    Возвращает (tf_ok, tf_trace)

    Политика:
      1) Сначала ищем ХОТЯ БЫ ОДНО совпадение в MW WL — если есть, TF пройден (origin="mw").
      2) Если MW-совпадений НЕТ и включён fallback → ищем PACK WL:
         - BL не используем в решении (фиксируется филлером).
         - Если WL>=1 → TF пройден (origin="pack"), иначе отказ.
    """
    tf_trace: Dict[str, Any] = {"tf": tf}

    # старт TF-проверки
    log.debug("[TF:%s] ▶️ start sid=%s symbol=%s dir=%s mode=%s", tf, sid, symbol, direction,
             "mw_then_pack" if use_pack_fallback else "mw_only")

    # 1) MW: строки WL по TF/направлению
    mw_rows_all = (infra.mw_wl_by_strategy.get(sid) or {}).get("rows", [])
    mw_rows = [r for r in mw_rows_all if (r.get("timeframe") == tf and r.get("direction") == direction)]
    log.info("[TF:%s] MW rows: %d", tf, len(mw_rows))

    # Если по TF нет MW-строк вовсе
    if not mw_rows:
        if trace:
            tf_trace["mw"] = {"matched": False}
        # При отсутствии строк MW переходим сразу к fallback (если включён)
        if not use_pack_fallback:
            log.debug("[TF:%s] ❌ no MW rules and fallback=OFF — deny", tf)
            return False, tf_trace
    else:
        # 2) Снимаем MW-состояния и проверяем совпадения
        needed_bases: List[str] = []
        for r in mw_rows:
            base = (r.get("agg_base") or "").strip().lower()
            if not base:
                continue
            for b in base.split("_"):
                if b in ("trend", "volatility", "extremes", "momentum") and b not in needed_bases:
                    needed_bases.append(b)

        precision = int(infra.enabled_tickers.get(symbol, {}).get("precision_price", 7))
        log.debug("[TF:%s] MW bases to read: %s", tf, ",".join(needed_bases) if needed_bases else "-")
        states = await _get_mw_states(symbol, tf, needed_bases, precision, deadline_ms)
        # чтобы не шуметь — показываем коротко
        log.info("[TF:%s] MW states: %s", tf, states if states else "{}")

        matched, _ = _mw_match_and_required_confirmation(mw_rows, states)
        if trace:
            tf_trace["mw"] = {"matched": matched}

        if matched:
            # MW дал минимум одно совпадение — TF пройден
            log.info("[TF:%s] ✅ allow by MW", tf)
            tf_trace["origin"] = "mw"
            return True, tf_trace
        else:
            log.info("[TF:%s] ℹ️ MW has no matches", tf)

    # 3) Fallback по PACK WL (только если разрешён)
    if not use_pack_fallback:
        # MW не совпал и fallback выключен
        log.info("[TF:%s] ❌ fallback=OFF — deny", tf)
        return False, tf_trace

    # PACK-строки WL/BL по TF/направлению (в решении используем только WL)
    pack_rows_all = (infra.pack_wl_by_strategy.get(sid) or {}).get("rows", [])
    rows_tf = [r for r in pack_rows_all if (r.get("timeframe") == tf and r.get("direction") == direction)]

    # Уникальные pack_base и загрузка объектов (cache-first → gateway)
    bases: List[str] = []
    for r in rows_tf:
        base = (r.get("pack_base") or "").strip().lower()
        if base and base not in bases:
            bases.append(base)

    log.info("[TF:%s] 🧩 PACK fallback: bases=%d", tf, len(bases))
    wl_hits = 0
    if bases:
        precision = int(infra.enabled_tickers.get(symbol, {}).get("precision_price", 7))
        pack_objs: Dict[str, Optional[dict]] = await _get_pack_objects_for_bases(symbol, tf, bases, precision, deadline_ms)

        # Считаем ТОЛЬКО WL-совпадения (BL в решении не используется)
        for r in rows_tf:
            if (r.get("list") or "").strip().lower() != "whitelist":
                continue
            base = (r.get("pack_base") or "").strip().lower()
            po = pack_objs.get(base)
            if not po:
                continue
            pack = (po.get("pack") or {})
            agg_key = (r.get("agg_key") or "").strip().lower()
            agg_val = (r.get("agg_value") or "").strip().lower()
            if not agg_key or not agg_val:
                continue

            # Сформировать факт в порядке ключей
            keys_k = [k.strip() for k in agg_key.split("|") if k.strip()]
            parts, ok = [], True
            for k in keys_k:
                v = pack.get(k)
                if v is None:
                    ok = False
                    break
                parts.append(f"{k}:{str(v).strip().lower()}")
            if not ok:
                continue
            fact = "|".join(parts)

            if fact == agg_val:
                wl_hits += 1

    if trace:
        tf_trace.setdefault("pack", {})
        tf_trace["pack"]["wl_hits"] = wl_hits
        tf_trace["pack"]["fallback_used"] = True

    log.debug("[TF:%s] PACK WL hits: %d", tf, wl_hits)

    if wl_hits >= 1:
        log.info("[TF:%s] ✅ allow by PACK fallback", tf)
        tf_trace["origin"] = "pack"
        return True, tf_trace

    log.info("[TF:%s] ❌ deny (no MW match, no PACK WL)", tf)
    return False, tf_trace
                    
# 🔸 Сохранение результата (после ответа), с client_strategy_id — двухфазный upsert для partial unique indexes
async def _persist_decision(
    req_id: str,
    log_uid: str,
    strategy_id: int,                 # master SID (WL/BL)
    client_strategy_id: Optional[int],# клиентский SID (gate), может быть None
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
    async with infra.pg_pool.acquire() as conn:
        if client_strategy_id is None:
            # 1) UPDATE по (log_uid, strategy_id) при client_strategy_id IS NULL
            upd_status = await conn.execute(
                """
                UPDATE public.signal_laboratory_entries
                   SET req_id=$1,
                       direction=$2,
                       symbol=$3,
                       timeframes_requested=$4,
                       timeframes_processed=$5,
                       allow=$6,
                       reason=$7,
                       tf_results=COALESCE($8::jsonb, signal_laboratory_entries.tf_results),
                       finished_at=$9,
                       duration_ms=$10,
                       cache_hits=$11,
                       gateway_requests=$12
                 WHERE log_uid=$13 AND strategy_id=$14 AND client_strategy_id IS NULL
                """,
                #            $1   $2        $3      $4      $5      $6     $7     $8          $9              $10        $11        $12        $13       $14
                req_id, direction, symbol, tfr_req, tfr_proc, allow, reason, tf_results_json, finished_at_dt, duration_ms, cache_hits, gateway_requests, log_uid, strategy_id
            )
            if upd_status.startswith("UPDATE 1"):
                log.debug("[AUDIT] 💾 UPDATE (master-only) log_uid=%s sid=%s allow=%s", log_uid, strategy_id, allow)
                return

            # 2) INSERT … DO NOTHING (client_strategy_id=NULL)
            ins_status = await conn.execute(
                """
                INSERT INTO public.signal_laboratory_entries
                    (req_id, log_uid, strategy_id, client_strategy_id, direction, symbol,
                     timeframes_requested, timeframes_processed, protocol_version,
                     allow, reason, tf_results, errors,
                     received_at, finished_at, duration_ms, cache_hits, gateway_requests)
                VALUES ($1,$2,$3,NULL,$4,$5,
                        $6,$7,'v1',
                        $8,$9, COALESCE($10::jsonb, NULL), NULL,
                        $11,$12,$13,$14,$15)
                ON CONFLICT DO NOTHING
                """,
                #        $1   $2      $3   $4         $5   $6      $7      $8   $9     $10           $11           $12           $13        $14        $15
                req_id, log_uid, strategy_id, direction, symbol, tfr_req, tfr_proc, allow, reason, tf_results_json, received_at_dt, finished_at_dt, duration_ms, cache_hits, gateway_requests
            )
            if ins_status.endswith(" 1"):
                log.debug("[AUDIT] 💾 INSERT (master-only) log_uid=%s sid=%s allow=%s", log_uid, strategy_id, allow)
                return

            # 3) В гонке — повторный UPDATE
            await conn.execute(
                """
                UPDATE public.signal_laboratory_entries
                   SET req_id=$1,
                       direction=$2,
                       symbol=$3,
                       timeframes_requested=$4,
                       timeframes_processed=$5,
                       allow=$6,
                       reason=$7,
                       tf_results=COALESCE($8::jsonb, signal_laboratory_entries.tf_results),
                       finished_at=$9,
                       duration_ms=$10,
                       cache_hits=$11,
                       gateway_requests=$12
                 WHERE log_uid=$13 AND strategy_id=$14 AND client_strategy_id IS NULL
                """,
                req_id, direction, symbol, tfr_req, tfr_proc, allow, reason, tf_results_json,
                finished_at_dt, duration_ms, cache_hits, gateway_requests, log_uid, strategy_id
            )
            log.debug("[AUDIT] 💾 UPDATE (race-master) log_uid=%s sid=%s allow=%s", log_uid, strategy_id, allow)
            return

        else:
            # client_strategy_id IS NOT NULL

            # 1) UPDATE по (log_uid, strategy_id, client_strategy_id)
            upd_status = await conn.execute(
                """
                UPDATE public.signal_laboratory_entries
                   SET req_id=$1,
                       direction=$2,
                       symbol=$3,
                       timeframes_requested=$4,
                       timeframes_processed=$5,
                       allow=$6,
                       reason=$7,
                       tf_results=COALESCE($8::jsonb, signal_laboratory_entries.tf_results),
                       finished_at=$9,
                       duration_ms=$10,
                       cache_hits=$11,
                       gateway_requests=$12
                 WHERE log_uid=$13 AND strategy_id=$14 AND client_strategy_id=$15
                """,
                #            $1   $2        $3      $4      $5      $6     $7     $8          $9              $10        $11        $12        $13       $14           $15
                req_id, direction, symbol, tfr_req, tfr_proc, allow, reason, tf_results_json, finished_at_dt, duration_ms, cache_hits, gateway_requests, log_uid, strategy_id, int(client_strategy_id)
            )
            if upd_status.startswith("UPDATE 1"):
                log.debug("[AUDIT] 💾 UPDATE (client) log_uid=%s sid=%s csid=%s allow=%s", log_uid, strategy_id, client_strategy_id, allow)
                return

            # 2) INSERT … DO NOTHING (client_strategy_id NOT NULL)
            ins_status = await conn.execute(
                """
                INSERT INTO public.signal_laboratory_entries
                    (req_id, log_uid, strategy_id, client_strategy_id, direction, symbol,
                     timeframes_requested, timeframes_processed, protocol_version,
                     allow, reason, tf_results, errors,
                     received_at, finished_at, duration_ms, cache_hits, gateway_requests)
                VALUES ($1,$2,$3,$4,$5,$6,
                        $7,$8,'v1',
                        $9,$10, COALESCE($11::jsonb, NULL), NULL,
                        $12,$13,$14,$15,$16)
                ON CONFLICT DO NOTHING
                """,
                #        $1   $2      $3   $4                   $5   $6      $7      $8   $9     $10           $11           $12           $13        $14        $15        $16
                req_id, log_uid, strategy_id, int(client_strategy_id), direction, symbol, tfr_req, tfr_proc, allow, reason, tf_results_json, received_at_dt, finished_at_dt, duration_ms, cache_hits, gateway_requests
            )
            if ins_status.endswith(" 1"):
                log.debug("[AUDIT] 💾 INSERT (client) log_uid=%s sid=%s csid=%s allow=%s", log_uid, strategy_id, client_strategy_id, allow)
                return

            # 3) В гонке — повторный UPDATE
            await conn.execute(
                """
                UPDATE public.signal_laboratory_entries
                   SET req_id=$1,
                       direction=$2,
                       symbol=$3,
                       timeframes_requested=$4,
                       timeframes_processed=$5,
                       allow=$6,
                       reason=$7,
                       tf_results=COALESCE($8::jsonb, signal_laboratory_entries.tf_results),
                       finished_at=$9,
                       duration_ms=$10,
                       cache_hits=$11,
                       gateway_requests=$12
                 WHERE log_uid=$13 AND strategy_id=$14 AND client_strategy_id=$15
                """,
                req_id, direction, symbol, tfr_req, tfr_proc, allow, reason, tf_results_json,
                finished_at_dt, duration_ms, cache_hits, gateway_requests, log_uid, strategy_id, int(client_strategy_id)
            )
            log.debug("[AUDIT] 💾 UPDATE (race-client) log_uid=%s sid=%s csid=%s allow=%s", log_uid, strategy_id, client_strategy_id, allow)
            return
            
# 🔸 Шторка/очередь: попытка стать лидером или постановка в очередь
async def _acquire_gate_or_enqueue(
    msg_id: str,
    fields: Dict[str, str],
    gate_sid: int,
    symbol: str,
    gate_ttl_sec: int = 60,
) -> Tuple[bool, Optional[str]]:
    gk = _gate_key(gate_sid, symbol)
    qk = _queue_key(gate_sid, symbol)
    fk = _qfields_key(msg_id)

    ok = await infra.redis_client.set(gk, msg_id, ex=gate_ttl_sec, nx=True)
    if ok:
        log.debug("[GATE] 🔐 Лидер получен gate_sid=%s %s req_id=%s", gate_sid, symbol, msg_id)
        return True, None

    await infra.redis_client.rpush(qk, msg_id)
    await infra.redis_client.set(fk, json.dumps(fields, ensure_ascii=False), ex=gate_ttl_sec + 60)
    log.debug("[GATE] ⏸️ В очередь gate_sid=%s %s req_id=%s", gate_sid, symbol, msg_id)
    return False, "enqueued"


# 🔸 Реакция на завершение лидера
async def _on_leader_finished(gate_sid: int, symbol: str, leader_req_id: str, allow: bool):
    gk = _gate_key(gate_sid, symbol)
    qk = _queue_key(gate_sid, symbol)
    await infra.redis_client.delete(gk)

    if allow:
        pending = await infra.redis_client.lrange(qk, 0, -1)
        await infra.redis_client.delete(qk)
        if pending:
            log.debug("[GATE] 🚫 DUPLICATED gate_sid=%s %s — отказываем очереди (%d шт.)", gate_sid, symbol, len(pending))
            for req_id in pending:
                await infra.redis_client.xadd(DECISION_RESP_STREAM, {
                    "req_id": req_id, "status": "ok", "allow": "false", "reason": "duplicated_entry"
                })
                await infra.redis_client.delete(_qfields_key(req_id))
        return

    next_req_id = await infra.redis_client.lpop(qk)
    if not next_req_id:
        log.debug("[GATE] 🔁 Очередь пуста gate_sid=%s %s — ждём новые запросы", gate_sid, symbol)
        return

    ok = await infra.redis_client.set(gk, next_req_id, ex=60, nx=True)
    if not ok:
        log.debug("[GATE] ⚠️ Не удалось назначить нового лидера gate_sid=%s %s req=%s", gate_sid, symbol, next_req_id)
        return

    raw = await infra.redis_client.get(_qfields_key(next_req_id))
    if not raw:
        await infra.redis_client.xadd(DECISION_RESP_STREAM, {
            "req_id": next_req_id, "status": "error", "error": "internal_error", "message": "queued payload missing"
        })
        log.debug("[GATE] ⚠️ Нет полей для queued req_id=%s — отправлен error", next_req_id)
        return

    try:
        fields = json.loads(raw)
    except Exception:
        await infra.redis_client.xadd(DECISION_RESP_STREAM, {
            "req_id": next_req_id, "status": "error", "error": "internal_error", "message": "queued payload invalid"
        })
        log.debug("[GATE] ⚠️ Невалидные поля для queued req_id=%s — отправлен error", next_req_id)
        return

    asyncio.create_task(_process_request_core(next_req_id, fields))

# 🔸 Ядро обработки запроса (для лидера) — MW-first; опционально fallback на PACK (WL), BL не участвует в решении
async def _process_request_core(msg_id: str, fields: Dict[str, str]):
    async with _decisions_sem:
        t0 = _now_monotonic_ms()
        received_at_dt = datetime.utcnow()

        log_uid = fields.get("log_uid") or ""
        strategy_id_s = fields.get("strategy_id") or ""
        client_sid_s = fields.get("client_strategy_id") or ""  # опционально
        direction = (fields.get("direction") or "").strip().lower()
        symbol = (fields.get("symbol") or "").strip().upper()
        tfs_raw = fields.get("timeframes") or ""
        trace_flag = (fields.get("trace") or "false").lower() == "true"

        # 🔸 Режим принятия решения: только MW или MW с fallback на PACK (WL)
        decision_mode = (fields.get("decision_mode") or "").strip().lower()
        use_pack_fallback = False
        if decision_mode in ("mw_then_pack", "mw_pack", "pack_fallback"):
            use_pack_fallback = True
        elif decision_mode in ("mw_only", ""):
            fb_raw = (fields.get("fallback_pack") or "").strip().lower()
            use_pack_fallback = (fb_raw == "true")
        # Нормализованный текстовый режим
        normalized_mode = "mw_then_pack" if use_pack_fallback else "mw_only"

        deadline_ms_req = None
        try:
            if "deadline_ms" in fields:
                deadline_ms_req = int(fields["deadline_ms"])
        except Exception:
            deadline_ms_req = None

        # 🔸 Базовая валидация
        if not log_uid or not strategy_id_s.isdigit() or direction not in ("long", "short") or not symbol or not tfs_raw:
            await infra.redis_client.xadd(DECISION_RESP_STREAM, {
                "req_id": msg_id, "status": "error", "error": "bad_request", "message": "missing or invalid fields"
            })
            log.debug("[REQ] ❌ bad_request fields=%s", fields)
            return

        # master SID (для WL/BL)
        sid = int(strategy_id_s)
        # gate SID (для шторки/очереди): клиентский, если передан и валиден, иначе master
        gate_sid = int(client_sid_s) if client_sid_s.isdigit() else sid

        tfs = _parse_timeframes(tfs_raw)
        if not tfs:
            await infra.redis_client.xadd(DECISION_RESP_STREAM, {
                "req_id": msg_id, "status": "error", "error": "bad_request", "message": "timeframes invalid"
            })
            log.debug("[REQ] ❌ bad_request timeframes=%s", tfs_raw)
            return

        if symbol not in infra.enabled_tickers:
            await infra.redis_client.xadd(DECISION_RESP_STREAM, {
                "req_id": msg_id, "status": "error", "error": "symbol_not_active", "message": f"{symbol}"
            })
            log.debug("[REQ] ❌ symbol_not_active %s", symbol)
            return

        if sid not in infra.enabled_strategies:
            await infra.redis_client.xadd(DECISION_RESP_STREAM, {
                "req_id": msg_id, "status": "error", "error": "strategy_not_enabled", "message": f"{sid}"
            })
            log.debug("[REQ] ❌ strategy_not_enabled %s", sid)
            return

        log.debug("[REQ] 📥 log_uid=%s master_sid=%s client_sid=%s %s %s tfs=%s mode=%s",
                  log_uid, sid, (client_sid_s or "-"), symbol, direction, ",".join(tfs), normalized_mode)

        # ждём «шторки» MW (PACK в decision-path вызывается только при fallback)
        await infra.wait_mw_ready(sid, timeout_sec=5.0)

        deadline_ms = t0 + (deadline_ms_req or SAFETY_DEADLINE_MS)

        telemetry = {"cache_hits": 0, "gateway_requests": 0}
        tf_results: List[Dict[str, Any]] = []
        tf_origins: Dict[str, str] = {}  # для фиксации источника прохода TF: "mw" | "pack"
        allow = True
        reason: Optional[str] = None

        # последовательная проверка TF (MW-first, опционально fallback на PACK WL)
        for tf in tfs:
            tf_ok, tf_trace = await _process_tf(
                sid=sid,
                symbol=symbol,
                direction=direction,
                tf=tf,
                trace=trace_flag,
                deadline_ms=deadline_ms,
                telemetry=telemetry,
                use_pack_fallback=use_pack_fallback,
            )

            # Сохраним origin для текущего TF (если он пройден)
            if tf_ok:
                origin = tf_trace.get("origin")
                if origin in ("mw", "pack"):
                    tf_origins[tf] = origin

            if trace_flag:
                tf_results.append(tf_trace)

            if not tf_ok:
                allow = False
                mw_matched = bool(tf_trace.get("mw", {}).get("matched", False))
                if not mw_matched and use_pack_fallback:
                    # MW нет, fallback разрешён, но WL по PACK не нашлось
                    reason = f"pack_no_wl@{tf}"
                else:
                    # либо fallback выключен, либо MW-совпадений нет
                    reason = f"mw_no_match@{tf}"
                log.debug("[TF:%s] ⛔ Останов по причине: %s", tf, reason)
                break
            else:
                log.debug("[TF:%s] ✅ TF пройден (origin=%s)", tf, tf_origins.get(tf, "mw"))

        finished_at_dt = datetime.utcnow()
        duration_ms = _now_monotonic_ms() - t0

        # формируем ответ
        resp = {
            "req_id": msg_id,
            "status": "ok",
            "allow": "true" if allow else "false",
            "log_uid": log_uid,
            "strategy_id": str(sid),           # master SID
            "direction": direction,
            "symbol": symbol,
            "timeframes": ",".join(tfs),
        }
        if client_sid_s:
            resp["client_strategy_id"] = client_sid_s
        if not allow and reason:
            resp["reason"] = reason
        if trace_flag:
            try:
                resp["tf_results"] = json.dumps(tf_results, ensure_ascii=False)
            except Exception:
                pass

        await infra.redis_client.xadd(DECISION_RESP_STREAM, resp)
        log.debug("[RESP] 📤 log_uid=%s master_sid=%s client_sid=%s allow=%s dur=%dms",
                  log_uid, sid, (client_sid_s or "-"), allow, duration_ms)

        # 🔸 Публикация seed-события для наполнителя статистики (только при allow=true)
        if allow:
            # компактная строка источников прохода TF: "m5:mw,m15:pack"
            decision_tf_origins = ",".join(f"{k}:{v}" for k, v in tf_origins.items()) if tf_origins else ""

            filler_payload = {
                "log_uid": log_uid,
                "strategy_id": str(sid),
                "symbol": symbol,
                "direction": direction,
                "timeframes": ",".join(tfs),
                "trace_basis": normalized_mode,
                "decision_mode": normalized_mode,
                "decision_tf_origins": decision_tf_origins,
            }
            if client_sid_s:
                filler_payload["client_strategy_id"] = client_sid_s

            try:
                await infra.redis_client.xadd(DECISION_FILLER_STREAM, filler_payload)
                log.debug("[FILLER] seed published log_uid=%s master_sid=%s client_sid=%s tfs=%s mode=%s origins=%s",
                          log_uid, sid, (client_sid_s or "-"), ",".join(tfs), normalized_mode, decision_tf_origins)
            except Exception:
                log.exception("[FILLER] ❌ Ошибка публикации seed-события log_uid=%s", log_uid)

        # запись в БД (журнал решений)
        try:
            tf_results_json = json.dumps(tf_results, ensure_ascii=False) if trace_flag else None
            await _persist_decision(
                req_id=msg_id,
                log_uid=log_uid,
                strategy_id=sid,
                client_strategy_id=int(client_sid_s) if client_sid_s.isdigit() else None,
                symbol=symbol,
                direction=direction,
                tfr_req=tfs_raw,
                tfr_proc=",".join(tfs),
                allow=allow,
                reason=reason,
                tf_results_json=tf_results_json,
                received_at_dt=received_at_dt,
                finished_at_dt=finished_at_dt,
                duration_ms=duration_ms,
                cache_hits=telemetry.get("cache_hits", 0),
                gateway_requests=telemetry.get("gateway_requests", 0),
            )
        except Exception:
            log.exception("[AUDIT] ❌ Ошибка записи аудита log_uid=%s master_sid=%s client_sid=%s", log_uid, sid, client_sid_s or "-")

        # реакция ворот (используем gate_sid)
        await _on_leader_finished(gate_sid=gate_sid, symbol=symbol, leader_req_id=msg_id, allow=allow)
                        
# 🔸 Обработка входящего: шторка/очередь → лидер или ожидание
async def _handle_incoming(msg_id: str, fields: Dict[str, str]):
    strategy_id_s = fields.get("strategy_id") or ""
    client_sid_s = fields.get("client_strategy_id") or ""
    symbol = (fields.get("symbol") or "").strip().upper()
    if not strategy_id_s.isdigit() or not symbol:
        await infra.redis_client.xadd(DECISION_RESP_STREAM, {
            "req_id": msg_id, "status": "error", "error": "bad_request", "message": "missing sid/symbol"
        })
        log.debug("[REQ] ❌ bad_request (no sid/symbol) fields=%s", fields)
        return

    sid = int(strategy_id_s)
    gate_sid = int(client_sid_s) if client_sid_s.isdigit() else sid

    is_leader, _ = await _acquire_gate_or_enqueue(msg_id, fields, gate_sid, symbol, gate_ttl_sec=60)
    if is_leader:
        await _process_request_core(msg_id, fields)
    else:
        log.debug("[REQ] ⏳ Запрос поставлен в очередь gate_sid=%s %s req_id=%s", gate_sid, symbol, msg_id)


# 🔸 Главный слушатель decision_request
async def run_laboratory_decision_maker():
    """
    Слушает laboratory:decision_request и формирует ответы в laboratory:decision_response.
    Обрабатывает только НОВЫЕ сообщения (старт с '$'). Встроены шторка/очередь per (gate_sid, symbol),
    где gate_sid = client_strategy_id (если передан) иначе strategy_id (master).
    """
    log.debug("🛰️ LAB_DECISION слушатель запущен (BLOCK=%d COUNT=%d MAX=%d)",
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

            for _, messages in resp:
                for msg_id, fields in messages:
                    last_id = msg_id
                    asyncio.create_task(_handle_incoming(msg_id, fields))

        except asyncio.CancelledError:
            log.debug("⏹️ LAB_DECISION остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ LAB_DECISION ошибка в основном цикле")
            await asyncio.sleep(1.0)