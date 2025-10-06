# laboratory_decision_maker.py — Этап 2: сбор полного снимка MW/PACK по запросу (всегда deny) + подсчёт совпадений (MW-WL / PACK-WL / PACK-BL) и запись по КАЖДОМУ TF

# 🔸 Импорты
import asyncio
import json
import logging
import time
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Инфраструктура
import laboratory_infra as infra

# 🔸 Логгер
log = logging.getLogger("LAB_DECISION")

# 🔸 Потоки и шлюзы
DECISION_REQ_STREAM = "laboratory:decision_request"
DECISION_RESP_STREAM = "laboratory:decision_response"
GATEWAY_REQ_STREAM = "indicator_gateway_request"

# 🔸 Параметры производительности/дедлайнов/конкуренции
LAB_DEADLINE_MS = 90_000                 # общий дедлайн на обработку одного запроса (90с)
XREAD_BLOCK_MS = 1_000                   # блокирующее чтение входного стрима
XREAD_COUNT = 50
MAX_IN_FLIGHT_DECISIONS = 32
MAX_CONCURRENT_GATEWAY_CALLS = 32
COALESCE_TTL_SEC = 3                     # коалесценс одинаковых gateway-запросов внутри процесса
GATE_TTL_SEC = 100                       # TTL «ворот» (gate_sid,symbol), чуть больше дедлайна

# 🔸 Таймфреймы (порядок нормализации)
TF_ORDER = ("m5", "m15", "h1")

# 🔸 Публичные префиксы PACK-кэша (KV)
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

# 🔸 Коалесценс (in-process): key -> (expire_ms, future)
_coalesce: Dict[str, Tuple[float, asyncio.Future]] = {}

# 🔸 Преобразование произвольного объекта к JSON-safe виду
def _to_json_safe(obj: Any) -> Any:
    # простые типы
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    # Decimal -> float
    if isinstance(obj, Decimal):
        try:
            return float(obj)
        except Exception:
            return str(obj)
    # datetime -> ISO
    if isinstance(obj, datetime):
        try:
            return obj.replace(tzinfo=None).isoformat()
        except Exception:
            return str(obj)
    # dict -> рекурсивно
    if isinstance(obj, dict):
        return {k: _to_json_safe(v) for k, v in obj.items()}
    # list/tuple -> рекурсивно
    if isinstance(obj, (list, tuple)):
        return [_to_json_safe(v) for v in obj]
    # fallback: строковое представление
    return str(obj)

# 🔸 Вспомогательные парсеры/утилиты
def _parse_timeframes(tf_str: str) -> List[str]:
    items = [x.strip().lower() for x in (tf_str or "").split(",") if x.strip()]
    seen, ordered = set(), []
    for tf in TF_ORDER:
        if tf in items and tf not in seen:
            seen.add(tf)
            ordered.append(tf)
    return ordered

# 🔸 Парсинг pack_base → (indicator, params)
def _parse_pack_base(base: str) -> Tuple[str, Dict[str, Any]]:
    s = (base or "").strip().lower()
    if not s:
        return "", {}
    if s.startswith("bb"):
        rest = s[2:]
        parts = rest.split("_", 2)
        L = int(parts[0])
        std = float(parts[1].replace("_", ".", 1)) if len(parts) > 1 else 2.0
        return "bb", {"length": L, "std": std}
    if s.startswith("macd"):
        return "macd", {"fast": int(s[4:] or 0)}
    if s.startswith("adx_dmi"):
        return "adx_dmi", {"length": int(s[7:] or 0)}
    if s.startswith("ema"):
        return "ema", {"length": int(s[3:] or 0)}
    if s.startswith("rsi"):
        return "rsi", {"length": int(s[3:] or 0)}
    if s.startswith("mfi"):
        return "mfi", {"length": int(s[3:] or 0)}
    if s.startswith("lr"):
        return "lr", {"length": int(s[2:] or 0)}
    if s.startswith("atr"):
        return "atr", {"length": int(s[3:] or 0)}
    # MW packs (trend/volatility/momentum/extremes)
    if s in ("trend", "volatility", "momentum", "extremes"):
        return s, {}
    return s, {}

# 🔸 Формирование публичного KV-ключа для PACK-объекта
def _public_pack_key(indicator: str, symbol: str, tf: str, base: str) -> str:
    pref = PACK_PUBLIC_PREFIX.get(indicator, f"{indicator}_pack")
    return f"{pref}:{symbol}:{tf}:{base}"

# 🔸 Публичный KV-ключ MW-пакета
def _public_mw_key(kind: str, symbol: str, tf: str) -> str:
    return f"{kind}_pack:{symbol}:{tf}:{kind}"

# 🔸 Безопасный json.loads
def _json_or_none(s: Optional[str]) -> Optional[dict]:
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return None

# 🔸 Текущее монотонное время в мс
def _now_monotonic_ms() -> int:
    return int(time.monotonic() * 1000)

# 🔸 MGET JSON пачкой
async def _mget_json(keys: List[str]) -> Dict[str, Optional[dict]]:
    if not keys:
        return {}
    values = await infra.redis_client.mget(*keys)
    return {k: _json_or_none(v) for k, v in zip(keys, values)}

# 🔸 Запрос и ожидание появления PACK в публичном KV (cache-first, с коалесценсом)
async def _ensure_pack_available(
    symbol: str,
    tf: str,
    indicator: str,
    base: str,
    gw_params: Dict[str, Any],
    deadline_ms: int,
    telemetry: Dict[str, int],
) -> Tuple[Optional[dict], str]:
    """
    Возвращает (obj, source), где source ∈ {"kv","gateway","timeout"}.
    """
    key = _public_pack_key(indicator, symbol, tf, base)
    # cache-first
    cached = await infra.redis_client.get(key)
    if cached:
        obj = _json_or_none(cached)
        if obj:
            telemetry["kv_hits"] = telemetry.get("kv_hits", 0) + 1
            return obj, "kv"

    # коалесценс одинаковых запросов в процессе
    co_key = f"COAL::pack::{key}"
    now = _now_monotonic_ms()
    rec = _coalesce.get(co_key)
    if rec and now < rec[0]:
        fut = rec[1]
        try:
            obj = await asyncio.wait_for(
                fut, timeout=max(0.05, (deadline_ms - _now_monotonic_ms()) / 1000)
            )
            if obj:
                return obj, "gateway"
            return None, "timeout"
        except Exception:
            return None, "timeout"

    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    _coalesce[co_key] = (now + COALESCE_TTL_SEC * 1000, fut)

    async with _gateway_sem:
        try:
            # формируем запрос к gateway
            req: Dict[str, Any] = {"symbol": symbol, "timeframe": tf, "indicator": indicator, "mode": "pack"}
            if indicator in ("ema", "rsi", "mfi", "lr", "atr", "adx_dmi"):
                L = int(gw_params.get("length") or 0)
                if L:
                    req["length"] = str(L)
            elif indicator == "macd":
                F = int(gw_params.get("fast") or 0)
                if F:
                    req["length"] = str(F)
            elif indicator == "bb":
                L = int(gw_params.get("length") or 0)
                S = float(gw_params.get("std") or 2.0)
                if L:
                    req["length"] = str(L)
                req["std"] = f"{S:.2f}"

            # отправляем запрос и ждём появления публичного KV до дедлайна
            telemetry["gateway_requests"] = telemetry.get("gateway_requests", 0) + 1
            await infra.redis_client.xadd(GATEWAY_REQ_STREAM, req)

            poll_sleep = 0.1
            while _now_monotonic_ms() < deadline_ms:
                cached = await infra.redis_client.get(key)
                if cached:
                    obj = _json_or_none(cached)
                    if obj:
                        if not fut.done():
                            fut.set_result(obj)
                        return obj, "gateway"
                await asyncio.sleep(poll_sleep)
                if poll_sleep < 0.2:
                    poll_sleep = 0.2

            # истёк дедлайн
            if not fut.done():
                fut.set_result(None)
            return None, "timeout"

        except Exception:
            log.exception("[PACK] ❌ Ошибка запроса к gateway ind=%s base=%s", indicator, base)
            if not fut.done():
                fut.set_result(None)
            return None, "timeout"
        finally:
            # очистка устаревших записей коалесценса
            now2 = _now_monotonic_ms()
            for ck, (exp, f) in list(_coalesce.items()):
                if now2 > exp or (f.done() and _json_or_none(f.result()) is None):
                    _coalesce.pop(ck, None)

# 🔸 Снятие MW-пакета (cache-first → gateway)
async def _get_mw_pack(symbol: str, tf: str, kind: str, deadline_ms: int, telemetry: Dict[str, int]) -> Tuple[Optional[dict], str]:
    key = _public_mw_key(kind, symbol, tf)
    cached = await infra.redis_client.get(key)
    if cached:
        obj = _json_or_none(cached)
        if obj:
            telemetry["kv_hits"] = telemetry.get("kv_hits", 0) + 1
            return obj, "kv"

    # если нет — обратимся к gateway (ind=kind, base=kind)
    obj, src = await _ensure_pack_available(
        symbol=symbol, tf=tf, indicator=kind, base=kind, gw_params={}, deadline_ms=deadline_ms, telemetry=telemetry
    )
    return obj, src

# 🔸 Снимок MW для (sid, symbol, direction, tf)
async def _collect_mw_snapshot(
    sid: int, symbol: str, direction: str, tf: str, deadline_ms: int, telemetry: Dict[str, int]
) -> Dict[str, Any]:
    out: Dict[str, Any] = {"states": {}, "rules": []}

    # always 4 MW bases
    for kind in ("trend", "volatility", "momentum", "extremes"):
        obj, src = await _get_mw_pack(symbol, tf, kind, deadline_ms, telemetry)
        if obj:
            obj2 = dict(obj)
            obj2["source"] = src
            out["states"][kind] = obj2
        else:
            out["states"][kind] = {"source": "timeout", "pack": None}

    # MW правила из кэша стратегии
    mw_rows_all = (infra.mw_wl_by_strategy.get(sid) or {}).get("rows", [])
    out["rules"] = [
        dict(r) for r in mw_rows_all
        if (str(r.get("timeframe")) == tf and str(r.get("direction")).lower() == direction)
    ]
    return out

# 🔸 Снимок PACK для (sid, symbol, direction, tf)
async def _collect_pack_snapshot(
    sid: int, symbol: str, direction: str, tf: str, deadline_ms: int, telemetry: Dict[str, int]
) -> Dict[str, Any]:
    out: Dict[str, Any] = {"objects": {}, "rules": []}

    # все правила PACK (WL и BL) для данного TF/направления
    pack_rows_all = (infra.pack_wl_by_strategy.get(sid) or {}).get("rows", [])
    rows_tf = [
        dict(r) for r in pack_rows_all
        if (str(r.get("timeframe")) == tf and str(r.get("direction")).lower() == direction)
    ]
    out["rules"] = rows_tf

    # список уникальных base из правил
    bases: List[str] = []
    for r in rows_tf:
        base = (r.get("pack_base") or "").strip().lower()
        if base and base not in bases:
            bases.append(base)

    # загрузка PACK-объектов по base
    tasks: List[asyncio.Task] = []
    meta: List[Tuple[str, str, Dict[str, Any]]] = []
    for base in bases:
        ind, params = _parse_pack_base(base)
        meta.append((base, ind, params))
        tasks.append(asyncio.create_task(_ensure_pack_available(
            symbol=symbol, tf=tf, indicator=ind, base=base, gw_params=params, deadline_ms=deadline_ms, telemetry=telemetry
        )))

    if tasks:
        fetched = await asyncio.gather(*tasks, return_exceptions=False)
        for (base, _ind, _params), (obj, src) in zip(meta, fetched):
            if obj:
                obj2 = dict(obj)
                obj2["source"] = src
                out["objects"][base] = obj2
            else:
                out["objects"][base] = {"source": "timeout", "pack": None}

    return out

# 🔸 Построение факта для MW по agg_base
def _build_mw_fact(states: Dict[str, Any], agg_base: str) -> Optional[str]:
    if not agg_base:
        return None
    parts: List[str] = []
    for base in (agg_base.strip().lower().split("_")):
        node = states.get(base) or {}
        pack = node.get("pack") or {}
        st = pack.get("state")
        if not isinstance(st, str) or not st:
            return None
        parts.append(f"{base}:{st.strip().lower()}")
    return "|".join(parts)

# 🔸 Подсчёт совпадений для MW-WL
def _mw_count_hits(mw_rules: List[Dict[str, Any]], states: Dict[str, Any]) -> Tuple[int, int]:
    total = 0
    hits = 0
    for r in mw_rules:
        agg_base = str(r.get("agg_base") or "").strip().lower()
        agg_state = str(r.get("agg_state") or "").strip().lower()
        if not agg_base or not agg_state:
            continue
        total += 1
        fact = _build_mw_fact(states, agg_base)
        if fact is not None and fact == agg_state:
            hits += 1
    return hits, total

# 🔸 Построение факта для PACK по agg_key из pack payload
def _build_pack_fact(pack_obj: Dict[str, Any], agg_key: str) -> Optional[str]:
    if not agg_key:
        return None
    pack_payload = (pack_obj or {}).get("pack") or {}
    keys = [k.strip() for k in agg_key.strip().lower().split("|") if k.strip()]
    parts: List[str] = []
    for k in keys:
        v = pack_payload.get(k)
        if v is None:
            return None
        parts.append(f"{k}:{str(v).strip().lower()}")
    return "|".join(parts)

# 🔸 Подсчёт совпадений для PACK (WL/BL)
def _pack_count_hits(
    pack_rules: List[Dict[str, Any]],
    pack_objs: Dict[str, Optional[dict]],
) -> Tuple[int, int, int, int]:
    """
    Возвращает: (wl_hits, wl_total, bl_hits, bl_total)
    """
    wl_total = 0
    wl_hits = 0
    bl_total = 0
    bl_hits = 0

    for r in pack_rules:
        list_type = str(r.get("list") or "").strip().lower()
        base = str(r.get("pack_base") or "").strip().lower()
        agg_key = str(r.get("agg_key") or "").strip().lower()
        agg_val = str(r.get("agg_value") or "").strip().lower()
        if not base or not agg_key or not agg_val:
            continue

        # считаем totals по типу
        if list_type == "whitelist":
            wl_total += 1
        elif list_type == "blacklist":
            bl_total += 1
        else:
            continue

        pack_obj = pack_objs.get(base)
        if not pack_obj:
            # объект не собран/timeout — не считаем hit
            continue

        fact = _build_pack_fact(pack_obj, agg_key)
        if fact is None:
            continue

        if fact == agg_val:
            if list_type == "whitelist":
                wl_hits += 1
            else:
                bl_hits += 1

    return wl_hits, wl_total, bl_hits, bl_total

# 🔸 Персист одной строки по КОНКРЕТНОМУ TF (Этап 2: всегда allow=false + reason=stage2_count_only + счётчики)
async def _persist_decision_tf(
    req_id: str,
    log_uid: str,
    strategy_id: int,
    client_strategy_id: Optional[int],
    symbol: str,
    direction: str,
    tf: str,
    tfr_req: str,
    tf_result_json: Optional[str],
    received_at_dt: datetime,
    finished_at_dt: datetime,
    duration_ms: int,
    kv_hits: int,
    gateway_requests: int,
    mw_wl_hits: int,
    mw_wl_total: int,
    pack_wl_hits: int,
    pack_wl_total: int,
    pack_bl_hits: int,
    pack_bl_total: int,
):
    async with infra.pg_pool.acquire() as conn:
        if client_strategy_id is None:
            # update (master-only)
            upd_status = await conn.execute(
                """
                UPDATE public.signal_laboratory_entries
                   SET req_id=$1,
                       direction=$2,
                       symbol=$3,
                       tf=$4,
                       timeframes_requested=$5,
                       timeframes_processed=$6,
                       allow=false,
                       reason='stage2_count_only',
                       tf_results=COALESCE($7::jsonb, signal_laboratory_entries.tf_results),
                       finished_at=$8,
                       duration_ms=$9,
                       cache_hits=$10,
                       gateway_requests=$11,
                       mw_wl_rules_total=$12,
                       mw_wl_hits=$13,
                       pack_wl_rules_total=$14,
                       pack_wl_hits=$15,
                       pack_bl_rules_total=$16,
                       pack_bl_hits=$17
                 WHERE log_uid=$18 AND strategy_id=$19 AND client_strategy_id IS NULL AND tf=$4
                """,
                req_id, direction, symbol, tf, tfr_req, tf, tf_result_json,
                finished_at_dt, duration_ms, kv_hits, gateway_requests,
                mw_wl_total, mw_wl_hits, pack_wl_total, pack_wl_hits, pack_bl_total, pack_bl_hits,
                log_uid, strategy_id
            )
            if upd_status.startswith("UPDATE 1"):
                return
            # insert (master-only)
            ins_status = await conn.execute(
                """
                INSERT INTO public.signal_laboratory_entries
                    (req_id, log_uid, strategy_id, client_strategy_id, direction, symbol, tf,
                     timeframes_requested, timeframes_processed, protocol_version,
                     allow, reason, tf_results, errors,
                     received_at, finished_at, duration_ms, cache_hits, gateway_requests,
                     mw_wl_rules_total, mw_wl_hits, pack_wl_rules_total, pack_wl_hits, pack_bl_rules_total, pack_bl_hits)
                VALUES ($1,$2,$3,NULL,$4,$5,$6,
                        $7,$8,'v1',
                        false,'stage2_count_only',COALESCE($9::jsonb,NULL),NULL,
                        $10,$11,$12,$13,$14,
                        $15,$16,$17,$18,$19,$20)
                ON CONFLICT DO NOTHING
                """,
                req_id, log_uid, strategy_id, direction, symbol, tf,
                tfr_req, tf, tf_result_json,
                received_at_dt, finished_at_dt, duration_ms, kv_hits, gateway_requests,
                mw_wl_total, mw_wl_hits, pack_wl_total, pack_wl_hits, pack_bl_total, pack_bl_hits
            )
            if ins_status.endswith(" 1"):
                return
            # race: update again
            await conn.execute(
                """
                UPDATE public.signal_laboratory_entries
                   SET req_id=$1,
                       direction=$2,
                       symbol=$3,
                       tf=$4,
                       timeframes_requested=$5,
                       timeframes_processed=$6,
                       allow=false,
                       reason='stage2_count_only',
                       tf_results=COALESCE($7::jsonb, signal_laboratory_entries.tf_results),
                       finished_at=$8,
                       duration_ms=$9,
                       cache_hits=$10,
                       gateway_requests=$11,
                       mw_wl_rules_total=$12,
                       mw_wl_hits=$13,
                       pack_wl_rules_total=$14,
                       pack_wl_hits=$15,
                       pack_bl_rules_total=$16,
                       pack_bl_hits=$17
                 WHERE log_uid=$18 AND strategy_id=$19 AND client_strategy_id IS NULL AND tf=$4
                """,
                req_id, direction, symbol, tf, tfr_req, tf, tf_result_json,
                finished_at_dt, duration_ms, kv_hits, gateway_requests,
                mw_wl_total, mw_wl_hits, pack_wl_total, pack_wl_hits, pack_bl_total, pack_bl_hits,
                log_uid, strategy_id
            )
            return

        # client_strategy_id IS NOT NULL
        upd_status = await conn.execute(
            """
            UPDATE public.signal_laboratory_entries
               SET req_id=$1,
                   direction=$2,
                   symbol=$3,
                   tf=$4,
                   timeframes_requested=$5,
                   timeframes_processed=$6,
                   allow=false,
                   reason='stage2_count_only',
                   tf_results=COALESCE($7::jsonb, signal_laboratory_entries.tf_results),
                   finished_at=$8,
                   duration_ms=$9,
                   cache_hits=$10,
                   gateway_requests=$11,
                   mw_wl_rules_total=$12,
                   mw_wl_hits=$13,
                   pack_wl_rules_total=$14,
                   pack_wl_hits=$15,
                   pack_bl_rules_total=$16,
                   pack_bl_hits=$17
             WHERE log_uid=$18 AND strategy_id=$19 AND client_strategy_id=$20 AND tf=$4
            """,
            req_id, direction, symbol, tf, tfr_req, tf, tf_result_json,
            finished_at_dt, duration_ms, kv_hits, gateway_requests,
            mw_wl_total, mw_wl_hits, pack_wl_total, pack_wl_hits, pack_bl_total, pack_bl_hits,
            log_uid, strategy_id, int(client_strategy_id)
        )
        if upd_status.startswith("UPDATE 1"):
            return
        ins_status = await conn.execute(
            """
            INSERT INTO public.signal_laboratory_entries
                (req_id, log_uid, strategy_id, client_strategy_id, direction, symbol, tf,
                 timeframes_requested, timeframes_processed, protocol_version,
                 allow, reason, tf_results, errors,
                 received_at, finished_at, duration_ms, cache_hits, gateway_requests,
                 mw_wl_rules_total, mw_wl_hits, pack_wl_rules_total, pack_wl_hits, pack_bl_rules_total, pack_bl_hits)
            VALUES ($1,$2,$3,$4,$5,$6,$7,
                    $8,$9,'v1',
                    false,'stage2_count_only',COALESCE($10::jsonb,NULL),NULL,
                    $11,$12,$13,$14,$15,
                    $16,$17,$18,$19,$20,$21)
            ON CONFLICT DO NOTHING
            """,
            req_id, log_uid, strategy_id, int(client_strategy_id), direction, symbol, tf,
            tfr_req, tf, tf_result_json,
            received_at_dt, finished_at_dt, duration_ms, kv_hits, gateway_requests,
            mw_wl_total, mw_wl_hits, pack_wl_total, pack_wl_hits, pack_bl_total, pack_bl_hits
        )
        if ins_status.endswith(" 1"):
            return
        await conn.execute(
            """
            UPDATE public.signal_laboratory_entries
               SET req_id=$1,
                   direction=$2,
                   symbol=$3,
                   tf=$4,
                   timeframes_requested=$5,
                   timeframes_processed=$6,
                   allow=false,
                   reason='stage2_count_only',
                   tf_results=COALESCE($7::jsonb, signal_laboratory_entries.tf_results),
                   finished_at=$8,
                   duration_ms=$9,
                   cache_hits=$10,
                   gateway_requests=$11,
                   mw_wl_rules_total=$12,
                   mw_wl_hits=$13,
                   pack_wl_rules_total=$14,
                   pack_wl_hits=$15,
                   pack_bl_rules_total=$16,
                   pack_bl_hits=$17
             WHERE log_uid=$18 AND strategy_id=$19 AND client_strategy_id=$20 AND tf=$4
            """,
            req_id, direction, symbol, tf, tfr_req, tf, tf_result_json,
            finished_at_dt, duration_ms, kv_hits, gateway_requests,
            mw_wl_total, mw_wl_hits, pack_wl_total, pack_wl_hits, pack_bl_total, pack_bl_hits,
            log_uid, strategy_id, int(client_strategy_id)
        )

# 🔸 Ключи шторки/очереди
def _gate_key(gate_sid: int, symbol: str) -> str:
    return f"lab:gate:{gate_sid}:{symbol}"

def _queue_key(gate_sid: int, symbol: str) -> str:
    return f"lab:qids:{gate_sid}:{symbol}"

def _qfields_key(req_id: str) -> str:
    return f"lab:qfields:{req_id}"

# 🔸 Получить лидерство или встать в очередь (anti-dup per (gate_sid,symbol))
async def _acquire_gate_or_enqueue(
    msg_id: str,
    fields: Dict[str, str],
    gate_sid: int,
    symbol: str,
    gate_ttl_sec: int = GATE_TTL_SEC,
) -> Tuple[bool, Optional[str]]:
    gk = _gate_key(gate_sid, symbol)
    qk = _queue_key(gate_sid, symbol)
    fk = _qfields_key(msg_id)

    ok = await infra.redis_client.set(gk, msg_id, ex=gate_ttl_sec, nx=True)
    if ok:
        log.debug("[GATE] 🔐 лидер получен gate_sid=%s %s req_id=%s", gate_sid, symbol, msg_id)
        return True, None

    await infra.redis_client.rpush(qk, msg_id)
    await infra.redis_client.set(fk, json.dumps(fields, ensure_ascii=False), ex=gate_ttl_sec + 60)
    log.debug("[GATE] ⏸️ в очередь gate_sid=%s %s req_id=%s", gate_sid, symbol, msg_id)
    return False, "enqueued"

# 🔸 Реакция по завершении лидера (allow=false → назначить следующего)
async def _on_leader_finished(gate_sid: int, symbol: str, leader_req_id: str, allow: bool):
    gk = _gate_key(gate_sid, symbol)
    qk = _queue_key(gate_sid, symbol)
    await infra.redis_client.delete(gk)

    if allow:
        pending = await infra.redis_client.lrange(qk, 0, -1)
        await infra.redis_client.delete(qk)
        if pending:
            log.debug("[GATE] 🚫 duplicated gate_sid=%s %s — отказываем очереди (%d)", gate_sid, symbol, len(pending))
            for req_id in pending:
                await infra.redis_client.xadd(DECISION_RESP_STREAM, {
                    "req_id": req_id, "status": "ok", "allow": "false", "reason": "duplicated_entry"
                })
                await infra.redis_client.delete(_qfields_key(req_id))
        return

    next_req_id = await infra.redis_client.lpop(qk)
    if not next_req_id:
        log.debug("[GATE] 🔁 очередь пуста gate_sid=%s %s", gate_sid, symbol)
        return

    ok = await infra.redis_client.set(gk, next_req_id, ex=GATE_TTL_SEC, nx=True)
    if not ok:
        log.debug("[GATE] ⚠️ не удалось назначить лидера gate_sid=%s %s req=%s", gate_sid, symbol, next_req_id)
        return

    raw = await infra.redis_client.get(_qfields_key(next_req_id))
    if not raw:
        await infra.redis_client.xadd(DECISION_RESP_STREAM, {
            "req_id": next_req_id, "status": "error", "error": "internal_error", "message": "queued payload missing"
        })
        return

    try:
        fields = json.loads(raw)
    except Exception:
        await infra.redis_client.xadd(DECISION_RESP_STREAM, {
            "req_id": next_req_id, "status": "error", "error": "internal_error", "message": "queued payload invalid"
        })
        return

    asyncio.create_task(_process_request_core(next_req_id, fields))

# 🔸 Ядро обработки запроса (Этап 2: сбор снимка + подсчёт совпадений; ВСЕГДА deny в ответ)
async def _process_request_core(msg_id: str, fields: Dict[str, str]):
    async with _decisions_sem:
        t0 = _now_monotonic_ms()
        received_at_dt = datetime.utcnow()

        # парсинг базовых полей
        log_uid = fields.get("log_uid") or ""
        strategy_id_s = fields.get("strategy_id") or ""
        client_sid_s = fields.get("client_strategy_id") or ""
        direction = (fields.get("direction") or "").strip().lower()
        symbol = (fields.get("symbol") or "").strip().upper()
        tfs_raw = fields.get("timeframes") or ""

        # базовая валидация
        if not log_uid or not strategy_id_s.isdigit() or direction not in ("long", "short") or not symbol or not tfs_raw:
            await infra.redis_client.xadd(DECISION_RESP_STREAM, {
                "req_id": msg_id, "status": "error", "error": "bad_request", "message": "missing or invalid fields"
            })
            log.info("[REQ] ❌ bad_request log_uid=%s sid=%s symbol=%s dir=%s tfs=%s", log_uid, strategy_id_s, symbol, direction, tfs_raw)
            return

        sid = int(strategy_id_s)
        gate_sid = int(client_sid_s) if client_sid_s.isdigit() else sid

        # активность тикера/стратегии
        if symbol not in infra.enabled_tickers:
            await infra.redis_client.xadd(DECISION_RESP_STREAM, {
                "req_id": msg_id, "status": "error", "error": "symbol_not_active", "message": f"{symbol}"
            })
            log.info("[REQ] ❌ symbol_not_active log_uid=%s sid=%s %s", log_uid, sid, symbol)
            return
        if sid not in infra.enabled_strategies:
            await infra.redis_client.xadd(DECISION_RESP_STREAM, {
                "req_id": msg_id, "status": "error", "error": "strategy_not_enabled", "message": f"{sid}"
            })
            log.info("[REQ] ❌ strategy_not_enabled log_uid=%s sid=%s", log_uid, sid)
            return

        # нормализация TF и дедлайн
        tfs = _parse_timeframes(tfs_raw)
        deadline_ms = t0 + LAB_DEADLINE_MS

        # ожидание готовности MW-кэша для стратегии (шторка)
        await infra.wait_mw_ready(sid, timeout_sec=5.0)

        # лог старта
        log.info(
            "[REQ] ▶️ start log_uid=%s master_sid=%s client_sid=%s %s %s tfs=%s deadline=90s",
            log_uid, sid, (client_sid_s or "-"), symbol, direction, ",".join(tfs)
        )

        telemetry: Dict[str, int] = {"kv_hits": 0, "gateway_requests": 0}
        tf_results_for_response: List[Dict[str, Any]] = []

        # цикл по каждой запрошенной TF — сбор снимка, подсчёт матчей и запись ОДНОЙ строки на TF
        for tf in tfs:
            # MW snapshot
            mw_snap = await _collect_mw_snapshot(
                sid=sid, symbol=symbol, direction=direction, tf=tf, deadline_ms=deadline_ms, telemetry=telemetry
            )

            # PACK snapshot
            pack_snap = await _collect_pack_snapshot(
                sid=sid, symbol=symbol, direction=direction, tf=tf, deadline_ms=deadline_ms, telemetry=telemetry
            )

            # флаг неполного сбора по TF
            incomplete = False
            for kind in ("trend", "volatility", "momentum", "extremes"):
                node = mw_snap["states"].get(kind)
                if not node or (isinstance(node, dict) and node.get("source") == "timeout"):
                    incomplete = True
                    break
            if not incomplete:
                for base, node in (pack_snap.get("objects") or {}).items():
                    if not node or (isinstance(node, dict) and node.get("source") == "timeout"):
                        incomplete = True
                        break

            # Подсчёт совпадений MW
            mw_hits, mw_total = _mw_count_hits(mw_snap.get("rules", []), mw_snap.get("states", {}))

            # Подсчёт совпадений PACK (WL/BL)
            pack_wl_hits, pack_wl_total, pack_bl_hits, pack_bl_total = _pack_count_hits(
                pack_snap.get("rules", []), pack_snap.get("objects", {})
            )

            # соберём одиночный TF-результат (для БД и ответа)
            tf_result_obj = {
                "tf": tf,
                "collect_incomplete": incomplete,
                "mw": mw_snap,
                "pack": pack_snap,
                "counters": {
                    "mw_wl_hits": mw_hits, "mw_wl_total": mw_total,
                    "pack_wl_hits": pack_wl_hits, "pack_wl_total": pack_wl_total,
                    "pack_bl_hits": pack_bl_hits, "pack_bl_total": pack_bl_total,
                },
            }
            tf_results_for_response.append(tf_result_obj)

            # лог по TF (log.info)
            log.info(
                "[TF:%s] match mw_wl: %d/%d  pack_wl: %d/%d  pack_bl: %d/%d  incomplete=%s  kv_hits=%d gw_reqs=%d",
                tf, mw_hits, mw_total, pack_wl_hits, pack_wl_total, pack_bl_hits, pack_bl_total,
                str(incomplete).lower(), telemetry.get("kv_hits", 0), telemetry.get("gateway_requests", 0)
            )

            # запись ОДНОЙ строки на TF
            finished_at_dt = datetime.utcnow()
            duration_ms = _now_monotonic_ms() - t0
            try:
                tf_json_safe = _to_json_safe(tf_result_obj)
                tf_json_text = json.dumps(tf_json_safe, ensure_ascii=False)
                await _persist_decision_tf(
                    req_id=msg_id,
                    log_uid=log_uid,
                    strategy_id=sid,
                    client_strategy_id=int(client_sid_s) if client_sid_s.isdigit() else None,
                    symbol=symbol,
                    direction=direction,
                    tf=tf,
                    tfr_req=tfs_raw,
                    tf_result_json=tf_json_text,
                    received_at_dt=received_at_dt,
                    finished_at_dt=finished_at_dt,
                    duration_ms=duration_ms,
                    kv_hits=telemetry.get("kv_hits", 0),
                    gateway_requests=telemetry.get("gateway_requests", 0),
                    mw_wl_hits=mw_hits,
                    mw_wl_total=mw_total,
                    pack_wl_hits=pack_wl_hits,
                    pack_wl_total=pack_wl_total,
                    pack_bl_hits=pack_bl_hits,
                    pack_bl_total=pack_bl_total,
                )
            except Exception:
                log.exception("[AUDIT] ❌ ошибка записи строки TF=%s log_uid=%s sid=%s csid=%s", tf, log_uid, sid, client_sid_s or "-")

        # формируем ОТВЕТ стратегии — всегда deny на Этапе 2 (единый ответ на весь запрос)
        finished_at_dt = datetime.utcnow()
        duration_ms_total = _now_monotonic_ms() - t0

        resp = {
            "req_id": msg_id,
            "status": "ok",
            "allow": "false",
            "reason": "stage2_count_only",
            "log_uid": log_uid,
            "strategy_id": str(sid),
            "direction": direction,
            "symbol": symbol,
            "timeframes": ",".join(tfs),
        }
        if client_sid_s:
            resp["client_strategy_id"] = client_sid_s

        # положим сводный массив TF в ответ (для трассировки)
        try:
            resp["tf_results"] = json.dumps(_to_json_safe(tf_results_for_response), ensure_ascii=False)
        except Exception:
            pass

        await infra.redis_client.xadd(DECISION_RESP_STREAM, resp)

        # финальный лог (log.info)
        log.info(
            "[RESP] ⛔ deny log_uid=%s sid=%s csid=%s reason=stage2_count_only dur=%dms kv_hits=%d gw_reqs=%d",
            log_uid, sid, (client_sid_s or "-"), duration_ms_total, telemetry.get("kv_hits", 0), telemetry.get("gateway_requests", 0)
        )

        # завершение ворот: allow=false → подобрать следующего из очереди
        await _on_leader_finished(gate_sid=gate_sid, symbol=symbol, leader_req_id=msg_id, allow=False)

# 🔸 Обработка входящего сообщения: получить лидерство или встать в очередь
async def _handle_incoming(msg_id: str, fields: Dict[str, str]):
    strategy_id_s = fields.get("strategy_id") or ""
    client_sid_s = fields.get("client_strategy_id") or ""
    symbol = (fields.get("symbol") or "").strip().upper()
    if not strategy_id_s.isdigit() or not symbol:
        await infra.redis_client.xadd(DECISION_RESP_STREAM, {
            "req_id": msg_id, "status": "error", "error": "bad_request", "message": "missing sid/symbol"
        })
        log.info("[REQ] ❌ bad_request (no sid/symbol) fields=%s", fields)
        return

    sid = int(strategy_id_s)
    gate_sid = int(client_sid_s) if client_sid_s.isdigit() else sid

    is_leader, _ = await _acquire_gate_or_enqueue(msg_id, fields, gate_sid, symbol, gate_ttl_sec=GATE_TTL_SEC)
    if is_leader:
        await _process_request_core(msg_id, fields)
    else:
        log.debug("[REQ] ⏳ queued gate_sid=%s %s req_id=%s", gate_sid, symbol, msg_id)

# 🔸 Главный слушатель decision_request (Этап 2)
async def run_laboratory_decision_maker():
    """
    Слушает laboratory:decision_request, собирает снимок MW/PACK, считает совпадения (MW-WL / PACK-WL / PACK-BL)
    и пишет ПО ОДНОЙ строке на каждый запрошенный TF в signal_laboratory_entries. В ответ стратегии возвращает
    status=ok, allow=false, reason=stage2_count_only.
    Работает только с НОВЫМИ сообщениями (старт с '$'). Встроены ворота/очередь per (gate_sid, symbol).
    """
    log.info("🛰️ LAB_DECISION слушатель запущен (BLOCK=%d COUNT=%d DEADLINE=%ds)",
             XREAD_BLOCK_MS, XREAD_COUNT, LAB_DEADLINE_MS // 1000)

    last_id = "$"  # только новые
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
            log.info("⏹️ LAB_DECISION остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ LAB_DECISION ошибка в основном цикле")
            await asyncio.sleep(1.0)