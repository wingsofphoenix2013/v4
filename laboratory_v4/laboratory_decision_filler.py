# laboratory_decision_filler.py — post-allow «писатель» статистики: читает seed-ивенты, собирает ВСЕ совпадения MW/PACK по TF и пишет в laboratoty_position_stat

import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Инфраструктура
import laboratory_infra as infra

# 🔸 Логгер
log = logging.getLogger("LAB_DECISION_FILLER")

# 🔸 Потоки и шлюз
DECISION_FILLER_STREAM = "laboratory_decision_filler"       # источник seed-событий от decision_maker
GATEWAY_REQ_STREAM      = "indicator_gateway_request"       # indicator_gateway входящий стрим

# 🔸 Производительность
XREAD_BLOCK_MS = 2000
XREAD_COUNT    = 50
MAX_IN_FLIGHT  = 16
MAX_CONCURRENT_GATEWAY_CALLS = 32
COALESCE_TTL_SEC = 3

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
_filler_sem  = asyncio.Semaphore(MAX_IN_FLIGHT)
_gateway_sem = asyncio.Semaphore(MAX_CONCURRENT_GATEWAY_CALLS)

# 🔸 Коалесценс (in-process): key -> (expire_ms, future)
_coalesce: Dict[str, Tuple[float, asyncio.Future]] = {}


# 🔸 Утилиты времени/парсинга
def _now_monotonic_ms() -> int:
    return int(time.monotonic() * 1000)


def _parse_timeframes(tf_str: str) -> List[str]:
    items = [x.strip().lower() for x in (tf_str or "").split(",") if x.strip()]
    seen, ordered = set(), []
    for tf in TF_ORDER:
        if tf in items and tf not in seen:
            seen.add(tf)
            ordered.append(tf)
    return ordered


def _parse_pack_base(base: str) -> Tuple[str, Dict[str, Any]]:
    """Возвращает (indicator, params) по pack_base (rsi14, ema21, bb20_2_0, macd12, adx_dmi14, lr50, atr14)."""
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


async def _mget_json(keys: List[str]) -> Dict[str, Optional[dict]]:
    if not keys:
        return {}
    values = await infra.redis_client.mget(*keys)
    return {k: _json_or_none(v) for k, v in zip(keys, values)}


# 🔸 Гарантированно получить pack-объект (cache-first + gateway)
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
        try:
            return await asyncio.wait_for(fut, timeout=max(0.2, (deadline_ms - _now_monotonic_ms()) / 1000))
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

            await infra.redis_client.xadd(GATEWAY_REQ_STREAM, req)
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
                if poll_sleep < 0.25:
                    poll_sleep = 0.25

            if not fut.done():
                fut.set_result(None)
            return None

        except Exception:
            log.exception("[FILLER] ❌ Ошибка запроса в gateway ind=%s base=%s", indicator, base)
            if not fut.done():
                fut.set_result(None)
            return None
        finally:
            now2 = _now_monotonic_ms()
            for ck, (exp, f) in list(_coalesce.items()):
                if now2 > exp or (f.done() and _json_or_none(f.result()) is None):
                    _coalesce.pop(ck, None)


# 🔸 MW: снять состояния + построить ВСЕ совпадения
async def _collect_mw(
    sid: int,
    symbol: str,
    tf: str,
    direction: str,
    precision: int,
    deadline_ms: int,
) -> Tuple[Dict[str, Optional[str]], List[Dict[str, Any]]]:
    """Возвращает (mw_states, mw_matches[])."""
    mw_rows_all = (infra.mw_wl_by_strategy.get(sid) or {}).get("rows", [])
    mw_rows = [r for r in mw_rows_all if (r.get("timeframe") == tf and r.get("direction") == direction)]

    # какие базы нужны
    needed_bases: List[str] = []
    for r in mw_rows:
        base = (r.get("agg_base") or "").strip().lower()
        if not base:
            continue
        for b in base.split("_"):
            if b in ("trend", "volatility", "extremes", "momentum") and b not in needed_bases:
                needed_bases.append(b)

    # states: cache-first → gateway
    states: Dict[str, Optional[str]] = {}
    keys = [_public_mw_key(b, symbol, tf) for b in needed_bases]
    kv = await _mget_json(keys)
    for base in needed_bases:
        st = None
        obj = kv.get(_public_mw_key(base, symbol, tf))
        if obj and isinstance(obj, dict):
            st = (obj.get("pack") or {}).get("state")
        if not st:
            obj = await _ensure_pack_available(symbol, tf, base, base, {}, precision, deadline_ms)
            if obj:
                st = (obj.get("pack") or {}).get("state")
        states[base] = st

    # построим ВСЕ совпадения
    matches: List[Dict[str, Any]] = []
    for r in mw_rows:
        agg_base = (r.get("agg_base") or "").strip().lower()
        agg_state = (r.get("agg_state") or "").strip().lower()
        if not agg_base or not agg_state:
            continue
        bases = agg_base.split("_")

        # построить факт
        if len(bases) == 1:
            base = bases[0]
            cur = states.get(base)
            if not cur:
                continue
            fact = cur.strip().lower()
        else:
            parts, ok = [], True
            for b in bases:
                cur = states.get(b)
                if not cur:
                    ok = False
                    break
                parts.append(f"{b}:{cur.strip().lower()}")
            if not ok:
                continue
            fact = "|".join(parts)

        if fact == agg_state:
            matches.append({
                "id": int(r.get("id")) if r.get("id") is not None else None,
                "agg_base": agg_base,
                "agg_state": agg_state,
                "confirmation": int(r.get("confirmation")) if r.get("confirmation") is not None else None,
                "winrate": float(r.get("winrate")) if r.get("winrate") is not None else None,
                "confidence": float(r.get("confidence")) if r.get("confidence") is not None else None,
            })

    return states, matches


# 🔸 PACK: собрать ВСЕ WL/BL совпадения (по WL/BL таблицам)
async def _collect_pack(
    sid: int,
    symbol: str,
    tf: str,
    direction: str,
    precision: int,
    deadline_ms: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int]]:
    """Возвращает (wl_matches[], bl_matches[], wl_family_counts)."""
    pack_rows_all = (infra.pack_wl_by_strategy.get(sid) or {}).get("rows", [])
    rows_tf = [r for r in pack_rows_all if (r.get("timeframe") == tf and r.get("direction") == direction)]

    # Список уникальных pack_base
    bases: List[str] = []
    for r in rows_tf:
        base = (r.get("pack_base") or "").strip().lower()
        if base and base not in bases:
            bases.append(base)

    # Получаем объекты PACK (cache-first → gateway) параллельно
    pack_objs: Dict[str, Optional[dict]] = {}
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
            pack_objs[base] = obj
        else:
            wanted.append((base, ind, params))
            tasks.append(asyncio.create_task(_ensure_pack_available(
                symbol=symbol, tf=tf, indicator=ind, base=base,
                gw_params=params, precision=precision, deadline_ms=deadline_ms
            )))
    if tasks:
        fetched = await asyncio.gather(*tasks, return_exceptions=False)
        for (base, _ind, _params), obj in zip(wanted, fetched):
            pack_objs[base] = obj

    # Строим ВСЕ совпадения WL/BL
    wl_matches: List[Dict[str, Any]] = []
    bl_matches: List[Dict[str, Any]] = []
    wl_family_counts: Dict[str, int] = {"ema":0,"lr":0,"rsi":0,"mfi":0,"bb":0,"atr":0,"adx_dmi":0,"macd":0}

    for r in rows_tf:
        base = (r.get("pack_base") or "").strip().lower()
        if not base:
            continue
        po = pack_objs.get(base)
        if not po:
            continue
        pack = (po.get("pack") or {})
        list_type = (r.get("list") or "").strip().lower()
        agg_key = (r.get("agg_key") or "").strip().lower()
        agg_val = (r.get("agg_value") or "").strip().lower()
        if not agg_key or not agg_val:
            continue

        # строим факт в порядке ключей
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
            ind, _ = _parse_pack_base(base)
            det = {
                "id": int(r.get("id")) if r.get("id") is not None else None,
                "pack_base": base,
                "agg_key": agg_key,
                "agg_value": agg_val,
                "winrate": float(r.get("winrate")) if r.get("winrate") is not None else None,
            }
            if list_type == "whitelist":
                wl_matches.append(det)
                # считаем по семействам WL
                if ind in wl_family_counts:
                    wl_family_counts[ind] += 1
            elif list_type == "blacklist":
                bl_matches.append(det)

    return wl_matches, bl_matches, wl_family_counts


# 🔸 Запись/апсерта в laboratoty_position_stat (двухфазный upsert)
async def _upsert_lps(
    log_uid: str,
    sid: int,
    client_sid: Optional[int],
    symbol: str,
    direction: str,
    tf: str,
    mw_states: Dict[str, Optional[str]],
    mw_matches: List[Dict[str, Any]],
    pack_wl_matches: List[Dict[str, Any]],
    pack_bl_matches: List[Dict[str, Any]],
    wl_family_counts: Dict[str, int],
):
    mw_match_count      = len(mw_matches)
    pack_wl_match_count = len(pack_wl_matches)
    pack_bl_match_count = len(pack_bl_matches)

    # json dumps
    states_json = json.dumps(mw_states, ensure_ascii=False) if mw_states else None
    mw_json     = json.dumps(mw_matches, ensure_ascii=False) if mw_matches else None
    wl_json     = json.dumps(pack_wl_matches, ensure_ascii=False) if pack_wl_matches else None
    bl_json     = json.dumps(pack_bl_matches, ensure_ascii=False) if pack_bl_matches else None
    fam_json    = json.dumps(wl_family_counts, ensure_ascii=False) if wl_family_counts else None

    async with infra.pg_pool.acquire() as conn:
        if client_sid is None:
            # UPDATE
            upd = await conn.execute(
                """
                UPDATE public.laboratoty_position_stat
                   SET mw_states = COALESCE($1::jsonb, mw_states),
                       mw_matches = COALESCE($2::jsonb, mw_matches),
                       pack_wl_matches = COALESCE($3::jsonb, pack_wl_matches),
                       pack_bl_matches = COALESCE($4::jsonb, pack_bl_matches),
                       mw_match_count = $5,
                       pack_wl_match_count = $6,
                       pack_bl_match_count = $7,
                       pack_family_counts = COALESCE($8::jsonb, pack_family_counts),
                       updated_at = NOW()
                 WHERE log_uid=$9 AND strategy_id=$10 AND client_strategy_id IS NULL AND tf=$11
                """,
                states_json, mw_json, wl_json, bl_json,
                mw_match_count, pack_wl_match_count, pack_bl_match_count,
                fam_json, log_uid, sid, tf
            )
            if upd.startswith("UPDATE 1"):
                log.debug("[FILLER] ✏️ LPS UPDATE log_uid=%s sid=%s tf=%s (master)", log_uid, sid, tf)
                return

            # INSERT DO NOTHING
            ins = await conn.execute(
                """
                INSERT INTO public.laboratoty_position_stat
                    (log_uid, strategy_id, client_strategy_id, symbol, direction, tf,
                     mw_states, mw_matches, pack_wl_matches, pack_bl_matches,
                     mw_match_count, pack_wl_match_count, pack_bl_match_count, pack_family_counts,
                     created_at, updated_at)
                VALUES ($1,$2,NULL,$3,$4,$5,
                        COALESCE($6::jsonb, NULL), COALESCE($7::jsonb, NULL),
                        COALESCE($8::jsonb, NULL), COALESCE($9::jsonb, NULL),
                        $10,$11,$12,COALESCE($13::jsonb, NULL),
                        NOW(), NOW())
                ON CONFLICT DO NOTHING
                """,
                log_uid, sid, symbol, direction, tf,
                states_json, mw_json, wl_json, bl_json,
                mw_match_count, pack_wl_match_count, pack_bl_match_count, fam_json
            )
            if ins.endswith(" 1"):
                log.debug("[FILLER] ✍️  LPS INSERT log_uid=%s sid=%s tf=%s (master)", log_uid, sid, tf)
                return

            # race → UPDATE
            await conn.execute(
                """
                UPDATE public.laboratoty_position_stat
                   SET mw_states = COALESCE($1::jsonb, mw_states),
                       mw_matches = COALESCE($2::jsonb, mw_matches),
                       pack_wl_matches = COALESCE($3::jsonb, pack_wl_matches),
                       pack_bl_matches = COALESCE($4::jsonb, pack_bl_matches),
                       mw_match_count = $5,
                       pack_wl_match_count = $6,
                       pack_bl_match_count = $7,
                       pack_family_counts = COALESCE($8::jsonb, pack_family_counts),
                       updated_at = NOW()
                 WHERE log_uid=$9 AND strategy_id=$10 AND client_strategy_id IS NULL AND tf=$11
                """,
                states_json, mw_json, wl_json, bl_json,
                mw_match_count, pack_wl_match_count, pack_bl_match_count,
                fam_json, log_uid, sid, tf
            )
            log.debug("[FILLER] ✏️ LPS UPDATE (race) log_uid=%s sid=%s tf=%s (master)", log_uid, sid, tf)

        else:
            # UPDATE
            upd = await conn.execute(
                """
                UPDATE public.laboratoty_position_stat
                   SET mw_states = COALESCE($1::jsonb, mw_states),
                       mw_matches = COALESCE($2::jsonb, mw_matches),
                       pack_wl_matches = COALESCE($3::jsonb, pack_wl_matches),
                       pack_bl_matches = COALESCE($4::jsonb, pack_bl_matches),
                       mw_match_count = $5,
                       pack_wl_match_count = $6,
                       pack_bl_match_count = $7,
                       pack_family_counts = COALESCE($8::jsonb, pack_family_counts),
                       updated_at = NOW()
                 WHERE log_uid=$9 AND strategy_id=$10 AND client_strategy_id=$11 AND tf=$12
                """,
                states_json, mw_json, wl_json, bl_json,
                mw_match_count, pack_wl_match_count, pack_bl_match_count,
                fam_json, log_uid, sid, int(client_sid), tf
            )
            if upd.startswith("UPDATE 1"):
                log.debug("[FILLER] ✏️ LPS UPDATE log_uid=%s sid=%s csid=%s tf=%s", log_uid, sid, client_sid, tf)
                return

            # INSERT DO NOTHING
            ins = await conn.execute(
                """
                INSERT INTO public.laboratoty_position_stat
                    (log_uid, strategy_id, client_strategy_id, symbol, direction, tf,
                     mw_states, mw_matches, pack_wl_matches, pack_bl_matches,
                     mw_match_count, pack_wl_match_count, pack_bl_match_count, pack_family_counts,
                     created_at, updated_at)
                VALUES ($1,$2,$3,$4,$5,$6,
                        COALESCE($7::jsonb, NULL), COALESCE($8::jsonb, NULL),
                        COALESCE($9::jsonb, NULL), COALESCE($10::jsonb, NULL),
                        $11,$12,$13,COALESCE($14::jsonb, NULL),
                        NOW(), NOW())
                ON CONFLICT DO NOTHING
                """,
                log_uid, sid, int(client_sid), symbol, direction, tf,
                states_json, mw_json, wl_json, bl_json,
                mw_match_count, pack_wl_match_count, pack_bl_match_count, fam_json
            )
            if ins.endswith(" 1"):
                log.debug("[FILLER] ✍️  LPS INSERT log_uid=%s sid=%s csid=%s tf=%s", log_uid, sid, client_sid, tf)
                return

            # race → UPDATE
            await conn.execute(
                """
                UPDATE public.laboratoty_position_stat
                   SET mw_states = COALESCE($1::jsonb, mw_states),
                       mw_matches = COALESCE($2::jsonb, mw_matches),
                       pack_wl_matches = COALESCE($3::jsonb, pack_wl_matches),
                       pack_bl_matches = COALESCE($4::jsonb, pack_bl_matches),
                       mw_match_count = $5,
                       pack_wl_match_count = $6,
                       pack_bl_match_count = $7,
                       pack_family_counts = COALESCE($8::jsonb, pack_family_counts),
                       updated_at = NOW()
                 WHERE log_uid=$9 AND strategy_id=$10 AND client_strategy_id=$11 AND tf=$12
                """,
                states_json, mw_json, wl_json, bl_json,
                mw_match_count, pack_wl_match_count, pack_bl_match_count,
                fam_json, log_uid, sid, int(client_sid), tf
            )
            log.debug("[FILLER] ✏️ LPS UPDATE (race) log_uid=%s sid=%s csid=%s tf=%s", log_uid, sid, client_sid, tf)


# 🔸 Обработка одного seed-сообщения (полный цикл по TF)
async def _process_seed(msg_id: str, fields: Dict[str, str]):
    async with _filler_sem:
        t0 = _now_monotonic_ms()

        log_uid   = fields.get("log_uid") or ""
        sid_s     = fields.get("strategy_id") or ""
        client_s  = fields.get("client_strategy_id") or ""
        symbol    = (fields.get("symbol") or "").strip().upper()
        direction = (fields.get("direction") or "").strip().lower()
        tfs_raw   = fields.get("timeframes") or ""

        if not log_uid or not sid_s.isdigit() or not symbol or direction not in ("long","short") or not tfs_raw:
            log.debug("[FILLER] ❌ bad seed msg=%s fields=%s", msg_id, fields)
            return

        sid = int(sid_s)
        client_sid = int(client_s) if client_s.isdigit() else None
        tfs = _parse_timeframes(tfs_raw)
        if not tfs:
            log.debug("[FILLER] ❌ empty TF seed log_uid=%s", log_uid)
            return

        precision = int(infra.enabled_tickers.get(symbol, {}).get("precision_price", 7))
        deadline_ms = _now_monotonic_ms() + 60_000  # верхний потолок на обогащение одной заявки

        for tf in tfs:
            try:
                # MW
                mw_states, mw_matches = await _collect_mw(sid, symbol, tf, direction, precision, deadline_ms)

                # PACK
                pack_wl_matches, pack_bl_matches, wl_family_counts = await _collect_pack(
                    sid, symbol, tf, direction, precision, deadline_ms
                )

                # Запись
                await _upsert_lps(
                    log_uid=log_uid,
                    sid=sid,
                    client_sid=client_sid,
                    symbol=symbol,
                    direction=direction,
                    tf=tf,
                    mw_states=mw_states,
                    mw_matches=mw_matches,
                    pack_wl_matches=pack_wl_matches,
                    pack_bl_matches=pack_bl_matches,
                    wl_family_counts=wl_family_counts,
                )
                log.info("[FILLER] ✅ TF записан log_uid=%s sid=%s csid=%s %s %s", log_uid, sid, client_sid, symbol, tf)

            except Exception:
                log.exception("[FILLER] ❌ Ошибка TF log_uid=%s sid=%s tf=%s", log_uid, sid, tf)

        dur = _now_monotonic_ms() - t0
        log.debug("[FILLER] 📦 seed done log_uid=%s sid=%s csid=%s tfs=%s dur=%dms",
                 log_uid, sid, client_sid, ",".join(tfs), dur)


# 🔸 Главный слушатель filler-стрима
async def run_laboratory_decision_filler():
    """
    Слушает laboratory_decision_filler и формирует строки в laboratoty_position_stat:
      — по каждому TF собирает ВСЕ совпадения MW/PACK,
      — считает короткие счётчики,
      — делает upsert (без позиционных полей).
    Обрабатывает только НОВЫЕ сообщения (старт с '$').
    """
    log.debug("🛰️ LAB_DECISION_FILLER слушатель запущен (BLOCK=%d COUNT=%d MAX=%d)",
             XREAD_BLOCK_MS, XREAD_COUNT, MAX_IN_FLIGHT)

    last_id = "$"
    redis = infra.redis_client

    while True:
        try:
            resp = await redis.xread(streams={DECISION_FILLER_STREAM: last_id},
                                     count=XREAD_COUNT, block=XREAD_BLOCK_MS)
            if not resp:
                continue

            for _, messages in resp:
                for msg_id, fields in messages:
                    last_id = msg_id
                    asyncio.create_task(_process_seed(msg_id, fields))

        except asyncio.CancelledError:
            log.debug("⏹️ LAB_DECISION_FILLER остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ LAB_DECISION_FILLER ошибка в основном цикле")
            await asyncio.sleep(1.0)