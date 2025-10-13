# 🔸 laboratory_decision_maker.py — воркер «советчика»: параллельная обработка запросов (до 16), внутри запроса — последовательная проверка TF (младший→старший) + сохранение winrate в матчах

# 🔸 Импорты
import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Tuple, Optional

import laboratory_infra as infra

# 🔸 Логгер
log = logging.getLogger("LAB_DECISION")

# 🔸 Константы стримов/групп
REQUEST_STREAM = "laboratory:decision_request"
RESPONSE_STREAM = "laboratory:decision_response"
CONSUMER_GROUP = "LAB_DECISION_GROUP"
CONSUMER_NAME = "LAB_DECISION_WORKER"

# 🔸 Идемпотентность ответа (ключи в Redis)
RESP_SENT_KEY_TMPL = "lab:decision:sent:{req_uid}"
RESP_SENT_TTL_SEC = 24 * 60 * 60  # 24h

# 🔸 Ограничитель (анти-дубликат по тикеру/направлению/клиент-стратегии)
GATE_KEY_TMPL = "lab:gate:busy:{client_sid}:{symbol}:{direction}"
DUP_GUARD_TTL_SEC = 20  # TTL ворот, сек (пока нет ответа по первому запросу)

# 🔸 Параллелизм/чтение
MAX_CONCURRENCY = 16     # одновременно обрабатываем до 16 запросов
READ_COUNT = 64
READ_BLOCK_MS = 30_000

# 🔸 Доп. константы
ALLOWED_TFS = ("m5", "m15", "h1")
ALLOWED_DECISION_MODES = ("mw_only", "mw_then_pack", "mw_and_pack", "pack_only")
ALLOWED_DIRECTIONS = ("long", "short")
ALLOWED_VERSIONS = ("v1", "v2")
MW_BASES = ("trend", "volatility", "momentum", "extremes")


# 🔸 Публичная точка входа воркера
async def run_laboratory_decision_maker():
    # условия достаточности
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск LAB_DECISION: PG/Redis не инициализированы")
        return

    # создать consumer group (идемпотентно)
    try:
        await infra.redis_client.xgroup_create(
            name=REQUEST_STREAM,
            groupname=CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug("📡 LAB_DECISION: создана consumer group для %s", REQUEST_STREAM)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            log.exception("❌ LAB_DECISION: ошибка создания consumer group")
            return

    log.debug("🚀 LAB_DECISION: старт воркера (parallel=%d)", MAX_CONCURRENCY)

    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    # основной цикл
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=CONSUMER_NAME,
                streams={REQUEST_STREAM: ">"},
                count=READ_COUNT,
                block=READ_BLOCK_MS,
            )
            if not resp:
                continue

            tasks = []
            for _, msgs in resp:
                for msg_id, fields in msgs:
                    tasks.append(_process_message_guard(sem, msg_id, fields))

            if tasks:
                await asyncio.gather(*tasks)

        except asyncio.CancelledError:
            log.debug("⏹️ LAB_DECISION: остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ LAB_DECISION: ошибка цикла — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Гард на обработку одного сообщения (параллельно, ACK в finally)
async def _process_message_guard(sem: asyncio.Semaphore, msg_id: str, fields: Dict[str, str]):
    async with sem:
        try:
            # парсинг payload (одно поле data с JSON)
            raw = fields.get("data", "{}")
            payload = json.loads(raw) if isinstance(raw, str) else (raw or {})
        except Exception:
            log.exception("❌ LAB_DECISION: не удалось распарсить payload")
            await _ack_safe(msg_id)
            return

        try:
            await _handle_request(payload)
        except Exception:
            log.exception("❌ LAB_DECISION: ошибка обработки запроса")
        finally:
            await _ack_safe(msg_id)


# 🔸 ACK безопасно
async def _ack_safe(msg_id: str):
    try:
        await infra.redis_client.xack(REQUEST_STREAM, CONSUMER_GROUP, msg_id)
    except Exception:
        log.exception("⚠️ LAB_DECISION: ошибка ACK (id=%s)", msg_id)


# 🔸 Утилиты парсинга
def _parse_bool(s: Optional[str]) -> bool:
    if s is None:
        return False
    return str(s).strip().lower() in ("1", "true", "t", "yes", "y")


def _normalize_symbol(s: str) -> str:
    return (s or "").upper().strip()


def _now_utc_naive() -> datetime:
    return datetime.utcnow().replace(tzinfo=None)


def _parse_timeframes(tfs: str) -> List[str]:
    # разбиваем, чистим, фильтруем по допустимым, сохраняем порядок и уникальность
    seen = set()
    out: List[str] = []
    for tf in (tfs or "").split(","):
        tf = tf.strip().lower()
        if tf in ALLOWED_TFS and tf not in seen:
            out.append(tf)
            seen.add(tf)
    return out


def _indicator_from_pack_base(pack_base: str) -> Optional[str]:
    s = (pack_base or "").strip().lower()
    if s.startswith("bb"):
        return "bb"
    if s.startswith("adx_dmi"):
        return "adx_dmi"
    for pref in ("rsi", "mfi", "ema", "atr", "lr", "macd"):
        if s.startswith(pref):
            return pref
    return None


# 🔸 Чтение live-данных
async def _get_live_mw_states(symbol: str, tf: str) -> Dict[str, str]:
    # считывает текущие состояния MW-баз из ind_mw_live:{symbol}:{tf}:{kind} (JSON)
    states: Dict[str, str] = {}
    for base in MW_BASES:
        key = f"ind_mw_live:{symbol}:{tf}:{base}"
        try:
            js = await infra.redis_client.get(key)
            if not js:
                continue
            obj = json.loads(js)
        except Exception:
            continue

        state = None
        if isinstance(obj, dict):
            state = obj.get("state") or obj.get("mw_state")
            if state is None and isinstance(obj.get("pack"), dict):
                state = obj["pack"].get("state") or obj["pack"].get("mw_state")
            if state is None:
                for k in ("label", "current", "value"):
                    v = obj.get(k) or (obj.get("pack", {}) if isinstance(obj.get("pack"), dict) else {}).get(k)
                    if isinstance(v, str) and v:
                        state = v
                        break
        if isinstance(state, str) and state:
            states[base] = state
    return states


async def _get_live_pack(symbol: str, tf: str, indicator: str, pack_base: str) -> Optional[dict]:
    # читает JSON live-пака из pack_live:{indicator}:{symbol}:{tf}:{base}
    key = f"pack_live:{indicator}:{symbol}:{tf}:{pack_base}"
    try:
        js = await infra.redis_client.get(key)
        if not js:
            return None
        obj = json.loads(js)
        if isinstance(obj, dict):
            return obj
    except Exception:
        return None
    return None


def _get_field_from_pack(obj: dict, field: str) -> Optional[str]:
    # достаёт строковое значение поля из пака (прямо или из obj['pack'] или obj['features'])
    if not isinstance(obj, dict):
        return None
    val = obj.get(field)
    if val is None and isinstance(obj.get("pack"), dict):
        val = obj["pack"].get(field)
    if val is None and isinstance(obj.get("features"), dict):
        val = obj["features"].get(field)
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return str(val)
    return str(val)


# 🔸 Сбор канонических строк для сравнения
def _build_mw_agg_state(agg_base: str, mw_states: Dict[str, str]) -> Optional[str]:
    # формирует каноническую строку agg_state для MW (или возвращает solo-state)
    bases = agg_base.split("_")
    if len(bases) == 1:
        b = bases[0]
        state = mw_states.get(b)
        return state if isinstance(state, str) and state else None
    parts: List[str] = []
    for b in bases:
        state = mw_states.get(b)
        if not (isinstance(state, str) and state):
            return None
        parts.append(f"{b}:{state}")
    return "|".join(parts)


def _build_pack_agg_value(agg_key: str, pack_obj: dict) -> Optional[str]:
    # формирует каноническую строку agg_value из пака по agg_key (field1|field2|...)
    fields = [p.strip() for p in agg_key.split("|") if p.strip()]
    if not fields:
        return None
    parts: List[str] = []
    for f in fields:
        val = _get_field_from_pack(pack_obj, f)
        if val is None:
            return None
        parts.append(f"{f}:{val}")
    return "|".join(parts)


# 🔸 Redis-ворота (ограничитель по тикеру/направлению/клиент-стратегии)
async def _acquire_gate(req_uid: str, client_sid: Optional[int], symbol: str, direction: str) -> Tuple[bool, Optional[str]]:
    # пытается поставить ворота. Возвращает (acquired, gate_key). Если ворота уже стоят → False.
    if client_sid is None:
        return True, None  # нет client_sid — не ограничиваем
    key = GATE_KEY_TMPL.format(client_sid=int(client_sid), symbol=symbol, direction=direction)
    try:
        ok = await infra.redis_client.set(key, req_uid, ex=DUP_GUARD_TTL_SEC, nx=True)
        if ok:
            return True, key
        return False, key
    except Exception:
        log.exception("⚠️ LAB_DECISION: acquire_gate error (key=%s)", key)
        return True, None


# скрипт безопасного релиза «только если значение совпадает»
_RELEASE_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
else
  return 0
end
"""

async def _release_gate(req_uid: str, gate_key: Optional[str]):
    if not gate_key:
        return
    try:
        # redis-py: eval(script, numkeys, *keys_and_args)
        await infra.redis_client.eval(_RELEASE_LUA, 1, gate_key, req_uid)
    except Exception:
        log.exception("⚠️ LAB_DECISION: release_gate error (key=%s)", gate_key)


# 🔸 Логика обработки одного запроса (внутри запроса — последовательный проход по TF)
async def _handle_request(payload: dict):
    # время начала
    t_recv = _now_utc_naive()
    t0 = time.monotonic()

    # извлекаем поля
    req_uid = str(payload.get("req_uid") or "").strip()
    log_uid = str(payload.get("log_uid") or "").strip()
    strategy_id = int(payload.get("strategy_id") or 0)         # мастер
    client_strategy_id = payload.get("client_strategy_id")     # не-мастер
    client_sid = int(client_strategy_id) if client_strategy_id not in (None, "") else None
    symbol = _normalize_symbol(payload.get("symbol") or "")
    direction = str(payload.get("direction") or "").lower()
    version = str(payload.get("version") or "").lower()
    decision_mode = str(payload.get("decision_mode") or "").lower()
    use_bl = _parse_bool(payload.get("use_bl"))
    timeframes_raw = str(payload.get("timeframes") or "")
    tfs = _parse_timeframes(timeframes_raw)

    # валидация минимальная
    bad_reasons = []
    if not req_uid:
        bad_reasons.append("bad_req_uid")
    if strategy_id <= 0:
        bad_reasons.append("bad_strategy_id")
    if symbol == "":
        bad_reasons.append("bad_symbol")
    if direction not in ALLOWED_DIRECTIONS:
        bad_reasons.append("bad_direction")
    if version not in ALLOWED_VERSIONS:
        bad_reasons.append("bad_version")
    if decision_mode not in ALLOWED_DECISION_MODES:
        bad_reasons.append("bad_decision_mode")
    if not tfs:
        bad_reasons.append("bad_timeframes")

    if bad_reasons:
        await _respond_once(req_uid, allow=False, reason="bad_request")
        await _write_request_head_only(
            req_id=req_uid,
            log_uid=log_uid,
            strategy_id=strategy_id,
            client_strategy_id=client_sid,
            direction=direction,
            symbol=symbol,
            tfs_requested=timeframes_raw,
            decision_mode=decision_mode or "",
            oracle_version=version or "",
            use_bl=use_bl,
            allow=False,
            reason="bad_request",
            t_recv=t_recv,
            t_fin=_now_utc_naive(),
            duration_ms=int((time.monotonic() - t0) * 1000),
            hits_summary={"mw": {}, "pwl": {}, "pbl": {}},
            used_path_by_tf={},
        )
        return

    # анти-дубликат: ворота на (client_sid, symbol, direction)
    acquired, gate_key = await _acquire_gate(req_uid, client_sid, symbol, direction)
    if not acquired:
        await _respond_once(req_uid, allow=False, reason="duplicated_entry")
        await _write_request_head_only(
            req_id=req_uid,
            log_uid=log_uid,
            strategy_id=strategy_id,
            client_strategy_id=client_sid,
            direction=direction,
            symbol=symbol,
            tfs_requested=timeframes_raw,
            decision_mode=decision_mode,
            oracle_version=version,
            use_bl=use_bl,
            allow=False,
            reason="duplicated_entry",
            t_recv=t_recv,
            t_fin=_now_utc_naive(),
            duration_ms=int((time.monotonic() - t0) * 1000),
            hits_summary={"mw": {}, "pwl": {}, "pbl": {}},
            used_path_by_tf={},
        )
        return

    # последовательная проверка TF: в порядке, как пришли (обычно m5 → m15 → h1)
    tf_rows: List[Tuple[str, dict]] = []
    hits_by_tf_mw: Dict[str, int] = {}
    hits_by_tf_pwl: Dict[str, int] = {}
    hits_by_tf_pbl: Dict[str, int] = {}
    used_path_by_tf: Dict[str, str] = {}

    final_allow = True
    final_reason = "ok"

    try:
        for tf in tfs:
            # извлечь кэши WL/BL и соответствующие winrate-карты для ключа (sid, tf, direction, version)
            cache_key = (strategy_id, tf, direction)
            mw_wl_set = infra.lab_mw_wl.get(version, {}).get(cache_key, set())
            pack_wl_set = infra.lab_pack_wl.get(version, {}).get(cache_key, set())
            pack_bl_set = infra.lab_pack_bl.get(version, {}).get(cache_key, set())
            mw_wr_map = infra.lab_mw_wl_wr.get(version, {}).get(cache_key, {})                 # {(agg_base, agg_state)->wr}
            pwl_wr_map = infra.lab_pack_wl_wr.get(version, {}).get(cache_key, {})              # {(base, agg_key, agg_value)->wr}
            pbl_wr_map = infra.lab_pack_bl_wr.get(version, {}).get(cache_key, {})              # {(base, agg_key, agg_value)->wr}

            mw_wl_total = len(mw_wl_set)
            pack_wl_total = len(pack_wl_set)
            pack_bl_total = len(pack_bl_set)

            # живые MW состояния
            mw_states = await _get_live_mw_states(symbol, tf)

            # MW сопоставления
            mw_hits = 0
            mw_matches: List[dict] = []
            if mw_wl_total > 0:
                for (agg_base, agg_state_need) in mw_wl_set:
                    state_live = _build_mw_agg_state(agg_base, mw_states)
                    if state_live is None:
                        continue
                    if state_live == agg_state_need:
                        mw_hits += 1
                        wr = float(mw_wr_map.get((agg_base, agg_state_need), 0.0))
                        mw_matches.append({"agg_base": agg_base, "agg_state": agg_state_need, "wr": wr})

            # PACK сопоставления: читаем паки один раз на base
            by_base_wl: Dict[str, List[Tuple[str, str]]] = {}
            for (pack_base, agg_key, agg_value) in pack_wl_set:
                by_base_wl.setdefault(pack_base, []).append((agg_key, agg_value))
            by_base_bl: Dict[str, List[Tuple[str, str]]] = {}
            for (pack_base, agg_key, agg_value) in pack_bl_set:
                by_base_bl.setdefault(pack_base, []).append((agg_key, agg_value))

            pack_wl_hits = 0
            pack_bl_hits = 0
            pack_wl_matches: List[dict] = []
            pack_bl_matches: List[dict] = []

            all_pack_bases = sorted(set(list(by_base_wl.keys()) + list(by_base_bl.keys())))
            pack_cache: Dict[str, dict] = {}
            missing_live: List[str] = []

            for base in all_pack_bases:
                indicator = _indicator_from_pack_base(base)
                if not indicator:
                    missing_live.append(f"pack:{base}")
                    continue
                obj = await _get_live_pack(symbol, tf, indicator, base)
                if obj is None:
                    missing_live.append(f"pack:{base}")
                    continue
                pack_cache[base] = obj

            # проверяем WL
            for base, rules in by_base_wl.items():
                obj = pack_cache.get(base)
                if not obj:
                    continue
                for agg_key, agg_value_need in rules:
                    val_live = _build_pack_agg_value(agg_key, obj)
                    if val_live is None:
                        continue
                    if val_live == agg_value_need:
                        pack_wl_hits += 1
                        wr = float(pwl_wr_map.get((base, agg_key, agg_value_need), 0.0))
                        pack_wl_matches.append({"pack_base": base, "agg_key": agg_key, "agg_value": agg_value_need, "wr": wr})

            # проверяем BL
            for base, rules in by_base_bl.items():
                obj = pack_cache.get(base)
                if not obj:
                    continue
                for agg_key, agg_value_need in rules:
                    val_live = _build_pack_agg_value(agg_key, obj)
                    if val_live is None:
                        continue
                    if val_live == agg_value_need:
                        pack_bl_hits += 1
                        wr = float(pbl_wr_map.get((base, agg_key, agg_value_need), 0.0))
                        pack_bl_matches.append({"pack_base": base, "agg_key": agg_key, "agg_value": agg_value_need, "wr": wr})

            # локальный вердикт по TF
            tf_allow, tf_reason, path_used = _decide_per_tf(
                decision_mode=decision_mode,
                use_bl=use_bl,
                mw_hits=mw_hits,
                pack_wl_hits=pack_wl_hits,
                pack_bl_hits=pack_bl_hits,
                mw_wl_total=mw_wl_total,
                pack_wl_total=pack_wl_total,
                pack_bl_total=pack_bl_total,
                mw_states=mw_states,
                live_missing=missing_live,
            )

            # аккумулируем сводки
            hits_by_tf_mw[tf] = mw_hits
            hits_by_tf_pwl[tf] = pack_wl_hits
            hits_by_tf_pbl[tf] = pack_bl_hits
            used_path_by_tf[tf] = path_used

            # общий итог — AND по TF (останавливать цикл нельзя: собираем полную статистику)
            if not tf_allow and final_allow:
                final_allow = False
                final_reason = tf_reason or final_reason

            # детальный JSON по TF
            tf_results = {
                "mw": {"wl_total": mw_wl_total, "wl_hits": mw_hits, "wl_matches": mw_matches},
                "pack": {
                    "wl_total": pack_wl_total,
                    "wl_hits": pack_wl_hits,
                    "wl_matches": pack_wl_matches,
                    "bl_total": pack_bl_total,
                    "bl_hits": pack_bl_hits,
                    "bl_matches": pack_bl_matches,
                },
                "live": {"mw_states": mw_states, "missing": missing_live},
            }

            # строка TF для БД
            tf_rows.append((tf, {
                "mw_wl_rules_total": mw_wl_total,
                "mw_wl_hits": mw_hits,
                "pack_wl_rules_total": pack_wl_total,
                "pack_wl_hits": pack_wl_hits,
                "pack_bl_rules_total": pack_bl_total,
                "pack_bl_hits": pack_bl_hits,
                "allow": tf_allow,
                "reason": tf_reason,
                "path_used": path_used,
                "tf_results": tf_results,
                "errors": None,
            }))

        # сформировать и отправить ответ СРАЗУ (до записи в БД)
        await _respond_once(req_uid, allow=final_allow, reason=final_reason)

    finally:
        # снимаем ворота (после отправки ответа)
        await _release_gate(req_uid, gate_key)

    # запись в БД (head + TF-строки)
    t_fin = _now_utc_naive()
    duration_ms = int((time.monotonic() - t0) * 1000)

    await _write_request_full(
        req_id=req_uid,
        log_uid=log_uid,
        strategy_id=strategy_id,
        client_strategy_id=client_sid,
        direction=direction,
        symbol=symbol,
        tfs_requested=timeframes_raw,
        decision_mode=decision_mode,
        oracle_version=version,
        use_bl=use_bl,
        allow=final_allow,
        reason=final_reason,
        t_recv=t_recv,
        t_fin=t_fin,
        duration_ms=duration_ms,
        tf_rows=tf_rows,
        hits_summary={"mw": hits_by_tf_mw, "pwl": hits_by_tf_pwl, "pbl": hits_by_tf_pbl},
        used_path_by_tf=used_path_by_tf,
    )

    # лог сводный
    log.debug(
        "LAB_DECISION: req=%s sid=%s %s %s tfs=%s ver=%s mode=%s bl=%s -> allow=%s reason=%s duration_ms=%d",
        req_uid, strategy_id, symbol, direction, timeframes_raw, version, decision_mode, use_bl,
        final_allow, final_reason, duration_ms
    )


# 🔸 Локальная логика решения по TF (возвращает путь)
def _decide_per_tf(
    decision_mode: str,
    use_bl: bool,
    mw_hits: int,
    pack_wl_hits: int,
    pack_bl_hits: int,
    mw_wl_total: int,
    pack_wl_total: int,
    pack_bl_total: int,
    mw_states: Dict[str, str],
    live_missing: List[str],
) -> Tuple[bool, str, str]:
    """
    Возвращает (allow, reason, path_used) по одному TF.
    path_used ∈ {'mw','pack','both','none','bl_veto'}
    """
    # BL-вето (если включён) — немедленный отказ
    if use_bl and pack_bl_hits > 0:
        return False, "bl_match", "bl_veto"

    missing = bool(live_missing)

    if decision_mode == "mw_only":
        if mw_hits > 0:
            return True, "ok", "mw"
        return False, ("missing_live_data" if missing else "no_mw_match"), "none"

    if decision_mode == "pack_only":
        if pack_wl_hits > 0:
            return True, "ok", "pack"
        return False, ("missing_live_data" if missing else "no_pack_match"), "none"

    if decision_mode == "mw_then_pack":
        if mw_hits > 0:
            return True, "ok", "mw"
        if pack_wl_hits > 0:
            return True, "ok", "pack"
        return False, ("missing_live_data" if missing else "no_mw_or_pack_match"), "none"

    if decision_mode == "mw_and_pack":
        if mw_hits > 0 and pack_wl_hits > 0:
            return True, "ok", "both"
        return False, ("missing_live_data" if missing else "no_mw_and_pack_match"), "none"

    return False, "bad_decision_mode", "none"


# 🔸 Ответ в стрим (идемпотентно)
async def _respond_once(req_uid: str, allow: bool, reason: str):
    if not req_uid:
        return
    key = RESP_SENT_KEY_TMPL.format(req_uid=req_uid)
    try:
        ok = await infra.redis_client.set(key, "1", ex=RESP_SENT_TTL_SEC, nx=True)
    except Exception:
        ok = True  # если Redis недоступен для idempotency-ключа — лучше всё-таки отправить
    if ok:
        try:
            payload = {"req_uid": req_uid, "allow": bool(allow), "reason": str(reason or "")}
            await infra.redis_client.xadd(
                name=RESPONSE_STREAM,
                fields={"data": json.dumps(payload, separators=(",", ":"))},
            )
        except Exception:
            log.exception("❌ LAB_DECISION: не удалось отправить ответ в стрим")


# 🔸 Запись в БД (только head)
async def _write_request_head_only(
    req_id: str,
    log_uid: str,
    strategy_id: int,
    client_strategy_id: Optional[int],
    direction: str,
    symbol: str,
    tfs_requested: str,
    decision_mode: str,
    oracle_version: str,
    use_bl: bool,
    allow: bool,
    reason: str,
    t_recv: datetime,
    t_fin: datetime,
    duration_ms: int,
    hits_summary: Dict[str, Dict[str, int]],
    used_path_by_tf: Optional[Dict[str, str]] = None,
):
    mw_js  = json.dumps(hits_summary.get("mw",  {}), separators=(",", ":"))
    pwl_js = json.dumps(hits_summary.get("pwl", {}), separators=(",", ":"))
    pbl_js = json.dumps(hits_summary.get("pbl", {}), separators=(",", ":"))
    upath_js = json.dumps(used_path_by_tf or {}, separators=(",", ":"))

    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO laboratory_request_head (
                req_id, log_uid, strategy_id, client_strategy_id,
                direction, symbol, timeframes_requested,
                decision_mode, oracle_version, use_bl,
                allow, reason,
                mw_wl_hits_by_tf, pack_wl_hits_by_tf, pack_bl_hits_by_tf,
                used_path_by_tf,
                received_at, finished_at, duration_ms
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14::jsonb,$15::jsonb,$16::jsonb,$17,$18,$19)
            ON CONFLICT (req_id) DO UPDATE SET
                log_uid = EXCLUDED.log_uid,
                strategy_id = EXCLUDED.strategy_id,
                client_strategy_id = EXCLUDED.client_strategy_id,
                direction = EXCLUDED.direction,
                symbol = EXCLUDED.symbol,
                timeframes_requested = EXCLUDED.timeframes_requested,
                decision_mode = EXCLUDED.decision_mode,
                oracle_version = EXCLUDED.oracle_version,
                use_bl = EXCLUDED.use_bl,
                allow = EXCLUDED.allow,
                reason = EXCLUDED.reason,
                mw_wl_hits_by_tf = EXCLUDED.mw_wl_hits_by_tf,
                pack_wl_hits_by_tf = EXCLUDED.pack_wl_hits_by_tf,
                pack_bl_hits_by_tf = EXCLUDED.pack_bl_hits_by_tf,
                used_path_by_tf = EXCLUDED.used_path_by_tf,
                received_at = EXCLUDED.received_at,
                finished_at = EXCLUDED.finished_at,
                duration_ms = EXCLUDED.duration_ms
            """,
            req_id, log_uid, int(strategy_id), client_strategy_id,
            direction, symbol, tfs_requested,
            decision_mode, oracle_version, bool(use_bl),
            bool(allow), reason or "",
            mw_js, pwl_js, pbl_js,
            upath_js,
            t_recv, t_fin, int(duration_ms),
        )


# 🔸 Полная запись (head + TF-строки)
async def _write_request_full(
    req_id: str,
    log_uid: str,
    strategy_id: int,
    client_strategy_id: Optional[int],
    direction: str,
    symbol: str,
    tfs_requested: str,
    decision_mode: str,
    oracle_version: str,
    use_bl: bool,
    allow: bool,
    reason: str,
    t_recv: datetime,
    t_fin: datetime,
    duration_ms: int,
    tf_rows: List[Tuple[str, dict]],
    hits_summary: Dict[str, Dict[str, int]],
    used_path_by_tf: Optional[Dict[str, str]] = None,
):
    mw_js  = json.dumps(hits_summary.get("mw",  {}), separators=(",", ":"))
    pwl_js = json.dumps(hits_summary.get("pwl", {}), separators=(",", ":"))
    pbl_js = json.dumps(hits_summary.get("pbl", {}), separators=(",", ":"))
    upath_js = json.dumps(used_path_by_tf or {}, separators=(",", ":"))

    async with infra.pg_pool.acquire() as conn:
        async with conn.transaction():
            # head
            await conn.execute(
                """
                INSERT INTO laboratory_request_head (
                    req_id, log_uid, strategy_id, client_strategy_id,
                    direction, symbol, timeframes_requested,
                    decision_mode, oracle_version, use_bl,
                    allow, reason,
                    mw_wl_hits_by_tf, pack_wl_hits_by_tf, pack_bl_hits_by_tf,
                    used_path_by_tf,
                    received_at, finished_at, duration_ms
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14::jsonb,$15::jsonb,$16::jsonb,$17,$18,$19)
                ON CONFLICT (req_id) DO UPDATE SET
                    log_uid = EXCLUDED.log_uid,
                    strategy_id = EXCLUDED.strategy_id,
                    client_strategy_id = EXCLUDED.client_strategy_id,
                    direction = EXCLUDED.direction,
                    symbol = EXCLUDED.symbol,
                    timeframes_requested = EXCLUDED.timeframes_requested,
                    decision_mode = EXCLUDED.decision_mode,
                    oracle_version = EXCLUDED.oracle_version,
                    use_bl = EXCLUDED.use_bl,
                    allow = EXCLUDED.allow,
                    reason = EXCLUDED.reason,
                    mw_wl_hits_by_tf = EXCLUDED.mw_wl_hits_by_tf,
                    pack_wl_hits_by_tf = EXCLUDED.pack_wl_hits_by_tf,
                    pack_bl_hits_by_tf = EXCLUDED.pack_bl_hits_by_tf,
                    used_path_by_tf = EXCLUDED.used_path_by_tf,
                    received_at = EXCLUDED.received_at,
                    finished_at = EXCLUDED.finished_at,
                    duration_ms = EXCLUDED.duration_ms
                """,
                req_id, log_uid, int(strategy_id), client_strategy_id,
                direction, symbol, tfs_requested,
                decision_mode, oracle_version, bool(use_bl),
                bool(allow), reason or "",
                mw_js, pwl_js, pbl_js,
                upath_js,
                t_recv, t_fin, int(duration_ms),
            )

            # tf rows
            if tf_rows:
                args = []
                for tf, row in tf_rows:
                    tf_results_js = json.dumps(row["tf_results"], ensure_ascii=False, separators=(",", ":")) if row.get("tf_results") is not None else None
                    errors_js = json.dumps(row["errors"], ensure_ascii=False, separators=(",", ":")) if row.get("errors") is not None else None
                    args.append((
                        req_id, tf,
                        int(row.get("mw_wl_rules_total", 0)),
                        int(row.get("pack_wl_rules_total", 0)),
                        int(row.get("pack_bl_rules_total", 0)),
                        int(row.get("mw_wl_hits", 0)),
                        int(row.get("pack_wl_hits", 0)),
                        int(row.get("pack_bl_hits", 0)),
                        bool(row.get("allow", False)),
                        str(row.get("reason", "") or ""),
                        str(row.get("path_used", "none") or "none"),
                        tf_results_js,
                        errors_js,
                    ))
                await conn.executemany(
                    """
                    INSERT INTO laboratory_request_tf (
                        req_id, tf,
                        mw_wl_rules_total, pack_wl_rules_total, pack_bl_rules_total,
                        mw_wl_hits, pack_wl_hits, pack_bl_hits,
                        allow, reason, path_used, tf_results, errors
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13::jsonb)
                    ON CONFLICT (req_id, tf) DO UPDATE SET
                        mw_wl_rules_total = EXCLUDED.mw_wl_rules_total,
                        pack_wl_rules_total = EXCLUDED.pack_wl_rules_total,
                        pack_bl_rules_total = EXCLUDED.pack_bl_rules_total,
                        mw_wl_hits = EXCLUDED.mw_wl_hits,
                        pack_wl_hits = EXCLUDED.pack_wl_hits,
                        pack_bl_hits = EXCLUDED.pack_bl_hits,
                        allow = EXCLUDED.allow,
                        reason = EXCLUDED.reason,
                        path_used = EXCLUDED.path_used,
                        tf_results = EXCLUDED.tf_results,
                        errors = EXCLUDED.errors
                    """,
                    args
                )