# 🔸 laboratory_decision_maker.py — воркер «советчика»: чтение запросов из Redis Stream, сверка с WL/BL, ответ в стрим и запись в БД

# 🔸 Импорты
import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Tuple, Set, Optional

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
RESP_SENT_TTL_SEC = 24 * 60 * 60

# 🔸 Параметры чтения
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

    log.info("🚀 LAB_DECISION: старт воркера")

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

            for _, msgs in resp:
                for msg_id, fields in msgs:
                    try:
                        # парсинг payload
                        raw = fields.get("data", "{}")
                        payload = json.loads(raw) if isinstance(raw, str) else (raw or {})
                    except Exception:
                        log.exception("❌ LAB_DECISION: не удалось распарсить payload")
                        # безопасный ack (чтобы не зациклиться на битом сообщении)
                        await _ack_safe(msg_id)
                        continue

                    # обработка одного запроса
                    try:
                        await _handle_request(payload)
                    except Exception:
                        log.exception("❌ LAB_DECISION: ошибка обработки запроса")
                    finally:
                        # ack после обработки (ответ уже отправлен; запись в БД сделана)
                        await _ack_safe(msg_id)

        except asyncio.CancelledError:
            log.debug("⏹️ LAB_DECISION: остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ LAB_DECISION: ошибка цикла — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Вспомогательные функции верхнего уровня

async def _ack_safe(msg_id: str):
    try:
        await infra.redis_client.xack(REQUEST_STREAM, CONSUMER_GROUP, msg_id)
    except Exception:
        log.exception("⚠️ LAB_DECISION: ошибка ACK (id=%s)", msg_id)


def _parse_bool(s: Optional[str]) -> bool:
    if s is None:
        return False
    return str(s).strip().lower() in ("1", "true", "t", "yes", "y")


def _normalize_symbol(s: str) -> str:
    return (s or "").upper().strip()


def _now_utc_naive() -> datetime:
    return datetime.utcnow().replace(tzinfo=None)


def _parse_timeframes(tfs: str) -> List[str]:
    # разбиваем, чистим, фильтруем по допустимым, сохраняем порядок
    seen = set()
    out: List[str] = []
    for tf in (tfs or "").split(","):
        tf = tf.strip().lower()
        if tf in ALLOWED_TFS and tf not in seen:
            out.append(tf)
            seen.add(tf)
    return out


def _indicator_from_pack_base(pack_base: str) -> Optional[str]:
    # точные префиксы
    if pack_base.startswith("bb"):
        return "bb"
    if pack_base.startswith("adx_dmi"):
        return "adx_dmi"
    # остальные — по буквенной части до цифр/подчёркиваний
    for pref in ("rsi", "mfi", "ema", "atr", "lr", "macd"):
        if pack_base.startswith(pref):
            return pref
    return None


async def _get_live_mw_states(symbol: str, tf: str) -> Dict[str, str]:
    """Считывает текущие состояния MW-баз из ind_mw_live:{symbol}:{tf}:{kind} (JSON)."""
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

        # пробуем извлечь «state» из paка (универсальные варианты)
        state = None
        if isinstance(obj, dict):
            # прямое поле
            state = obj.get("state") or obj.get("mw_state")
            # под-узел pack
            if state is None and isinstance(obj.get("pack"), dict):
                state = obj["pack"].get("state") or obj["pack"].get("mw_state")
            # fallback: явные поля некоторых паков
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
    """Читает JSON live-пака из pack_live:{indicator}:{symbol}:{tf}:{base}."""
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
    """Достаёт строковое значение поля из пака (прямо или из obj['pack'])."""
    if not isinstance(obj, dict):
        return None
    # прямое поле
    val = obj.get(field)
    if val is None and isinstance(obj.get("pack"), dict):
        val = obj["pack"].get(field)
    if val is None and isinstance(obj.get("features"), dict):
        val = obj["features"].get(field)
    # приводим к строке без модификаций
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return str(val)
    return str(val)


def _build_mw_agg_state(agg_base: str, mw_states: Dict[str, str]) -> Optional[str]:
    """Формирует каноническую строку agg_state для MW (или возвращает solo-state)."""
    bases = agg_base.split("_")
    if len(bases) == 1:
        # solo — возвращаем чистое состояние этой базы
        b = bases[0]
        state = mw_states.get(b)
        return state if isinstance(state, str) and state else None
    # combo — base:state|...
    parts: List[str] = []
    for b in bases:
        state = mw_states.get(b)
        if not (isinstance(state, str) and state):
            return None
        parts.append(f"{b}:{state}")
    return "|".join(parts)


def _build_pack_agg_value(agg_key: str, pack_obj: dict) -> Optional[str]:
    """Формирует каноническую строку agg_value из пака по agg_key (field1|field2|...)."""
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


# 🔸 Логика обработки одного запроса

async def _handle_request(payload: dict):
    # время начала
    t_recv = _now_utc_naive()
    t0 = time.monotonic()

    # извлекаем поля
    req_uid = str(payload.get("req_uid") or "").strip()
    log_uid = str(payload.get("log_uid") or "").strip()
    strategy_id = int(payload.get("strategy_id") or 0)
    client_strategy_id = payload.get("client_strategy_id")
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
        # формируем отказ немедленно
        await _respond_once(req_uid, allow=False, reason="bad_request")
        # в БД тоже зафиксируем как head без TF (errors)
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
        )
        return

    # последовательная проверка TF: m5 → m15 → h1 (в том порядке, как в запросе)
    tf_rows: List[Tuple[str, dict]] = []  # (tf, tf_row_data_for_db)
    hits_by_tf_mw: Dict[str, int] = {}
    hits_by_tf_pwl: Dict[str, int] = {}
    hits_by_tf_pbl: Dict[str, int] = {}

    final_allow = True
    final_reason = "ok"

    for tf in tfs:
        # извлечь кэши WL/BL для ключа (sid, tf, direction, version)
        mw_wl_set = infra.lab_mw_wl.get(version, {}).get((strategy_id, tf, direction), set())
        pack_wl_set = infra.lab_pack_wl.get(version, {}).get((strategy_id, tf, direction), set())
        pack_bl_set = infra.lab_pack_bl.get(version, {}).get((strategy_id, tf, direction), set())

        mw_wl_total = len(mw_wl_set)
        pack_wl_total = len(pack_wl_set)
        pack_bl_total = len(pack_bl_set)

        # живые состояния
        mw_states = await _get_live_mw_states(symbol, tf)

        # MW сопоставления
        mw_hits = 0
        mw_matches: List[dict] = []
        if mw_wl_total > 0:
            for (agg_base, agg_state_needed) in mw_wl_set:
                state_live = _build_mw_agg_state(agg_base, mw_states)
                if state_live is None:
                    continue
                if (agg_base.find("_") == -1 and state_live == agg_state_needed) or (
                    agg_base.find("_") != -1 and state_live == agg_state_needed
                ):
                    mw_hits += 1
                    mw_matches.append({"agg_base": agg_base, "agg_state": agg_state_needed})

        # PACK сопоставления (подгружаем паки по каждому pack_base единожды)
        # сгруппируем правила по pack_base
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

        # набор всех pack_base, которые нам нужно читать
        all_pack_bases = sorted(set(list(by_base_wl.keys()) + list(by_base_bl.keys())))
        # читаем по одному разу каждый pack_base
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
                    pack_wl_matches.append({"pack_base": base, "agg_key": agg_key, "agg_value": agg_value_need})

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
                    pack_bl_matches.append({"pack_base": base, "agg_key": agg_key, "agg_value": agg_value_need})

        # локальный вердикт по TF
        tf_allow, tf_reason = _decide_per_tf(
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

        # итог общий — AND по TF
        if not tf_allow and final_allow:
            final_allow = False
            final_reason = tf_reason or final_reason

        # готовим строку для laboratory_request_tf
        tf_results = {
            "mw": {
                "wl_total": mw_wl_total,
                "wl_hits": mw_hits,
                "wl_matches": mw_matches,
            },
            "pack": {
                "wl_total": pack_wl_total,
                "wl_hits": pack_wl_hits,
                "wl_matches": pack_wl_matches,
                "bl_total": pack_bl_total,
                "bl_hits": pack_bl_hits,
                "bl_matches": pack_bl_matches,
            },
            "live": {
                "mw_states": mw_states,
                "missing": missing_live,
            },
        }

        tf_rows.append((tf, {
            "mw_wl_rules_total": mw_wl_total,
            "mw_wl_hits": mw_hits,
            "pack_wl_rules_total": pack_wl_total,
            "pack_wl_hits": pack_wl_hits,
            "pack_bl_rules_total": pack_bl_total,
            "pack_bl_hits": pack_bl_hits,
            "allow": tf_allow,
            "reason": tf_reason,
            "tf_results": tf_results,
            "errors": None,
        }))

    # сформировать и отправить ответ СРАЗУ (до записи в БД)
    await _respond_once(req_uid, allow=final_allow, reason=final_reason)

    # после ответа — запись в БД (head + tf-строки)
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
        hits_summary={
            "mw": hits_by_tf_mw,
            "pwl": hits_by_tf_pwl,
            "pbl": hits_by_tf_pbl,
        },
    )

    # лог сводный
    log.info(
        "LAB_DECISION: req=%s sid=%s %s %s tfs=%s ver=%s mode=%s bl=%s -> allow=%s reason=%s duration_ms=%d",
        req_uid, strategy_id, symbol, direction, timeframes_raw, version, decision_mode, use_bl,
        final_allow, final_reason, duration_ms
    )


# 🔸 Локальная логика решения по TF

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
) -> Tuple[bool, str]:
    """Возвращает (allow, reason) по одному TF."""
    # BL вето (если включён) — сразу отказ
    if use_bl and pack_bl_hits > 0:
        return False, "bl_match"

    # отсутствие live-данных не даёт «auto-deny», но если нет матчей — это хорошая причина
    missing = bool(live_missing)

    if decision_mode == "mw_only":
        if mw_hits > 0:
            return True, "ok"
        return False, "no_mw_match" if not missing else "missing_live_data"

    if decision_mode == "pack_only":
        if pack_wl_hits > 0:
            return True, "ok"
        return False, "no_pack_match" if not missing else "missing_live_data"

    if decision_mode == "mw_then_pack":
        if mw_hits > 0:
            return True, "ok"
        if pack_wl_hits > 0:
            return True, "ok"
        return False, "no_mw_or_pack_match" if not missing else "missing_live_data"

    if decision_mode == "mw_and_pack":
        if mw_hits > 0 and pack_wl_hits > 0:
            return True, "ok"
        return False, "no_mw_and_pack_match" if not missing else "missing_live_data"

    # fallback
    return False, "bad_decision_mode"


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
                maxlen=10_000,
                approximate=True,
            )
        except Exception:
            log.exception("❌ LAB_DECISION: не удалось отправить ответ в стрим")
    else:
        # ответ уже был отправлен ранее — ничего не делаем
        pass


# 🔸 Запись в БД

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
):
    mw_js = json.dumps(hits_summary.get("mw", {}), separators=(",", ":"))
    pwl_js = json.dumps(hits_summary.get("pwl", {}), separators=(",", ":"))
    pbl_js = json.dumps(hits_summary.get("pbl", {}), separators=(",", ":"))

    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO laboratory_request_head (
                req_id, log_uid, strategy_id, client_strategy_id,
                direction, symbol, timeframes_requested,
                decision_mode, oracle_version, use_bl,
                allow, reason,
                mw_wl_hits_by_tf, pack_wl_hits_by_tf, pack_bl_hits_by_tf,
                received_at, finished_at, duration_ms
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14::jsonb,$15::jsonb,$16,$17,$18)
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
                received_at = EXCLUDED.received_at,
                finished_at = EXCLUDED.finished_at,
                duration_ms = EXCLUDED.duration_ms
            """,
            req_id, log_uid, int(strategy_id), client_strategy_id,
            direction, symbol, tfs_requested,
            decision_mode, oracle_version, bool(use_bl),
            bool(allow), reason or "",
            mw_js, pwl_js, pbl_js,
            t_recv, t_fin, int(duration_ms),
        )


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
):
    mw_js = json.dumps(hits_summary.get("mw", {}), separators=(",", ":"))
    pwl_js = json.dumps(hits_summary.get("pwl", {}), separators=(",", ":"))
    pbl_js = json.dumps(hits_summary.get("pbl", {}), separators=(",", ":"))

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
                    received_at, finished_at, duration_ms
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14::jsonb,$15::jsonb,$16,$17,$18)
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
                    received_at = EXCLUDED.received_at,
                    finished_at = EXCLUDED.finished_at,
                    duration_ms = EXCLUDED.duration_ms
                """,
                req_id, log_uid, int(strategy_id), client_strategy_id,
                direction, symbol, tfs_requested,
                decision_mode, oracle_version, bool(use_bl),
                bool(allow), reason or "",
                mw_js, pwl_js, pbl_js,
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
                        tf_results_js,
                        errors_js,
                    ))
                await conn.executemany(
                    """
                    INSERT INTO laboratory_request_tf (
                        req_id, tf,
                        mw_wl_rules_total, pack_wl_rules_total, pack_bl_rules_total,
                        mw_wl_hits, pack_wl_hits, pack_bl_hits,
                        allow, reason, tf_results, errors
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12::jsonb)
                    ON CONFLICT (req_id, tf) DO UPDATE SET
                        mw_wl_rules_total = EXCLUDED.mw_wl_rules_total,
                        pack_wl_rules_total = EXCLUDED.pack_wl_rules_total,
                        pack_bl_rules_total = EXCLUDED.pack_bl_rules_total,
                        mw_wl_hits = EXCLUDED.mw_wl_hits,
                        pack_wl_hits = EXCLUDED.pack_wl_hits,
                        pack_bl_hits = EXCLUDED.pack_bl_hits,
                        allow = EXCLUDED.allow,
                        reason = EXCLUDED.reason,
                        tf_results = EXCLUDED.tf_results,
                        errors = EXCLUDED.errors
                    """,
                    args
                )