# 🔸 laboratory_decision_auditor.py — воркер «советчика» по ветке аудитора: идеи/thresholds → allow/deny

# 🔸 Импорты
import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any

import laboratory_infra as infra
from laboratory_auditor_config import (
    get_best_for,
    get_thresholds,
)

# 🔸 Импорт обработчика идеи emacross_cs (остальные идеи будут добавляться аналогично)
from laboratory_auditor_emacross import evaluate_emacross_cs

# 🔸 Логгер
log = logging.getLogger("LAB_DECISION_AUDITOR")

# 🔸 Константы стримов/групп
REQUEST_STREAM = "laboratory:decision_request"
RESPONSE_STREAM = "laboratory:decision_response"
CONSUMER_GROUP = "LAB_DECISION_AUDITOR_GROUP"
CONSUMER_NAME = "LAB_DECISION_AUDITOR_WORKER"

# 🔸 Идемпотентность ответа (ключи в Redis)
RESP_SENT_KEY_TMPL = "lab:decision:sent:{req_uid}"
RESP_SENT_TTL_SEC = 24 * 60 * 60  # 24h

# 🔸 Ограничитель (анти-дубликат по тикеру/направлению/клиент-стратегии)
GATE_KEY_TMPL = "lab:gate:busy:{client_sid}:{symbol}:{direction}"
DUP_GUARD_TTL_SEC = 20  # TTL ворот, сек (пока нет ответа по первому запросу)

# 🔸 Параллелизм/чтение
MAX_CONCURRENCY = 16
READ_COUNT = 32
READ_BLOCK_MS = 3000

# 🔸 Доп. константы
ALLOWED_TFS = ("m5", "m15", "h1")
ALLOWED_DIRECTIONS = ("long", "short")

# 🔸 Маппинг idea_key → обработчик
IDEA_HANDLERS = {
    "emacross_cs": evaluate_emacross_cs,
    # последующие идеи будут добавляться сюда
}

# скрипт безопасного релиза «только если значение совпадает»
_RELEASE_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
else
  return 0
end
"""


# 🔸 Публичная точка входа воркера
async def run_laboratory_decision_auditor():
    # условия достаточности
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ LAB_DECISION_AUDITOR: PG/Redis не инициализированы, воркер не запущен")
        return

    # создать consumer group (идемпотентно)
    try:
        await infra.redis_client.xgroup_create(
            name=REQUEST_STREAM,
            groupname=CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug("📡 LAB_DECISION_AUDITOR: создана consumer group для %s", REQUEST_STREAM)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            log.exception("❌ LAB_DECISION_AUDITOR: ошибка создания consumer group")
            return

    log.debug("🚀 LAB_DECISION_AUDITOR: старт воркера (parallel=%d)", MAX_CONCURRENCY)

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

            tasks: List[asyncio.Task] = []
            for _, msgs in resp:
                for msg_id, fields in msgs:
                    tasks.append(_process_message_guard(sem, msg_id, fields))

            if tasks:
                await asyncio.gather(*tasks)

        except asyncio.CancelledError:
            log.debug("⏹️ LAB_DECISION_AUDITOR: остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ LAB_DECISION_AUDITOR: ошибка цикла — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Гард обработки одного сообщения (ACK в finally)
async def _process_message_guard(sem: asyncio.Semaphore, msg_id: str, fields: Dict[str, str]):
    async with sem:
        try:
            raw = fields.get("data", "{}")
            payload = json.loads(raw) if isinstance(raw, str) else (raw or {})
        except Exception:
            log.exception("❌ LAB_DECISION_AUDITOR: не удалось распарсить payload")
            await _ack_safe(msg_id)
            return

        try:
            # флаг аудиторной ветки (может отсутствовать)
            use_auditor = _parse_bool(payload.get("use_auditor"))
            if not use_auditor:
                # это запрос для oracle-ветки — игнорируем в аудиторном воркере
                await _ack_safe(msg_id)
                return

            await _handle_request_auditor(payload)
        except Exception:
            log.exception("❌ LAB_DECISION_AUDITOR: ошибка обработки запроса")
        finally:
            await _ack_safe(msg_id)


# 🔸 ACK безопасно
async def _ack_safe(msg_id: str):
    try:
        await infra.redis_client.xack(REQUEST_STREAM, CONSUMER_GROUP, msg_id)
    except Exception:
        log.exception("⚠️ LAB_DECISION_AUDITOR: ошибка ACK (id=%s)", msg_id)


# 🔸 Логика обработки одного запроса аудиторной ветки
async def _handle_request_auditor(payload: dict):
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
    timeframes_raw = str(payload.get("timeframes") or "")
    tfs = _parse_timeframes(timeframes_raw)
    use_auditor = _parse_bool(payload.get("use_auditor"))

    # минимальная валидация
    bad_reasons = []
    if not req_uid:
        bad_reasons.append("bad_req_uid")
    if strategy_id <= 0:
        bad_reasons.append("bad_strategy_id")
    if symbol == "":
        bad_reasons.append("bad_symbol")
    if direction not in ALLOWED_DIRECTIONS:
        bad_reasons.append("bad_direction")
    if not tfs:
        bad_reasons.append("bad_timeframes")
    if not use_auditor:
        bad_reasons.append("use_auditor_false")

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
            allow=False,
            reason="duplicated_entry",
            t_recv=t_recv,
            t_fin=_now_utc_naive(),
            duration_ms=int((time.monotonic() - t0) * 1000),
            hits_summary={"mw": {}, "pwl": {}, "pbl": {}},
            used_path_by_tf={},
        )
        return

    final_allow = True
    final_reason = "ok"
    tf_rows: List[Tuple[str, dict]] = []
    used_path_by_tf: Dict[str, str] = {}
    hits_by_tf_mw: Dict[str, int] = {}
    hits_by_tf_pwl: Dict[str, int] = {}
    hits_by_tf_pbl: Dict[str, int] = {}

    try:
        # достаём лучшую идею из витрины аудитора
        best = get_best_for(strategy_id, direction)
        if best is None:
            final_allow = False
            final_reason = "no_auditor_idea"
            await _respond_once(req_uid, allow=False, reason=final_reason)
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
                allow=False,
                reason=final_reason,
                t_recv=t_recv,
                t_fin=_now_utc_naive(),
                duration_ms=int((time.monotonic() - t0) * 1000),
                hits_summary={"mw": {}, "pwl": {}, "pbl": {}},
                used_path_by_tf={},
            )
            return

        handler = IDEA_HANDLERS.get(best.idea_key)
        if handler is None:
            final_allow = False
            final_reason = "no_handler_for_idea"
            await _respond_once(req_uid, allow=False, reason=final_reason)
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
                allow=False,
                reason=final_reason,
                t_recv=t_recv,
                t_fin=_now_utc_naive(),
                duration_ms=int((time.monotonic() - t0) * 1000),
                hits_summary={"mw": {}, "pwl": {}, "pbl": {}},
                used_path_by_tf={},
            )
            return

        # проход по TF: m5 → m15 → h1, с учётом variant_key
        for tf in tfs:
            if not _tf_is_used_in_variant(tf, best.variant_key):
                continue

            # по аудиторной ветке MW/PACK-хитов нет
            hits_by_tf_mw[tf] = 0
            hits_by_tf_pwl[tf] = 0
            hits_by_tf_pbl[tf] = 0

            # thresholds для этой идеи/символа/TF/окна
            thr = await get_thresholds(
                idea_key=best.idea_key,
                run_id=best.source_run_id,
                strategy_id=strategy_id,
                direction=direction,
                symbol=symbol,
                timeframe=tf,
                window_tag=best.primary_window,
            )

            if thr is None:
                tf_allow = False
                tf_reason = "no_thresholds_for_symbol"
                tf_details: Dict[str, Any] = {
                    "auditor": {
                        "idea_key": best.idea_key,
                        "variant": best.variant_key,
                        "primary_window": best.primary_window,
                        "source_run_id": best.source_run_id,
                        "allow": False,
                        "reason": tf_reason,
                        "thresholds": None,
                    }
                }
            else:
                try:
                    tf_allow, tf_reason, tf_details = await handler(
                        strategy_id=strategy_id,
                        client_strategy_id=client_sid,
                        symbol=symbol,
                        direction=direction,
                        timeframe=tf,
                        best=best,
                        thresholds=thr,
                        redis_client=infra.redis_client,
                    )
                except Exception:
                    log.exception(
                        "❌ LAB_DECISION_AUDITOR: ошибка обработчика идеи (idea=%s, sid=%s, symbol=%s, dir=%s, tf=%s)",
                        best.idea_key, strategy_id, symbol, direction, tf
                    )
                    tf_allow = False
                    tf_reason = "idea_handler_error"
                    tf_details = {
                        "auditor": {
                            "idea_key": best.idea_key,
                            "variant": best.variant_key,
                            "primary_window": best.primary_window,
                            "source_run_id": best.source_run_id,
                            "allow": False,
                            "reason": tf_reason,
                        }
                    }

            used_path_by_tf[tf] = "none"

            # формируем строку TF для записи
            tf_rows.append(
                (
                    tf,
                    {
                        "mw_wl_rules_total": 0,
                        "mw_wl_hits": 0,
                        "pack_wl_rules_total": 0,
                        "pack_wl_hits": 0,
                        "pack_bl_rules_total": 0,
                        "pack_bl_hits": 0,
                        "allow": tf_allow,
                        "reason": tf_reason,
                        "path_used": "none",
                        "tf_results": tf_details,
                        "errors": None,
                    },
                )
            )

            # общий итог — AND по TF
            if not tf_allow and final_allow:
                final_allow = False
                final_reason = tf_reason or final_reason

        # если ни один TF не участвовал — трактуем как отказ
        if not tf_rows:
            final_allow = False
            final_reason = "no_timeframes_for_variant"

        # сформировать и отправить ответ
        await _respond_once(req_uid, allow=final_allow, reason=final_reason)

    finally:
        # снимаем ворота (после отправки ответа или раннего выхода)
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
        allow=final_allow,
        reason=final_reason,
        t_recv=t_recv,
        t_fin=t_fin,
        duration_ms=duration_ms,
        tf_rows=tf_rows,
        hits_summary={"mw": hits_by_tf_mw, "pwl": hits_by_tf_pwl, "pbl": hits_by_tf_pbl},
        used_path_by_tf=used_path_by_tf,
    )

    log.info(
        "LAB_DECISION_AUDITOR: req=%s sid=%s %s %s idea=%s variant=%s -> allow=%s reason=%s duration_ms=%d",
        req_uid,
        strategy_id,
        symbol,
        direction,
        best.idea_key if 'best' in locals() and best else "-",
        best.variant_key if 'best' in locals() and best else "-",
        final_allow,
        final_reason,
        duration_ms,
    )


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
        log.exception("⚠️ LAB_DECISION_AUDITOR: acquire_gate error (key=%s)", key)
        return True, None


async def _release_gate(req_uid: str, gate_key: Optional[str]):
    if not gate_key:
        return
    try:
        # redis-py: eval(script, numkeys, *keys_and_args)
        await infra.redis_client.eval(_RELEASE_LUA, 1, gate_key, req_uid)
    except Exception:
        log.exception("⚠️ LAB_DECISION_AUDITOR: release_gate error (key=%s)", gate_key)


# 🔸 Утилиты парсинга и времени

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


def _tf_is_used_in_variant(tf: str, variant_key: str) -> bool:
    tf_norm = str(tf).lower().strip()
    var = str(variant_key).lower().strip()
    if var == "m5_only":
        return tf_norm == "m5"
    if var == "m5_m15":
        return tf_norm in ("m5", "m15")
    if var == "m5_m15_h1":
        return tf_norm in ("m5", "m15", "h1")
    # неизвестный variant — по умолчанию используем только m5
    return tf_norm == "m5"


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
            log.exception("❌ LAB_DECISION_AUDITOR: не удалось отправить ответ в стрим")


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
    allow: bool,
    reason: str,
    t_recv: datetime,
    t_fin: datetime,
    duration_ms: int,
    hits_summary: Dict[str, Dict[str, int]],
    used_path_by_tf: Optional[Dict[str, str]] = None,
):
    # сериализация сводок
    mw_js = json.dumps(hits_summary.get("mw", {}), separators=(",", ":"))
    pwl_js = json.dumps(hits_summary.get("pwl", {}), separators=(",", ":"))
    pbl_js = json.dumps(hits_summary.get("pbl", {}), separators=(",", ":"))
    upath_js = json.dumps(used_path_by_tf or {}, separators=(",", ":"))

    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO laboratory_request_head (
                req_id, log_uid, strategy_id, client_strategy_id,
                direction, symbol, timeframes_requested,
                decision_mode, oracle_version,
                allow, reason,
                mw_wl_hits_by_tf, pack_wl_hits_by_tf, pack_bl_hits_by_tf,
                used_path_by_tf,
                received_at, finished_at, duration_ms,
                decision_engine
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13::jsonb,$14::jsonb,$15::jsonb,$16,$17,$18,'auditor')
            ON CONFLICT (req_id) DO UPDATE SET
                log_uid = EXCLUDED.log_uid,
                strategy_id = EXCLUDED.strategy_id,
                client_strategy_id = EXCLUDED.client_strategy_id,
                direction = EXCLUDED.direction,
                symbol = EXCLUDED.symbol,
                timeframes_requested = EXCLUDED.timeframes_requested,
                decision_mode = EXCLUDED.decision_mode,
                oracle_version = EXCLUDED.oracle_version,
                allow = EXCLUDED.allow,
                reason = EXCLUDED.reason,
                mw_wl_hits_by_tf = EXCLUDED.mw_wl_hits_by_tf,
                pack_wl_hits_by_tf = EXCLUDED.pack_wl_hits_by_tf,
                pack_bl_hits_by_tf = EXCLUDED.pack_bl_hits_by_tf,
                used_path_by_tf = EXCLUDED.used_path_by_tf,
                received_at = EXCLUDED.received_at,
                finished_at = EXCLUDED.finished_at,
                duration_ms = EXCLUDED.duration_ms,
                decision_engine = EXCLUDED.decision_engine
            """,
            req_id,
            log_uid,
            int(strategy_id),
            client_strategy_id,
            direction,
            symbol,
            tfs_requested,
            decision_mode,
            oracle_version,
            bool(allow),
            reason or "",
            mw_js,
            pwl_js,
            pbl_js,
            upath_js,
            t_recv,
            t_fin,
            int(duration_ms),
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
    allow: bool,
    reason: str,
    t_recv: datetime,
    t_fin: datetime,
    duration_ms: int,
    tf_rows: List[Tuple[str, dict]],
    hits_summary: Dict[str, Dict[str, int]],
    used_path_by_tf: Optional[Dict[str, str]] = None,
):
    # сериализация сводок
    mw_js = json.dumps(hits_summary.get("mw", {}), separators=(",", ":"))
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
                    decision_mode, oracle_version,
                    allow, reason,
                    mw_wl_hits_by_tf, pack_wl_hits_by_tf, pack_bl_hits_by_tf,
                    used_path_by_tf,
                    received_at, finished_at, duration_ms,
                    decision_engine
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13::jsonb,$14::jsonb,$15::jsonb,$16,$17,$18,'auditor')
                ON CONFLICT (req_id) DO UPDATE SET
                    log_uid = EXCLUDED.log_uid,
                    strategy_id = EXCLUDED.strategy_id,
                    client_strategy_id = EXCLUDED.client_strategy_id,
                    direction = EXCLUDED.direction,
                    symbol = EXCLUDED.symbol,
                    timeframes_requested = EXCLUDED.timeframes_requested,
                    decision_mode = EXCLUDED.decision_mode,
                    oracle_version = EXCLUDED.oracle_version,
                    allow = EXCLUDED.allow,
                    reason = EXCLUDED.reason,
                    mw_wl_hits_by_tf = EXCLUDED.mw_wl_hits_by_tf,
                    pack_wl_hits_by_tf = EXCLUDED.pack_wl_hits_by_tf,
                    pack_bl_hits_by_tf = EXCLUDED.pack_bl_hits_by_tf,
                    used_path_by_tf = EXCLUDED.used_path_by_tf,
                    received_at = EXCLUDED.received_at,
                    finished_at = EXCLUDED.finished_at,
                    duration_ms = EXCLUDED.duration_ms,
                    decision_engine = EXCLUDED.decision_engine
                """,
                req_id,
                log_uid,
                int(strategy_id),
                client_strategy_id,
                direction,
                symbol,
                tfs_requested,
                decision_mode,
                oracle_version,
                bool(allow),
                reason or "",
                mw_js,
                pwl_js,
                pbl_js,
                upath_js,
                t_recv,
                t_fin,
                int(duration_ms),
            )

            # tf rows
            if tf_rows:
                args = []
                for tf, row in tf_rows:
                    tf_results_js = (
                        json.dumps(row["tf_results"], ensure_ascii=False, separators=(",", ":"))
                        if row.get("tf_results") is not None
                        else None
                    )
                    errors_js = (
                        json.dumps(row["errors"], ensure_ascii=False, separators=(",", ":"))
                        if row.get("errors") is not None
                        else None
                    )
                    args.append(
                        (
                            req_id,
                            tf,
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
                        )
                    )
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
                    args,
                )