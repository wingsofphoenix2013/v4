# laboratory_decision_filler.py — пост-allow наполнитель статистики (LPS): сидинг из SLE и допись по закрытию позиций

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Инфраструктура
import laboratory_infra as infra

# 🔸 Логгер
log = logging.getLogger("LAB_FILLER")

# 🔸 Стримы
DECISION_FILLER_STREAM = "laboratory_decision_filler"   # сидинг после allow=true
SIGNAL_LOG_QUEUE_STREAM = "signal_log_queue"            # внешняя шина: событие закрытия позиции

# 🔸 Параметры чтения стримов
XREAD_BLOCK_MS = 2000
XREAD_COUNT = 50


# 🔸 Утилиты

def _now_ms() -> int:
    return int(asyncio.get_running_loop().time() * 1000)


def _as_int(x: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return default


def _lower_str(x: Any) -> str:
    return str(x).strip().lower()


def _extract_stream_payload(fields: Dict[str, str]) -> Dict[str, Any]:
    """
    Поддерживает два формата:
      - плоские поля
      - {'data': '<json>'} или {'data':'{...}'}
    """
    payload: Dict[str, Any] = {}
    # базовая распаковка
    for k, v in fields.items():
        if isinstance(v, str) and v.startswith("{"):
            try:
                payload[k] = json.loads(v)
            except Exception:
                payload[k] = v
        else:
            payload[k] = v

    # если всё лежит под 'data' — разворачиваем
    if "data" in payload and isinstance(payload["data"], dict):
        payload = payload["data"]

    return payload


def _pack_family_from_base(pack_base: str) -> str:
    s = _lower_str(pack_base)
    if s.startswith("ema"):
        return "ema"
    if s.startswith("macd"):
        return "macd"
    if s.startswith("lr"):
        return "lr"
    if s.startswith("adx_dmi"):
        return "adx_dmi"
    if s.startswith("bb"):
        return "bb"
    if s.startswith("atr"):
        return "atr"
    if s.startswith("rsi"):
        return "rsi"
    if s.startswith("mfi"):
        return "mfi"
    return s.split("_", 1)[0] if "_" in s else s


def _match_pack_rule(rule: Dict[str, Any], pack_objs: Dict[str, Any]) -> bool:
    """
    Сопоставление PACK-правила с объектом:
      - rule['agg_key'] = "key1|key2"
      - rule['agg_value'] = "key1:val1|key2:val2" или "some_scalar" (solo с ключом)
      - сравнение выполняем как полное равенство факт-строки и agg_value (оба в lower)
    """
    base = _lower_str(rule.get("pack_base", ""))
    if not base:
        return False
    po = pack_objs.get(base) or {}
    pack = po.get("pack") or {}
    agg_key = _lower_str(rule.get("agg_key", ""))
    agg_val = _lower_str(rule.get("agg_value", ""))
    if not agg_key or not agg_val:
        return False

    keys = [k.strip() for k in agg_key.split("|") if k.strip()]
    parts: List[str] = []
    for k in keys:
        v = pack.get(k)
        if v is None:
            return False
        parts.append(f"{k}:{_lower_str(v)}")
    fact = "|".join(parts)
    return fact == agg_val


def _compute_pack_family_counts_for_matches(tf_pack: Dict[str, Any]) -> Dict[str, Dict[str, int]]:
    """
    Считает pack_family_counts по СОВПАВШИМ правилам (отдельно WL/BL):
      {"ema":{"wl":5,"bl":1}, "lr":{"wl":3,"bl":0}, ...}
    """
    rules: List[Dict[str, Any]] = (tf_pack or {}).get("rules") or []
    objs: Dict[str, Any] = (tf_pack or {}).get("objects") or {}
    out: Dict[str, Dict[str, int]] = {}
    if not rules or not objs:
        return out

    for r in rules:
        list_tag = _lower_str(r.get("list", ""))
        if list_tag not in ("whitelist", "blacklist"):
            continue
        matched = _match_pack_rule(r, objs)
        if not matched:
            continue
        fam = _pack_family_from_base(str(r.get("pack_base", "")))
        out.setdefault(fam, {"wl": 0, "bl": 0})
        if list_tag == "whitelist":
            out[fam]["wl"] += 1
        else:
            out[fam]["bl"] += 1

    return out


def _parse_tf_origin_map(s: Optional[str]) -> Dict[str, str]:
    """
    Принимает строку вида "m5:mw,m15:pack" → {"m5":"mw","m15":"pack"}
    """
    out: Dict[str, str] = {}
    if not s:
        return out
    for part in str(s).split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        tf, origin = part.split(":", 1)
        tf = _lower_str(tf)
        origin = _lower_str(origin)
        if tf in ("m5", "m15", "h1") and origin in ("mw", "pack"):
            out[tf] = origin
    return out


# 🔸 Сидинг LPS из SLE по одному allow-событию
async def _seed_lps_from_sle(
    req_id: str,
    log_uid: str,
    master_sid: int,
    client_sid_opt: Optional[int],
    decision_mode_opt: Optional[str],
    decision_tf_origins_opt: Optional[str],
) -> int:
    """
    Берёт строки из signal_laboratory_entries по (log_uid, strategy_id, [client_strategy_id]) и заводит/обновляет
    по одной строке на каждый TF в laboratoty_position_stat.
    Возвращает количество обработанных TF-строк.
    """
    tf_origin_map = _parse_tf_origin_map(decision_tf_origins_opt)

    async with infra.pg_pool.acquire() as conn:
        # читаем все TF-строки по конкретному запросу
        if client_sid_opt is not None:
            rows = await conn.fetch(
                """
                SELECT
                    strategy_id, client_strategy_id, symbol, direction,
                    timeframes_processed, tf_results,
                    mw_wl_match_count, pack_wl_match_count, pack_bl_match_count
                FROM signal_laboratory_entries
                WHERE log_uid = $1
                  AND strategy_id = $2
                  AND client_strategy_id = $3
                """,
                log_uid, master_sid, client_sid_opt
            )
        else:
            rows = await conn.fetch(
                """
                SELECT
                    strategy_id, client_strategy_id, symbol, direction,
                    timeframes_processed, tf_results,
                    mw_wl_match_count, pack_wl_match_count, pack_bl_match_count
                FROM signal_laboratory_entries
                WHERE log_uid = $1
                  AND strategy_id = $2
                  AND client_strategy_id IS NULL
                """,
                log_uid, master_sid
            )

        if not rows:
            log.info("[SEED] ⚠️ SLE не найден (log_uid=%s sid=%s csid=%s)", log_uid, master_sid, client_sid_opt or "-")
            return 0

        processed = 0
        for r in rows:
            tf = _lower_str(r["timeframes_processed"] or "")
            if tf not in ("m5", "m15", "h1"):
                continue

            symbol = str(r["symbol"]).upper()
            direction = _lower_str(r["direction"])
            mw_cnt = int(r["mw_wl_match_count"] or 0)
            pack_wl_cnt = int(r["pack_wl_match_count"] or 0)
            pack_bl_cnt = int(r["pack_bl_match_count"] or 0)

            # tf_results — одна TF-структура (мы писали по строке на TF)
            try:
                tf_obj = json.loads(r["tf_results"]) if r["tf_results"] else {}
            except Exception:
                tf_obj = {}

            mw_states = (tf_obj.get("mw") or {}).get("states")
            pack_family_counts = _compute_pack_family_counts_for_matches(tf_obj.get("pack") or {})

            # decision_origin по TF
            tf_origin = tf_origin_map.get(tf)
            if not tf_origin:
                # эвристика: если MW дал совпадение — "mw", иначе (allow=true гарантировал хотя бы одно подтверждение) → "pack"
                tf_origin = "mw" if mw_cnt >= 1 else ("pack" if pack_wl_cnt >= 1 else None)

            # upsert в LPS по уникальному ключу uq_lps_unique
            try:
                await conn.execute(
                    """
                    INSERT INTO laboratoty_position_stat (
                        log_uid, strategy_id, client_strategy_id,
                        symbol, direction, tf,
                        mw_states,
                        mw_match_count, pack_wl_match_count, pack_bl_match_count,
                        pack_family_counts,
                        decision_mode, decision_origin,
                        created_at, updated_at
                    ) VALUES (
                        $1, $2, $3,
                        $4, $5, $6,
                        COALESCE($7::jsonb, NULL),
                        $8, $9, $10,
                        COALESCE($11::jsonb, NULL),
                        $12, $13,
                        NOW(), NOW()
                    )
                    ON CONFLICT ON CONSTRAINT uq_lps_unique DO UPDATE SET
                        mw_states = COALESCE(EXCLUDED.mw_states, laboratoty_position_stat.mw_states),
                        mw_match_count = EXCLUDED.mw_match_count,
                        pack_wl_match_count = EXCLUDED.pack_wl_match_count,
                        pack_bl_match_count = EXCLUDED.pack_bl_match_count,
                        pack_family_counts = COALESCE(EXCLUDED.pack_family_counts, laboratoty_position_stat.pack_family_counts),
                        decision_mode = COALESCE(EXCLUDED.decision_mode, laboratoty_position_stat.decision_mode),
                        decision_origin = COALESCE(EXCLUDED.decision_origin, laboratoty_position_stat.decision_origin),
                        updated_at = NOW()
                    """,
                    log_uid, int(r["strategy_id"]), _as_int(r["client_strategy_id"]),
                    symbol, direction, tf,
                    json.dumps(mw_states, ensure_ascii=False) if isinstance(mw_states, dict) else None,
                    mw_cnt, pack_wl_cnt, pack_bl_cnt,
                    json.dumps(pack_family_counts, ensure_ascii=False) if pack_family_counts else None,
                    (decision_mode_opt or None),
                    (tf_origin or None),
                )
                processed += 1
            except Exception:
                log.exception("[SEED] ❌ ошибка upsert LPS (log_uid=%s tf=%s)", log_uid, tf)

        log.info(
            "[SEED] ✅ LPS upsert завершён: log_uid=%s sid=%s csid=%s rows=%d",
            log_uid, master_sid, client_sid_opt or "-", processed
        )
        return processed


# 🔸 Обработчик события allow=true из decision_maker
async def _handle_seed_message(msg_id: str, fields: Dict[str, str]):
    payload = _extract_stream_payload(fields)

    log_uid = payload.get("log_uid")
    req_id = payload.get("req_id")
    master_sid = _as_int(payload.get("strategy_id"))
    client_sid = _as_int(payload.get("client_strategy_id"))
    decision_mode = _lower_str(payload.get("decision_mode", "")) or None
    tf_origins = payload.get("decision_tf_origins")  # "m5:mw,m15:pack" (если передано)

    if not log_uid or not master_sid or not req_id:
        log.info("[SEED] ⚠️ пропуск msg=%s: недостаточно полей payload=%s", msg_id, payload)
        return

    try:
        await _seed_lps_from_sle(
            req_id=req_id,
            log_uid=str(log_uid),
            master_sid=int(master_sid),
            client_sid_opt=client_sid,
            decision_mode_opt=decision_mode,
            decision_tf_origins_opt=tf_origins,
        )
    except Exception:
        log.exception("[SEED] ❌ ошибка обработки seed-сообщения (log_uid=%s)", log_uid)


# 🔸 Обновление LPS по событию закрытия позиции
async def _handle_close_message(msg_id: str, fields: Dict[str, str]):
    payload = _extract_stream_payload(fields)

    if _lower_str(payload.get("status", "")) != "closed":
        return

    log_uid = payload.get("log_uid")
    position_uid = payload.get("position_uid")
    client_sid = _as_int(payload.get("strategy_id"))  # в этом стриме — SID зеркала!

    if not log_uid or not position_uid or client_sid is None:
        log.info("[CLOSE] ⚠️ пропуск msg=%s: нет log_uid/position_uid/strategy_id payload=%s", msg_id, payload)
        return

    # читаем позицию (уже обновлена внешним модулем)
    async with infra.pg_pool.acquire() as conn:
        pos = await conn.fetchrow(
            """
            SELECT position_uid, pnl, closed_at
            FROM positions_v4
            WHERE position_uid = $1
            """,
            position_uid
        )
        if not pos:
            log.info("[CLOSE] ⚠️ позиция не найдена position_uid=%s", position_uid)
            return

        pnl = pos["pnl"]
        closed_at = pos["closed_at"]
        # result: строго > 0
        result_flag = bool(pnl is not None and float(pnl) > 0.0)

        # апдейт всех TF-строк LPS по (log_uid, client_sid)
        try:
            status = await conn.execute(
                """
                UPDATE laboratoty_position_stat
                   SET position_uid = $1,
                       pnl = $2,
                       result = $3,
                       closed_at = $4,
                       updated_at = NOW()
                 WHERE log_uid = $5
                   AND client_strategy_id = $6
                """,
                position_uid, pnl, result_flag, closed_at, log_uid, client_sid
            )
            # status выглядит как "UPDATE <n>"
            updated = int(status.split()[-1]) if status.startswith("UPDATE") else 0
            log.info(
                "[CLOSE] ✅ LPS обновлён: log_uid=%s csid=%s pos=%s pnl=%s result=%s rows=%d",
                log_uid, client_sid, position_uid, str(pnl), str(result_flag).lower(), updated
            )
        except Exception:
            log.exception("[CLOSE] ❌ ошибка обновления LPS (log_uid=%s csid=%s)", log_uid, client_sid)


# 🔸 Главный слушатель: сидинг после allow=true
async def run_laboratory_decision_filler():
    """
    Слушает laboratory_decision_filler и на каждое allow=true событие создаёт/обновляет строки в laboratoty_position_stat
    по всем TF данного запроса (по данным из signal_laboratory_entries).
    """
    log.debug("🛰️ LAB_DECISION_FILLER слушатель запущен (BLOCK=%d COUNT=%d)", XREAD_BLOCK_MS, XREAD_COUNT)

    last_id = "$"
    redis = infra.redis_client

    while True:
        try:
            resp = await redis.xread(
                streams={DECISION_FILLER_STREAM: last_id},
                count=XREAD_COUNT,
                block=XREAD_BLOCK_MS
            )
            if not resp:
                continue

            for _, messages in resp:
                for msg_id, fields in messages:
                    last_id = msg_id
                    try:
                        await _handle_seed_message(msg_id, fields)
                    except Exception:
                        log.exception("❌ Ошибка seed-сообщения msg_id=%s", msg_id)

        except asyncio.CancelledError:
            log.debug("⏹️ LAB_DECISION_FILLER остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ LAB_DECISION_FILLER ошибка цикла")
            await asyncio.sleep(1.0)


# 🔸 Слушатель закрытия позиций: дополняет LPS pnl/result/closed_at/position_uid
async def run_position_close_updater():
    """
    Слушает signal_log_queue. На событиях со status='closed' подтягивает из positions_v4 PnL/closed_at
    и дописывает их в laboratoty_position_stat (по log_uid + client_strategy_id).
    """
    log.debug("🛰️ LAB_POS_CLOSE_FILLER слушатель запущен (BLOCK=%d COUNT=%d)", XREAD_BLOCK_MS, XREAD_COUNT)

    last_id = "$"
    redis = infra.redis_client

    while True:
        try:
            resp = await redis.xread(
                streams={SIGNAL_LOG_QUEUE_STREAM: last_id},
                count=XREAD_COUNT,
                block=XREAD_BLOCK_MS
            )
            if not resp:
                continue

            for _, messages in resp:
                for msg_id, fields in messages:
                    last_id = msg_id
                    try:
                        await _handle_close_message(msg_id, fields)
                    except Exception:
                        log.exception("❌ Ошибка close-сообщения msg_id=%s", msg_id)

        except asyncio.CancelledError:
            log.debug("⏹️ LAB_POS_CLOSE_FILLER остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ LAB_POS_CLOSE_FILLER ошибка цикла")
            await asyncio.sleep(1.0)