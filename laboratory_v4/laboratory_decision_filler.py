# laboratory_decision_filler.py — пост-allow наполнитель LPS (c версиями oracle v1/v2) и (заглушка) апдейтер закрытий

# 🔸 Импорты
import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Инфраструктура
import laboratory_infra as infra

# 🔸 Логгеры
log = logging.getLogger("LAB_DECISION_FILLER")
log_pos = logging.getLogger("LAB_POS_CLOSE_FILLER")

# 🔸 Константы Streams/таблиц
DECISION_FILLER_STREAM = "laboratory_decision_filler"  # seed: {req_id, log_uid}
SLE_TABLE = "public.signal_laboratory_entries"
LPS_TABLE = "public.laboratoty_position_stat"

# 🔸 Параметры чтения Streams
XREAD_BLOCK_MS = 1_000
XREAD_COUNT = 50


# 🔸 Утилиты JSON (совместимо со стилем maker)
def _to_json_safe(obj: Any) -> Any:
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, datetime):
        try:
            return obj.replace(tzinfo=None).isoformat()
        except Exception:
            return str(obj)
    if isinstance(obj, dict):
        return {k: _to_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_json_safe(v) for v in obj]
    return str(obj)


# 🔸 Парсинг PACK-базы для семейства (совместимо с maker)
def _parse_pack_base_family(base: str) -> str:
    s = (base or "").strip().lower()
    if not s:
        return ""
    if s.startswith("bb"):
        return "bb"
    if s.startswith("macd"):
        return "macd"
    if s.startswith("adx_dmi"):
        return "adx_dmi"
    if s.startswith("ema"):
        return "ema"
    if s.startswith("rsi"):
        return "rsi"
    if s.startswith("mfi"):
        return "mfi"
    if s.startswith("lr"):
        return "lr"
    if s.startswith("atr"):
        return "atr"
    return s  # fallback


# 🔸 Построение факта MW по agg_base (совместимо с maker)
def _build_mw_fact(states: Dict[str, Any], agg_base: str) -> Optional[str]:
    if not agg_base:
        return None
    parts: List[str] = []
    for base in agg_base.strip().lower().split("_"):
        node = (states or {}).get(base) or {}
        pack = node.get("pack") or {}
        st = pack.get("state")
        if not isinstance(st, str) or not st:
            return None
        parts.append(f"{base}:{st.strip().lower()}")
    return "|".join(parts)


# 🔸 Построение факта PACK по agg_key (совместимо с maker)
def _build_pack_fact(pack_obj: Dict[str, Any], agg_key: str) -> Optional[str]:
    if not agg_key:
        return None
    payload = (pack_obj or {}).get("pack") or {}
    keys = [k.strip() for k in agg_key.strip().lower().split("|") if k.strip()]
    parts: List[str] = []
    for k in keys:
        v = payload.get(k)
        if v is None:
            return None
        parts.append(f"{k}:{str(v).strip().lower()}")
    return "|".join(parts)


# 🔸 Безопасный доступ к tf_results (jsonb → dict)
def _as_dict(maybe_json) -> Dict[str, Any]:
    if maybe_json is None:
        return {}
    if isinstance(maybe_json, dict):
        return maybe_json
    if isinstance(maybe_json, str):
        try:
            return json.loads(maybe_json)
        except Exception:
            return {}
    return {}


# 🔸 Извлечение и расчёт полей LPS из одной строки SLE (включая oracle_version)
def _extract_lps_from_sle_row(row: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    # базовые поля
    log_uid: str = row["log_uid"]
    strategy_id: int = int(row["strategy_id"])
    client_strategy_id = row.get("client_strategy_id")
    symbol: str = row["symbol"]
    direction: str = row["direction"]
    tf: str = row["tf"]

    # tf_results
    tfres = _as_dict(row.get("tf_results"))
    mw = _as_dict(tfres.get("mw"))
    pack = _as_dict(tfres.get("pack"))
    mw_states = _as_dict(mw.get("states"))
    mw_rules: List[Dict[str, Any]] = list(mw.get("rules") or [])

    pack_objs: Dict[str, Any] = _as_dict(pack.get("objects"))
    pack_rules: List[Dict[str, Any]] = list(pack.get("rules") or [])

    # counters (копируем как есть)
    counters = _as_dict(tfres.get("counters"))
    mw_match_count = int(counters.get("mw_wl_hits") or 0)
    pack_wl_match_count = int(counters.get("pack_wl_hits") or 0)
    pack_bl_match_count = int(counters.get("pack_bl_hits") or 0)

    # decision.{mode,origin,version} (мы уже пишем их в maker)
    decision = _as_dict(tfres.get("decision"))
    decision_mode = decision.get("mode")
    decision_origin = decision.get("origin")
    # версия: колонка SLE приоритетна, иначе из tf_results.decision.version
    oracle_version = (row.get("oracle_version") or decision.get("version") or "v1")
    oracle_version = str(oracle_version).strip().lower()

    # расчёт совпавших правил (без внешних запросов)
    mw_matches: List[Dict[str, Any]] = []
    for r in mw_rules:
        agg_base = str(r.get("agg_base") or "").strip().lower()
        agg_state = str(r.get("agg_state") or "").strip().lower()
        if not agg_base or not agg_state:
            continue
        fact = _build_mw_fact(mw_states, agg_base)
        if fact is not None and fact == agg_state:
            mw_matches.append(r)

    pack_wl_matches: List[Dict[str, Any]] = []
    pack_bl_matches: List[Dict[str, Any]] = []
    pack_family_counts: Dict[str, Dict[str, int]] = {}

    for r in pack_rules:
        list_type = str(r.get("list") or "").strip().lower()
        base = str(r.get("pack_base") or "").strip().lower()
        agg_key = str(r.get("agg_key") or "").strip().lower()
        agg_val = str(r.get("agg_value") or "").strip().lower()
        if not base or not agg_key or not agg_val or list_type not in ("whitelist", "blacklist"):
            continue
        pack_obj = _as_dict(pack_objs.get(base))
        if not pack_obj:
            continue
        fact = _build_pack_fact(pack_obj, agg_key)
        if fact is None or fact != agg_val:
            continue
        # матч по типу списка
        if list_type == "whitelist":
            pack_wl_matches.append(r)
        else:
            pack_bl_matches.append(r)

        # агрегаты по семейству
        fam = _parse_pack_base_family(base)
        fam_stat = pack_family_counts.setdefault(fam, {"wl": 0, "bl": 0})
        if list_type == "whitelist":
            fam_stat["wl"] += 1
        else:
            fam_stat["bl"] += 1

    # итоговая мапа для UPSERT
    lps_values = {
        "log_uid": log_uid,
        "strategy_id": strategy_id,
        "client_strategy_id": int(client_strategy_id) if client_strategy_id is not None else None,
        "symbol": symbol,
        "direction": direction,
        "tf": tf,
        "mw_states": json.dumps(_to_json_safe(mw_states), ensure_ascii=False),
        "mw_matches": json.dumps(_to_json_safe(mw_matches), ensure_ascii=False),
        "pack_wl_matches": json.dumps(_to_json_safe(pack_wl_matches), ensure_ascii=False),
        "pack_bl_matches": json.dumps(_to_json_safe(pack_bl_matches), ensure_ascii=False),
        "mw_match_count": mw_match_count if mw_match_count is not None else len(mw_matches),
        "pack_wl_match_count": pack_wl_match_count if pack_wl_match_count is not None else len(pack_wl_matches),
        "pack_bl_match_count": pack_bl_match_count if pack_bl_match_count is not None else len(pack_bl_matches),
        "pack_family_counts": json.dumps(_to_json_safe(pack_family_counts), ensure_ascii=False),
        "decision_mode": decision_mode,
        "decision_origin": decision_origin,
        "oracle_version": oracle_version,
    }

    # ключ для логов
    key = {
        "log_uid": log_uid,
        "sid": strategy_id,
        "csid": int(client_strategy_id) if client_strategy_id is not None else None,
        "symbol": symbol,
        "tf": tf,
    }
    return lps_values, key


# 🔸 UPSERT одной TF-строки в LPS (UPDATE → INSERT DO NOTHING → UPDATE), c oracle_version
async def _upsert_lps(conn, lps: Dict[str, Any]) -> str:
    # update (сохранить значения полей закрытия, не трогать их)
    upd_status = await conn.execute(
        f"""
        UPDATE {LPS_TABLE}
           SET symbol=$1,
               direction=$2,
               mw_states=COALESCE($3::jsonb, mw_states),
               mw_matches=COALESCE($4::jsonb, mw_matches),
               pack_wl_matches=COALESCE($5::jsonb, pack_wl_matches),
               pack_bl_matches=COALESCE($6::jsonb, pack_bl_matches),
               mw_match_count=$7,
               pack_wl_match_count=$8,
               pack_bl_match_count=$9,
               pack_family_counts=COALESCE($10::jsonb, pack_family_counts),
               decision_mode=$11,
               decision_origin=$12,
               oracle_version=$17,
               updated_at=now()
         WHERE log_uid=$13
           AND strategy_id=$14
           AND ((client_strategy_id IS NULL AND $15::int IS NULL) OR client_strategy_id=$15::int)
           AND tf=$16
           AND oracle_version=$17
        """,
        lps["symbol"],
        lps["direction"],
        lps["mw_states"],
        lps["mw_matches"],
        lps["pack_wl_matches"],
        lps["pack_bl_matches"],
        lps["mw_match_count"],
        lps["pack_wl_match_count"],
        lps["pack_bl_match_count"],
        lps["pack_family_counts"],
        lps["decision_mode"],
        lps["decision_origin"],
        lps["log_uid"],
        lps["strategy_id"],
        lps["client_strategy_id"],
        lps["tf"],
        lps["oracle_version"],
    )
    if upd_status.startswith("UPDATE 1"):
        return "updated"

    # insert (не задаём position_uid/pnl/result/closed_at)
    ins_status = await conn.execute(
        f"""
        INSERT INTO {LPS_TABLE}
            (log_uid, strategy_id, client_strategy_id, symbol, direction, tf,
             mw_states, mw_matches, pack_wl_matches, pack_bl_matches,
             mw_match_count, pack_wl_match_count, pack_bl_match_count,
             pack_family_counts, decision_mode, decision_origin, oracle_version)
        VALUES ($1,$2,$3,$4,$5,$6,
                COALESCE($7::jsonb,NULL), COALESCE($8::jsonb,NULL), COALESCE($9::jsonb,NULL), COALESCE($10::jsonb,NULL),
                $11,$12,$13,
                COALESCE($14::jsonb,NULL), $15, $16, $17)
        ON CONFLICT (log_uid, strategy_id, (COALESCE(client_strategy_id, '-1'::integer)), tf, oracle_version) DO NOTHING
        """,
        lps["log_uid"],
        lps["strategy_id"],
        lps["client_strategy_id"],
        lps["symbol"],
        lps["direction"],
        lps["tf"],
        lps["mw_states"],
        lps["mw_matches"],
        lps["pack_wl_matches"],
        lps["pack_bl_matches"],
        lps["mw_match_count"],
        lps["pack_wl_match_count"],
        lps["pack_bl_match_count"],
        lps["pack_family_counts"],
        lps["decision_mode"],
        lps["decision_origin"],
        lps["oracle_version"],
    )
    if ins_status.endswith(" 1"):
        return "inserted"

    # race: update again
    await conn.execute(
        f"""
        UPDATE {LPS_TABLE}
           SET symbol=$1,
               direction=$2,
               mw_states=COALESCE($3::jsonb, mw_states),
               mw_matches=COALESCE($4::jsonb, mw_matches),
               pack_wl_matches=COALESCE($5::jsonb, pack_wl_matches),
               pack_bl_matches=COALESCE($6::jsonb, pack_bl_matches),
               mw_match_count=$7,
               pack_wl_match_count=$8,
               pack_bl_match_count=$9,
               pack_family_counts=COALESCE($10::jsonb, pack_family_counts),
               decision_mode=$11,
               decision_origin=$12,
               oracle_version=$17,
               updated_at=now()
         WHERE log_uid=$13
           AND strategy_id=$14
           AND ((client_strategy_id IS NULL AND $15::int IS NULL) OR client_strategy_id=$15::int)
           AND tf=$16
           AND oracle_version=$17
        """,
        lps["symbol"],
        lps["direction"],
        lps["mw_states"],
        lps["mw_matches"],
        lps["pack_wl_matches"],
        lps["pack_bl_matches"],
        lps["mw_match_count"],
        lps["pack_wl_match_count"],
        lps["pack_bl_match_count"],
        lps["pack_family_counts"],
        lps["decision_mode"],
        lps["decision_origin"],
        lps["log_uid"],
        lps["strategy_id"],
        lps["client_strategy_id"],
        lps["tf"],
        lps["oracle_version"],
    )
    return "updated"


# 🔸 Обработка одного seed (req_id, log_uid) → перенос в LPS
async def _process_seed(req_id: str, log_uid: str):
    # логичное предположение: у одного запроса все строки SLE имеют один sid/csid/symbol
    inserted = 0
    updated = 0
    tfs: List[str] = []
    sid: Optional[int] = None
    csid: Optional[int] = None
    symbol: Optional[str] = None

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT log_uid, strategy_id, client_strategy_id, symbol, direction, tf, tf_results, oracle_version
              FROM {SLE_TABLE}
             WHERE req_id=$1 AND allow=true
            """,
            req_id,
        )
        if not rows:
            log.debug("[LPS] ⚠️ нет строк SLE для req_id=%s log_uid=%s (allow=true)", req_id, log_uid)
            return

        for r in rows:
            lps_values, key = _extract_lps_from_sle_row(dict(r))
            res = await _upsert_lps(conn, lps_values)
            if res == "inserted":
                inserted += 1
            else:
                updated += 1

            # сбор агрегатов для лога
            sid = sid or key["sid"]
            csid = csid if csid is not None else key["csid"]
            symbol = symbol or key["symbol"]
            tfs.append(key["tf"])

    # лог результата
    uniq_tfs = ",".join(sorted(set(tfs), key=lambda x: ["m5", "m15", "h1"].index(x) if x in ("m5", "m15", "h1") else 9))
    log.debug(
        "[LPS] ✅ filled req_id=%s log_uid=%s sid=%s csid=%s %s tfs=[%s] ins=%d upd=%d",
        req_id,
        log_uid,
        (sid if sid is not None else "-"),
        (csid if csid is not None else "-"),
        (symbol or "-"),
        uniq_tfs,
        inserted,
        updated,
    )


# 🔸 Главный слушатель seed → перенос SLE → LPS
async def run_laboratory_decision_filler():
    """
    Слушает laboratory_decision_filler и для каждого seed {req_id, log_uid}
    переносит готовые данные из signal_laboratory_entries в laboratoty_position_stat.
    Не запрашивает on-demand никакие индикаторы. Учитывает oracle_version (v1/v2).
    """
    log.debug("🛰️ LAB_DECISION_FILLER слушатель запущен (BLOCK=%d COUNT=%d)", XREAD_BLOCK_MS, XREAD_COUNT)

    last_id = "$"  # только новые
    redis = infra.redis_client

    while True:
        try:
            resp = await redis.xread(
                streams={DECISION_FILLER_STREAM: last_id},
                count=XREAD_COUNT,
                block=XREAD_BLOCK_MS,
            )
            if not resp:
                continue

            for _, messages in resp:
                for msg_id, fields in messages:
                    last_id = msg_id
                    # распаковка полей
                    req_id = (fields.get("req_id") or "").strip()
                    log_uid = (fields.get("log_uid") or "").strip()
                    if not req_id or not log_uid:
                        # вложенный формат с key 'data' (на всякий случай)
                        raw = fields.get("data")
                        if raw and isinstance(raw, str):
                            try:
                                data = json.loads(raw)
                                req_id = (data.get("req_id") or req_id).strip()
                                log_uid = (data.get("log_uid") or log_uid).strip()
                            except Exception:
                                pass
                    if not req_id or not log_uid:
                        log.debug("[LPS] ⚠️ пропуск seed msg=%s: неполные поля: %s", msg_id, fields)
                        continue

                    # обработка seed
                    try:
                        await _process_seed(req_id=req_id, log_uid=log_uid)
                    except Exception:
                        log.exception("[LPS] ❌ ошибка обработки seed req_id=%s log_uid=%s", req_id, log_uid)

        except asyncio.CancelledError:
            log.debug("⏹️ LAB_DECISION_FILLER остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ LAB_DECISION_FILLER ошибка в основном цикле")
            await asyncio.sleep(1.0)