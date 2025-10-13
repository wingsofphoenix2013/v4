# 🔸 laboratory_postproc.py — пост-процессинг закрытых позиций: читает signal_log_queue, сопоставляет с лабораторией, пишет в laboratory_positions_stat

# 🔸 Импорты
import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import laboratory_infra as infra

# 🔸 Логгер
log = logging.getLogger("LAB_POSTPROC")

# 🔸 Константы стрима/группы
SOURCE_STREAM = "signal_log_queue"
CONSUMER_GROUP = "LAB_POSTPROC_GROUP"
CONSUMER_NAME = "LAB_POSTPROC_WORKER"

# 🔸 Параллелизм/чтение
MAX_CONCURRENCY = 16
READ_COUNT = 128
READ_BLOCK_MS = 30_000

# 🔸 Доп. константы/проверки
ALLOWED_TFS = ("m5", "m15", "h1")


# 🔸 Публичная точка входа воркера
async def run_laboratory_postproc():
    # условия достаточности
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск LAB_POSTPROC: PG/Redis не инициализированы")
        return

    # создать consumer group (идемпотентно)
    try:
        await infra.redis_client.xgroup_create(
            name=SOURCE_STREAM,
            groupname=CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug("📡 LAB_POSTPROC: создана consumer group для %s", SOURCE_STREAM)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            log.exception("❌ LAB_POSTPROC: ошибка создания consumer group")
            return

    log.debug("🚀 LAB_POSTPROC: старт воркера")

    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    # основной цикл
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=CONSUMER_NAME,
                streams={SOURCE_STREAM: ">"},
                count=READ_COUNT,
                block=READ_BLOCK_MS,
            )
            if not resp:
                continue

            tasks = []
            msg_ids: List[str] = []

            for _, msgs in resp:
                for msg_id, fields in msgs:
                    msg_ids.append(msg_id)
                    tasks.append(_process_message_guard(sem, msg_id, fields))

            if tasks:
                await asyncio.gather(*tasks)

        except asyncio.CancelledError:
            log.debug("⏹️ LAB_POSTPROC: остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ LAB_POSTPROC: ошибка цикла — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Гард обработки одного сообщения
async def _process_message_guard(sem: asyncio.Semaphore, msg_id: str, fields: Dict[str, str]):
    async with sem:
        try:
            await _process_message(msg_id, fields)
        except Exception:
            log.exception("❌ LAB_POSTPROC: сбой обработки сообщения (id=%s)", msg_id)
        finally:
            await _ack_safe(msg_id)


# 🔸 ACK безопасно
async def _ack_safe(msg_id: str):
    try:
        await infra.redis_client.xack(SOURCE_STREAM, CONSUMER_GROUP, msg_id)
    except Exception:
        log.exception("⚠️ LAB_POSTPROC: ошибка ACK (id=%s)", msg_id)


# 🔸 Основная логика обработки сообщения
async def _process_message(msg_id: str, fields: Dict[str, str]):
    # ожидаем плоские поля (не JSON): log_uid, strategy_id, status, position_uid, note, logged_at
    status = str(fields.get("status") or "").lower().strip()
    if status != "closed":
        return

    log_uid = (fields.get("log_uid") or "").strip()
    client_sid_str = (fields.get("strategy_id") or "").strip()
    position_uid = (fields.get("position_uid") or "").strip()

    if not log_uid or not client_sid_str or not position_uid:
        log.debug("ℹ️ LAB_POSTPROC: пропуск (нехватает полей) id=%s payload=%s", msg_id, fields)
        return

    try:
        client_sid = int(client_sid_str)
    except Exception:
        log.debug("ℹ️ LAB_POSTPROC: неверный strategy_id=%r (id=%s)", client_sid_str, msg_id)
        return

    # проверка: стратегия не мастер (market_watcher=false)
    if not await _is_non_master_strategy(client_sid):
        # не интересует — ACK уже будет в гарде
        return

    # подтянем позицию (символ/направление/pnl/closed_at)
    pos = await _load_position(position_uid)
    if pos is None:
        # позиция не найдена — логируем и выходим
        log.debug("ℹ️ LAB_POSTPROC: позиция не найдена (position_uid=%s)", position_uid)
        return

    symbol, direction, pnl, closed_at = pos

    # находим связанный лабораторный запрос по log_uid + client_strategy_id
    head = await _find_lab_head(log_uid, client_sid)
    if head is None:
        # не наше событие — пропускаем без ретраев
        log.debug("ℹ️ LAB_POSTPROC: head не найден (log_uid=%s, client_sid=%s)", log_uid, client_sid)
        return

    # head: req_id, master_sid, oracle_version, decision_mode, timeframes_requested, allow
    req_id, master_sid, oracle_version, decision_mode, tfs_requested, head_allow = head

    # строк на каждую TF из head.timeframes_requested (если tf не в списке — не пишем)
    tfs = _parse_timeframes(tfs_requested)

    # подтянем TF-строки
    tf_rows = await _load_lab_tf_rows(req_id, tfs)
    if not tf_rows:
        log.debug("ℹ️ LAB_POSTPROC: TF-строки не найдены (req_id=%s)", req_id)

    # подготовим и запишем статистику по каждой TF
    inserted = 0
    updated = 0

    async with infra.pg_pool.acquire() as conn:
        # вставляем/обновляем по одной строке на TF
        for tf in tfs:
            tfrow = tf_rows.get(tf)
            if not tfrow:
                continue

            mw_hits = int(tfrow.get("mw_wl_hits") or 0)
            pack_wl_hits = int(tfrow.get("pack_wl_hits") or 0)
            pack_bl_hits = int(tfrow.get("pack_bl_hits") or 0)
            path_used = str(tfrow.get("path_used") or "none")

            tf_results = tfrow.get("tf_results") or {}
            mw_matches = list(tf_results.get("mw", {}).get("wl_matches", []))
            pack_wl_matches = list(tf_results.get("pack", {}).get("wl_matches", []))
            pack_bl_matches = list(tf_results.get("pack", {}).get("bl_matches", []))

            wl_families = _build_pack_families(pack_wl_matches)
            bl_families = _build_pack_families(pack_bl_matches)

            status_label = "win" if (pnl or 0) > 0 else "loose"

            res = await conn.execute(
                """
                INSERT INTO laboratory_positions_stat (
                    log_uid, strategy_id, client_strategy_id,
                    position_uid, symbol, direction, tf,
                    oracle_version, decision_mode, decision_origin,
                    mw_match_count, mw_matches,
                    pack_wl_match_count, pack_wl_matches, pack_wl_families,
                    pack_bl_match_count, pack_bl_matches, pack_bl_families,
                    pnl, status, closed_at, created_at, updated_at
                )
                VALUES (
                    $1,$2,$3,
                    $4,$5,$6,$7,
                    $8,$9,$10,
                    $11,$12::jsonb,
                    $13,$14::jsonb,$15::jsonb,
                    $16,$17::jsonb,$18::jsonb,
                    $19,$20,$21, now(), now()
                )
                ON CONFLICT (position_uid, tf) DO UPDATE SET
                    strategy_id = EXCLUDED.strategy_id,
                    client_strategy_id = EXCLUDED.client_strategy_id,
                    symbol = EXCLUDED.symbol,
                    direction = EXCLUDED.direction,
                    oracle_version = EXCLUDED.oracle_version,
                    decision_mode = EXCLUDED.decision_mode,
                    decision_origin = EXCLUDED.decision_origin,
                    mw_match_count = EXCLUDED.mw_match_count,
                    mw_matches = EXCLUDED.mw_matches,
                    pack_wl_match_count = EXCLUDED.pack_wl_match_count,
                    pack_wl_matches = EXCLUDED.pack_wl_matches,
                    pack_wl_families = EXCLUDED.pack_wl_families,
                    pack_bl_match_count = EXCLUDED.pack_bl_match_count,
                    pack_bl_matches = EXCLUDED.pack_bl_matches,
                    pack_bl_families = EXCLUDED.pack_bl_families,
                    pnl = EXCLUDED.pnl,
                    status = EXCLUDED.status,
                    closed_at = EXCLUDED.closed_at,
                    updated_at = now()
                """,
                # values
                log_uid, int(master_sid), int(client_sid),
                position_uid, symbol, direction, tf,
                oracle_version, decision_mode, path_used,
                mw_hits, json.dumps(mw_matches, ensure_ascii=False, separators=(",", ":")),
                pack_wl_hits, json.dumps(pack_wl_matches, ensure_ascii=False, separators=(",", ":")), json.dumps(wl_families, separators=(",", ":")),
                pack_bl_hits, json.dumps(pack_bl_matches, ensure_ascii=False, separators=(",", ":")), json.dumps(bl_families, separators=(",", ":")),
                float(pnl or 0), status_label, closed_at,
            )
            if str(res).startswith("INSERT"):
                inserted += 1
            else:
                updated += 1

    # итоговые логи по событию
    log.debug(
        "LAB_POSTPROC: closed position stored (log_uid=%s, client_sid=%s, pos=%s, tfs=%s) -> inserted=%d updated=%d",
        log_uid, client_sid, position_uid, ",".join(tfs), inserted, updated
    )


# условия достаточности
def _parse_timeframes(tfs: str) -> List[str]:
    seen = set()
    out: List[str] = []
    for tf in (tfs or "").split(","):
        tf = tf.strip().lower()
        if tf in ALLOWED_TFS and tf not in seen:
            out.append(tf)
            seen.add(tf)
    return out


# проверка стратегии: не мастер (market_watcher=false)
async def _is_non_master_strategy(client_sid: int) -> bool:
    async with infra.pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT COALESCE(market_watcher, false) AS mw FROM strategies_v4 WHERE id = $1",
            int(client_sid)
        )
    if not row:
        return False
    return (not bool(row["mw"]))


# загрузка позиции
async def _load_position(position_uid: str) -> Optional[Tuple[str, str, float, Optional[datetime]]]:
    async with infra.pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT symbol, direction, COALESCE(pnl,0) AS pnl, closed_at
            FROM positions_v4
            WHERE position_uid = $1
            """,
            position_uid
        )
    if not row:
        return None
    symbol = str(row["symbol"])
    direction = str(row["direction"])
    pnl = float(row["pnl"] or 0.0)
    closed_at = row["closed_at"]
    return symbol, direction, pnl, closed_at


# поиск заголовка лаборатории по log_uid + client_sid
async def _find_lab_head(log_uid: str, client_sid: int) -> Optional[Tuple[str, int, str, str, str, bool]]:
    async with infra.pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
              req_id, strategy_id, oracle_version, decision_mode, timeframes_requested, allow
            FROM laboratory_request_head
            WHERE log_uid = $1 AND client_strategy_id = $2
            ORDER BY finished_at DESC
            LIMIT 1
            """,
            log_uid, int(client_sid)
        )
    if not row:
        return None
    return (
        str(row["req_id"]),
        int(row["strategy_id"]),
        str(row["oracle_version"]),
        str(row["decision_mode"]),
        str(row["timeframes_requested"]),
        bool(row["allow"]),
    )


# загрузка TF-строк по req_id
async def _load_lab_tf_rows(req_id: str, tfs: List[str]) -> Dict[str, Dict]:
    if not tfs:
        return {}
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT tf,
                   mw_wl_hits,
                   pack_wl_hits,
                   pack_bl_hits,
                   path_used,
                   tf_results
            FROM laboratory_request_tf
            WHERE req_id = $1
              AND tf = ANY($2::text[])
            """,
            req_id, tfs
        )
    res: Dict[str, Dict] = {}
    for r in rows:
        tf = str(r["tf"])
        res[tf] = {
            "mw_wl_hits": int(r["mw_wl_hits"] or 0),
            "pack_wl_hits": int(r["pack_wl_hits"] or 0),
            "pack_bl_hits": int(r["pack_bl_hits"] or 0),
            "path_used": str(r["path_used"] or "none"),
            "tf_results": (dict(r["tf_results"]) if r["tf_results"] is not None else {}),
        }
    return res


# построение семейств/баз из списка матчей PACK
def _build_pack_families(matches: List[Dict]) -> Dict:
    # ожидается список объектов {"pack_base": "...", ...}
    families_count: Dict[str, int] = {}
    bases_count: Dict[str, int] = {}
    for m in (matches or []):
        base = str(m.get("pack_base") or "")
        if not base:
            continue
        fam = _pack_family_from_base(base)
        families_count[fam] = families_count.get(fam, 0) + 1
        bases_count[base] = bases_count.get(base, 0) + 1
    return {"families": families_count, "bases": bases_count}


def _pack_family_from_base(pack_base: str) -> str:
    # bb20_2_0  -> bb
    # adx_dmi21 -> adx_dmi
    # rsi14     -> rsi
    s = pack_base.strip().lower()
    if s.startswith("bb"):
        return "bb"
    if s.startswith("adx_dmi"):
        return "adx_dmi"
    # остальное — первые буквы до цифры/подчёркивания
    out = []
    for ch in s:
        if ch.isalpha():
            out.append(ch)
        else:
            break
    return "".join(out) or s