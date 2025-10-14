# 🔸 laboratory_bl_analyzer.py — анализатор BL-порогов: расчёт лучшего T по ROI и поддержка in-memory кэша активных порогов

# 🔸 Импорты
import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

import laboratory_infra as infra

# 🔸 Логгер
log = logging.getLogger("LAB_BL_ANALYZER")

# 🔸 Параметры воркера
INITIAL_DELAY_SEC = 60                  # задержка перед первым запуском
MAX_CONCURRENCY_CLIENTS = 8            # параллельная обработка клиентских стратегий
READ_COUNT = 128
READ_BLOCK_MS = 30_000

# 🔸 Стрим, который триггерит пересчёт (обновление PACK WL/BL в oracle)
PACK_LISTS_READY_STREAM = "oracle:pack_lists:reports_ready"
BL_CONSUMER_GROUP = "LAB_BL_ANALYZER_GROUP"
BL_CONSUMER_NAME = "LAB_BL_ANALYZER_WORKER"

# 🔸 Константы домена
ALLOWED_TFS = ("m5", "m15", "h1")
DECISION_MODES = ("mw_only", "mw_then_pack", "mw_and_pack", "pack_only")
DIRECTIONS = ("long", "short")
VERSIONS = ("v1", "v2")


# 🔸 Публичная точка входа воркера
async def run_laboratory_bl_analyzer():
    # условия достаточности окружения
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск LAB_BL_ANALYZER: PG/Redis не инициализированы")
        return

    # стартовая задержка
    if INITIAL_DELAY_SEC > 0:
        log.info("⏳ LAB_BL_ANALYZER: ожидание %d сек перед стартом", INITIAL_DELAY_SEC)
        await asyncio.sleep(INITIAL_DELAY_SEC)

    # загрузка активных порогов из БД в память
    await _load_active_from_db()

    # полный пересчёт по всем релевантным клиентам
    await _recompute_all_clients()

    # подписка на стрим oracle:pack_lists:reports_ready
    try:
        await infra.redis_client.xgroup_create(
            name=PACK_LISTS_READY_STREAM,
            groupname=BL_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug("📡 LAB_BL_ANALYZER: создана consumer group для %s", PACK_LISTS_READY_STREAM)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            log.exception("❌ LAB_BL_ANALYZER: ошибка создания consumer group")
            return

    log.info("🚀 LAB_BL_ANALYZER: слушаю %s", PACK_LISTS_READY_STREAM)

    # основной цикл (реакция на таргетные обновления)
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=BL_CONSUMER_GROUP,
                consumername=BL_CONSUMER_NAME,
                streams={PACK_LISTS_READY_STREAM: ">"},
                count=READ_COUNT,
                block=READ_BLOCK_MS,
            )
            if not resp:
                continue

            acks: List[str] = []
            for _, msgs in resp:
                for msg_id, fields in msgs:
                    try:
                        payload = json.loads(fields.get("data", "{}"))
                        master_sid = int(payload.get("strategy_id", 0))
                        version = str(payload.get("version", "")).lower()
                        if master_sid > 0 and version in VERSIONS:
                            await _recompute_by_master_and_version(master_sid, version)
                        else:
                            log.debug("ℹ️ LAB_BL_ANALYZER: пропуск payload=%s", payload)
                        acks.append(msg_id)
                    except Exception:
                        log.exception("❌ LAB_BL_ANALYZER: ошибка обработки сообщения")
                        acks.append(msg_id)

            if acks:
                try:
                    await infra.redis_client.xack(PACK_LISTS_READY_STREAM, BL_CONSUMER_GROUP, *acks)
                except Exception:
                    log.exception("⚠️ LAB_BL_ANALYZER: ошибка ACK")

        except asyncio.CancelledError:
            log.debug("⏹️ LAB_BL_ANALYZER: остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ LAB_BL_ANALYZER: ошибка цикла — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Стартовая загрузка активных порогов из БД в память
async def _load_active_from_db():
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT master_strategy_id, oracle_version, decision_mode, direction, tf,
                   best_threshold, best_roi, roi_base, positions_total, deposit_used,
                   computed_at
            FROM laboratory_bl_active
            """
        )
    m: Dict[Tuple[int, str, str, str, str], Dict] = {}
    for r in rows:
        key = (int(r["master_strategy_id"]), str(r["oracle_version"]), str(r["decision_mode"]),
               str(r["direction"]), str(r["tf"]))
        m[key] = {
            "threshold": int(r["best_threshold"] or 0),
            "best_roi": float(r["best_roi"] or 0.0),
            "roi_base": float(r["roi_base"] or 0.0),
            "positions_total": int(r["positions_total"] or 0),
            "deposit_used": float(r["deposit_used"] or 0.0),
            "computed_at": (r["computed_at"].isoformat() if r["computed_at"] else ""),
        }
    infra.set_bl_active_bulk(m)


# 🔸 Полный пересчёт по всем клиентским стратегиям
async def _recompute_all_clients():
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id AS client_sid, COALESCE(market_mirrow,0) AS master_sid, COALESCE(deposit,0) AS deposit
            FROM strategies_v4
            WHERE enabled = true AND (archived IS NOT TRUE)
              AND market_watcher = false
              AND blacklist_watcher = true
            """
        )
    if not rows:
        log.info("ℹ️ LAB_BL_ANALYZER: подходящих клиентских стратегий не найдено")
        return

    sem = asyncio.Semaphore(MAX_CONCURRENCY_CLIENTS)
    tasks = [asyncio.create_task(_recompute_for_client_guard(sem, int(r["client_sid"]), int(r["master_sid"]), float(r["deposit"] or 0.0)))
             for r in rows]
    await asyncio.gather(*tasks)
    log.info("✅ LAB_BL_ANALYZER: полный пересчёт завершён (strategies=%d)", len(rows))


# 🔸 Таргетный пересчёт (по событию PACK LISTS READY) — только указанный мастер и версия
async def _recompute_by_master_and_version(master_sid: int, version: str):
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id AS client_sid, COALESCE(deposit,0) AS deposit
            FROM strategies_v4
            WHERE enabled = true AND (archived IS NOT TRUE)
              AND market_watcher = false
              AND blacklist_watcher = true
              AND market_mirrow = $1
            """,
            int(master_sid)
        )
    if not rows:
        log.debug("ℹ️ LAB_BL_ANALYZER: нет клиентов для master=%s", master_sid)
        return

    sem = asyncio.Semaphore(MAX_CONCURRENCY_CLIENTS)
    tasks = [asyncio.create_task(_recompute_for_client_guard(sem, int(r["client_sid"]), int(master_sid), float(r["deposit"] or 0.0), version_only=version))
             for r in rows]
    await asyncio.gather(*tasks)
    log.info("🔁 LAB_BL_ANALYZER: мастер=%s версия=%s пересчитаны (clients=%d)", master_sid, version, len(rows))


# 🔸 Гард клиента
async def _recompute_for_client_guard(sem: asyncio.Semaphore, client_sid: int, master_sid: int, deposit: float, version_only: Optional[str] = None):
    async with sem:
        try:
            await _recompute_for_client(client_sid, master_sid, deposit, version_only=version_only)
        except Exception:
            log.exception("❌ LAB_BL_ANALYZER: сбой пересчёта client_sid=%s master_sid=%s", client_sid, master_sid)


# 🔸 Пересчёт для одной клиентской стратегии (внутри — последовательно по версиям/режимам/направлениям/TF)
async def _recompute_for_client(client_sid: int, master_sid: int, deposit: float, version_only: Optional[str] = None):
    # страхуемся от некорректного депозита
    dep = float(deposit or 0.0)
    if dep <= 0:
        dep = 1.0  # защита от деления на ноль
        log.warning("⚠️ LAB_BL_ANALYZER: deposit<=0, используем 1.0 (client_sid=%s)", client_sid)

    now = datetime.utcnow().replace(tzinfo=None)
    win_end = now
    win_start = now - timedelta(days=7)

    versions = (version_only,) if version_only in VERSIONS else VERSIONS

    for version in versions:
        for mode in DECISION_MODES:
            for direction in DIRECTIONS:
                for tf in ALLOWED_TFS:
                    # вытягиваем сделки из лабораторной статистики
                    rows = await _load_positions_slice(client_sid, version, mode, direction, tf, win_start, win_end)
                    positions_total = len(rows)
                    pnl_sum_base = sum(r[1] for r in rows) if rows else 0.0
                    roi_base = (pnl_sum_base / dep) if dep > 0 else 0.0

                    # строим множество BL-хитов
                    bl_counts = sorted({int(r[0] or 0) for r in rows}) if rows else [0]
                    if 0 not in bl_counts:
                        bl_counts = [0] + bl_counts

                    # кривая ROI по порогам
                    roi_by_threshold: Dict[int, Dict[str, float | int]] = {}
                    best_T = 0
                    best_roi = roi_base
                    best_n = positions_total
                    best_pnl = pnl_sum_base

                    for T in bl_counts:
                        if T == 0:
                            passed = rows  # без фильтра
                        else:
                            passed = [r for r in rows if int(r[0] or 0) < T]
                        n_passed = len(passed)
                        pnl_sum = sum(r[1] for r in passed) if n_passed else 0.0
                        roi_T = (pnl_sum / dep) if (dep > 0 and n_passed > 0) else 0.0

                        roi_by_threshold[T] = {"n": n_passed, "pnl": float(pnl_sum), "roi": float(roi_T)}

                        # выбор лучшего: максимальный ROI, при равенстве — минимальный T
                        if (roi_T > best_roi) or (roi_T == best_roi and T < best_T):
                            best_T = T
                            best_roi = roi_T
                            best_n = n_passed
                            best_pnl = pnl_sum

                    # записываем историю и актив, обновляем кэш
                    await _persist_analysis_and_active(
                        master_sid=master_sid,
                        client_sid=client_sid,
                        version=version,
                        mode=mode,
                        direction=direction,
                        tf=tf,
                        window=(win_start, win_end),
                        deposit_used=dep,
                        positions_total=positions_total,
                        pnl_sum_base=pnl_sum_base,
                        roi_base=roi_base,
                        roi_curve=roi_by_threshold,
                        best_threshold=best_T,
                        best_positions=best_n,
                        best_pnl_sum=best_pnl,
                        best_roi=best_roi,
                    )


# 🔸 Загрузка среза сделок из laboratory_positions_stat
async def _load_positions_slice(client_sid: int, version: str, mode: str, direction: str, tf: str,
                                win_start: datetime, win_end: datetime) -> List[Tuple[int, float]]:
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT pack_bl_match_count AS blc, COALESCE(pnl,0) AS pnl
            FROM laboratory_positions_stat
            WHERE client_strategy_id = $1
              AND oracle_version = $2
              AND decision_mode = $3
              AND direction = $4
              AND tf = $5
              AND closed_at >= $6 AND closed_at < $7
            """,
            int(client_sid), str(version), str(mode), str(direction), str(tf),
            win_start, win_end
        )
    # вернём список (blc, pnl) как числа
    return [(int(r["blc"] or 0), float(r["pnl"] or 0.0)) for r in rows]


# 🔸 Запись истории и активного среза + обновление in-memory кэша
async def _persist_analysis_and_active(
    *,
    master_sid: int,
    client_sid: int,
    version: str,
    mode: str,
    direction: str,
    tf: str,
    window: Tuple[datetime, datetime],
    deposit_used: float,
    positions_total: int,
    pnl_sum_base: float,
    roi_base: float,
    roi_curve: Dict[int, Dict[str, float | int]],
    best_threshold: int,
    best_positions: int,
    best_pnl_sum: float,
    best_roi: float,
):
    win_start, win_end = window
    computed_at = datetime.utcnow().replace(tzinfo=None)

    # история
    async with infra.pg_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO laboratory_bl_analysis (
                  master_strategy_id, client_strategy_id, oracle_version, decision_mode, direction, tf,
                  window_start, window_end,
                  deposit_used, positions_total, positions_passed_base, pnl_sum_base, roi_base,
                  roi_by_threshold, best_threshold, best_positions, best_pnl_sum, best_roi, computed_at
                ) VALUES (
                  $1,$2,$3,$4,$5,$6,
                  $7,$8,
                  $9,$10,$11,$12,$13,
                  $14::jsonb, $15,$16,$17,$18,$19
                )
                """,
                int(master_sid), int(client_sid), version, mode, direction, tf,
                win_start, win_end,
                float(deposit_used), int(positions_total), int(positions_total), float(pnl_sum_base), float(roi_base),
                json.dumps({str(k): v for k, v in roi_curve.items()}, separators=(",", ":")),
                int(best_threshold), int(best_positions), float(best_pnl_sum), float(best_roi),
                computed_at,
            )

            # активный срез (UPSERT)
            await conn.execute(
                """
                INSERT INTO laboratory_bl_active (
                  master_strategy_id, oracle_version, decision_mode, direction, tf,
                  best_threshold, best_roi, roi_base, positions_total, deposit_used,
                  source_client_strategy_id, window_start, window_end, computed_at, updated_at
                ) VALUES (
                  $1,$2,$3,$4,$5,
                  $6,$7,$8,$9,$10,
                  $11,$12,$13,$14, now()
                )
                ON CONFLICT (master_strategy_id, oracle_version, decision_mode, direction, tf)
                DO UPDATE SET
                  best_threshold = EXCLUDED.best_threshold,
                  best_roi       = EXCLUDED.best_roi,
                  roi_base       = EXCLUDED.roi_base,
                  positions_total= EXCLUDED.positions_total,
                  deposit_used   = EXCLUDED.deposit_used,
                  source_client_strategy_id = EXCLUDED.source_client_strategy_id,
                  window_start   = EXCLUDED.window_start,
                  window_end     = EXCLUDED.window_end,
                  computed_at    = EXCLUDED.computed_at,
                  updated_at     = now()
                """,
                int(master_sid), version, mode, direction, tf,
                int(best_threshold), float(best_roi), float(roi_base), int(positions_total), float(deposit_used),
                int(client_sid), win_start, win_end, computed_at,
            )

    # обновляем in-memory кэш активного порога
    infra.upsert_bl_active(
        master_sid=master_sid,
        version=version,
        decision_mode=mode,
        direction=direction,
        tf=tf,
        threshold=best_threshold,
        best_roi=best_roi,
        roi_base=roi_base,
        positions_total=positions_total,
        deposit_used=deposit_used,
        computed_at=computed_at.isoformat(),
    )

    # лог
    log.debug(
        "LAB_BL_ANALYZER: master=%s ver=%s mode=%s %s tf=%s -> T*=%d ROI=%.6f (base=%.6f, n=%d)",
        master_sid, version, mode, direction, tf, best_threshold, best_roi, roi_base, positions_total
    )