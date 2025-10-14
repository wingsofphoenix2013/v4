# 🔸 laboratory_bl_analyzer.py — анализатор BL-порогов: фиксированный клиент на (master,version,mode), расчёт лучшего T по ROI, активный кэш

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
MAX_CONCURRENCY_CLIENTS = 8            # параллельная обработка клиентов (мап-кейсов)
READ_COUNT = 128
READ_BLOCK_MS = 30_000

# 🔸 Стрим, который триггерит пересчёт (обновление PACK WL/BL в oracle)
PACK_LISTS_READY_STREAM = "oracle:pack_lists:reports_ready"
BL_CONSUMER_GROUP = "LAB_BL_ANALYZER_GROUP"
BL_CONSUMER_NAME = "LAB_BL_ANALYZER_WORKER"

# 🔸 Доменные константы
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

    # построить карту соответствий (master,version,mode) -> (client, direction, tfs, deposit)
    mapping = await _build_master_mode_map()
    log.info("🔎 LAB_BL_ANALYZER: карта соответствий собрана (комбинаций=%d)", len(mapping))

    # полный пересчёт по всем ключам карты
    await _recompute_mapping(mapping)

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
                            # обновляем карту (вдруг появились новые клиенты) и пересчитываем таргет
                            mapping = await _build_master_mode_map()
                            await _recompute_by_master_and_version(mapping, master_sid, version)
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

# 🔸 Построение карты: (master_sid, version, mode) -> (client_sid, direction, tfs, deposit)
async def _build_master_mode_map() -> Dict[Tuple[int, str, str], Tuple[int, str, str, float]]:
    """
    Вариант B: строим фиксированную карту 12×2×4 = 96 комбинаций из истории позиций,
    с фолбэком к laboratory_request_head. Находим единственного клиента-дублёра
    для каждой тройки (master, version, mode), его direction и tfs (если есть в head).
    """
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            WITH clients AS (
              SELECT id AS client_sid,
                     COALESCE(market_mirrow, 0) AS master_sid,
                     COALESCE(deposit, 0)       AS deposit
              FROM strategies_v4
              WHERE enabled = true AND (archived IS NOT TRUE)
                AND market_watcher = false
                AND blacklist_watcher = true
                AND market_mirrow IS NOT NULL
            ),
            masters AS (
              SELECT DISTINCT master_sid FROM clients
            ),
            expected AS (
              SELECT m.master_sid, v.version, d.mode
              FROM masters m
              CROSS JOIN (VALUES ('v1'),('v2')) AS v(version)
              CROSS JOIN (VALUES ('mw_only'),('mw_then_pack'),('mw_and_pack'),('pack_only')) AS d(mode)
            ),

            -- выбор по истории позиций: берём клиента с макс. кол-вом сделок (и самым свежим закрытием при равенстве)
            pos_pick AS (
              SELECT *
              FROM (
                SELECT
                  lps.strategy_id        AS master_sid,
                  lps.oracle_version     AS version,
                  lps.decision_mode      AS mode,
                  lps.client_strategy_id AS client_sid,
                  lps.direction          AS direction,
                  COUNT(*)               AS n,
                  MAX(lps.closed_at)     AS last_closed,
                  ROW_NUMBER() OVER (
                    PARTITION BY lps.strategy_id, lps.oracle_version, lps.decision_mode
                    ORDER BY COUNT(*) DESC, MAX(lps.closed_at) DESC
                  ) AS rn
                FROM laboratory_positions_stat lps
                JOIN clients c ON c.client_sid = lps.client_strategy_id
                GROUP BY lps.strategy_id, lps.oracle_version, lps.decision_mode, lps.client_strategy_id, lps.direction
              ) s
              WHERE rn = 1
            ),

            -- фолбэк по head: последняя заявка на тройку
            head_pick AS (
              SELECT *
              FROM (
                SELECT
                  h.strategy_id        AS master_sid,
                  h.oracle_version     AS version,
                  h.decision_mode      AS mode,
                  h.client_strategy_id AS client_sid,
                  h.direction          AS direction,
                  h.timeframes_requested AS tfs,
                  ROW_NUMBER() OVER (
                    PARTITION BY h.strategy_id, h.oracle_version, h.decision_mode
                    ORDER BY h.finished_at DESC
                  ) AS rn
                FROM laboratory_request_head h
                JOIN clients c ON c.client_sid = h.client_strategy_id
              ) s
              WHERE rn = 1
            )

            SELECT
              e.master_sid,
              e.version,
              e.mode,
              COALESCE(pp.client_sid, hp.client_sid)                       AS client_sid,
              COALESCE(pp.direction,  hp.direction)                        AS direction,
              COALESCE(hp.tfs, 'm5,m15,h1')                                AS tfs,
              COALESCE(c.deposit, 0)                                       AS deposit
            FROM expected e
            LEFT JOIN pos_pick pp
              ON pp.master_sid = e.master_sid AND pp.version = e.version AND pp.mode = e.mode
            LEFT JOIN head_pick hp
              ON hp.master_sid = e.master_sid AND hp.version = e.version AND hp.mode = e.mode
            LEFT JOIN strategies_v4 c
              ON c.id = COALESCE(pp.client_sid, hp.client_sid)
            ORDER BY e.master_sid, e.version, e.mode
            """
        )

    mapping: Dict[Tuple[int, str, str], Tuple[int, str, str, float]] = {}
    missing: List[Tuple[int, str, str]] = []

    for r in rows:
        master_sid = int(r["master_sid"])
        version    = str(r["version"])
        mode       = str(r["mode"])
        client_sid = r["client_sid"]
        if client_sid is None:
            # нет ни позиций, ни head — пропускаем эту тройку, отметим как missing
            missing.append((master_sid, version, mode))
            continue
        direction  = str(r["direction"])
        tfs        = str(r["tfs"] or "m5,m15,h1")
        deposit    = float(r["deposit"] or 0.0)

        mapping[(master_sid, version, mode)] = (int(client_sid), direction, tfs, deposit)

    if missing:
        log.debug("ℹ️ LAB_BL_ANALYZER: нет данных для %d комбинаций (они будут пропущены): %s",
                  len(missing), ", ".join(f"{ms}/{v}/{m}" for ms,v,m in missing))
    log.info("🔎 LAB_BL_ANALYZER: карта соответствий собрана (комбинаций=%d)", len(mapping))
    return mapping

# 🔸 Полный пересчёт по всей карте
async def _recompute_mapping(mapping: Dict[Tuple[int, str, str], Tuple[int, str, str, float]]):
    if not mapping:
        log.info("ℹ️ LAB_BL_ANALYZER: карта пустая — нечего пересчитывать")
        return

    sem = asyncio.Semaphore(MAX_CONCURRENCY_CLIENTS)

    async def _one(key, val):
        master_sid, version, mode = key
        client_sid, direction, tfs, deposit = val
        await _recompute_for_tuple(master_sid, version, mode, client_sid, direction, tfs, deposit)

    await asyncio.gather(*[asyncio.create_task(_one(k, v)) for k, v in mapping.items()])
    log.info("✅ LAB_BL_ANALYZER: полный пересчёт завершён (combos=%d)", len(mapping))


# 🔸 Таргетный пересчёт: только (master, version)
async def _recompute_by_master_and_version(mapping: Dict[Tuple[int, str, str], Tuple[int, str, str, float]],
                                           master_sid: int, version: str):
    candidates = [(k, v) for k, v in mapping.items() if k[0] == int(master_sid) and k[1] == version]
    if not candidates:
        log.debug("ℹ️ LAB_BL_ANALYZER: нет соответствий для master=%s version=%s", master_sid, version)
        return

    sem = asyncio.Semaphore(MAX_CONCURRENCY_CLIENTS)

    async def _one(item):
        (m_sid, ver, mode), (client_sid, direction, tfs, deposit) = item
        await _recompute_for_tuple(m_sid, ver, mode, client_sid, direction, tfs, deposit)

    await asyncio.gather(*[asyncio.create_task(_one(item)) for item in candidates])
    log.info("🔁 LAB_BL_ANALYZER: таргетный пересчёт master=%s version=%s завершён (combos=%d)", master_sid, version, len(candidates))

# 🔸 Пересчёт одной комбинации (внутри — последовательно по TF; если выборка пуста — ничего не пишем и чистим актив)
async def _recompute_for_tuple(master_sid: int, version: str, mode: str,
                               client_sid: int, direction: str, tfs_requested: str, deposit: float):
    # страховка от деления на 0
    dep = float(deposit or 0.0)
    if dep <= 0.0:
        dep = 1.0
        log.warning("⚠️ LAB_BL_ANALYZER: deposit<=0, используем 1.0 (client_sid=%s)", client_sid)

    # окно 7 суток
    now = datetime.utcnow().replace(tzinfo=None)
    win_end = now
    win_start = now - timedelta(days=7)

    tfs = _parse_tfs(tfs_requested)

    for tf in tfs:
        # срез сделок
        rows = await _load_positions_slice(client_sid, version, mode, direction, tf, win_start, win_end)
        positions_total = len(rows)

        if positions_total == 0:
            # ничего не пишем в историю/актив — чистим актив и кэш
            await _delete_active_if_exists(master_sid, version, mode, direction, tf)
            log.debug("🧹 LAB_BL_ANALYZER: пустой срез — актив удалён (master=%s ver=%s mode=%s %s tf=%s)",
                      master_sid, version, mode, direction, tf)
            continue

        # базовый ROI
        pnl_sum_base = sum(p for _, p in rows)
        roi_base = (pnl_sum_base / dep) if dep > 0 else 0.0

        # —— Новый расчёт кривой ROI по ВСЕМ целым порогам 0..max(BL_count) ——
        # гистограмма BL_count: count и pnl-сумма на каждом значении
        hist_n: Dict[int, int] = {}
        hist_p: Dict[int, float] = {}
        max_c = 0
        for blc, pnl in rows:
            c = int(blc or 0)
            hist_n[c] = hist_n.get(c, 0) + 1
            hist_p[c] = hist_p.get(c, 0.0) + float(pnl or 0.0)
            if c > max_c:
                max_c = c

        # префиксные суммы до k: sum_{c<=k} n(c), sum_{c<=k} pnl(c)
        pref_n = [0] * (max_c + 1)
        pref_p = [0.0] * (max_c + 1)
        acc_n = 0
        acc_p = 0.0
        for c in range(0, max_c + 1):
            acc_n += hist_n.get(c, 0)
            acc_p += hist_p.get(c, 0.0)
            pref_n[c] = acc_n
            pref_p[c] = acc_p

        roi_by_threshold: Dict[int, Dict[str, float | int]] = {}
        best_T = 0
        best_roi = roi_base
        best_n = positions_total
        best_pnl = pnl_sum_base

        # T=0 — базовый срез: пропускаем все сделки (без фильтра)
        roi_by_threshold[0] = {"n": positions_total, "pnl": float(pnl_sum_base), "roi": float(roi_base)}

        # для T >= 1 пропускаем BL_count < T → это префикс до (T-1)
        for T in range(1, max_c + 1):
            n_passed = pref_n[T - 1]
            pnl_sum = pref_p[T - 1]
            roi_T = (pnl_sum / dep) if (dep > 0 and n_passed > 0) else 0.0
            roi_by_threshold[T] = {"n": n_passed, "pnl": float(pnl_sum), "roi": float(roi_T)}

            # лучший ROI; tie-break: минимальный T
            if (roi_T > best_roi) or (roi_T == best_roi and T < best_T):
                best_T = T
                best_roi = roi_T
                best_n = n_passed
                best_pnl = pnl_sum

        # запись результатов (есть позиции, т.к. выше early-return при positions_total == 0)
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
    return [(int(r["blc"] or 0), float(r["pnl"] or 0.0)) for r in rows]


# 🔸 Вспомогательные — запись истории и актива

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

    # история — пишем ТОЛЬКО если есть сделки в срезе
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
                  $14::jsonb,$15,$16,$17,$18,$19
                )
                """,
                int(master_sid), int(client_sid), version, mode, direction, tf,
                win_start, win_end,
                float(deposit_used), int(positions_total), int(positions_total), float(pnl_sum_base), float(roi_base),
                json.dumps({str(k): v for k, v in roi_curve.items()}, separators=(",", ":")),
                int(best_threshold), int(best_positions), float(best_pnl_sum), float(best_roi),
                computed_at,
            )

            # активный срез (UPSERT без условий — т.к. для каждой комбинации единственный клиент)
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

    log.debug(
        "LAB_BL_ANALYZER: master=%s ver=%s mode=%s %s tf=%s -> T*=%d ROI=%.6f (base=%.6f, n=%d)",
        master_sid, version, mode, direction, tf, best_threshold, best_roi, roi_base, positions_total
    )


async def _delete_active_if_exists(master_sid: int, version: str, mode: str, direction: str, tf: str):
    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            DELETE FROM laboratory_bl_active
            WHERE master_strategy_id = $1
              AND oracle_version = $2
              AND decision_mode = $3
              AND direction = $4
              AND tf = $5
            """,
            int(master_sid), str(version), str(mode), str(direction), str(tf)
        )
    # чистим in-memory кэш
    key = (int(master_sid), str(version), str(mode), str(direction), str(tf))
    infra.lab_bl_active.pop(key, None)


# 🔸 Утилиты

def _parse_tfs(tfs: str) -> List[str]:
    seen = set()
    out: List[str] = []
    for tf in (tfs or "").split(","):
        tf = tf.strip().lower()
        if tf in ALLOWED_TFS and tf not in seen:
            out.append(tf)
            seen.add(tf)
    # если вдруг пусто — дефолт к m5
    return out or ["m5"]