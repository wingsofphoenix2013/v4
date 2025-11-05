# 🔸 laboratory_wl_analyzer.py — анализатор WL-порогов (winrate): расчёт лучшего T_wr по ROI для MW и PACK, активный кэш

# 🔸 Импорты
import asyncio
import json
import logging
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP, getcontext
from typing import Dict, List, Tuple, Optional

import laboratory_infra as infra

# 🔸 Логгер
log = logging.getLogger("LAB_WL_ANALYZER")

# 🔸 Параметры воркера
INITIAL_DELAY_SEC = 60                  # задержка перед первым запуском
MAX_CONCURRENCY = 8                     # параллельная обработка комбинаций master/version/mode
READ_COUNT = 128
READ_BLOCK_MS = 30_000

# 🔸 Стримы триггеров (обновление WL у oracle)
MW_WL_READY_STREAM = "oracle:mw_whitelist:reports_ready"
PACK_LISTS_READY_STREAM = "oracle:pack_lists:reports_ready"
WL_CONSUMER_GROUP = "LAB_WL_ANALYZER_GROUP"
WL_CONSUMER_NAME = "LAB_WL_ANALYZER_WORKER"

# 🔸 Доменные константы
ALLOWED_TFS = ("m5", "m15", "h1")
DECISION_MODES = ("mw_only", "mw_then_pack", "mw_and_pack", "pack_only")
DIRECTIONS = ("long", "short")
VERSIONS = ("v1","v2","v3","v4")
SOURCES = ("mw", "pack")

# 🔸 Сетка порогов WR
WR_MIN = Decimal("0.55")
WR_STEP = Decimal("0.01")
WR_ONE = Decimal("1.00")

# 🔸 Настройка Decimal
getcontext().prec = 28  # достаточно для наших расчётов


# 🔸 Публичная точка входа воркера
async def run_laboratory_wl_analyzer():
    # условия достаточности окружения
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск LAB_WL_ANALYZER: PG/Redis не инициализированы")
        return

    # стартовая задержка
    if INITIAL_DELAY_SEC > 0:
        log.debug("⏳ LAB_WL_ANALYZER: ожидание %d сек перед стартом", INITIAL_DELAY_SEC)
        await asyncio.sleep(INITIAL_DELAY_SEC)

    # загрузка активных WL-порогов из БД в память
    await _load_wl_active_from_db()

    # построить карту соответствий (master, version, mode) -> (client, direction, tfs, deposit)
    mapping = await _build_master_mode_map()
    log.debug("🔎 LAB_WL_ANALYZER: карта соответствий собрана (комбинаций=%d)", len(mapping))

    # полный пересчёт по всей карте для обоих источников (MW и PACK)
    await _recompute_mapping(mapping)

    # подписки на стримы триггеров
    for s in (MW_WL_READY_STREAM, PACK_LISTS_READY_STREAM):
        try:
            await infra.redis_client.xgroup_create(
                name=s, groupname=WL_CONSUMER_GROUP, id="$", mkstream=True
            )
            log.debug("📡 LAB_WL_ANALYZER: создана consumer group для %s", s)
        except Exception as e:
            if "BUSYGROUP" in str(e):
                pass
            else:
                log.exception("❌ LAB_WL_ANALYZER: ошибка создания consumer group для %s", s)
                return

    log.debug("🚀 LAB_WL_ANALYZER: слушаю %s и %s", MW_WL_READY_STREAM, PACK_LISTS_READY_STREAM)

    # основной цикл (реакция на таргетные обновления WL)
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=WL_CONSUMER_GROUP,
                consumername=WL_CONSUMER_NAME,
                streams={MW_WL_READY_STREAM: ">", PACK_LISTS_READY_STREAM: ">"},
                count=READ_COUNT,
                block=READ_BLOCK_MS,
            )
            if not resp:
                continue

            acks: Dict[str, List[str]] = {}
            for stream_name, msgs in resp:
                for msg_id, fields in msgs:
                    try:
                        payload = json.loads(fields.get("data", "{}"))
                        master_sid = int(payload.get("strategy_id", 0))
                        version = str(payload.get("version", "")).lower()
                        if master_sid > 0 and version in VERSIONS:
                            # освежаем карту (на случай новых клиентов), таргетный пересчёт
                            mapping = await _build_master_mode_map()
                            source = "mw" if stream_name == MW_WL_READY_STREAM else "pack"
                            await _recompute_by_master_version_source(mapping, master_sid, version, source)
                        else:
                            log.debug("ℹ️ LAB_WL_ANALYZER: пропуск payload=%s", payload)
                        acks.setdefault(stream_name, []).append(msg_id)
                    except Exception:
                        log.exception("❌ LAB_WL_ANALYZER: ошибка обработки сообщения")
                        acks.setdefault(stream_name, []).append(msg_id)

            # ACK
            for s, ids in acks.items():
                if ids:
                    try:
                        await infra.redis_client.xack(s, WL_CONSUMER_GROUP, *ids)
                    except Exception:
                        log.exception("⚠️ LAB_WL_ANALYZER: ошибка ACK в %s", s)

        except asyncio.CancelledError:
            log.debug("⏹️ LAB_WL_ANALYZER: остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ LAB_WL_ANALYZER: ошибка цикла — пауза 5 секунд")
            await asyncio.sleep(5)

# 🔸 построение карты соответствий из истории позиций (fallback к head; версии v1–v4)
async def _build_master_mode_map() -> Dict[Tuple[int, str, str], Tuple[int, str, str, float]]:
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
              CROSS JOIN (VALUES ('v1'),('v2'),('v3'),('v4')) AS v(version)
              CROSS JOIN (VALUES ('mw_only'),('mw_then_pack'),('mw_and_pack'),('pack_only')) AS d(mode)
            ),
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
            head_pick AS (
              SELECT *
              FROM (
                SELECT
                  h.strategy_id          AS master_sid,
                  h.oracle_version       AS version,
                  h.decision_mode        AS mode,
                  h.client_strategy_id   AS client_sid,
                  h.direction            AS direction,
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
        version = str(r["version"])
        mode = str(r["mode"])
        client_sid = r["client_sid"]
        if client_sid is None:
            missing.append((master_sid, version, mode))
            continue
        mapping[(master_sid, version, mode)] = (
            int(client_sid),
            str(r["direction"]),
            str(r["tfs"] or "m5,m15,h1"),
            float(r["deposit"] or 0.0),
        )
    if missing:
        log.debug("ℹ️ LAB_WL_ANALYZER: нет данных для %d комбинаций: %s",
                  len(missing), ", ".join(f"{ms}/{v}/{m}" for ms, v, m in missing))
    return mapping

# полный пересчёт по всей карте для обоих источников
async def _recompute_mapping(mapping: Dict[Tuple[int, str, str], Tuple[int, str, str, float]]):
    if not mapping:
        log.debug("ℹ️ LAB_WL_ANALYZER: карта пустая — нечего пересчитывать")
        return

    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    async def _one(key, val):
        master_sid, version, mode = key
        client_sid, direction, tfs, deposit = val
        for src in SOURCES:
            await _recompute_for_tuple(master_sid, version, mode, client_sid, direction, tfs, deposit, src)

    await asyncio.gather(*[asyncio.create_task(_one(k, v)) for k, v in mapping.items()])
    log.debug("✅ LAB_WL_ANALYZER: полный пересчёт завершён (combos=%d × sources=2)", len(mapping))


# таргетный пересчёт: только (master, version) и только нужный source ('mw'|'pack')
async def _recompute_by_master_version_source(mapping: Dict[Tuple[int, str, str], Tuple[int, str, str, float]],
                                              master_sid: int, version: str, source: str):
    candidates = [(k, v) for k, v in mapping.items() if k[0] == int(master_sid) and k[1] == version]
    if not candidates:
        log.debug("ℹ️ LAB_WL_ANALYZER: нет соответствий для master=%s version=%s", master_sid, version)
        return

    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    async def _one(item):
        (m_sid, ver, mode), (client_sid, direction, tfs, deposit) = item
        await _recompute_for_tuple(m_sid, ver, mode, client_sid, direction, tfs, deposit, source)

    await asyncio.gather(*[asyncio.create_task(_one(item)) for item in candidates])
    log.debug("🔁 LAB_WL_ANALYZER: таргетный пересчёт master=%s version=%s source=%s завершён (combos=%d)",
             master_sid, version, source, len(candidates))


# пересчёт одной комбинации и одного источника (внутри — последовательно по TF)
async def _recompute_for_tuple(master_sid: int, version: str, mode: str,
                               client_sid: int, direction: str, tfs_requested: str,
                               deposit: float, source: str):
    # страховка от деления на 0
    dep = float(deposit or 0.0)
    if dep <= 0.0:
        dep = 1.0
        log.warning("⚠️ LAB_WL_ANALYZER: deposit<=0, используем 1.0 (client_sid=%s)", client_sid)

    # окно 7 суток
    now = datetime.utcnow().replace(tzinfo=None)
    win_end = now
    win_start = now - timedelta(days=7)

    tfs = _parse_tfs(tfs_requested)

    for tf in tfs:
        # срез сделок: wr_source для выбранного источника + pnl
        rows = await _load_positions_slice_wl(client_sid, version, mode, direction, tf, source, win_start, win_end)
        positions_total = len(rows)

        if positions_total == 0:
            # удалить актив и очистить кэш
            await _delete_wl_active_if_exists(master_sid, version, mode, direction, tf, source)
            log.debug("🧹 LAB_WL_ANALYZER: пустой срез — актив WL удалён (master=%s ver=%s mode=%s %s tf=%s src=%s)",
                      master_sid, version, mode, direction, tf, source)
            continue

        # максимум wr в срезе (для построения верхней точки сетки)
        wr_max = max((r[0] for r in rows), default=0.0)
        if Decimal(str(wr_max)) < WR_MIN:
            # нет ни одной сделки с wr>=0.55 → ничего не пишем, удаляем актив
            await _delete_wl_active_if_exists(master_sid, version, mode, direction, tf, source)
            log.debug("🧹 LAB_WL_ANALYZER: в срезе нет wr>=%.2f — актив WL удалён (master=%s ver=%s mode=%s %s tf=%s src=%s)",
                      float(WR_MIN), master_sid, version, mode, direction, tf, source)
            continue

        # базовый ROI на нижнем пороге (T = 0.55)
        pnl_sum_base = sum(p for w, p in rows if Decimal(str(w)) >= WR_MIN)
        roi_base = float(pnl_sum_base / dep) if pnl_sum_base != 0 else 0.0

        # сетка порогов: 0.55, 0.56, ... до next-grid-above(max wr), но ≤1.00
        wr_top = _next_grid_above(Decimal(str(wr_max)))
        if wr_top > WR_ONE:
            wr_top = WR_ONE

        thresholds: List[Decimal] = []
        t = WR_MIN
        while t <= wr_top:
            thresholds.append(t)
            t = (t + WR_STEP).quantize(WR_STEP, rounding=ROUND_HALF_UP)
        if thresholds[-1] != WR_ONE and wr_top == WR_ONE:
            thresholds.append(WR_ONE)

        # кумулятивы по wr: отсортируем wr и сделаем префикс ↑ (или просто фильтр по каждому T)
        # здесь сделок на TF не так много — сделаем прямой расчёт для читаемости
        roi_by_threshold: Dict[str, Dict[str, float | int]] = {}
        best_T = WR_MIN
        best_roi = roi_base
        best_n = sum(1 for w, _ in rows if Decimal(str(w)) >= WR_MIN)
        best_pnl = pnl_sum_base

        for T in thresholds:
            n_passed = 0
            pnl_sum = 0.0
            T_f = float(T)
            for w, p in rows:
                if w >= T_f:
                    n_passed += 1
                    pnl_sum += p
            roi_T = float(pnl_sum / dep) if (dep > 0 and n_passed > 0) else 0.0

            key = f"{T:.2f}"
            roi_by_threshold[key] = {"n": n_passed, "pnl": float(pnl_sum), "roi": float(roi_T)}

            if (roi_T > best_roi) or (roi_T == best_roi and T < best_T):
                best_T = T
                best_roi = roi_T
                best_n = n_passed
                best_pnl = pnl_sum

        # запись истории и актива
        await _persist_wl_analysis_and_active(
            master_sid=master_sid,
            client_sid=client_sid,
            version=version,
            mode=mode,
            direction=direction,
            tf=tf,
            source=source,
            window=(win_start, win_end),
            deposit_used=dep,
            positions_total=positions_total,
            pnl_sum_base=pnl_sum_base,
            roi_base=roi_base,
            roi_curve=roi_by_threshold,
            best_threshold=float(best_T),
            best_positions=best_n,
            best_pnl_sum=best_pnl,
            best_roi=best_roi,
        )


# загрузка активных WL из БД в память
async def _load_wl_active_from_db():
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT master_strategy_id, oracle_version, decision_mode, direction, tf, source,
                   best_threshold, best_roi, roi_base, positions_total, deposit_used, computed_at
            FROM laboratory_wl_active
            """
        )
    m: Dict[Tuple[int, str, str, str, str, str], Dict] = {}
    for r in rows:
        key = (int(r["master_strategy_id"]), str(r["oracle_version"]), str(r["decision_mode"]),
               str(r["direction"]), str(r["tf"]), str(r["source"]))
        m[key] = {
            "threshold": float(r["best_threshold"] or 0.55),
            "best_roi": float(r["best_roi"] or 0.0),
            "roi_base": float(r["roi_base"] or 0.0),
            "positions_total": int(r["positions_total"] or 0),
            "deposit_used": float(r["deposit_used"] or 0.0),
            "computed_at": (r["computed_at"].isoformat() if r["computed_at"] else ""),
        }
    infra.set_wl_active_bulk(m)


# чтение позиций для WR: возвращает список (wr_source, pnl)
async def _load_positions_slice_wl(client_sid: int, version: str, mode: str, direction: str, tf: str,
                                   source: str, win_start: datetime, win_end: datetime) -> List[Tuple[float, float]]:
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
              COALESCE(mw_matches, '[]'::jsonb)   AS mw_matches,
              COALESCE(pack_wl_matches, '[]'::jsonb) AS pack_wl_matches,
              COALESCE(pnl, 0) AS pnl
            FROM laboratory_positions_stat
            WHERE client_strategy_id = $1
              AND oracle_version = $2
              AND decision_mode  = $3
              AND direction      = $4
              AND tf             = $5
              AND closed_at >= $6 AND closed_at < $7
            """,
            int(client_sid), str(version), str(mode), str(direction), str(tf),
            win_start, win_end
        )

    def _parse_json_value(val):
        if isinstance(val, list):
            return val
        if isinstance(val, (bytes, bytearray, memoryview)):
            try:
                return json.loads(bytes(val).decode("utf-8"))
            except Exception:
                return []
        if isinstance(val, str):
            s = val.strip()
            if not s:
                return []
            try:
                return json.loads(s)
            except Exception:
                return []
        return []

    res: List[Tuple[float, float]] = []
    src_key = "mw" if source == "mw" else "pack"

    for r in rows:
        pnl = float(r["pnl"] or 0.0)
        matches = _parse_json_value(r["mw_matches"] if src_key == "mw" else r["pack_wl_matches"])
        # wr_source = max wr среди совпавших WL (если нет матчей — 0.0)
        wrs = []
        for m in matches:
            try:
                w = float(m.get("wr", 0.0))
            except Exception:
                w = 0.0
            wrs.append(w)
        wr_src = max(wrs) if wrs else 0.0
        res.append((wr_src, pnl))
    return res


# запись истории и актива WL
async def _persist_wl_analysis_and_active(
    *,
    master_sid: int,
    client_sid: int,
    version: str,
    mode: str,
    direction: str,
    tf: str,
    source: str,
    window: Tuple[datetime, datetime],
    deposit_used: float,
    positions_total: int,
    pnl_sum_base: float,
    roi_base: float,
    roi_curve: Dict[str, Dict[str, float | int]],
    best_threshold: float,
    best_positions: int,
    best_pnl_sum: float,
    best_roi: float,
):
    win_start, win_end = window
    computed_at = datetime.utcnow().replace(tzinfo=None)

    async with infra.pg_pool.acquire() as conn:
        async with conn.transaction():
            # история
            await conn.execute(
                """
                INSERT INTO laboratory_wl_analysis (
                  master_strategy_id, client_strategy_id, oracle_version, decision_mode, direction, tf, source,
                  window_start, window_end,
                  deposit_used, positions_total, pnl_sum_base, roi_base,
                  wr_by_threshold, best_threshold, best_positions, best_pnl_sum, best_roi, computed_at
                ) VALUES (
                  $1,$2,$3,$4,$5,$6,$7,
                  $8,$9,
                  $10,$11,$12,$13,
                  $14::jsonb,$15,$16,$17,$18,$19
                )
                """,
                int(master_sid), int(client_sid), version, mode, direction, tf, source,
                win_start, win_end,
                float(deposit_used), int(positions_total), float(pnl_sum_base), float(roi_base),
                json.dumps(roi_curve, separators=(",", ":")),
                float(round(best_threshold, 2)), int(best_positions), float(best_pnl_sum), float(best_roi),
                computed_at,
            )

            # актив (UPSERT)
            await conn.execute(
                """
                INSERT INTO laboratory_wl_active (
                  master_strategy_id, oracle_version, decision_mode, direction, tf, source,
                  best_threshold, best_roi, roi_base, positions_total, deposit_used,
                  source_client_strategy_id, window_start, window_end, computed_at, updated_at
                ) VALUES (
                  $1,$2,$3,$4,$5,$6,
                  $7,$8,$9,$10,$11,
                  $12,$13,$14,$15, now()
                )
                ON CONFLICT (master_strategy_id, oracle_version, decision_mode, direction, tf, source)
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
                int(master_sid), version, mode, direction, tf, source,
                float(round(best_threshold, 2)), float(best_roi), float(roi_base), int(positions_total), float(deposit_used),
                int(client_sid), win_start, win_end, computed_at,
            )

    # обновить кэш
    infra.upsert_wl_active(
        master_sid=master_sid,
        version=version,
        decision_mode=mode,
        direction=direction,
        tf=tf,
        source=source,
        threshold=float(round(best_threshold, 2)),
        best_roi=best_roi,
        roi_base=roi_base,
        positions_total=positions_total,
        deposit_used=deposit_used,
        computed_at=computed_at.isoformat(),
    )

    log.debug(
        "LAB_WL_ANALYZER: master=%s ver=%s mode=%s %s tf=%s src=%s -> T*=%.2f ROI=%.6f (base=%.6f, n=%d)",
        master_sid, version, mode, direction, tf, source, float(round(best_threshold, 2)), best_roi, roi_base, positions_total
    )


# удалить актив WL (и почистить кэш) — при пустом/невалидном срезе
async def _delete_wl_active_if_exists(master_sid: int, version: str, mode: str, direction: str, tf: str, source: str):
    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            DELETE FROM laboratory_wl_active
            WHERE master_strategy_id = $1
              AND oracle_version = $2
              AND decision_mode = $3
              AND direction = $4
              AND tf = $5
              AND source = $6
            """,
            int(master_sid), str(version), str(mode), str(direction), str(tf), str(source)
        )
    key = (int(master_sid), str(version), str(mode), str(direction), str(tf), str(source))
    infra.lab_wl_active.pop(key, None)


# утилиты

def _parse_tfs(tfs: str) -> List[str]:
    seen = set()
    out: List[str] = []
    for tf in (tfs or "").split(","):
        tf = tf.strip().lower()
        if tf in ALLOWED_TFS and tf not in seen:
            out.append(tf)
            seen.add(tf)
    return out or ["m5"]


def _next_grid_above(x: Decimal) -> Decimal:
    """
    Следующий узел сетки (0.55 + k*0.01), строго не ниже x, и плюс один шаг (как «крышка»).
    Возвращает «один шаг выше x по сетке»; потом обрежем по 1.00.
    """
    if x <= WR_MIN:
        return (WR_MIN + WR_STEP).quantize(WR_STEP, rounding=ROUND_HALF_UP)
    # сколько шагов от минимума до x
    steps = ((x - WR_MIN) / WR_STEP).to_integral_value(rounding=ROUND_HALF_UP)
    candidate = WR_MIN + (steps * WR_STEP)
    if candidate < x:
        candidate = candidate + WR_STEP
    else:
        candidate = candidate + WR_STEP  # «один шаг выше»
    return candidate.quantize(WR_STEP, rounding=ROUND_HALF_UP)