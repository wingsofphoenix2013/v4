# laboratory_bl_analyzer.py — оффлайн-аналитика BL: ROI(K) по окну 7×24ч, выбор best_k, публикация компактного KV

# 🔸 Импорты
import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Tuple, Optional

# 🔸 Инфраструктура
import laboratory_infra as infra

# 🔸 Логгер
log = logging.getLogger("LAB_BL_ANALYZER")

# 🔸 Параметры окна/периодичности и стартовая задержка
WINDOW_HOURS = 168          # 7 * 24
RUN_EVERY_SEC = 3600        # раз в час
START_DELAY_SEC = 90        # задержка запуска после старта сервиса

# 🔸 Таблицы
LPS_TABLE = "public.laboratoty_position_stat"
POS_TABLE = "public.positions_v4"
STRAT_TABLE = "public.strategies_v4"
SCAN_TABLE = "public.laboratory_bl_scan"
SUMMARY_TABLE = "public.laboratory_bl_summary"
ACTIVE_TABLE = "public.laboratory_bl_summary_active"

# 🔸 Ключи Redis для «активных» порогов (минимальный формат)
def _kv_key(master_sid: int, tf: str) -> str:
    return f"laboratory:bl:k:{master_sid}:{tf}"


# 🔸 Структура ряда для расчёта
class Row:
    __slots__ = ("csid", "master_sid", "tf", "direction", "bl_hits", "pnl", "deposit")
    def __init__(self, csid: int, master_sid: int, tf: str, direction: str, bl_hits: int, pnl: Decimal, deposit: Decimal):
        self.csid = csid
        self.master_sid = master_sid
        self.tf = tf
        self.direction = direction
        self.bl_hits = bl_hits
        self.pnl = pnl
        self.deposit = deposit


# 🔸 Забор данных в окно по закрытым позициям → ряды уровня LPS (по TF), только для стратегий с blacklist_watcher=true
async def _fetch_rows(window_start: datetime, window_end: datetime) -> List[Row]:
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                lps.client_strategy_id AS csid,
                lps.strategy_id        AS master_sid,
                lps.tf,
                lps.direction,
                COALESCE(lps.pack_bl_match_count, 0) AS bl_hits,
                COALESCE(lps.pnl, p.pnl)              AS pnl,
                s.deposit                              AS deposit
            FROM {LPS_TABLE} lps
            JOIN {POS_TABLE} p
              ON p.log_uid = lps.log_uid
             AND p.strategy_id = lps.client_strategy_id
            JOIN {STRAT_TABLE} s
              ON s.id = lps.client_strategy_id
             AND s.blacklist_watcher = TRUE
            WHERE p.status = 'closed'
              AND p.closed_at >  $1
              AND p.closed_at <= $2
              AND COALESCE(lps.pnl, p.pnl) IS NOT NULL
            """,
            window_start, window_end
        )
    out: List[Row] = []
    for r in rows:
        try:
            dep = Decimal(r["deposit"])
            if dep <= 0:
                continue
            out.append(
                Row(
                    csid=int(r["csid"]),
                    master_sid=int(r["master_sid"]),
                    tf=str(r["tf"]),
                    direction=str(r["direction"]),
                    bl_hits=int(r["bl_hits"]),
                    pnl=Decimal(r["pnl"]),
                    deposit=dep,
                )
            )
        except Exception:
            # пропускаем некорректные строки
            continue
    return out


# 🔸 Расчёт профиля ROI(K) + выбор best_k (максимальный ROI, baseline K=0 участвует)
def _compute_profile(rows: List[Row], deposit: Decimal) -> Tuple[Dict[int, Dict], Dict]:
    # rows — все сделки (LPS-строки) в группе csid×TF×direction
    if not rows or deposit is None or deposit <= 0:
        return {}, {}

    n_total = len(rows)
    pnl_total = sum((r.pnl for r in rows), Decimal("0"))
    roi_base = (pnl_total / deposit)

    # сетка порогов: 1..Kmax (K=0 трактуем как baseline)
    kmax = max((max(1, r.bl_hits) for r in rows), default=1)
    profile: Dict[int, Dict] = {}

    # стартовый лучший — K=0 (без фильтра)
    best_k = 0
    best_roi = roi_base
    best_allowed = n_total
    best_blocked = 0

    for k in range(1, kmax + 1):
        allowed = [r for r in rows if r.bl_hits < k]
        blocked = n_total - len(allowed)
        pnl_allowed = sum((r.pnl for r in allowed), Decimal("0"))
        roi_k = (pnl_allowed / deposit)

        profile[k] = {
            "threshold_k": k,
            "n_total": n_total,
            "n_blocked": blocked,
            "n_allowed": len(allowed),
            "pnl_total": pnl_total,
            "pnl_allowed": pnl_allowed,
            "deposit": deposit,
            "roi_base": roi_base,
            "roi_k": roi_k,
        }

        # выбираем максимум ROI без доп. критериев
        if roi_k > best_roi:
            best_roi = roi_k
            best_k = k
            best_allowed = len(allowed)
            best_blocked = blocked

    summary = {
        "n_total": n_total,
        "roi_base": roi_base,
        "roi_bin": profile.get(1, {}).get("roi_k", roi_base),  # K=1
        "best_k": best_k,                   # может быть 0 (не фильтровать)
        "roi_best": best_roi,
        "n_allowed_best": best_allowed,
        "n_blocked_best": best_blocked,
        "uplift_abs": (best_roi - roi_base),
        "uplift_rel_pct": ((best_roi - roi_base) / (abs(roi_base) if roi_base != 0 else Decimal("1"))),
        "criteria_note": "criterion=max_roi",
    }
    return profile, summary


# 🔸 Публикация компактных KV в Redis: per (master_sid, tf) → значение str(best_k)
async def _publish_active_kv(items: List[Tuple[int, str, int, Decimal]]):
    # items: [(master_sid, tf, best_k, roi_best)]
    if not items:
        log.info("[BL] ℹ️ KV: нет пар для публикации")
        return

    # выберем по (master, tf) запись с максимальным roi_best (на случай нескольких csid под одним master)
    chosen: Dict[Tuple[int, str], Tuple[int, Decimal]] = {}
    masters: Dict[int, set] = {}

    for master_sid, tf, best_k, roi_best in items:
        key = (master_sid, tf)
        masters.setdefault(master_sid, set()).add(tf)
        prev = chosen.get(key)
        if prev is None or roi_best > prev[1]:
            chosen[key] = (best_k, roi_best)

    # сначала зачистим ключи по каждому master_sid для стандартных TF, затем выставим актуальные
    tfs_all = ("m5", "m15", "h1")
    deleted = 0
    set_count = 0

    for master_sid in masters.keys():
        # удаляем старые ключи по всем стандартным TF
        for tf in tfs_all:
            del_key = _kv_key(master_sid, tf)
            try:
                res = await infra.redis_client.delete(del_key)
                deleted += int(res or 0)
            except Exception:
                # безопасно игнорируем ошибки удаления
                pass

    for (master_sid, tf), (best_k, _roi) in chosen.items():
        key = _kv_key(master_sid, tf)
        val = str(int(best_k))  # "0" — фильтр off
        await infra.redis_client.set(key, val)
        set_count += 1

    log.info("[BL] ✅ KV published: masters=%d keys_set=%d keys_deleted=%d",
             len(masters), set_count, deleted)


# 🔸 Один прогон: построить профиль и сводку по всем csid×TF×direction и записать в таблицы + опубликовать KV
async def _run_once(window_hours: int):
    window_end = datetime.utcnow().replace(tzinfo=None)
    window_start = window_end - timedelta(hours=window_hours)

    # логичное упорядочивание TF в итоговом логе
    tf_order = {"m5": 0, "m15": 1, "h1": 2}

    rows = await _fetch_rows(window_start, window_end)
    if not rows:
        log.info("[BL] ℹ️ окно[%s..%s]: нет данных для анализа", window_start.isoformat(), window_end.isoformat())
        return

    # группировка по csid×TF×direction (мастер_id протащим отдельно)
    groups: Dict[Tuple[int, str, str], List[Row]] = {}
    master_of_group: Dict[Tuple[int, str, str], int] = {}
    for r in rows:
        key = (r.csid, r.tf, r.direction)
        groups.setdefault(key, []).append(r)
        master_of_group[key] = r.master_sid  # по группе ожидаем один master

    scan_rows: List[Tuple] = []     # для laboratory_bl_scan
    summary_rows: List[Tuple] = []  # для laboratory_bl_summary
    active_rows: List[Tuple] = []   # для laboratory_bl_summary_active
    kv_items: List[Tuple[int, str, int, Decimal]] = []  # (master_sid, tf, best_k, roi_best)

    processed = 0

    for (csid, tf, direction), vec in groups.items():
        # депозит берём из первого ряда (он одинаковый в группе, т.к. csid один)
        deposit = vec[0].deposit if vec and vec[0].deposit is not None else None
        if deposit is None or deposit <= 0:
            log.info("[BL] ⚠️ пропуск csid=%s tf=%s dir=%s: deposit отсутствует/<=0", csid, tf, direction)
            continue

        profile, summary = _compute_profile(vec, deposit)
        if not profile:
            continue

        # профиль по K — в SCAN
        for k, p in profile.items():
            scan_rows.append((
                window_start, window_end, csid, tf, direction,            # ключ
                p["threshold_k"], p["n_total"], p["n_blocked"], p["n_allowed"],
                p["pnl_total"], p["pnl_allowed"], p["deposit"],
                p["roi_base"], p["roi_k"]
            ))

        # сводка (best_k + базовые) — в SUMMARY
        summary_rows.append((
            window_start, window_end, csid, tf, direction,
            deposit, summary["n_total"],
            summary["roi_base"], summary["roi_bin"],
            summary["best_k"], summary["roi_best"],
            summary["n_blocked_best"], summary["n_allowed_best"],
            summary["uplift_abs"], summary["uplift_rel_pct"],
            summary["criteria_note"]
        ))

        # активный срез (перезапишем целиком таблицу после сбора всех групп)
        active_rows.append((
            csid, tf, direction,
            WINDOW_HOURS, window_start, window_end, deposit, summary["n_total"],
            summary["roi_base"], summary["roi_bin"],
            summary["best_k"], summary["roi_best"],
            summary["n_blocked_best"], summary["n_allowed_best"],
            summary["uplift_abs"], summary["uplift_rel_pct"],
            summary["criteria_note"]
        ))

        # подготовка KV: per master_sid×TF → best_k
        master_sid = master_of_group[(csid, tf, direction)]
        kv_items.append((master_sid, tf, int(summary["best_k"]), summary["roi_best"]))

        processed += 1

    if not scan_rows:
        log.info("[BL] ℹ️ окно[%s..%s]: групп после фильтра нет", window_start.isoformat(), window_end.isoformat())
        return

    # запись в БД + публикация KV
    async with infra.pg_pool.acquire() as conn:
        async with conn.transaction():
            # upsert SCAN
            await conn.executemany(
                f"""
                INSERT INTO {SCAN_TABLE} (
                    window_start, window_end, client_strategy_id, tf, direction,
                    threshold_k, n_total, n_blocked, n_allowed,
                    pnl_total, pnl_allowed, deposit,
                    roi_base, roi_k
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14
                )
                ON CONFLICT (window_end, client_strategy_id, tf, direction, threshold_k)
                DO UPDATE SET
                    window_start = EXCLUDED.window_start,
                    n_total      = EXCLUDED.n_total,
                    n_blocked    = EXCLUDED.n_blocked,
                    n_allowed    = EXCLUDED.n_allowed,
                    pnl_total    = EXCLUDED.pnl_total,
                    pnl_allowed  = EXCLUDED.pnl_allowed,
                    deposit      = EXCLUDED.deposit,
                    roi_base     = EXCLUDED.roi_base,
                    roi_k        = EXCLUDED.roi_k,
                    created_at   = NOW()
                """,
                scan_rows
            )

            # upsert SUMMARY (история часа)
            await conn.executemany(
                f"""
                INSERT INTO {SUMMARY_TABLE} (
                    window_start, window_end, client_strategy_id, tf, direction,
                    deposit, n_total, roi_base, roi_bin,
                    best_k, roi_best, n_blocked_best, n_allowed_best,
                    uplift_abs, uplift_rel_pct, criteria_note
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16
                )
                ON CONFLICT (window_end, client_strategy_id, tf, direction)
                DO UPDATE SET
                    window_start   = EXCLUDED.window_start,
                    deposit        = EXCLUDED.deposit,
                    n_total        = EXCLUDED.n_total,
                    roi_base       = EXCLUDED.roi_base,
                    roi_bin        = EXCLUDED.roi_bin,
                    best_k         = EXCLUDED.best_k,
                    roi_best       = EXCLUDED.roi_best,
                    n_blocked_best = EXCLUDED.n_blocked_best,
                    n_allowed_best = EXCLUDED.n_allowed_best,
                    uplift_abs     = EXCLUDED.uplift_abs,
                    uplift_rel_pct = EXCLUDED.uplift_rel_pct,
                    criteria_note  = EXCLUDED.criteria_note,
                    created_at     = NOW()
                """,
                summary_rows
            )

            # активный срез: полностью пересобираем таблицу (атомарно)
            await conn.execute(f"TRUNCATE {ACTIVE_TABLE}")
            await conn.executemany(
                f"""
                INSERT INTO {ACTIVE_TABLE} (
                    client_strategy_id, tf, direction,
                    window_hours, window_start, window_end, deposit, n_total,
                    roi_base, roi_bin, best_k, roi_best,
                    n_blocked_best, n_allowed_best, uplift_abs, uplift_rel_pct,
                    criteria_note
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17
                )
                """,
                active_rows
            )

    # публикация KV (после фиксации БД)
    await _publish_active_kv(kv_items)

    # лог результата прогона
    # подготовим компактную сводку TF
    tf_seen = sorted({tf for _, tf, _, _ in kv_items}, key=lambda x: tf_order.get(x, 9))
    log.info(
        "[BL] ✅ окно[%s..%s] групп=%d scan_rows=%d summary_rows=%d active_rows=%d kv_pairs=%d tfs=%s",
        window_start.isoformat(), window_end.isoformat(),
        processed, len(scan_rows), len(summary_rows), len(active_rows), len(kv_items),
        ",".join(tf_seen) if tf_seen else "-"
    )


# 🔸 Главный цикл анализатора (ежечасно, со стартовой задержкой 90s)
async def run_laboratory_bl_analyzer():
    """
    Ежечасный анализ влияния blacklist по окну 7×24ч на уровне csid×TF×direction:
    - строит ROI-профиль по порогам K (scan),
    - выбирает best_k по критерию max ROI (baseline K=0 участвует),
    - пишет историю часа (summary), пересобирает активный срез (summary_active),
    - публикует в Redis компактные KV-ключи: laboratory:bl:k:{master_sid}:{tf} = "<best_k>".
    Учитываются только стратегии, где strategies_v4.blacklist_watcher = true.
    """
    log.debug("🛰️ LAB_BL_ANALYZER запущен: WINDOW=%dh, EVERY=%ds", WINDOW_HOURS, RUN_EVERY_SEC)
    log.info("⏳ LAB_BL_ANALYZER: старт через %ds (grace period после загрузки)", START_DELAY_SEC)
    await asyncio.sleep(START_DELAY_SEC)

    while True:
        try:
            await _run_once(WINDOW_HOURS)
        except asyncio.CancelledError:
            log.debug("⏹️ LAB_BL_ANALYZER остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ LAB_BL_ANALАЛYZER ошибка прогона")
        # пауза до следующего часа
        await asyncio.sleep(RUN_EVERY_SEC)