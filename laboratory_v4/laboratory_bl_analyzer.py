# laboratory_bl_analyzer.py — оффлайн-аналитика BL: ROI(K) по окну 7×24ч, best_k и «активный» срез

# 🔸 Импорты
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Tuple, Optional, Iterable

# 🔸 Инфраструктура
import laboratory_infra as infra

# 🔸 Логгер
log = logging.getLogger("LAB_BL_ANALYZER")

# 🔸 Параметры окна/периодичности и критерии стабильности
WINDOW_HOURS = 168          # 7 * 24
RUN_EVERY_SEC = 3600        # раз в час
N_MIN_ALLOWED = 30          # минимальное число сделок после фильтра
DELTA_MIN_ROI = Decimal("0.02")  # минимальный прирост ROI (2 п.п.)

# 🔸 Таблицы
LPS_TABLE = "public.laboratoty_position_stat"
POS_TABLE = "public.positions_v4"
STRAT_TABLE = "public.strategies_v4"
SCAN_TABLE = "public.laboratory_bl_scan"
SUMMARY_TABLE = "public.laboratory_bl_summary"
ACTIVE_TABLE = "public.laboratory_bl_summary_active"


# 🔸 Вспомогательная структура ряда
class Row:
    __slots__ = ("csid", "tf", "direction", "bl_hits", "pnl")
    def __init__(self, csid: int, tf: str, direction: str, bl_hits: int, pnl: Decimal):
        self.csid = csid
        self.tf = tf
        self.direction = direction
        self.bl_hits = bl_hits
        self.pnl = pnl


# 🔸 Забор данных в окно по закрытым позициям → ряды уровня LPS (по TF)
async def _fetch_rows(window_start: datetime, window_end: datetime) -> List[Row]:
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                lps.client_strategy_id AS csid,
                lps.tf,
                lps.direction,
                COALESCE(lps.pack_bl_match_count, 0) AS bl_hits,
                COALESCE(lps.pnl, p.pnl) AS pnl
            FROM {LPS_TABLE} lps
            JOIN {POS_TABLE} p
              ON p.log_uid = lps.log_uid
             AND p.strategy_id = lps.client_strategy_id
            WHERE p.status = 'closed'
              AND p.closed_at > $1 AND p.closed_at <= $2
              AND COALESCE(lps.pnl, p.pnl) IS NOT NULL
            """,
            window_start, window_end
        )
    out: List[Row] = []
    for r in rows:
        try:
            out.append(Row(int(r["csid"]), str(r["tf"]), str(r["direction"]), int(r["bl_hits"]), Decimal(r["pnl"])))
        except Exception:
            # если встречаются некорректные строки — просто пропускаем
            continue
    return out


# 🔸 Получить депозиты по csid (client_strategy_id)
async def _fetch_deposits(csids: Iterable[int]) -> Dict[int, Decimal]:
    uniq = sorted({int(x) for x in csids if x is not None})
    if not uniq:
        return {}
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT id, deposit FROM {STRAT_TABLE} WHERE id = ANY($1::int[])",
            uniq
        )
    out: Dict[int, Decimal] = {}
    for r in rows:
        dep = r["deposit"]
        if dep is None:
            continue
        try:
            out[int(r["id"])] = Decimal(dep)
        except Exception:
            continue
    return out


# 🔸 Расчёт профиля ROI(K) + выбор best_k для одной группы csid×TF×direction
def _compute_profile(rows: List[Row], deposit: Decimal) -> Tuple[Dict[int, Dict], Dict]:
    # rows: все сделки (LPS-строки) в группе
    if not rows or deposit is None or deposit <= 0:
        return {}, {}

    n_total = len(rows)
    pnl_total = sum((r.pnl for r in rows), Decimal("0"))
    roi_base = (pnl_total / deposit) if deposit else Decimal("0")

    kmax = max((max(1, r.bl_hits) for r in rows), default=1)  # хотя бы 1
    profile: Dict[int, Dict] = {}

    best_k = 1
    best_roi = roi_base
    best_allowed = n_total
    best_blocked = 0

    for k in range(1, kmax + 1):
        allowed = [r for r in rows if r.bl_hits < k]
        blocked = n_total - len(allowed)
        pnl_allowed = sum((r.pnl for r in allowed), Decimal("0"))
        roi_k = (pnl_allowed / deposit) if deposit else Decimal("0")

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

        # условия достаточности
        if len(allowed) >= N_MIN_ALLOWED and (roi_k - roi_base) >= DELTA_MIN_ROI:
            if roi_k > best_roi:
                best_roi = roi_k
                best_k = k
                best_allowed = len(allowed)
                best_blocked = blocked

    summary = {
        "n_total": n_total,
        "roi_base": roi_base,
        "roi_bin": profile.get(1, {}).get("roi_k", roi_base),  # K=1
        "best_k": best_k,
        "roi_best": best_roi,
        "n_allowed_best": best_allowed,
        "n_blocked_best": best_blocked,
        "uplift_abs": (best_roi - roi_base),
        "uplift_rel_pct": ((best_roi - roi_base) / (abs(roi_base) if roi_base != 0 else Decimal("1"))),
    }

    return profile, summary


# 🔸 Один прогон: построить профиль и сводку по всем csid×TF×direction и записать в таблицы
async def _run_once(window_hours: int):
    now_utc = datetime.utcnow().replace(tzinfo=None)
    window_end = now_utc
    window_start = window_end - timedelta(hours=window_hours)

    rows = await _fetch_rows(window_start, window_end)
    if not rows:
        log.info("[BL] ℹ️ окно[%s..%s]: нет данных для анализа", window_start.isoformat(), window_end.isoformat())
        return

    # группировка по csid×TF×direction
    groups: Dict[Tuple[int, str, str], List[Row]] = {}
    for r in rows:
        groups.setdefault((r.csid, r.tf, r.direction), []).append(r)

    deposits = await _fetch_deposits(csid for csid, _, _ in groups.keys())

    scan_rows: List[Tuple] = []     # для laboratory_bl_scan
    summary_rows: List[Tuple] = []  # для laboratory_bl_summary
    active_rows: List[Tuple] = []   # для laboratory_bl_summary_active

    processed = 0

    for (csid, tf, direction), vec in groups.items():
        dep = deposits.get(csid)
        if dep is None or dep <= 0:
            # пропускаем группу без депозита
            log.info("[BL] ⚠️ пропуск csid=%s tf=%s dir=%s: deposit отсутствует/<=0", csid, tf, direction)
            continue

        profile, summary = _compute_profile(vec, dep)
        if not profile:
            continue

        # профиль по всем K — складываем
        for k, p in profile.items():
            scan_rows.append((
                window_start, window_end, csid, tf, direction,            # ключ
                p["threshold_k"], p["n_total"], p["n_blocked"], p["n_allowed"],
                p["pnl_total"], p["pnl_allowed"], p["deposit"],
                p["roi_base"], p["roi_k"]
            ))

        # сводка (best_k + базовые)
        summary_rows.append((
            window_start, window_end, csid, tf, direction,
            dep, summary["n_total"],
            summary["roi_base"], summary["roi_bin"],
            summary["best_k"], summary["roi_best"],
            summary["n_blocked_best"], summary["n_allowed_best"],
            summary["uplift_abs"], summary["uplift_rel_pct"],
            f"Nmin={N_MIN_ALLOWED};Δmin={str(DELTA_MIN_ROI)}"
        ))

        # активный срез
        active_rows.append((
            csid, tf, direction,
            WINDOW_HOURS, window_start, window_end, dep, summary["n_total"],
            summary["roi_base"], summary["roi_bin"],
            summary["best_k"], summary["roi_best"],
            summary["n_blocked_best"], summary["n_allowed_best"],
            summary["uplift_abs"], summary["uplift_rel_pct"],
            f"Nmin={N_MIN_ALLOWED};Δmin={str(DELTA_MIN_ROI)}"
        ))

        processed += 1

    if not scan_rows:
        log.info("[BL] ℹ️ окно[%s..%s]: групп после фильтра нет", window_start.isoformat(), window_end.isoformat())
        return

    # запись в БД: профиль, сводка и активный срез
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

    # лог результата прогона
    log.info(
        "[BL] ✅ окно[%s..%s] групп=%d scan_rows=%d summary_rows=%d active_rows=%d",
        window_start.isoformat(), window_end.isoformat(),
        processed, len(scan_rows), len(summary_rows), len(active_rows)
    )


# 🔸 Главный цикл анализатора (раз в час, скользящее окно 7×24ч) со стартовой задержкой 90s
async def run_laboratory_bl_analyzer():
    """
    Ежечасный анализ влияния blacklist по окну 7×24ч на уровне csid×TF×direction:
    - строит ROI-профиль по порогам K (scan),
    - выбирает best_k по критериям стабильности,
    - пишет историю часа (summary) и полностью пересобирает активный срез (summary_active).
    Стартовая задержка 90 секунд после загрузки сервиса.
    """
    log.debug("🛰️ LAB_BL_ANALYZER запущен: WINDOW=%dh, EVERY=%ds", WINDOW_HOURS, RUN_EVERY_SEC)

    # стартовый grace period
    start_delay_sec = 90
    log.info("⏳ LAB_BL_ANALYZER: старт через %ds (grace period после загрузки)", start_delay_sec)
    await asyncio.sleep(start_delay_sec)

    while True:
        try:
            await _run_once(WINDOW_HOURS)
        except asyncio.CancelledError:
            log.debug("⏹️ LAB_BL_ANALYZER остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ LAB_BL_ANALYZER ошибка прогона")
        # пауза до следующего часа
        await asyncio.sleep(RUN_EVERY_SEC)