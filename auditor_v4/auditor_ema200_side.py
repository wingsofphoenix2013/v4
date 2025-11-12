# 🔸 auditor_ema200_side.py — аудит идеи ema200_side:
#     — на входе позиции вычисляет по TF (m5/m15/h1): side (price vs ema200) и dist = |price-ema200|/atr14
#     — пишет в БД: runs / coverage / side_stats (aligned/opposite/equal) / bin_stats по dist (обычно для aligned)
#     — формирует три маски, совместимые с тест-режимом: m5_only / m5_m15 / m5_m15_h1
#     — выбирает primary-маску как best-of-three на основном окне (ΔROI ↓, затем ROI_sel, затем conf, затем coverage)
#     — публикует лучший вариант в оркестратор витрины (Redis Stream auditor:best:candidates) с idea_key='ema200_side'

# 🔸 Импорты
import asyncio
import logging
import datetime as dt
import uuid
import json
from typing import Dict, List, Tuple, Optional, Iterable, Any, Set

import auditor_infra as infra
from auditor_config import load_active_mw_strategies

# 🔸 Логгер
log = logging.getLogger("AUD_EMA200")

# 🔸 Константы аудита (правим здесь, не через ENV)
WINDOWS: List[Tuple[str, Optional[int]]] = [("7d", 7), ("14d", 14), ("28d", 28), ("total", None)]
TIMEFRAMES: Tuple[str, ...] = ("m5", "m15", "h1")
MIN_SAMPLE_PER_CELL = 50                 # пометка «мало наблюдений»
INITIAL_DELAY_SEC = 0                    # стартовую задержку контролируем из main
SLEEP_BETWEEN_RUNS_SEC = 3 * 60 * 60     # пауза между проходами (3 часа)

# 🔸 Пороговые правила классификации dist по бинам (аналог cross-strength)
DELTA_ROI_HIGH_PP = 5.0   # ΔROI(B5−B1) ≥ 5 п.п. → HIGH
DELTA_ROI_LOW_PP  = -5.0  # ΔROI(B5−B1) ≤ -5 п.п. → LOW
U_SHAPE_MIN_GAIN  = 3.0   # ROI(B3) − max(ROI(B1), ROI(B5)) ≥ 3 п.п. → MID

# 🔸 Порог выбора «основного окна»
PRIMARY_28D_COVERAGE = 0.80
PRIMARY_14D_COVERAGE = 0.70
SECONDARY_MIN_COVER  = 0.50

# 🔸 Квантили-биннинг для dist
MASK_BINS: Dict[str, Set[int]] = {
    "any": {1, 2, 3, 4, 5},
    "low": {1, 2, 3},          # ≤ Q60
    "mid": {2, 3, 4},          # Q20..Q80
    "high": {4, 5},            # ≥ Q60 (базово; можно ужесточить до ≥Q80)
}
MASK_QBOUNDS: Dict[str, Tuple[Optional[int], Optional[int]]] = {
    "any":  (None, None),
    "low":  (0, 60),
    "mid":  (20, 80),
    "high": (60, 100),
}

# 🔸 Маппинг side
def _side_state(entry_price: float, ema200: float, direction: str) -> str:
    if ema200 is None:
        return "equal"
    # equal – редкий случай; сведём к aligned/opposite по порогу можно позже
    if direction == "long":
        return "aligned" if entry_price >= ema200 else "opposite"
    else:
        return "aligned" if entry_price <= ema200 else "opposite"

# 🔸 Выбор primary окна по покрытию
def _choose_primary_window(cov_map: Dict[str, Dict[str, Any]]) -> str:
    cov28 = cov_map.get("28d", {}).get("window_coverage_pct", 0.0)
    cov14 = cov_map.get("14d", {}).get("window_coverage_pct", 0.0)
    if cov28 >= PRIMARY_28D_COVERAGE * 100.0:
        return "28d"
    if cov14 >= PRIMARY_14D_COVERAGE * 100.0:
        return "14d"
    return "7d"

# 🔸 Выбор secondary окна (для проверки знака)
def _choose_secondary_window(cov_map: Dict[str, Dict[str, Any]], primary: str) -> Optional[str]:
    if primary == "28d":
        if cov_map.get("14d", {}).get("window_coverage_pct", 0.0) >= SECONDARY_MIN_COVER * 100.0:
            return "14d"
        if cov_map.get("7d", {}).get("window_coverage_pct", 0.0) >= SECONDARY_MIN_COVER * 100.0:
            return "7d"
        return None
    if primary == "14d":
        if cov_map.get("7d", {}).get("window_coverage_pct", 0.0) >= SECONDARY_MIN_COVER * 100.0:
            return "7d"
        return None
    # primary = '7d'
    return None

# 🔸 Точка входа воркера
async def run_auditor_ema200_side():
    # условия достаточности
    if infra.pg_pool is None:
        log.debug("❌ Пропуск auditor_ema200_side: PG не инициализирован")
        return

    if INITIAL_DELAY_SEC > 0:
        log.debug("⏳ AUD_EMA200: ожидание %d сек перед первым запуском", INITIAL_DELAY_SEC)
        await asyncio.sleep(int(INITIAL_DELAY_SEC))

    # основной цикл
    while True:
        try:
            await _run_once()
        except asyncio.CancelledError:
            log.debug("⏹️ AUD_EMA200: остановлено по сигналу")
            raise
        except Exception:
            log.exception("❌ AUD_EMA200: ошибка прохода — пауза 5 секунд")
            await asyncio.sleep(5)

        log.debug("😴 AUD_EMA200: пауза %d сек до следующего запуска", SLEEP_BETWEEN_RUNS_SEC)
        await asyncio.sleep(int(SLEEP_BETWEEN_RUNS_SEC))


# 🔸 Один проход: создать run, пройти по стратегиям, записать покрытие/статы/маски и опубликовать лучший вариант
async def _run_once():
    strategies = await load_active_mw_strategies()
    log.debug("📦 AUD_EMA200: найдено активных MW-стратегий: %d", len(strategies))
    if not strategies:
        return

    # фиксируем "сейчас" и окна
    now_utc = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    win_bounds = {"7d": now_utc - dt.timedelta(days=7),
                  "14d": now_utc - dt.timedelta(days=14),
                  "28d": now_utc - dt.timedelta(days=28),
                  "total": None}
    log.debug("🕒 AUD_EMA200: окна — now=%s; 7d>=%s; 14d>=%s; 28d>=%s",
             now_utc, win_bounds["7d"], win_bounds["14d"], win_bounds["28d"])

    run_id = await _create_run(now_utc, win_bounds)
    log.debug("🧾 AUD_EMA200: создан run_id=%s", run_id)

    for sid, meta in strategies.items():
        try:
            await _process_strategy(run_id, sid, meta, now_utc, win_bounds)
        except Exception:
            log.exception("❌ AUD_EMA200: ошибка обработки стратегии sid=%s", sid)


# 🔸 Создать запись в auditor_ema200_side_runs
async def _create_run(now_utc: dt.datetime, win_bounds: Dict[str, Optional[dt.datetime]]) -> int:
    async with infra.pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO auditor_ema200_side_runs (now_utc, window_7d_from, window_14d_from, window_28d_from)
            VALUES ($1, $2, $3, $4)
            RETURNING id
            """,
            now_utc, win_bounds["7d"], win_bounds["14d"], win_bounds["28d"]
        )
        return int(row["id"])


# 🔸 Полная обработка стратегии
async def _process_strategy(
    run_id: int,
    sid: int,
    meta: Dict[str, Any],
    now_utc: dt.datetime,
    win_bounds: Dict[str, Optional[dt.datetime]],
):
    name = meta.get("name") or f"sid_{sid}"
    human = meta.get("human_name") or ""
    title = f'{sid} "{name}"' if not human else f'{sid} "{name}" ({human})'

    # депозит стратегии (аксиома: > 0)
    deposit = await _load_strategy_deposit(sid)

    # закрытые позиции
    positions = await _load_closed_positions_for_strategy(sid)
    if not positions:
        log.debug("ℹ️ AUD_EMA200: %s — закрытых позиций нет", title)
        return

    # принадлежность окнам
    for p in positions:
        ca = p["closed_at"]
        p["in_window"] = {
            "7d":  (ca is not None and ca >= win_bounds["7d"]),
            "14d": (ca is not None and ca >= win_bounds["14d"]),
            "28d": (ca is not None and ca >= win_bounds["28d"]),
            "total": True,
        }

    # покрытие по окнам
    coverage = _compute_coverage(positions, now_utc)
    await _insert_coverage_rows(run_id, sid, coverage)

    # снапшоты ema200/atr14 по TF (m5/m15/h1)
    pos_uids = [p["position_uid"] for p in positions]
    snaps = await _load_indicator_snapshots_for_positions(pos_uids)

    # подготовка контейнеров:
    # pos_side[tf][w][dir][puid] -> 'aligned'|'opposite'|'equal'
    pos_side: Dict[str, Dict[str, Dict[str, Dict[str, str]]]] = {
        tf: {w: {"long": {}, "short": {}} for w, _ in WINDOWS} for tf in TIMEFRAMES
    }
    # pos_dist_bins[tf][w][dir][puid] -> bin_index 1..5
    pos_dist_bins: Dict[str, Dict[str, Dict[str, Dict[str, int]]]] = {
        tf: {w: {"long": {}, "short": {}} for w, _ in WINDOWS} for tf in TIMEFRAMES
    }

    # Для биннинга dist: соберём per-symbol dist-листы на (tf,w,dir)
    dist_values: Dict[str, Dict[str, Dict[str, Dict[str, List[float]]]]] = {
        tf: {w: {"long": {}, "short": {}} for w, _ in WINDOWS} for tf in TIMEFRAMES
    }

    # Пройти по позициям: вычислить side и dist, накопить dist_values
    for p in positions:
        puid = p["position_uid"]; sym = p["symbol"]; direction = p["direction"]
        entry = float(p["entry_price"] or 0.0)
        for tf in TIMEFRAMES:
            key = (puid, tf)
            snap = snaps.get(key)
            if not snap:
                continue
            ema200 = snap.get("ema200"); atr14 = snap.get("atr14")
            if ema200 is None or atr14 is None or atr14 <= 0:
                continue
            side = _side_state(entry, ema200, direction)
            dist = abs(entry - float(ema200)) / float(atr14)
            for w, _days in WINDOWS:
                if p["in_window"][w]:
                    pos_side[tf][w][direction][puid] = side
                    dl = dist_values[tf][w][direction].setdefault(sym, [])
                    dl.append(dist)

    # Биннинг dist по per-symbol квантилям и запись pos_dist_bins
    for tf in TIMEFRAMES:
        for w, _days in WINDOWS:
            for direction in ("long", "short"):
                sym_map = dist_values[tf][w][direction]
                for sym, arr in sym_map.items():
                    if not arr:
                        continue
                    edges = _quantile_edges(arr, (0.2, 0.4, 0.6, 0.8))
                    # присвоить бины всем позициям этого symbol/dir/tf/w
                    # пройдём повторно по позициям и тем же условиям
                    for p in positions:
                        if p["symbol"] != sym or not p["in_window"][w] or direction != p["direction"]:
                            continue
                        puid = p["position_uid"]
                        snap = snaps.get((puid, tf))
                        if not snap:
                            continue
                        ema200 = snap.get("ema200"); atr14 = snap.get("atr14")
                        if ema200 is None or atr14 is None or atr14 <= 0:
                            continue
                        entry = float(p["entry_price"] or 0.0)
                        dist = abs(entry - float(ema200)) / float(atr14)
                        b = _assign_bin(dist, edges)
                        pos_dist_bins[tf][w][direction][puid] = b

    # SIDE_STATS и BIN_STATS (для aligned)
    async with infra.pg_pool.acquire() as conn:
        for tf in TIMEFRAMES:
            for w, _days in WINDOWS:
                for direction in ("long", "short"):
                    # side_stats
                    side_aggr = {"aligned": {"N": 0, "wins": 0, "pnl": 0.0},
                                 "opposite": {"N": 0, "wins": 0, "pnl": 0.0},
                                 "equal": {"N": 0, "wins": 0, "pnl": 0.0}}
                    total_n = 0
                    for p in positions:
                        if not p["in_window"][w] or p["direction"] != direction:
                            continue
                        puid = p["position_uid"]
                        s = pos_side[tf][w][direction].get(puid)
                        if not s:
                            continue
                        total_n += 1
                        rec = side_aggr[s]
                        rec["N"] += 1
                        rec["wins"] += 1 if p["pnl"] >= 0 else 0
                        rec["pnl"] += float(p["pnl"] or 0.0)

                    # лог и запись side_stats
                    warn = " (N<50)" if total_n < MIN_SAMPLE_PER_CELL else ""
                    log.debug('📊 AUD_EMA200 | %s | TF=%s | dir=%s | window=%s — side_stats%s',
                             title, tf, direction, w, warn)
                    for s_key in ("aligned", "opposite", "equal"):
                        ag = side_aggr[s_key]
                        N = int(ag["N"]); wins = int(ag["wins"]); pnl_sum = float(ag["pnl"])
                        roi_pct = (pnl_sum / float(deposit) * 100.0) if deposit > 0 and N > 0 else 0.0
                        log.debug("  side=%s: N=%d WR=%.2f%% ΣPnL=%.6f ROI=%.4f%%",
                                 s_key, N, (wins / N * 100.0) if N > 0 else 0.0, pnl_sum, roi_pct)
                        await conn.execute(
                            """
                            INSERT INTO auditor_ema200_side_side_stats
                            (run_id, strategy_id, timeframe, direction, window_tag, side_state,
                             n, wins, pnl_sum, deposit_used, roi_pct)
                            VALUES ($1,$2,$3,$4,$5,$6, $7,$8,$9,$10,$11)
                            """,
                            run_id, sid, tf, direction, w, s_key,
                            N, wins, pnl_sum, float(deposit), float(roi_pct)
                        )

                    # bin_stats для aligned
                    btot = {1: {"N": 0, "wins": 0, "pnl": 0.0},
                            2: {"N": 0, "wins": 0, "pnl": 0.0},
                            3: {"N": 0, "wins": 0, "pnl": 0.0},
                            4: {"N": 0, "wins": 0, "pnl": 0.0},
                            5: {"N": 0, "wins": 0, "pnl": 0.0}}
                    for p in positions:
                        if not p["in_window"][w] or p["direction"] != direction:
                            continue
                        puid = p["position_uid"]
                        if pos_side[tf][w][direction].get(puid) != "aligned":
                            continue
                        b = pos_dist_bins[tf][w][direction].get(puid)
                        if b is None:
                            continue
                        rec = btot[b]
                        rec["N"] += 1
                        rec["wins"] += 1 if p["pnl"] >= 0 else 0
                        rec["pnl"] += float(p["pnl"] or 0.0)

                    # лог и запись bin_stats
                    for idx in (1, 2, 3, 4, 5):
                        rec = btot[idx]
                        N = int(rec["N"]); wins = int(rec["wins"]); pnl_sum = float(rec["pnl"])
                        roi_pct = (pnl_sum / float(deposit) * 100.0) if deposit > 0 and N > 0 else 0.0
                        log.debug("  dist_bin B%d: N=%d WR=%.2f%% ΣPnL=%.6f ROI=%.4f%%",
                                 idx, N, (wins / N * 100.0) if N > 0 else 0.0, pnl_sum, roi_pct)
                        await conn.execute(
                            """
                            INSERT INTO auditor_ema200_side_bin_stats
                            (run_id, strategy_id, timeframe, direction, window_tag, side_state,
                             bin_index, n, wins, pnl_sum, deposit_used, roi_pct, clip_applied, clip_p99)
                            VALUES ($1,$2,$3,$4,$5,$6, $7,$8,$9,$10,$11,$12,$13,$14)
                            """,
                            run_id, sid, tf, direction, w, "aligned",
                            idx, N, wins, pnl_sum, float(deposit), float(roi_pct), False, None
                        )

    # Выбор primary окна
    for direction in ("long", "short"):
        dir_positions = [p for p in positions if p["direction"] == direction]
        if not dir_positions:
            continue

        primary_win = _choose_primary_window(coverage.get(direction, {}))
        secondary_win = _choose_secondary_window(coverage.get(direction, {}), primary_win)

        # классификация m5_dist_mode по aligned/bin_stats
        m5_mode = _classify_dist_mode_from_bins(
            _aggregate_bins_for(tf="m5", window=primary_win, direction=direction,
                                positions=positions, pos_side=pos_side, pos_bins=pos_dist_bins),
            deposit
        )
        # три маски
        masks: List[Tuple[str, Dict[str, str], str]] = [
            ("m5_only",     {"m5_side": "aligned", "m5_dist_mode": m5_mode,
                              "m15_side": "any",     "m15_dist_mode": "any",
                              "h1_side": "any",      "h1_dist_mode": "any"}, "mask=m5_only"),
            ("m5_m15",      {"m5_side": "aligned", "m5_dist_mode": m5_mode,
                              "m15_side": "aligned", "m15_dist_mode": "any",
                              "h1_side": "any",      "h1_dist_mode": "any"}, "mask=m5_m15"),
            ("m5_m15_h1",   {"m5_side": "aligned", "m5_dist_mode": m5_mode,
                              "m15_side": "aligned", "m15_dist_mode": "any",
                              "h1_side": "aligned",  "h1_dist_mode": "any"}, "mask=m5_m15_h1"),
        ]

        # оценим маски на primary и выберем лучшую
        ranking: List[Tuple[float, float, float, float, int]] = []  # (eligible, delta_roi, roi_sel, conf, coverage, idx)
        mask_metrics_primary: List[Dict[str, Any]] = []
        mask_metrics_all_primary: List[Dict[str, Any]] = []
        for idx, (_label, mask_modes, _note) in enumerate(masks):
            m_sel = _evaluate_mask_on_positions_side(dir_positions, pos_side, pos_dist_bins, mask_modes, primary_win, direction, deposit)
            m_all = _evaluate_mask_on_positions_side(dir_positions, pos_side, pos_dist_bins,
                                                     {"m5_side":"any","m5_dist_mode":"any",
                                                      "m15_side":"any","m15_dist_mode":"any",
                                                      "h1_side":"any","h1_dist_mode":"any"},
                                                     primary_win, direction, deposit)
            mask_metrics_primary.append(m_sel)
            mask_metrics_all_primary.append(m_all)

            # confidence по той же логике (через secondary)
            m_sel_sec = m_all_sec = None
            if secondary_win is not None:
                m_sel_sec = _evaluate_mask_on_positions_side(dir_positions, pos_side, pos_dist_bins, mask_modes, secondary_win, direction, deposit)
                m_all_sec = _evaluate_mask_on_positions_side(dir_positions, pos_side, pos_dist_bins,
                                                             {"m5_side":"any","m5_dist_mode":"any",
                                                              "m15_side":"any","m15_dist_mode":"any",
                                                              "h1_side":"any","h1_dist_mode":"any"},
                                                             secondary_win, direction, deposit)
            _, dec_conf, _ = _make_decision(primary_win, coverage.get(direction, {}), m_sel, m_all, m_sel_sec, m_all_sec)

            roi_sel = m_sel["roi_selected_pct"]; roi_all = m_sel["roi_all_pct"]
            delta_roi = (roi_sel - roi_all) if (roi_sel is not None and roi_all is not None) else float("-inf")
            eligible = 1.0 if (roi_sel is not None and roi_sel > 0.0) else 0.0
            ranking.append((eligible, delta_roi, (roi_sel or -1e9), dec_conf, m_sel["coverage_pct"], idx))

        ranking.sort(reverse=True, key=lambda t: (t[0], t[1], t[2], t[3], t[4]))
        best_idx = ranking[0][-1]

        # записываем все три маски в БД (primary у best_idx)
        for idx, (label, mask_modes, note) in enumerate(masks):
            is_primary_mask = (idx == best_idx)
            await _record_mask_with_validation(
                run_id=run_id, sid=sid, title=title, direction=direction,
                primary_win=primary_win, secondary_win=secondary_win,
                coverage_dir=coverage.get(direction, {}),
                dir_positions=dir_positions, pos_side=pos_side, pos_bins=pos_dist_bins,
                deposit=deposit, mask_modes=mask_modes,
                is_primary=is_primary_mask, primary_note=note
            )

        # публикуем лучший вариант в оркестратор (всегда; если ROI<=0 — eligible=false)
        best_label, best_mask, _ = masks[best_idx]
        best_sel = mask_metrics_primary[best_idx]
        best_all = mask_metrics_all_primary[best_idx]
        best_sel_sec = best_all_sec = None
        if secondary_win is not None:
            best_sel_sec = _evaluate_mask_on_positions_side(dir_positions, pos_side, pos_dist_bins, best_mask, secondary_win, direction, deposit)
            best_all_sec = _evaluate_mask_on_positions_side(dir_positions, pos_side, pos_dist_bins,
                                                            {"m5_side":"any","m5_dist_mode":"any",
                                                             "m15_side":"any","m15_dist_mode":"any",
                                                             "h1_side":"any","h1_dist_mode":"any"},
                                                            secondary_win, direction, deposit)
        _, best_conf, _ = _make_decision(primary_win, coverage.get(direction, {}), best_sel, best_all, best_sel_sec, best_all_sec)

        await _publish_best_candidate(
            run_id=run_id, sid=sid, direction=direction, primary_win=primary_win,
            label=best_label, mask_modes=best_mask, metrics_sel=best_sel, metrics_all=best_all,
            decision_conf=best_conf
        )


# 🔸 Агрегатор бинов для классификации dist_mode по m5 (aligned)
def _aggregate_bins_for(tf: str, window: str, direction: str,
                        positions: List[Dict[str, Any]],
                        pos_side: Dict[str, Dict[str, Dict[str, Dict[str, str]]]],
                        pos_bins: Dict[str, Dict[str, Dict[str, Dict[str, int]]]]) -> Optional[Dict[int, Dict[str, float]]]:
    if tf not in TIMEFRAMES:
        return None
    btot = {1: {"N": 0, "wins": 0, "pnl": 0.0},
            2: {"N": 0, "wins": 0, "pnl": 0.0},
            3: {"N": 0, "wins": 0, "pnl": 0.0},
            4: {"N": 0, "wins": 0, "pnl": 0.0},
            5: {"N": 0, "wins": 0, "pnl": 0.0}}
    for p in positions:
        if not p["in_window"][window] or p["direction"] != direction:
            continue
        puid = p["position_uid"]
        if pos_side[tf][window][direction].get(puid) != "aligned":
            continue
        b = pos_bins[tf][window][direction].get(puid)
        if b is None:
            continue
        rec = btot[b]
        rec["N"] += 1
        rec["wins"] += 1 if p["pnl"] >= 0 else 0
        rec["pnl"] += float(p["pnl"] or 0.0)
    return btot


# 🔸 Классификация dist_mode по bin-агрегатам (m5)
def _classify_dist_mode_from_bins(bins: Optional[Dict[int, Dict[str, float]]], deposit: float) -> str:
    if not bins:
        return "any"

    def roi_pp(pnl: float) -> float:
        return (pnl / float(deposit) * 100.0) if deposit > 0 else 0.0

    r1 = roi_pp(bins[1]["pnl"]); r3 = roi_pp(bins[3]["pnl"]); r5 = roi_pp(bins[5]["pnl"])
    d_roi = r5 - r1
    if (r3 - max(r1, r5)) >= U_SHAPE_MIN_GAIN:
        return "mid"
    if d_roi >= DELTA_ROI_HIGH_PP:
        return "high"
    if d_roi <= DELTA_ROI_LOW_PP:
        return "low"
    return "any"


# 🔸 Запись масок + валидации
async def _record_mask_with_validation(
    run_id: int,
    sid: int,
    title: str,
    direction: str,
    primary_win: str,
    secondary_win: Optional[str],
    coverage_dir: Dict[str, Dict[str, Any]],
    dir_positions: List[Dict[str, Any]],
    pos_side: Dict[str, Dict[str, Dict[str, Dict[str, str]]]],
    pos_bins: Dict[str, Dict[str, Dict[str, Dict[str, int]]]],
    deposit: float,
    mask_modes: Dict[str, str],
    is_primary: bool,
    primary_note: str,
):
    metrics_sel_primary = _evaluate_mask_on_positions_side(dir_positions, pos_side, pos_bins, mask_modes, primary_win, direction, deposit)
    metrics_all_primary = _evaluate_mask_on_positions_side(dir_positions, pos_side, pos_bins,
                                                           {"m5_side":"any","m5_dist_mode":"any",
                                                            "m15_side":"any","m15_dist_mode":"any",
                                                            "h1_side":"any","h1_dist_mode":"any"},
                                                           primary_win, direction, deposit)

    metrics_sel_secondary = metrics_all_secondary = None
    if secondary_win is not None:
        metrics_sel_secondary = _evaluate_mask_on_positions_side(dir_positions, pos_side, pos_bins, mask_modes, secondary_win, direction, deposit)
        metrics_all_secondary = _evaluate_mask_on_positions_side(dir_positions, pos_side, pos_bins,
                                                                 {"m5_side":"any","m5_dist_mode":"any",
                                                                  "m15_side":"any","m15_dist_mode":"any",
                                                                  "h1_side":"any","h1_dist_mode":"any"},
                                                                 secondary_win, direction, deposit)

    decision_class, decision_conf, rationale = _make_decision(
        primary_window=primary_win, cov_map_dir=coverage_dir,
        metrics_primary=metrics_sel_primary, metrics_base_primary=metrics_all_primary,
        metrics_secondary=metrics_sel_secondary, metrics_base_secondary=metrics_all_secondary
    )

    await _insert_mask_result(
        run_id=run_id, sid=sid, direction=direction, window_tag=primary_win,
        is_primary=is_primary, primary_window=primary_win,
        mask_modes=mask_modes, metrics_sel=metrics_sel_primary, metrics_all=metrics_all_primary,
        decision_class=decision_class, decision_confidence=decision_conf,
        rationale=(primary_note if primary_note else rationale)
    )

    tag = "DECISION" if is_primary else "ALT"
    d_roi_pp = 0.0
    if metrics_sel_primary["roi_selected_pct"] is not None and metrics_sel_primary["roi_all_pct"] is not None:
        d_roi_pp = metrics_sel_primary["roi_selected_pct"] - metrics_sel_primary["roi_all_pct"]
    log.debug(
        "✅ %s | sid=%s | dir=%s | primary=%s | class=%s (conf=%.2f) | mask: %s | ΔROI=%.2f pp | ΔWR=%.2f pp",
        tag, sid, direction, primary_win, decision_class, decision_conf,
        json.dumps(mask_modes), d_roi_pp,
        metrics_sel_primary["wr_selected_pct"] - metrics_sel_primary["wr_all_pct"]
    )

    # валидации на других окнах
    for w, _ in WINDOWS:
        if w == primary_win:
            continue
        m_sel = _evaluate_mask_on_positions_side(dir_positions, pos_side, pos_bins, mask_modes, w, direction, deposit)
        m_all = _evaluate_mask_on_positions_side(dir_positions, pos_side, pos_bins,
                                                 {"m5_side":"any","m5_dist_mode":"any",
                                                  "m15_side":"any","m15_dist_mode":"any",
                                                  "h1_side":"any","h1_dist_mode":"any"},
                                                 w, direction, deposit)
        if m_all["n_all"] > 0:
            await _insert_mask_result(
                run_id=run_id, sid=sid, direction=direction, window_tag=w,
                is_primary=False, primary_window=primary_win,
                mask_modes=mask_modes, metrics_sel=m_sel, metrics_all=m_all,
                decision_class=decision_class, decision_confidence=max(0.0, decision_conf - 0.1),
                rationale=("validation-window; " + (primary_note or ""))
            )


# 🔸 Оценка маски для ema200_side
def _evaluate_mask_on_positions_side(
    dir_positions: List[Dict[str, Any]],
    pos_side: Dict[str, Dict[str, Dict[str, Dict[str, str]]]],
    pos_bins: Dict[str, Dict[str, Dict[str, Dict[str, int]]]],
    mask_modes: Dict[str, str],  # {'m5_side','m5_dist_mode','m15_side','m15_dist_mode','h1_side','h1_dist_mode'}
    window_tag: str,
    direction: str,
    deposit: float,
) -> Dict[str, Any]:
    # подготовить условия
    side_cond = {
        tf: mask_modes.get(f"{tf}_side", "any") for tf in TIMEFRAMES
    }
    dist_mode = {
        tf: mask_modes.get(f"{tf}_dist_mode", "any") for tf in TIMEFRAMES
    }

    all_list = [p for p in dir_positions if p["in_window"][window_tag]]
    n_all = len(all_list)
    pnl_all = sum(p["pnl"] for p in all_list)
    wr_all = (sum(1 for p in all_list if p["pnl"] >= 0) / n_all * 100.0) if n_all > 0 else 0.0
    roi_all = ((pnl_all / deposit) * 100.0) if (deposit > 0 and n_all > 0) else None

    selected = []
    for p in all_list:
        puid = p["position_uid"]
        ok = True
        for tf in TIMEFRAMES:
            # side
            s = pos_side.get(tf, {}).get(window_tag, {}).get(direction, {}).get(puid)
            cond = side_cond[tf]
            if cond != "any" and s != cond:
                ok = False
                break
            # dist бин — учитываем только для m5 (по задумке)
            if tf == "m5":
                b = pos_bins.get(tf, {}).get(window_tag, {}).get(direction, {}).get(puid)
                allowed = MASK_BINS.get(dist_mode[tf], MASK_BINS["any"])
                if b is None or b not in allowed:
                    ok = False
                    break
        if ok:
            selected.append(p)

    n_sel = len(selected)
    pnl_sel = sum(p["pnl"] for p in selected)
    wr_sel = (sum(1 for p in selected if p["pnl"] >= 0) / n_sel * 100.0) if n_sel > 0 else 0.0
    roi_sel = ((pnl_sel / deposit) * 100.0) if (deposit > 0 and n_sel > 0) else None

    return {
        "n_all": n_all,
        "n_selected": n_sel,
        "coverage_pct": (n_sel / n_all * 100.0) if n_all > 0 else 0.0,
        "pnl_sum_all": pnl_all,
        "pnl_sum_selected": pnl_sel,
        "wr_all_pct": wr_all,
        "wr_selected_pct": wr_sel,
        "roi_all_pct": roi_all,
        "roi_selected_pct": roi_sel,
    }


# 🔸 Принять решение (класс/уверенность/обоснование) — логика как в cross-strength
def _make_decision(
    primary_window: str,
    cov_map_dir: Dict[str, Dict[str, Any]],
    metrics_primary: Dict[str, Any],
    metrics_base_primary: Dict[str, Any],
    metrics_secondary: Optional[Dict[str, Any]],
    metrics_base_secondary: Optional[Dict[str, Any]],
) -> Tuple[str, float, str]:
    if metrics_primary["n_all"] == 0 or metrics_base_primary["n_all"] == 0:
        return ("red", 0.0, "no-data")

    d_wr = metrics_primary["wr_selected_pct"] - metrics_primary["wr_all_pct"]
    d_roi = None
    if metrics_primary["roi_selected_pct"] is not None and metrics_primary["roi_all_pct"] is not None:
        d_roi = metrics_primary["roi_selected_pct"] - metrics_primary["roi_all_pct"]

    cov_pct = cov_map_dir.get(primary_window, {}).get("window_coverage_pct", 0.0) / 100.0
    conf = min(1.0, max(0.0, cov_pct))

    secondary_ok = False
    if metrics_secondary and metrics_base_secondary and metrics_secondary["n_all"] > 0:
        d_wr_s = metrics_secondary["wr_selected_pct"] - metrics_secondary["wr_all_pct"]
        d_roi_s = None
        if metrics_secondary["roi_selected_pct"] is not None and metrics_secondary["roi_all_pct"] is not None:
            d_roi_s = metrics_secondary["roi_selected_pct"] - metrics_secondary["roi_all_pct"]
        if (d_wr >= 0 and d_wr_s >= 0) or (d_wr <= 0 and d_wr_s <= 0):
            secondary_ok = True
        if d_roi is not None and d_roi_s is not None:
            if (d_roi >= 0 and d_roi_s >= 0) or (d_roi <= 0 and d_roi_s <= 0):
                secondary_ok = True
    if secondary_ok:
        conf = min(1.0, conf + 0.1)

    if d_roi is not None:
        if (d_roi >= 5.0 and d_wr >= 3.0 and cov_pct >= 0.7):
            return ("green", conf, f"primary={primary_window} ΔROI={d_roi:.2f}pp ΔWR={d_wr:.2f}pp")
        if d_roi >= 2.0 and d_wr >= 0.0:
            return ("yellow", conf, f"primary={primary_window} ΔROI={d_roi:.2f}pp ΔWR={d_wr:.2f}pp (weak)")
        return ("red", conf, f"primary={primary_window} ΔROI={d_roi:.2f}pp ΔWR={d_wr:.2f}pp (no gain)")
    if d_wr >= 3.0 and cov_pct >= 0.7:
        return ("yellow", conf, f"primary={primary_window} ΔWR={d_wr:.2f}pp (ROI=n/a)")
    if d_wr >= 0.0:
        return ("yellow", conf, f"primary={primary_window} ΔWR={d_wr:.2f}pp (weak, ROI=n/a)")
    return ("red", conf, f"primary={primary_window} ΔWR={d_wr:.2f}pp (no gain, ROI=n/a)")


# 🔸 Вставка строки в auditor_ema200_side_mask_results
async def _insert_mask_result(
    run_id: int,
    sid: int,
    direction: str,
    window_tag: str,
    is_primary: bool,
    primary_window: Optional[str],
    mask_modes: Dict[str, str],
    metrics_sel: Dict[str, Any],
    metrics_all: Dict[str, Any],
    decision_class: str,
    decision_confidence: float,
    rationale: str,
):
    # распаковка режимов и квантилей (только для m5_dist_mode есть границы)
    m5_side = mask_modes.get("m5_side", "any")
    m5_mode = mask_modes.get("m5_dist_mode", "any")
    m15_side = mask_modes.get("m15_side", "any")
    m15_mode = mask_modes.get("m15_dist_mode", "any")
    h1_side = mask_modes.get("h1_side", "any")
    h1_mode = mask_modes.get("h1_dist_mode", "any")

    m5_l, m5_h = MASK_QBOUNDS[m5_mode]
    m15_l, m15_h = MASK_QBOUNDS[m15_mode]
    h1_l, h1_h = MASK_QBOUNDS[h1_mode]

    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO auditor_ema200_side_mask_results
            (run_id, strategy_id, direction, window_tag,
             is_primary, primary_window,
             m5_side, m5_dist_mode, m5_q_low, m5_q_high,
             m15_side, m15_dist_mode, m15_q_low, m15_q_high,
             h1_side, h1_dist_mode, h1_q_low, h1_q_high,
             n_selected, n_all, coverage_pct,
             pnl_sum_selected, pnl_sum_all,
             roi_selected_pct, roi_all_pct,
             wr_selected_pct, wr_all_pct,
             decision_class, decision_confidence, rationale)
            VALUES
            ($1,$2,$3,$4, $5,$6,
             $7,$8,$9,$10, $11,$12,$13,$14, $15,$16,$17,$18,
             $19,$20,$21, $22,$23, $24,$25, $26,$27, $28,$29,$30)
            """,
            run_id, sid, direction, window_tag,
            is_primary, primary_window,
            m5_side, m5_mode, m5_l, m5_h,
            m15_side, m15_mode, m15_l, m15_h,
            h1_side, h1_mode, h1_l, h1_h,
            int(metrics_sel["n_selected"]), int(metrics_sel["n_all"]), float(metrics_sel["coverage_pct"]),
            float(metrics_sel["pnl_sum_selected"]), float(metrics_sel["pnl_sum_all"]),
            metrics_sel["roi_selected_pct"], metrics_sel["roi_all_pct"],
            float(metrics_sel["wr_selected_pct"]), float(metrics_sel["wr_all_pct"]),
            decision_class, float(decision_confidence), rationale
        )


# 🔸 Публикация лучшего кандидата в оркестратор витрины
async def _publish_best_candidate(
    run_id: int,
    sid: int,
    direction: str,
    primary_win: str,
    label: str,                 # 'm5_only' | 'm5_m15' | 'm5_m15_h1'
    mask_modes: Dict[str, str], # {'m5_side','m5_dist_mode','m15_side','m15_dist_mode','h1_side','h1_dist_mode'}
    metrics_sel: Dict[str, Any],
    metrics_all: Dict[str, Any],
    decision_conf: float,
):
    if infra.redis_client is None:
        log.debug("ℹ️ AUD_EMA200: Redis не инициализирован — пропуск публикации кандидата")
        return

    idea_key = "ema200_side"
    stream = "auditor:best:candidates"

    roi_sel = metrics_sel["roi_selected_pct"]
    roi_all = metrics_sel["roi_all_pct"]
    eligible = (roi_sel is not None and roi_sel > 0.0)
    event_uid = f"{run_id}:{sid}:{direction}:{idea_key}:{label}:{uuid.uuid4().hex[:8]}"

    fields = {
        "type": "result",
        "run_id": str(run_id),
        "strategy_id": str(sid),
        "direction": str(direction),
        "idea_key": idea_key,
        "variant_key": label,
        "primary_window": str(primary_win),
        "eligible": "true" if eligible else "false",
        "event_uid": event_uid,
    }
    if eligible:
        delta_roi = roi_sel - roi_all
        fields.update({
            "roi_selected_pct": f"{roi_sel}",
            "roi_all_pct": f"{roi_all}",
            "delta_roi_pp": f"{delta_roi}",
            "wr_selected_pct": f"{metrics_sel['wr_selected_pct']}",
            "wr_all_pct": f"{metrics_sel['wr_all_pct']}",
            "coverage_pct": f"{metrics_sel['coverage_pct']}",
            "decision_confidence": f"{decision_conf}",
            "config_json": json.dumps({
                "m5_side": mask_modes.get("m5_side","any"),
                "m5_dist_mode": mask_modes.get("m5_dist_mode","any"),
                "m5_q_low":  MASK_QBOUNDS[mask_modes.get("m5_dist_mode","any")][0],
                "m5_q_high": MASK_QBOUNDS[mask_modes.get("m5_dist_mode","any")][1],
                "m15_side": mask_modes.get("m15_side","any"),
                "m15_dist_mode": mask_modes.get("m15_dist_mode","any"),
                "m15_q_low": MASK_QBOUNDS[mask_modes.get("m15_dist_mode","any")][0],
                "m15_q_high":MASK_QBOUNDS[mask_modes.get("m15_dist_mode","any")][1],
                "h1_side":  mask_modes.get("h1_side","any"),
                "h1_dist_mode": mask_modes.get("h1_dist_mode","any"),
                "h1_q_low": MASK_QBOUNDS[mask_modes.get("h1_dist_mode","any")][0],
                "h1_q_high":MASK_QBOUNDS[mask_modes.get("h1_dist_mode","any")][1],
            }),
            "source_table": "auditor_ema200_side_mask_results",
            "source_run_id": str(run_id),
        })
    try:
        await infra.redis_client.xadd(stream, fields, id="*")
        log.debug("📨 AUD_EMA200 → BEST_SELECTOR | sid=%s dir=%s | variant=%s | eligible=%s",
                 sid, direction, label, fields["eligible"])
    except Exception:
        log.exception("❌ AUD_EMA200: ошибка публикации кандидата в %s", stream)


# 🔸 Загрузка депозита стратегии
async def _load_strategy_deposit(sid: int) -> float:
    async with infra.pg_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT deposit FROM strategies_v4 WHERE id = $1", int(sid))
    return float(row["deposit"])


# 🔸 Выборка закрытых позиций стратегии
async def _load_closed_positions_for_strategy(sid: int) -> List[Dict[str, Any]]:
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT position_uid, symbol, direction, entry_price, pnl, notional_value, created_at, closed_at
            FROM positions_v4
            WHERE strategy_id = $1 AND status = 'closed' AND direction IN ('long','short')
            """,
            int(sid)
        )
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            "position_uid": str(r["position_uid"]),
            "symbol": str(r["symbol"]),
            "direction": str(r["direction"]),
            "entry_price": float(r["entry_price"] or 0.0),
            "pnl": float(r["pnl"] or 0.0),
            "notional_value": float(r["notional_value"] or 0.0),
            "created_at": r["created_at"],
            "closed_at": r["closed_at"],
        })
    return out


# 🔸 Подтянуть снапшоты ema200/atr14 для позиций по всем TF
async def _load_indicator_snapshots_for_positions(pos_uids: List[str]) -> Dict[Tuple[str, str], Dict[str, float]]:
    snaps: Dict[Tuple[str, str], Dict[str, float]] = {}
    if not pos_uids:
        return snaps
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT position_uid, timeframe,
                   MAX(value_num) FILTER (WHERE param_type='indicator' AND param_base='ema' AND param_name='ema200') AS ema200,
                   MAX(value_num) FILTER (WHERE param_type='indicator' AND param_base='atr' AND param_name='atr14')   AS atr14
            FROM indicator_position_stat
            WHERE position_uid = ANY($1)
              AND status = 'ok'
              AND param_type = 'indicator'
              AND timeframe IN ('m5','m15','h1')
            GROUP BY position_uid, timeframe
            """,
            pos_uids
        )
    for r in rows:
        key = (str(r["position_uid"]), str(r["timeframe"]))
        snaps[key] = {"ema200": _to_float_or_none(r["ema200"]), "atr14": _to_float_or_none(r["atr14"])}
    return snaps


# 🔸 Покрытие по окнам
def _compute_coverage(positions: List[Dict[str, Any]], now_utc: dt.datetime) -> Dict[str, Dict[str, Dict[str, Any]]]:
    out: Dict[str, Dict[str, Dict[str, Any]]] = {"long": {}, "short": {}}
    by_dir: Dict[str, List[Dict[str, Any]]] = {"long": [], "short": []}
    for p in positions:
        by_dir[p["direction"]].append(p)
    for direction in ("long", "short"):
        dir_positions = by_dir[direction]
        if not dir_positions:
            continue
        for w, nominal_days in WINDOWS:
            if w == "total":
                n_positions = len(dir_positions)
                first_closed = min((p["closed_at"] for p in dir_positions if p["closed_at"]), default=None)
                last_closed = max((p["closed_at"] for p in dir_positions if p["closed_at"]), default=None)
                out[direction][w] = {"window_days_effective": 0, "window_days_nominal": 0, "window_coverage_pct": 100.0,
                                     "n_positions": n_positions, "first_closed_at": first_closed, "last_closed_at": last_closed}
                continue
            cutoff = now_utc - dt.timedelta(days=int(nominal_days or 0))
            win_positions = [p for p in dir_positions if p["closed_at"] and p["closed_at"] >= cutoff]
            n_positions = len(win_positions)
            if n_positions > 0:
                first_closed = min(p["closed_at"] for p in win_positions if p["closed_at"])
                last_closed = max(p["closed_at"] for p in win_positions if p["closed_at"])
                eff_days = (now_utc - first_closed).total_seconds() / 86400.0
                eff_days = max(0.0, min(eff_days, float(nominal_days)))
                coverage_pct = (eff_days / float(nominal_days)) * 100.0 if nominal_days else 100.0
            else:
                first_closed = None; last_closed = None; eff_days = 0.0; coverage_pct = 0.0
            out[direction][w] = {"window_days_effective": int(eff_days), "window_days_nominal": int(nominal_days or 0),
                                 "window_coverage_pct": float(coverage_pct),
                                 "n_positions": n_positions, "first_closed_at": first_closed, "last_closed_at": last_closed}
    return out


# 🔸 Вставка coverage
async def _insert_coverage_rows(run_id: int, sid: int, coverage: Dict[str, Dict[str, Dict[str, Any]]]):
    async with infra.pg_pool.acquire() as conn:
        for direction in ("long", "short"):
            if direction not in coverage:
                continue
            for w, rec in coverage[direction].items():
                await conn.execute(
                    """
                    INSERT INTO auditor_ema200_side_coverage
                    (run_id, strategy_id, direction, window_tag,
                     window_days_effective, window_days_nominal, window_coverage_pct,
                     n_positions, first_closed_at, last_closed_at)
                    VALUES ($1,$2,$3,$4, $5,$6,$7, $8,$9,$10)
                    """,
                    run_id, sid, direction, w,
                    rec["window_days_effective"], rec["window_days_nominal"], rec["window_coverage_pct"],
                    rec["n_positions"], rec["first_closed_at"], rec["last_closed_at"]
                )


# 🔸 Квантильные утилиты / преобразования

def _quantile_edges(values: List[float], probs: Iterable[float]) -> Tuple[float, float, float, float]:
    arr = sorted(float(x) for x in values)
    n = len(arr)
    if n == 0:
        return (0.0, 0.0, 0.0, 0.0)
    edges: List[float] = []
    for p in probs:
        idx = int(round(p * (n - 1)))
        idx = min(max(idx, 0), n - 1)
        edges.append(arr[idx])
    e1, e2, e3, e4 = edges
    if e1 > e2: e2 = e1
    if e2 > e3: e3 = e2
    if e3 > e4: e4 = e3
    return (e1, e2, e3, e4)


def _assign_bin(x: float, edges: Tuple[float, float, float, float]) -> int:
    q20, q40, q60, q80 = edges
    if x <= q20:
        return 1
    elif x <= q40:
        return 2
    elif x <= q60:
        return 3
    elif x <= q80:
        return 4
    else:
        return 5


def _to_float_or_none(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None