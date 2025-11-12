# 🔸 auditor_cross_strength.py — аудит «силы кросса» EMA9/EMA21:
#     — считает бины по abs(ema9-ema21)/atr14 на TF (m5/m15/h1) и окнах (7d/14d/28d/total)
#     — пишет в БД: runs / coverage / bin_stats / mask_results
#     — формирует три маски под тест-модуль: m5_only, m5_m15, m5_m15_h1
#     — primary-маска выбирается как «best-of-three» на основном окне (по ΔROI, затем ROI, затем conf, затем coverage)
#     — публикует лучший вариант в оркестратор витрины (Redis Stream auditor:best:candidates)

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
log = logging.getLogger("AUD_XSTR")

# 🔸 Константы аудита (правим здесь, не через ENV)
WINDOWS: List[Tuple[str, Optional[int]]] = [("7d", 7), ("14d", 14), ("28d", 28), ("total", None)]
TIMEFRAMES: Tuple[str, ...] = ("m5", "m15", "h1")
MIN_SAMPLE_PER_CELL = 50                 # пометка «мало наблюдений»
INITIAL_DELAY_SEC = 0                    # стартовую задержку контролируем из main
SLEEP_BETWEEN_RUNS_SEC = 3 * 60 * 60     # пауза между проходами (3 часа)

# 🔸 Пороговые правила классификации TF по бинам
DELTA_ROI_HIGH_PP = 5.0   # ΔROI(B5−B1) ≥ 5 п.п. → HIGH
DELTA_ROI_LOW_PP  = -5.0  # ΔROI(B5−B1) ≤ -5 п.п. → LOW
U_SHAPE_MIN_GAIN  = 3.0   # ROI(B3) − max(ROI(B1), ROI(B5)) ≥ 3 п.п. → MID

# 🔸 Порог выбора «основного окна»
PRIMARY_28D_COVERAGE = 0.80
PRIMARY_14D_COVERAGE = 0.70
SECONDARY_MIN_COVER  = 0.50

# 🔸 Маски квантилей для режимов
MASK_BINS: Dict[str, Set[int]] = {
    "any": {1, 2, 3, 4, 5},
    "low": {1, 2, 3},          # ≤ Q60
    "mid": {2, 3, 4},          # Q20..Q80
    "high": {4, 5},            # ≥ Q60 (базово; можно ужесточить до ≥Q80 при необходимости)
}
MASK_QBOUNDS: Dict[str, Tuple[Optional[int], Optional[int]]] = {
    "any":  (None, None),
    "low":  (0, 60),
    "mid":  (20, 80),
    "high": (60, 100),
}


# 🔸 Точка входа воркера
async def run_auditor_cross_strength():
    # условия достаточности
    if infra.pg_pool is None:
        log.info("❌ Пропуск auditor_cross_strength: PG не инициализирован")
        return

    # стартовая задержка (если нужна)
    if INITIAL_DELAY_SEC > 0:
        log.info("⏳ AUD_XSTR: ожидание %d сек перед первым запуском", INITIAL_DELAY_SEC)
        await asyncio.sleep(int(INITIAL_DELAY_SEC))

    # основной цикл
    while True:
        try:
            await _run_once()
        except asyncio.CancelledError:
            log.info("⏹️ AUD_XSTR: остановлено по сигналу")
            raise
        except Exception:
            log.exception("❌ AUD_XSTR: ошибка прохода — пауза 5 секунд")
            await asyncio.sleep(5)

        log.info("😴 AUD_XSTR: пауза %d сек до следующего запуска", SLEEP_BETWEEN_RUNS_SEC)
        await asyncio.sleep(int(SLEEP_BETWEEN_RUNS_SEC))


# 🔸 Один проход: создать run, пройти по стратегиям, записать покрытия/бины/вердикты
async def _run_once():
    # загрузка активных MW-стратегий
    strategies = await load_active_mw_strategies()
    log.info("📦 AUD_XSTR: найдено активных MW-стратегий: %d", len(strategies))
    if not strategies:
        return

    # фиксируем "сейчас" (aware→naive UTC) и границы окон
    now_utc = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    win_bounds = {
        "7d":  now_utc - dt.timedelta(days=7),
        "14d": now_utc - dt.timedelta(days=14),
        "28d": now_utc - dt.timedelta(days=28),
        "total": None,
    }
    log.info(
        "🕒 AUD_XSTR: окна — now=%s; 7d>=%s; 14d>=%s; 28d>=%s",
        now_utc, win_bounds["7d"], win_bounds["14d"], win_bounds["28d"]
    )

    # создаём запись прогона
    run_id = await _create_run(now_utc, win_bounds)
    log.info("🧾 AUD_XSTR: создан run_id=%s", run_id)

    # последовательный проход по стратегиям
    for sid, meta in strategies.items():
        try:
            await _process_strategy(run_id, sid, meta, now_utc, win_bounds)
        except Exception:
            log.exception("❌ AUD_XSTR: ошибка обработки стратегии sid=%s", sid)


# 🔸 Создать запись в auditor_emacross_runs
async def _create_run(now_utc: dt.datetime, win_bounds: Dict[str, Optional[dt.datetime]]) -> int:
    async with infra.pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO auditor_emacross_runs (now_utc, window_7d_from, window_14d_from, window_28d_from)
            VALUES ($1, $2, $3, $4)
            RETURNING id
            """,
            now_utc, win_bounds["7d"], win_bounds["14d"], win_bounds["28d"]
        )
        return int(row["id"])


# 🔸 Полная обработка одной стратегии
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

    # получить депозит стратегии (аксиома: > 0)
    deposit = await _load_strategy_deposit(sid)
    dep_used_for_bins = float(deposit)

    # выбрать все закрытые позиции (total)
    positions = await _load_closed_positions_for_strategy(sid)
    if not positions:
        log.info("ℹ️ AUD_XSTR: %s — закрытых позиций нет", title)
        return

    # пометить принадлежность окнам
    for p in positions:
        closed_at = p["closed_at"]
        p["in_window"] = {
            "7d":  (closed_at is not None and closed_at >= win_bounds["7d"]),
            "14d": (closed_at is not None and closed_at >= win_bounds["14d"]),
            "28d": (closed_at is not None and closed_at >= win_bounds["28d"]),
            "total": True,
        }

    # посчитать покрытия по окнам и записать в auditor_emacross_coverage
    coverage = _compute_coverage(positions, now_utc)
    await _insert_coverage_rows(run_id, sid, coverage)

    # подтянуть снапшоты индикаторов по TF (ema9/ema21/atr14)
    pos_uids = [p["position_uid"] for p in positions]
    snaps = await _load_indicator_snapshots_for_positions(pos_uids)

    # подготовка корзин: data[tf][window][direction][symbol] -> [(position_uid, cs, pnl)]
    data: Dict[str, Dict[str, Dict[str, Dict[str, List[Tuple[str, float, float]]]]]] = {
        tf: {w: {"long": {}, "short": {}} for w, _ in WINDOWS} for tf in TIMEFRAMES
    }

    # расчёт cross_strength и раскладка по корзинам
    for p in positions:
        symbol = p["symbol"]
        direction = p["direction"]
        pnl = float(p["pnl"] or 0.0)
        puid = p["position_uid"]

        for tf in TIMEFRAMES:
            s = snaps.get((puid, tf))
            if not s:
                continue
            ema9 = s.get("ema9"); ema21 = s.get("ema21"); atr14 = s.get("atr14")
            if ema9 is None or ema21 is None or atr14 is None:
                continue
            if atr14 <= 0:
                continue

            cs = abs(float(ema9) - float(ema21)) / float(atr14)

            for w, _days in WINDOWS:
                if p["in_window"][w]:
                    bucket = data[tf][w][direction].setdefault(symbol, [])
                    bucket.append((puid, cs, pnl))

    # агрегаты бинов и карта «бин позиции» для дальнейшей оценки маски
    bin_aggr: Dict[str, Dict[str, Dict[str, Dict[int, Dict[str, float]]]]] = {
        tf: {w: {"long": {}, "short": {}} for w, _ in WINDOWS} for tf in TIMEFRAMES
    }
    pos_bins: Dict[str, Dict[str, Dict[str, Dict[str, int]]]] = {
        tf: {w: {"long": {}, "short": {}} for w, _ in WINDOWS} for tf in TIMEFRAMES
    }

    # пройти по всем ячейкам TF×window×direction: присвоить бины, посчитать агрегаты и записать в БД
    async with infra.pg_pool.acquire() as conn:
        for tf in TIMEFRAMES:
            for w, _days in WINDOWS:
                for direction in ("long", "short"):
                    symbol_series = data[tf][w][direction]
                    total_n = sum(len(v) for v in symbol_series.values())
                    if total_n == 0:
                        continue

                    btot = {
                        1: {"N": 0, "wins": 0, "pnl_sum": 0.0},
                        2: {"N": 0, "wins": 0, "pnl_sum": 0.0},
                        3: {"N": 0, "wins": 0, "pnl_sum": 0.0},
                        4: {"N": 0, "wins": 0, "pnl_sum": 0.0},
                        5: {"N": 0, "wins": 0, "pnl_sum": 0.0},
                    }

                    # квантильные границы по символу и присвоение бинов
                    for symbol, triples in symbol_series.items():
                        if not triples:
                            continue
                        xs = [cs for (_puid, cs, _pnl) in triples]
                        edges = _quantile_edges(xs, (0.2, 0.4, 0.6, 0.8))
                        for puid, cs, pnl in triples:
                            b = _assign_bin(cs, edges)
                            pos_bins[tf][w][direction][puid] = b
                            rec = btot[b]
                            rec["N"] += 1
                            rec["wins"] += 1 if pnl >= 0 else 0
                            rec["pnl_sum"] += float(pnl)

                    # сохранить аггрегаты
                    bin_aggr[tf][w][direction] = btot

                    # лог (как прежде)
                    warn = " (N<50)" if total_n < MIN_SAMPLE_PER_CELL else ""
                    log.info('📈 AUD_XSTR | %s | TF=%s | dir=%s | window=%s — bins by cross_strength%s',
                             title, tf, direction, w, warn)
                    for idx in (1, 2, 3, 4, 5):
                        rec = btot[idx]
                        N = rec["N"]
                        WR = (rec["wins"] / N * 100.0) if N > 0 else 0.0
                        pnl_sum = rec["pnl_sum"]
                        roi = (pnl_sum / dep_used_for_bins * 100.0) if dep_used_for_bins > 0 else 0.0
                        log.info("  B%d: N=%d, WR=%.2f%%, ΣPnL=%.6f, ROI=%.4f%%", idx, N, WR, pnl_sum, roi)

                    # итоги по ячейке
                    first, last = btot[1], btot[5]
                    d_wr = _delta_wr(first, last)
                    d_roi = (last["pnl_sum"] - first["pnl_sum"]) / dep_used_for_bins * 100.0
                    log.info("  ΔWR(B5−B1)=%.2f pp, ΔROI(B5−B1)=%.4f pp", d_wr, d_roi)

                    # запись в auditor_emacross_bin_stats — 5 строк
                    cov = coverage.get(direction, {}).get(w, None)
                    win_eff = cov["window_days_effective"] if cov else (0 if w == "total" else (7 if w == "7d" else 14 if w == "14d" else 28))
                    win_nom = cov["window_days_nominal"] if cov else (0 if w == "total" else (7 if w == "7d" else 14 if w == "14d" else 28))
                    win_cov = cov["window_coverage_pct"] if cov else (100.0 if w == "total" else 0.0)

                    for idx in (1, 2, 3, 4, 5):
                        rec = btot[idx]
                        N = int(rec["N"]); wins = int(rec["wins"]); pnl_sum = float(rec["pnl_sum"])
                        roi_pct = (pnl_sum / dep_used_for_bins * 100.0) if dep_used_for_bins > 0 else 0.0
                        await conn.execute(
                            """
                            INSERT INTO auditor_emacross_bin_stats
                            (run_id, strategy_id, timeframe, direction, window_tag,
                             window_days_effective, window_days_nominal, window_coverage_pct,
                             bin_index, n, wins, pnl_sum, deposit_used, roi_pct,
                             clip_applied, clip_p99)
                            VALUES ($1,$2,$3,$4,$5, $6,$7,$8, $9,$10,$11,$12,$13,$14, $15,$16)
                            """,
                            run_id, sid, tf, direction, w,
                            int(win_eff), int(win_nom), float(win_cov),
                            int(idx), N, wins, pnl_sum, float(dep_used_for_bins), float(roi_pct),
                            False, None
                        )

    # для каждого направления: выбрать primary-окно, построить классы TF, сформировать 3 маски и выбрать «best-of-three»
    for direction in ("long", "short"):
        dir_positions = [p for p in positions if p["direction"] == direction]
        if not dir_positions:
            continue

        primary_win = _choose_primary_window(coverage.get(direction, {}))
        secondary_win = _choose_secondary_window(coverage.get(direction, {}), primary_win)

        # классификация по TF на primary окне
        tf_classes = {}
        for tf in TIMEFRAMES:
            btot = bin_aggr.get(tf, {}).get(primary_win, {}).get(direction, None)
            tf_classes[tf] = _classify_tf(btot, dep_used_for_bins)

        # три маски под тест-модуль
        masks: List[Tuple[str, Dict[str, str], str]] = [
            ("m5_only",     {"m5": tf_classes.get("m5", "any"),  "m15": "any",                         "h1": "any"},                         "mask=m5_only"),
            ("m5_m15",      {"m5": tf_classes.get("m5", "any"),  "m15": tf_classes.get("m15", "any"), "h1": "any"},                         "mask=m5_m15"),
            ("m5_m15_h1",   {"m5": tf_classes.get("m5", "any"),  "m15": tf_classes.get("m15", "any"), "h1": tf_classes.get("h1", "any")},  "mask=m5_m15_h1"),
        ]

        # оценим каждую маску на primary окне и выберем лучшую (best-of-three)
        ranking: List[Tuple[float, float, float, float, int]] = []  # (eligible, delta_roi, roi_sel, conf, coverage, idx)
        mask_metrics_primary: List[Dict[str, Any]] = []
        mask_metrics_all_primary: List[Dict[str, Any]] = []
        mask_decision_cache: List[Tuple[str, float]] = []  # (class, conf)

        for idx, (_label, mask_modes, _note) in enumerate(masks):
            m_sel = _evaluate_mask_on_positions(dir_positions, pos_bins, mask_modes, primary_win, direction, deposit)
            m_all = _evaluate_mask_on_positions(dir_positions, pos_bins, {"m5": "any", "m15": "any", "h1": "any"}, primary_win, direction, deposit)
            mask_metrics_primary.append(m_sel)
            mask_metrics_all_primary.append(m_all)

            # предварительная оценка для confidence (как в решении)
            m_sel_sec = None; m_all_sec = None
            if secondary_win is not None:
                m_sel_sec = _evaluate_mask_on_positions(dir_positions, pos_bins, mask_modes, secondary_win, direction, deposit)
                m_all_sec = _evaluate_mask_on_positions(dir_positions, pos_bins, {"m5": "any", "m15": "any", "h1": "any"}, secondary_win, direction, deposit)
            dec_class, dec_conf, _ = _make_decision(primary_win, coverage.get(direction, {}), m_sel, m_all, m_sel_sec, m_all_sec)
            mask_decision_cache.append((dec_class, dec_conf))

            roi_sel = m_sel["roi_selected_pct"]; roi_all = m_sel["roi_all_pct"]
            delta_roi = (roi_sel - roi_all) if (roi_sel is not None and roi_all is not None) else float("-inf")
            eligible = 1.0 if (roi_sel is not None and roi_sel > 0.0) else 0.0
            coverage_pct = m_sel["coverage_pct"]
            # ранжирование: eligible (1/0) → ΔROI → ROI_selected → confidence → coverage
            ranking.append((eligible, delta_roi, (roi_sel or -1e9), dec_conf, coverage_pct, idx))

        # индекс лучшей маски
        ranking.sort(reverse=True, key=lambda t: (t[0], t[1], t[2], t[3], t[4]))
        best_idx = ranking[0][-1]

        # записываем все три маски (primary для best_idx; остальные — ALT), с валидациями по прочим окнам
        for idx, (label, mask_modes, note) in enumerate(masks):
            is_primary_mask = (idx == best_idx)
            await _record_mask_with_validation(
                run_id=run_id,
                sid=sid,
                title=title,
                direction=direction,
                primary_win=primary_win,
                secondary_win=secondary_win,
                coverage_dir=coverage.get(direction, {}),
                dir_positions=dir_positions,
                pos_bins=pos_bins,
                deposit=deposit,
                mask_modes=mask_modes,
                is_primary=is_primary_mask,
                primary_note=note
            )

        # 🔸 Публикуем лучший вариант в оркестратор (всегда; если ROI<=0 — как ineligible)
        best_label, best_mask, _ = masks[best_idx]
        best_sel = mask_metrics_primary[best_idx]
        best_all = mask_metrics_all_primary[best_idx]
        # confidence такой же логикой, как в решении
        best_sel_sec = best_all_sec = None
        if secondary_win is not None:
            best_sel_sec = _evaluate_mask_on_positions(dir_positions, pos_bins, best_mask, secondary_win, direction, deposit)
            best_all_sec = _evaluate_mask_on_positions(dir_positions, pos_bins, {"m5":"any","m15":"any","h1":"any"}, secondary_win, direction, deposit)
        _, best_conf, _ = _make_decision(primary_win, coverage.get(direction, {}), best_sel, best_all, best_sel_sec, best_all_sec)

        await _publish_best_candidate(
            run_id=run_id,
            sid=sid,
            direction=direction,
            primary_win=primary_win,
            label=best_label,
            mask_modes=best_mask,
            metrics_sel=best_sel,
            metrics_all=best_all,
            decision_conf=best_conf,
        )


# 🔸 Запись одной маски (primary + валидация по другим окнам)
async def _record_mask_with_validation(
    run_id: int,
    sid: int,
    title: str,
    direction: str,
    primary_win: str,
    secondary_win: Optional[str],
    coverage_dir: Dict[str, Dict[str, Any]],
    dir_positions: List[Dict[str, Any]],
    pos_bins: Dict[str, Dict[str, Dict[str, Dict[str, int]]]],
    deposit: float,
    mask_modes: Dict[str, str],
    is_primary: bool,
    primary_note: str,
):
    # оценка на primary
    metrics_sel_primary = _evaluate_mask_on_positions(dir_positions, pos_bins, mask_modes, primary_win, direction, deposit)
    metrics_all_primary = _evaluate_mask_on_positions(dir_positions, pos_bins, {"m5": "any", "m15": "any", "h1": "any"}, primary_win, direction, deposit)

    # оценка на secondary (если есть)
    metrics_sel_secondary = None
    metrics_all_secondary = None
    if secondary_win is not None:
        metrics_sel_secondary = _evaluate_mask_on_positions(dir_positions, pos_bins, mask_modes, secondary_win, direction, deposit)
        metrics_all_secondary = _evaluate_mask_on_positions(dir_positions, pos_bins, {"m5": "any", "m15": "any", "h1": "any"}, secondary_win, direction, deposit)

    # принять решение
    decision_class, decision_conf, rationale = _make_decision(
        primary_window=primary_win,
        cov_map_dir=coverage_dir,
        metrics_primary=metrics_sel_primary,
        metrics_base_primary=metrics_all_primary,
        metrics_secondary=metrics_sel_secondary,
        metrics_base_secondary=metrics_all_secondary
    )

    # запись primary строки в mask_results
    await _insert_mask_result(
        run_id=run_id, sid=sid, direction=direction, window_tag=primary_win,
        is_primary=is_primary, primary_window=primary_win,
        mask_modes=mask_modes, metrics_sel=metrics_sel_primary, metrics_all=metrics_all_primary,
        decision_class=decision_class, decision_confidence=decision_conf, rationale=(primary_note if primary_note else rationale)
    )

    # лог (DECISION для primary, ALT — для остальных)
    tag = "DECISION" if is_primary else "ALT"
    d_roi_pp = 0.0
    if metrics_sel_primary["roi_selected_pct"] is not None and metrics_sel_primary["roi_all_pct"] is not None:
        d_roi_pp = metrics_sel_primary["roi_selected_pct"] - metrics_sel_primary["roi_all_pct"]
    log.info(
        "✅ %s | sid=%s | dir=%s | primary=%s | class=%s (conf=%.2f) | mask: m5=%s, m15=%s, h1=%s | ΔROI=%.2f pp | ΔWR=%.2f pp",
        tag, sid, direction, primary_win, decision_class, decision_conf,
        mask_modes.get('m5','any'), mask_modes.get('m15','any'), mask_modes.get('h1','any'),
        d_roi_pp, metrics_sel_primary["wr_selected_pct"] - metrics_sel_primary["wr_all_pct"]
    )

    # валидации на других окнах
    for w, _ in WINDOWS:
        if w == primary_win:
            continue
        m_sel = _evaluate_mask_on_positions(dir_positions, pos_bins, mask_modes, w, direction, deposit)
        m_all = _evaluate_mask_on_positions(dir_positions, pos_bins, {"m5": "any", "m15": "any", "h1": "any"}, w, direction, deposit)
        if m_all["n_all"] > 0:
            await _insert_mask_result(
                run_id=run_id, sid=sid, direction=direction, window_tag=w,
                is_primary=False, primary_window=primary_win,
                mask_modes=mask_modes, metrics_sel=m_sel, metrics_all=m_all,
                decision_class=decision_class, decision_confidence=max(0.0, decision_conf - 0.1),
                rationale=("validation-window; " + (primary_note or ""))
            )


# 🔸 Публикация «лучшего кандидата» в оркестратор витрины
async def _publish_best_candidate(
    run_id: int,
    sid: int,
    direction: str,
    primary_win: str,
    label: str,                 # 'm5_only' | 'm5_m15' | 'm5_m15_h1'
    mask_modes: Dict[str, str],
    metrics_sel: Dict[str, Any],
    metrics_all: Dict[str, Any],
    decision_conf: float,
):
    # условия достаточности
    if infra.redis_client is None:
        log.info("ℹ️ AUD_XSTR: Redis не инициализирован — пропуск публикации кандидата")
        return

    idea_key = "emacross_cs"
    stream = "auditor:best:candidates"

    roi_sel = metrics_sel["roi_selected_pct"]
    roi_all = metrics_sel["roi_all_pct"]
    eligible = (roi_sel is not None and roi_sel > 0.0)
    event_uid = f"{run_id}:{sid}:{direction}:{idea_key}:{label}:{uuid.uuid4().hex[:8]}"

    # базовые поля — всегда
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

    # метрики и конфиг — только если eligible=true
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
                "m5_mode": mask_modes.get("m5","any"),
                "m15_mode": mask_modes.get("m15","any"),
                "h1_mode": mask_modes.get("h1","any"),
                "m5_q_low":  MASK_QBOUNDS[mask_modes.get("m5","any")][0],
                "m5_q_high": MASK_QBOUNDS[mask_modes.get("m5","any")][1],
                "m15_q_low":  MASK_QBOUNDS[mask_modes.get("m15","any")][0],
                "m15_q_high": MASK_QBOUNDS[mask_modes.get("m15","any")][1],
                "h1_q_low":  MASK_QBOUNDS[mask_modes.get("h1","any")][0],
                "h1_q_high": MASK_QBOUNDS[mask_modes.get("h1","any")][1],
            }),
            "source_table": "auditor_emacross_mask_results",
            "source_run_id": str(run_id),
        })

    # публикация
    try:
        await infra.redis_client.xadd(stream, fields, id="*")
        log.info("📨 AUD_XSTR → BEST_SELECTOR | sid=%s dir=%s | variant=%s | eligible=%s",
                 sid, direction, label, fields["eligible"])
    except Exception:
        log.exception("❌ AUD_XSTR: ошибка публикации кандидата в %s", stream)


# 🔸 Загрузка депозита стратегии
async def _load_strategy_deposit(sid: int) -> float:
    async with infra.pg_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT deposit FROM strategies_v4 WHERE id = $1", int(sid))
    # аксиома: депозит всегда задан и > 0
    val = float(row["deposit"])
    return val


# 🔸 Выборка закрытых позиций стратегии (total)
async def _load_closed_positions_for_strategy(sid: int) -> List[Dict[str, Any]]:
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT position_uid, symbol, direction, pnl, notional_value, created_at, closed_at
            FROM positions_v4
            WHERE strategy_id = $1
              AND status = 'closed'
              AND direction IN ('long','short')
            """,
            int(sid)
        )
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            "position_uid": str(r["position_uid"]),
            "symbol": str(r["symbol"]),
            "direction": str(r["direction"]),
            "pnl": float(r["pnl"] or 0.0),
            "notional_value": float(r["notional_value"] or 0.0),
            "created_at": r["created_at"],
            "closed_at": r["closed_at"],
        })
    return out


# 🔸 Подтянуть снапшоты ema9/ema21/atr14 для позиций по всем TF
async def _load_indicator_snapshots_for_positions(pos_uids: List[str]) -> Dict[Tuple[str, str], Dict[str, float]]:
    snaps: Dict[Tuple[str, str], Dict[str, float]] = {}
    if not pos_uids:
        return snaps

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT position_uid, timeframe,
                   MAX(value_num) FILTER (WHERE param_type='indicator' AND param_base='ema' AND param_name='ema9')  AS ema9,
                   MAX(value_num) FILTER (WHERE param_type='indicator' AND param_base='ema' AND param_name='ema21') AS ema21,
                   MAX(value_num) FILTER (WHERE param_type='indicator' AND param_base='atr' AND param_name='atr14') AS atr14
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
        snaps[key] = {
            "ema9": _to_float_or_none(r["ema9"]),
            "ema21": _to_float_or_none(r["ema21"]),
            "atr14": _to_float_or_none(r["atr14"]),
        }
    return snaps


# 🔸 Посчитать покрытия по окнам для стратегии (по направлениям)
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
                out[direction][w] = {
                    "window_days_effective": 0,
                    "window_days_nominal": 0,
                    "window_coverage_pct": 100.0,
                    "n_positions": n_positions,
                    "first_closed_at": first_closed,
                    "last_closed_at": last_closed,
                }
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
                first_closed = None
                last_closed = None
                eff_days = 0.0
                coverage_pct = 0.0

            out[direction][w] = {
                "window_days_effective": int(eff_days),
                "window_days_nominal": int(nominal_days or 0),
                "window_coverage_pct": float(coverage_pct),
                "n_positions": n_positions,
                "first_closed_at": first_closed,
                "last_closed_at": last_closed,
            }

    return out


# 🔸 Вставить покрытия в auditor_emacross_coverage
async def _insert_coverage_rows(run_id: int, sid: int, coverage: Dict[str, Dict[str, Dict[str, Any]]]):
    async with infra.pg_pool.acquire() as conn:
        for direction in ("long", "short"):
            if direction not in coverage:
                continue
            for w, rec in coverage[direction].items():
                await conn.execute(
                    """
                    INSERT INTO auditor_emacross_coverage
                    (run_id, strategy_id, direction, window_tag,
                     window_days_effective, window_days_nominal, window_coverage_pct,
                     n_positions, first_closed_at, last_closed_at)
                    VALUES ($1,$2,$3,$4, $5,$6,$7, $8,$9,$10)
                    """,
                    run_id, sid, direction, w,
                    rec["window_days_effective"], rec["window_days_nominal"], rec["window_coverage_pct"],
                    rec["n_positions"], rec["first_closed_at"], rec["last_closed_at"]
                )


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
    return None  # primary=7d


# 🔸 Классификация TF по бинам (HIGH/LOW/MID/any) на окне
def _classify_tf(btot: Optional[Dict[int, Dict[str, float]]], dep_used_for_bins: float) -> str:
    if not btot:
        return "any"

    def roi_pp(x: float) -> float:
        return (x / dep_used_for_bins * 100.0) if dep_used_for_bins > 0 else 0.0

    r1 = roi_pp(btot[1]["pnl_sum"])
    r3 = roi_pp(btot[3]["pnl_sum"])
    r5 = roi_pp(btot[5]["pnl_sum"])
    d_roi = r5 - r1

    if (r3 - max(r1, r5)) >= U_SHAPE_MIN_GAIN:
        return "mid"
    if d_roi >= DELTA_ROI_HIGH_PP:
        return "high"
    if d_roi <= DELTA_ROI_LOW_PP:
        return "low"
    return "any"


# 🔸 Оценить маску на позициях (пересечение условий по TF)
def _evaluate_mask_on_positions(
    dir_positions: List[Dict[str, Any]],
    pos_bins: Dict[str, Dict[str, Dict[str, Dict[str, int]]]],
    mask_modes: Dict[str, str],
    window_tag: str,
    direction: str,
    deposit: float,
) -> Dict[str, Any]:
    allowed = {tf: MASK_BINS.get(mask_modes.get(tf, "any"), MASK_BINS["any"]) for tf in TIMEFRAMES}

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
            bin_map = pos_bins.get(tf, {}).get(window_tag, {}).get(direction, {})
            b = bin_map.get(puid, None)
            if b is None or (b not in allowed[tf]):
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


# 🔸 Принять решение по маске (class/confidence/rationale)
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


# 🔸 Вставить строку в auditor_emacross_mask_results
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
    m5_mode, m15_mode, h1_mode = mask_modes.get("m5", "any"), mask_modes.get("m15", "any"), mask_modes.get("h1", "any")
    m5_l, m5_h = MASK_QBOUNDS[m5_mode]
    m15_l, m15_h = MASK_QBOUNDS[m15_mode]
    h1_l, h1_h = MASK_QBOUNDS[h1_mode]

    n_sel = int(metrics_sel["n_selected"])
    n_all = int(metrics_sel["n_all"])
    coverage_pct = float(metrics_sel["coverage_pct"])
    pnl_sel = float(metrics_sel["pnl_sum_selected"])
    pnl_all = float(metrics_sel["pnl_sum_all"])
    roi_sel = metrics_sel["roi_selected_pct"]
    roi_all = metrics_sel["roi_all_pct"]
    wr_sel = float(metrics_sel["wr_selected_pct"])
    wr_all = float(metrics_sel["wr_all_pct"])

    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO auditor_emacross_mask_results
            (run_id, strategy_id, direction, window_tag,
             is_primary, primary_window,
             m5_mode, m5_q_low, m5_q_high,
             m15_mode, m15_q_low, m15_q_high,
             h1_mode, h1_q_low, h1_q_high,
             n_selected, n_all, coverage_pct,
             pnl_sum_selected, pnl_sum_all,
             roi_selected_pct, roi_all_pct,
             wr_selected_pct, wr_all_pct,
             decision_class, decision_confidence, rationale)
            VALUES
            ($1,$2,$3,$4, $5,$6,
             $7,$8,$9, $10,$11,$12, $13,$14,$15,
             $16,$17,$18, $19,$20, $21,$22, $23,$24, $25,$26,$27)
            """,
            run_id, sid, direction, window_tag,
            is_primary, primary_window,
            m5_mode, m5_l, m5_h,
            m15_mode, m15_l, m15_h,
            h1_mode, h1_l, h1_h,
            n_sel, n_all, coverage_pct,
            pnl_sel, pnl_all,
            roi_sel, roi_all,
            wr_sel, wr_all,
            decision_class, float(decision_confidence), rationale
        )


# 🔸 Утилиты квантилей/бинов/дельт/преобразований

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


def _delta_wr(b1: Dict[str, Any], b5: Dict[str, Any]) -> float:
    n1 = max(b1["N"], 1)
    n5 = max(b5["N"], 1)
    wr1 = (b1["wins"] / n1) * 100.0
    wr5 = (b5["wins"] / n5) * 100.0
    return wr5 - wr1


def _to_float_or_none(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None