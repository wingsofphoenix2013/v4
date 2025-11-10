# oracle_pack_bl_detailed.py — воркер детального BL-анализа (post-WL v5): качество по каждому правилу, история и full-refresh active

# 🔸 Импорты
import asyncio
import json
import logging
import math
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import infra

# 🔸 Логгер
log = logging.getLogger("ORACLE_PACK_BL_DETAILED")

# 🔸 Константы стримов/конфигурации
PACK_WL_READY_STREAM = "oracle:pack_lists:reports_ready"
PACK_WL_CONSUMER_GROUP = "oracle_pack_bl_detailed_group"
PACK_WL_CONSUMER_NAME = "oracle_pack_bl_detailed_worker"
READ_COUNT = 128
READ_BLOCK_MS = 30_000

# 🔸 Доменные константы
ONLY_VERSION = "v5"
ONLY_TIME_FRAME = "7d"
TF_LIST = ("m5", "m15", "h1")
DIRECTIONS = ("long", "short")
WINDOW_SIZE_7D = timedelta(days=7)

# 🔸 Пороги/настройки отбора в ACTIVE
MIN_SUPPORT_N = 25                 # минимум хитов правила в окне
MIN_LIFT_ABS = 0.10                # минимум абсолютного лифта по loss-rate
MAX_P_VALUE = 0.10                 # максимум p-value (двусторонний z-тест)
MIN_DELTA_ROI = 0.00               # минимум улучшения ROI при исключении правила
HISTORY_K = 8                      # размер окна истории для устойчивости
STREAK_MIN = 2                     # минимум подряд идущих появлений
APPEAR_RATE_MIN = 0.50             # минимум доли появлений за K окон
EWM_HALF_LIFE = 6                  # полураспад для EWM по появлениям
# вычислим alpha для EWM
EWM_ALPHA = 1.0 - math.pow(0.5, 1.0 / max(1, EWM_HALF_LIFE))

# 🔸 Публичная точка входа воркера
async def run_oracle_pack_bl_detailed():
    # условия достаточности
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск PACK_BL_DETAILED: PG/Redis не инициализированы")
        return

    # создание consumer group (идемпотентно)
    try:
        await infra.redis_client.xgroup_create(
            name=PACK_WL_READY_STREAM,
            groupname=PACK_WL_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug("📡 Создана consumer group для %s", PACK_WL_READY_STREAM)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            log.exception("❌ Ошибка создания consumer group")
            return

    log.info("🚀 Старт oracle_pack_bl_detailed (v5, post-WL)")

    # основной цикл чтения событий
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=PACK_WL_CONSUMER_GROUP,
                consumername=PACK_WL_CONSUMER_NAME,
                streams={PACK_WL_READY_STREAM: ">"},
                count=READ_COUNT,
                block=READ_BLOCK_MS,
            )
            if not resp:
                continue

            acks: List[str] = []
            async with infra.pg_pool.acquire() as conn:
                for _stream_name, msgs in resp:
                    for msg_id, fields in msgs:
                        try:
                            payload = json.loads(fields.get("data", "{}"))
                            sid = int(payload.get("strategy_id", 0))
                            version = str(payload.get("version", "")).lower()
                            time_frame = str(payload.get("time_frame", "")).lower()
                            win_end_iso = payload.get("window_end")

                            # фильтр: только v5 / 7d / валидные поля
                            if not (sid and version == ONLY_VERSION and time_frame == ONLY_TIME_FRAME and win_end_iso):
                                acks.append(msg_id)
                                continue

                            # стратегия должна быть в MW-кэше (тот же набор)
                            if infra.market_watcher_strategies and sid not in infra.market_watcher_strategies:
                                acks.append(msg_id)
                                continue

                            t_end = _parse_iso_utcnaive(win_end_iso)
                            if t_end is None:
                                acks.append(msg_id)
                                continue

                            # гард «последний 7d (source='pack')»
                            if not await _is_latest_or_equal_7d_pack(conn, sid, t_end):
                                acks.append(msg_id)
                                continue

                            # окно и депозит
                            t_start = t_end - WINDOW_SIZE_7D
                            deposit = await conn.fetchval("SELECT deposit FROM strategies_v4 WHERE id = $1", int(sid))
                            dep = float(deposit or 0.0)
                            if dep <= 0.0:
                                dep = 1.0

                            # детальный анализ по всем TF×DIR
                            total_rules_exact = 0
                            total_rules_bykey = 0
                            inserted_analysis = 0
                            candidates_active: List[Tuple] = []

                            for tf in TF_LIST:
                                for direction in DIRECTIONS:
                                    try:
                                        # посчитать базу (post-WL)
                                        base_stats = await _fetch_base_post_wl(conn, sid, tf, direction, t_start, t_end)
                                        n_total = base_stats["n_total"]
                                        base_losses = base_stats["losses"]
                                        base_pnl = base_stats["pnl_sum"]
                                        if n_total == 0:
                                            # записывать нечего по этому срезу
                                            continue

                                        loss_rate_base = base_losses / n_total
                                        roi_base = base_pnl / dep

                                        # exact-статистика (по pack_base|agg_key|agg_value)
                                        rows_exact = await _fetch_hits_exact(conn, sid, tf, direction, t_start, t_end)
                                        # by_key-статистика (по pack_base|agg_key, без значения)
                                        rows_bykey = await _fetch_hits_bykey(conn, sid, tf, direction, t_start, t_end)

                                        # вставка в историю oracle_pack_bl_detailed_analysis
                                        ana_rows = []
                                        for (pack_base, agg_key, agg_value, n_hits, losses, wins, pnl_sum) in rows_exact:
                                            rec = _build_analysis_row(
                                                level="exact",
                                                sid=sid, tf=tf, direction=direction,
                                                win_start=t_start, win_end=t_end,
                                                pack_base=pack_base, agg_key=agg_key, agg_value=agg_value,
                                                n_total_after_wl=n_total, n_hits=n_hits,
                                                losses=losses, wins=wins, pnl_sum=pnl_sum,
                                                loss_rate_base=loss_rate_base, roi_base=roi_base, deposit_used=dep,
                                            )
                                            ana_rows.append(rec)
                                        for (pack_base, agg_key, n_hits, losses, wins, pnl_sum) in rows_bykey:
                                            rec = _build_analysis_row(
                                                level="by_key",
                                                sid=sid, tf=tf, direction=direction,
                                                win_start=t_start, win_end=t_end,
                                                pack_base=pack_base, agg_key=agg_key, agg_value=None,
                                                n_total_after_wl=n_total, n_hits=n_hits,
                                                losses=losses, wins=wins, pnl_sum=pnl_sum,
                                                loss_rate_base=loss_rate_base, roi_base=roi_base, deposit_used=dep,
                                            )
                                            ana_rows.append(rec)

                                        if ana_rows:
                                            inserted = await _insert_analysis_batch(conn, ana_rows)
                                            inserted_analysis += inserted

                                        total_rules_exact += len(rows_exact)
                                        total_rules_bykey += len(rows_bykey)

                                        # отбор кандидатов в ACTIVE по текущему окну
                                        for rec in ana_rows:
                                            if _passes_active_thresholds(rec):
                                                candidates_active.append(rec)

                                    except Exception:
                                        log.exception("❌ Ошибка детального анализа sid=%s tf=%s dir=%s", sid, tf, direction)

                            # построение устойчивости и full-refresh ACTIVE по стратегии
                            active_rows = []
                            if candidates_active:
                                for rec in candidates_active:
                                    try:
                                        pres = await _compute_presence(conn, rec)
                                        active_rows.append(_build_active_row(rec, pres))
                                    except Exception:
                                        log.exception("❌ Ошибка рассчёта устойчивости по правилу sid=%s tf=%s dir=%s %s|%s",
                                                      rec["strategy_id"], rec["timeframe"], rec["direction"],
                                                      rec["pack_base"], rec["agg_key"])

                            # full refresh ACTIVE (по стратегии)
                            await _refresh_active_for_strategy(conn, sid, active_rows)

                            # лог сводки по сообщению
                            log.info(
                                "[BL_DETAILED] sid=%s win=[%s..%s) rules: exact=%d by_key=%d | analysis_rows=%d | active_rows=%d",
                                sid, t_start.isoformat(), t_end.isoformat(),
                                total_rules_exact, total_rules_bykey, inserted_analysis, len(active_rows)
                            )

                            acks.append(msg_id)

                        except Exception:
                            log.exception("❌ Ошибка обработки сообщения PACK_BL_DETAILED")
                            acks.append(msg_id)

            # ACK обработанных сообщений
            if acks:
                try:
                    await infra.redis_client.xack(PACK_WL_READY_STREAM, PACK_WL_CONSUMER_GROUP, *acks)
                except Exception:
                    log.exception("⚠️ Ошибка ACK в PACK_BL_DETAILED")

        except asyncio.CancelledError:
            log.debug("⏹️ PACK_BL_DETAILED остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка цикла PACK_BL_DEТАILED — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 БАЗА post-WL: позиции и агрегаты по окну (TF, DIR)
async def _fetch_base_post_wl(conn, strategy_id: int, tf: str, direction: str, win_start: datetime, win_end: datetime) -> Dict:
    # строим базу: закрытые позиции → прошли WL v5
    row = await conn.fetchrow(
        """
        WITH base_p AS (
          SELECT p.position_uid, p.pnl
          FROM positions_v4 p
          WHERE p.strategy_id = $1
            AND p.status = 'closed'
            AND p.direction = $2
            AND p.closed_at >= $3 AND p.closed_at < $4
        ),
        wl_match AS (
          SELECT DISTINCT p.position_uid
          FROM base_p p
          JOIN oracle_pack_positions opp ON opp.position_uid = p.position_uid
          JOIN oracle_pack_dict pd       ON pd.id = opp.pack_dict_id
          JOIN oracle_pack_whitelist wwl ON wwl.version = 'v5' AND wwl.list = 'whitelist'
                                        AND wwl.strategy_id = $1
                                        AND wwl.timeframe   = $5
                                        AND wwl.direction   = $2
                                        AND wwl.pack_base = pd.pack_base
                                        AND wwl.agg_key   = pd.agg_key
                                        AND wwl.agg_value = pd.agg_value
          WHERE pd.strategy_id = $1 AND pd.timeframe = $5 AND pd.direction = $2
        ),
        passed AS (
          SELECT p.position_uid, p.pnl
          FROM base_p p
          JOIN wl_match m ON m.position_uid = p.position_uid
        )
        SELECT
          COUNT(*)::int                                   AS n_total,
          COALESCE(SUM((p.pnl <= 0)::int), 0)::int        AS losses,
          COALESCE(SUM(p.pnl), 0)::float8                 AS pnl_sum
        FROM passed p
        """,
        int(strategy_id), str(direction), win_start, win_end, str(tf)
    )
    return {"n_total": int(row["n_total"] or 0), "losses": int(row["losses"] or 0), "pnl_sum": float(row["pnl_sum"] or 0.0)}


# 🔸 ХИТЫ exact: pack_base|agg_key|agg_value (по DISTINCT позициям)
async def _fetch_hits_exact(conn, strategy_id: int, tf: str, direction: str, win_start: datetime, win_end: datetime) -> List[Tuple]:
    rows = await conn.fetch(
        """
        WITH base_p AS (
          SELECT p.position_uid, p.pnl
          FROM positions_v4 p
          WHERE p.strategy_id = $1
            AND p.status = 'closed'
            AND p.direction = $2
            AND p.closed_at >= $3 AND p.closed_at < $4
        ),
        wl_match AS (
          SELECT DISTINCT p.position_uid
          FROM base_p p
          JOIN oracle_pack_positions opp ON opp.position_uid = p.position_uid
          JOIN oracle_pack_dict pd       ON pd.id = opp.pack_dict_id
          JOIN oracle_pack_whitelist wwl ON wwl.version = 'v5' AND wwl.list = 'whitelist'
                                        AND wwl.strategy_id = $1
                                        AND wwl.timeframe   = $5
                                        AND wwl.direction   = $2
                                        AND wwl.pack_base = pd.pack_base
                                        AND wwl.agg_key   = pd.agg_key
                                        AND wwl.agg_value = pd.agg_value
          WHERE pd.strategy_id = $1 AND pd.timeframe = $5 AND pd.direction = $2
        ),
        passed AS (
          SELECT p.position_uid, p.pnl
          FROM base_p p
          JOIN wl_match m ON m.position_uid = p.position_uid
        ),
        hits AS (
          SELECT DISTINCT pa.position_uid, pd.pack_base, pd.agg_key, pd.agg_value
          FROM passed pa
          JOIN oracle_pack_positions opp ON opp.position_uid = pa.position_uid
          JOIN oracle_pack_dict pd       ON pd.id = opp.pack_dict_id
          JOIN oracle_pack_whitelist wbl ON wbl.version = 'v5' AND wbl.list = 'blacklist'
                                        AND wbl.strategy_id = $1
                                        AND wbl.timeframe   = $5
                                        AND wbl.direction   = $2
                                        AND wbl.pack_base = pd.pack_base
                                        AND wbl.agg_key   = pd.agg_key
                                        AND wbl.agg_value = pd.agg_value
          WHERE pd.strategy_id = $1 AND pd.timeframe = $5 AND pd.direction = $2
        )
        SELECT h.pack_base, h.agg_key, h.agg_value,
               COUNT(*)::int                                           AS n_hits,
               SUM( (pa.pnl <= 0)::int )::int                          AS losses,
               SUM( (pa.pnl > 0)::int )::int                           AS wins,
               COALESCE(SUM(pa.pnl), 0)::float8                        AS pnl_sum
        FROM hits h
        JOIN passed pa ON pa.position_uid = h.position_uid
        GROUP BY h.pack_base, h.agg_key, h.agg_value
        """,
        int(strategy_id), str(direction), win_start, win_end, str(tf)
    )
    # -> [(pack_base, agg_key, agg_value, n_hits, losses, wins, pnl_sum), ...]
    return [(
        str(r["pack_base"]), str(r["agg_key"]), str(r["agg_value"]),
        int(r["n_hits"]), int(r["losses"]), int(r["wins"]), float(r["pnl_sum"])
    ) for r in rows]


# 🔸 ХИТЫ by_key: pack_base|agg_key (объединение по значениям)
async def _fetch_hits_bykey(conn, strategy_id: int, tf: str, direction: str, win_start: datetime, win_end: datetime) -> List[Tuple]:
    rows = await conn.fetch(
        """
        WITH base_p AS (
          SELECT p.position_uid, p.pnl
          FROM positions_v4 p
          WHERE p.strategy_id = $1
            AND p.status = 'closed'
            AND p.direction = $2
            AND p.closed_at >= $3 AND p.closed_at < $4
        ),
        wl_match AS (
          SELECT DISTINCT p.position_uid
          FROM base_p p
          JOIN oracle_pack_positions opp ON opp.position_uid = p.position_uid
          JOIN oracle_pack_dict pd       ON pd.id = opp.pack_dict_id
          JOIN oracle_pack_whitelist wwl ON wwl.version = 'v5' AND wwl.list = 'whitelist'
                                        AND wwl.strategy_id = $1
                                        AND wwl.timeframe   = $5
                                        AND wwl.direction   = $2
                                        AND wwl.pack_base = pd.pack_base
                                        AND wwl.agg_key   = pd.agg_key
                                        AND wwl.agg_value = pd.agg_value
          WHERE pd.strategy_id = $1 AND pd.timeframe = $5 AND pd.direction = $2
        ),
        passed AS (
          SELECT p.position_uid, p.pnl
          FROM base_p p
          JOIN wl_match m ON m.position_uid = p.position_uid
        ),
        hits_exact AS (
          SELECT DISTINCT pa.position_uid, pd.pack_base, pd.agg_key
          FROM passed pa
          JOIN oracle_pack_positions opp ON opp.position_uid = pa.position_uid
          JOIN oracle_pack_dict pd       ON pd.id = opp.pack_dict_id
          JOIN oracle_pack_whitelist wbl ON wbl.version = 'v5' AND wbl.list = 'blacklist'
                                        AND wbl.strategy_id = $1
                                        AND wbl.timeframe   = $5
                                        AND wbl.direction   = $2
                                        AND wbl.pack_base = pd.pack_base
                                        AND wbl.agg_key   = pd.agg_key
                                        AND wbl.agg_value = pd.agg_value
          WHERE pd.strategy_id = $1 AND pd.timeframe = $5 AND pd.direction = $2
        )
        SELECT h.pack_base, h.agg_key,
               COUNT(*)::int                               AS n_hits,
               SUM( (pa.pnl <= 0)::int )::int              AS losses,
               SUM( (pa.pnl > 0)::int )::int               AS wins,
               COALESCE(SUM(pa.pnl), 0)::float8            AS pnl_sum
        FROM hits_exact h
        JOIN passed pa ON pa.position_uid = h.position_uid
        GROUP BY h.pack_base, h.agg_key
        """,
        int(strategy_id), str(direction), win_start, win_end, str(tf)
    )
    # -> [(pack_base, agg_key, n_hits, losses, wins, pnl_sum), ...]
    return [(
        str(r["pack_base"]), str(r["agg_key"]),
        int(r["n_hits"]), int(r["losses"]), int(r["wins"]), float(r["pnl_sum"])
    ) for r in rows]


# 🔸 Построение словаря-строки для истории detailed_analysis (с расчётом метрик)
def _build_analysis_row(
    *,
    level: str,
    sid: int,
    tf: str,
    direction: str,
    win_start: datetime,
    win_end: datetime,
    pack_base: str,
    agg_key: str,
    agg_value: Optional[str],
    n_total_after_wl: int,
    n_hits: int,
    losses: int,
    wins: int,
    pnl_sum: float,
    loss_rate_base: float,
    roi_base: float,
    deposit_used: float,
) -> Dict:
    # базовые вычисления
    loss_rate_rule = (losses / n_hits) if n_hits > 0 else 0.0
    lift_abs = loss_rate_rule - loss_rate_base
    lift_rel = (loss_rate_rule / loss_rate_base) if loss_rate_base > 0 else 0.0
    pnl_avg_rule = (pnl_sum / n_hits) if n_hits > 0 else 0.0

    # ROI, если исключить это правило
    pnl_drop = pnl_sum
    n_remain = max(0, n_total_after_wl - n_hits)
    roi_if_drop = (( (roi_base * deposit_used) - pnl_drop ) / deposit_used) if (deposit_used > 0 and n_remain > 0) else 0.0
    delta_roi = roi_if_drop - roi_base

    # доверительный интервал Вилсона и p-value (z-тест двух пропорций)
    ci_low, ci_high = _wilson_ci(losses, n_hits, z=1.96)
    p_value = _two_prop_pvalue(losses, n_hits, loss_rate_base)

    # support_ratio
    support_ratio = (n_hits / n_total_after_wl) if n_total_after_wl > 0 else 0.0

    return {
        "analysis_level": level,
        "strategy_id": int(sid),
        "timeframe": str(tf),
        "direction": str(direction),
        "version": ONLY_VERSION,
        "window_start": win_start,
        "window_end": win_end,
        "pack_base": str(pack_base),
        "agg_key": str(agg_key),
        "agg_value": (None if level == "by_key" else (None if agg_value is None else str(agg_value))),
        "n_total_after_wl": int(n_total_after_wl),
        "n_rule_hits": int(n_hits),
        "tp": int(losses),
        "fp": int(wins),
        "fn": int(max(0, (loss_rate_base * n_total_after_wl)) - losses) if n_total_after_wl > 0 else 0,  # справочная оценка
        "tn": int(n_total_after_wl - int(loss_rate_base * n_total_after_wl) - wins) if n_total_after_wl > 0 else 0,
        "loss_rate_rule": round(float(loss_rate_rule), 4),
        "loss_rate_base": round(float(loss_rate_base), 4),
        "lift_abs": round(float(lift_abs), 4),
        "lift_rel": round(float(lift_rel), 4),
        "pnl_sum_rule": round(float(pnl_sum), 4),
        "pnl_avg_rule": round(float(pnl_avg_rule), 4),
        "roi_base_after_wl": round(float(roi_base), 6),
        "roi_if_drop_rule": round(float(roi_if_drop), 6),
        "delta_roi": round(float(delta_roi), 6),
        "support_ratio": round(float(support_ratio), 4),
        "p_value": round(float(p_value), 6),
        "ci_low": round(float(ci_low), 4),
        "ci_high": round(float(ci_high), 4),
    }


# 🔸 Батч-вставка истории в oracle_pack_bl_detailed_analysis
async def _insert_analysis_batch(conn, rows: List[Dict]) -> int:
    if not rows:
        return 0
    await conn.executemany(
        """
        INSERT INTO oracle_pack_bl_detailed_analysis (
            strategy_id, timeframe, direction, version, analysis_level,
            window_start, window_end,
            pack_base, agg_key, agg_value,
            n_total_after_wl, n_rule_hits, tp, fp, fn, tn,
            loss_rate_rule, loss_rate_base, lift_abs, lift_rel,
            pnl_sum_rule, pnl_avg_rule, roi_base_after_wl, roi_if_drop_rule, delta_roi,
            support_ratio, p_value, ci_low, ci_high, created_at
        ) VALUES (
            $1,$2,$3,$4,$5,
            $6,$7,
            $8,$9,$10,
            $11,$12,$13,$14,$15,$16,
            $17,$18,$19,$20,
            $21,$22,$23,$24,$25,
            $26,$27,$28,$29, now()
        )
        ON CONFLICT (strategy_id, timeframe, direction, version, analysis_level, window_start, window_end, pack_base, agg_key, COALESCE(agg_value,'∅'))
        DO UPDATE SET
            n_total_after_wl = EXCLUDED.n_total_after_wl,
            n_rule_hits      = EXCLUDED.n_rule_hits,
            tp               = EXCLUDED.tp,
            fp               = EXCLUDED.fp,
            fn               = EXCLUDED.fn,
            tn               = EXCLUDED.tn,
            loss_rate_rule   = EXCLUDED.loss_rate_rule,
            loss_rate_base   = EXCLUDED.loss_rate_base,
            lift_abs         = EXCLUDED.lift_abs,
            lift_rel         = EXCLUDED.lift_rel,
            pnl_sum_rule     = EXCLUDED.pnl_sum_rule,
            pnl_avg_rule     = EXCLUDED.pnl_avg_rule,
            roi_base_after_wl= EXCLUDED.roi_base_after_wl,
            roi_if_drop_rule = EXCLUDED.roi_if_drop_rule,
            delta_roi        = EXCLUDED.delta_roi,
            support_ratio    = EXCLUDED.support_ratio,
            p_value          = EXCLUDED.p_value,
            ci_low           = EXCLUDED.ci_low,
            ci_high          = EXCLUDED.ci_high,
            created_at       = now()
        """,
        [(
            r["strategy_id"], r["timeframe"], r["direction"], r["version"], r["analysis_level"],
            r["window_start"], r["window_end"],
            r["pack_base"], r["agg_key"], r["agg_value"],
            r["n_total_after_wl"], r["n_rule_hits"], r["tp"], r["fp"], r["fn"], r["tn"],
            r["loss_rate_rule"], r["loss_rate_base"], r["lift_abs"], r["lift_rel"],
            r["pnl_sum_rule"], r["pnl_avg_rule"], r["roi_base_after_wl"], r["roi_if_drop_rule"], r["delta_roi"],
            r["support_ratio"], r["p_value"], r["ci_low"], r["ci_high"]
        ) for r in rows]
    )
    return len(rows)


# 🔸 Проверка прохождения порогов для ACTIVE
def _passes_active_thresholds(r: Dict) -> bool:
    # условия пригодности текущего окна
    if r["n_rule_hits"] < MIN_SUPPORT_N:
        return False
    if r["lift_abs"] < MIN_LIFT_ABS:
        return False
    if r["p_value"] > MAX_P_VALUE:
        return False
    if r["delta_roi"] < MIN_DELTA_ROI:
        return False
    return True


# 🔸 Рассчёт устойчивости появления правила по истории (последние K окон)
async def _compute_presence(conn, r: Dict) -> Dict:
    # выборка последних K окон для конкретного ключа правила
    rows = await conn.fetch(
        """
        SELECT window_end,
               n_rule_hits, loss_rate_rule, loss_rate_base, lift_abs, p_value, delta_roi
        FROM oracle_pack_bl_detailed_analysis
        WHERE strategy_id = $1 AND timeframe = $2 AND direction = $3 AND version = $4
          AND analysis_level = $5
          AND pack_base = $6 AND agg_key = $7
          AND ( (analysis_level = 'by_key'  AND agg_value IS NULL)
             OR (analysis_level = 'exact'   AND agg_value = $8) )
        ORDER BY window_end DESC
        LIMIT $9
        """,
        int(r["strategy_id"]), str(r["timeframe"]), str(r["direction"]), ONLY_VERSION,
        str(r["analysis_level"]), str(r["pack_base"]), str(r["agg_key"]), r["agg_value"], int(HISTORY_K)
    )
    # подготовка последовательности (от старых к новым)
    hist = list(reversed([dict(x) for x in rows]))
    windows_considered = len(hist)
    if windows_considered == 0:
        return {
            "windows_considered": 0, "appearance_count": 0, "appearance_rate": 0.0,
            "appearance_streak": 0, "ewm_presence": 0.0, "first_seen_at": None, "last_seen_at": None
        }

    # функция проверки прохождения порогов для каждого старого окна
    def is_active(p):
        return (
            int(p["n_rule_hits"] or 0) >= MIN_SUPPORT_N and
            float(p["lift_abs"] or 0.0) >= MIN_LIFT_ABS and
            float(p["p_value"] or 1.0) <= MAX_P_VALUE and
            float(p["delta_roi"] or 0.0) >= MIN_DELTA_ROI
        )

    # подсчёт appearance_count / rate
    flags = [1 if is_active(p) else 0 for p in hist]
    appearance_count = sum(flags)
    appearance_rate = (appearance_count / windows_considered) if windows_considered > 0 else 0.0

    # streak (серия в конце)
    appearance_streak = 0
    for v in reversed(flags):
        if v == 1:
            appearance_streak += 1
        else:
            break

    # EWM по появлениям
    ewm = 0.0
    for v in flags:
        ewm = EWM_ALPHA * v + (1.0 - EWM_ALPHA) * ewm

    # first/last seen
    first_seen_at = hist[0]["window_end"]
    last_seen_at = hist[-1]["window_end"]

    return {
        "windows_considered": int(windows_considered),
        "appearance_count": int(appearance_count),
        "appearance_rate": round(float(appearance_rate), 4),
        "appearance_streak": int(appearance_streak),
        "ewm_presence": round(float(ewm), 4),
        "first_seen_at": first_seen_at,
        "last_seen_at": last_seen_at,
    }


# 🔸 Сборка строки для ACTIVE
def _build_active_row(r: Dict, pres: Dict) -> Tuple:
    # статус по устойчивости
    status = "active"
    if (pres["appearance_rate"] < APPEAR_RATE_MIN) and (pres["appearance_streak"] < STREAK_MIN) and (pres["ewm_presence"] < APPEAR_RATE_MIN):
        status = "candidate"

    # простой скоринг (можно заменить позже)
    score = (
        2.0 * r["lift_abs"] +
        1.0 * r["delta_roi"] +
        0.5 * pres["appearance_rate"] +
        0.3 * pres["ewm_presence"]
    )

    return (
        int(r["strategy_id"]), str(r["timeframe"]), str(r["direction"]),
        ONLY_VERSION, str(r["analysis_level"]),
        str(r["pack_base"]), str(r["agg_key"]), r["agg_value"],
        int(r["n_total_after_wl"]), int(r["n_rule_hits"]),
        float(r["loss_rate_rule"]), float(r["loss_rate_base"]),
        float(r["lift_abs"]), float(r["lift_rel"]),
        float(r["pnl_sum_rule"]), float(r["pnl_avg_rule"]),
        float(r["roi_base_after_wl"]), float(r["roi_if_drop_rule"]), float(r["delta_roi"]),
        float(r["support_ratio"]), float(r["p_value"]), float(r["ci_low"]), float(r["ci_high"]),
        int(pres["windows_considered"]), int(pres["appearance_count"]),
        float(pres["appearance_rate"]), int(pres["appearance_streak"]),
        float(pres["ewm_presence"]), pres["first_seen_at"], pres["last_seen_at"],
        str(status), float(round(score, 6))
    )


# 🔸 Full refresh ACTIVE по стратегии
async def _refresh_active_for_strategy(conn, strategy_id: int, rows: List[Tuple]):
    # удаляем старое
    await conn.execute(
        "DELETE FROM oracle_pack_bl_detailed_active WHERE strategy_id = $1 AND version = $2",
        int(strategy_id), ONLY_VERSION
    )
    inserted = 0
    if rows:
        await conn.executemany(
            """
            INSERT INTO oracle_pack_bl_detailed_active (
                strategy_id, timeframe, direction, version, analysis_level,
                pack_base, agg_key, agg_value,
                n_total_after_wl, n_rule_hits,
                loss_rate_rule, loss_rate_base, lift_abs, lift_rel,
                pnl_sum_rule, pnl_avg_rule, roi_base_after_wl, roi_if_drop_rule, delta_roi,
                support_ratio, p_value, ci_low, ci_high,
                windows_considered, appearance_count, appearance_rate, appearance_streak, ewm_presence,
                first_seen_at, last_seen_at, status, score,
                computed_at, updated_at
            ) VALUES (
                $1,$2,$3,$4,$5,
                $6,$7,$8,
                $9,$10,
                $11,$12,$13,$14,
                $15,$16,$17,$18,$19,
                $20,$21,$22,$23,
                $24,$25,$26,$27,$28,
                $29,$30,$31,$32,
                now(), now()
            )
            """,
            rows
        )
        inserted = len(rows)
    # итог по стратегии
    log.info("[BL_DETAILED_ACTIVE] sid=%s full-refresh: inserted=%d", strategy_id, inserted)


# 🔸 Вспомогательные: ISO-парсинг и гард «последний 7d pack»
def _parse_iso_utcnaive(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", ""))
    except Exception:
        return None


async def _is_latest_or_equal_7d_pack(conn, strategy_id: int, window_end: datetime) -> bool:
    last = await conn.fetchval(
        """
        SELECT MAX(window_end) FROM oracle_report_stat
        WHERE strategy_id = $1 AND time_frame = '7d' AND source = 'pack'
        """,
        int(strategy_id)
    )
    if last is None:
        return True
    return window_end >= last


# 🔸 Вспомогательные: Wilson CI и p-value (двухпроп. z-тест)
def _wilson_ci(k: int, n: int, z: float = 1.96) -> Tuple[float, float]:
    if n <= 0:
        return 0.0, 0.0
    p = k / n
    denom = 1 + (z**2) / n
    center = (p + (z**2)/(2*n)) / denom
    margin = (z * math.sqrt((p*(1 - p) + (z**2)/(4*n)) / n)) / denom
    lo = max(0.0, center - margin)
    hi = min(1.0, center + margin)
    return lo, hi


def _two_prop_pvalue(k_rule: int, n_rule: int, p_base: float) -> float:
    # тест: H0: p_rule == p_base; H1 (двусторонняя): p_rule != p_base
    if n_rule <= 0:
        return 1.0
    if p_base <= 0.0 or p_base >= 1.0:
        return 1.0
    p_rule = k_rule / n_rule
    se = math.sqrt(p_base * (1.0 - p_base) / n_rule)
    if se == 0.0:
        return 1.0
    z = (p_rule - p_base) / se
    # двусторонний p-value через норм. распр.
    # Phi(z) ≈ 0.5 * (1 + erf(z / sqrt(2)))
    tail = 0.5 * (1.0 - math.erf(abs(z) / math.sqrt(2.0)))
    return max(0.0, min(1.0, 2.0 * tail))