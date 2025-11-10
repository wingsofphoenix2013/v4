# oracle_pack_bl_analyzer.py — воркер PACK BL-анализатора (v5, post-WL): счётчик BL-соответствий, кривая ROI(T), выбор T*, история и сглаженный порог в active

# 🔸 Импорты
import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

import infra

# 🔸 Логгер
log = logging.getLogger("ORACLE_PACK_BL_ANALYZER")

# 🔸 Константы воркера / стримы
PACK_WL_READY_STREAM = "oracle:pack_lists:reports_ready"   # источник событий (v5 ready)
PACK_WL_CONSUMER_GROUP = "oracle_pack_bl_analyzer_group"
PACK_WL_CONSUMER_NAME  = "oracle_pack_bl_analyzer_worker"
READ_COUNT = 128
READ_BLOCK_MS = 30_000

# 🔸 Доменные константы
ONLY_VERSION = "v5"
ONLY_TIME_FRAME = "7d"
TF_LIST = ("m5", "m15", "h1")
DIRECTIONS = ("long", "short")

# 🔸 Окно анализа
WINDOW_SIZE_7D = timedelta(days=7)

# 🔸 Сглаживание
SMOOTH_K = 8                 # размер окна истории
SMOOTH_MIN_POINTS = 3        # минимум точек для применения сглаживания (иначе берём T_curr)


# 🔸 Публичная точка входа воркера
async def run_oracle_pack_bl_analyzer():
    # условия достаточности окружения
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск PACK_BL_ANALYZER: PG/Redis не инициализированы")
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

    log.info("🚀 Старт oracle_pack_bl_analyzer (v5, post-WL)")

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

                            # стратегия должна быть в MW-кэше (для PACK мы используем тот же набор стратегий)
                            if infra.market_watcher_strategies and sid not in infra.market_watcher_strategies:
                                acks.append(msg_id)
                                continue

                            t_ref = _parse_iso_utcnaive(win_end_iso)
                            if t_ref is None:
                                acks.append(msg_id)
                                continue

                            # гард «последний 7d» по source='pack'
                            if not await _is_latest_or_equal_7d_pack(conn, sid, t_ref):
                                acks.append(msg_id)
                                continue

                            # обработка трёх TF × два направления последовательно
                            win_start = t_ref - WINDOW_SIZE_7D
                            await _process_strategy_7d(conn, sid, win_start, t_ref)

                            acks.append(msg_id)

                        except Exception:
                            log.exception("❌ Ошибка обработки сообщения PACK_BL_ANALYZER")
                            acks.append(msg_id)

            # ACK обработанных сообщений
            if acks:
                try:
                    await infra.redis_client.xack(PACK_WL_READY_STREAM, PACK_WL_CONSUMER_GROUP, *acks)
                except Exception:
                    log.exception("⚠️ Ошибка ACK в PACK_BL_ANALYZER")

        except asyncio.CancelledError:
            log.debug("⏹️ PACK_BL_ANALYZER остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка цикла PACK_BL_ANALYZER — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Обработка одной стратегии: все TF×DIR по окну 7d
async def _process_strategy_7d(conn, strategy_id: int, win_start: datetime, win_end: datetime):
    # депозит стратегии (нормировка ROI)
    deposit = await conn.fetchval("SELECT deposit FROM strategies_v4 WHERE id = $1", int(strategy_id))
    dep = float(deposit or 0.0)
    if dep <= 0.0:
        dep = 1.0

    for tf in TF_LIST:
        for direction in DIRECTIONS:
            try:
                await _process_pair_tf_dir(conn, strategy_id, tf, direction, win_start, win_end, dep)
            except Exception:
                log.exception("❌ Ошибка PACK sid=%s tf=%s dir=%s", strategy_id, tf, direction)


# 🔸 Расчёт по паре (TF,DIR): post-WL выборка, кривая ROI(T), запись истории и active
async def _process_pair_tf_dir(
    conn,
    strategy_id: int,
    tf: str,
    direction: str,
    win_start: datetime,
    win_end: datetime,
    deposit_used: float,
):
    # выборка пар (bl_count, pnl) только для позиций, прошедших WL v5
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
        bl_counts AS (
          SELECT pa.position_uid,
                 COALESCE(COUNT(DISTINCT wbl.aggregated_id), 0) AS blc
          FROM passed pa
          LEFT JOIN oracle_pack_positions opp ON opp.position_uid = pa.position_uid
          LEFT JOIN oracle_pack_dict pd       ON pd.id = opp.pack_dict_id
          LEFT JOIN oracle_pack_whitelist wbl ON wbl.version = 'v5' AND wbl.list = 'blacklist'
                                             AND wbl.strategy_id = $1
                                             AND wbl.timeframe   = $5
                                             AND wbl.direction   = $2
                                             AND pd.strategy_id  = $1
                                             AND pd.timeframe    = $5
                                             AND pd.direction    = $2
                                             AND wbl.pack_base = pd.pack_base
                                             AND wbl.agg_key   = pd.agg_key
                                             AND wbl.agg_value = pd.agg_value
          GROUP BY pa.position_uid
        )
        SELECT b.blc AS bl_count, pa.pnl::float8 AS pnl
        FROM bl_counts b
        JOIN passed pa ON pa.position_uid = b.position_uid
        """,
        int(strategy_id), str(direction), win_start, win_end, str(tf)
    )

    if not rows:
        # нет позиций, прошедших WL — удаляем active (если был)
        await _delete_active_if_exists(conn, strategy_id, tf, direction)
        log.debug("🧹 PACK_BL_ACTIVE удалён (нет post-WL позиций): sid=%s tf=%s dir=%s", strategy_id, tf, direction)
        return

    # базовые величины
    pairs = [(int(r["bl_count"] or 0), float(r["pnl"] or 0.0)) for r in rows]
    positions_total = len(pairs)
    pnl_base = sum(p for _, p in pairs)
    roi_base = float(pnl_base) / float(deposit_used) if deposit_used > 0 else 0.0

    # гистограмма по bl_count и префиксы
    hist_n: Dict[int, int] = {}
    hist_p: Dict[int, float] = {}
    max_c = 0
    for c, pnl in pairs:
        hist_n[c] = hist_n.get(c, 0) + 1
        hist_p[c] = hist_p.get(c, 0.0) + float(pnl)
        if c > max_c:
            max_c = c

    pref_n = [0] * (max_c + 1)
    pref_p = [0.0] * (max_c + 1)
    acc_n = 0
    acc_p = 0.0
    for c in range(0, max_c + 1):
        acc_n += hist_n.get(c, 0)
        acc_p += hist_p.get(c, 0.0)
        pref_n[c] = acc_n
        pref_p[c] = acc_p

    # кривая ROI(T) и выбор T*
    roi_by_t: Dict[int, Dict[str, float | int]] = {}
    best_T = 0
    best_roi = roi_base
    best_n = positions_total
    best_pnl = pnl_base

    # T=0 — база
    roi_by_t[0] = {"n": positions_total, "pnl": float(pnl_base), "roi": float(roi_base)}

    for T in range(1, max_c + 1):
        n_passed = pref_n[T - 1]
        pnl_sum  = pref_p[T - 1]
        roi_T = (pnl_sum / deposit_used) if (deposit_used > 0 and n_passed > 0) else 0.0
        roi_by_t[T] = {"n": n_passed, "pnl": float(pnl_sum), "roi": float(roi_T)}

        # tie-break: минимальный T при равенстве ROI
        if (roi_T > best_roi) or (roi_T == best_roi and T < best_T):
            best_T = T
            best_roi = roi_T
            best_n = n_passed
            best_pnl = pnl_sum

    # запись истории
    await _insert_history(
        conn=conn,
        strategy_id=strategy_id,
        tf=tf,
        direction=direction,
        window=(win_start, win_end),
        deposit_used=deposit_used,
        positions_total=positions_total,
        pnl_sum_base=pnl_base,
        roi_base=roi_base,
        roi_curve=roi_by_t,
        best_T=best_T,
        best_positions=best_n,
        best_pnl_sum=best_pnl,
        best_roi=best_roi,
    )

    # сглаживание порога по истории
    T_smooth, hist_len, smooth_components = await _compute_smoothed_threshold(conn, strategy_id, tf, direction, best_T)

    # upsert active
    await _upsert_active(
        conn=conn,
        strategy_id=strategy_id,
        tf=tf,
        direction=direction,
        best_T_curr=best_T,
        best_T_smooth=T_smooth,
        best_roi=best_roi,
        roi_base=roi_base,
        positions_total=positions_total,
        deposit_used=deposit_used,
        window=(win_start, win_end),
        history_len=hist_len,
        smoothing_components=smooth_components,
    )

    log.debug("✅ PACK BL sid=%s tf=%s dir=%s → T*=%d (smooth=%d) ROI=%.6f base=%.6f n=%d",
              strategy_id, tf, direction, best_T, T_smooth, best_roi, roi_base, positions_total)


# 🔸 История: вставка расчёта
async def _insert_history(
    *,
    conn,
    strategy_id: int,
    tf: str,
    direction: str,
    window: Tuple[datetime, datetime],
    deposit_used: float,
    positions_total: int,
    pnl_sum_base: float,
    roi_base: float,
    roi_curve: Dict[int, Dict[str, float | int]],
    best_T: int,
    best_positions: int,
    best_pnl_sum: float,
    best_roi: float,
):
    win_start, win_end = window
    await conn.execute(
        """
        INSERT INTO oracle_pack_bl_analysis (
            strategy_id, timeframe, direction, window_start, window_end,
            deposit_used, positions_total_after_wl, pnl_sum_base_after_wl, roi_base_after_wl,
            roi_by_threshold, best_threshold, best_positions, best_pnl_sum, best_roi, created_at
        ) VALUES (
            $1,$2,$3,$4,$5,
            $6,$7,$8,$9,
            $10::jsonb,$11,$12,$13,$14, now()
        )
        """,
        int(strategy_id), str(tf), str(direction), win_start, win_end,
        float(deposit_used), int(positions_total), float(pnl_sum_base), float(roi_base),
        json.dumps({str(k): v for k, v in roi_curve.items()}, separators=(",", ":")),
        int(best_T), int(best_positions), float(best_pnl_sum), float(best_roi),
    )


# 🔸 Active: UPSERT текущего и сглаженного порога
async def _upsert_active(
    *,
    conn,
    strategy_id: int,
    tf: str,
    direction: str,
    best_T_curr: int,
    best_T_smooth: int,
    best_roi: float,
    roi_base: float,
    positions_total: int,
    deposit_used: float,
    window: Tuple[datetime, datetime],
    history_len: int,
    smoothing_components: Dict,
):
    win_start, win_end = window
    await conn.execute(
        """
        INSERT INTO oracle_pack_bl_active (
            strategy_id, timeframe, direction,
            best_threshold_curr, best_threshold_smoothed,
            best_roi, roi_base, positions_total, deposit_used,
            history_len, smoothing_components,
            window_start, window_end, computed_at, updated_at
        ) VALUES (
            $1,$2,$3,
            $4,$5,
            $6,$7,$8,$9,
            $10, $11::jsonb,
            $12,$13, now(), now()
        )
        ON CONFLICT (strategy_id, timeframe, direction)
        DO UPDATE SET
            best_threshold_curr      = EXCLUDED.best_threshold_curr,
            best_threshold_smoothed  = EXCLUDED.best_threshold_smoothed,
            best_roi                 = EXCLUDED.best_roi,
            roi_base                 = EXCLUDED.roi_base,
            positions_total          = EXCLUDED.positions_total,
            deposit_used             = EXCLUDED.deposit_used,
            history_len              = EXCLUDED.history_len,
            smoothing_components     = EXCLUDED.smoothing_components,
            window_start             = EXCLUDED.window_start,
            window_end               = EXCLUDED.window_end,
            computed_at              = EXCLUDED.computed_at,
            updated_at               = now()
        """,
        int(strategy_id), str(tf), str(direction),
        int(best_T_curr), int(best_T_smooth),
        float(best_roi), float(roi_base), int(positions_total), float(deposit_used),
        int(history_len), json.dumps(smoothing_components, separators=(",", ":")),
        win_start, win_end,
    )


# 🔸 Active: удаление при отсутствии выборки
async def _delete_active_if_exists(conn, strategy_id: int, tf: str, direction: str):
    await conn.execute(
        """
        DELETE FROM oracle_pack_bl_active
        WHERE strategy_id = $1 AND timeframe = $2 AND direction = $3
        """,
        int(strategy_id), str(tf), str(direction)
    )


# 🔸 Сглаживание порога (взвешенная медиана по последним K точкам)
async def _compute_smoothed_threshold(conn, strategy_id: int, tf: str, direction: str, T_curr: int) -> Tuple[int, int, Dict]:
    rows = await conn.fetch(
        """
        SELECT best_threshold AS t, positions_total_after_wl AS n
        FROM oracle_pack_bl_analysis
        WHERE strategy_id = $1 AND timeframe = $2 AND direction = $3
        ORDER BY created_at DESC
        LIMIT $4
        """,
        int(strategy_id), str(tf), str(direction), int(SMOOTH_K)
    )
    hist = [(int(r["t"] or 0), int(r["n"] or 0)) for r in rows]

    # если истории мало — сглаженный = текущий
    if len(hist) < SMOOTH_MIN_POINTS:
        return int(T_curr), len(hist), {"mode": "fallback", "items": [{"T": int(T_curr), "w": 1}]}

    # взвешенная медиана (w = число позиций), тай-брейк — больший T из центральных
    items = sorted(hist, key=lambda x: (x[0],))  # по T
    total_w = sum(max(1, w) for _, w in items)
    mid = (total_w + 1) // 2

    acc = 0
    T_smooth = items[-1][0]
    for T, w in items:
        acc += max(1, w)
        if acc >= mid:
            T_smooth = T
            break

    components = {"mode": "wmedian", "items": [{"T": int(T), "w": int(max(1, w))} for T, w in items]}
    return int(T_smooth), len(items), components


# 🔸 Вспомогательные

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