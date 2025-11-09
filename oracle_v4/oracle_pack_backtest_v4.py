# oracle_pack_backtest_v4.py — воркер v4-бэктеста (PACK): кривая ROI(t) по состояниям оси, выбор порога, WL/BL v4 + глобальная очистка старых записей

# 🔸 Импорты
import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List, Tuple

import infra

# 🔸 Логгер
log = logging.getLogger("ORACLE_PACK_BACKTEST_V4")

# 🔸 Стрим-источник (готов отчёт PACK — можно строить v4)
PACK_REPORT_STREAM = "oracle:pack:reports_ready"
PACK_V4_CONSUMER_GROUP = "oracle_pack_backtest_v4_group"
PACK_V4_CONSUMER_NAME = "oracle_pack_backtest_v4_worker"

# 🔸 Стрим-уведомление: списки готовы (симметрия с v1/v2/v3)
PACK_LISTS_READY_STREAM = "oracle:pack_lists:reports_ready"
PACK_LISTS_READY_MAXLEN = 10_000

# 🔸 Общие настройки
TF_LIST = ("m5", "m15", "h1")
DIRECTIONS = ("long", "short")

# 🔸 Порог для дефолта (как в v2 логике)
FALLBACK_SHARE = 0.02

# 🔸 Критерии WL/BL после выбора порога
WL_WR_MIN = 0.5999
BL_WR_MAX = 0.6


# 🔸 Публичная точка входа воркера (вызывается из oracle_v4_main.py → run_safe_loop)
async def run_oracle_pack_backtest_v4():
    # условия достаточности окружения
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск: PG/Redis не инициализированы")
        return

    # создание consumer group (идемпотентно)
    try:
        await infra.redis_client.xgroup_create(
            name=PACK_REPORT_STREAM, groupname=PACK_V4_CONSUMER_GROUP, id="$", mkstream=True
        )
        log.debug("📡 Создана группа потребителей в Redis Stream: %s", PACK_V4_CONSUMER_GROUP)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            log.exception("❌ Ошибка инициализации группы Redis Stream")
            return

    log.debug("🚀 Старт воркера PACK backtest v4")

    # основной цикл чтения стрима
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=PACK_V4_CONSUMER_GROUP,
                consumername=PACK_V4_CONSUMER_NAME,
                streams={PACK_REPORT_STREAM: ">"},
                count=64,
                block=30_000,
            )
            if not resp:
                continue

            for _stream_name, msgs in resp:
                for msg_id, fields in msgs:
                    try:
                        payload = json.loads(fields.get("data", "{}"))
                        report_id = int(payload.get("report_id", 0))
                        strategy_id = int(payload.get("strategy_id", 0))
                        time_frame = str(payload.get("time_frame") or "")
                        window_end = payload.get("window_end")

                        # обрабатываем только 7d
                        if not (report_id and strategy_id and window_end and time_frame == "7d"):
                            await infra.redis_client.xack(PACK_REPORT_STREAM, PACK_V4_CONSUMER_GROUP, msg_id)
                            continue

                        await _process_report_v4(report_id, strategy_id, window_end)
                        await infra.redis_client.xack(PACK_REPORT_STREAM, PACK_V4_CONSUMER_GROUP, msg_id)
                    except Exception:
                        log.exception("❌ Ошибка обработки сообщения в PACK v4")
        except asyncio.CancelledError:
            log.debug("⏹️ Воркер PACK v4 остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка цикла PACK v4 — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Обработка одного отчёта 7d для всех TF (формирование кривых и публикация WL/BL v4)
async def _process_report_v4(report_id: int, strategy_id: int, window_end_iso: str):
    # парсинг window_end (для лога/пэйлоада)
    try:
        window_end_dt = datetime.fromisoformat(str(window_end_iso).replace("Z", ""))
    except Exception:
        log.exception("❌ Неверный формат window_end: %r", window_end_iso)
        return

    async with infra.pg_pool.acquire() as conn:
        # гард: обрабатываем только самый свежий PACK-отчёт 7d по стратегии
        latest_id = await conn.fetchval(
            """
            SELECT id
            FROM oracle_report_stat
            WHERE strategy_id = $1 AND time_frame = '7d' AND source = 'pack'
            ORDER BY window_end DESC, created_at DESC
            LIMIT 1
            """,
            int(strategy_id)
        )
        if latest_id is None:
            log.debug("ℹ️ Пропуск sid=%s: нет PACK-отчётов 7d", strategy_id)
            return
        if int(latest_id) != int(report_id):
            log.debug("⏭️ Пропуск sid=%s rep=%s: не последний PACK-отчёт (latest=%s)", strategy_id, report_id, latest_id)
            return

        # глобальная очистка: удалить ВСЕ v4 записи PACK WL/BL по стратегии, ссылающиеся на старые report_id
        res = await conn.execute(
            """
            DELETE FROM oracle_pack_whitelist w
            USING oracle_pack_aggregated_stat a
            WHERE w.version = 'v4'
              AND w.strategy_id = $1
              AND w.aggregated_id = a.id
              AND a.report_id <> $2
            """,
            int(strategy_id), int(report_id)
        )
        log.debug("🧹 PACK v4 очистка по sid=%s rep=%s: %s", strategy_id, report_id, res)

        # депозит стратегии
        deposit = await conn.fetchval("SELECT deposit FROM strategies_v4 WHERE id = $1", int(strategy_id))
        if deposit is None:
            log.debug("ℹ️ Пропуск sid=%s: отсутствует депозит (после очистки)", strategy_id)
            return
        deposit = float(deposit)

        method = "v4"

        # берём все PACK-агрегаты этого отчёта 7d
        rows = await conn.fetch(
            """
            SELECT
              id, direction, timeframe, pack_base, agg_type, agg_key, agg_value,
              trades_total, pnl_sum_total, winrate, confidence
            FROM oracle_pack_aggregated_stat
            WHERE report_id = $1
              AND time_frame = '7d'
            """,
            int(report_id)
        )
        if not rows:
            log.debug("ℹ️ Нет PACK-агрегатов для report_id=%s (после очистки)", report_id)
            return

        # группировка по оси: (tf, dir, pack_base, agg_type, agg_key)
        by_axis: Dict[Tuple[str, str, str, str, str], List[dict]] = {}
        for r in rows:
            key = (str(r["timeframe"]), str(r["direction"]), str(r["pack_base"]), str(r["agg_type"]), str(r["agg_key"]))
            by_axis.setdefault(key, []).append(dict(r))

        total_wl_inserted = 0
        total_bl_inserted = 0

        # обработка по TF: на каждый TF — свой run в oracle_pack_backtest_log
        for tf in TF_LIST:
            axes_tf = [(tf, d, pb, at, ak) for (t, d, pb, at, ak) in by_axis.keys() if t == tf]
            if not axes_tf:
                continue

            # идемпотентность: один run на (sid, report_id, tf, method='v4')
            exists = await conn.fetchval(
                """
                SELECT 1 FROM oracle_pack_backtest_log
                 WHERE strategy_id = $1 AND report_id = $2 AND timeframe = $3 AND method = $4
                """,
                int(strategy_id), int(report_id), tf, method
            )
            if exists:
                log.debug("⏭️ Пропуск: уже есть pack_backtest_log (sid=%s rep=%s tf=%s method=%s)", strategy_id, report_id, tf, method)
                continue

            run_id = await _create_run_log(conn, strategy_id, report_id, tf, method, deposit, window_end_dt.isoformat())

            summary_total = 0
            summary_improved = 0
            summary_fallback = 0
            summary_skipped = 0
            grid_rows_written = 0
            wl_written = 0
            bl_written = 0

            for _tf, direction, pack_base, agg_type, agg_key in axes_tf:
                states = by_axis.get((_tf, direction, pack_base, agg_type, agg_key), [])
                if not states:
                    continue
                summary_total += 1

                # построение кривой по массе и выбор решения
                written, decision = await _build_curve_and_decide_v4(
                    conn=conn,
                    run_id=run_id,
                    strategy_id=strategy_id,
                    report_id=report_id,
                    timeframe=tf,
                    direction=direction,
                    pack_base=pack_base,
                    agg_type=agg_type,
                    agg_key=agg_key,
                    deposit=deposit,
                    states=states,
                )
                grid_rows_written += written

                # публикация WL/BL по выбранному набору
                if decision["skip_negative"]:
                    summary_skipped += 1
                    continue

                if decision["is_fallback"]:
                    summary_fallback += 1
                else:
                    summary_improved += 1

                w_wl, w_bl = await _publish_v4_lists_for_decision(
                    conn=conn,
                    strategy_id=strategy_id,
                    timeframe=tf,
                    direction=direction,
                    pack_base=pack_base,
                    agg_type=agg_type,
                    agg_key=agg_key,
                    kept_ids=set(decision["kept_ids"]),
                )
                wl_written += w_wl
                bl_written += w_bl

            # финализируем run по TF
            await _finalize_run_log(
                conn=conn,
                run_id=run_id,
                status="ok",
                summary={
                    "total": summary_total,
                    "improved": summary_improved,
                    "fallback": summary_fallback,
                    "skipped": summary_skipped,
                    "grid_rows": grid_rows_written,
                    "wl_rows": wl_written,
                    "bl_rows": bl_written,
                },
            )
            log.debug(
                "✅ PACK v4 готов: sid=%s rep=%s tf=%s total=%d improved=%d fallback=%d skipped=%d wl=%d bl=%d",
                strategy_id, report_id, tf, summary_total, summary_improved, summary_fallback, summary_skipped, wl_written, bl_written
            )

            total_wl_inserted += wl_written
            total_bl_inserted += bl_written

        # после всех TF публикуем событие готовности v4
        try:
            payload = {
                "strategy_id": int(strategy_id),
                "report_id": int(report_id),
                "time_frame": "7d",
                "version": "v4",
                "window_end": window_end_dt.isoformat(),
                "rows_inserted": int(total_wl_inserted + total_bl_inserted),
                "generated_at": datetime.utcnow().replace(tzinfo=None).isoformat(),
            }
            await infra.redis_client.xadd(
                name=PACK_LISTS_READY_STREAM,
                fields={"data": json.dumps(payload, separators=(",", ":"))},
                maxlen=PACK_LISTS_READY_MAXLEN,
                approximate=True,
            )
            log.debug("[PACK_WL_READY v4] sid=%s rep=%s rows=%d", strategy_id, report_id, payload["rows_inserted"])
        except Exception:
            log.exception("❌ Ошибка публикации события в %s (v4)", PACK_LISTS_READY_STREAM)


# 🔸 Создание записи в oracle_pack_backtest_log
async def _create_run_log(
    conn,
    strategy_id: int,
    report_id: int,
    timeframe: str,
    method: str,
    deposit: float,
    stream_msg_id: str,
) -> int:
    row = await conn.fetchrow(
        """
        INSERT INTO oracle_pack_backtest_log (
          strategy_id, report_id, timeframe, method, status,
          started_at, deposit_used, created_at, updated_at, stream_msg_id
        ) VALUES (
          $1,$2,$3,$4,'ok', now(), $5, now(), now(), $6
        )
        RETURNING id
        """,
        int(strategy_id), int(report_id), str(timeframe), str(method), float(deposit), str(stream_msg_id)
    )
    return int(row["id"])


# 🔸 Финализация oracle_pack_backtest_log
async def _finalize_run_log(conn, run_id: int, status: str, summary: Dict):
    await conn.execute(
        """
        UPDATE oracle_pack_backtest_log
           SET status = $2,
               finished_at = now(),
               summary_keys_total       = $3,
               summary_improved         = $4,
               summary_fallback         = $5,
               summary_skipped_negative = $6,
               grid_rows_written        = $7,
               wl_rows_written          = $8,
               bl_rows_written          = $9,
               updated_at               = now()
         WHERE id = $1
        """,
        int(run_id),
        str(status),
        int((summary or {}).get("total", 0)),
        int((summary or {}).get("improved", 0)),
        int((summary or {}).get("fallback", 0)),
        int((summary or {}).get("skipped", 0)),
        int((summary or {}).get("grid_rows", 0)),
        int((summary or {}).get("wl_rows", 0)),
        int((summary or {}).get("bl_rows", 0)),
    )


# 🔸 Построение кривой по оси (массовый тест) и выбор решения
async def _build_curve_and_decide_v4(
    conn,
    run_id: int,
    strategy_id: int,
    report_id: int,
    timeframe: str,
    direction: str,
    pack_base: str,
    agg_type: str,
    agg_key: str,
    deposit: float,
    states: List[dict],
) -> Tuple[int, Dict]:
    # базовые суммы по оси
    total_trades = sum(int(s["trades_total"] or 0) for s in states)
    pnl_base = sum(float(s["pnl_sum_total"] or 0.0) for s in states)
    roi_base = (pnl_base / deposit) if deposit else 0.0

    # подготовка элементов (n, pnl, доля)
    items = []
    for s in states:
        n = int(s["trades_total"] or 0)
        pnl = float(s["pnl_sum_total"] or 0.0)
        share = (float(n) / float(total_trades)) if total_trades > 0 else 0.0
        items.append({"id": int(s["id"]), "n": n, "pnl": pnl, "share": share})
    items.sort(key=lambda it: (it["n"], it["id"]))  # от редких к частым

    # шаг 0 (без фильтра)
    grid_rows = []
    kept_ids_all = [it["id"] for it in items]
    grid_rows.append({
        "step_rank": 0,
        "cutoff_share": 0.0,
        "kept_states_count": len(items),
        "kept_trades": total_trades,
        "kept_mass_share": 1.0 if total_trades > 0 else 0.0,
        "pnl_kept": float(round(pnl_base, 4)),
        "roi": float(round(roi_base, 6)),
        "roi_delta": 0.0,
        "kept_ids": kept_ids_all,
        "is_winner": False,
        "is_fallback": False,
        "skip_negative": False,
    })

    # шаг по порогу: отбрасываем состояния с share ≤ t
    def build_step_for_cut(t: float):
        kept = [it for it in items if it["share"] > t]
        kept_ids = [it["id"] for it in kept]
        kept_trades = sum(it["n"] for it in kept)
        kept_pnl = sum(it["pnl"] for it in kept)
        roi = (kept_pnl / float(deposit)) if deposit else 0.0
        roi_delta = roi - roi_base
        mass_share = (kept_trades / total_trades) if total_trades else 0.0
        return {
            "cutoff_share": float(round(t, 8)),
            "kept_states_count": len(kept),
            "kept_trades": int(kept_trades),
            "kept_mass_share": float(round(mass_share, 8)),
            "pnl_kept": float(round(kept_pnl, 4)),
            "roi": float(round(roi, 6)),
            "roi_delta": float(round(roi_delta, 6)),
            "kept_ids": kept_ids,
            "is_winner": False,
            "is_fallback": False,
            "skip_negative": False,
        }

    # уникальные пороги — эмпирические доли состояний (по возрастанию)
    unique_cuts = []
    seen = set()
    for it in items:
        t = it["share"]
        if t not in seen:
            seen.add(t)
            unique_cuts.append(t)

    # проходим по порогам; последний шаг фиксируем даже если пусто
    rank = 1
    for t in unique_cuts:
        step = build_step_for_cut(t)
        if not step["kept_ids"]:
            grid_rows.append({**step, "step_rank": rank})
            rank += 1
            break
        if grid_rows and set(step["kept_ids"]) == set(grid_rows[-1]["kept_ids"]):
            continue
        grid_rows.append({**step, "step_rank": rank})
        rank += 1

    # выбор лучшего шага (макс ROI; tie-break — меньший cutoff_share)
    best = max(grid_rows, key=lambda r: (r["roi"], -r["cutoff_share"]))
    improved = best["roi"] > roi_base

    # fallback к 2% если улучшения нет
    used_fallback = False
    if not improved:
        fb = build_step_for_cut(FALLBACK_SHARE)
        fb["step_rank"] = rank
        fb["is_fallback"] = True
        grid_rows.append(fb)
        best = fb
        used_fallback = True

    # стоп-правило: итоговый ROI < 0 → публикаций по оси нет
    skip_negative = best["roi"] < 0.0
    best["is_winner"] = True
    best["skip_negative"] = skip_negative

    # запись сетки
    written = await _insert_grid_rows(
        conn=conn,
        run_id=run_id,
        strategy_id=strategy_id,
        report_id=report_id,
        timeframe=timeframe,
        direction=direction,
        pack_base=pack_base,
        agg_type=agg_type,
        agg_key=agg_key,
        rows=grid_rows,
    )

    decision = {
        "is_fallback": used_fallback,
        "skip_negative": skip_negative,
        "kept_ids": best["kept_ids"] if not skip_negative else [],
    }
    return written, decision


# 🔸 Вставка строк сетки (oracle_pack_backtest_grid)
async def _insert_grid_rows(
    conn,
    run_id: int,
    strategy_id: int,
    report_id: int,
    timeframe: str,
    direction: str,
    pack_base: str,
    agg_type: str,
    agg_key: str,
    rows: List[Dict],
) -> int:
    data = [
        (
            int(run_id),
            int(strategy_id),
            int(report_id),
            str(timeframe),
            str(direction),
            str(pack_base),
            str(agg_type),
            str(agg_key),
            int(r["step_rank"]),
            float(r["cutoff_share"]),
            int(r["kept_states_count"]),
            int(r["kept_trades"]),
            float(r["kept_mass_share"]),
            float(r["pnl_kept"]),
            float(r["roi"]),
            float(r["roi_delta"]),
            bool(r["is_winner"]),
            bool(r["is_fallback"]),
            bool(r["skip_negative"]),
        )
        for r in rows
    ]
    if not data:
        return 0

    await conn.executemany(
        """
        INSERT INTO oracle_pack_backtest_grid (
            run_id, strategy_id, report_id, timeframe, direction, pack_base, agg_type, agg_key,
            step_rank, cutoff_share,
            kept_states_count, kept_trades, kept_mass_share, pnl_kept, roi, roi_delta_vs_base,
            is_winner, is_fallback, skip_negative, created_at
        ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,
            $9,$10,
            $11,$12,$13,$14,$15,$16,
            $17,$18,$19, now()
        )
        """,
        data
    )
    return len(rows)


# 🔸 Публикация WL/BL v4 по выбранному набору состояний (по winrate)
async def _publish_v4_lists_for_decision(
    conn,
    strategy_id: int,
    timeframe: str,
    direction: str,
    pack_base: str,
    agg_type: str,
    agg_key: str,
    kept_ids: set,
) -> Tuple[int, int]:
    if not kept_ids:
        return 0, 0

    rows = await conn.fetch(
        """
        SELECT
          id            AS aggregated_id,
          strategy_id   AS strategy_id,
          direction     AS direction,
          timeframe     AS timeframe,
          pack_base     AS pack_base,
          agg_type      AS agg_type,
          agg_key       AS agg_key,
          agg_value     AS agg_value,
          winrate       AS winrate,
          confidence    AS confidence
        FROM oracle_pack_aggregated_stat
        WHERE id = ANY($1::bigint[])
        """,
        list(kept_ids)
    )
    if not rows:
        return 0, 0

    async with conn.transaction():
        # чистим прежний v4 по этой оси
        await conn.execute(
            """
            DELETE FROM oracle_pack_whitelist
             WHERE strategy_id = $1
               AND timeframe   = $2
               AND direction   = $3
               AND pack_base   = $4
               AND agg_type    = $5
               AND agg_key     = $6
               AND version     = 'v4'
            """,
            int(strategy_id), str(timeframe), str(direction), str(pack_base), str(agg_type), str(agg_key)
        )

        wl_batch, bl_batch = [], []
        for r in rows:
            wr = float(r["winrate"] or 0.0)
            rec = (
                int(r["aggregated_id"]),
                int(r["strategy_id"]),
                str(r["direction"]),
                str(r["timeframe"]),
                str(r["pack_base"]),
                str(r["agg_type"]),
                str(r["agg_key"]),
                str(r["agg_value"]),
                float(round(wr, 4)),
                float(round(float(r["confidence"] or 0.0), 4)),
            )
            if wr > WL_WR_MIN:
                wl_batch.append(rec)
            elif wr < BL_WR_MAX:
                bl_batch.append(rec)

        if wl_batch:
            await conn.executemany(
                """
                INSERT INTO oracle_pack_whitelist (
                    aggregated_id, strategy_id, direction, timeframe,
                    pack_base, agg_type, agg_key, agg_value,
                    winrate, confidence, version, list
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'v4','whitelist'
                )
                """,
                wl_batch
            )
        if bl_batch:
            await conn.executemany(
                """
                INSERT INTO oracle_pack_whitelist (
                    aggregated_id, strategy_id, direction, timeframe,
                    pack_base, agg_type, agg_key, agg_value,
                    winrate, confidence, version, list
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'v4','blacklist'
                )
                """,
                bl_batch
            )

    return (len(wl_batch), len(bl_batch))