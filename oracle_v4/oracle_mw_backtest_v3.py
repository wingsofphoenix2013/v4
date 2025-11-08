# oracle_mw_backtest_v3.py — воркер v3-бэктеста: ROI по порогу confidence (гейт sense>0.5), публикация WL/BL v3 + глобальная очистка старых записей

# 🔸 Импорты
import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List, Tuple

import infra

# 🔸 Логгер
log = logging.getLogger("ORACLE_MW_BACKTEST_V3")

# 🔸 Константы стрима (триггер — готов whitelist v1, значит confidence+sense уже посчитаны)
WHITELIST_READY_STREAM = "oracle:mw_whitelist:reports_ready"
WHITELIST_READY_MAXLEN = 10_000
CONSUMER_GROUP = "oracle_backtest_v3_group"
CONSUMER_NAME = "oracle_backtest_v3_worker"

# 🔸 Общие настройки
TF_LIST = ("m5", "m15", "h1")
DIRECTIONS = ("long", "short")

# 🔸 Порог по confidence (база и fallback)
CONF_BASE_MIN = 0.20   # базовый набор: учитываем только строки с conf >= 0.20
CONF_FALLBACK = 0.50   # дефолтный порог при отсутствии улучшения

# 🔸 Гейт по sense на уровне agg_base
SENSE_SCORE_MIN = 0.50

# 🔸 Критерии WL/BL для публикации (после выбора порога)
WL_WR_MIN = 0.65
BL_WR_MAX = 0.55


# 🔸 Публичная точка входа воркера
async def run_oracle_mw_backtest_v3():
    # условия достаточности окружения
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск: PG/Redis не инициализированы")
        return

    # создание группы потребителей (идемпотентно)
    try:
        await infra.redis_client.xgroup_create(
            name=WHITELIST_READY_STREAM, groupname=CONSUMER_GROUP, id="$", mkstream=True
        )
        log.debug("📡 Создана группа потребителей в Redis Stream: %s", CONSUMER_GROUP)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            log.exception("❌ Ошибка инициализации группы Redis Stream")
            return

    log.debug("🚀 Старт воркера backtest v3")

    # основной цикл
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=CONSUMER_NAME,
                streams={WHITELIST_READY_STREAM: ">"},
                count=64,
                block=30_000,
            )
            if not resp:
                continue

            for _stream_name, msgs in resp:
                for msg_id, fields in msgs:
                    try:
                        payload = json.loads(fields.get("data", "{}"))
                        # ожидаем payload от oracle_mw_sense_stat (v1/v2), нас интересует v1/7d
                        version = str(payload.get("version") or "")
                        time_frame = str(payload.get("time_frame") or "")
                        report_id = int(payload.get("report_id", 0))
                        strategy_id = int(payload.get("strategy_id", 0))

                        if not (version == "v1" and time_frame == "7d" and report_id and strategy_id):
                            await infra.redis_client.xack(WHITELIST_READY_STREAM, CONSUMER_GROUP, msg_id)
                            continue

                        await _process_report_v3(report_id, strategy_id, msg_id)
                        await infra.redis_client.xack(WHITELIST_READY_STREAM, CONSUMER_GROUP, msg_id)
                    except Exception:
                        log.exception("❌ Ошибка обработки сообщения в backtest v3")
        except asyncio.CancelledError:
            log.debug("⏹️ Воркер backtest v3 остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка цикла backtest v3 — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Обработка одного 7d-отчёта по всем TF (v3) с глобальной очисткой старых v3-записей
async def _process_report_v3(report_id: int, strategy_id: int, stream_msg_id: str):
    async with infra.pg_pool.acquire() as conn:
        # депозит стратегии
        deposit = await conn.fetchval("SELECT deposit FROM strategies_v4 WHERE id = $1", int(strategy_id))
        if deposit is None:
            log.debug("ℹ️ Пропуск sid=%s: депозит отсутствует", strategy_id)
            return
        deposit = float(deposit)

        # ранний гард: обрабатываем только самый свежий 7d-отчёт стратегии
        latest_id = await conn.fetchval(
            """
            SELECT id
            FROM oracle_report_stat
            WHERE strategy_id = $1 AND time_frame = '7d' AND source = 'mw'
            ORDER BY window_end DESC, created_at DESC
            LIMIT 1
            """,
            int(strategy_id)
        )
        if latest_id is None:
            log.debug("ℹ️ Пропуск sid=%s: нет записей oracle_report_stat для 7d", strategy_id)
            return
        if int(latest_id) != int(report_id):
            log.debug("⏭️ Пропуск sid=%s rep=%s: не последний отчёт (latest=%s)", strategy_id, report_id, latest_id)
            return

        # глобальная очистка: удалить ВСЕ v3 записи WL/BL по стратегии, ссылающиеся на старые report_id
        res = await conn.execute(
            """
            DELETE FROM oracle_mw_whitelist w
            USING oracle_mw_aggregated_stat a
            WHERE w.version = 'v3'
              AND w.strategy_id = $1
              AND w.aggregated_id = a.id
              AND a.report_id <> $2
            """,
            int(strategy_id), int(report_id)
        )
        log.debug("🧹 Очистка v3 по sid=%s rep=%s: %s", strategy_id, report_id, res)

        method = "v3"

        # гейт по sense: набор ключей (tf, direction, agg_base), где score_smoothed > 0.5 для данного report_id
        rows_sense = await conn.fetch(
            """
            SELECT timeframe, direction, agg_base
              FROM oracle_mw_sense_stat
             WHERE report_id = $1
               AND time_frame = '7d'
               AND score_smoothed > $2
            """,
            int(report_id), float(SENSE_SCORE_MIN)
        )
        sense_keys = {(str(r["timeframe"]), str(r["direction"]), str(r["agg_base"])) for r in rows_sense}
        if not sense_keys:
            log.debug("ℹ️ Нет баз с sense>%.2f для report_id=%s — v3 расчёт пропущен (после очистки)", SENSE_SCORE_MIN, report_id)
            return

        # поднабор агрегатов по conf >= 0.20 и только по ключам из sense
        rows = await conn.fetch(
            """
            SELECT
              id, direction, timeframe, agg_base, agg_state,
              trades_total, pnl_sum_total, winrate, confidence
            FROM oracle_mw_aggregated_stat
            WHERE report_id = $1
              AND confidence >= $2
            """,
            int(report_id), float(CONF_BASE_MIN)
        )
        if not rows:
            log.debug("ℹ️ Нет агрегатов с confidence>=%.2f для report_id=%s (после очистки)", CONF_BASE_MIN, report_id)
            return

        # группируем по ключу и фильтруем sense
        by_key: Dict[Tuple[str, str, str], List[dict]] = {}
        for r in rows:
            key = (str(r["timeframe"]), str(r["direction"]), str(r["agg_base"]))
            if key not in sense_keys:
                continue
            by_key.setdefault(key, []).append(dict(r))

        # подсчёт итоговых вставок (для единого события v3 по всему report_id)
        total_wl_inserted = 0
        total_bl_inserted = 0

        # по каждому TF — отдельный run
        for tf in TF_LIST:
            # если для TF нет ключей — пропускаем
            keys_tf = [(tf, d, b) for (t, d, b) in by_key.keys() if t == tf]
            if not keys_tf:
                continue

            # идемпотентность: один run на (sid, report_id, tf, v3)
            exists = await conn.fetchval(
                """
                SELECT 1 FROM oracle_mw_backtest_log
                 WHERE strategy_id = $1 AND report_id = $2 AND timeframe = $3 AND method = $4
                """,
                int(strategy_id), int(report_id), tf, method
            )
            if exists:
                log.debug("⏭️ Пропуск: уже есть backtest_log (sid=%s rep=%s tf=%s method=%s)", strategy_id, report_id, tf, method)
                continue

            run_id = await _create_run_log(conn, strategy_id, report_id, tf, method, deposit, stream_msg_id)

            summary_total = 0
            summary_improved = 0
            summary_fallback = 0
            summary_skipped = 0
            grid_rows_written = 0
            wl_written = 0
            bl_written = 0

            for _tf, direction, agg_base in keys_tf:
                states = by_key.get((_tf, direction, agg_base), [])
                if not states:
                    continue
                summary_total += 1

                # кривая по порогам confidence (>=0.2)
                written, decision = await _build_curve_and_decide_v3(
                    conn=conn,
                    run_id=run_id,
                    strategy_id=strategy_id,
                    report_id=report_id,
                    timeframe=tf,
                    direction=direction,
                    agg_base=agg_base,
                    deposit=deposit,
                    states=states,
                )
                grid_rows_written += written

                # публикация списков по решению
                if decision["skip_negative"]:
                    summary_skipped += 1
                    continue

                if decision["is_fallback"]:
                    summary_fallback += 1
                else:
                    summary_improved += 1

                w_wl, w_bl = await _publish_v3_lists_for_decision(
                    conn=conn,
                    strategy_id=strategy_id,
                    timeframe=tf,
                    direction=direction,
                    agg_base=agg_base,
                    kept_ids=set(decision["kept_ids"]),
                )
                wl_written += w_wl
                bl_written += w_bl

            # аккумулируем итог по TF
            total_wl_inserted += wl_written
            total_bl_inserted += bl_written

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
                "✅ backtest v3 готов: sid=%s rep=%s tf=%s total=%d improved=%d fallback=%d skipped=%d wl=%d bl=%d",
                strategy_id, report_id, tf, summary_total, summary_improved, summary_fallback, summary_skipped, wl_written, bl_written
            )

        # после обработки ВСЕХ TF публикуем событие, как в v1/v2 (одна запись на report_id)
        try:
            window_end_dt = await conn.fetchval(
                "SELECT window_end FROM oracle_report_stat WHERE id = $1",
                int(report_id)
            )
            payload = {
                "strategy_id": int(strategy_id),
                "report_id": int(report_id),
                "time_frame": "7d",
                "version": "v3",
                "window_end": (window_end_dt.isoformat() if hasattr(window_end_dt, 'isoformat') else str(window_end_dt)),
                "rows_inserted": int(total_wl_inserted + total_bl_inserted),
                "generated_at": datetime.utcnow().replace(tzinfo=None).isoformat(),
            }
            await infra.redis_client.xadd(
                name=WHITELIST_READY_STREAM,
                fields={"data": json.dumps(payload, separators=(",", ":"))},
                maxlen=WHITELIST_READY_MAXLEN,
                approximate=True,
            )
            log.debug("[WL_READY v3] sid=%s rep=%s rows=%d", strategy_id, report_id, payload["rows_inserted"])
        except Exception:
            log.exception("❌ Ошибка публикации события в %s (v3)", WHITELIST_READY_STREAM)


# 🔸 Создание записи лога прогона
async def _create_run_log(conn, strategy_id: int, report_id: int, timeframe: str, method: str, deposit: float, stream_msg_id: str) -> int:
    row = await conn.fetchrow(
        """
        INSERT INTO oracle_mw_backtest_log (
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


# 🔸 Финализация лога прогона
async def _finalize_run_log(conn, run_id: int, status: str, summary: Dict):
    await conn.execute(
        """
        UPDATE oracle_mw_backtest_log
           SET status = $2,
               finished_at = now(),
               summary_keys_total       = $3,
               summary_improved         = $4,
               summary_fallback         = $5,
               summary_skipped_negative = $6,
               grid_rows_written        = $7,
               wl_rows_written_v4       = $8,  -- поле общее; для v3 используем те же колонки
               bl_rows_written_v4       = $9,
               updated_at = now()
         WHERE id = $1
        """,
        int(run_id),
        str(status),
        int(summary.get("total", 0)),
        int(summary.get("improved", 0)),
        int(summary.get("fallback", 0)),
        int(summary.get("skipped", 0)),
        int(summary.get("grid_rows", 0)),
        int(summary.get("wl_rows", 0)),
        int(summary.get("bl_rows", 0)),
    )


# 🔸 Построение кривой (confidence-тест) и выбор решения
async def _build_curve_and_decide_v3(
    conn,
    run_id: int,
    strategy_id: int,
    report_id: int,
    timeframe: str,
    direction: str,
    agg_base: str,
    deposit: float,
    states: List[dict],
) -> Tuple[int, Dict]:
    # базовый набор: только строки с conf >= 0.20 (states уже фильтрованы в запросе)
    total_trades = sum(int(s["trades_total"] or 0) for s in states)
    pnl_base = sum(float(s["pnl_sum_total"] or 0.0) for s in states)
    roi_base = float(pnl_base) / float(deposit) if deposit else 0.0

    # подготовим элементы
    items = []
    for s in states:
        n = int(s["trades_total"] or 0)
        pnl = float(s["pnl_sum_total"] or 0.0)
        conf = float(s["confidence"] or 0.0)
        items.append({
            "id": int(s["id"]),
            "n": n,
            "pnl": pnl,
            "conf": conf,
        })
    # сортируем по conf возрастанию (для стабильного прохода порогов)
    items.sort(key=lambda x: (x["conf"], x["id"]))

    # базовый шаг: cutoff_share трактуем как порог conf базового набора (0.20)
    grid_rows = []
    kept_ids_all = [it["id"] for it in items]
    grid_rows.append({
        "step_rank": 0,
        "cutoff_share": float(round(CONF_BASE_MIN, 8)),
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

    # функция шага: оставляем строки с conf >= t
    def build_step_for_conf(t: float):
        kept = [it for it in items if it["conf"] >= t]
        kept_ids = [it["id"] for it in kept]
        kept_trades = sum(it["n"] for it in kept)
        kept_pnl = sum(it["pnl"] for it in kept)
        roi = (kept_pnl / deposit) if deposit else 0.0
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

    # набор порогов — уникальные conf из items (они уже >= 0.20), по возрастанию
    unique_cuts = sorted({it["conf"] for it in items})
    rank = 1
    for t in unique_cuts:
        # базовый порог 0.20 уже отражён шагом 0 — начинаем со следующего уникального
        if t <= CONF_BASE_MIN:
            continue
        step = build_step_for_conf(t)
        if not step["kept_ids"]:
            grid_rows.append({**step, "step_rank": rank})
            rank += 1
            break
        if set(step["kept_ids"]) == set(grid_rows[-1]["kept_ids"]):
            continue
        grid_rows.append({**step, "step_rank": rank})
        rank += 1

    # выбор лучшего шага (макс ROI; при равенстве — минимальный порог)
    best = max(grid_rows, key=lambda r: (r["roi"], -r["cutoff_share"]))
    improved = best["roi"] > roi_base

    # fallback к 0.5 при отсутствии улучшения
    used_fallback = False
    if not improved:
        fb = build_step_for_conf(CONF_FALLBACK)
        fb["step_rank"] = rank
        fb["is_fallback"] = True
        grid_rows.append(fb)
        best = fb
        used_fallback = True

    # стоп-правило: итоговый ROI < 0 → публикаций нет
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
        agg_base=agg_base,
        rows=grid_rows,
    )

    decision = {
        "is_fallback": used_fallback,
        "skip_negative": skip_negative,
        "kept_ids": best["kept_ids"] if not skip_negative else [],
    }
    return written, decision


# 🔸 Вставка строк сетки (общая для v3)
async def _insert_grid_rows(
    conn,
    run_id: int,
    strategy_id: int,
    report_id: int,
    timeframe: str,
    direction: str,
    agg_base: str,
    rows: List[Dict],
) -> int:
    data = [
        (
            int(run_id),
            int(strategy_id),
            int(report_id),
            str(timeframe),
            str(direction),
            str(agg_base),
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
        INSERT INTO oracle_mw_backtest_grid (
            run_id, strategy_id, report_id, timeframe, direction, agg_base,
            step_rank, cutoff_share,
            kept_states_count, kept_trades, kept_mass_share, pnl_kept, roi, roi_delta_vs_base,
            is_winner, is_fallback, skip_negative, created_at
        ) VALUES (
            $1,$2,$3,$4,$5,$6,
            $7,$8,
            $9,$10,$11,$12,$13,$14,
            $15,$16,$17, now()
        )
        """,
        data
    )
    return len(rows)


# 🔸 Публикация WL/BL v3 для выбранного набора (по winrate)
async def _publish_v3_lists_for_decision(
    conn,
    strategy_id: int,
    timeframe: str,
    direction: str,
    agg_base: str,
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
          agg_base      AS agg_base,
          agg_state     AS agg_state,
          winrate       AS winrate,
          confidence    AS confidence
        FROM oracle_mw_aggregated_stat
        WHERE id = ANY($1::bigint[])
        """,
        list(kept_ids)
    )
    if not rows:
        return 0, 0

    async with conn.transaction():
        # чистим старые v3 по срезу
        await conn.execute(
            """
            DELETE FROM oracle_mw_whitelist
             WHERE strategy_id = $1
               AND timeframe = $2
               AND direction = $3
               AND agg_base = $4
               AND version = 'v3'
            """,
            int(strategy_id), str(timeframe), str(direction), str(agg_base)
        )

        wl_batch, bl_batch = [], []
        for r in rows:
            wr = float(r["winrate"] or 0.0)
            rec = (
                int(r["aggregated_id"]),
                int(r["strategy_id"]),
                str(r["direction"]),
                str(r["timeframe"]),
                str(r["agg_base"]),
                str(r["agg_state"]),
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
                INSERT INTO oracle_mw_whitelist (
                    aggregated_id, strategy_id, direction, timeframe,
                    agg_base, agg_state, winrate, confidence, version, list
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,'v3','whitelist'
                )
                """,
                wl_batch
            )
        if bl_batch:
            await conn.executemany(
                """
                INSERT INTO oracle_mw_whitelist (
                    aggregated_id, strategy_id, direction, timeframe,
                    agg_base, agg_state, winrate, confidence, version, list
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,'v3','blacklist'
                )
                """,
                bl_batch
            )

    return (len(wl_batch), len(bl_batch))