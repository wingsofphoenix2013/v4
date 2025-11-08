# oracle_mw_backtest_v4.py — воркер v4-бэктеста: кривая ROI(t) по agg_state, выбор порога, публикация WL/BL v4 + глобальная очистка старых записей

# 🔸 Импорты
import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List, Tuple

import infra

# 🔸 Логгер
log = logging.getLogger("ORACLE_MW_BACKTEST")

# 🔸 Константы стрима (триггер — готов отчёт MW)
REPORT_STREAM = "oracle:mw:reports_ready"
CONSUMER_GROUP = "oracle_backtest_group"
CONSUMER_NAME = "oracle_backtest_worker"

# 🔸 Стрим для уведомления о готовности WL/BL (как у v1/v2/v3)
WHITELIST_READY_STREAM = "oracle:mw_whitelist:reports_ready"
WHITELIST_READY_MAXLEN = 10_000

# 🔸 Общие настройки
TF_LIST = ("m5", "m15", "h1")
DIRECTIONS = ("long", "short")

# 🔸 Допустимые базы (совместимы с CHECK таблиц)
AGG_BASES = (
    "trend",
    "trend_volatility",
    "trend_extremes",
    "trend_momentum",
    "trend_mom_align",
    "trend_volatility_extremes",
    "trend_volatility_momentum",
    "trend_extremes_momentum",
    "trend_volatility_mom_align",
    "trend_volatility_extremes_momentum",
)

# 🔸 Порог для дефолтного шага (как в v2)
FALLBACK_SHARE = 0.02

# 🔸 Критерии WL/BL на финальном наборе (после выбора порога)
WL_WR_MIN = 0.65
BL_WR_MAX = 0.55


# 🔸 Публичная точка входа воркера (поднимается из oracle_v4_main.py)
async def run_oracle_mw_backtest():
    # условия достаточности окружения
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск: PG/Redis не инициализированы")
        return

    # создание группы потребителей (идемпотентно)
    try:
        await infra.redis_client.xgroup_create(
            name=REPORT_STREAM, groupname=CONSUMER_GROUP, id="$", mkstream=True
        )
        log.debug("📡 Создана группа потребителей в Redis Stream: %s", CONSUMER_GROUP)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            log.exception("❌ Ошибка инициализации группы Redis Stream")
            return

    log.debug("🚀 Старт воркера backtest v4")

    # основной цикл
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=CONSUMER_NAME,
                streams={REPORT_STREAM: ">"},
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

                        # обрабатываем ТОЛЬКО 7d
                        if not (report_id and strategy_id and time_frame == "7d" and window_end):
                            await infra.redis_client.xack(REPORT_STREAM, CONSUMER_GROUP, msg_id)
                            continue

                        await _process_report_7d(report_id, strategy_id, window_end)
                        await infra.redis_client.xack(REPORT_STREAM, CONSUMER_GROUP, msg_id)
                    except Exception:
                        log.exception("❌ Ошибка обработки сообщения в backtest v4")
        except asyncio.CancelledError:
            log.debug("⏹️ Воркер backtest v4 остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка цикла backtest v4 — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Обработка одного 7d-отчёта: по каждому TF считаем кривые v4, публикуем WL/BL и отправляем событие готовности
async def _process_report_7d(report_id: int, strategy_id: int, window_end_iso: str):
    # парсинг window_end (для логов/метаданных)
    try:
        window_end_dt = datetime.fromisoformat(str(window_end_iso).replace("Z", ""))
    except Exception:
        log.exception("❌ Неверный формат window_end: %r", window_end_iso)
        return

    async with infra.pg_pool.acquire() as conn:
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

        # глобальная очистка: удалить ВСЕ v4 записи WL/BL по стратегии, ссылающиеся на старые report_id
        res = await conn.execute(
            """
            DELETE FROM oracle_mw_whitelist w
            USING oracle_mw_aggregated_stat a
            WHERE w.version = 'v4'
              AND w.strategy_id = $1
              AND w.aggregated_id = a.id
              AND a.report_id <> $2
            """,
            int(strategy_id), int(report_id)
        )
        log.debug("🧹 Очистка v4 по sid=%s rep=%s: %s", strategy_id, report_id, res)

        # депозит стратегии (ROI считается по нему)
        deposit = await conn.fetchval("SELECT deposit FROM strategies_v4 WHERE id = $1", int(strategy_id))
        if deposit is None:
            log.debug("ℹ️ Пропуск sid=%s: депозит отсутствует (после очистки)", strategy_id)
            return
        deposit = float(deposit)

        method = "v4"

        # собираем все строки агрегатов отчёта (за один проход)
        rows = await conn.fetch(
            """
            SELECT
              id, direction, timeframe, agg_base, agg_state,
              trades_total, pnl_sum_total, winrate, confidence
            FROM oracle_mw_aggregated_stat
            WHERE report_id = $1
              AND agg_base = ANY($2::text[])
              AND timeframe = ANY($3::text[])
              AND direction = ANY($4::text[])
            """,
            int(report_id), list(AGG_BASES), list(TF_LIST), list(DIRECTIONS)
        )
        if not rows:
            log.debug("ℹ️ Нет агрегатов для report_id=%s (после очистки)", report_id)
            return

        # группируем по ключу (tf, dir, base)
        by_key: Dict[Tuple[str, str, str], List[dict]] = {}
        for r in rows:
            key = (str(r["timeframe"]), str(r["direction"]), str(r["agg_base"]))
            by_key.setdefault(key, []).append(dict(r))

        # для единого события после всех TF накапливаем суммы вставленных строк
        total_wl_inserted = 0
        total_bl_inserted = 0

        # проход по каждому TF — один run на TF
        for tf in TF_LIST:
            # идемпотентность: по одному логу на TF/метод
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

            # создаём лог
            run_id = await _create_run_log(conn, strategy_id, report_id, tf, method, deposit, window_end_dt)

            # агрегируем ключи этой TF
            keys_tf = [(tf, d, b) for (t, d, b) in by_key.keys() if t == tf]
            summary_total = 0
            summary_improved = 0
            summary_fallback = 0
            summary_skipped = 0
            grid_rows_written = 0
            wl_written = 0
            bl_written = 0

            # по каждому (direction, agg_base) строим кривую и принимаем решение
            for _tf, direction, agg_base in keys_tf:
                summary_total += 1
                agg_rows = by_key.get((_tf, direction, agg_base), [])
                if not agg_rows:
                    continue

                # кривая по эмпирическим порогам
                written, decision = await _build_curve_and_decide(
                    conn=conn,
                    run_id=run_id,
                    strategy_id=strategy_id,
                    report_id=report_id,
                    timeframe=tf,
                    direction=direction,
                    agg_base=agg_base,
                    deposit=deposit,
                    states=agg_rows,
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

                w_wl, w_bl = await _publish_v4_lists_for_decision(
                    conn=conn,
                    strategy_id=strategy_id,
                    timeframe=tf,
                    direction=direction,
                    agg_base=agg_base,
                    kept_ids=set(decision["kept_ids"]),
                )
                wl_written += w_wl
                bl_written += w_bl

            # финализируем лог
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
                "✅ backtest v4 готов: sid=%s rep=%s tf=%s total=%d improved=%d fallback=%d skipped=%d wl=%d bl=%d",
                strategy_id, report_id, tf, summary_total, summary_improved, summary_fallback, summary_skipped, wl_written, bl_written
            )

            # аккумулируем итог по TF для общего события
            total_wl_inserted += wl_written
            total_bl_inserted += bl_written

        # отправляем ОДНО уведомление в стрим (как v1/v2/v3) после всех TF
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
                name=WHITELIST_READY_STREAM,
                fields={"data": json.dumps(payload, separators=(",", ":"))},
                maxlen=WHITELIST_READY_MAXLEN,
                approximate=True,
            )
            log.debug("[WL_READY v4] sid=%s rep=%s rows=%d", strategy_id, report_id, payload["rows_inserted"])
        except Exception:
            log.exception("❌ Ошибка публикации события в %s (v4)", WHITELIST_READY_STREAM)


# 🔸 Создание записи лога прогона
async def _create_run_log(conn, strategy_id: int, report_id: int, timeframe: str, method: str, deposit: float, window_end_dt: datetime) -> int:
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
        int(strategy_id), int(report_id), str(timeframe), str(method), float(deposit), window_end_dt.isoformat()
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
               wl_rows_written_v4       = $8,
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


# 🔸 Построение кривой по ключу и выбор решения
async def _build_curve_and_decide(
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
    # базовые величины по ключу
    total_trades = sum(int(s["trades_total"] or 0) for s in states)
    pnl_base = sum(float(s["pnl_sum_total"] or 0.0) for s in states)
    roi_base = float(pnl_base) / float(deposit) if deposit else 0.0

    # подготовка шага 0 (без фильтра) и сортировка по массе
    items = []
    for s in states:
        n = int(s["trades_total"] or 0)
        pnl = float(s["pnl_sum_total"] or 0.0)
        share = (float(n) / float(total_trades)) if total_trades > 0 else 0.0
        items.append({
            "id": int(s["id"]),
            "n": n,
            "pnl": pnl,
            "winrate": float(s["winrate"] or 0.0),
            "confidence": float(s["confidence"] or 0.0),
            "share": share,
        })
    items.sort(key=lambda x: (x["n"], x["id"]))  # устойчивый порядок «от редких к частым»

    # шаг 0 — без фильтра
    grid_rows = []
    kept_ids_all = [it["id"] for it in items]
    grid_rows.append({
        "step_rank": 0,
        "cutoff_share": 0.0,
        "kept_states_count": len(items),
        "kept_trades": total_trades,
        "kept_mass_share": 1.0 if total_trades > 0 else 0.0,
        "pnl_kept": pnl_base,
        "roi": roi_base,
        "roi_delta": 0.0,
        "kept_ids": kept_ids_all,
        "is_winner": False,
        "is_fallback": False,
        "skip_negative": False,
    })

    # функция для фиксации шага по порогу (отбрасываем все с share <= t)
    def build_step_for_cut(t: float):
        kept = [it for it in items if it["share"] > t]
        kept_ids = [it["id"] for it in kept]
        kept_trades = sum(it["n"] for it in kept)
        kept_pnl = sum(it["pnl"] for it in kept)
        roi = (kept_pnl / deposit) if deposit else 0.0
        roi_delta = roi - roi_base
        return {
            "cutoff_share": float(round(t, 8)),
            "kept_states_count": len(kept),
            "kept_trades": int(kept_trades),
            "kept_mass_share": float(round((kept_trades / total_trades) if total_trades else 0.0, 8)),
            "pnl_kept": float(round(kept_pnl, 4)),
            "roi": float(round(roi, 6)),
            "roi_delta": float(round(roi_delta, 6)),
            "kept_ids": kept_ids,
            "is_winner": False,
            "is_fallback": False,
            "skip_negative": False,
        }

    # последовательные пороги — уникальные share самых редких (n по возрастанию)
    unique_cuts = []
    seen = set()
    for it in items:
        t = it["share"]
        if t not in seen:
            seen.add(t)
            unique_cuts.append(t)
    # для каждого порога добавляем шаг (отбрасываем все с share <= t)
    rank = 1
    for t in unique_cuts:
        step = build_step_for_cut(t)
        # пропускаем дубликаты, где состав не меняется (например, при t=0.0 это будет базовый)
        if not step["kept_ids"]:
            # если никого не осталось — всё равно фиксируем финальную точку
            grid_rows.append({
                **step,
                "step_rank": rank,
            })
            rank += 1
            break
        # если состав совпал с предыдущим шагом — пропускаем
        if grid_rows and set(step["kept_ids"]) == set(grid_rows[-1]["kept_ids"]):
            continue
        grid_rows.append({
            **step,
            "step_rank": rank,
        })
        rank += 1

    # выбор лучшего шага (макс. ROI; tie-break — минимальный cutoff_share)
    best = max(grid_rows, key=lambda r: (r["roi"], -r["cutoff_share"]))
    improved = best["roi"] > roi_base

    # fallback 2% — если улучшения нет
    used_fallback = False
    if not improved:
        fb = build_step_for_cut(FALLBACK_SHARE)
        fb["step_rank"] = rank
        fb["is_fallback"] = True
        grid_rows.append(fb)
        best = fb
        used_fallback = True

    # стоп-правило: если итоговый ROI < 0 — публикации не делаем
    skip_negative = best["roi"] < 0.0
    best["is_winner"] = True
    best["skip_negative"] = skip_negative

    # запись сетки в БД
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

    # возвращаем решение по ключу
    decision = {
        "is_fallback": used_fallback,
        "skip_negative": skip_negative,
        "kept_ids": best["kept_ids"] if not skip_negative else [],
    }
    return written, decision


# 🔸 Вставка строк сетки в oracle_mw_backtest_grid
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
    # подготовка пачки
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


# 🔸 Публикация WL/BL v4 для выбранного набора (по winrate)
async def _publish_v4_lists_for_decision(
    conn,
    strategy_id: int,
    timeframe: str,
    direction: str,
    agg_base: str,
    kept_ids: set,
) -> Tuple[int, int]:
    # читаем выбранные строки агрегатов
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

    # очищаем старые v4 по этому срезу
    async with conn.transaction():
        await conn.execute(
            """
            DELETE FROM oracle_mw_whitelist
             WHERE strategy_id = $1
               AND timeframe = $2
               AND direction = $3
               AND agg_base = $4
               AND version = 'v4'
            """,
            int(strategy_id), str(timeframe), str(direction), str(agg_base)
        )

        # формируем батчи WL/BL
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

        # вставка WL
        if wl_batch:
            await conn.executemany(
                """
                INSERT INTO oracle_mw_whitelist (
                    aggregated_id, strategy_id, direction, timeframe,
                    agg_base, agg_state, winrate, confidence, version, list
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,'v4','whitelist'
                )
                """,
                wl_batch
            )

        # вставка BL
        if bl_batch:
            await conn.executemany(
                """
                INSERT INTO oracle_mw_whitelist (
                    aggregated_id, strategy_id, direction, timeframe,
                    agg_base, agg_state, winrate, confidence, version, list
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,'v4','blacklist'
                )
                """,
                bl_batch
            )

    return (len(wl_batch), len(bl_batch))