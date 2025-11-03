# oracle_mw_backtest.py — воркер MW-backtest: подбор порогов (winrate/confidence) по 7d-отчёту на сетке фактических значений, запись поверхности (grid) и победителей (winner)

# 🔸 Импорты
import asyncio
import logging
from typing import Dict, List, Tuple, Iterable
from datetime import datetime

import infra

# 🔸 Логгер
log = logging.getLogger("ORACLE_MW_BACKTEST")

# 🔸 Константы стрима-триггера (берём готовность отчётов для SENSE → confidence уже посчитан)
SENSE_REPORT_READY_STREAM = "oracle:mw_sense:reports_ready"
BT_CONSUMER_GROUP = "oracle_mw_backtest_group"
BT_CONSUMER_NAME  = "oracle_mw_backtest_worker"

# 🔸 Параллелизм обработки окон (по стратегиям/окнам)
MAX_CONCURRENT_RUNS = 2

# 🔸 Размер батча для вставки grid
GRID_INSERT_BATCH = 1000


# 🔸 Публичная точка входа воркера
async def run_oracle_mw_backtest():
    # условия достаточности окружения
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск BACKTEST: PG/Redis не инициализированы")
        return

    # создаём consumer-group (идемпотентно)
    try:
        await infra.redis_client.xgroup_create(
            name=SENSE_REPORT_READY_STREAM,
            groupname=BT_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug("📡 Создана группа потребителей в Redis Stream: %s", BT_CONSUMER_GROUP)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.exception("❌ Ошибка инициализации группы Redis Stream для BACKTEST")
            return

    sem = asyncio.Semaphore(MAX_CONCURRENT_RUNS)
    log.debug("🚀 Старт воркера MW-backtest (max_parallel_runs=%d)", MAX_CONCURRENT_RUNS)

    # основной цикл
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=BT_CONSUMER_GROUP,
                consumername=BT_CONSUMER_NAME,
                streams={SENSE_REPORT_READY_STREAM: ">"},
                count=64,
                block=30_000,
            )
            if not resp:
                continue

            tasks = []
            to_ack: List[Tuple[str, str]] = []  # (stream, msg_id)
            for stream_name, msgs in resp:
                for msg_id, fields in msgs:
                    to_ack.append((stream_name, msg_id))
                    try:
                        payload = _safe_load_json(fields.get("data"))
                        # берем только 7d
                        tf = str(payload.get("time_frame", "")).strip()
                        if tf != "7d":
                            continue
                        strategy_id = int(payload.get("strategy_id", 0))
                        report_id   = int(payload.get("report_id", 0))
                        window_end  = payload.get("window_end")
                        if not (strategy_id and report_id and window_end):
                            continue
                        # запускаем guarded-задачу
                        tasks.append(asyncio.create_task(
                            _guarded_run(sem, strategy_id, report_id, window_end)
                        ))
                    except Exception:
                        log.exception("❌ Ошибка парсинга сообщения BACKTEST")

            # ждём завершения задач по этому чтению
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=False)

            # ACK сообщений (после успешных задач — они сами не ACKают, ACK тут батчом)
            if to_ack:
                try:
                    await infra.redis_client.xack(
                        SENSE_REPORT_READY_STREAM,
                        BT_CONSUMER_GROUP,
                        *[mid for (_s, mid) in to_ack]
                    )
                except Exception:
                    log.exception("⚠️ Ошибка ACK для BACKTEST")

        except asyncio.CancelledError:
            log.debug("⏹️ Воркер BACKTEST остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка цикла BACKTEST — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Гард: семафор + ловим исключения
async def _guarded_run(sem: asyncio.Semaphore, strategy_id: int, report_id: int, window_end_iso: str):
    async with sem:
        try:
            await _run_for_report(strategy_id, report_id, window_end_iso)
        except Exception:
            log.exception("❌ Сбой BACKTEST sid=%s report_id=%s", strategy_id, report_id)


# 🔸 Основной расчёт для одного отчёта 7d (по стратегии)
async def _run_for_report(strategy_id: int, report_id: int, window_end_iso: str):
    # парсинг времени окна
    try:
        window_end_dt = datetime.fromisoformat(str(window_end_iso).replace("Z", ""))
    except Exception:
        log.exception("❌ Неверный формат window_end: %r", window_end_iso)
        return

    async with infra.pg_pool.acquire() as conn:
        # читаем депозит стратегии
        deposit = await conn.fetchval("SELECT deposit FROM strategies_v4 WHERE id=$1", int(strategy_id))
        try:
            deposit_used = float(deposit) if (deposit is not None and float(deposit) > 0) else 1.0
        except Exception:
            deposit_used = 1.0

        # идемпотентный bt_run (уникален на report_id)
        row = await conn.fetchrow(
            """
            INSERT INTO oracle_mw_bt_run (strategy_id, report_id, time_frame, window_end, deposit_used)
            VALUES ($1,$2,'7d',$3,$4)
            ON CONFLICT (report_id) DO UPDATE
              SET deposit_used = EXCLUDED.deposit_used
            RETURNING id
            """,
            int(strategy_id), int(report_id), window_end_dt, float(deposit_used)
        )
        bt_run_id = int(row["id"])

        # тянем все агрегаты по этому report_id
        rows = await conn.fetch(
            """
            SELECT direction, timeframe, agg_type, agg_base,
                   winrate::float8 AS wr,
                   confidence::float8 AS conf,
                   trades_total::int4 AS n,
                   pnl_sum_total::float8 AS pnl
            FROM oracle_mw_aggregated_stat
            WHERE report_id = $1
            """,
            int(report_id)
        )
        if not rows:
            log.info("ℹ️ BACKTEST: пустые агрегаты report_id=%s sid=%s — пропуск", report_id, strategy_id)
            return

        # группируем по блоку
        blocks: Dict[Tuple[str, str, str, str], List[dict]] = {}
        for r in rows:
            key = (str(r["direction"]), str(r["timeframe"]), str(r["agg_type"]), str(r["agg_base"]))
            blocks.setdefault(key, []).append({
                "wr":   _r4(r["wr"]),
                "conf": _r4(r["conf"]),
                "n":    int(r["n"] or 0),
                "pnl":  _r4f(r["pnl"]),
            })

        # чистим прошлые результаты этого bt_run (пересчёт идемпотентно)
        async with conn.transaction():
            await conn.execute("DELETE FROM oracle_mw_bt_grid   WHERE bt_run_id = $1", bt_run_id)
            await conn.execute("DELETE FROM oracle_mw_bt_winner WHERE bt_run_id = $1", bt_run_id)

        total_blocks = 0
        total_cells  = 0
        winners_written = 0

        # обрабатываем каждый блок
        for (direction, timeframe, agg_type, agg_base), items in blocks.items():
            # baseline по блоку
            base_pnl = sum(x["pnl"] for x in items)
            base_trd = sum(x["n"]   for x in items)
            base_roi = base_pnl / deposit_used if deposit_used != 0 else 0.0

            # дискретная сетка порогов
            w_vals = sorted({_r4(x["wr"]) for x in items} | {0.0})
            c_vals = sorted({_r4(x["conf"]) for x in items} | {0.0})
            wi = {w: i for i, w in enumerate(w_vals)}
            ci = {c: j for j, c in enumerate(c_vals)}
            nw, nc = len(w_vals), len(c_vals)

            # матрицы масс
            P = [[0.0 for _ in range(nc)] for _ in range(nw)]
            T = [[0   for _ in range(nc)] for _ in range(nw)]

            for x in items:
                i = wi[_r4(x["wr"])]
                j = ci[_r4(x["conf"])]
                P[i][j] += x["pnl"]
                T[i][j] += x["n"]

            # суффиксные суммы (u ≥ i, v ≥ j)
            PS = [[0.0 for _ in range(nc)] for _ in range(nw)]
            TS = [[0   for _ in range(nc)] for _ in range(nw)]
            for i in range(nw - 1, -1, -1):
                for j in range(nc - 1, -1, -1):
                    sP = P[i][j]
                    sT = T[i][j]
                    if i + 1 < nw:
                        sP += PS[i + 1][j]
                        sT += TS[i + 1][j]
                    if j + 1 < nc:
                        sP += PS[i][j + 1]
                        sT += TS[i][j + 1]
                    if (i + 1 < nw) and (j + 1 < nc):
                        sP -= PS[i + 1][j + 1]
                        sT -= TS[i + 1][j + 1]
                    PS[i][j] = sP
                    TS[i][j] = sT

            # сбор сетки для вставки + поиск победителя
            grid_rows = []
            best = None  # (roi, trades_kept, conf_min, wr_min, pnl)
            for i, wmin in enumerate(w_vals):
                for j, cmin in enumerate(c_vals):
                    pnl_kept = _r4f(PS[i][j])
                    trd_kept = int(TS[i][j])
                    roi = pnl_kept / deposit_used if deposit_used != 0 else 0.0

                    grid_rows.append((
                        int(bt_run_id), str(direction), str(timeframe), str(agg_type), str(agg_base),
                        _r4(wmin), _r4(cmin),
                        int(trd_kept), _r4f(pnl_kept), _r6f(roi),
                        int(base_trd), _r4f(base_pnl), _r6f(base_roi),
                    ))

                    cand = (roi, trd_kept, cmin, wmin, pnl_kept)
                    if (best is None) or _better(cand, best):
                        best = cand

            # вставка grid батчами
            total_cells += len(grid_rows)
            await _insert_grid_rows(conn, grid_rows)

            # запись победителя
            if best is not None:
                roi, trd_kept, cmin, wmin, pnl_kept = best
                uplift = roi - base_roi
                await conn.execute(
                    """
                    INSERT INTO oracle_mw_bt_winner (
                      bt_run_id, direction, timeframe, agg_type, agg_base,
                      wr_min, conf_min, trades_kept, pnl_sum_total, roi,
                      baseline_trades, baseline_pnl_sum, baseline_roi, uplift_roi
                    ) VALUES (
                      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14
                    )
                    ON CONFLICT (bt_run_id, direction, timeframe, agg_type, agg_base)
                    DO UPDATE SET
                      wr_min=$6, conf_min=$7, trades_kept=$8, pnl_sum_total=$9, roi=$10,
                      baseline_trades=$11, baseline_pnl_sum=$12, baseline_roi=$13, uplift_roi=$14
                    """,
                    int(bt_run_id), str(direction), str(timeframe), str(agg_type), str(agg_base),
                    _r4(wmin), _r4(cmin), int(trd_kept), _r4f(pnl_kept), _r6f(roi),
                    int(base_trd), _r4f(base_pnl), _r6f(base_roi), _r6f(uplift)
                )
                winners_written += 1

            total_blocks += 1

        # итоговый лог
        log.info(
            "✅ MW_BACKTEST: sid=%s report_id=%s bt_run_id=%s blocks=%d grid_cells=%d winners=%d deposit=%.4f",
            strategy_id, report_id, bt_run_id, total_blocks, total_cells, winners_written, deposit_used
        )


# 🔸 Безопасный JSON loader (строки из Redis Stream)
def _safe_load_json(s):
    try:
        import json
        return json.loads(s or "{}")
    except Exception:
        return {}

# 🔸 Округления под числовые поля БД
def _r4(x) -> float:
    try:
        return round(float(x or 0.0), 4)
    except Exception:
        return 0.0

def _r4f(x) -> float:
    try:
        return round(float(x or 0.0), 4)
    except Exception:
        return 0.0

def _r6f(x) -> float:
    try:
        return round(float(x or 0.0), 6)
    except Exception:
        return 0.0

# 🔸 Сравнение кандидатов победителя (tie-breakers)
def _better(a: Tuple[float, int, float, float, float],
            b: Tuple[float, int, float, float, float]) -> bool:
    # порядок: roi DESC, trades_kept DESC, conf_min DESC, wr_min DESC
    ar, an, ac, aw, _ = a
    br, bn, bc, bw, _ = b
    if ar != br: return ar > br
    if an != bn: return an > bn
    if ac != bc: return ac > bc
    if aw != bw: return aw > bw
    return False

# 🔸 Вставка grid батчами
async def _insert_grid_rows(conn, rows: List[Tuple]):
    if not rows:
        return
    sql = """
    INSERT INTO oracle_mw_bt_grid (
      bt_run_id, direction, timeframe, agg_type, agg_base,
      wr_min, conf_min, trades_kept, pnl_sum_total, roi,
      baseline_trades, baseline_pnl_sum, baseline_roi
    ) VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13
    )
    """
    i = 0
    total = len(rows)
    while i < total:
        chunk = rows[i:i+GRID_INSERT_BATCH]
        async with conn.transaction():
            await conn.executemany(sql, chunk)
        i += len(chunk)