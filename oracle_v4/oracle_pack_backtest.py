# oracle_pack_backtest.py — воркер PACK-backtest: 7d-подбор порогов (winrate/confidence) по PACK-осям, защита от гонок, публикация WL/BL v3 (пороги как NUMERIC(6,4) + расширенные логи)

# 🔸 Импорты
import asyncio
import logging
import json
import math
from typing import Dict, List, Tuple
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

import infra

# 🔸 Логгер
log = logging.getLogger("ORACLE_PACK_BACKTEST")

# 🔸 Стримы
PACK_SENSE_REPORT_READY_STREAM = "oracle:pack_sense:reports_ready"     # вход: готовность PACK-confidence
PACK_BT_CONSUMER_GROUP = "oracle_pack_backtest_group"
PACK_BT_CONSUMER_NAME  = "oracle_pack_backtest_worker"

PACK_LISTS_REPORTS_READY_STREAM = "oracle:pack_lists:reports_ready"    # выход: готовность WL/BL v3
PACK_LISTS_REPORTS_READY_MAXLEN = 10_000

# 🔸 Параллелизм
MAX_CONCURRENT_RUNS = 2

# 🔸 Пакетные размеры
GRID_INSERT_BATCH = 1000
WL_INSERT_BATCH   = 1000

# 🔸 Пороговые ограничения BT
CONF_BT_MIN     = 0.25   # глобальный нижний порог по confidence для сетки/победителя
WINNER_MIN_ABS  = 20     # минимальная масса сделок у победителя (абс.)
WINNER_MIN_FRAC = 0.10   # минимальная масса у победителя (доля от baseline_trades)
ROW_MIN_SHARE   = 0.03   # строка попадает в сетку, если её масса ≥ 3% от всех сделок стратегии (7d)
WR_BL_MAX       = 0.50   # порог для blacklist v3 (WinRate < 0.50)

# 🔸 Публичная точка входа воркера
async def run_oracle_pack_backtest():
    # условия достаточности окружения
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск PACK-BACKTEST: PG/Redis не инициализированы")
        return

    # создание consumer group (идемпотентно)
    try:
        await infra.redis_client.xgroup_create(
            name=PACK_SENSE_REPORT_READY_STREAM,
            groupname=PACK_BT_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug("📡 Создана группа потребителей в Redis Stream: %s", PACK_BT_CONSUMER_GROUP)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.exception("❌ Ошибка инициализации группы Redis Stream для PACK-BACKTEST")
            return

    sem = asyncio.Semaphore(MAX_CONCURRENT_RUNS)
    log.debug(
        "🚀 Старт PACK-backtest (parallel=%d, conf_min>=%.2f, winner_mass≥max(%d,%d%% baseline), row_min_share=%.1f%%)",
        MAX_CONCURRENT_RUNS, CONF_BT_MIN, WINNER_MIN_ABS, int(WINNER_MIN_FRAC*100), ROW_MIN_SHARE*100
    )

    # основной цикл
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=PACK_BT_CONSUMER_GROUP,
                consumername=PACK_BT_CONSUMER_NAME,
                streams={PACK_SENSE_REPORT_READY_STREAM: ">"},
                count=64,
                block=30_000,
            )
            if not resp:
                continue

            tasks: List[asyncio.Task] = []
            to_ack: List[str] = []
            seen: set[Tuple[int, int]] = set()  # дедуп в батче: (strategy_id, report_id)

            for _stream, msgs in resp:
                for msg_id, fields in msgs:
                    to_ack.append(msg_id)
                    try:
                        payload = json.loads(fields.get("data", "{}") or "{}")
                    except Exception:
                        payload = {}

                    # работаем только с 7d
                    if str(payload.get("time_frame", "")).strip() != "7d":
                        continue

                    try:
                        strategy_id = int(payload.get("strategy_id", 0) or 0)
                        report_id   = int(payload.get("report_id", 0) or 0)
                        window_end  = payload.get("window_end")
                    except Exception:
                        strategy_id = 0; report_id = 0; window_end = None

                    if not (strategy_id and report_id and window_end):
                        continue

                    k = (strategy_id, report_id)
                    if k in seen:
                        continue
                    seen.add(k)

                    tasks.append(asyncio.create_task(_guarded_run(sem, strategy_id, report_id, window_end)))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=False)

            if to_ack:
                try:
                    await infra.redis_client.xack(PACK_SENSE_REPORT_READY_STREAM, PACK_BT_CONSUMER_GROUP, *to_ack)
                except Exception:
                    log.exception("⚠️ Ошибка ACK для PACK-BACKTEST")

        except asyncio.CancelledError:
            log.debug("⏹️ PACK-backtest остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка цикла PACK-backtest — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Гард: семафор + ловим исключения
async def _guarded_run(sem: asyncio.Semaphore, strategy_id: int, report_id: int, window_end_iso: str):
    async with sem:
        try:
            await _run_for_report(strategy_id, report_id, window_end_iso)
        except Exception:
            log.exception("❌ Сбой PACK-BACKTEST sid=%s report_id=%s", strategy_id, report_id)


# 🔸 Основной расчёт для одного отчёта 7d по стратегии
async def _run_for_report(strategy_id: int, report_id: int, window_end_iso: str):
    # парсинг времени окна
    try:
        window_end_dt = datetime.fromisoformat(str(window_end_iso).replace("Z", ""))
    except Exception:
        log.exception("❌ Неверный формат window_end: %r", window_end_iso)
        return

    async with infra.pg_pool.acquire() as conn:
        # advisory lock на report_id (сериализация расчёта)
        locked = await conn.fetchval("SELECT pg_try_advisory_lock($1)", int(report_id))
        if not locked:
            log.debug("⏭️ PACK-BACKTEST: пропуск (уже идёт расчёт) sid=%s report_id=%s", strategy_id, report_id)
            return
        try:
            # депозит стратегии
            deposit = await conn.fetchval("SELECT deposit FROM strategies_v4 WHERE id=$1", int(strategy_id))
            try:
                deposit_used = float(deposit) if (deposit is not None and float(deposit) > 0) else 1.0
            except Exception:
                deposit_used = 1.0

            # фиксация bt_run (уникален на report_id)
            row = await conn.fetchrow(
                """
                INSERT INTO oracle_pack_bt_run (strategy_id, report_id, time_frame, window_end, deposit_used)
                VALUES ($1,$2,'7d',$3,$4)
                ON CONFLICT (report_id) DO UPDATE
                  SET deposit_used = EXCLUDED.deposit_used
                RETURNING id
                """,
                int(strategy_id), int(report_id), window_end_dt, float(deposit_used)
            )
            bt_run_id = int(row["id"])

            # общий объём закрытых сделок стратегии (7d) — для порога строки
            closed_total = await conn.fetchval(
                "SELECT closed_total FROM oracle_report_stat WHERE id = $1",
                int(report_id)
            )
            closed_total = int(closed_total or 0)
            row_min_trades = max(1, int(math.ceil(ROW_MIN_SHARE * float(closed_total))))

            # агрегаты 7d (весь набор)
            rows = await conn.fetch(
                """
                SELECT direction, timeframe, pack_base, agg_type, agg_key, agg_value,
                       winrate::float8 AS wr,
                       confidence::float8 AS conf,
                       trades_total::int4 AS n,
                       pnl_sum_total::float8 AS pnl
                FROM oracle_pack_aggregated_stat
                WHERE report_id = $1
                  AND time_frame = '7d'
                """,
                int(report_id)
            )
            if not rows:
                log.debug("ℹ️ PACK-BACKTEST: пустые агрегаты report_id=%s sid=%s — пропуск", report_id, strategy_id)
                return

            # baseline и элементы блоков
            baseline_acc: Dict[Tuple[str, str, str, str, str], Dict[str, float]] = {}
            blocks: Dict[Tuple[str, str, str, str, str], List[dict]] = {}

            for r in rows:
                key = (str(r["direction"]), str(r["timeframe"]), str(r["pack_base"]), str(r["agg_type"]), str(r["agg_key"]))
                n   = int(r["n"] or 0)
                pnl = _r4f(r["pnl"])

                acc = baseline_acc.setdefault(key, {"trd": 0, "pnl": 0.0})
                acc["trd"] += n
                acc["pnl"] += pnl

                if n < row_min_trades:
                    continue  # в сетку не пускаем малые строки

                blocks.setdefault(key, []).append({
                    "wr":   _r4f(r["wr"]),
                    "conf": _r4f(r["conf"]),
                    "n":    n,
                    "pnl":  pnl,
                })

            # зачистка прошлых результатов этого bt_run
            async with conn.transaction():
                await conn.execute("DELETE FROM oracle_pack_bt_grid   WHERE bt_run_id = $1", bt_run_id)
                await conn.execute("DELETE FROM oracle_pack_bt_winner WHERE bt_run_id = $1", bt_run_id)

            total_blocks = 0
            total_cells  = 0
            winners_written = 0
            wl_v3_rows: List[Tuple] = []
            bl_v3_rows: List[Tuple] = []

            for (direction, timeframe, pack_base, agg_type, agg_key), items in blocks.items():
                base_trd = int(baseline_acc.get((direction, timeframe, pack_base, agg_type, agg_key), {}).get("trd", 0))
                base_pnl = float(baseline_acc.get((direction, timeframe, pack_base, agg_type, agg_key), {}).get("pnl", 0.0))
                base_roi = base_pnl / deposit_used if deposit_used != 0 else 0.0

                # дискретная сетка порогов (+0.0)
                w_vals = sorted(({_r4f(x["wr"]) for x in items} | {0.0})) if items else [0.0]
                c_vals = sorted(({_r4f(x["conf"]) for x in items} | {0.0})) if items else [0.0]
                wi = {w: i for i, w in enumerate(w_vals)}
                ci = {c: j for j, c in enumerate(c_vals)}
                nw, nc = len(w_vals), len(c_vals)

                # матрицы масс
                P = [[0.0 for _ in range(nc)] for _ in range(nw)]
                T = [[0   for _ in range(nc)] for _ in range(nw)]
                for x in items:
                    i = wi[_r4f(x["wr"])]
                    j = ci[_r4f(x["conf"])]
                    P[i][j] += x["pnl"]
                    T[i][j] += x["n"]

                # суффиксные суммы
                PS = [[0.0 for _ in range(nc)] for _ in range(nw)]
                TS = [[0   for _ in range(nc)] for _ in range(nw)]
                for i in range(nw - 1, -1, -1):
                    for j in range(nc - 1, -1, -1):
                        sP = P[i][j]; sT = T[i][j]
                        if i + 1 < nw:
                            sP += PS[i + 1][j]; sT += TS[i + 1][j]
                        if j + 1 < nc:
                            sP += PS[i][j + 1]; sT += TS[i][j + 1]
                        if (i + 1 < nw) and (j + 1 < nc):
                            sP -= PS[i + 1][j + 1]; sT -= TS[i + 1][j + 1]
                        PS[i][j] = sP; TS[i][j] = sT

                # сетка + выбор победителя (ограничения: conf_min, масса)
                grid_rows = []
                best = None  # (roi, trades_kept, conf_min(Dec), wr_min(Dec), pnl)
                min_trades_winner = max(WINNER_MIN_ABS, int(round(WINNER_MIN_FRAC * base_trd)))

                for i, wmin in enumerate(w_vals):
                    for j, cmin in enumerate(c_vals):
                        if cmin < CONF_BT_MIN:
                            continue  # глобальный пол по confidence

                        d_wmin  = _n4d(wmin)   # NUMERIC(6,4)
                        d_cmin  = _n4d(cmin)   # NUMERIC(6,4)

                        pnl_kept = _r4f(PS[i][j])
                        trd_kept = int(TS[i][j])
                        roi = pnl_kept / deposit_used if deposit_used != 0 else 0.0

                        grid_rows.append((
                            int(bt_run_id), str(direction), str(timeframe), str(pack_base), str(agg_type), str(agg_key),
                            d_wmin, d_cmin,
                            int(trd_kept), _r4f(pnl_kept), _r6f(roi),
                            int(base_trd), _r4f(base_pnl), _r6f(base_roi),
                        ))

                        if trd_kept < min_trades_winner:
                            continue
                        cand = (roi, trd_kept, d_cmin, d_wmin, pnl_kept)
                        if (best is None) or _better(cand, best):
                            best = cand

                # запись grid
                total_cells += len(grid_rows)
                await _insert_grid_rows(conn, grid_rows)

                # запись победителя + подготовка WL/BL v3
                if best is not None:
                    roi, trd_kept, d_cmin, d_wmin, pnl_kept = best

                    if pnl_kept <= 0.0:
                        log.debug(
                            "⚠️ PACK-BACKTEST: skip winner (non-positive pnl) sid=%s report=%s dir=%s tf=%s base=%s/%s key=%s wr>=%s conf>=%s kept=%d pnl=%.4f",
                            strategy_id, report_id, direction, timeframe, pack_base, agg_type, agg_key, d_wmin, d_cmin, trd_kept, pnl_kept
                        )
                        total_blocks += 1
                        continue

                    uplift = roi - base_roi
                    await conn.execute(
                        """
                        INSERT INTO oracle_pack_bt_winner (
                          bt_run_id, strategy_id, direction, timeframe, pack_base, agg_type, agg_key,
                          wr_min, conf_min, trades_kept, pnl_sum_total, roi,
                          baseline_trades, baseline_pnl_sum, baseline_roi, uplift_roi
                        ) VALUES (
                          $1,$2,$3,$4,$5,$6,$7,
                          $8,$9,$10,$11,$12,
                          $13,$14,$15,$16
                        )
                        ON CONFLICT (bt_run_id, direction, timeframe, pack_base, agg_type, agg_key)
                        DO UPDATE SET
                          strategy_id      = EXCLUDED.strategy_id,
                          wr_min           = EXCLUDED.wr_min,
                          conf_min         = EXCLUDED.conf_min,
                          trades_kept      = EXCLUDED.trades_kept,
                          pnl_sum_total    = EXCLUDED.pnl_sum_total,
                          roi              = EXCLUDED.roi,
                          baseline_trades  = EXCLUDED.baseline_trades,
                          baseline_pnl_sum = EXCLUDED.baseline_pnl_sum,
                          baseline_roi     = EXCLUDED.baseline_roi,
                          uplift_roi       = EXCLUDED.uplift_roi
                        """,
                        int(bt_run_id), int(strategy_id),
                        str(direction), str(timeframe), str(pack_base), str(agg_type), str(agg_key),
                        d_wmin, d_cmin, int(trd_kept), _r4f(pnl_kept), _r6f(roi),
                        int(base_trd), _r4f(base_pnl), _r6f(base_roi), _r6f(uplift)
                    )
                    winners_written += 1

                    # диагностический лог перед сбором WL/BL v3
                    log.debug(
                        "PACK-BT winner thresholds: sid=%s dir=%s tf=%s base=%s/%s key=%s wr_min=%s conf_min=%s row_min=%d baseline_trd=%d",
                        strategy_id, direction, timeframe, pack_base, agg_type, agg_key, d_wmin, d_cmin, row_min_trades, base_trd
                    )

                    # WL v3 (по победным порогам) и BL v3 (дополнение в рамках блока)
                    wl_rows, bl_rows, wl_probe_count = await _collect_wl_bl_v3_for_block(
                        conn=conn,
                        report_id=report_id,
                        strategy_id=strategy_id,
                        direction=direction,
                        timeframe=timeframe,
                        pack_base=pack_base,
                        agg_type=agg_type,
                        agg_key=agg_key,
                        wr_min=d_wmin,
                        conf_min=d_cmin,
                        row_min_trades=row_min_trades,
                    )
                    if wl_probe_count == 0:
                        log.debug(
                            "WL v3 empty despite winner: sid=%s dir=%s tf=%s base=%s/%s key=%s wr_min=%s conf_min=%s",
                            strategy_id, direction, timeframe, pack_base, agg_type, agg_key, d_wmin, d_cmin
                        )
                    wl_v3_rows.extend(wl_rows)
                    bl_v3_rows.extend(bl_rows)

                total_blocks += 1

            # публикация WL/BL v3 (перестройка версии 'v3' для стратегии)
            wl_count = 0
            bl_count = 0
            if wl_v3_rows or bl_v3_rows:
                async with conn.transaction():
                    await conn.execute(
                        "DELETE FROM oracle_pack_whitelist WHERE strategy_id = $1 AND version = 'v3'",
                        int(strategy_id)
                    )
                    # WL
                    i = 0
                    while i < len(wl_v3_rows):
                        chunk = wl_v3_rows[i:i+WL_INSERT_BATCH]
                        await conn.executemany(
                            """
                            INSERT INTO oracle_pack_whitelist (
                              aggregated_id, strategy_id, direction, timeframe,
                              pack_base, agg_type, agg_key, agg_value,
                              winrate, confidence, list, version
                            ) VALUES (
                              $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'whitelist','v3'
                            )
                            """,
                            chunk
                        )
                        wl_count += len(chunk)
                        i += len(chunk)
                    # BL
                    j = 0
                    while j < len(bl_v3_rows):
                        chunk = bl_v3_rows[j:j+WL_INSERT_BATCH]
                        await conn.executemany(
                            """
                            INSERT INTO oracle_pack_whitelist (
                              aggregated_id, strategy_id, direction, timeframe,
                              pack_base, agg_type, agg_key, agg_value,
                              winrate, confidence, list, version
                            ) VALUES (
                              $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'blacklist','v3'
                            )
                            """,
                            chunk
                        )
                        bl_count += len(chunk)
                        j += len(chunk)

                # уведомление consumers о готовности v3
                try:
                    payload = {
                        "strategy_id": int(strategy_id),
                        "report_id": int(report_id),
                        "time_frame": "7d",
                        "version": "v3",
                        "window_end": window_end_dt.isoformat(),
                        "rows_whitelist": int(wl_count),
                        "rows_blacklist": int(bl_count),
                        "generated_at": datetime.utcnow().replace(tzinfo=None).isoformat(),
                    }
                    await infra.redis_client.xadd(
                        name=PACK_LISTS_REPORTS_READY_STREAM,
                        fields={"data": json.dumps(payload, separators=(",", ":"))},
                        maxlen=PACK_LISTS_REPORTS_READY_MAXLEN,
                        approximate=True,
                    )
                except Exception:
                    log.exception("❌ Ошибка публикации события v3 в %s", PACK_LISTS_REPORTS_READY_STREAM)

            # итоговый лог
            log.debug(
                "✅ PACK_BACKTEST: sid=%s report_id=%s bt_run_id=%s blocks=%d grid_cells=%d winners=%d wl_v3=%d bl_v3=%d deposit=%.4f row_min=%d conf_min>=%.2f",
                strategy_id, report_id, bt_run_id, total_blocks, total_cells, winners_written, wl_count, bl_count,
                deposit_used, row_min_trades, CONF_BT_MIN
            )

        finally:
            # unlock
            try:
                await conn.execute("SELECT pg_advisory_unlock($1)", int(report_id))
            except Exception:
                pass


# 🔸 Сбор WL/BL v3 для одного блока (пороги NUMERIC(6,4) + диагностические логи)
async def _collect_wl_bl_v3_for_block(
    *,
    conn,
    report_id: int,
    strategy_id: int,
    direction: str,
    timeframe: str,
    pack_base: str,
    agg_type: str,
    agg_key: str,
    wr_min: Decimal,
    conf_min: Decimal,
    row_min_trades: int,
) -> Tuple[List[Tuple], List[Tuple], int]:
    # лог параметров отбора
    log.debug(
        "WL/BL v3 collect: sid=%s dir=%s tf=%s base=%s/%s key=%s wr_min=%s conf_min=%s row_min=%d",
        strategy_id, direction, timeframe, pack_base, agg_type, agg_key, wr_min, conf_min, row_min_trades
    )

    # выбираем строки блока, прошедшие победные пороги (для WL v3)
    rows_wl = await conn.fetch(
        """
        SELECT
          a.id            AS aggregated_id,
          a.strategy_id   AS strategy_id,
          a.direction     AS direction,
          a.timeframe     AS timeframe,
          a.pack_base     AS pack_base,
          a.agg_type      AS agg_type,
          a.agg_key       AS agg_key,
          a.agg_value     AS agg_value,
          a.winrate       AS winrate,
          a.confidence    AS confidence
        FROM oracle_pack_aggregated_stat a
        WHERE a.report_id   = $1
          AND a.strategy_id = $2
          AND a.direction   = $3
          AND a.timeframe   = $4
          AND a.pack_base   = $5
          AND a.agg_type    = $6
          AND a.agg_key     = $7
          AND a.winrate     >= $8
          AND a.confidence  >= $9
          AND a.trades_total >= $10
        """,
        int(report_id), int(strategy_id), str(direction), str(timeframe),
        str(pack_base), str(agg_type), str(agg_key),
        wr_min, conf_min, int(row_min_trades)
    )

    wl_rows: List[Tuple] = []
    for r in rows_wl:
        wl_rows.append((
            int(r["aggregated_id"]),
            int(r["strategy_id"]),
            str(r["direction"]),
            str(r["timeframe"]),
            str(r["pack_base"]),
            str(r["agg_type"]),
            str(r["agg_key"]),
            str(r["agg_value"]),
            float(r["winrate"] or 0.0),
            float(r["confidence"] or 0.0),
        ))

    # выбираем строки блока, НЕ прошедшие победные пороги и с низким WR (для BL v3)
    rows_bl = await conn.fetch(
        """
        SELECT
          a.id            AS aggregated_id,
          a.strategy_id   AS strategy_id,
          a.direction     AS direction,
          a.timeframe     AS timeframe,
          a.pack_base     AS pack_base,
          a.agg_type      AS agg_type,
          a.agg_key       AS agg_key,
          a.agg_value     AS agg_value,
          a.winrate       AS winrate,
          a.confidence    AS confidence
        FROM oracle_pack_aggregated_stat a
        WHERE a.report_id   = $1
          AND a.strategy_id = $2
          AND a.direction   = $3
          AND a.timeframe   = $4
          AND a.pack_base   = $5
          AND a.agg_type    = $6
          AND a.agg_key     = $7
          AND a.trades_total >= $8
          AND (a.winrate <  $9 OR a.confidence < $10)
          AND a.winrate <  $11
        """,
        int(report_id), int(strategy_id), str(direction), str(timeframe),
        str(pack_base), str(agg_type), str(agg_key),
        int(row_min_trades), wr_min, conf_min, Decimal(str(WR_BL_MAX)).quantize(Decimal("0.0001"))
    )

    bl_rows: List[Tuple] = []
    for r in rows_bl:
        bl_rows.append((
            int(r["aggregated_id"]),
            int(r["strategy_id"]),
            str(r["direction"]),
            str(r["timeframe"]),
            str(r["pack_base"]),
            str(r["agg_type"]),
            str(r["agg_key"]),
            str(r["agg_value"]),
            float(r["winrate"] or 0.0),
            float(r["confidence"] or 0.0),
        ))

    # лог о факте отбора по блоку
    log.debug(
        "WL/BL v3 block result: sid=%s dir=%s tf=%s base=%s/%s key=%s wl=%d bl=%d",
        strategy_id, direction, timeframe, pack_base, agg_type, agg_key, len(wl_rows), len(bl_rows)
    )

    return wl_rows, bl_rows, len(rows_wl)


# 🔸 Округления/квантизации под числовые поля БД
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

def _n4d(x) -> Decimal:
    try:
        return Decimal(str(x)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    except Exception:
        return Decimal("0.0000")


# 🔸 Сравнение кандидатов победителя (tie-breakers)
def _better(a: Tuple[float, int, Decimal, Decimal, float],
            b: Tuple[float, int, Decimal, Decimal, float]) -> bool:
    # порядок: roi DESC, trades_kept DESC, conf_min DESC, wr_min DESC
    ar, an, ac, aw, _ = a
    br, bn, bc, bw, _ = b
    if ar != br: return ar > br
    if an != bn: return an > bn
    if ac != bc: return ac > bc
    if aw != bw: return aw > bw
    return False


# 🔸 Вставка grid батчами (UPSERT по уникальному ключу клетки)
async def _insert_grid_rows(conn, rows: List[Tuple]):
    if not rows:
        return
    sql = """
    INSERT INTO oracle_pack_bt_grid (
      bt_run_id, direction, timeframe, pack_base, agg_type, agg_key,
      wr_min, conf_min, trades_kept, pnl_sum_total, roi,
      baseline_trades, baseline_pnl_sum, baseline_roi
    ) VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14
    )
    ON CONFLICT (bt_run_id, direction, timeframe, pack_base, agg_type, agg_key, wr_min, conf_min)
    DO UPDATE SET
      trades_kept      = EXCLUDED.trades_kept,
      pnl_sum_total    = EXCLUDED.pnl_sum_total,
      roi              = EXCLUDED.roi,
      baseline_trades  = EXCLUDED.baseline_trades,
      baseline_pnl_sum = EXCLUDED.baseline_pnl_sum,
      baseline_roi     = EXCLUDED.baseline_roi
    """
    i = 0
    total = len(rows)
    while i < total:
        chunk = rows[i:i+GRID_INSERT_BATCH]
        async with conn.transaction():
            await conn.executemany(sql, chunk)
        i += len(chunk)