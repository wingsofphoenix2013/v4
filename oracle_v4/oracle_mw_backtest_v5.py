# oracle_mw_backtest_v5.py — воркер v5 (лайт): WL/BL только по winrate (>=0.60 → WL, <0.60 → BL), очистка версии перед вставкой

# 🔸 Импорты
import asyncio
import json
import logging
from datetime import datetime
from typing import List, Tuple

import infra

# 🔸 Логгер
log = logging.getLogger("ORACLE_MW_BACKTEST_V5")

# 🔸 Константы стримов (триггер → готов MW-отчёт; уведомление → списки готовы)
REPORT_STREAM = "oracle:mw:reports_ready"
CONSUMER_GROUP = "oracle_backtest_v5_group"
CONSUMER_NAME = "oracle_backtest_v5_worker"

WHITELIST_READY_STREAM = "oracle:mw_whitelist:reports_ready"
WHITELIST_READY_MAXLEN = 10_000

# 🔸 Настройки v5
ONLY_TIME_FRAME = "7d"
WL_VERSION = "v5"
WR_THRESHOLD = 0.60  # winrate >= 0.60 → whitelist, иначе blacklist


# 🔸 Публичная точка входа воркера (поднимается из oracle_v4_main.py → run_safe_loop)
async def run_oracle_mw_backtest_v5():
    # условия достаточности
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск: PG/Redis не инициализированы")
        return

    # создание consumer group (идемпотентно)
    try:
        await infra.redis_client.xgroup_create(
            name=REPORT_STREAM, groupname=CONSUMER_GROUP, id="$", mkstream=True
        )
        log.debug("📡 Создана группа потребителей Redis Stream: %s", CONSUMER_GROUP)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            log.exception("❌ Ошибка инициализации группы Redis Stream")
            return

    log.debug("🚀 Старт воркера MW backtest v5 (лайт)")

    # основной цикл
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=CONSUMER_NAME,
                streams={REPORT_STREAM: ">"},
                count=128,
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
                        if not (report_id and strategy_id and window_end and time_frame == ONLY_TIME_FRAME):
                            await infra.redis_client.xack(REPORT_STREAM, CONSUMER_GROUP, msg_id)
                            continue

                        await _process_report_7d_v5(report_id, strategy_id, window_end)
                        await infra.redis_client.xack(REPORT_STREAM, CONSUMER_GROUP, msg_id)

                    except Exception:
                        log.exception("❌ Ошибка обработки сообщения в MW v5")
        except asyncio.CancelledError:
            log.debug("⏹️ Воркер MW v5 остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка цикла MW v5 — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Обработка одного 7d-отчёта: полный reset версии v5 и раскладка по winrate
async def _process_report_7d_v5(report_id: int, strategy_id: int, window_end_iso: str):
    # парсинг window_end (для лога/пэйлоада)
    try:
        window_end_dt = datetime.fromisoformat(str(window_end_iso).replace("Z", ""))
    except Exception:
        log.exception("❌ Неверный формат window_end: %r", window_end_iso)
        return

    async with infra.pg_pool.acquire() as conn:
        # гард «последний отчёт 7d (source='mw')»
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
            log.debug("ℹ️ Пропуск sid=%s: нет MW-отчётов 7d", strategy_id)
            return
        if int(latest_id) != int(report_id):
            log.debug("⏭️ Пропуск sid=%s rep=%s: не последний MW-отчёт (latest=%s)", strategy_id, report_id, latest_id)
            return

        # полная очистка версии v5 для стратегии (и WL, и BL)
        async with conn.transaction():
            # удаляем все v5 по стратегии (reset версии)
            res_del = await conn.execute(
                "DELETE FROM oracle_mw_whitelist WHERE strategy_id = $1 AND version = $2",
                int(strategy_id), str(WL_VERSION)
            )
            log.debug("🧹 MW v5 reset для sid=%s: %s", strategy_id, res_del)

            # берём все агрегаты текущего отчёта 7d
            rows = await conn.fetch(
                """
                SELECT
                  id            AS aggregated_id,
                  strategy_id   AS strategy_id,
                  direction     AS direction,
                  timeframe     AS timeframe,
                  agg_base      AS agg_base,
                  agg_state     AS agg_state,
                  winrate       AS winrate
                FROM oracle_mw_aggregated_stat
                WHERE report_id = $1
                  AND time_frame = '7d'
                """,
                int(report_id)
            )

            rows_written = 0
            if rows:
                wl_batch: List[Tuple] = []
                bl_batch: List[Tuple] = []

                # раскладываем по winrate
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
                        0.0,  # confidence для v5 = 0
                    )
                    if wr >= WR_THRESHOLD:
                        wl_batch.append(rec)
                    else:
                        bl_batch.append(rec)

                # вставка WL
                if wl_batch:
                    await conn.executemany(
                        """
                        INSERT INTO oracle_mw_whitelist (
                            aggregated_id, strategy_id, direction, timeframe,
                            agg_base, agg_state, winrate, confidence, version, list
                        ) VALUES (
                            $1,$2,$3,$4,$5,$6,$7,$8,$9,'whitelist'
                        )
                        """,
                        [(*rec, WL_VERSION) for rec in wl_batch]
                    )
                    rows_written += len(wl_batch)

                # вставка BL
                if bl_batch:
                    await conn.executemany(
                        """
                        INSERT INTO oracle_mw_whitelist (
                            aggregated_id, strategy_id, direction, timeframe,
                            agg_base, agg_state, winrate, confidence, version, list
                        ) VALUES (
                            $1,$2,$3,$4,$5,$6,$7,$8,$9,'blacklist'
                        )
                        """,
                        [(*rec, WL_VERSION) for rec in bl_batch]
                    )
                    rows_written += len(bl_batch)

        # публикация события «lists ready» для v5
        try:
            payload = {
                "strategy_id": int(strategy_id),
                "report_id": int(report_id),
                "time_frame": ONLY_TIME_FRAME,
                "version": WL_VERSION,
                "window_end": window_end_dt.isoformat(),
                "rows_inserted": int(rows_written),
                "generated_at": datetime.utcnow().replace(tzinfo=None).isoformat(),
            }
            await infra.redis_client.xadd(
                name=WHITELIST_READY_STREAM,
                fields={"data": json.dumps(payload, separators=(",", ":"))},
                maxlen=WHITELIST_READY_MAXLEN,
                approximate=True,
            )
            log.debug("[MW_WL_READY v5] sid=%s rep=%s rows=%d", strategy_id, report_id, rows_written)
        except Exception:
            log.exception("❌ Ошибка публикации события в %s (v5)", WHITELIST_READY_STREAM)