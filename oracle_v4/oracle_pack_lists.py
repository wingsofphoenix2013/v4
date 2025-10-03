# oracle_pack_lists.py — воркер PACK-lists: сбор whitelist/blacklist по правилам (7d, sense>0.5; confidence>0.5; wr≥0.55 → WL, wr<0.5 → BL) + уведомления о готовности

# 🔸 Импорты
import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List, Tuple

import infra

# 🔸 Логгер
log = logging.getLogger("ORACLE_PACK_LISTS")

# 🔸 Стрим-источник (готовность SENSE → время собирать списки)
PACK_LISTS_BUILD_READY_STREAM = "oracle:pack_lists:build_ready"
PACK_LISTS_CONSUMER_GROUP = "oracle_pack_lists_group"
PACK_LISTS_CONSUMER_NAME = "oracle_pack_lists_worker"

# 🔸 Стрим-уведомление: списки готовы
PACK_LISTS_REPORTS_READY_STREAM = "oracle:pack_lists:reports_ready"
PACK_LISTS_REPORTS_READY_MAXLEN = 10_000

# 🔸 Пороговые значения (легко меняются → перезапуск сервиса)
SENSE_SCORE_MIN = 0.5     # score_smoothed > 0.5 на оси (pack_base+agg_type+agg_key)
CONF_MIN = 0.5            # aggregated confidence > 0.5
WR_WL_MIN = 0.6          # в whitelist при winrate ≥ 0.6
WR_BL_MAX = 0.5           # в blacklist при winrate < 0.5

# 🔸 Публичная точка входа воркера
async def run_oracle_pack_lists():
    # условия достаточности окружения
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск PACK-lists: нет PG/Redis")
        return

    # создаём consumer group (идемпотентно)
    try:
        await infra.redis_client.xgroup_create(
            name=PACK_LISTS_BUILD_READY_STREAM,
            groupname=PACK_LISTS_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug("📡 Создана группа потребителей: %s", PACK_LISTS_CONSUMER_GROUP)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            log.exception("❌ Ошибка инициализации группы Redis Stream")
            return

    log.debug("🚀 Старт воркера PACK-lists (WL/BL)")

    # основной цикл
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=PACK_LISTS_CONSUMER_GROUP,
                consumername=PACK_LISTS_CONSUMER_NAME,
                streams={PACK_LISTS_BUILD_READY_STREAM: ">"},
                count=64,
                block=30_000,
            )
            if not resp:
                continue

            for _, msgs in resp:
                for msg_id, fields in msgs:
                    try:
                        payload = json.loads(fields.get("data", "{}"))
                        report_id = int(payload.get("report_id", 0))
                        strategy_id = int(payload.get("strategy_id", 0))
                        time_frame = payload.get("time_frame")
                        window_end = payload.get("window_end")

                        if not (report_id and strategy_id and time_frame and window_end):
                            log.debug("ℹ️ Пропуск: мало данных в сообщении %s", payload)
                            await infra.redis_client.xack(PACK_LISTS_BUILD_READY_STREAM, PACK_LISTS_CONSUMER_GROUP, msg_id)
                            continue

                        # обрабатываем ТОЛЬКО 7d (как договорились)
                        if str(time_frame) != "7d":
                            await infra.redis_client.xack(PACK_LISTS_BUILD_READY_STREAM, PACK_LISTS_CONSUMER_GROUP, msg_id)
                            continue

                        rows_total, rows_wl, rows_bl = await _build_lists_for_7d(report_id, strategy_id, window_end)

                        # уведомляем downstream: списки готовы
                        try:
                            payload2 = {
                                "strategy_id": int(strategy_id),
                                "report_id": int(report_id),
                                "time_frame": "7d",
                                "window_end": datetime.fromisoformat(str(window_end).replace("Z", "")).isoformat(),
                                "rows_total": int(rows_total),
                                "rows_whitelist": int(rows_wl),
                                "rows_blacklist": int(rows_bl),
                                "generated_at": datetime.utcnow().replace(tzinfo=None).isoformat(),
                            }
                            await infra.redis_client.xadd(
                                name=PACK_LISTS_REPORTS_READY_STREAM,
                                fields={"data": json.dumps(payload2, separators=(",", ":"))},
                                maxlen=PACK_LISTS_REPORTS_READY_MAXLEN,
                                approximate=True,
                            )
                            log.debug("[PACK_LISTS_REPORTS_READY] sid=%s report_id=%s wl=%d bl=%d",
                                      strategy_id, report_id, rows_wl, rows_bl)
                        except Exception:
                            log.exception("❌ Ошибка публикации события в %s", PACK_LISTS_REPORTS_READY_STREAM)

                        await infra.redis_client.xack(PACK_LISTS_BUILD_READY_STREAM, PACK_LISTS_CONSUMER_GROUP, msg_id)

                    except Exception:
                        log.exception("❌ Ошибка обработки сообщения PACK-lists")

        except asyncio.CancelledError:
            log.debug("⏹️ PACK-lists остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка цикла PACK-lists — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Сбор WL/BL для 7d: очищаем по стратегии и вставляем свежий набор
async def _build_lists_for_7d(report_id: int, strategy_id: int, window_end_iso: str) -> Tuple[int, int, int]:
    try:
        window_end_dt = datetime.fromisoformat(str(window_end_iso).replace("Z", ""))
    except Exception:
        log.exception("❌ Неверный формат window_end: %r", window_end_iso)
        return 0, 0, 0

    async with infra.pg_pool.acquire() as conn:
        # 1) оси, прошедшие sense-фильтр (score_smoothed > 0.5) на этом отчёте
        axes = await conn.fetch(
            """
            SELECT timeframe, direction, pack_base, agg_type, agg_key
            FROM oracle_pack_sense_stat
            WHERE report_id = $1
              AND time_frame = '7d'
              AND score_smoothed > $2
            """,
            report_id, float(SENSE_SCORE_MIN)
        )
        if not axes:
            # очистим текущий WL/BL по стратегии — срез пустой
            async with conn.transaction():
                await conn.execute("DELETE FROM oracle_pack_whitelist WHERE strategy_id = $1", strategy_id)
            log.debug("ℹ️ PACK-lists: нет осей sense>%.2f (sid=%s, report=%s) — списки очищены", SENSE_SCORE_MIN, strategy_id, report_id)
            return 0, 0, 0

        selectors = {(r["timeframe"], r["direction"], r["pack_base"], r["agg_type"], r["agg_key"]) for r in axes}

        # 2) кандидаты из aggregated_stat (confidence > 0.5) по 7d
        cand_rows = await conn.fetch(
            """
            SELECT
              a.id          AS aggregated_id,
              a.strategy_id AS strategy_id,
              a.direction   AS direction,
              a.timeframe   AS timeframe,
              a.pack_base   AS pack_base,
              a.agg_type    AS agg_type,
              a.agg_key     AS agg_key,
              a.agg_value   AS agg_value,
              a.winrate     AS winrate,
              a.confidence  AS confidence
            FROM oracle_pack_aggregated_stat a
            WHERE a.report_id = $1
              AND a.time_frame = '7d'
              AND a.strategy_id = $2
              AND a.confidence > $3
            """,
            report_id, strategy_id, float(CONF_MIN)
        )

        # 3) фильтр по осям из sense и классификация WL/BL в Python (по winrate)
        to_insert = []
        rows_wl = 0
        rows_bl = 0
        for r in cand_rows:
            key = (r["timeframe"], r["direction"], r["pack_base"], r["agg_type"], r["agg_key"])
            if key not in selectors:
                continue

            wr = float(r["winrate"] or 0.0)

            if wr >= WR_WL_MIN:
                list_tag = "whitelist"; rows_wl += 1
            elif wr < WR_BL_MAX:
                list_tag = "blacklist"; rows_bl += 1
            else:
                continue  # диапазон [0.5, 0.549...] не попадает

            to_insert.append({
                "aggregated_id": int(r["aggregated_id"]),
                "strategy_id": int(r["strategy_id"]),
                "direction": str(r["direction"]),
                "timeframe": str(r["timeframe"]),
                "pack_base": str(r["pack_base"]),
                "agg_type": str(r["agg_type"]),
                "agg_key": str(r["agg_key"]),
                "agg_value": str(r["agg_value"]),
                "winrate": float(wr),
                "confidence": float(r["confidence"] or 0.0),
                "list": list_tag,
            })

        # 4) атомарное обновление «актуального среза»
        async with conn.transaction():
            await conn.execute("DELETE FROM oracle_pack_whitelist WHERE strategy_id = $1", strategy_id)

            if to_insert:
                await conn.executemany(
                    """
                    INSERT INTO oracle_pack_whitelist (
                        aggregated_id, strategy_id, direction, timeframe,
                        pack_base, agg_type, agg_key, agg_value,
                        winrate, confidence, list
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11
                    )
                    """,
                    [
                        (
                            row["aggregated_id"],
                            row["strategy_id"],
                            row["direction"],
                            row["timeframe"],
                            row["pack_base"],
                            row["agg_type"],
                            row["agg_key"],
                            row["agg_value"],
                            row["winrate"],
                            row["confidence"],
                            row["list"],
                        )
                        for row in to_insert
                    ]
                )

        rows_total = len(to_insert)
        log.debug("✅ PACK-lists обновлён (7d): sid=%s report_id=%s rows_total=%d wl=%d bl=%d",
                 strategy_id, report_id, rows_total, rows_wl, rows_bl)

        return rows_total, rows_wl, rows_bl