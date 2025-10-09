# oracle_pack_lists.py — воркер PACK-lists: WL/BL v1 (sense+conf+wr) и v2 (масса+wr), оба только для 7d

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
SENSE_SCORE_MIN = 0.5     # v1: score_smoothed > 0.5 на оси (pack_base+agg_type+agg_key)
CONF_MIN = 0.5            # v1: aggregated confidence > 0.5
WR_WL_MIN = 0.55          # в whitelist при winrate ≥ 0.55
WR_BL_MAX = 0.5           # в blacklist при winrate < 0.5
WL_V2_MIN_SHARE = 0.01    # v2: доля от общего числа закрытых сделок (1%)

# 🔸 Публичная точка входа воркера
async def run_oracle_pack_lists():
    # условия достаточности
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск PACK-lists: нет PG/Redis")
        return

    # создание consumer group (идемпотентно)
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

    log.debug("🚀 Старт воркера PACK-lists (WL/BL v1+v2)")

    # основной цикл чтения стрима
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

                        # валидация сообщения
                        if not (report_id and strategy_id and time_frame and window_end):
                            log.debug("ℹ️ Пропуск: мало данных в сообщении %s", payload)
                            await infra.redis_client.xack(PACK_LISTS_BUILD_READY_STREAM, PACK_LISTS_CONSUMER_GROUP, msg_id)
                            continue

                        # обрабатываем ТОЛЬКО 7d
                        if str(time_frame) != "7d":
                            await infra.redis_client.xack(PACK_LISTS_BUILD_READY_STREAM, PACK_LISTS_CONSUMER_GROUP, msg_id)
                            continue

                        # построение v1 (sense+conf+wr)
                        rows_total_v1, rows_wl_v1, rows_bl_v1 = await _build_lists_v1_7d(report_id, strategy_id, window_end)
                        # уведомление v1
                        try:
                            payload_v1 = {
                                "strategy_id": int(strategy_id),
                                "report_id": int(report_id),
                                "time_frame": "7d",
                                "version": "v1",
                                "window_end": datetime.fromisoformat(str(window_end).replace("Z", "")).isoformat(),
                                "rows_total": int(rows_total_v1),
                                "rows_whitelist": int(rows_wl_v1),
                                "rows_blacklist": int(rows_bl_v1),
                                "generated_at": datetime.utcnow().replace(tzinfo=None).isoformat(),
                            }
                            await infra.redis_client.xadd(
                                name=PACK_LISTS_REPORTS_READY_STREAM,
                                fields={"data": json.dumps(payload_v1, separators=(",", ":"))},
                                maxlen=PACK_LISTS_REPORTS_READY_MAXLEN,
                                approximate=True,
                            )
                            log.debug("[PACK_LISTS_REPORTS_READY] v1 sid=%s report_id=%s wl=%d bl=%d",
                                      strategy_id, report_id, rows_wl_v1, rows_bl_v1)
                        except Exception:
                            log.exception("❌ Ошибка публикации v1 события в %s", PACK_LISTS_REPORTS_READY_STREAM)

                        # построение v2 (масса+wr, без sense/conf)
                        rows_total_v2, rows_wl_v2, rows_bl_v2 = await _build_lists_v2_7d(report_id, strategy_id, window_end)
                        # уведомление v2
                        try:
                            payload_v2 = {
                                "strategy_id": int(strategy_id),
                                "report_id": int(report_id),
                                "time_frame": "7d",
                                "version": "v2",
                                "window_end": datetime.fromisoformat(str(window_end).replace("Z", "")).isoformat(),
                                "rows_total": int(rows_total_v2),
                                "rows_whitelist": int(rows_wl_v2),
                                "rows_blacklist": int(rows_bl_v2),
                                "generated_at": datetime.utcnow().replace(tzinfo=None).isoformat(),
                            }
                            await infra.redis_client.xadd(
                                name=PACK_LISTS_REPORTS_READY_STREAM,
                                fields={"data": json.dumps(payload_v2, separators=(",", ":"))},
                                maxlen=PACK_LISTS_REPORTS_READY_MAXLEN,
                                approximate=True,
                            )
                            log.debug("[PACK_LISTS_REPORTS_READY] v2 sid=%s report_id=%s wl=%d bl=%d",
                                      strategy_id, report_id, rows_wl_v2, rows_bl_v2)
                        except Exception:
                            log.exception("❌ Ошибка публикации v2 события в %s", PACK_LISTS_REPORTS_READY_STREAM)

                        # ACK сообщения после успешного построения v1 и v2
                        await infra.redis_client.xack(PACK_LISTS_BUILD_READY_STREAM, PACK_LISTS_CONSUMER_GROUP, msg_id)

                    except Exception:
                        log.exception("❌ Ошибка обработки сообщения PACK-lists")

        except asyncio.CancelledError:
            log.debug("⏹️ PACK-lists остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка цикла PACK-lists — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Сбор WL/BL v1 (7d): sense>0.5, confidence>0.5, по winrate → WL/BL
async def _build_lists_v1_7d(report_id: int, strategy_id: int, window_end_iso: str) -> Tuple[int, int, int]:
    # парсинг времени
    try:
        window_end_dt = datetime.fromisoformat(str(window_end_iso).replace("Z", ""))
    except Exception:
        log.exception("❌ Неверный формат window_end (v1): %r", window_end_iso)
        return 0, 0, 0

    async with infra.pg_pool.acquire() as conn:
        # оси, прошедшие sense-фильтр
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
            # очищаем только v1-срез
            async with conn.transaction():
                await conn.execute("DELETE FROM oracle_pack_whitelist WHERE strategy_id = $1 AND version = 'v1'", strategy_id)
            log.debug("ℹ️ PACK-lists v1: нет осей sense>%.2f (sid=%s, report=%s) — v1 очищен", SENSE_SCORE_MIN, strategy_id, report_id)
            return 0, 0, 0

        selectors = {(r["timeframe"], r["direction"], r["pack_base"], r["agg_type"], r["agg_key"]) for r in axes}

        # кандидаты aggregated_stat с порогом по confidence
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
            WHERE a.report_id   = $1
              AND a.time_frame  = '7d'
              AND a.strategy_id = $2
              AND a.confidence  > $3
            """,
            report_id, strategy_id, float(CONF_MIN)
        )

        # классификация WL/BL по winrate
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
                continue

            to_insert.append((
                int(r["aggregated_id"]),
                int(r["strategy_id"]),
                str(r["direction"]),
                str(r["timeframe"]),
                str(r["pack_base"]),
                str(r["agg_type"]),
                str(r["agg_key"]),
                str(r["agg_value"]),
                float(wr),
                float(r["confidence"] or 0.0),
            ))

        # атомарно перестраиваем v1-срез
        async with conn.transaction():
            await conn.execute("DELETE FROM oracle_pack_whitelist WHERE strategy_id = $1 AND version = 'v1'", strategy_id)
            if to_insert:
                await conn.executemany(
                    """
                    INSERT INTO oracle_pack_whitelist (
                        aggregated_id, strategy_id, direction, timeframe,
                        pack_base, agg_type, agg_key, agg_value,
                        winrate, confidence, list, version
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,CASE WHEN $9 >= $11 THEN 'whitelist' ELSE 'blacklist' END,'v1'
                    )
                    """,
                    [row + (WR_WL_MIN,) for row in to_insert]
                )

        rows_total = len(to_insert)
        log.debug("✅ PACK-lists v1 обновлён (7d): sid=%s report_id=%s rows_total=%d wl=%d bl=%d",
                  strategy_id, report_id, rows_total, rows_wl, rows_bl)
        return rows_total, rows_wl, rows_bl


# 🔸 Сбор WL/BL v2 (7d): по доле сделок и winrate (без sense/conf)
async def _build_lists_v2_7d(report_id: int, strategy_id: int, window_end_iso: str) -> Tuple[int, int, int]:
    # парсинг времени
    try:
        window_end_dt = datetime.fromisoformat(str(window_end_iso).replace("Z", ""))
    except Exception:
        log.exception("❌ Неверный формат window_end (v2): %r", window_end_iso)
        return 0, 0, 0

    async with infra.pg_pool.acquire() as conn:
        # общий объём закрытых сделок из шапки отчёта 7d
        closed_total = await conn.fetchval(
            "SELECT closed_total FROM oracle_report_stat WHERE id = $1",
            report_id
        )
        closed_total = int(closed_total or 0)
        if closed_total <= 0:
            # очищаем v2-срез
            async with conn.transaction():
                await conn.execute("DELETE FROM oracle_pack_whitelist WHERE strategy_id = $1 AND version = 'v2'", strategy_id)
            log.debug("ℹ️ PACK-lists v2: closed_total=0 (sid=%s, report=%s) — v2 очищен", strategy_id, report_id)
            return 0, 0, 0

        # порог по доле
        threshold = float(closed_total) * float(WL_V2_MIN_SHARE)

        # все агрегаты текущего отчёта 7d (без фильтров sense/conf)
        cand_rows = await conn.fetch(
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
              a.trades_total  AS trades_total,
              a.winrate       AS winrate,
              a.confidence    AS confidence
            FROM oracle_pack_aggregated_stat a
            WHERE a.report_id = $1
              AND a.time_frame = '7d'
              AND a.strategy_id = $2
            """,
            report_id, strategy_id
        )

        # фильтр по массе и классификация WL/BL по winrate
        to_insert = []
        rows_wl = 0
        rows_bl = 0
        for r in cand_rows:
            n = float(r["trades_total"] or 0.0)
            if n <= threshold:
                continue

            wr = float(r["winrate"] or 0.0)
            if wr >= WR_WL_MIN:
                list_tag = "whitelist"; rows_wl += 1
            elif wr < WR_BL_MAX:
                list_tag = "blacklist"; rows_bl += 1
            else:
                continue

            to_insert.append((
                int(r["aggregated_id"]),
                int(r["strategy_id"]),
                str(r["direction"]),
                str(r["timeframe"]),
                str(r["pack_base"]),
                str(r["agg_type"]),
                str(r["agg_key"]),
                str(r["agg_value"]),
                float(wr),
                float(r["confidence"] or 0.0),
                list_tag,
            ))

        # атомарно перестраиваем v2-срез
        async with conn.transaction():
            await conn.execute("DELETE FROM oracle_pack_whitelist WHERE strategy_id = $1 AND version = 'v2'", strategy_id)
            if to_insert:
                await conn.executemany(
                    """
                    INSERT INTO oracle_pack_whitelist (
                        aggregated_id, strategy_id, direction, timeframe,
                        pack_base, agg_type, agg_key, agg_value,
                        winrate, confidence, list, version
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'v2'
                    )
                    """,
                    to_insert
                )

        rows_total = len(to_insert)
        log.debug("✅ PACK-lists v2 обновлён (7d): sid=%s report_id=%s rows_total=%d wl=%d bl=%d (threshold=%.4f of %d)",
                  strategy_id, report_id, rows_total, rows_wl, rows_bl, threshold, closed_total)
        return rows_total, rows_wl, rows_bl