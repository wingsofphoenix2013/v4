# oracle_bt_audit.py — воркер BT-audit: проверка соответствия WL/BL v3 фактическим данным по winner-порогам (без записи в БД — только логи)

# 🔸 Импорты
import asyncio
import logging
import json
from typing import Dict, List, Tuple, Set
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

import infra

# 🔸 Логгер
log = logging.getLogger("ORACLE_BT_AUDIT")

# 🔸 Стримы (слушаем готовность v3)
MW_WL_READY_STREAM    = "oracle:mw_whitelist:reports_ready"    # payload: {strategy_id, report_id, time_frame, version, ...}
PACK_WL_READY_STREAM  = "oracle:pack_lists:reports_ready"      # payload: {strategy_id, report_id, time_frame, version, ...}

AUDIT_CONSUMER_GROUP  = "oracle_bt_audit_group"
AUDIT_CONSUMER_NAME   = "oracle_bt_audit_worker"

# 🔸 Параллелизм
MAX_CONCURRENT_AUDITS = 2

# 🔸 Общие пороги аудита (совпадают с backtest)
ROW_MIN_SHARE = 0.03     # минимальная масса для агрегатной строки (доля от всех сделок стратегии за 7d)
WR_BL_MAX     = Decimal("0.5000")  # порог для BL v3 (PACK), WinRate < 0.50

# 🔸 Квантайзер NUMERIC(6,4)
def _n4d(x) -> Decimal:
    try:
        return Decimal(str(x)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    except Exception:
        return Decimal("0.0000")


# 🔸 Публичная точка входа воркера
async def run_oracle_bt_audit():
    # условия достаточности
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск BT-AUDIT: PG/Redis не инициализированы")
        return

    # идемпотентное создание CG для обоих стримов
    for stream in (MW_WL_READY_STREAM, PACK_WL_READY_STREAM):
        try:
            await infra.redis_client.xgroup_create(
                name=stream, groupname=AUDIT_CONSUMER_GROUP, id="$", mkstream=True
            )
            log.debug("📡 Создана группа потребителей BT-AUDIT для стрима: %s", stream)
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                log.exception("❌ Ошибка инициализации CG BT-AUDIT для %s", stream)
                return

    sem = asyncio.Semaphore(MAX_CONCURRENT_AUDITS)
    log.debug("🚀 Старт воркера BT-AUDIT (parallel=%d)", MAX_CONCURRENT_AUDITS)

    # основной цикл чтения из обоих стримов
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=AUDIT_CONSUMER_GROUP,
                consumername=AUDIT_CONSUMER_NAME,
                streams={MW_WL_READY_STREAM: ">", PACK_WL_READY_STREAM: ">"},
                count=64,
                block=30_000,
            )
            if not resp:
                continue

            tasks: List[asyncio.Task] = []
            to_ack: List[Tuple[str, str]] = []
            seen: Set[Tuple[str, int, int]] = set()  # (src, sid, rid) — дедуп в батче

            for stream_name, msgs in resp:
                for msg_id, fields in msgs:
                    to_ack.append((stream_name, msg_id))
                    try:
                        payload = json.loads(fields.get("data", "{}") or "{}")
                    except Exception:
                        payload = {}

                    # фильтруем только v3 и 7d
                    version = str(payload.get("version", "")).strip().lower()
                    time_frame = str(payload.get("time_frame", "")).strip()
                    if version != "v3" or time_frame != "7d":
                        continue

                    try:
                        sid = int(payload.get("strategy_id", 0) or 0)
                        rid = int(payload.get("report_id", 0) or 0)
                        window_end = payload.get("window_end")
                    except Exception:
                        sid = 0; rid = 0; window_end = None

                    if not (sid and rid and window_end):
                        continue

                    src = "MW" if stream_name == MW_WL_READY_STREAM else "PACK"
                    key = (src, sid, rid)
                    if key in seen:
                        continue
                    seen.add(key)

                    tasks.append(asyncio.create_task(_guarded_audit(sem, src, sid, rid, window_end)))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=False)

            # ACK сообщений
            for stream_name, msg_id in to_ack:
                try:
                    await infra.redis_client.xack(stream_name, AUDIT_CONSUMER_GROUP, msg_id)
                except Exception:
                    log.exception("⚠️ Ошибка ACK в BT-AUDIT для стрима %s", stream_name)

        except asyncio.CancelledError:
            log.debug("⏹️ BT-AUDIT остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка цикла BT-AUDIT — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Гард на семафор и исключения
async def _guarded_audit(sem: asyncio.Semaphore, src: str, strategy_id: int, report_id: int, window_end_iso: str):
    async with sem:
        try:
            if src == "MW":
                await _audit_mw(strategy_id, report_id, window_end_iso)
            else:
                await _audit_pack(strategy_id, report_id, window_end_iso)
        except Exception:
            log.exception("❌ BT-AUDIT сбой: src=%s sid=%s rid=%s", src, strategy_id, report_id)


# 🔸 Аудит MW v3 (только WL)
async def _audit_mw(strategy_id: int, report_id: int, window_end_iso: str):
    async with infra.pg_pool.acquire() as conn:
        # порог массы на строку
        closed_total = await conn.fetchval("SELECT closed_total FROM oracle_report_stat WHERE id=$1", int(report_id))
        closed_total = int(closed_total or 0)
        row_min_trades = max(1, int((ROW_MIN_SHARE * closed_total) + 0.9999))  # ceil

        # winners по report_id
        bt_run_ids = [int(r["id"]) for r in await conn.fetch("SELECT id FROM oracle_mw_bt_run WHERE report_id=$1", int(report_id))]
        if not bt_run_ids:
            log.debug("ℹ️ BT_AUDIT MW: нет bt_run для sid=%s rid=%s — пропуск", strategy_id, report_id)
            return
        winners = await conn.fetch(
            """
            SELECT direction, timeframe, agg_type, agg_base, wr_min, conf_min
            FROM oracle_mw_bt_winner
            WHERE bt_run_id = ANY($1::bigint[]) AND strategy_id = $2
            """,
            bt_run_ids, int(strategy_id)
        )
        if not winners:
            log.debug("ℹ️ BT_AUDIT MW: winners=0 (sid=%s rid=%s) — WL v3 должен быть пуст по всем блокам", strategy_id, report_id)
            return

        total_blocks = 0
        total_missing = 0
        total_extra = 0

        for w in winners:
            direction  = str(w["direction"])
            timeframe  = str(w["timeframe"])
            agg_type   = str(w["agg_type"])
            agg_base   = str(w["agg_base"])
            wr_min     = _n4d(w["wr_min"])
            conf_min   = _n4d(w["conf_min"])

            # ожидаемые aggregated_id по winner-порогам
            expected = await conn.fetch(
                """
                SELECT a.id AS aggregated_id
                FROM oracle_mw_aggregated_stat a
                WHERE a.report_id   = $1
                  AND a.strategy_id = $2
                  AND a.direction   = $3
                  AND a.timeframe   = $4
                  AND a.agg_type    = $5
                  AND a.agg_base    = $6
                  AND a.trades_total >= $7
                  AND a.winrate     >= $8
                  AND a.confidence  >= $9
                """,
                int(report_id), int(strategy_id),
                direction, timeframe, agg_type, agg_base,
                int(row_min_trades), wr_min, conf_min
            )
            expected_ids = {int(r["aggregated_id"]) for r in expected}

            # фактические WL v3 для этого блока
            actual = await conn.fetch(
                """
                SELECT aggregated_id
                FROM oracle_mw_whitelist
                WHERE strategy_id = $1
                  AND version     = 'v3'
                  AND direction   = $2
                  AND timeframe   = $3
                  AND agg_base    = $4
                """,
                int(strategy_id), direction, timeframe, agg_base
            )
            actual_ids = {int(r["aggregated_id"]) for r in actual}

            missing_ids = sorted(expected_ids - actual_ids)
            extra_ids   = sorted(actual_ids - expected_ids)

            if missing_ids or extra_ids:
                total_missing += len(missing_ids)
                total_extra   += len(extra_ids)
                log.info(
                    "❌ BT_AUDIT MISMATCH (MW): sid=%s rid=%s dir=%s tf=%s base=%s type=%s wr_min=%s conf_min=%s "
                    "expected=%d actual=%d missing=%s extra=%s row_min=%d",
                    strategy_id, report_id, direction, timeframe, agg_base, agg_type,
                    wr_min, conf_min, len(expected_ids), len(actual_ids), missing_ids, extra_ids, row_min_trades
                )
            else:
                log.debug(
                    "✅ BT_AUDIT OK (MW): sid=%s rid=%s dir=%s tf=%s base=%s type=%s items=%d",
                    strategy_id, report_id, direction, timeframe, agg_base, agg_type, len(expected_ids)
                )

            total_blocks += 1

        log.debug(
            "📊 BT_AUDIT SUMMARY (MW): sid=%s rid=%s blocks=%d missing=%d extra=%d row_min=%d",
            strategy_id, report_id, total_blocks, total_missing, total_extra, row_min_trades
        )


# 🔸 Аудит PACK v3 (WL и BL)
async def _audit_pack(strategy_id: int, report_id: int, window_end_iso: str):
    async with infra.pg_pool.acquire() as conn:
        # порог массы на строку
        closed_total = await conn.fetchval("SELECT closed_total FROM oracle_report_stat WHERE id=$1", int(report_id))
        closed_total = int(closed_total or 0)
        row_min_trades = max(1, int((ROW_MIN_SHARE * closed_total) + 0.9999))  # ceil

        # winners по report_id
        bt_run_ids = [int(r["id"]) for r in await conn.fetch("SELECT id FROM oracle_pack_bt_run WHERE report_id=$1", int(report_id))]
        if not bt_run_ids:
            log.debug("ℹ️ BT_AUDIT PACK: нет bt_run для sid=%s rid=%s — пропуск", strategy_id, report_id)
            return
        winners = await conn.fetch(
            """
            SELECT direction, timeframe, pack_base, agg_type, agg_key, wr_min, conf_min
            FROM oracle_pack_bt_winner
            WHERE bt_run_id = ANY($1::bigint[]) AND strategy_id = $2
            """,
            bt_run_ids, int(strategy_id)
        )
        if not winners:
            log.debug("ℹ️ BT_AUDIT PACK: winners=0 (sid=%s rid=%s) — WL/BL v3 должен быть пуст по всем блокам", strategy_id, report_id)
            return

        total_blocks = 0
        total_wl_missing = total_wl_extra = 0
        total_bl_missing = total_bl_extra = 0

        for w in winners:
            direction  = str(w["direction"])
            timeframe  = str(w["timeframe"])
            pack_base  = str(w["pack_base"])
            agg_type   = str(w["agg_type"])
            agg_key    = str(w["agg_key"])
            wr_min     = _n4d(w["wr_min"])
            conf_min   = _n4d(w["conf_min"])

            # WL expected
            wl_expected = await conn.fetch(
                """
                SELECT a.id AS aggregated_id
                FROM oracle_pack_aggregated_stat a
                WHERE a.report_id   = $1
                  AND a.strategy_id = $2
                  AND a.direction   = $3
                  AND a.timeframe   = $4
                  AND a.pack_base   = $5
                  AND a.agg_type    = $6
                  AND a.agg_key     = $7
                  AND a.trades_total >= $8
                  AND a.winrate     >= $9
                  AND a.confidence  >= $10
                """,
                int(report_id), int(strategy_id),
                direction, timeframe, pack_base, agg_type, agg_key,
                int(row_min_trades), wr_min, conf_min
            )
            wl_expected_ids = {int(r["aggregated_id"]) for r in wl_expected}

            wl_actual = await conn.fetch(
                """
                SELECT aggregated_id
                FROM oracle_pack_whitelist
                WHERE strategy_id = $1 AND version='v3' AND list='whitelist'
                  AND direction=$2 AND timeframe=$3 AND pack_base=$4 AND agg_type=$5 AND agg_key=$6
                """,
                int(strategy_id), direction, timeframe, pack_base, agg_type, agg_key
            )
            wl_actual_ids = {int(r["aggregated_id"]) for r in wl_actual}

            wl_missing = sorted(wl_expected_ids - wl_actual_ids)
            wl_extra   = sorted(wl_actual_ids - wl_expected_ids)
            if wl_missing or wl_extra:
                total_wl_missing += len(wl_missing)
                total_wl_extra   += len(wl_extra)
                log.info(
                    "❌ BT_AUDIT MISMATCH (PACK WL): sid=%s rid=%s dir=%s tf=%s base=%s type=%s key=%s "
                    "wr_min=%s conf_min=%s expected=%d actual=%d missing=%s extra=%s row_min=%d",
                    strategy_id, report_id, direction, timeframe, pack_base, agg_type, agg_key,
                    wr_min, conf_min, len(wl_expected_ids), len(wl_actual_ids), wl_missing, wl_extra, row_min_trades
                )
            else:
                log.debug(
                    "✅ BT_AUDIT OK (PACK WL): sid=%s rid=%s dir=%s tf=%s base=%s type=%s key=%s items=%d",
                    strategy_id, report_id, direction, timeframe, pack_base, agg_type, agg_key, len(wl_expected_ids)
                )

            # BL expected (дополнение по победным порогам)
            bl_expected = await conn.fetch(
                """
                SELECT a.id AS aggregated_id
                FROM oracle_pack_aggregated_stat a
                WHERE a.report_id   = $1
                  AND a.strategy_id = $2
                  AND a.direction   = $3
                  AND a.timeframe   = $4
                  AND a.pack_base   = $5
                  AND a.agg_type    = $6
                  AND a.agg_key     = $7
                  AND a.trades_total >= $8
                  AND (a.winrate < $9 OR a.confidence < $10)
                  AND a.winrate < $11
                """,
                int(report_id), int(strategy_id),
                direction, timeframe, pack_base, agg_type, agg_key,
                int(row_min_trades), wr_min, conf_min, WR_BL_MAX
            )
            bl_expected_ids = {int(r["aggregated_id"]) for r in bl_expected}

            bl_actual = await conn.fetch(
                """
                SELECT aggregated_id
                FROM oracle_pack_whitelist
                WHERE strategy_id = $1 AND version='v3' AND list='blacklist'
                  AND direction=$2 AND timeframe=$3 AND pack_base=$4 AND agg_type=$5 AND agg_key=$6
                """,
                int(strategy_id), direction, timeframe, pack_base, agg_type, agg_key
            )
            bl_actual_ids = {int(r["aggregated_id"]) for r in bl_actual}

            bl_missing = sorted(bl_expected_ids - bl_actual_ids)
            bl_extra   = sorted(bl_actual_ids - bl_expected_ids)
            if bl_missing or bl_extra:
                total_bl_missing += len(bl_missing)
                total_bl_extra   += len(bl_extra)
                log.info(
                    "❌ BT_AUDIT MISMATCH (PACK BL): sid=%s rid=%s dir=%s tf=%s base=%s type=%s key=%s "
                    "wr_min=%s conf_min=%s expected=%d actual=%d missing=%s extra=%s row_min=%d",
                    strategy_id, report_id, direction, timeframe, pack_base, agg_type, agg_key,
                    wr_min, conf_min, len(bl_expected_ids), len(bl_actual_ids), bl_missing, bl_extra, row_min_trades
                )
            else:
                log.debug(
                    "✅ BT_AUDIT OK (PACK BL): sid=%s rid=%s dir=%s tf=%s base=%s type=%s key=%s items=%d",
                    strategy_id, report_id, direction, timeframe, pack_base, agg_type, agg_key, len(bl_expected_ids)
                )

            total_blocks += 1

        log.debug(
            "📊 BT_AUDIT SUMMARY (PACK): sid=%s rid=%s blocks=%d wl_missing=%d wl_extra=%d bl_missing=%d bl_extra=%d row_min=%d",
            strategy_id, report_id, total_blocks,
            total_wl_missing, total_wl_extra, total_bl_missing, total_bl_extra, row_min_trades
        )