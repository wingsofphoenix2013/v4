# oracle_bt_audit.py — воркер BT-audit: проверка соответствия WL/BL v3 фактическим данным по winner-порогам и в pass-through (без записи в БД — только логи)

# 🔸 Импорты
import asyncio
import logging
import json
from typing import List, Tuple, Set
from decimal import Decimal, ROUND_HALF_UP

import infra

# 🔸 Логгер
log = logging.getLogger("ORACLE_BT_AUDIT")

# 🔸 Стримы (слушаем готовность v3)
MW_WL_READY_STREAM   = "oracle:mw_whitelist:reports_ready"   # payload: {strategy_id, report_id, time_frame, version, ...}
PACK_WL_READY_STREAM = "oracle:pack_lists:reports_ready"     # payload: {strategy_id, report_id, time_frame, version, ...}

AUDIT_CONSUMER_GROUP = "oracle_bt_audit_group"
AUDIT_CONSUMER_NAME  = "oracle_bt_audit_worker"

# 🔸 Параллелизм
MAX_CONCURRENT_AUDITS = 2

# 🔸 Пороги (как в backtest)
ROW_MIN_SHARE = 0.03                       # минимальная масса строки: ceil(3% * closed_total_7d)
WR_BL_MAX     = Decimal("0.5000")          # нижний порог WR для включения в BL v3 (wr < 0.50)

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

    # идемпотентно создаём CG для обоих стримов
    for stream in (MW_WL_READY_STREAM, PACK_WL_READY_STREAM):
        try:
            await infra.redis_client.xgroup_create(
                name=stream, groupname=AUDIT_CONSUMER_GROUP, id="$", mkstream=True
            )
            log.debug("📡 Создана consumer group BT-AUDIT для стрима: %s", stream)
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
            seen: Set[Tuple[str, int, int]] = set()  # (src, sid, rid)

            for stream_name, msgs in resp:
                for msg_id, fields in msgs:
                    to_ack.append((stream_name, msg_id))
                    try:
                        payload = json.loads(fields.get("data", "{}") or "{}")
                    except Exception:
                        payload = {}

                    # фильтруем только v3 и 7d
                    version    = str(payload.get("version", "")).strip().lower()
                    time_frame = str(payload.get("time_frame", "")).strip().lower()
                    if version != "v3" or time_frame != "7d":
                        continue

                    try:
                        sid = int(payload.get("strategy_id", 0) or 0)
                        rid = int(payload.get("report_id", 0) or 0)
                    except Exception:
                        sid = 0; rid = 0

                    if not (sid and rid):
                        continue

                    src = "MW" if stream_name == MW_WL_READY_STREAM else "PACK"
                    key = (src, sid, rid)
                    if key in seen:
                        continue
                    seen.add(key)

                    tasks.append(asyncio.create_task(_guarded_audit(sem, src, sid, rid)))

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
async def _guarded_audit(sem: asyncio.Semaphore, src: str, strategy_id: int, report_id: int):
    async with sem:
        try:
            if src == "MW":
                await _audit_mw(strategy_id, report_id)
            else:
                await _audit_pack(strategy_id, report_id)
        except Exception:
            log.exception("❌ BT-AUDIT сбой: src=%s sid=%s rid=%s", src, strategy_id, report_id)


# 🔸 Аудит MW v3 (WL и BL) + pass-through
async def _audit_mw(strategy_id: int, report_id: int):
    async with infra.pg_pool.acquire() as conn:
        # порог массы на строку
        closed_total = await conn.fetchval(
            "SELECT COALESCE(closed_total,0) FROM oracle_report_stat WHERE id=$1",
            int(report_id)
        )
        row_min_trades = max(1, int((ROW_MIN_SHARE * int(closed_total)) + 0.9999))  # ceil

        # winners по report_id
        bt_run_ids = [int(r["id"]) for r in await conn.fetch(
            "SELECT id FROM oracle_mw_bt_run WHERE report_id=$1", int(report_id)
        )]

        winners = await conn.fetch(
            """
            SELECT direction, timeframe, agg_type, agg_base, wr_min, conf_min
            FROM oracle_mw_bt_winner
            WHERE bt_run_id = ANY($1::bigint[]) AND strategy_id = $2
            """,
            bt_run_ids, int(strategy_id)
        ) if bt_run_ids else []

        total_wl_missing = total_wl_extra = 0
        total_bl_missing = total_bl_extra = 0
        blocks = 0

        # набор winner-блоков
        winner_blocks: Set[Tuple[str, str, str, str]] = set()

        # — проверка winner-блоков
        for w in winners:
            direction = str(w["direction"])
            timeframe = str(w["timeframe"])
            agg_type  = str(w["agg_type"])
            agg_base  = str(w["agg_base"])
            wr_min    = _n4d(w["wr_min"])
            conf_min  = _n4d(w["conf_min"])

            winner_blocks.add((direction, timeframe, agg_type, agg_base))

            # WL expected (по порогам)
            wl_expected = await conn.fetch(
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
            wl_expected_ids = {int(r["aggregated_id"]) for r in wl_expected}

            wl_actual = await conn.fetch(
                """
                SELECT aggregated_id
                FROM oracle_mw_whitelist
                WHERE strategy_id = $1
                  AND version     = 'v3'
                  AND list        = 'whitelist'
                  AND direction   = $2
                  AND timeframe   = $3
                  AND agg_base    = $4
                """,
                int(strategy_id), direction, timeframe, agg_base
            )
            wl_actual_ids = {int(r["aggregated_id"]) for r in wl_actual}

            wl_missing = sorted(wl_expected_ids - wl_actual_ids)
            wl_extra   = sorted(wl_actual_ids - wl_expected_ids)
            if wl_missing or wl_extra:
                total_wl_missing += len(wl_missing)
                total_wl_extra   += len(wl_extra)
                log.info(
                    "❌ BT_AUDIT MISMATCH (MW WL): sid=%s rid=%s dir=%s tf=%s base=%s type=%s wr_min=%s conf_min=%s "
                    "expected=%d actual=%d missing=%s extra=%s row_min=%d",
                    strategy_id, report_id, direction, timeframe, agg_base, agg_type,
                    wr_min, conf_min, len(wl_expected_ids), len(wl_actual_ids), wl_missing, wl_extra, row_min_trades
                )
            else:
                log.debug(
                    "✅ BT_AUDIT OK (MW WL): sid=%s rid=%s dir=%s tf=%s base=%s type=%s items=%d",
                    strategy_id, report_id, direction, timeframe, agg_base, agg_type, len(wl_expected_ids)
                )

            # BL expected (масса & (wr<wr_min OR conf<conf_min) & wr<0.5)
            bl_expected = await conn.fetch(
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
                  AND (a.winrate < $8 OR a.confidence < $9)
                  AND a.winrate < $10
                """,
                int(report_id), int(strategy_id),
                direction, timeframe, agg_type, agg_base,
                int(row_min_trades), wr_min, conf_min, WR_BL_MAX
            )
            bl_expected_ids = {int(r["aggregated_id"]) for r in bl_expected}

            bl_actual = await conn.fetch(
                """
                SELECT aggregated_id
                FROM oracle_mw_whitelist
                WHERE strategy_id = $1
                  AND version     = 'v3'
                  AND list        = 'blacklist'
                  AND direction   = $2
                  AND timeframe   = $3
                  AND agg_base    = $4
                """,
                int(strategy_id), direction, timeframe, agg_base
            )
            bl_actual_ids = {int(r["aggregated_id"]) for r in bl_actual}

            bl_missing = sorted(bl_expected_ids - bl_actual_ids)
            bl_extra   = sorted(bl_actual_ids - bl_expected_ids)
            if bl_missing or bl_extra:
                total_bl_missing += len(bl_missing)
                total_bl_extra   += len(bl_extra)
                log.info(
                    "❌ BT_AUDIT MISMATCH (MW BL): sid=%s rid=%s dir=%s tf=%s base=%s type=%s wr_min=%s conf_min=%s "
                    "expected=%d actual=%d missing=%s extra=%s row_min=%d",
                    strategy_id, report_id, direction, timeframe, agg_base, agg_type,
                    wr_min, conf_min, len(bl_expected_ids), len(bl_actual_ids), bl_missing, bl_extra, row_min_trades
                )
            else:
                log.debug(
                    "✅ BT_AUDIT OK (MW BL): sid=%s rid=%s dir=%s tf=%s base=%s type=%s items=%d",
                    strategy_id, report_id, direction, timeframe, agg_base, agg_type, len(bl_expected_ids)
                )

            blocks += 1

        # 🔸 PASS-THROUGH (MW): блоки без winner, но присутствующие в v3 → WL: wr≥0.5; BL: wr<0.5 (оба с порогом массы)
        v3_blocks_rows = await conn.fetch(
            """
            SELECT DISTINCT a.direction, a.timeframe, a.agg_type, a.agg_base
            FROM oracle_mw_whitelist w
            JOIN oracle_mw_aggregated_stat a ON a.id = w.aggregated_id
            WHERE w.strategy_id = $1 AND w.version = 'v3' AND a.report_id = $2
            """,
            int(strategy_id), int(report_id)
        )
        v3_blocks = {(r["direction"], r["timeframe"], r["agg_type"], r["agg_base"]) for r in v3_blocks_rows}
        pt_blocks = sorted(v3_blocks - {tuple(x) for x in winner_blocks})

        for (direction, timeframe, agg_type, agg_base) in pt_blocks:
            # WL expected (pass-through): mass & wr ≥ 0.50
            pt_wl_exp = await conn.fetch(
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
                  AND a.winrate >= $8
                """,
                int(report_id), int(strategy_id),
                direction, timeframe, agg_type, agg_base,
                int(row_min_trades), WR_BL_MAX
            )
            pt_wl_exp_ids = {int(r["aggregated_id"]) for r in pt_wl_exp}

            pt_wl_act = await conn.fetch(
                """
                SELECT w.aggregated_id
                FROM oracle_mw_whitelist w
                JOIN oracle_mw_aggregated_stat a ON a.id = w.aggregated_id
                WHERE w.strategy_id = $1 AND w.version='v3' AND w.list='whitelist'
                  AND a.report_id   = $2
                  AND a.direction   = $3
                  AND a.timeframe   = $4
                  AND a.agg_type    = $5
                  AND a.agg_base    = $6
                """,
                int(strategy_id), int(report_id),
                direction, timeframe, agg_type, agg_base
            )
            pt_wl_act_ids = {int(r["aggregated_id"]) for r in pt_wl_act}

            wl_missing = sorted(pt_wl_exp_ids - pt_wl_act_ids)
            wl_extra   = sorted(pt_wl_act_ids - pt_wl_exp_ids)
            if wl_missing or wl_extra:
                total_wl_missing += len(wl_missing)
                total_wl_extra   += len(wl_extra)
                log.info(
                    "❌ BT_AUDIT MISMATCH (MW WL PT): sid=%s rid=%s dir=%s tf=%s base=%s type=%s "
                    "expected=%d actual=%d missing=%s extra=%s row_min=%d",
                    strategy_id, report_id, direction, timeframe, agg_base, agg_type,
                    len(pt_wl_exp_ids), len(pt_wl_act_ids), wl_missing, wl_extra, row_min_trades
                )

            # BL expected (pass-through): mass & wr < 0.50
            pt_bl_exp = await conn.fetch(
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
                  AND a.winrate < $8
                """,
                int(report_id), int(strategy_id),
                direction, timeframe, agg_type, agg_base,
                int(row_min_trades), WR_BL_MAX
            )
            pt_bl_exp_ids = {int(r["aggregated_id"]) for r in pt_bl_exp}

            pt_bl_act = await conn.fetch(
                """
                SELECT w.aggregated_id
                FROM oracle_mw_whitelist w
                JOIN oracle_mw_aggregated_stat a ON a.id = w.aggregated_id
                WHERE w.strategy_id = $1 AND w.version='v3' AND w.list='blacklist'
                  AND a.report_id   = $2
                  AND a.direction   = $3
                  AND a.timeframe   = $4
                  AND a.agg_type    = $5
                  AND a.agg_base    = $6
                """,
                int(strategy_id), int(report_id),
                direction, timeframe, agg_type, agg_base
            )
            pt_bl_act_ids = {int(r["aggregated_id"]) for r in pt_bl_act}

            bl_missing = sorted(pt_bl_exp_ids - pt_bl_act_ids)
            bl_extra   = sorted(pt_bl_act_ids - pt_bl_exp_ids)
            if bl_missing or bl_extra:
                total_bl_missing += len(bl_missing)
                total_bl_extra   += len(bl_extra)
                log.info(
                    "❌ BT_AUDIT MISMATCH (MW BL PT): sid=%s rid=%s dir=%s tf=%s base=%s type=%s "
                    "expected=%d actual=%d missing=%s extra=%s row_min=%d",
                    strategy_id, report_id, direction, timeframe, agg_base, agg_type,
                    len(pt_bl_exp_ids), len(pt_bl_act_ids), bl_missing, bl_extra, row_min_trades
                )

        log.debug(
            "📊 BT_AUDIT SUMMARY (MW): sid=%s rid=%s blocks=%d wl_missing=%d wl_extra=%d bl_missing=%d bl_extra=%d row_min=%d",
            strategy_id, report_id, blocks, total_wl_missing, total_wl_extra, total_bl_missing, total_bl_extra, row_min_trades
        )


# 🔸 Аудит PACK v3 (WL и BL) + pass-through
async def _audit_pack(strategy_id: int, report_id: int):
    async with infra.pg_pool.acquire() as conn:
        # порог массы на строку
        closed_total = await conn.fetchval(
            "SELECT COALESCE(closed_total,0) FROM oracle_report_stat WHERE id=$1",
            int(report_id)
        )
        row_min_trades = max(1, int((ROW_MIN_SHARE * int(closed_total)) + 0.9999))  # ceil

        # winners по report_id
        bt_run_ids = [int(r["id"]) for r in await conn.fetch(
            "SELECT id FROM oracle_pack_bt_run WHERE report_id=$1", int(report_id)
        )]

        winners = await conn.fetch(
            """
            SELECT direction, timeframe, pack_base, agg_type, agg_key, wr_min, conf_min
            FROM oracle_pack_bt_winner
            WHERE bt_run_id = ANY($1::bigint[]) AND strategy_id = $2
            """,
            bt_run_ids, int(strategy_id)
        ) if bt_run_ids else []

        total_wl_missing = total_wl_extra = 0
        total_bl_missing = total_bl_extra = 0
        blocks = 0

        # набор winner-блоков
        winner_blocks: Set[Tuple[str, str, str, str, str]] = set()

        # — проверка winner-блоков
        for w in winners:
            direction = str(w["direction"])
            timeframe = str(w["timeframe"])
            pack_base = str(w["pack_base"])
            agg_type  = str(w["agg_type"])
            agg_key   = str(w["agg_key"])
            wr_min    = _n4d(w["wr_min"])
            conf_min  = _n4d(w["conf_min"])

            winner_blocks.add((direction, timeframe, pack_base, agg_type, agg_key))

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

            # BL expected
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
                    "✅ BT_AUDIT OK (PACK BL): sid=%s rid=%s dir=%s tf=%s base=%s type=%s items=%d",
                    strategy_id, report_id, direction, timeframe, pack_base, agg_type, len(bl_expected_ids)
                )

            blocks += 1

        # 🔸 PASS-THROUGH (PACK): блоки без winner, но присутствующие в v3 → WL: wr≥0.5; BL: wr<0.5
        v3_blocks_rows = await conn.fetch(
            """
            SELECT DISTINCT a.direction, a.timeframe, a.pack_base, a.agg_type, a.agg_key
            FROM oracle_pack_whitelist w
            JOIN oracle_pack_aggregated_stat a ON a.id = w.aggregated_id
            WHERE w.strategy_id = $1 AND w.version = 'v3' AND a.report_id = $2
            """,
            int(strategy_id), int(report_id)
        )
        v3_blocks = {(r["direction"], r["timeframe"], r["pack_base"], r["agg_type"], r["agg_key"]) for r in v3_blocks_rows}
        winner_blocks_pack = {(d, t, b, at, ak) for (d, t, b, at, ak) in v3_blocks if (d, t, b, at, ak) in v3_blocks}  # no-op, для читаемости
        pt_blocks = sorted(v3_blocks - {tuple(x) for x in winner_blocks_pack}.union(set()))  # v3 - winners

        for (direction, timeframe, pack_base, agg_type, agg_key) in pt_blocks:
            # WL expected (pass-through): mass & wr ≥ 0.50
            pt_wl_exp = await conn.fetch(
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
                  AND a.winrate >= $9
                """,
                int(report_id), int(strategy_id),
                direction, timeframe, pack_base, agg_type, agg_key,
                int(row_min_trades), WR_BL_MAX
            )
            pt_wl_exp_ids = {int(r["aggregated_id"]) for r in pt_wl_exp}

            pt_wl_act = await conn.fetch(
                """
                SELECT w.aggregated_id
                FROM oracle_pack_whitelist w
                JOIN oracle_pack_aggregated_stat a ON a.id = w.aggregated_id
                WHERE w.strategy_id = $1 AND w.version='v3' AND w.list='whitelist'
                  AND a.report_id   = $2
                  AND a.direction   = $3
                  AND a.timeframe   = $4
                  AND a.pack_base   = $5
                  AND a.agg_type    = $6
                  AND a.agg_key     = $7
                """,
                int(strategy_id), int(report_id),
                direction, timeframe, pack_base, agg_type, agg_key
            )
            pt_wl_act_ids = {int(r["aggregated_id"]) for r in pt_wl_act}

            wl_missing = sorted(pt_wl_exp_ids - pt_wl_act_ids)
            wl_extra   = sorted(pt_wl_act_ids - pt_wl_exp_ids)
            if wl_missing or wl_extra:
                total_wl_missing += len(wl_missing)
                total_wl_extra   += len(wl_extra)
                log.info(
                    "❌ BT_AUDIT MISMATCH (PACK WL PT): sid=%s rid=%s dir=%s tf=%s base=%s type=%s key=%s "
                    "expected=%d actual=%d missing=%s extra=%s row_min=%d",
                    strategy_id, report_id, direction, timeframe, pack_base, agg_type, agg_key,
                    len(pt_wl_exp_ids), len(pt_wl_act_ids), wl_missing, wl_extra, row_min_trades
                )

            # BL expected (pass-through): mass & wr < 0.50
            pt_bl_exp = await conn.fetch(
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
                  AND a.winrate < $9
                """,
                int(report_id), int(strategy_id),
                direction, timeframe, pack_base, agg_type, agg_key,
                int(row_min_trades), WR_BL_MAX
            )
            pt_bl_exp_ids = {int(r["aggregated_id"]) for r in pt_bl_exp}

            pt_bl_act = await conn.fetch(
                """
                SELECT w.aggregated_id
                FROM oracle_pack_whitelist w
                JOIN oracle_pack_aggregated_stat a ON a.id = w.aggregated_id
                WHERE w.strategy_id = $1 AND w.version='v3' AND w.list='blacklist'
                  AND a.report_id   = $2
                  AND a.direction   = $3
                  AND a.timeframe   = $4
                  AND a.pack_base   = $5
                  AND a.agg_type    = $6
                  AND a.agg_key     = $7
                """,
                int(strategy_id), int(report_id),
                direction, timeframe, pack_base, agg_type, agg_key
            )
            pt_bl_act_ids = {int(r["aggregated_id"]) for r in pt_bl_act}

            bl_missing = sorted(pt_bl_exp_ids - pt_bl_act_ids)
            bl_extra   = sorted(pt_bl_act_ids - pt_bl_exp_ids)
            if bl_missing or bl_extra:
                total_bl_missing += len(bl_missing)
                total_bl_extra   += len(bl_extra)
                log.info(
                    "❌ BT_AUDIT MISMATCH (PACK BL PT): sid=%s rid=%s dir=%s tf=%s base=%s type=%s key=%s "
                    "expected=%d actual=%d missing=%s extra=%s row_min=%d",
                    strategy_id, report_id, direction, timeframe, pack_base, agg_type, agg_key,
                    len(pt_bl_exp_ids), len(pt_bl_act_ids), bl_missing, bl_extra, row_min_trades
                )

        log.debug(
            "📊 BT_AUDIT SUMMARY (PACK): sid=%s rid=%s blocks=%d wl_missing=%d wl_extra=%d bl_missing=%d bl_extra=%d row_min=%d",
            strategy_id, report_id, blocks,
            total_wl_missing, total_wl_extra, total_bl_missing, total_bl_extra, row_min_trades
        )