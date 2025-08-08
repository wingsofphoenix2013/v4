# snapshot_aggregator_worker.py

import asyncio
import logging
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
import json

import infra

# 🔸 Логгер
log = logging.getLogger("SNAPSHOT_AGGREGATOR")


# 🔸 Округление decimal с заданной точностью
def quantize(value: Decimal, places: int) -> Decimal:
    return value.quantize(Decimal(f"1e-{places}"), rounding=ROUND_HALF_UP)


# 🔸 Основная точка входа воркера
async def run_snapshot_aggregator_worker():
    try:
        await process_batch()
    except Exception:
        log.exception("Ошибка в snapshot_aggregator_worker")

# 🔸 Обработка одной порции необработанных логов
async def process_batch(batch_size: int = 200):
    async with infra.pg_pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                """
                SELECT * FROM emasnapshot_position_log
                WHERE aggregated_at IS NULL
                ORDER BY position_id
                LIMIT $1
                FOR UPDATE SKIP LOCKED
                """,
                batch_size
            )

            if not rows:
                log.debug("Нет новых строк для агрегации")
                return

            now = datetime.utcnow()
            snapshot_stats = {}  # ключ: (tf, strategy_id, direction, emasnapshot_dict_id)
            pattern_stats = {}   # ключ: (tf, strategy_id, direction, pattern_id)
            strategies_by_table = {
                "positions_emasnapshot_m5_stat": set(),
                "positions_emasnapshot_m15_stat": set(),
                "positions_emasnapshot_h1_stat": set(),
                "positions_emapattern_m5_stat": set(),
                "positions_emapattern_m15_stat": set(),
                "positions_emapattern_h1_stat": set(),
            }

            rsi_targets = set()         # (tf, strategy_id, emasnapshot_dict_id)
            position_keys_for_rsi = []  # (position_id, tf, strategy_id, emasnapshot_dict_id)

            for r in rows:
                tf = r["tf"]
                sid = r["strategy_id"]
                dir_ = r["direction"]
                snap_id = r["emasnapshot_dict_id"]
                pattern_id = r["pattern_id"]
                pnl = Decimal(r["pnl"])

                key_s = (tf, sid, dir_, snap_id)
                key_p = (tf, sid, dir_, pattern_id)

                # Агрегация по снапшоту
                agg = snapshot_stats.setdefault(key_s, {
                    "num_trades": 0,
                    "num_wins": 0,
                    "num_losses": 0,
                    "total_pnl": Decimal(0)
                })
                agg["num_trades"] += 1
                agg["total_pnl"] += pnl
                if pnl > 0:
                    agg["num_wins"] += 1
                elif pnl < 0:
                    agg["num_losses"] += 1

                # Агрегация по паттерну (если есть)
                if pattern_id is not None:
                    agg_p = pattern_stats.setdefault(key_p, {
                        "num_trades": 0,
                        "num_wins": 0,
                        "num_losses": 0,
                        "total_pnl": Decimal(0)
                    })
                    agg_p["num_trades"] += 1
                    agg_p["total_pnl"] += pnl
                    if pnl > 0:
                        agg_p["num_wins"] += 1
                    elif pnl < 0:
                        agg_p["num_losses"] += 1

                # Кандидаты для RSI-анализа
                rsi_targets.add((tf, sid, snap_id))
                position_keys_for_rsi.append((r["position_id"], tf, sid, snap_id))

            # Обновление агрегатов по снапшотам
            for (tf, sid, dir_, snap_id), data in snapshot_stats.items():
                table = f"positions_emasnapshot_{tf}_stat"
                strategies_by_table[table].add(sid)
                await upsert_aggregation(conn, table, sid, dir_, snap_id, data)

            # Обновление агрегатов по паттернам
            for (tf, sid, dir_, pattern_id), data in pattern_stats.items():
                table = f"positions_emapattern_{tf}_stat"
                strategies_by_table[table].add(sid)
                await upsert_aggregation(conn, table, sid, dir_, pattern_id, data, is_pattern=True)

            # Обновляем статус строк как агрегированных
            await conn.executemany(
                """
                UPDATE emasnapshot_position_log
                SET aggregated_at = $3
                WHERE position_id = $1 AND tf = $2
                """,
                [(pid, tf, now) for pid, tf, _, _ in position_keys_for_rsi]
            )

            # Публикация сообщений в Redis Stream
            for table_name, strategy_ids in strategies_by_table.items():
                if strategy_ids:
                    await infra.redis_client.xadd(
                        "emasnapshot:ratings:commands",
                        {
                            "table": table_name,
                            "strategies": json.dumps(sorted(strategy_ids))
                        }
                    )
                    log.debug(f"Redis XADD → {table_name}: стратегии {sorted(strategy_ids)}")

            log.debug(f"Агрегировано строк: {len(rows)}")

            # 🔹 Фильтрация по стратегиям с rsi_snapshot_check = true
            allowed_strategies = {
                r["id"]
                for r in await conn.fetch(
                    "SELECT id FROM strategies_v4 WHERE rsi_snapshot_check = true"
                )
            }
            rsi_targets = {
                t for t in rsi_targets if t[1] in allowed_strategies
            }

            # 🔹 Если есть что анализировать по RSI
            rsi_results = []
            if rsi_targets:
                # Оставляем только позиции из батча и нужных стратегий/снапшотов
                filtered_positions = [
                    (pid, tf) for pid, tf, sid, snap_id in position_keys_for_rsi
                    if (tf, sid, snap_id) in rsi_targets
                ]

                if filtered_positions:
                    position_ids = [pid for pid, tf in filtered_positions]
                    tfs = [tf for pid, tf in filtered_positions]

                    rsi_data = await conn.fetch(
                        """
                        SELECT el.tf,
                               el.strategy_id,
                               el.emasnapshot_dict_id,
                               pis.value AS rsi_value,
                               el.pnl
                        FROM emasnapshot_position_log el
                        JOIN positions_v4 p
                          ON p.id = el.position_id
                        JOIN position_ind_stat_v4 pis
                          ON pis.position_uid = p.position_uid
                         AND pis.param_name = 'rsi14'
                         AND pis.timeframe = el.tf
                        WHERE (el.position_id, el.tf) IN (
                            SELECT UNNEST($1::int[]), UNNEST($2::text[])
                        )
                        """,
                        position_ids, tfs
                    )

                    # Группировка по tf, strategy_id, snap_id, bucket
                    stats = {}
                    for rec in rsi_data:
                        tf = rec["tf"]
                        sid = rec["strategy_id"]
                        snap_id = rec["emasnapshot_dict_id"]
                        rsi_val = rec["rsi_value"]
                        pnl = Decimal(rec["pnl"])
                        bucket = int(rsi_val // 5) * 5

                        key = (tf, sid, snap_id, bucket)
                        agg = stats.setdefault(key, {"num": 0, "wins": 0})
                        agg["num"] += 1
                        if pnl > 0:
                            agg["wins"] += 1

                    # Определение allow/reject
                    for (tf, sid, snap_id, bucket), agg in stats.items():
                        num = agg["num"]
                        if num == 0:
                            continue
                        winrate = agg["wins"] / num
                        verdict = "allow" if winrate > 0.5 else "reject"
                        rsi_results.append((tf, sid, snap_id, bucket, verdict))

    # 🔹 Запись в Redis (уже после коммита транзакции)
    if rsi_results:
        for tf, sid, snap_id, bucket, verdict in rsi_results:
            key = f"emarsicheck:{tf}:{sid}:{snap_id}:{bucket}"
            await infra.redis_client.set(key, verdict)
            log.debug(f"RSI-check → {key} = {verdict}")
            
# 🔸 Выполнение UPSERT в таблицу агрегации
async def upsert_aggregation(conn, table: str, strategy_id: int, direction: str, ref_id: int, data: dict, is_pattern=False):
    num_trades = data["num_trades"]
    num_wins = data["num_wins"]
    num_losses = data["num_losses"]
    total_pnl = data["total_pnl"]

    avg_pnl = quantize(total_pnl / num_trades, 4) if num_trades > 0 else Decimal("0.0000")
    winrate = quantize(Decimal(num_wins) / num_trades, 4) if num_trades > 0 else Decimal("0.0000")
    base_rating = quantize(winrate * avg_pnl, 6)

    col = "pattern_id" if is_pattern else "emasnapshot_dict_id"

    await conn.execute(
        f"""
        INSERT INTO {table} (
            strategy_id, direction, {col}, num_trades, num_wins, num_losses,
            total_pnl, avg_pnl, winrate, base_rating, last_updated
        ) VALUES (
            $1, $2, $3, $4, $5, $6,
            $7, $8, $9, $10, now()
        )
        ON CONFLICT (strategy_id, direction, {col}) DO UPDATE
        SET
            num_trades = {table}.num_trades + $4,
            num_wins = {table}.num_wins + $5,
            num_losses = {table}.num_losses + $6,
            total_pnl = {table}.total_pnl + $7,
            avg_pnl = ROUND((({table}.total_pnl + $7)::numeric / ({table}.num_trades + $4)), 4),
            winrate = ROUND((({table}.num_wins + $5)::numeric / ({table}.num_trades + $4)), 4),
            base_rating = ROUND(
                (({table}.num_wins + $5)::numeric / ({table}.num_trades + $4)) *
                (({table}.total_pnl + $7)::numeric / ({table}.num_trades + $4)), 6
            ),
            last_updated = now()
        """,
        strategy_id, direction, ref_id, num_trades, num_wins, num_losses,
        total_pnl, avg_pnl, winrate, base_rating
    )

# 🔹 Периодический полный пересчёт RSI по всем стратегиям с флагом rsi_snapshot_check
async def rsi_full_refresh():
    try:
        async with infra.pg_pool.acquire() as conn:
            strategies = await conn.fetch(
                "SELECT id FROM strategies_v4 WHERE rsi_snapshot_check = true"
            )
            strategy_ids = [r["id"] for r in strategies]
            if not strategy_ids:
                log.info("RSI Full Refresh → нет стратегий с rsi_snapshot_check = true")
                return

            log.info(f"RSI Full Refresh → обрабатываем стратегии: {strategy_ids}")

            rsi_data = await conn.fetch(
                """
                SELECT el.tf,
                       el.strategy_id,
                       el.emasnapshot_dict_id,
                       pis.value AS rsi_value,
                       el.pnl
                FROM emasnapshot_position_log el
                JOIN positions_v4 p
                  ON p.id = el.position_id
                JOIN position_ind_stat_v4 pis
                  ON pis.position_uid = p.position_uid
                 AND pis.param_name = 'rsi14'
                 AND pis.timeframe = el.tf
                WHERE el.strategy_id = ANY($1)
                """,
                strategy_ids
            )

            if not rsi_data:
                log.info("RSI Full Refresh → нет данных для обработки")
                return

            # Группировка по tf, strategy_id, snap_id, bucket
            stats = {}
            for rec in rsi_data:
                tf = rec["tf"]
                sid = rec["strategy_id"]
                snap_id = rec["emasnapshot_dict_id"]
                rsi_val = rec["rsi_value"]
                pnl = Decimal(rec["pnl"])
                bucket = int(rsi_val // 5) * 5

                key = (tf, sid, snap_id, bucket)
                agg = stats.setdefault(key, {"num": 0, "wins": 0})
                agg["num"] += 1
                if pnl > 0:
                    agg["wins"] += 1

            # Запись в Redis
            for (tf, sid, snap_id, bucket), agg in stats.items():
                if agg["num"] == 0:
                    continue
                winrate = agg["wins"] / agg["num"]
                verdict = "allow" if winrate > 0.5 else "reject"
                redis_key = f"emarsicheck:{tf}:{sid}:{snap_id}:{bucket}"
                await infra.redis_client.set(redis_key, verdict)
                log.debug(f"RSI Full Refresh → {redis_key} = {verdict}")

            log.info(f"RSI Full Refresh → завершено, сформировано ключей: {len(stats)}")

    except Exception:
        log.exception("RSI Full Refresh → ошибка при обработке")