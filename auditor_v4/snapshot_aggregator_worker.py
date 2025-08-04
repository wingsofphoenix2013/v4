# snapshot_aggregator_worker.py

import asyncio
import logging
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime

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
                log.info("Нет новых строк для агрегации")
                return

            now = datetime.utcnow()
            snapshot_stats = {}  # ключ: (tf, strategy_id, direction, emasnapshot_dict_id)
            pattern_stats = {}   # ключ: (tf, strategy_id, direction, pattern_id)

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

            # Обновление агрегатов по снапшотам
            for (tf, sid, dir_, snap_id), data in snapshot_stats.items():
                table = f"positions_emasnapshot_{tf}_stat"
                await upsert_aggregation(conn, table, sid, dir_, snap_id, data)

            # Обновление агрегатов по паттернам
            for (tf, sid, dir_, pattern_id), data in pattern_stats.items():
                table = f"positions_emapattern_{tf}_stat"
                await upsert_aggregation(conn, table, sid, dir_, pattern_id, data, is_pattern=True)

            # Обновляем статус строк как агрегированных
            ids = [(r["position_id"], r["tf"]) for r in rows]
            await conn.executemany(
                """
                UPDATE emasnapshot_position_log
                SET aggregated_at = $3
                WHERE position_id = $1 AND tf = $2
                """,
                [(pid, tf, now) for pid, tf in ids]
            )

            log.info(f"Агрегировано строк: {len(rows)}")


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