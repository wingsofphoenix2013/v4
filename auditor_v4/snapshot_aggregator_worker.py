# 🔸 snapshot_aggregator_worker.py

import logging
from decimal import Decimal, ROUND_HALF_UP

import infra

log = logging.getLogger("SNAPSHOT_AGGREGATOR")

# 🔸 Утилита округления Decimal
def quantize_decimal(value: Decimal, precision: int) -> Decimal:
    return value.quantize(Decimal(f'1e-{precision}'), rounding=ROUND_HALF_UP)
    
# 🔸 Фоновый агрегатор по лог-таблице
async def run_snapshot_aggregator():
    try:
        pending = await infra.redis_client.get("emasnapshot:agg:pending")
        if pending != "1":
            return  # Нет работы

        # Пытаемся захватить Redis-блокировку
        locked = await infra.redis_client.set(
            "emasnapshot:agg:lock", "1", ex=5, nx=True
        )
        if not locked:
            return  # Уже кто-то агрегирует

        log.info("🚀 Начало агрегирования по лог-таблице")

        async with infra.pg_pool.acquire() as conn:

            # 🔹 Агрегация по emasnapshot_dict_id
            rows = await conn.fetch("""
                SELECT
                    strategy_id,
                    direction,
                    tf,
                    emasnapshot_dict_id,
                    COUNT(*) AS num_trades,
                    COUNT(*) FILTER (WHERE pnl > 0) AS num_wins,
                    COUNT(*) FILTER (WHERE pnl <= 0) AS num_losses,
                    SUM(pnl)::numeric(20,4) AS total_pnl
                FROM emasnapshot_position_log
                GROUP BY strategy_id, direction, tf, emasnapshot_dict_id
            """)

            for r in rows:
                table = f"positions_emasnapshot_{r['tf']}_stat"

                total_pnl = quantize_decimal(r["total_pnl"] or Decimal(0), 4)
                num_trades = r["num_trades"]
                num_wins = r["num_wins"]
                num_losses = r["num_losses"]
                avg_pnl = quantize_decimal(total_pnl / num_trades, 4)
                winrate = quantize_decimal(Decimal(num_wins) / num_trades, 4)
                base_rating = quantize_decimal(Decimal(0), 6)

                await conn.execute(f"""
                    INSERT INTO {table} (
                        strategy_id, direction, emasnapshot_dict_id,
                        num_trades, num_wins, num_losses,
                        total_pnl, avg_pnl, winrate, base_rating, last_updated
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
                    ON CONFLICT (strategy_id, direction, emasnapshot_dict_id)
                    DO UPDATE SET
                        num_trades = EXCLUDED.num_trades,
                        num_wins = EXCLUDED.num_wins,
                        num_losses = EXCLUDED.num_losses,
                        total_pnl = EXCLUDED.total_pnl,
                        avg_pnl = EXCLUDED.avg_pnl,
                        winrate = EXCLUDED.winrate,
                        base_rating = EXCLUDED.base_rating,
                        last_updated = NOW()
                """, r["strategy_id"], r["direction"], r["emasnapshot_dict_id"],
                     num_trades, num_wins, num_losses,
                     total_pnl, avg_pnl, winrate, base_rating)

            # 🔹 Агрегация по pattern_id
            rows = await conn.fetch("""
                SELECT
                    strategy_id,
                    direction,
                    tf,
                    pattern_id,
                    COUNT(*) AS num_trades,
                    COUNT(*) FILTER (WHERE pnl > 0) AS num_wins,
                    COUNT(*) FILTER (WHERE pnl <= 0) AS num_losses,
                    SUM(pnl)::numeric(20,4) AS total_pnl
                FROM emasnapshot_position_log
                WHERE pattern_id IS NOT NULL
                GROUP BY strategy_id, direction, tf, pattern_id
            """)

            for r in rows:
                table = f"positions_emapattern_{r['tf']}_stat"

                total_pnl = quantize_decimal(r["total_pnl"] or Decimal(0), 4)
                num_trades = r["num_trades"]
                num_wins = r["num_wins"]
                num_losses = r["num_losses"]
                avg_pnl = quantize_decimal(total_pnl / num_trades, 4)
                winrate = quantize_decimal(Decimal(num_wins) / num_trades, 4)
                base_rating = quantize_decimal(Decimal(0), 6)

                await conn.execute(f"""
                    INSERT INTO {table} (
                        strategy_id, direction, pattern_id,
                        num_trades, num_wins, num_losses,
                        total_pnl, avg_pnl, winrate, base_rating, last_updated
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
                    ON CONFLICT (strategy_id, direction, pattern_id)
                    DO UPDATE SET
                        num_trades = EXCLUDED.num_trades,
                        num_wins = EXCLUDED.num_wins,
                        num_losses = EXCLUDED.num_losses,
                        total_pnl = EXCLUDED.total_pnl,
                        avg_pnl = EXCLUDED.avg_pnl,
                        winrate = EXCLUDED.winrate,
                        base_rating = EXCLUDED.base_rating,
                        last_updated = NOW()
                """, r["strategy_id"], r["direction"], r["pattern_id"],
                     num_trades, num_wins, num_losses,
                     total_pnl, avg_pnl, winrate, base_rating)

        await infra.redis_client.set("emasnapshot:agg:pending", 0)
        log.info("✅ Агрегаты обновлены")

    except Exception:
        log.exception("❌ Ошибка при агрегировании логов")