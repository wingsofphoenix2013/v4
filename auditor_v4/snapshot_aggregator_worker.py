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
            # Собираем агрегаты ТОЛЬКО по новым строкам
            rows = await conn.fetch("""
                SELECT
                    strategy_id,
                    direction,
                    tf,
                    emasnapshot_dict_id,
                    COUNT(*) AS num_trades,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS num_wins,
                    SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) AS num_losses,
                    SUM(pnl) AS total_pnl,
                    AVG(pnl) AS avg_pnl,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END)::DECIMAL / COUNT(*) AS winrate
                FROM emasnapshot_position_log
                WHERE aggregated_at IS NULL
                GROUP BY strategy_id, direction, tf, emasnapshot_dict_id
            """)

            for r in rows:
                table = f"positions_emasnapshot_{r['tf']}_stat"
                await conn.execute(f"""
                    INSERT INTO {table} (
                        strategy_id, direction, emasnapshot_dict_id,
                        num_trades, num_wins, num_losses,
                        total_pnl, avg_pnl, winrate, base_rating, last_updated
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,0,NOW())
                    ON CONFLICT (strategy_id, direction, emasnapshot_dict_id)
                    DO UPDATE SET
                        num_trades = positions_emasnapshot_{r['tf']}_stat.num_trades + EXCLUDED.num_trades,
                        num_wins = positions_emasnapshot_{r['tf']}_stat.num_wins + EXCLUDED.num_wins,
                        num_losses = positions_emasnapshot_{r['tf']}_stat.num_losses + EXCLUDED.num_losses,
                        total_pnl = positions_emasnapshot_{r['tf']}_stat.total_pnl + EXCLUDED.total_pnl,
                        avg_pnl = (positions_emasnapshot_{r['tf']}_stat.total_pnl + EXCLUDED.total_pnl) 
                                  / (positions_emasnapshot_{r['tf']}_stat.num_trades + EXCLUDED.num_trades),
                        winrate = (positions_emasnapshot_{r['tf']}_stat.num_wins + EXCLUDED.num_wins)::DECIMAL 
                                  / (positions_emasnapshot_{r['tf']}_stat.num_trades + EXCLUDED.num_trades),
                        last_updated = NOW()
                """, r["strategy_id"], r["direction"], r["emasnapshot_dict_id"],
                     r["num_trades"], r["num_wins"], r["num_losses"],
                     Decimal(r["total_pnl"]), Decimal(r["avg_pnl"]), Decimal(r["winrate"]))

            # Отмечаем как обработанные
            await conn.execute("""
                UPDATE emasnapshot_position_log
                SET aggregated_at = now()
                WHERE aggregated_at IS NULL
            """)

        await infra.redis_client.set("emasnapshot:agg:pending", 0)
        log.info("✅ Агрегаты обновлены")

    except Exception:
        log.exception("❌ Ошибка при агрегировании логов")