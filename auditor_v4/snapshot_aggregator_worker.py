# 🔸 snapshot_aggregator_worker.py

import logging
from decimal import Decimal

import infra

log = logging.getLogger("SNAPSHOT_AGGREGATOR")

# 🔸 Генерация паттерна из строки ordering
def extract_pattern_from_ordering(ordering: str) -> str:
    values = []
    for group in ordering.split(">"):
        items = [x.strip() for x in group.split("=")]
        for item in items:
            if len(values) < 3:
                values.append(item)
            else:
                break
        if len(values) >= 3:
            break

    # Сборка обратно в pattern
    groups = []
    buffer = []

    for item in values:
        if not buffer:
            buffer = [item]
        elif "=" in ordering and f"{buffer[-1]}={item}" in ordering:
            buffer.append(item)
        else:
            groups.append("=".join(buffer))
            buffer = [item]

    if buffer:
        groups.append("=".join(buffer))

    return " > ".join(groups)
    
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
            # Собираем агрегаты
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
                        num_trades = EXCLUDED.num_trades,
                        num_wins = EXCLUDED.num_wins,
                        num_losses = EXCLUDED.num_losses,
                        total_pnl = EXCLUDED.total_pnl,
                        avg_pnl = EXCLUDED.avg_pnl,
                        winrate = EXCLUDED.winrate,
                        last_updated = NOW();
                """, r["strategy_id"], r["direction"], r["emasnapshot_dict_id"],
                     r["num_trades"], r["num_wins"], r["num_losses"],
                     Decimal(r["total_pnl"]), Decimal(r["avg_pnl"]), Decimal(r["winrate"]))

        await infra.redis_client.set("emasnapshot:agg:pending", 0)
        log.info("✅ Агрегаты обновлены")

    except Exception:
        log.exception("❌ Ошибка при агрегировании логов")
# 🔸 Однократная синхронизация pattern_id для oracle_emasnapshot_dict

async def sync_snapshot_patterns():
    log.info("🚀 Начало синхронизации pattern_id для снапшотов")

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, ordering
            FROM oracle_emasnapshot_dict
            WHERE pattern_id IS NULL
        """)

        updated = 0

        for row in rows:
            dict_id = row["id"]
            ordering = row["ordering"]

            # Генерация паттерна
            try:
                pattern = extract_pattern_from_ordering(ordering)
            except Exception as e:
                log.warning(f"⛔ Не удалось обработать ordering id={dict_id}: {e}")
                continue

            # Поиск pattern_id
            pattern_row = await conn.fetchrow("""
                SELECT id FROM oracle_emasnapshot_pattern
                WHERE pattern = $1
            """, pattern)

            if not pattern_row:
                log.warning(f"❗ Паттерн не найден: '{pattern}' для ordering id={dict_id}")
                continue

            pattern_id = pattern_row["id"]

            # Обновление pattern_id
            await conn.execute("""
                UPDATE oracle_emasnapshot_dict
                SET pattern_id = $1
                WHERE id = $2
            """, pattern_id, dict_id)

            updated += 1

    log.info(f"✅ Синхронизация завершена. Обновлено строк: {updated}")