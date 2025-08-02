# emasnapshot_worker.py

import asyncio
import logging
from decimal import Decimal
from datetime import datetime


import infra

# 🔸 Логгер
log = logging.getLogger("EMASNAPSHOT_WORKER")

# 🔸 Основная точка входа воркера
async def run_emasnapshot_worker():
    log.info("🚀 Воркер EMA Snapshot запущен")

    async with infra.pg_pool.acquire() as conn:
        # Получаем все стратегии, где включён флаг emasnapshot
        strategies = await conn.fetch("""
            SELECT id FROM strategies_v4
            WHERE emasnapshot = true
        """)

        strategy_ids = [row["id"] for row in strategies]
        log.info(f"🔍 Найдено стратегий с emasnapshot = true: {len(strategy_ids)}")

        if not strategy_ids:
            log.info("⛔ Стратегий для анализа не найдено")
            return

        # Получаем позиции по этим стратегиям
        positions = await conn.fetch("""
            SELECT id, symbol, created_at, strategy_id, direction, pnl
            FROM positions_v4
            WHERE strategy_id = ANY($1)
              AND status = 'closed'
              AND emasnapshot_checked = false
        """, strategy_ids)

        log.info(f"📦 Найдено позиций для обработки: {len(positions)}")

        # Ограничиваем количество обрабатываемых позиций
        positions = positions[:200]

        # Параллельная отладочная обработка с семафором
        sem = asyncio.Semaphore(10)
        tasks = [process_position_debug(row, sem) for row in positions]
        await asyncio.gather(*tasks)
        
# 🔸 Обработка одной позиции (отладочный режим, без записи в БД)
async def process_position_debug(position, sem):
    async with sem:
        try:
            import infra  # отложенный импорт
            async with infra.pg_pool.acquire() as conn:
                symbol = position["symbol"]
                created_at = position["created_at"]
                strategy_id = position["strategy_id"]
                direction = position["direction"]
                pnl = Decimal(position["pnl"] or 0)

                # Приведение времени к началу пятиминутной свечи
                open_time = created_at.replace(
                    minute=(created_at.minute // 5) * 5,
                    second=0,
                    microsecond=0
                )

                # Получаем снапшот
                snapshot = await conn.fetchrow("""
                    SELECT ordering FROM oracle_ema_snapshot_v4
                    WHERE symbol = $1 AND interval = 'm5' AND open_time = $2
                """, symbol, open_time)

                if not snapshot:
                    log.warning(f"⛔ Нет снапшота для {symbol} @ {open_time}")
                    return

                ordering = snapshot["ordering"]

                # Получаем ID из словаря
                flag_row = await conn.fetchrow("""
                    SELECT id FROM oracle_emasnapshot_dict
                    WHERE ordering = $1
                """, ordering)

                if not flag_row:
                    log.warning(f"⛔ Нет записи в dict для ordering: {ordering}")
                    return

                emasnapshot_dict_id = flag_row["id"]
                is_win = pnl > 0

                # Получаем текущую статистику (если есть)
                stat = await conn.fetchrow("""
                    SELECT * FROM positions_emasnapshot_m5_stat
                    WHERE strategy_id = $1 AND direction = $2 AND emasnapshot_dict_id = $3
                """, strategy_id, direction, emasnapshot_dict_id)

                if stat:
                    num_trades = stat["num_trades"] + 1
                    num_wins = stat["num_wins"] + (1 if is_win else 0)
                    num_losses = stat["num_losses"] + (0 if is_win else 1)
                    total_pnl = Decimal(stat["total_pnl"]) + pnl
                else:
                    num_trades = 1
                    num_wins = 1 if is_win else 0
                    num_losses = 0 if is_win else 1
                    total_pnl = pnl

                avg_pnl = total_pnl / num_trades
                winrate = Decimal(num_wins) / num_trades
                base_rating = Decimal(0)

                # Округление
                from decimal_utils import quantize_decimal
                total_pnl = quantize_decimal(total_pnl, 4)
                avg_pnl = quantize_decimal(avg_pnl, 4)
                winrate = quantize_decimal(winrate, 4)
                base_rating = quantize_decimal(base_rating, 6)

                # Лог
                log.info(
                    f"[sim] Позиция ID={position['id']} | strategy={strategy_id} | {direction.upper()} | "
                    f"ordering={ordering} | flag_id={emasnapshot_dict_id} | pnl={pnl} | "
                    f"{'WIN' if is_win else 'LOSS'}"
                )
                log.info(
                    f"[sim] → num_trades={num_trades}, num_wins={num_wins}, num_losses={num_losses}, "
                    f"total_pnl={total_pnl}, avg_pnl={avg_pnl}, winrate={winrate}, base_rating={base_rating}"
                )

        except Exception:
            log.exception(f"❌ Ошибка при отладочной обработке позиции id={position['id']}")