# emasnapshot_worker.py

import asyncio
import logging
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timedelta

import infra

# 🔸 Кэш для ускорения доступа к словарю EMA-флагов
emasnapshot_dict_cache = {}

# 🔸 Логгер
log = logging.getLogger("EMASNAPSHOT_WORKER")

# 🔸 Утилита округления Decimal
def quantize_decimal(value: Decimal, precision: int) -> Decimal:
    return value.quantize(Decimal(f'1e-{precision}'), rounding=ROUND_HALF_UP)
    
# 🔸 Основная точка входа воркера
async def run_emasnapshot_worker():
    log.info("🚀 Воркер EMA Snapshot запущен")

    # Получаем стратегии с включённым флагом из кэша
    strategy_ids = [
        sid for sid, s in infra.enabled_strategies.items()
        if s.get("emasnapshot") is True
    ]

    log.info(f"🔍 Найдено стратегий с emasnapshot = true: {len(strategy_ids)}")

    if not strategy_ids:
        log.info("⛔ Стратегий для анализа не найдено")
        return

    async with infra.pg_pool.acquire() as conn:
        # Получаем позиции по этим стратегиям
        positions = await conn.fetch("""
            SELECT id, symbol, created_at, strategy_id, direction, pnl
            FROM positions_v4
            WHERE strategy_id = ANY($1)
              AND status = 'closed'
              AND emasnapshot_checked = false
        """, strategy_ids)

    log.info(f"📦 Найдено позиций для обработки: {len(positions)}")

    positions = positions[:200]

    sem = asyncio.Semaphore(10)
    tasks = [process_position(row, sem) for row in positions]
    await asyncio.gather(*tasks)
    
# 🔸 Обработка одной позиции (боевой режим — с записью в БД)
async def process_position(position, sem):
    async with sem:
        try:
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

                # Получаем ID из словаря с кэшированием
                if ordering in emasnapshot_dict_cache:
                    emasnapshot_dict_id = emasnapshot_dict_cache[ordering]
                else:
                    flag_row = await conn.fetchrow("""
                        SELECT id FROM oracle_emasnapshot_dict
                        WHERE ordering = $1
                    """, ordering)

                    if not flag_row:
                        log.warning(f"⛔ Нет записи в dict для ordering: {ordering}")
                        return

                    emasnapshot_dict_id = flag_row["id"]
                    emasnapshot_dict_cache[ordering] = emasnapshot_dict_id

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
                total_pnl = quantize_decimal(total_pnl, 4)
                avg_pnl = quantize_decimal(avg_pnl, 4)
                winrate = quantize_decimal(winrate, 4)
                base_rating = quantize_decimal(base_rating, 6)

                # Вставка/обновление агрегированной статистики
                await conn.execute("""
                    INSERT INTO positions_emasnapshot_m5_stat (
                        strategy_id, direction, emasnapshot_dict_id,
                        num_trades, num_wins, num_losses,
                        total_pnl, avg_pnl, winrate, base_rating, last_updated
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now())
                    ON CONFLICT (strategy_id, direction, emasnapshot_dict_id)
                    DO UPDATE SET
                        num_trades = EXCLUDED.num_trades,
                        num_wins = EXCLUDED.num_wins,
                        num_losses = EXCLUDED.num_losses,
                        total_pnl = EXCLUDED.total_pnl,
                        avg_pnl = EXCLUDED.avg_pnl,
                        winrate = EXCLUDED.winrate,
                        base_rating = EXCLUDED.base_rating,
                        last_updated = now()
                """, strategy_id, direction, emasnapshot_dict_id,
                     num_trades, num_wins, num_losses,
                     total_pnl, avg_pnl, winrate, base_rating)

                # Обновление позиции: отмечаем, что она обработана
                await conn.execute("""
                    UPDATE positions_v4
                    SET emasnapshot_checked = true
                    WHERE id = $1
                """, position["id"])

                log.info(f"✅ Обновлена статистика для позиции id={position['id']} (flag_id={emasnapshot_dict_id})")

        except Exception:
            log.exception(f"❌ Ошибка при обработке позиции id={position['id']}")