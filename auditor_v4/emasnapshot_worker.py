# emasnapshot_worker.py

import asyncio
import logging
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timedelta
from collections import defaultdict

import infra

# 🔸 Кэш для ускорения доступа к словарю EMA-флагов
emasnapshot_dict_cache = {}

# 🔸 Логгер
log = logging.getLogger("EMASNAPSHOT_WORKER")

# 🔸 Утилита округления Decimal
def quantize_decimal(value: Decimal, precision: int) -> Decimal:
    return value.quantize(Decimal(f'1e-{precision}'), rounding=ROUND_HALF_UP)

# 🔸 Расчёт времени предыдущей свечи для любого таймфрейма
def get_previous_tf_open_time(timestamp, tf: str) -> datetime:
    if tf == "m5":
        interval = 5
    elif tf == "m15":
        interval = 15
    elif tf == "h1":
        interval = 60
    else:
        raise ValueError(f"Unsupported timeframe: {tf}")

    # Округляем вниз до начала текущей свечи
    rounded = timestamp.replace(
        minute=(timestamp.minute // interval) * interval,
        second=0,
        microsecond=0
    )

    # Возвращаем начало предыдущей свечи
    return rounded - timedelta(minutes=interval)
    
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
    completed_tfs = defaultdict(set)  # {position_id: set of completed tfs}
    tasks = []

    # Обрабатываем каждую позицию по всем таймфреймам
    for tf in ("m5", "m15", "h1"):
        tasks.extend([
            process_position_for_tf(row, tf, sem, completed_tfs)
            for row in positions
        ])

    await asyncio.gather(*tasks)

    # Отмечаем только те позиции, которые обработаны по всем трём ТФ
    complete_ids = [
        pid for pid, tfs in completed_tfs.items()
        if {"m5", "m15", "h1"}.issubset(tfs)
    ]

    async with infra.pg_pool.acquire() as conn:
        await conn.executemany("""
            UPDATE positions_v4
            SET emasnapshot_checked = true
            WHERE id = $1
        """, [(pid,) for pid in complete_ids])

    log.info(f"✅ Отмечено как обработанных: {len(complete_ids)} позиций")
            
# 🔸 Обработка одной позиции под заданный таймфрейм (m5, m15, h1)
async def process_position_for_tf(position, tf: str, sem, completed_tfs):
    async with sem:
        try:
            import infra  # отложенный импорт
            async with infra.pg_pool.acquire() as conn:
                position_id = position["id"]
                symbol = position["symbol"]
                created_at = position["created_at"]
                strategy_id = position["strategy_id"]
                direction = position["direction"]

                try:
                    pnl = Decimal(position["pnl"])
                except Exception:
                    log.warning(f"⏭ [{tf}] Пропущена позиция id={position_id} — PnL не распарсился: {position['pnl']}")
                    return

                log.debug(f"▶️ [{tf}] Начата обработка позиции id={position_id}")

                # Приведение ко времени предыдущей свечи данного таймфрейма
                open_time = get_previous_tf_open_time(created_at, tf)

                # Получаем снапшот нужного таймфрейма
                snapshot = await conn.fetchrow("""
                    SELECT ordering FROM oracle_ema_snapshot_v4
                    WHERE symbol = $1 AND interval = $2 AND open_time = $3
                """, symbol, tf, open_time)

                if not snapshot:
                    log.warning(f"⏭ [{tf}] Пропущена позиция id={position_id} — нет снапшота @ {open_time}")
                    return

                ordering = snapshot["ordering"]

                # Кэширование словаря флагов
                if ordering in emasnapshot_dict_cache:
                    emasnapshot_dict_id = emasnapshot_dict_cache[ordering]
                else:
                    flag_row = await conn.fetchrow("""
                        SELECT id FROM oracle_emasnapshot_dict
                        WHERE ordering = $1
                    """, ordering)

                    if not flag_row:
                        log.warning(f"⏭ [{tf}] Пропущена позиция id={position_id} — нет записи в dict для ordering: {ordering}")
                        return

                    emasnapshot_dict_id = flag_row["id"]
                    emasnapshot_dict_cache[ordering] = emasnapshot_dict_id

                is_win = pnl > 0
                stat_table = f"positions_emasnapshot_{tf}_stat"

                # Получение текущей статистики
                stat = await conn.fetchrow(f"""
                    SELECT * FROM {stat_table}
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

                log.debug(f"🟡 [{tf}] Готов к UPSERT позиции id={position_id}, flag_id={emasnapshot_dict_id}")

                # UPSERT в нужную таблицу
                try:
                    await conn.execute(f"""
                        INSERT INTO {stat_table} (
                            strategy_id, direction, emasnapshot_dict_id,
                            num_trades, num_wins, num_losses,
                            total_pnl, avg_pnl, winrate, base_rating, last_updated
                        )
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, now())
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

                except Exception as upsert_exc:
                    log.exception(f"❌ [{tf}] Ошибка при UPSERT позиции id={position_id}: {upsert_exc}")
                    return
                    
                log.info(f"✅ [{tf}] Обновлена статистика для позиции id={position_id} (flag_id={emasnapshot_dict_id})")
                
                # Отмечаем, что позиция успешно обработана по этому таймфрейму
                completed_tfs[position_id].add(tf)

        except Exception:
            log.exception(f"❌ [{tf}] Ошибка при полной обработке позиции id={position['id']}")