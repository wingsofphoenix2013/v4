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

# 🔸 Обработка позиции по всем таймфреймам последовательно
async def process_position_all_tfs(position, sem):
    async with sem:
        try:
            async with infra.pg_pool.acquire() as conn:
                success = True

                for tf in ("m5", "m15", "h1"):
                    tf_success = await process_position_for_tf(position, tf, conn)
                    if not tf_success:
                        log.warning(f"⏭ [{tf}] Позиция id={position['id']} не прошла обработку")
                        success = False

                if success:
                    await conn.execute("""
                        UPDATE positions_v4
                        SET emasnapshot_checked = true
                        WHERE id = $1
                    """, position["id"])
                    log.info(f"✅ Позиция id={position['id']} полностью обработана по всем ТФ")

        except Exception:
            log.exception(f"❌ Ошибка при полной обработке позиции id={position['id']}")
            
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

    # Одна асинхронная задача на каждую позицию
    sem = asyncio.Semaphore(10)
    tasks = [process_position_all_tfs(pos, sem) for pos in positions]
    await asyncio.gather(*tasks)
            
# 🔸 Обработка позиции по одному таймфрейму, возвращает True/False
async def process_position_for_tf(position, tf: str, conn) -> bool:
    try:
        position_id = position["id"]
        symbol = position["symbol"]
        created_at = position["created_at"]
        strategy_id = position["strategy_id"]
        direction = position["direction"]

        try:
            pnl = Decimal(position["pnl"])
        except Exception:
            log.warning(f"⏭ [{tf}] Пропущена позиция id={position_id} — PnL нераспарсен: {position['pnl']}")
            return False

        # Округляем ко времени предыдущей свечи по ТФ
        open_time = get_previous_tf_open_time(created_at, tf)

        # Ищем снапшот
        snapshot = await conn.fetchrow("""
            SELECT ordering FROM oracle_ema_snapshot_v4
            WHERE symbol = $1 AND interval = $2 AND open_time = $3
        """, symbol, tf, open_time)

        if not snapshot:
            log.warning(f"⏭ [{tf}] Позиция id={position_id} — нет снапшота @ {open_time}")
            return False

        ordering = snapshot["ordering"]

        # Ищем flag_id в словаре
        if ordering in emasnapshot_dict_cache:
            emasnapshot_dict_id = emasnapshot_dict_cache[ordering]
        else:
            flag_row = await conn.fetchrow("""
                SELECT id FROM oracle_emasnapshot_dict
                WHERE ordering = $1
            """, ordering)

            if not flag_row:
                log.warning(f"⏭ [{tf}] Позиция id={position_id} — ordering не найден в dict: {ordering}")
                return False

            emasnapshot_dict_id = flag_row["id"]
            emasnapshot_dict_cache[ordering] = emasnapshot_dict_id

        is_win = pnl > 0
        stat_table = f"positions_emasnapshot_{tf}_stat"

        # Получаем текущую строку статистики
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

        # Округляем
        total_pnl = quantize_decimal(total_pnl, 4)
        avg_pnl = quantize_decimal(avg_pnl, 4)
        winrate = quantize_decimal(winrate, 4)
        base_rating = quantize_decimal(base_rating, 6)

        log.info(
            f"[{tf}] ▶️ UPSERT по позиции id={position_id} — pnl={pnl}, total={total_pnl}, "
            f"avg={avg_pnl}, winrate={winrate}, flag_id={emasnapshot_dict_id}"
        )

        # Выполняем UPSERT
        result = await conn.execute(f"""
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

        log.info(f"[{tf}] ✅ UPSERT result: {result}")
        log.info(f"✅ [{tf}] Обновлена статистика для позиции id={position_id} (flag_id={emasnapshot_dict_id})")

        return True

    except Exception:
        log.exception(f"❌ [{tf}] Ошибка при обработке позиции id={position['id']}")
        return False