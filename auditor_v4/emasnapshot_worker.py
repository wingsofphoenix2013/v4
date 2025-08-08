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
            import infra
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
                    log.debug(f"✅ Позиция id={position['id']} полностью обработана по всем ТФ")
                else:
                    count_logs = await conn.fetchval("""
                        SELECT COUNT(*) FROM emasnapshot_position_log
                        WHERE position_id = $1
                    """, position["id"])
                    log.info(f"⚠️ Позиция id={position['id']} частично обработана — логов записано: {count_logs}/3")

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
            LIMIT 200
        """, strategy_ids)

    log.debug(f"📦 Найдено позиций для обработки: {len(positions)}")

    # Одна асинхронная задача на каждую позицию
    sem = asyncio.Semaphore(10)
    tasks = [process_position_all_tfs(pos, sem) for pos in positions]
    await asyncio.gather(*tasks)
            
# 🔸 Обработка позиции по одному таймфрейму — логируем в raw-таблицу
async def process_position_for_tf(position, tf: str, conn) -> bool:
    try:
        position_id = position["id"]
        strategy_id = position["strategy_id"]
        direction = position["direction"]
        created_at = position["created_at"]
        symbol = position["symbol"]

        try:
            pnl = Decimal(position["pnl"])
        except Exception:
            log.warning(f"[{tf}] position_id={position_id} — pnl невалиден")
            return False

        # Приведение времени к предыдущей свече
        open_time = get_previous_tf_open_time(created_at, tf)

        # Поиск снапшота
        snapshot = await conn.fetchrow("""
            SELECT ordering FROM oracle_ema_snapshot_v4
            WHERE symbol = $1 AND interval = $2 AND open_time = $3
        """, symbol, tf, open_time)

        if not snapshot:
            log.warning(f"[{tf}] position_id={position_id} — снапшот отсутствует @ {open_time}")
            return False

        ordering = snapshot["ordering"]

        # Поиск или кеширование флага + pattern_id
        if ordering in emasnapshot_dict_cache:
            emasnapshot_dict_id, pattern_id = emasnapshot_dict_cache[ordering]
        else:
            row = await conn.fetchrow("""
                SELECT id, pattern_id
                FROM oracle_emasnapshot_dict
                WHERE ordering = $1
            """, ordering)

            if not row:
                log.warning(f"[{tf}] position_id={position_id} — ordering не найден в dict")
                return False

            emasnapshot_dict_id = row["id"]
            pattern_id = row["pattern_id"]
            emasnapshot_dict_cache[ordering] = (emasnapshot_dict_id, pattern_id)
            
        # Вставка лог-записи
        await conn.execute("""
            INSERT INTO emasnapshot_position_log (
                position_id, strategy_id, direction, tf,
                emasnapshot_dict_id, pattern_id, pnl
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT DO NOTHING
        """, position_id, strategy_id, direction, tf,
             emasnapshot_dict_id, pattern_id, pnl)

        log.debug(f"[{tf}] 📥 Лог сохранён: id={position_id}, flag={emasnapshot_dict_id}, pattern={pattern_id}, pnl={pnl}")

        # Сигнал агрегатору на пересчёт
        await infra.redis_client.set("emasnapshot:agg:pending", 1)

        return True

    except Exception:
        log.exception(f"[{tf}] ❌ Ошибка при логировании позиции id={position['id']}")
        return False