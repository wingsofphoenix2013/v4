# core_io.py — запись OHLCV-свечей из Redis Stream в PostgreSQL с ограничением в 14 дней

import asyncio
import json
import logging
from infra import info_log
from datetime import datetime, timedelta

STREAM_NAME = "ohlcv_stream"
GROUP_NAME = "core_writer"
CONSUMER_NAME = "writer_1"

# 🔁 Фоновый цикл проверки и восстановления M5-свечей
async def m5_integrity_loop(pg):
    while True:
        now = datetime.utcnow().replace(second=0, microsecond=0)
        latest_check_time = now - timedelta(minutes=5)
        earliest_check_time = now - timedelta(minutes=15)

        info_log("CORE_IO", f"🔍 Запуск проверки пропущенных M5: {earliest_check_time} → {latest_check_time}")

        async with pg.acquire() as conn:
            # Получаем все символы, у которых есть M5
            rows = await conn.fetch("SELECT DISTINCT symbol FROM ohlcv4_m5")
            symbols = [r["symbol"] for r in rows]

        for symbol in symbols:
            ts = earliest_check_time
            while ts <= latest_check_time:
                # Проверяем: есть ли уже такая M5
                async with pg.acquire() as conn:
                    exists = await conn.fetchval(
                        "SELECT 1 FROM ohlcv4_m5 WHERE symbol = $1 AND open_time = $2",
                        symbol, ts
                    )
                    if exists:
                        ts += timedelta(minutes=5)
                        continue

                    # Собираем 5 M1-свечей
                    m1_rows = await conn.fetch("""
                        SELECT open, high, low, close, volume FROM ohlcv4_m1
                        WHERE symbol = $1 AND open_time >= $2 AND open_time < $3
                        ORDER BY open_time ASC
                    """, symbol, ts, ts + timedelta(minutes=5))

                    if len(m1_rows) < 5:
                        info_log("CORE_IO", f"⚠️ Недостаточно M1 для восстановления M5: {symbol} @ {ts}")
                        ts += timedelta(minutes=5)
                        continue

                    # Агрегируем
                    o = m1_rows[0]["open"]
                    h = max(r["high"] for r in m1_rows)
                    l = min(r["low"] for r in m1_rows)
                    c = m1_rows[-1]["close"]
                    v = sum(r["volume"] for r in m1_rows)

                    await conn.execute("""
                        INSERT INTO ohlcv4_m5 (symbol, open_time, open, high, low, close, volume, source, inserted_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, 'recovery', NOW())
                        ON CONFLICT DO NOTHING
                    """, symbol, ts, o, h, l, c, v)

                    info_log("CORE_IO", f"🔁 Восстановлена M5: {symbol} @ {ts}")

                ts += timedelta(minutes=5)

        await asyncio.sleep(300)  # 5 минут пауза между итерациями
# ▶ Основной запуск воркера записи свечей
async def run_core_writer(pg, redis):
    info_log("CORE_IO", "▶ Старт обработчика core_writer")

    try:
        await redis.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
    except Exception:
        pass  # группа уже существует

    # ▶ Старт фоновой задачи восстановления M5
    asyncio.create_task(m5_integrity_loop(pg))

    while True:
        try:
            messages = await redis.xreadgroup(
                GROUP_NAME,
                CONSUMER_NAME,
                streams={STREAM_NAME: ">"},
                count=50,
                block=1000,
            )

            for _, entries in messages:
                for msg_id, data in entries:
                    try:
                        symbol = data.get("symbol")
                        interval = data.get("interval")
                        timestamp = int(data.get("timestamp"))

                        if not all([symbol, interval, timestamp]):
                            info_log("CORE_IO", f"⚠️ Некорректное сообщение: {data}")
                            await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
                            continue

                        key = f"ohlcv:{symbol.lower()}:{interval}:{timestamp}"
                        raw = await redis.execute_command("JSON.GET", key, "$")
                        if not raw:
                            info_log("CORE_IO", f"⚠️ Пустой ключ в Redis: {key}")
                            await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
                            continue

                        candle = json.loads(raw)[0]
                        await insert_candle(pg, symbol, interval, timestamp, candle)

                        info_log("CORE_IO", f"✅ [{symbol}] {interval.upper()} записана в PG: {timestamp}")
                        await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)

                    except Exception as e:
                        info_log("CORE_IO", f"❌ Ошибка обработки записи: {e}")

        except Exception as e:
            info_log("CORE_IO", f"❌ Ошибка чтения из потока: {e}")
            await asyncio.sleep(3)
# Вставка свечи в нужную таблицу + удаление старых (> 14 дней)
async def insert_candle(pg, symbol, interval, ts, candle):
    open_time = datetime.utcfromtimestamp(ts / 1000)
    table = f"ohlcv4_{interval}"

    async with pg.acquire() as conn:
        # Вставка новой свечи
        await conn.execute(f"""
            INSERT INTO {table} (symbol, open_time, open, high, low, close, volume, source, inserted_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
            ON CONFLICT DO NOTHING
        """, symbol, open_time, candle["o"], candle["h"], candle["l"], candle["c"],
             candle["v"], candle.get("fixed") and "api" or "stream")

        # Удаление устаревших записей
        await conn.execute(f"""
            DELETE FROM {table}
            WHERE symbol = $1 AND open_time < NOW() - INTERVAL '14 days'
        """, symbol)