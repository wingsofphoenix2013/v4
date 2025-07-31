# indicators_auditor.py

import asyncio
import logging
from datetime import datetime, timedelta

# 🔸 Ожидаемые имена параметров по типу индикатора
EXPECTED_PARAMS = {
    "ema": lambda params: [f"ema{params['length']}"],
    "kama": lambda params: [f"kama{params['length']}"],
    "atr": lambda params: [f"atr{params['length']}"],
    "mfi": lambda params: [f"mfi{params['length']}"],
    "rsi": lambda params: [f"rsi{params['length']}"],
    "adx_dmi": lambda params: [
        f"adx_dmi{params['length']}_adx",
        f"adx_dmi{params['length']}_plus_di",
        f"adx_dmi{params['length']}_minus_di",
    ],
    "lr": lambda params: [
        f"lr{params['length']}_upper",
        f"lr{params['length']}_lower",
        f"lr{params['length']}_center",
        f"lr{params['length']}_angle",
    ],
    "bb": lambda params: [
        f"bb{params['length']}_{params['std']}_0_upper",
        f"bb{params['length']}_{params['std']}_0_lower",
        f"bb{params['length']}_{params['std']}_0_center",
        f"bb{params['length']}_{params['std']}_5_upper",
        f"bb{params['length']}_{params['std']}_5_lower",
        f"bb{params['length']}_{params['std']}_5_center",
    ],
    "macd": lambda params: [
        f"macd{params['fast']}_macd",
        f"macd{params['fast']}_macd_signal",
        f"macd{params['fast']}_macd_hist",
    ],
}

# 🔸 Длительность таймфреймов в миллисекундах
TIMEFRAME_STEPS = {
    "m1": 60_000,
    "m5": 300_000,
    "m15": 900_000,
    "h1": 3_600_000,
}

# 🔸 Основной цикл аудита
async def audit_loop(pg):
    log = logging.getLogger("AUDITOR")
    try:
        log.info("Аудит: запуск проверки расчётов за 7 дней")
        await run_audit_check(pg, log)
        log.info("Аудит завершён")
    except Exception as e:
        log.exception(f"Ошибка во время аудита: {e}")

# 🔸 Проверка наличия расчётов индикаторов
async def run_audit_check(pg, log):
    async with pg.acquire() as conn:
        # Загрузка активных тикеров
        tickers = await conn.fetch("""
            SELECT symbol FROM tickers_v4
            WHERE status = 'enabled' AND tradepermission = 'enabled'
        """)
        active_symbols = {r['symbol'] for r in tickers}
        log.info(f"Загружено активных тикеров: {len(active_symbols)}")

        # Загрузка включённых индикаторов и параметров
        instances = await conn.fetch("""
            SELECT ii.id, ii.indicator, ii.timeframe,
                   ip.param, ip.value
            FROM indicator_instances_v4 ii
            JOIN indicator_parameters_v4 ip ON ip.instance_id = ii.id
            WHERE ii.enabled = true
        """)

        # Сборка инстансов с параметрами
        instance_map = {}
        for row in instances:
            iid = row['id']
            if iid not in instance_map:
                instance_map[iid] = {
                    "indicator": row['indicator'],
                    "timeframe": row['timeframe'],
                    "params": {}
                }
            instance_map[iid]["params"][row["param"]] = row["value"]

        log.info(f"Загружено активных индикаторов: {len(instance_map)}")

    now = datetime.utcnow()
    semaphore = asyncio.Semaphore(10)
    tasks = []

    for iid, inst in instance_map.items():
        indicator = inst["indicator"]
        tf = inst["timeframe"]
        params = inst["params"]

        if tf not in TIMEFRAME_STEPS:
            log.info(f"Пропуск: неизвестный таймфрейм '{tf}' для iid={iid}")
            continue

        try:
            expected = EXPECTED_PARAMS[indicator](params)
        except Exception as e:
            log.warning(f"Пропуск: не удалось получить ожидаемые параметры для {indicator} id={iid}: {e}")
            continue

        step_sec = TIMEFRAME_STEPS[tf] // 1000
        last_ts = int(now.timestamp())
        last_ts -= last_ts % step_sec
        last_ts -= 2 * step_sec
        start_ts = last_ts - 7 * 86400

        open_times = [
            datetime.utcfromtimestamp(ts).replace(microsecond=0)
            for ts in range(start_ts, last_ts + 1, step_sec)
        ]

        for symbol in active_symbols:
            tasks.append(
                audit_instance_symbol(pg, iid, symbol, tf, indicator, expected, open_times, semaphore, log)
            )

    await asyncio.gather(*tasks)

# 🔸 Проверка расчётов по одному индикатору и символу с разбиением по чанкам
async def audit_instance_symbol(pg, iid, symbol, tf, indicator, expected, open_times, semaphore, log):
    chunk_size = 200

    async with semaphore:
        for i in range(0, len(open_times), chunk_size):
            chunk = open_times[i:i + chunk_size]

            count_ok = 0
            count_missing = 0

            async with pg.acquire() as conn:
                for open_time in chunk:
                    values = await conn.fetch("""
                        SELECT param_name FROM indicator_values_v4
                        WHERE instance_id = $1 AND symbol = $2 AND open_time = $3
                    """, iid, symbol, open_time)

                    actual = {row["param_name"] for row in values}
                    missing = [p for p in expected if p not in actual]

                    if not missing:
                        count_ok += 1
                        await conn.execute("""
                            UPDATE indicator_gaps_v4
                            SET recovered_at = now(), status = 'recovered'
                            WHERE instance_id = $1 AND symbol = $2 AND open_time = $3 AND status = 'missing'
                        """, iid, symbol, open_time)
                    else:
                        count_missing += 1
                        await conn.execute("""
                            INSERT INTO indicator_gaps_v4 (instance_id, symbol, open_time)
                            VALUES ($1, $2, $3)
                            ON CONFLICT DO NOTHING
                        """, iid, symbol, open_time)

            chunk_from = chunk[0].isoformat()
            chunk_to = chunk[-1].isoformat()
            total = len(chunk)
            if count_missing:
                result = f"MISSING: {count_missing}"
            else:
                result = "OK"

            log.debug(
                f"{indicator.upper()} id={iid} {symbol} {tf} | "
                f"Проверено {total} свечей: {chunk_from} — {chunk_to} → {result}"
            )

            if i + chunk_size < len(open_times):
                log.debug(f"Ожидание 60 сек перед следующим чанком: {indicator.upper()} id={iid} {symbol} {tf}")
                await asyncio.sleep(60)