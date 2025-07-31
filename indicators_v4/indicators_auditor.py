# indicators_auditor.py

import asyncio
import logging
from datetime import datetime, timedelta, timezone

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
        f"bb{params['length']}_{params['deviation']}_0_upper",
        f"bb{params['length']}_{params['deviation']}_0_lower",
        f"bb{params['length']}_{params['deviation']}_0_center",
        f"bb{params['length']}_{params['deviation']}_5_upper",
        f"bb{params['length']}_{params['deviation']}_5_lower",
        f"bb{params['length']}_{params['deviation']}_5_center",
        f"bb{params['length']}_{params['deviation']}_upper",
        f"bb{params['length']}_{params['deviation']}_lower",
        f"bb{params['length']}_{params['deviation']}_center",
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
    while True:
        try:
            log.info("Аудит: запуск проверки расчётов за 7 дней")
            await run_audit_check(pg, log)
            log.info("Аудит завершён, пауза 5 минут")
        except Exception as e:
            log.exception(f"Ошибка во время аудита: {e}")
        await asyncio.sleep(300)

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
            last_ts = int((now.timestamp() // step_sec - 2) * step_sec)
            start_ts = int((now - timedelta(days=7)).timestamp())
            open_times = [
                datetime.utcfromtimestamp(ts).replace(microsecond=0)
                for ts in range(start_ts, last_ts + 1, step_sec)
            ]

            for symbol in active_symbols:
                for open_time in open_times:
                    # Получение уже сохранённых параметров
                    values = await conn.fetch("""
                        SELECT param_name FROM indicator_values_v4
                        WHERE instance_id = $1 AND symbol = $2 AND open_time = $3
                    """, iid, symbol, open_time)

                    actual = {row["param_name"] for row in values}
                    missing = [p for p in expected if p not in actual]

                    if not missing:
                        # Обновление статуса, если ранее был пропуск
                        await conn.execute("""
                            UPDATE indicator_gaps_v4
                            SET recovered_at = now(), status = 'recovered'
                            WHERE instance_id = $1 AND symbol = $2 AND open_time = $3 AND status = 'missing'
                        """, iid, symbol, open_time)
                    else:
                        # Запись нового пропуска, если ещё не зафиксирован
                        await conn.execute("""
                            INSERT INTO indicator_gaps_v4 (instance_id, symbol, open_time)
                            VALUES ($1, $2, $3)
                            ON CONFLICT DO NOTHING
                        """, iid, symbol, open_time)
                        log.info(
                            f"Пропущен расчёт: {indicator} id={iid} {symbol} {tf} @ {open_time.isoformat()} "
                            f"→ отсутствуют: {', '.join(missing)}"
                        )