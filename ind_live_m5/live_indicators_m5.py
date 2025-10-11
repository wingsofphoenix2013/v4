# live_indicators_m5.py — фоновой воркер публикации «живых» индикаторов m5 в Redis KV (ind_live:*)

# 🔸 Импорты
import asyncio
import logging
import time
from datetime import datetime

from indicators.compute_and_store import compute_snapshot_values_async
from packs.pack_utils import floor_to_bar, load_ohlcv_df


# 🔸 Логгер
log = logging.getLogger("LIVE_M5")


# 🔸 Константы воркера
TF = "m5"                            # фиксированный таймфрейм для Stage-1
BARS = 800                           # глубина истории для расчёта
INITIAL_DELAY_SEC = 60               # задержка перед первым проходом
SLEEP_BETWEEN_CYCLES_SEC = 3         # пауза между проходами
TTL_SEC = 90                         # TTL для ind_live:* ключей, сек
MAX_CONCURRENCY = 30                 # ограничение параллелизма по символам


# 🔸 Публикация значений в Redis KV (ind_live:*), с TTL
async def _publish_values(redis, symbol: str, tf: str, values: dict[str, str]) -> tuple[int, int]:
    # возвращаем (успешно_установлено, ошибок)
    ok = 0
    err = 0
    tasks = []
    for pname, sval in values.items():
        key = f"ind_live:{symbol}:{tf}:{pname}"
        try:
            tasks.append(redis.set(key, sval, ex=TTL_SEC))
        except Exception:
            err += 1
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                err += 1
            else:
                ok += 1
    return ok, err


# 🔸 Обработка одного символа: загрузка DF, вычисление по всем инстансам m5, публикация
async def _process_symbol(redis,
                          symbol: str,
                          precision: int,
                          instances_m5: list[dict],
                          now_ms: int) -> dict:
    # нормализуем время к началу текущего бара
    bar_open_ms = floor_to_bar(now_ms, TF)

    # загрузка OHLCV одним батчем
    df = await load_ohlcv_df(redis, symbol, TF, bar_open_ms, BARS)
    if df is None or df.empty:
        return {"symbol": symbol, "computed": 0, "written": 0, "errors": 0, "skipped": len(instances_m5)}

    computed = 0
    written = 0
    errors = 0
    skipped = 0

    # по всем инстансам m5
    for inst in instances_m5:
        # проверка enabled_at: если бар раньше активации — пропускаем
        enabled_at = inst.get("enabled_at")
        if enabled_at:
            enabled_ms = int(enabled_at.replace(tzinfo=None).timestamp() * 1000)
            if bar_open_ms < enabled_ms:
                skipped += 1
                continue

        # расчёт «живых» значений (строковые, с округлением по precision)
        try:
            values = await compute_snapshot_values_async(inst, symbol, df, precision)
        except Exception:
            errors += 1
            continue

        if not values:
            skipped += 1
            continue

        computed += 1

        # публикация в ind_live:{symbol}:{m5}:{param}
        ok, err = await _publish_values(redis, symbol, TF, values)
        written += ok
        errors += err

    return {"symbol": symbol, "computed": computed, "written": written, "errors": errors, "skipped": skipped}


# 🔸 Основной воркер: каждые ~SLEEP_BETWEEN_CYCLES_SEC cчитает RAW индикаторы m5 для всех тикеров и пишет в ind_live:*
async def run_live_indicators_m5(pg,
                                 redis,
                                 get_instances_by_tf,
                                 get_precision,
                                 get_active_symbols):
    log.debug("LIVE_M5: воркер запущен (Stage-1: только m5, только RAW → Redis ind_live:*)")

    # начальная задержка, чтобы не стартовать вровень с другими процессами
    await asyncio.sleep(INITIAL_DELAY_SEC)

    # предсоздаём семафор параллелизма
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    while True:
        t0 = time.monotonic()

        # снимок активных символов и инстансов m5
        symbols = list(get_active_symbols()) or []
        instances_m5 = [i for i in get_instances_by_tf(TF)]
        total_instances = len(instances_m5)

        # быстрые проверки наличия работы
        if not symbols or not instances_m5:
            # пустой проход — но всё равно соблюдаем цикл и логируем
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            log.info(
                f"LIVE_M5 PASS done: symbols={len(symbols)} instances={total_instances} "
                f"computed=0 written=0 errors=0 skipped=0 elapsed_ms={elapsed_ms}"
            )
            await asyncio.sleep(SLEEP_BETWEEN_CYCLES_SEC)
            continue

        # текущее время (ms) для консистентности прохода
        now_ms = int(datetime.utcnow().timestamp() * 1000)

        # обработка всех символов с ограничением параллелизма
        async def _wrap(symbol: str):
            async with sem:
                try:
                    precision = int(get_precision(symbol) or 8)
                except Exception:
                    precision = 8
                return await _process_symbol(redis, symbol, precision, instances_m5, now_ms)

        tasks = [asyncio.create_task(_wrap(sym)) for sym in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=False)

        # агрегируем метрики
        agg_computed = sum(r["computed"] for r in results)
        agg_written  = sum(r["written"] for r in results)
        agg_errors   = sum(r["errors"] for r in results)
        agg_skipped  = sum(r["skipped"] for r in results)

        elapsed_ms = int((time.monotonic() - t0) * 1000)
        log.info(
            f"LIVE_M5 PASS done: symbols={len(symbols)} instances={total_instances} "
            f"computed={agg_computed} written={agg_written} errors={agg_errors} skipped={agg_skipped} "
            f"elapsed_ms={elapsed_ms}"
        )

        # пауза до следующего цикла
        await asyncio.sleep(SLEEP_BETWEEN_CYCLES_SEC)