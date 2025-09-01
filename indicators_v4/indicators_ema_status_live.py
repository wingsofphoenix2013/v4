# indicators_ema_status_live.py — ежеминутный on-demand EMA-status: Этап 2 (срез DF по текущему бару)

import os
import asyncio
import logging
from datetime import datetime
import pandas as pd

log = logging.getLogger("EMA_STATUS_LIVE")

# 🔸 Конфиг
INTERVAL_SEC = int(os.getenv("EMA_STATUS_LIVE_INTERVAL_SEC", "60"))
REQUIRED_TFS = ("m5", "m15", "h1")
REQUIRED_BARS_DEFAULT = int(os.getenv("EMA_STATUS_LIVE_REQUIRED_BARS", "800"))

# 🔸 Вспомогательные мапы для шага времени
_STEP_MS = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}
_FIELDS = ("o", "h", "l", "c", "v")

# 🔸 Флор к началу бара TF (UTC, мс)
def _floor_to_bar_ms(ts_ms: int, tf: str) -> int:
    step = _STEP_MS[tf]
    return (ts_ms // step) * step

# 🔸 Загрузка OHLCV из Redis TS до bar_open_ms включительно (последние N баров)
async def _load_df_for_current_bar(redis, symbol: str, tf: str, bar_open_ms: int, depth: int) -> pd.DataFrame | None:
    step_ms = _STEP_MS[tf]
    start_ts = bar_open_ms - (depth - 1) * step_ms
    keys = {f: f"ts:{symbol}:{tf}:{f}" for f in _FIELDS}
    tasks = {f: redis.execute_command("TS.RANGE", keys[f], start_ts, bar_open_ms) for f in _FIELDS}
    res = await asyncio.gather(*tasks.values(), return_exceptions=True)

    # собрать по общему индексу (по доступным меткам времени)
    series = {}
    for f, r in zip(tasks.keys(), res):
        if isinstance(r, Exception):
            log.debug("[TSERR] %s err=%s", keys[f], r)
            continue
        if r:
            series[f] = {int(ts): float(v) for ts, v in r if v is not None}

    if "c" not in series or not series["c"]:
        return None

    idx = sorted(series["c"].keys())
    data = {f: [series.get(f, {}).get(ts) for ts in idx] for f in _FIELDS}
    df = pd.DataFrame(data, index=pd.to_datetime(idx, unit="ms"))
    df.index.name = "open_time"
    return df

# 🔸 Основной воркер (Этап 2: таймер + получение DF; Этап 1 логи → debug)
async def run_indicators_ema_status_live(pg, redis, get_instances_by_tf, get_precision, get_active_symbols):
    """
    Этап 2:
      - Каждые INTERVAL_SEC секунд стартует тик.
      - Для каждого активного symbol и TF рассчитываем bar_open_ms (текущий бар),
        загружаем OHLCV из Redis TS и формируем DataFrame, обрезанный по bar_open_ms.
      - Логируем успех загрузки DF и количество баров.
    """
    while True:
        try:
            tick_iso = datetime.utcnow().isoformat()
            symbols = list(get_active_symbols() or [])
            planned = 0
            ok = 0
            skipped = 0

            # Этап 1: понижаем до debug
            log.debug("[TICK] start @ %s, symbols=%d", tick_iso, len(symbols))

            now_ms = int(datetime.utcnow().timestamp() * 1000)

            for sym in symbols:
                for tf in REQUIRED_TFS:
                    planned += 1
                    bar_open_ms = _floor_to_bar_ms(now_ms, tf)
                    df = await _load_df_for_current_bar(redis, sym, tf, bar_open_ms, REQUIRED_BARS_DEFAULT)
                    if df is None or df.empty:
                        skipped += 1
                        # На Этапе 2 фиксируем отсутствие данных на info, чтобы сразу видеть проблемные пары
                        log.info("[DF] miss %s/%s @ %s", sym, tf, datetime.utcfromtimestamp(bar_open_ms/1000).isoformat())
                        continue

                    ok += 1
                    # Этап 2: основные логи — info
                    log.info("[DF] ok %s/%s bars=%d @ %s",
                             sym, tf, len(df), datetime.utcfromtimestamp(bar_open_ms/1000).isoformat())

            # Этап 1 итог — debug
            log.debug("[TICK] end, planned=%d ok=%d skipped=%d", planned, ok, skipped)

        except Exception as e:
            log.error("loop error: %s", e, exc_info=True)

        await asyncio.sleep(INTERVAL_SEC)