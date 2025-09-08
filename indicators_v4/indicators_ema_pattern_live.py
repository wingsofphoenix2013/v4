# indicators_ema_pattern_live.py — ежеминутный LIVE-воркер: определение EMA-паттерна (PRICE vs EMA 9/21/50/100/200) по mark price и публикация в Redis KV

# 🔸 Импорты
import os
import asyncio
import logging
from datetime import datetime
from typing import Dict, Optional

import pandas as pd
from indicators.compute_and_store import compute_snapshot_values_async

# 🔸 Логгер
log = logging.getLogger("EMA_PATTERN_LIVE")

# 🔸 Конфиг
INTERVAL_SEC = int(os.getenv("EMA_PATTERN_LIVE_INTERVAL_SEC", "30"))
TTL_SEC = int(os.getenv("EMA_PATTERN_LIVE_TTL_SEC", "60"))
MAX_CONCURRENCY = int(os.getenv("EMA_PATTERN_LIVE_MAX_CONCURRENCY", "45"))
MAX_PER_SYMBOL = int(os.getenv("EMA_PATTERN_LIVE_MAX_PER_SYMBOL", "3"))
REQUIRED_BARS_DEFAULT = int(os.getenv("EMA_PATTERN_LIVE_REQUIRED_BARS", "800"))

# 🔸 Таймфреймы и шаги
_REQUIRED_TFS = ("m5", "m15", "h1")
_STEP_MS: Dict[str, int] = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}

# 🔸 Параметры EMA-паттерна
EMA_NAMES = ("ema9", "ema21", "ema50", "ema100", "ema200")
EMA_LEN: Dict[str, int] = {"ema9": 9, "ema21": 21, "ema50": 50, "ema100": 100, "ema200": 200}
EPSILON_REL = float(os.getenv("EMA_PATTERN_EQUAL_EPS", "0.0005"))  # 0.05% относительное равенство

# 🔸 Глобальный кэш словаря паттернов (pattern_text -> id), загружается на старте воркера
_PATTERN_DICT: Dict[str, int] = {}


# 🔸 Флор к началу бара (UTC, мс)
def _floor_to_bar_ms(ts_ms: int, tf: str) -> int:
    return (ts_ms // _STEP_MS[tf]) * _STEP_MS[tf]


# 🔸 Загрузка OHLCV до текущего бара t (включительно)
async def _load_df_for_current_bar(redis, symbol: str, tf: str, bar_open_ms: int, depth: int) -> Optional[pd.DataFrame]:
    fields = ("o", "h", "l", "c", "v")
    start_ts = bar_open_ms - (depth - 1) * _STEP_MS[tf]

    keys = {f: f"ts:{symbol}:{tf}:{f}" for f in fields}
    tasks = {f: redis.execute_command("TS.RANGE", keys[f], start_ts, bar_open_ms) for f in fields}
    res = await asyncio.gather(*tasks.values(), return_exceptions=True)

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
    data = {f: [series.get(f, {}).get(ts) for ts in idx] for f in fields}
    df = pd.DataFrame(data, index=pd.to_datetime(idx, unit="ms"))
    df.index.name = "open_time"
    return df


# 🔸 Подбор нужных EMA-инстансов на TF
def _pick_ema_instances(instances_tf: list) -> Dict[int, dict]:
    by_len: Dict[int, dict] = {}
    for inst in instances_tf:
        if inst.get("indicator") != "ema":
            continue
        p = inst.get("params", {}) or {}
        try:
            L = int(p.get("length", 0))
            if L in (9, 21, 50, 100, 200) and L not in by_len:
                by_len[L] = inst
        except Exception:
            continue
    return by_len


# 🔸 Относительное равенство (порог 0.05%)
def _rel_equal(a: float, b: float) -> bool:
    m = max(abs(a), abs(b), 1e-12)
    return abs(a - b) <= EPSILON_REL * m


# 🔸 Построение каноничного текста паттерна из PRICE и 5 EMA
def _build_pattern_text(price: float, emas: Dict[str, float]) -> str:
    pairs = [("PRICE", float(price))]
    for ename in EMA_NAMES:
        pairs.append((ename.upper(), float(emas[ename])))

    pairs.sort(key=lambda kv: kv[1], reverse=True)

    groups = []
    cur = []
    for token, val in pairs:
        if not cur:
            cur = [(token, val)]
            continue
        ref_val = cur[0][1]
        if _rel_equal(val, ref_val):
            cur.append((token, val))
        else:
            groups.append([t for t, _ in cur])
            cur = [(token, val)]
    if cur:
        groups.append([t for t, _ in cur])

    canon = []
    for g in groups:
        if "PRICE" in g:
            rest = [t for t in g if t != "PRICE"]
            rest.sort(key=lambda t: EMA_LEN[t.lower()])
            canon.append(["PRICE"] + rest)
        else:
            gg = list(g)
            gg.sort(key=lambda t: EMA_LEN[t.lower()])
            canon.append(gg)

    return " > ".join(" = ".join(g) for g in canon)


# 🔸 Единоразовая загрузка словаря паттернов (в память)
async def _load_pattern_dict(pg) -> None:
    global _PATTERN_DICT
    async with pg.acquire() as conn:
        rows = await conn.fetch("SELECT id, pattern_text FROM indicator_emapattern_dict")
    _PATTERN_DICT = {str(r["pattern_text"]): int(r["id"]) for r in rows}
    log.debug(f"[DICT_LOADED] patterns={len(_PATTERN_DICT)}")


# 🔸 Основной воркер: LIVE EMA-паттерн → Redis KV (text + id), словарь загружается на старте
async def run_indicators_ema_pattern_live(pg, redis, get_instances_by_tf, get_precision, get_active_symbols):
    if not _PATTERN_DICT:
        await _load_pattern_dict(pg)

    symbol_semaphores: Dict[str, asyncio.Semaphore] = {}
    gate = asyncio.Semaphore(MAX_CONCURRENCY)

    while True:
        try:
            now_ms = int(datetime.utcnow().timestamp() * 1000)
            symbols = list(get_active_symbols() or [])
            log.debug("[TICK] start symbols=%d", len(symbols))

            async def handle_pair(sym: str, tf: str) -> int:
                written = 0
                async with gate:
                    if sym not in symbol_semaphores:
                        symbol_semaphores[sym] = asyncio.Semaphore(MAX_PER_SYMBOL)
                    async with symbol_semaphores[sym]:
                        try:
                            bar_open_ms = _floor_to_bar_ms(now_ms, tf)
                            precision = get_precision(sym)

                            df = await _load_df_for_current_bar(redis, sym, tf, bar_open_ms, REQUIRED_BARS_DEFAULT)
                            if df is None or df.empty:
                                return 0

                            inst_map = _pick_ema_instances(get_instances_by_tf(tf))
                            if not all(L in inst_map for L in (9, 21, 50, 100, 200)):
                                return 0

                            emas_num: Dict[str, float] = {}
                            for L in (9, 21, 50, 100, 200):
                                vals = await compute_snapshot_values_async(inst_map[L], sym, df, precision)
                                key = f"ema{L}"
                                if not vals or key not in vals:
                                    return 0
                                try:
                                    emas_num[key] = float(vals[key])
                                except Exception:
                                    return 0

                            try:
                                mark_raw = await redis.get(f"price:{sym}")
                                price = float(mark_raw) if mark_raw is not None else float(df["c"].iloc[-1])
                            except Exception:
                                price = float(df["c"].iloc[-1])

                            pattern_text = _build_pattern_text(price, emas_num)
                            pattern_id = _PATTERN_DICT.get(pattern_text)  # без похода в БД

                            await redis.setex(f"ind_live:{sym}:{tf}:ema_pattern_text", TTL_SEC, pattern_text)
                            written += 1
                            if pattern_id is not None:
                                await redis.setex(f"ind_live:{sym}:{tf}:ema_pattern_id", TTL_SEC, str(pattern_id))
                                written += 1

                            log.debug("[PATTERN] %s/%s → %s (id=%s)", sym, tf, pattern_text, str(pattern_id))

                        except Exception as e:
                            log.debug("[PAIR] %s/%s err=%s", sym, tf, e)

                return written

            tasks = [asyncio.create_task(handle_pair(sym, tf)) for sym in symbols for tf in _REQUIRED_TFS]
            results = await asyncio.gather(*tasks)
            total_written = sum(results)

            log.debug("[TICK] end written=%d", total_written)

        except Exception as e:
            log.error("loop error: %s", e, exc_info=True)

        await asyncio.sleep(INTERVAL_SEC)