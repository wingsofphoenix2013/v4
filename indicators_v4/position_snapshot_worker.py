# position_snapshot_worker.py — чтение positions_open_stream и on-demand срез индикаторов + EMA-status + EMA-pattern

# 🔸 Импорты
import os
import asyncio
import logging
import json
from datetime import datetime

import pandas as pd
from indicators.compute_and_store import compute_snapshot_values_async
from indicators_ema_status import _classify_with_prev, EPS0, EPS1
from position_snapshot_sharedmemory import put_snapshot_tf

# 🔸 Логгер
log = logging.getLogger("IND_POS_SNAPSHOT")

# 🔸 Конфиг потоков
STREAM   = "positions_open_stream"
GROUP    = "indicators_position_group"
CONSUMER = "ind_pos_1"

# 🔸 Общие тайминги
STEP_MIN = {"m5": 5, "m15": 15, "h1": 60}
REQUIRED_BARS_DEFAULT = 800
STEP_MS = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}

# 🔸 Параллелизм on-demand расчётов (настраивается через ENV)
SNAPSHOT_MAX_CONCURRENCY = int(os.getenv("SNAPSHOT_MAX_CONCURRENCY", "16"))

# 🔸 EMA-паттерн: константы, фейковые instance_id и кэш словаря
EMA_NAMES  = ("ema9", "ema21", "ema50", "ema100", "ema200")
EMA_LEN    = {"ema9": 9, "ema21": 21, "ema50": 50, "ema100": 100, "ema200": 200}
EPSILON_REL = 0.0005  # относительное равенство 0.05%

EMAPATTERN_INSTANCE_ID = {"m5": 1004, "m15": 1005, "h1": 1006}
_EMA_PATTERN_DICT: dict[str, int] = {}

# 🔸 Флор времени к началу бара TF
def floor_to_bar_ms(ts_ms: int, tf: str) -> int:
    step_ms = STEP_MIN[tf] * 60_000
    return (ts_ms // step_ms) * step_ms

# 🔸 Чтение диапазона из TS в dict
async def ts_range_map(redis, key: str, start_ms: int, end_ms: int):
    if not key:
        return {}
    try:
        res = await redis.execute_command("TS.RANGE", key, start_ms, end_ms)
        return {int(ts): float(v) for ts, v in (res or [])}
    except Exception as e:
        log.debug(f"[TSERR] key={key} err={e}")
        return {}

# 🔸 Чтение одной точки по точному штампу
async def ts_get_point(redis, key: str, ts_ms: int):
    m = await ts_range_map(redis, key, ts_ms, ts_ms)
    if not m:
        return None
    return m.get(ts_ms)

# 🔸 Относительное равенство для EMA-паттерна
def _rel_equal(a: float, b: float) -> bool:
    m = max(abs(a), abs(b), 1e-12)
    return abs(a - b) <= EPSILON_REL * m

# 🔸 Построение текста EMA-паттерна из entry_price и 5 EMA
def _build_emapattern_text(entry_price: float, emas: dict[str, float]) -> str:
    pairs = [("PRICE", float(entry_price))]
    for name in EMA_NAMES:
        pairs.append((name.upper(), float(emas[name])))

    pairs.sort(key=lambda kv: kv[1], reverse=True)

    groups: list[list[str]] = []
    cur: list[tuple[str, float]] = []
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

    canon: list[list[str]] = []
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

# 🔸 Разовая загрузка словаря EMA-паттернов (pattern_text -> id)
async def _load_emapattern_dict(pg) -> None:
    global _EMA_PATTERN_DICT
    async with pg.acquire() as conn:
        rows = await conn.fetch("SELECT id, pattern_text FROM indicator_emapattern_dict")
    _EMA_PATTERN_DICT = {str(r["pattern_text"]): int(r["id"]) for r in rows}
    log.debug(f"[EMA_DICT] loaded={len(_EMA_PATTERN_DICT)}")

# 🔸 Подбор требуемых инстансов на TF
def pick_required_instances(instances_tf: list, ema_lens: list[int] = None):
    ema_by_len = {}
    atr14 = None
    bb_20_2 = None
    macd12 = None
    adx_14_or_28 = None
    for inst in instances_tf:
        ind = inst.get("indicator")
        p = inst.get("params", {})
        try:
            if ind == "ema":
                L = int(p.get("length"))
                if ema_lens is None or L in ema_lens:
                    ema_by_len[L] = inst
            elif ind == "atr" and int(p.get("length", 0)) == 14 and atr14 is None:
                atr14 = inst
            elif ind == "bb" and int(p.get("length", 0)) == 20 and abs(float(p.get("std", 0)) - 2.0) < 1e-9 and bb_20_2 is None:
                bb_20_2 = inst
            elif ind == "macd" and int(p.get("fast", 0)) == 12 and macd12 is None:
                macd12 = inst
            elif ind == "adx_dmi" and adx_14_or_28 is None:
                adx_14_or_28 = inst  # длина определяется через TS-ключи/TF
        except Exception:
            continue
    return ema_by_len, atr14, bb_20_2, macd12, adx_14_or_28

# 🔸 Основной воркер
async def run_position_snapshot_worker(pg, redis, get_instances_by_tf, get_precision, get_strategy_mw=lambda _sid: True):
    # локальный импорт для публикации снапшотов TF в общую память
    try:
        from position_snapshot_sharedmemory import put_snapshot_tf
    except Exception:
        put_snapshot_tf = None
        log.warning("position_snapshot_sharedmemory.put_snapshot_tf недоступен — пост-обработка отключена")

    # создание consumer-group
    try:
        await redis.xgroup_create(STREAM, GROUP, id="$", mkstream=True)
        log.debug(f"Группа {GROUP} создана для {STREAM}")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug(f"Группа {GROUP} уже существует")
        else:
            log.exception("Ошибка создания consumer group")
            return

    # разовая загрузка словаря EMA-паттернов
    if not _EMA_PATTERN_DICT:
        try:
            await _load_emapattern_dict(pg)
        except Exception:
            log.exception("Не удалось загрузить словарь EMA-паттернов (indicator_emapattern_dict)")

    sem = asyncio.Semaphore(SNAPSHOT_MAX_CONCURRENCY)

    while True:
        try:
            resp = await redis.xreadgroup(
                groupname=GROUP,
                consumername=CONSUMER,
                streams={STREAM: ">"},
                count=10,
                block=2000
            )
            if not resp:
                continue

            to_ack = []

            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)
                    try:
                        uid         = data.get("position_uid")
                        sym         = data.get("symbol")
                        strat       = int(data.get("strategy_id"))
                        side        = data.get("direction")
                        created_iso = data.get("created_at")
                        log_uid     = data.get("log_uid")  # важен для пост-обработчика

                        log.debug(f"[OPENED] uid={uid} {sym} strategy={strat} dir={side} created_at={created_iso}")

                        # 🔸 Фильтр по стратегиям: обрабатываем ТОЛЬКО если market_watcher = true
                        if not get_strategy_mw(strat):
                            log.debug(f"[SKIP_MW] uid={uid} strategy={strat} market_watcher=false — пропускаем обработку")
                            continue
                            
                        created_dt = datetime.fromisoformat(created_iso)
                        created_ms = int(created_dt.timestamp() * 1000)
                        precision  = get_precision(sym)

                        # entry_price: сначала из сообщения, при отсутствии — фоллбэк к БД
                        entry_price = None
                        try:
                            ep_raw = data.get("entry_price")
                            if ep_raw is not None:
                                entry_price = float(ep_raw)
                        except Exception:
                            entry_price = None
                        if entry_price is None:
                            async with pg.acquire() as conn:
                                ep_row = await conn.fetchrow(
                                    "SELECT entry_price FROM positions_v4 WHERE position_uid = $1",
                                    uid
                                )
                            entry_price = float(ep_row["entry_price"]) if (ep_row and ep_row["entry_price"] is not None) else None
                        if entry_price is None:
                            log.debug(f"[SKIP_EMAPATTERN] uid={uid} нет entry_price")

                        total_ind = 0
                        total_params = 0
                        rows_all = []

                        for tf in ("m5", "m15", "h1"):
                            instances = get_instances_by_tf(tf)
                            if not instances:
                                continue

                            bar_open_ms = floor_to_bar_ms(created_ms, tf)
                            step_ms = STEP_MS[tf]
                            start_ts = bar_open_ms - (REQUIRED_BARS_DEFAULT - 1) * step_ms

                            # загрузка OHLCV
                            fields = ["o", "h", "l", "c", "v"]
                            keys = {f: f"ts:{sym}:{tf}:{f}" for f in fields}
                            tasks = {f: redis.execute_command("TS.RANGE", keys[f], start_ts, bar_open_ms) for f in fields}
                            res = await asyncio.gather(*tasks.values(), return_exceptions=True)

                            series = {}
                            for f, r in zip(tasks.keys(), res):
                                if isinstance(r, Exception):
                                    log.warning(f"TS.RANGE {keys[f]} error: {r}")
                                    continue
                                if r:
                                    series[f] = {int(ts): float(val) for ts, val in r if val is not None}

                            if not series or "c" not in series:
                                log.warning(f"[SKIP] uid={uid} TF={tf} нет OHLCV для среза")
                                continue

                            idx = sorted(series["c"].keys())
                            df = {f: [series.get(f, {}).get(ts) for ts in idx] for f in fields}
                            pdf = pd.DataFrame(df, index=pd.to_datetime(idx, unit="ms"))
                            pdf.index.name = "open_time"

                            tf_inst_count = 0
                            tf_param_count = 0
                            rows = []

                            close_t = float(pdf["c"].iloc[-1])
                            close_prev = float(pdf["c"].iloc[-2]) if len(pdf) > 1 else None

                            # локальный кэш t-значений по инстансам
                            tf_cache_values: dict[int, dict[str, str]] = {}

                            # пробег по инстансам
                            for inst in instances:
                                en = inst.get("enabled_at")
                                if en and bar_open_ms < int(en.replace(tzinfo=None).timestamp() * 1000):
                                    continue

                                async with sem:
                                    values = await compute_snapshot_values_async(inst, sym, pdf, precision)
                                if not values:
                                    continue

                                try:
                                    tf_cache_values[int(inst["id"])] = values
                                except Exception:
                                    pass

                                tf_inst_count += 1
                                tf_param_count += len(values)

                                kv = ", ".join(f"{k}={v}" for k, v in values.items())
                                log.debug(f"[SNAPSHOT] uid={uid} TF={tf} inst={inst['id']} {kv}")

                                bar_open_dt = datetime.utcfromtimestamp(bar_open_ms / 1000)
                                enabled_at = inst.get("enabled_at")
                                params_json = json.dumps(inst.get("params", {}))
                                for pname, vstr in values.items():
                                    try:
                                        vnum = float(vstr)
                                    except Exception:
                                        vnum = None
                                    rows.append((
                                        uid, strat, side, tf,
                                        int(inst["id"]), pname, vstr, vnum,
                                        bar_open_dt,
                                        enabled_at,
                                        params_json
                                    ))

                                # EMA-status
                                if inst.get("indicator") == "ema":
                                    try:
                                        L = int(inst["params"].get("length"))
                                    except Exception:
                                        continue

                                    ema_t = None
                                    vals = tf_cache_values.get(int(inst["id"]))
                                    if vals and f"ema{L}" in vals:
                                        try:
                                            ema_t = float(vals[f"ema{L}"])
                                        except Exception:
                                            ema_t = None

                                    ema_p = None
                                    prev_ms = bar_open_ms - STEP_MS[tf]
                                    try:
                                        ema_p = await ts_get_point(redis, f"ts_ind:{sym}:{tf}:ema{L}", prev_ms)
                                    except Exception:
                                        ema_p = None
                                    if ema_p is None and len(pdf) > 1:
                                        async with sem:
                                            prev_vals = await compute_snapshot_values_async(inst, sym, pdf.iloc[:-1], precision)
                                        if prev_vals and f"ema{L}" in prev_vals:
                                            try:
                                                ema_p = float(prev_vals[f"ema{L}"])
                                            except Exception:
                                                ema_p = None

                                    # scale: high-low
                                    scale_t = None
                                    scale_prev = None
                                    try:
                                        scale_t = float(pdf["h"].iloc[-1]) - float(pdf["l"].iloc[-1])
                                        if len(pdf) > 1:
                                            scale_prev = float(pdf["h"].iloc[-2]) - float(pdf["l"].iloc[-2])
                                    except Exception:
                                        pass

                                    cls = _classify_with_prev(close_t, close_prev, ema_t, ema_p, scale_t, scale_prev, EPS0, EPS1, None)
                                    if cls is not None:
                                        code, label, nd, d, delta_d = cls
                                        rows.append((
                                            uid, strat, side, tf,
                                            int(inst["id"]), f"ema{L}_status", str(code), code,
                                            bar_open_dt,
                                            enabled_at,
                                            params_json
                                        ))

                            # EMA-паттерн
                            if entry_price is not None:
                                try:
                                    ema_map_for_tf: dict[str, float] = {}
                                    for inst in instances:
                                        if inst.get("indicator") != "ema":
                                            continue
                                        L = inst.get("params", {}).get("length")
                                        try:
                                            L = int(L)
                                        except Exception:
                                            continue
                                        if L not in (9, 21, 50, 100, 200):
                                            continue
                                        key = f"ema{L}"
                                        vals = tf_cache_values.get(int(inst["id"]))
                                        if vals and key in vals:
                                            try:
                                                ema_map_for_tf[key] = float(vals[key])
                                            except Exception:
                                                pass

                                    if all(name in ema_map_for_tf for name in EMA_NAMES):
                                        pattern_text = _build_emapattern_text(entry_price, ema_map_for_tf)
                                        pattern_id = _EMA_PATTERN_DICT.get(pattern_text)
                                        rows.append((
                                            uid, strat, side, tf,
                                            EMAPATTERN_INSTANCE_ID[tf], "emapattern",
                                            pattern_text,
                                            (pattern_id if pattern_id is not None else None),
                                            datetime.utcfromtimestamp(bar_open_ms / 1000),
                                            None,
                                            None
                                        ))
                                        tf_param_count += 1
                                        log.debug(f"[EMA_PATTERN] uid={uid} TF={tf} → {pattern_text} (id={pattern_id})")
                                    else:
                                        log.debug(f"[EMA_PATTERN_SKIP] uid={uid} TF={tf} неполный набор EMA: {sorted(ema_map_for_tf.keys())}")
                                except Exception:
                                    log.exception(f"[EMA_PATTERN_ERR] uid={uid} TF={tf}")

                            # публикация снапшота TF в общую память для пост-обработки
                            try:
                                if put_snapshot_tf is not None and rows:
                                    bar_iso = datetime.utcfromtimestamp(bar_open_ms / 1000).isoformat()
                                    payload_dict = {r[5]: r[6] for r in rows if r[5] and (r[6] is not None)}  # param_name -> value_str
                                    await put_snapshot_tf(
                                        position_uid=uid,
                                        log_uid=log_uid,
                                        strategy_id=strat,
                                        symbol=sym,
                                        direction=side,
                                        timeframe=tf,
                                        bar_open_time=bar_iso,
                                        payload=payload_dict,
                                        entry_price=entry_price
                                    )
                            except Exception:
                                log.exception(f"[SHM_PUT_ERR] uid={uid} TF={tf}")

                            # накопить строки TF в общий батч для PG
                            if rows:
                                rows_all.extend(rows)

                            bar_iso = datetime.utcfromtimestamp(bar_open_ms / 1000).isoformat()
                            log.debug(f"[SUMMARY] uid={uid} TF={tf} bar={bar_iso} indicators={tf_inst_count} params={tf_param_count}")
                            total_ind += tf_inst_count
                            total_params += tf_param_count

                        # запись в PG одним батчем
                        if rows_all:
                            async with pg.acquire() as conn:
                                async with conn.transaction():
                                    await conn.executemany(
                                        """
                                        INSERT INTO positions_indicators_stat
                                        (position_uid, strategy_id, direction, timeframe,
                                         instance_id, param_name, value_str, value_num,
                                         bar_open_time, enabled_at, params_json)
                                        VALUES
                                        ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                                        ON CONFLICT (position_uid, timeframe, instance_id, param_name, bar_open_time)
                                        DO NOTHING
                                        """,
                                        rows_all
                                    )

                        log.debug(f"[SUMMARY_ALL] uid={uid} indicators_total={total_ind} params_total={total_params}")

                    except Exception:
                        log.exception("Ошибка обработки события positions_open_stream")

            if to_ack:
                await redis.xack(STREAM, GROUP, *to_ack)

        except Exception as e:
            log.error(f"Ошибка в цикле IND_POS_SNAPSHOT: {e}", exc_info=True)
            await asyncio.sleep(2)