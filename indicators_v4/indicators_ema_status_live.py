# 🔸 indicators_ema_status_live.py — live: on-demand EMA-status для всех тикеров раз в минуту, публикация в Redis KV (без групп)

import os
import asyncio
import json
import logging
from datetime import datetime, timezone

log = logging.getLogger("EMA_STATUS_LIVE")

# 🔸 Конфиг
EMA_LENS = [int(x) for x in os.getenv("EMA_STATUS_EMA_LENS", "9,21,50,100,200").split(",")]
INTERVAL_SEC = int(os.getenv("EMA_STATUS_LIVE_INTERVAL", "60"))
CONCURRENCY  = int(os.getenv("EMA_STATUS_LIVE_CONCURRENCY", "8"))
EPS0 = float(os.getenv("EMA_STATUS_EPS0", "0.05"))
EPS1 = float(os.getenv("EMA_STATUS_EPS1", "0.02"))
PRICE_KEY_FMT = os.getenv("EMA_STATUS_LIVE_PRICE_KEY_FMT", "mark:{symbol}:price")

REQ_STREAM  = "indicator_request"
RESP_STREAM = "indicator_response"

STATE_LABELS = {
    0: "below_away",
    1: "below_towards",
    2: "equal",
    3: "above_towards",
    4: "above_away",
}

# 🔸 утилиты времени
def _to_ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

def _floor_to_bar_ms(now_ms: int, tf: str) -> int:
    step = 300_000 if tf == "m5" else (900_000 if tf == "m15" else 3_600_000)
    return (now_ms // step) * step

def _prev_bar_ms(open_ms: int, tf: str) -> int:
    step = 300_000 if tf == "m5" else (900_000 if tf == "m15" else 3_600_000)
    return open_ms - step

# 🔸 TS ключи
def k_close(sym: str, tf: str) -> str:
    return f"ts:{sym}:{tf}:c"
def k_ema(sym: str, tf: str, L: int) -> str:
    return f"ts_ind:{sym}:{tf}:ema{L}"
def k_atr(sym: str, tf: str) -> str:
    return f"ts_ind:{sym}:{tf}:atr14"
def k_bb(sym: str, tf: str, part: str) -> str:
    return f"ts_ind:{sym}:{tf}:bb20_2_0_{part}"

# 🔸 KV ключи публикации live-статусов
def kv_tf(symbol: str, tf: str, L: int) -> str:
    return f"indlive:{symbol}:{tf}:ema{L}_status"
def kv_comp(symbol: str, L: int) -> str:
    return f"indlive:{symbol}:ema{L}_triplet"

# 🔸 простое точечное чтение из TS
async def ts_get(redis, key: str, ts_ms: int):
    try:
        r = await redis.execute_command("TS.RANGE", key, ts_ms, ts_ms)
        if r and int(r[0][0]) == ts_ms:
            return float(r[0][1])
    except Exception as e:
        log.debug("[TSERR] key=%s err=%s", key, e)
    return None

# 🔸 загрузка instance_id для on-demand (EMA/ATR/BB), кеш в памяти
async def load_instance_map(pg) -> dict:
    out = {'m5': {'ema': {}, 'atr14': None, 'bb': None},
           'm15': {'ema': {}, 'atr14': None, 'bb': None},
           'h1': {'ema': {}, 'atr14': None, 'bb': None}}
    async with pg.acquire() as conn:
        # EMA
        rows = await conn.fetch("""
            SELECT i.id, i.timeframe, p.value::int AS len
            FROM indicator_instances_v4 i
            JOIN indicator_parameters_v4 p ON p.instance_id=i.id AND p.param='length'
            WHERE i.enabled = true AND i.indicator='ema' AND i.timeframe IN ('m5','m15','h1')
              AND p.value::int = ANY($1::int[])
        """, EMA_LENS)
        for r in rows:
            out[r['timeframe']]['ema'][int(r['len'])] = int(r['id'])
        # ATR14 (m5/m15)
        rows = await conn.fetch("""
            SELECT i.id, i.timeframe
            FROM indicator_instances_v4 i
            JOIN indicator_parameters_v4 p ON p.instance_id=i.id AND p.param='length'
            WHERE i.enabled = true AND i.indicator='atr' AND i.timeframe IN ('m5','m15') AND p.value::int=14
        """)
        for r in rows:
            out[r['timeframe']]['atr14'] = int(r['id'])
        # BB 20/2.0
        rows = await conn.fetch("""
            SELECT i.id, i.timeframe
            FROM indicator_instances_v4 i
            JOIN indicator_parameters_v4 p1 ON p1.instance_id=i.id AND p1.param='length' AND p1.value::int=20
            JOIN indicator_parameters_v4 p2 ON p2.instance_id=i.id AND p2.param='std'
            WHERE i.enabled = true AND i.indicator='bb' AND i.timeframe IN ('m5','m15','h1')
              AND (p2.value::text='2.0' OR p2.value::text='2')
        """)
        for r in rows:
            out[r['timeframe']]['bb'] = int(r['id'])
    # лог диагностики
    for tf in ('m5','m15','h1'):
        miss = [L for L in EMA_LENS if L not in out[tf]['ema']]
        log.info("[LIVE] TF=%s EMA ids: %s; ATR14 id=%s; BB id=%s; missing EMA=%s",
                 tf, out[tf]['ema'], out[tf]['atr14'], out[tf]['bb'], miss)
    return out

# 🔸 отправить пакет on-demand запросов по TF на bar_open
async def send_requests_for_tf(redis, symbol: str, tf: str, bar_open_ms: int, inst_map: dict) -> dict:
    pending = {}
    # EMA
    for L in EMA_LENS:
        inst_id = inst_map[tf]['ema'].get(L)
        if not inst_id: continue
        req_id = await redis.xadd(REQ_STREAM, {
            "symbol": symbol, "timeframe": tf,
            "instance_id": str(inst_id), "timestamp_ms": str(bar_open_ms)
        })
        pending[req_id] = ('ema', L)
    # ATR14 (m5/m15)
    if tf in ('m5','m15') and inst_map[tf].get('atr14'):
        req_id = await redis.xadd(REQ_STREAM, {
            "symbol": symbol, "timeframe": tf,
            "instance_id": str(inst_map[tf]['atr14']), "timestamp_ms": str(bar_open_ms)
        })
        pending[req_id] = ('atr', None)
    # BB 20/2.0 (upper/center/lower придут одним ответом)
    if inst_map[tf].get('bb'):
        req_id = await redis.xadd(REQ_STREAM, {
            "symbol": symbol, "timeframe": tf,
            "instance_id": str(inst_map[tf]['bb']), "timestamp_ms": str(bar_open_ms)
        })
        pending[req_id] = ('bb', None)
    return pending

# 🔸 дождаться ответов по нашим req_id через XREAD (без consumer-group)
async def collect_responses(redis, pending: dict, timeout_ms: int = 1500) -> dict:
    results = {'ema': {}, 'atr': None, 'bb': {}}
    want = set(pending.keys())
    last_id = '$'  # читать только новые
    deadline = _to_ms(datetime.utcnow()) + timeout_ms

    while want and _to_ms(datetime.utcnow()) < deadline:
        try:
            resp = await redis.xread({RESP_STREAM: last_id}, count=200, block=200)
        except Exception:
            resp = None
        if not resp:
            continue
        for _, messages in resp:
            for msg_id, data in messages:
                last_id = msg_id  # продвигаем курсор
                req_id = data.get("req_id")
                if req_id not in want:
                    continue
                if data.get("status") != "ok":
                    want.discard(req_id)
                    continue
                try:
                    parsed = json.loads(data.get("results", "{}"))
                except Exception:
                    parsed = {}
                kind = pending[req_id]
                if kind[0] == 'ema':
                    val = None
                    for k in (f"ema{kind[1]}", "value"):
                        if k in parsed:
                            val = float(parsed[k]); break
                    if val is not None:
                        results['ema'][kind[1]] = val
                elif kind[0] == 'atr':
                    for k in ("atr14", "atr"):
                        if k in parsed:
                            results['atr'] = float(parsed[k]); break
                elif kind[0] == 'bb':
                    for k, v in parsed.items():
                        if "upper" in k:  results['bb']['upper']  = float(v)
                        elif "lower" in k:results['bb']['lower']  = float(v)
                        elif "center" in k:results['bb']['center'] = float(v)
                want.discard(req_id)
        # цикл пойдёт дальше до таймаута или пока все наши req_id не придут
    return results

# 🔸 классификация статуса (один TF×EMA)
def classify(price_t, price_p, ema_t, ema_p, scale_t, scale_p) -> int | None:
    if None in (price_t, price_p, ema_t, ema_p, scale_t, scale_p):
        return None
    if scale_t <= 0.0 or scale_p <= 0.0:
        return None
    nd_t = (price_t - ema_t) / scale_t
    nd_p = (price_p - ema_p) / scale_p
    d_t  = abs(nd_t)
    d_p  = abs(nd_p)
    delta_d = d_t - d_p
    if d_t <= EPS0:
        return 2
    above = nd_t > 0.0
    if delta_d >= EPS1:
        return 4 if above else 0
    elif delta_d <= -EPS1:
        return 3 if above else 1
    else:
        return 3 if above else 1  # консервативно towards

# 🔸 один символ целиком: on-demand по TF → статусы → KV
async def process_symbol(pg, redis, symbol: str, inst_map: dict):
    now_ms = _to_ms(datetime.utcnow())
    tf_list = ('m5','m15','h1')

    # цена сейчас
    price_now = await redis.get(PRICE_KEY_FMT.format(symbol=symbol))
    if price_now is None:
        log.debug("[LIVE] %s: нет текущей цены (%s) → skip", symbol, PRICE_KEY_FMT.format(symbol=symbol))
        return
    price_now = float(price_now)

    tf_status: dict[str, dict[int, int]] = {tf: {} for tf in tf_list}

    for tf in tf_list:
        # проверка, что есть нужные инстансы
        if not inst_map[tf]['ema'] or inst_map[tf]['bb'] is None or (tf in ('m5','m15') and inst_map[tf]['atr14'] is None):
            # ATR на h1 не обязателен
            if tf == 'h1' and inst_map[tf]['bb'] is not None and inst_map[tf]['ema']:
                pass
            else:
                log.debug("[LIVE] %s/%s: нет инстансов EMA/BB/ATR → skip", symbol, tf)
                continue

        bar_open = _floor_to_bar_ms(now_ms, tf)
        prev_ms  = _prev_bar_ms(bar_open, tf)

        # отправить on-demand для текущего бара
        pending = await send_requests_for_tf(redis, symbol, tf, bar_open, inst_map)
        od = await collect_responses(redis, pending, timeout_ms=1500)

        # prev из TS
        price_prev = await ts_get(redis, k_close(symbol, tf), prev_ms)
        # scale_prev
        if tf in ('m5','m15'):
            atr_prev = await ts_get(redis, k_atr(symbol, tf), prev_ms)
            if atr_prev is not None and atr_prev > 0.0:
                scale_prev = atr_prev
            else:
                bbu_p = await ts_get(redis, k_bb(symbol, tf, 'upper'), prev_ms)
                bbl_p = await ts_get(redis, k_bb(symbol, tf, 'lower'), prev_ms)
                scale_prev = (bbu_p - bbl_p) if (bbu_p is not None and bbl_p is not None and (bbu_p - bbl_p) > 0.0) else None
        else:
            bbu_p = await ts_get(redis, k_bb(symbol, tf, 'upper'), prev_ms)
            bbl_p = await ts_get(redis, k_bb(symbol, tf, 'lower'), prev_ms)
            scale_prev = (bbu_p - bbl_p) if (bbu_p is not None and bbl_p is not None and (bbu_p - bbl_p) > 0.0) else None

        # scale_now из on-demand
        if tf in ('m5','m15'):
            if od.get('atr') is not None and od['atr'] > 0.0:
                scale_now = od['atr']
            else:
                bu = od['bb'].get('upper'); bl = od['bb'].get('lower')
                scale_now = (bu - bl) if (bu is not None and bl is not None and (bu - bl) > 0.0) else None
        else:
            bu = od['bb'].get('upper'); bl = od['bb'].get('lower')
            scale_now = (bu - bl) if (bu is not None and bl is not None and (bu - bl) > 0.0) else None

        if price_prev is None or scale_prev is None or scale_now is None:
            log.debug("[LIVE] %s/%s: нет prev/scale → skip", symbol, tf)
            continue

        # статусы по всем EMA длинам
        for L in EMA_LENS:
            ema_now = od['ema'].get(L)
            ema_prev = await ts_get(redis, k_ema(symbol, tf, L), prev_ms)
            code = classify(price_now, price_prev, ema_now, ema_prev, scale_now, scale_prev)
            if code is None:
                continue
            tf_status[tf][L] = code
            try:
                await redis.set(kv_tf(symbol, tf, L), str(code))
            except Exception:
                pass

    # композиты
    for L in EMA_LENS:
        if all(L in tf_status[tf] for tf in ('m5','m15','h1')):
            triplet = f"{tf_status['m5'][L]}-{tf_status['m15'][L]}-{tf_status['h1'][L]}"
            try:
                await redis.set(kv_comp(symbol, L), triplet)
            except Exception:
                pass

# 🔸 основной цикл: раз в INTERVAL_SEC, параллельная обработка символов
async def run_indicators_ema_status_live(pg, redis):
    log.info("🚀 EMA Status LIVE: interval=%ds, concurrency=%d, eps0=%.3f eps1=%.3f",
             INTERVAL_SEC, CONCURRENCY, EPS0, EPS1)

    inst_map = await load_instance_map(pg)
    async with pg.acquire() as conn:
        rows = await conn.fetch("""
            SELECT symbol
            FROM tickers_v4
            WHERE status='enabled' AND tradepermission='enabled'
        """)
    symbols = [r['symbol'] for r in rows]
    sem = asyncio.Semaphore(CONCURRENCY)

    while True:
        start = datetime.utcnow()
        try:
            async def runner(sym: str):
                async with sem:
                    await process_symbol(pg, redis, sym, inst_map)
            tasks = [asyncio.create_task(runner(sym)) for sym in symbols]
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            log.exception("❌ LIVE loop error: %s", e)

        # ждём до следующего интервала
        elapsed = (datetime.utcnow() - start).total_seconds()
        sleep_s = max(0.0, INTERVAL_SEC - elapsed)
        await asyncio.sleep(sleep_s)