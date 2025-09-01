# 🔸 indicators_ema_status_live.py — live: on-demand EMA-status для всех тикеров раз в минуту, публикация в Redis KV

import os
import asyncio
import json
import logging
from datetime import datetime, timezone

# 🔸 Логгер
log = logging.getLogger("EMA_STATUS_LIVE")

# 🔸 Конфиг
EMA_LENS = [int(x) for x in os.getenv("EMA_STATUS_EMA_LENS", "9,21,50,100,200").split(",")]
INTERVAL_SEC = int(os.getenv("EMA_STATUS_LIVE_INTERVAL", "60"))    # период пересчёта
CONCURRENCY  = int(os.getenv("EMA_STATUS_LIVE_CONCURRENCY", "8"))  # параллелизм по тикерам
EPS0 = float(os.getenv("EMA_STATUS_EPS0", "0.05"))  # зона equal
EPS1 = float(os.getenv("EMA_STATUS_EPS1", "0.02"))  # значимое изменение ΔD

# 🔸 Ключи и группы on-demand
REQ_STREAM  = "indicator_request"
RESP_STREAM = "indicator_response"
RESP_GROUP  = os.getenv("EMA_STATUS_LIVE_RESP_GROUP", "ema_status_live")
RESP_CONSUM = os.getenv("EMA_STATUS_LIVE_CONSUMER",   "ema_status_live_1")

# 🔸 Маппинг кода → label
STATE_LABELS = {
    0: "below_away",
    1: "below_towards",
    2: "equal",
    3: "above_towards",
    4: "above_away",
}

# 🔸 Вспомогательные утилиты времени
def _floor_to_bar_ms(now_ms: int, tf: str) -> int:
    step = 300_000 if tf == "m5" else (900_000 if tf == "m15" else 3_600_000)
    return (now_ms // step) * step

def _prev_bar_ms(open_ms: int, tf: str) -> int:
    step = 300_000 if tf == "m5" else (900_000 if tf == "m15" else 3_600_000)
    return open_ms - step

def _to_ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

# 🔸 TS ключи (Close/EMA/ATR/BB)
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

# 🔸 Простой TS.RANGE точечного чтения
async def ts_get(redis, key: str, ts_ms: int):
    try:
        r = await redis.execute_command("TS.RANGE", key, ts_ms, ts_ms)
        if r and int(r[0][0]) == ts_ms:
            return float(r[0][1])
    except Exception as e:
        log.debug("[TSERR] key=%s err=%s", key, e)
    return None

# 🔸 Загрузка instance_id для on-demand (EMA/ATR/BB), кеш в памяти
async def load_instance_map(pg) -> dict:
    """
    Возвращает:
    {
      'm5':  {'ema': {9:id,...}, 'atr14': id_or_None, 'bb': id_bb},
      'm15': {...}, 'h1': {'ema': {...}, 'atr14': None, 'bb': id_bb}
    }
    """
    out = {'m5': {'ema': {}}, 'm15': {'ema': {}}, 'h1': {'ema': {}}}
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
            out[r['timeframe']].setdefault('ema', {})[int(r['len'])] = int(r['id'])
        # ATR14 (только m5/m15)
        rows = await conn.fetch("""
            SELECT i.id, i.timeframe
            FROM indicator_instances_v4 i
            JOIN indicator_parameters_v4 p ON p.instance_id=i.id AND p.param='length'
            WHERE i.enabled = true AND i.indicator='atr' AND i.timeframe IN ('m5','m15') AND p.value::int=14
        """)
        for r in rows:
            out[r['timeframe']]['atr14'] = int(r['id'])
        # BB 20/2.0 (все TF)
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
    # заполним отсутствующие atr14 None
    for tf in ('m5','m15','h1'):
        out[tf].setdefault('atr14', None)
    return out

# 🔸 Создание группы потребителей для indicator_response (идемпотентно)
async def ensure_resp_group(redis):
    try:
        await redis.xgroup_create(RESP_STREAM, RESP_GROUP, id="$", mkstream=True)
        log.info("✅ resp-group '%s' создана на '%s'", RESP_GROUP, RESP_STREAM)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug("ℹ️ resp-group '%s' уже существует", RESP_GROUP)
        else:
            log.exception("❌ XGROUP CREATE (resp) error: %s", e)
            raise

# 🔸 Отправить пакет on-demand запросов по TF (EMA/ATR/BB) на bar_open, вернуть {req_id: ('ema',L| 'atr'| 'bb')}
async def send_requests_for_tf(redis, symbol: str, tf: str, bar_open_ms: int, inst_map: dict) -> dict:
    pending = {}
    # EMA
    for L in EMA_LENS:
        inst_id = inst_map[tf]['ema'].get(L)
        if not inst_id: continue
        req_id = await redis.xadd(REQ_STREAM, {
            "symbol": symbol, "timeframe": tf, "instance_id": str(inst_id), "timestamp_ms": str(bar_open_ms)
        })
        pending[req_id] = ('ema', L)
    # ATR14 (m5/m15)
    if inst_map[tf].get('atr14'):
        req_id = await redis.xadd(REQ_STREAM, {
            "symbol": symbol, "timeframe": tf, "instance_id": str(inst_map[tf]['atr14']), "timestamp_ms": str(bar_open_ms)
        })
        pending[req_id] = ('atr', None)
    # BB 20/2.0 (upper/center/lower придут одним ответом)
    if inst_map[tf].get('bb'):
        req_id = await redis.xadd(REQ_STREAM, {
            "symbol": symbol, "timeframe": tf, "instance_id": str(inst_map[tf]['bb']), "timestamp_ms": str(bar_open_ms)
        })
        pending[req_id] = ('bb', None)
    return pending

# 🔸 Дождаться ответов на наш пакет req_id → собрать значения { 'ema':{L:val}, 'atr':val, 'bb':{'upper':..,'lower':..} }
async def collect_responses(redis, pending: dict, timeout_ms: int = 1500) -> dict:
    results = {'ema': {}, 'atr': None, 'bb': {}}
    want = set(pending.keys())
    # читаем ответ своей группой
    end = _to_ms(datetime.utcnow()) + timeout_ms
    while want and _to_ms(datetime.utcnow()) < end:
        resp = await redis.xreadgroup(RESP_GROUP, RESP_CONSUM, streams={RESP_STREAM: ">"}, count=100, block=200)
        if not resp:
            continue
        to_ack = []
        for _, messages in resp:
            for msg_id, data in messages:
                to_ack.append(msg_id)
                req_id = data.get("req_id")
                if req_id not in want:
                    continue
                status = data.get("status")
                if status != "ok":
                    want.discard(req_id)
                    continue
                try:
                    parsed = json.loads(data.get("results", "{}"))
                except Exception:
                    parsed = {}
                kind = pending[req_id]
                if kind[0] == 'ema':
                    # ожидаем ключ вида 'ema{L}' или 'ema'
                    val = None
                    # варианты ключей
                    for k in (f"ema{kind[1]}", "value"):
                        if k in parsed:
                            val = float(parsed[k])
                            break
                    if val is not None:
                        results['ema'][kind[1]] = val
                elif kind[0] == 'atr':
                    # 'atr14' или 'atr'
                    for k in ("atr14", "atr"):
                        if k in parsed:
                            results['atr'] = float(parsed[k])
                            break
                elif kind[0] == 'bb':
                    # bb20_2_0_{upper,center,lower}
                    for k in parsed.keys():
                        if "upper" in k:
                            results['bb']['upper'] = float(parsed[k])
                        elif "lower" in k:
                            results['bb']['lower'] = float(parsed[k])
                        elif "center" in k:
                            results['bb']['center'] = float(parsed[k])
                want.discard(req_id)
        if to_ack:
            try:
                await redis.xack(RESP_STREAM, RESP_GROUP, *to_ack)
            except Exception:
                pass
    return results

# 🔸 Классификация статуса (один TF×EMA)
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

# 🔸 Обработка одного символа: собрать on-demand по всем TF → рассчитать статусы → KV publish
async def process_symbol(pg, redis, symbol: str, inst_map: dict, price_key_fmt: str):
    now_ms = _to_ms(datetime.utcnow())
    tf_list = ('m5', 'm15', 'h1')
    tf_status: dict[str, dict[int, int]] = {tf: {} for tf in tf_list}  # tf -> {L -> code}

    # цена сейчас (из маркета/редиса)
    try:
        price_now = float(await redis.get(price_key_fmt.format(symbol=symbol)))
    except Exception:
        price_now = None

    if price_now is None:
        log.debug("[LIVE] %s: нет текущей цены → skip", symbol)
        return

    # ensure resp group
    await ensure_resp_group(redis)

    for tf in tf_list:
        bar_open = _floor_to_bar_ms(now_ms, tf)
        prev_ms  = _prev_bar_ms(bar_open, tf)

        # отправляем on-demand EMA/ATR/BB на текущий бар
        pending = await send_requests_for_tf(redis, symbol, tf, bar_open, inst_map)
        od = await collect_responses(redis, pending, timeout_ms=1500)

        # добираем prev из TS
        price_prev = await ts_get(redis, k_close(symbol, tf), prev_ms)
        scale_prev = None
        if tf in ('m5','m15'):
            atr_prev = await ts_get(redis, k_atr(symbol, tf), prev_ms)
            if atr_prev is not None and atr_prev > 0.0:
                scale_prev = atr_prev
            else:
                bbu_p = await ts_get(redis, k_bb(symbol, tf, 'upper'), prev_ms)
                bbl_p = await ts_get(redis, k_bb(symbol, tf, 'lower'), prev_ms)
                if bbu_p is not None and bbl_p is not None and (bbu_p - bbl_p) > 0.0:
                    scale_prev = bbu_p - bbl_p
        else:
            bbu_p = await ts_get(redis, k_bb(symbol, tf, 'upper'), prev_ms)
            bbl_p = await ts_get(redis, k_bb(symbol, tf, 'lower'), prev_ms)
            if bbu_p is not None and bbl_p is not None and (bbu_p - bbl_p) > 0.0:
                scale_prev = bbu_p - bbl_p

        # масштаб текущий из on-demand (m5/m15: ATR; если нет — bb width; h1: bb width)
        scale_now = None
        if tf in ('m5','m15'):
            if od.get('atr') is not None and od['atr'] > 0.0:
                scale_now = od['atr']
            else:
                bu = od['bb'].get('upper'); bl = od['bb'].get('lower')
                if bu is not None and bl is not None and (bu - bl) > 0.0:
                    scale_now = bu - bl
        else:
            bu = od['bb'].get('upper'); bl = od['bb'].get('lower')
            if bu is not None and bl is not None and (bu - bl) > 0.0:
                scale_now = bu - bl

        # для каждой EMA длины рассчитать код
        for L in EMA_LENS:
            ema_t = od['ema'].get(L)
            ema_p = await ts_get(redis, k_ema(symbol, tf, L), prev_ms)
            code = classify(price_now, price_prev, ema_t, ema_p, scale_now, scale_prev)
            if code is not None:
                tf_status[tf][L] = code
                # KV per-TF
                try:
                    await redis.set(kv_tf(symbol, tf, L), str(code))
                except Exception:
                    pass

    # композиты: соберём тройки m5-m15-h1 для каждой L
    for L in EMA_LENS:
        if all(L in tf_status[tf] for tf in ('m5','m15','h1')):
            triplet = f"{tf_status['m5'][L]}-{tf_status['m15'][L]}-{tf_status['h1'][L]}"
            try:
                await redis.set(kv_comp(symbol, L), triplet)
            except Exception:
                pass

# 🔸 Периодический цикл: каждые INTERVAL_SEC, параллелизм CONCURRENCY
async def run_indicators_ema_status_live(pg, redis):
    log.info("🚀 EMA Status LIVE: interval=%ds, concurrency=%d, eps0=%.3f eps1=%.3f",
             INTERVAL_SEC, CONCURRENCY, EPS0, EPS1)

    # загрузим инстансы on-demand
    inst_map = await load_instance_map(pg)
    # получим список активных тикеров
    async with pg.acquire() as conn:
        rows = await conn.fetch("""
            SELECT symbol
            FROM tickers_v4
            WHERE status='enabled' AND tradepermission='enabled'
        """)
    symbols = [r['symbol'] for r in rows]

    sem = asyncio.Semaphore(CONCURRENCY)
    price_key_fmt = os.getenv("EMA_STATUS_LIVE_PRICE_KEY_FMT", "mark:{symbol}:price")

    while True:
        started = datetime.utcnow()
        try:
            # параллельная обработка всех символов
            async def runner(sym: str):
                async with sem:
                    await process_symbol(pg, redis, sym, inst_map, price_key_fmt)

            tasks = [asyncio.create_task(runner(sym)) for sym in symbols]
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            log.exception("❌ LIVE loop error: %s", e)

        # мягкая задержка до следующей минуты
        elapsed = (datetime.utcnow() - started).total_seconds()
        sleep_sec = max(0.0, INTERVAL_SEC - elapsed)
        await asyncio.sleep(sleep_sec)