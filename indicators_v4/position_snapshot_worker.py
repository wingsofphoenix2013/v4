# position_snapshot_worker.py — воркер снапшотов индикаторов/паков/MW из live-ключей Redis (без gateway, без ретраев/таймаутов)

# 🔸 Импорты
import asyncio
import json
import logging
import re
from datetime import datetime, timezone

# 🔸 Логгер
log = logging.getLogger("POS_SNAPSHOT")

# 🔸 Константы/настройки воркера (без ENV)
REQ_STREAM_POSITIONS = "positions_open_stream"   # входной стрим открытых позиций

POS_CONCURRENCY       = 5                         # сколько позиций одновременно обрабатываем
BATCH_INSERT_SIZE     = 400                       # батч вставки в PG
POS_TFS_ORDER         = ["m5", "m15", "h1"]       # порядок TF (m5 всегда первым)
POS_DRY_RUN           = False                     # True = не пишем в PG, только логи
DB_UPSERT_TIMEOUT_SEC = 15.0                      # таймаут на один батч UPSERT в БД (сек)

# 🔸 Пакеты: какие поля писать в indicator_position_stat (param_type='pack')
PACK_WHITELIST = {
    "rsi":     ["bucket_low", "trend"],
    "mfi":     ["bucket_low", "trend"],
    "bb":      ["bucket", "bucket_delta", "bw_trend_strict", "bw_trend_smooth"],
    "lr":      ["bucket", "bucket_delta", "angle_trend"],
    "atr":     ["bucket", "bucket_delta"],
    "adx_dmi": ["adx_bucket_low", "adx_dynamic_strict", "adx_dynamic_smooth",
                "gap_bucket_low", "gap_dynamic_strict", "gap_dynamic_smooth"],
    "ema":     ["side", "dynamic", "dynamic_strict", "dynamic_smooth"],
    "macd":    ["mode", "cross", "zero_side", "hist_bucket_low_pct",
                "hist_trend_strict", "hist_trend_smooth"],
}

# 🔸 Типы MW и префиксы RAW
MW_TYPES = ("trend", "volatility", "momentum", "extremes")
RAW_PREFIXES = ("ema", "rsi", "mfi", "atr", "lr", "adx_dmi", "macd", "bb")
RAW_PREFIX_RE = re.compile(r'^(ema|rsi|mfi|atr|lr|adx_dmi|macd|bb)')

# 🔸 Вспомогательные: floor к началу бара по TF (ms → ms)
def floor_to_bar_ms(ts_ms: int, tf: str) -> int:
    step_map = {"m5": 5, "m15": 15, "h1": 60}
    step = step_map.get(tf)
    if not step:
        raise ValueError(f"unsupported tf: {tf}")
    step_ms = step * 60_000
    return (ts_ms // step_ms) * step_ms

# 🔸 Вспомогательные: ISO → ms (UTC-naive input → treat as UTC)
def iso_to_ms(iso_str: str) -> int:
    dt = datetime.fromisoformat(iso_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

# 🔸 Вспомогательные: извлечь базу RAW-индикатора из имени параметра (ema21 → ema; lr50_angle → lr)
def indicator_from_raw_name(pname: str) -> str:
    m = RAW_PREFIX_RE.match(pname)
    if m:
        return m.group(1)
    # fallback: до первого "_"
    return pname.split("_", 1)[0]

# 🔸 UPSERT строк в indicator_position_stat (батчами) с statement_timeout
async def upsert_rows(pg, rows: list[tuple]):
    if not rows:
        return
    sql = """
    INSERT INTO indicator_position_stat
      (position_uid, strategy_id, symbol, timeframe, param_type, param_base, param_name, value_num, value_text, open_time, status, error_code)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
    ON CONFLICT (position_uid, timeframe, param_type, param_base, param_name)
    DO UPDATE SET
      value_num = EXCLUDED.value_num,
      value_text = EXCLUDED.value_text,
      open_time = EXCLUDED.open_time,
      status = EXCLUDED.status,
      error_code = EXCLUDED.error_code,
      captured_at = NOW()
    """
    # подготовим строковый timeout для PG, например '15s'
    pg_stmt_timeout = f"{int(DB_UPSERT_TIMEOUT_SEC)}s"

    i = 0
    total = len(rows)
    async with pg.acquire() as conn:
        while i < total:
            chunk = rows[i:i+BATCH_INSERT_SIZE]
            async with conn.transaction():
                # локальный statement_timeout только на эту транзакцию
                try:
                    await conn.execute(f"SET LOCAL statement_timeout = '{pg_stmt_timeout}'")
                except Exception:
                    pass
                await conn.executemany(sql, chunk)
            i += len(chunk)

# 🔸 Сбор RAW значений из ind_live:{symbol}:{tf}:*
async def collect_live_raw_rows(redis, position_uid: str, strategy_id: int, symbol: str, tf: str, open_time_iso: str) -> list[tuple]:
    rows: list[tuple] = []
    when = datetime.fromisoformat(open_time_iso)

    # собираем все ключи одним SCAN и читаем пачкой
    raw_keys = [k async for k in redis.scan_iter(f"ind_live:{symbol}:{tf}:*")]
    if not raw_keys:
        return rows

    values = await redis.mget(raw_keys)
    for k, v in zip(raw_keys, values):
        if v is None:
            continue
        # имя параметра — хвост ключа: ema21 / lr50_angle / bb20_2_0_upper ...
        pname = k.split(":", 3)[-1]
        pbase = indicator_from_raw_name(pname)
        # попытка привести к float; иначе пишем как текст
        try:
            vnum, vtext = float(v), None
        except Exception:
            vnum, vtext = None, str(v)
        rows.append((
            position_uid, strategy_id, symbol, tf,
            "indicator", pbase, pname,
            vnum, vtext,
            when, "ok", None
        ))
    return rows

# 🔸 Сбор MW state из ind_mw_live:{symbol}:{tf}:{trend|volatility|momentum|extremes}
async def collect_live_mw_rows(redis, position_uid: str, strategy_id: int, symbol: str, tf: str, open_time_iso: str) -> list[tuple]:
    rows: list[tuple] = []
    when = datetime.fromisoformat(open_time_iso)

    for kind in MW_TYPES:
        payload = await redis.get(f"ind_mw_live:{symbol}:{tf}:{kind}")
        if not payload:
            # по условиям (TTL 90s, постоянный пересчёт) ключи должны быть; если нет — просто пропустим
            continue
        try:
            js = json.loads(payload)
            state = js.get("state")
            if state is None:
                continue
            rows.append((
                position_uid, strategy_id, symbol, tf,
                "marketwatch", kind, "state",
                None, str(state),
                when, "ok", None
            ))
        except Exception:
            # ошибки парсинга игнорируем, так как live-ключи считаются стабильными
            log.debug(f"[MW] parse skip {symbol}/{tf}/{kind}")
            continue
    return rows

# 🔸 Сбор PACK значений из pack_live:{indicator}:{symbol}:{tf}:{base}
async def collect_live_pack_rows(redis, position_uid: str, strategy_id: int, symbol: str, tf: str, open_time_iso: str) -> list[tuple]:
    rows: list[tuple] = []
    when = datetime.fromisoformat(open_time_iso)

    for ind in PACK_WHITELIST.keys():
        # собираем все базы для данного индикатора
        pack_keys = [k async for k in redis.scan_iter(f"pack_live:{ind}:{symbol}:{tf}:*")]
        if not pack_keys:
            continue

        pack_vals = await redis.mget(pack_keys)
        fields = PACK_WHITELIST.get(ind, [])
        for k, v in zip(pack_keys, pack_vals):
            if not v:
                continue
            base = k.split(":")[-1]  # напр. bb20_2_0 / ema21 / macd12
            try:
                js = json.loads(v)
                # допускаем как плоский pack (поля на верхнем уровне), так и вложенный {"pack": {...}}
                pack = js.get("pack", js)
                for fname in fields:
                    if fname in pack:
                        val = pack[fname]
                        try:
                            vnum, vtext = float(val), None
                        except Exception:
                            vnum, vtext = None, str(val)
                        rows.append((
                            position_uid, strategy_id, symbol, tf,
                            "pack", base, fname,
                            vnum, vtext,
                            when, "ok", None
                        ))
            except Exception:
                log.debug(f"[PACK] parse skip {symbol}/{tf}/{ind}/{base}")
                continue
    return rows

# 🔸 Обработка одного TF: сбор RAW/MW/PACK из live-ключей и UPSERT
async def process_tf_live(pg, redis, position_uid: str, strategy_id: int, symbol: str, tf: str, created_at_ms: int):
    # считаем формальный open_time свечи по TF
    bar_open_ms = floor_to_bar_ms(created_at_ms, tf)
    open_time_iso = datetime.utcfromtimestamp(bar_open_ms / 1000).isoformat()

    # собираем строки из трёх семейств live-ключей
    raw_rows  = await collect_live_raw_rows(redis, position_uid, strategy_id, symbol, tf, open_time_iso)
    mw_rows   = await collect_live_mw_rows(redis, position_uid, strategy_id, symbol, tf, open_time_iso)
    pack_rows = await collect_live_pack_rows(redis, position_uid, strategy_id, symbol, tf, open_time_iso)

    rows = raw_rows + mw_rows + pack_rows
    log.debug(f"[TF LIVE] {symbol}/{tf} rows={len(rows)} raw={len(raw_rows)} mw={len(mw_rows)} pack={len(pack_rows)}")

    if not POS_DRY_RUN and rows:
        await upsert_rows(pg, rows)

# 🔸 Обработка одной позиции: TF последовательно (m5 → m15 → h1)
async def process_position(pg, redis, get_strategy_mw, pos_payload: dict) -> None:
    # извлекаем базовые поля
    position_uid = pos_payload.get("position_uid")
    symbol = pos_payload.get("symbol")
    strategy_id = int(pos_payload.get("strategy_id")) if pos_payload.get("strategy_id") is not None else None
    created_at_iso = pos_payload.get("created_at")

    # мимнимальные проверки
    if not position_uid or not symbol or strategy_id is None or not created_at_iso:
        log.debug(f"[SKIP] bad position payload: {pos_payload}")
        return

    # фильтр по стратегиям: только те, где market_watcher=true
    try:
        if not get_strategy_mw(int(strategy_id)):
            log.debug(f"[SKIP] strategy_id={strategy_id} market_watcher=false")
            return
    except Exception:
        log.debug(f"[SKIP] strategy_id={strategy_id} (mw flag check failed)")
        return

    created_at_ms = iso_to_ms(created_at_iso)

    # последовательная обработка TF
    tfs = POS_TFS_ORDER[:] if POS_TFS_ORDER else ["m5", "m15", "h1"]
    if "m5" not in tfs:
        tfs.insert(0, "m5")

    total_rows = 0
    for tf in tfs:
        await process_tf_live(pg, redis, position_uid, strategy_id, symbol, tf, created_at_ms)
        # счётчики для лога на TF уровне уже пишутся; здесь оставим суммарный таймер при желании
        total_rows += 1  # условный счётчик пройденных TF

    log.debug(f"POS_SNAPSHOT LIVE uid={position_uid} sym={symbol} tfs={total_rows}")

# 🔸 Основной воркер: подписка на positions_open_stream; TF последовательно; без ретраев/таймаутов
async def run_position_snapshot_worker(pg, redis, get_instances_by_tf, get_precision, get_strategy_mw):
    log.debug("POS_SNAPSHOT LIVE: воркер запущен")

    group = "possnap_group"
    consumer = "possnap_1"

    # создать consumer-group (идемпотентно)
    try:
        await redis.xgroup_create(REQ_STREAM_POSITIONS, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create error: {e}")

    sem = asyncio.Semaphore(POS_CONCURRENCY)

    async def handle_one(msg_id: str, data: dict) -> str | None:
        # ограничение параллелизма по позициям
        async with sem:
            try:
                await process_position(pg, redis, get_strategy_mw, data)
            except Exception as e:
                log.warning(f"[POS] error {e}", exc_info=True)
            return msg_id

    while True:
        try:
            resp = await redis.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={REQ_STREAM_POSITIONS: ">"},
                count=50,
                block=2000
            )
        except Exception as e:
            log.error(f"POS_SNAPSHOT read error: {e}", exc_info=True)
            await asyncio.sleep(0.5)
            continue

        if not resp:
            continue

        try:
            tasks = []
            for _, messages in resp:
                for msg_id, data in messages:
                    # отправляем в обработку и после — ACK
                    tasks.append(asyncio.create_task(handle_one(msg_id, data)))

            done_ids = await asyncio.gather(*tasks, return_exceptions=False)
            ack_ids = [mid for mid in done_ids if mid]
            if ack_ids:
                await redis.xack(REQ_STREAM_POSITIONS, group, *ack_ids)
        except Exception as e:
            log.error(f"POS_SNAPSHOT batch error: {e}", exc_info=True)
            await asyncio.sleep(0.5)