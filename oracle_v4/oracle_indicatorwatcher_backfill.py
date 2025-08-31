# 🔸 oracle_indicatorwatcher_backfill.py — IND-MW бэкофилл: батчи по 200, транзакционная догрузка (суммарные INFO-логи)

import os
import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import json
import time

import infra

# 🔸 Параметры бэкофилла
START_DELAY_SEC = int(os.getenv("ORACLE_IND_MW_BF_START_DELAY_SEC", "120"))   # 2 минуты до первого прогона
BF_BATCH_LIMIT = int(os.getenv("ORACLE_IND_MW_BF_BATCH_LIMIT", "200"))        # размер батча
BF_SLEEP_BETWEEN_BATCH_MS = int(os.getenv("ORACLE_IND_MW_BF_SLEEP_BETWEEN_BATCH_MS", "100"))
BF_MAX_RUN_SECONDS = int(os.getenv("ORACLE_IND_MW_BF_MAX_RUN_SECONDS", "1800"))  # бюджет на проход (10 мин)

log = logging.getLogger("ORACLE_IND_MW_BF")

# доступность индикаторов по TF
IND_TF_MAP = {
    "RSI": ("m5", "m15", "h1"),
    "MFI": ("m5", "m15"),           # нет на h1
    "ADX": ("m5", "m15"),           # нет на h1
    "BB" : ("m5", "m15", "h1"),
}
# param_name в positions_indicators_stat
PARAMS = {
    "RSI": ("rsi14",),
    "MFI": ("mfi14",),
    "ADX": ("adx_dmi14_adx",),
    "BB" : ("bb20_2_0_lower", "bb20_2_0_center", "bb20_2_0_upper"),
}

# 🔸 Вспомогательные: floor времени (UTC) под TF
def _floor_to_step_utc(dt: datetime, minutes: int) -> datetime:
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    base = dt.replace(second=0, microsecond=0)
    floored_minute = (base.minute // minutes) * minutes
    return base.replace(minute=floored_minute)

# 🔸 Бин для RSI/MFI/ADX (0..100, шаг 5)
def _bin_0_100_step5(val: float) -> str | None:
    if val is None:
        return None
    v = max(0.0, min(100.0, float(val)))
    return str(int(v // 5) * 5)

# 🔸 BB-сектор 1..12 по цене входа и уровням Bollinger 20/2.0
def _bb_sector(entry_price: float, lower: float, center: float, upper: float) -> str | None:
    try:
        p = float(entry_price); l = float(lower); u = float(upper)
    except Exception:
        return None
    width = u - l
    if width <= 0:
        return None
    sector_h = width / 6.0
    if p < l:
        below_idx = int((l - p) / sector_h + 0.999999)   # ceil
        return str(min(3, below_idx))                    # 1..3
    if p >= u:
        above_idx = int((p - u) / sector_h + 0.999999)   # ceil
        return str(min(12, 9 + above_idx))               # 10..12
    inside_idx = int((p - l) // sector_h)                # 0..5
    return str(4 + inside_idx)                           # 4..9

# 🔸 Redis ключи
def tf_stat_key(strategy_id: int, direction: str, marker3: str, indicator: str, tf: str, bucket: str) -> str:
    return f"oracle:indmw:stat:{strategy_id}:{direction}:{marker3}:{indicator}:{tf}:{bucket}"

def comp_stat_key(strategy_id: int, direction: str, marker3: str, indicator: str, triplet: str) -> str:
    return f"oracle:indmw:stat:{strategy_id}:{direction}:{marker3}:{indicator}:comp:{triplet}"

# 🔸 UPSERT помощники
async def _upsert_tf(conn, strategy_id: int, direction: str, marker3: str,
                     ind: str, tf: str, bucket: str, pnl: Decimal, is_win: int):
    stat = await conn.fetchrow("""
        SELECT closed_trades, won_trades, pnl_sum
        FROM positions_indicators_mw_stat_tf
        WHERE strategy_id=$1 AND direction=$2 AND marker3_code=$3
          AND indicator=$4 AND timeframe=$5 AND bucket=$6
        FOR UPDATE
    """, strategy_id, direction, marker3, ind, tf, bucket)
    if stat:
        c = int(stat["closed_trades"]) + 1
        w = int(stat["won_trades"]) + is_win
        s = (Decimal(str(stat["pnl_sum"])) + pnl).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    else:
        c, w, s = 1, is_win, pnl
    wr = (Decimal(w) / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    ap = (s / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    await conn.execute("""
        INSERT INTO positions_indicators_mw_stat_tf
          (strategy_id, direction, marker3_code, indicator, timeframe, bucket,
           closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
        ON CONFLICT (strategy_id, direction, marker3_code, indicator, timeframe, bucket)
        DO UPDATE SET
          closed_trades=$7, won_trades=$8, pnl_sum=$9, winrate=$10, avg_pnl=$11, updated_at=NOW()
    """, strategy_id, direction, marker3, ind, tf, bucket, c, w, str(s), str(wr), str(ap))
    # Redis
    try:
        value = json.dumps({"closed_trades": c, "winrate": float(wr)})
        await infra.redis_client.set(tf_stat_key(strategy_id, direction, marker3, ind, tf, bucket), value)
    except Exception:
        log.exception("Redis SET failed (per-TF)")

async def _upsert_comp(conn, strategy_id: int, direction: str, marker3: str,
                       ind: str, triplet: str, pnl: Decimal, is_win: int):
    stat = await conn.fetchrow("""
        SELECT closed_trades, won_trades, pnl_sum
        FROM positions_indicators_mw_stat_comp
        WHERE strategy_id=$1 AND direction=$2 AND marker3_code=$3
          AND indicator=$4 AND bucket_triplet=$5
        FOR UPDATE
    """, strategy_id, direction, marker3, ind, triplet)
    if stat:
        c = int(stat["closed_trades"]) + 1
        w = int(stat["won_trades"]) + is_win
        s = (Decimal(str(stat["pnl_sum"])) + pnl).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    else:
        c, w, s = 1, is_win, pnl
    wr = (Decimal(w) / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    ap = (s / Decimal(c)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    await conn.execute("""
        INSERT INTO positions_indicators_mw_stat_comp
          (strategy_id, direction, marker3_code, indicator, bucket_triplet,
           closed_trades, won_trades, pnl_sum, winrate, avg_pnl, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
        ON CONFLICT (strategy_id, direction, marker3_code, indicator, bucket_triplet)
        DO UPDATE SET
          closed_trades=$6, won_trades=$7, pnl_sum=$8, winrate=$9, avg_pnl=$10, updated_at=NOW()
    """, strategy_id, direction, marker3, ind, triplet, c, w, str(s), str(wr), str(ap))
    # Redis
    try:
        value = json.dumps({"closed_trades": c, "winrate": float(wr)})
        await infra.redis_client.set(comp_stat_key(strategy_id, direction, marker3, ind, triplet), value)
    except Exception:
        log.exception("Redis SET failed (comp)")

# 🔸 Транзакционная обработка одной позиции
async def _process_one_position(conn, position_uid: str) -> tuple[bool, str | None]:
    # позиция FOR UPDATE
    pos = await conn.fetchrow("""
        SELECT p.id, p.strategy_id, p.symbol, p.direction, p.created_at, p.entry_price, p.pnl, p.status,
               COALESCE(p.mrk_indwatch_checked, false) AS checked
        FROM positions_v4 p
        WHERE p.position_uid = $1
        FOR UPDATE
    """, position_uid)
    if not pos:
        return False, "not_found"
    if pos["status"] != "closed":
        return False, "not_closed"
    if pos["checked"]:
        return False, "already_checked"

    # стратегия активна и market_watcher=true?
    strat = await conn.fetchrow("""
        SELECT id, enabled, COALESCE(market_watcher, false) AS mw
        FROM strategies_v4
        WHERE id = $1
    """, int(pos["strategy_id"]))
    if not strat or not strat["enabled"] or not strat["mw"]:
        return False, "strategy_inactive"

    # вычисляем bar_open по TF
    created_at: datetime = pos["created_at"]
    m5_open  = _floor_to_step_utc(created_at, 5)
    m15_open = _floor_to_step_utc(created_at, 15)
    h1_open  = _floor_to_step_utc(created_at, 60)

    # market-флаг (3 из 3)
    rows = await conn.fetch("""
        SELECT timeframe, regime_code
        FROM indicator_marketwatcher_v4
        WHERE symbol = $1
          AND timeframe IN ('m5','m15','h1')
          AND open_time IN ($2, $3, $4)
    """, pos["symbol"], m5_open, m15_open, h1_open)
    markers = {r["timeframe"]: int(r["regime_code"]) for r in rows}
    if not all(tf in markers for tf in ("m5","m15","h1")):
        return False, "markers_missing"
    marker3 = f"{markers['m5']}-{markers['m15']}-{markers['h1']}"

    # читаем snapshots ровно TF→open_time и using_current_bar=true
    snaps = await conn.fetch("""
        SELECT timeframe, param_name, value_num
        FROM positions_indicators_stat
        WHERE position_uid = $1
          AND using_current_bar = true
          AND (
               (timeframe='m5'  AND bar_open_time=$2) OR
               (timeframe='m15' AND bar_open_time=$3) OR
               (timeframe='h1'  AND bar_open_time=$4)
          )
          AND param_name IN ('rsi14','mfi14','adx_dmi14_adx',
                             'bb20_2_0_center','bb20_2_0_upper','bb20_2_0_lower')
    """, position_uid, m5_open, m15_open, h1_open)

    vals = { 'm5': {}, 'm15': {}, 'h1': {} }
    for r in snaps:
        vals[r["timeframe"]][r["param_name"]] = r["value_num"]

    entry_price = float(pos["entry_price"])
    strategy_id = int(pos["strategy_id"])
    direction: str = pos["direction"]
    pnl = Decimal(str(pos["pnl"])).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    is_win = 1 if pnl > Decimal("0") else 0

    # --- PER-TF агрегаты ---
    rsi_bins, mfi_bins, adx_bins, bb_bins = {}, {}, {}, {}

    for tf in ("m5", "m15", "h1"):
        # RSI
        if tf in IND_TF_MAP["RSI"] and ("rsi14" in vals[tf]):
            b = _bin_0_100_step5(vals[tf]["rsi14"])
            if b is not None:
                await _upsert_tf(conn, strategy_id, direction, marker3, "RSI", tf, b, pnl, is_win)
                rsi_bins[tf] = b
        # MFI (нет на h1)
        if tf in IND_TF_MAP["MFI"] and ("mfi14" in vals[tf]):
            b = _bin_0_100_step5(vals[tf]["mfi14"])
            if b is not None:
                await _upsert_tf(conn, strategy_id, direction, marker3, "MFI", tf, b, pnl, is_win)
                mfi_bins[tf] = b
        # ADX (нет на h1)
        if tf in IND_TF_MAP["ADX"] and ("adx_dmi14_adx" in vals[tf]):
            b = _bin_0_100_step5(vals[tf]["adx_dmi14_adx"])
            if b is not None:
                await _upsert_tf(conn, strategy_id, direction, marker3, "ADX", tf, b, pnl, is_win)
                adx_bins[tf] = b
        # BB (нужны три уровня)
        if tf in IND_TF_MAP["BB"] and all(k in vals[tf] for k in PARAMS["BB"]):
            b = _bb_sector(entry_price, vals[tf]["bb20_2_0_lower"], vals[tf]["bb20_2_0_center"], vals[tf]["bb20_2_0_upper"])
            if b is not None:
                await _upsert_tf(conn, strategy_id, direction, marker3, "BB", tf, b, pnl, is_win)
                bb_bins[tf] = b

    # --- COMPOSITE агрегаты ---
    # RSI/BB → m5-m15-h1 если есть все три bins
    if all(tf in rsi_bins for tf in ("m5", "m15", "h1")):
        triplet = f"{rsi_bins['m5']}-{rsi_bins['m15']}-{rsi_bins['h1']}"
        await _upsert_comp(conn, strategy_id, direction, marker3, "RSI", triplet, pnl, is_win)
    if all(tf in bb_bins for tf in ("m5", "m15", "h1")):
        triplet = f"{bb_bins['m5']}-{bb_bins['m15']}-{bb_bins['h1']}"
        await _upsert_comp(conn, strategy_id, direction, marker3, "BB", triplet, pnl, is_win)

    # MFI/ADX → только m5-m15 в фиксированном порядке
    if all(tf in mfi_bins for tf in ("m5", "m15")):
        pair = f"{mfi_bins['m5']}-{mfi_bins['m15']}"
        await _upsert_comp(conn, strategy_id, direction, marker3, "MFI", pair, pnl, is_win)
    if all(tf in adx_bins for tf in ("m5", "m15")):
        pair = f"{adx_bins['m5']}-{adx_bins['m15']}"
        await _upsert_comp(conn, strategy_id, direction, marker3, "ADX", pair, pnl, is_win)

    wrote_any = any([rsi_bins, mfi_bins, adx_bins, bb_bins])
    if not wrote_any:
        return False, "no_bins"

    # отметить позицию как учтённую индикаторным агрегатором
    await conn.execute("""
        UPDATE positions_v4
        SET mrk_indwatch_checked = true
        WHERE position_uid = $1
    """, position_uid)

    return True, None


# 🔸 Один проход: батчи по BF_BATCH_LIMIT до исчерпания хвоста или по времени
async def run_oracle_indicatorwatcher_backfill_once():
    start_ts = time.monotonic()
    processed = 0
    deferred = 0
    batches = 0

    while True:
        if (time.monotonic() - start_ts) >= BF_MAX_RUN_SECONDS:
            log.info("[IND-BF] время вышло: processed=%d deferred=%d batches=%d", processed, deferred, batches)
            break

        async with infra.pg_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT p.position_uid
                FROM positions_v4 p
                JOIN strategies_v4 s ON s.id = p.strategy_id
                WHERE p.status = 'closed'
                  AND COALESCE(p.mrk_indwatch_checked, false) = false
                  AND s.enabled = true
                  AND COALESCE(s.market_watcher, false) = true
                ORDER BY p.created_at ASC
                LIMIT $1
            """, BF_BATCH_LIMIT)

            if not rows:
                log.info("[IND-BF] хвост пуст: processed=%d deferred=%d batches=%d", processed, deferred, batches)
                break

            for r in rows:
                uid = r["position_uid"]
                try:
                    async with conn.transaction():
                        ok, reason = await _process_one_position(conn, uid)
                    if ok:
                        processed += 1
                    else:
                        # no_bins / marker_missing — считаем deferred
                        if reason in ("no_bins", "marker_missing"):
                            deferred += 1
                except Exception as e:
                    log.exception("❌ Ошибка индикаторного бэкофилла, позиция %s: %s", uid, e)

        batches += 1
        log.info("[IND-BF] batch processed: %d (total %d), deferred=%d",
                 len(rows), processed, deferred)

        if len(rows) < BF_BATCH_LIMIT:
            log.info("[IND-BF] завершено: processed=%d deferred=%d batches=%d", processed, deferred, batches)
            break

        await asyncio.sleep(BF_SLEEP_BETWEEN_BATCH_MS / 1000)


# 🔸 Периодический цикл: старт через 2 минуты, затем каждый час
async def run_oracle_indicatorwatcher_backfill_periodic():
    log.info("🚀 IND-BF: старт через %d сек, батчи по %d, бюджет %d сек, затем каждый час",
             START_DELAY_SEC, BF_BATCH_LIMIT, BF_MAX_RUN_SECONDS)
    await asyncio.sleep(START_DELAY_SEC)

    while True:
        try:
            await run_oracle_indicatorwatcher_backfill_once()
        except asyncio.CancelledError:
            log.info("⏹️ Индикаторный бэкофилл остановлен")
            raise
        except Exception as e:
            log.exception("❌ Ошибка в индикаторном бэкофилле: %s", e)

        await asyncio.sleep(3600)  # 1 час