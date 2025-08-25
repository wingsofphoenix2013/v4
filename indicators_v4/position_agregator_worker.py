# position_agregator_worker.py — воркер агрегации позиций (RSI, MFI, ADX, DMI-spread, EMA, KAMA, ATR, MACD)

import asyncio
import logging
import math
from decimal import Decimal, ROUND_FLOOR

log = logging.getLogger("IND_AGG")

STREAM   = "signal_log_queue"   # post-commit поток из модуля позиций
GROUP    = "indicators_agg_group"
CONSUMER = "ind_agg_1"

READ_COUNT = 50
READ_BLOCK_MS = 2000

# шаги/границы бинирования
RSI_BUCKET_STEP = 5
MFI_BUCKET_STEP = 5
ADX_BUCKET_STEP = 5
DMI_SPREAD_STEP = 5

EMA_KAMA_PCT_CLAMP = 5.0   # диапазон для signed_dist_pct: [-5.0%, 5.0%)
EMA_KAMA_PCT_STEP  = 0.1   # квантация вниз для EMA/KAMA: 0.1%

ATR_PCT_CLAMP = 10.0       # диапазон для atr_pct: [0.0%, 10.0%)
ATR_PCT_STEP  = 0.1        # квантация вниз для ATR: 0.1%

MACD_PCT_CLAMP = 2.0       # диапазон для hist_pct: [-2.0%, 2.0%)
MACD_PCT_STEP  = 0.1       # квантация вниз для MACD hist_pct: 0.1%
MACD_FLAT_EPS  = 0.05      # |hist_pct| < 0.05% → 'flat'


# 🔸 Загрузка позиции по uid из positions_v4
async def _fetch_position(pg, position_uid: str):
    async with pg.acquire() as conn:
        return await conn.fetchrow(
            """
            SELECT
                position_uid,
                strategy_id,
                status,
                direction,
                entry_price,
                pnl,
                audited,
                closed_at
            FROM positions_v4
            WHERE position_uid = $1
            """,
            position_uid,
        )


# 🔸 Единая выборка всех снимков позиции
async def _fetch_snapshots_all(pg, position_uid: str):
    async with pg.acquire() as conn:
        return await conn.fetch(
            """
            SELECT timeframe, param_name, value_num
            FROM positions_indicators_stat
            WHERE position_uid = $1
              AND value_num IS NOT NULL
            """,
            position_uid,
        )


# 🔸 Разбиение снимков по индикаторам по префиксу param_name
def _partition_snapshots_by_indicator(rows):
    buckets = {
        "rsi": [],
        "mfi": [],
        "adx_dmi": [],
        "ema": [],
        "kama": [],
        "atr": [],
        "macd": [],
        "bb": [],
        "lr": [],
    }
    for r in rows or []:
        p = r["param_name"]
        if   p.startswith("rsi"):     buckets["rsi"].append(r)
        elif p.startswith("mfi"):     buckets["mfi"].append(r)
        elif p.startswith("adx_dmi"): buckets["adx_dmi"].append(r)
        elif p.startswith("ema"):     buckets["ema"].append(r)
        elif p.startswith("kama"):    buckets["kama"].append(r)
        elif p.startswith("atr"):     buckets["atr"].append(r)
        elif p.startswith("macd"):    buckets["macd"].append(r)
        elif p.startswith("bb"):      buckets["bb"].append(r)
        elif p.startswith("lr"):      buckets["lr"].append(r)
    return buckets


# 🔸 Общая бин-функция для диапазона [0..100) (RSI/MFI/ADX)
def _bucket_0_100(value: float, step: int) -> int | None:
    try:
        v = float(value)
    except Exception:
        return None
    if not math.isfinite(v):
        return None
    if v < 0.0:
        v = 0.0
    if v >= 100.0:
        v = 99.9999
    return int(math.floor(v / step) * step)


# 🔸 Бин-функция для DMI spread в диапазоне [-100..100)
def _bucket_minus100_100(value: float, step: int) -> int | None:
    try:
        v = float(value)
    except Exception:
        return None
    if not math.isfinite(v):
        return None
    if v < -100.0:
        v = -100.0
    if v >= 100.0:
        v = 99.9999
    return int(math.floor(v / step) * step)


# 🔸 Квантация вниз к сетке 0.1% (EMA/KAMA) в пределах ±5.0% → (from, to)
def _pct_bin_range_ema_kama(val_pct: float) -> tuple[float, float] | None:
    try:
        v = float(val_pct)
    except Exception:
        return None
    if not math.isfinite(v):
        return None
    if v < -EMA_KAMA_PCT_CLAMP:
        v = -EMA_KAMA_PCT_CLAMP
    if v >= EMA_KAMA_PCT_CLAMP:
        v = EMA_KAMA_PCT_CLAMP - 1e-6
    d = Decimal(v).quantize(Decimal("0.1"), rounding=ROUND_FLOOR)
    frm = float(d)
    to  = float(d + Decimal("0.1"))
    return frm, to


# 🔸 Квантация вниз к сетке 0.1% (ATR) в пределах [0.0, 10.0) → (from, to)
def _pct_bin_range_atr(val_pct: float) -> tuple[float, float] | None:
    try:
        v = float(val_pct)
    except Exception:
        return None
    if not math.isfinite(v) or v < 0.0:
        v = 0.0
    if v >= ATR_PCT_CLAMP:
        v = ATR_PCT_CLAMP - 1e-6
    d = Decimal(v).quantize(Decimal("0.1"), rounding=ROUND_FLOOR)
    frm = float(d)
    to  = float(d + Decimal("0.1"))
    return frm, to


# 🔸 Квантация вниз к сетке 0.1% (MACD hist_pct) в пределах ±2.0% → (from, to)
def _pct_bin_range_macd(val_pct: float) -> tuple[float, float] | None:
    try:
        v = float(val_pct)
    except Exception:
        return None
    if not math.isfinite(v):
        return None
    if v < -MACD_PCT_CLAMP:
        v = -MACD_PCT_CLAMP
    if v >= MACD_PCT_CLAMP:
        v = MACD_PCT_CLAMP - 1e-6
    d = Decimal(v).quantize(Decimal("0.1"), rounding=ROUND_FLOOR)
    frm = float(d)
    to  = float(d + Decimal("0.1"))
    return frm, to


# 🔸 Сбор дельт по RSI для одной позиции (value_bin/value)
def _collect_rsi_deltas(snaps, strategy_id: int, pnl: float):
    deltas = []
    win = 1 if pnl is not None and float(pnl) > 0 else 0
    for s in snaps:
        tf = s["timeframe"]
        param = s["param_name"]
        value = s["value_num"]
        bucket = _bucket_0_100(value, RSI_BUCKET_STEP)
        if bucket is None:
            continue
        deltas.append({
            "strategy_id": strategy_id,
            "timeframe": tf,
            "indicator": "rsi",
            "param_name": param,
            "bucket_type": "value_bin",
            "bucket_key": "value",
            "bucket_int": bucket,
            "dc": 1,
            "dp": float(pnl) if pnl is not None else 0.0,
            "dw": win,
        })
    return deltas


# 🔸 Сбор дельт по MFI для одной позиции (value_bin/value)
def _collect_mfi_deltas(snaps, strategy_id: int, pnl: float):
    deltas = []
    win = 1 if pnl is not None and float(pnl) > 0 else 0
    for s in snaps:
        tf = s["timeframe"]
        param = s["param_name"]
        value = s["value_num"]
        bucket = _bucket_0_100(value, MFI_BUCKET_STEP)
        if bucket is None:
            continue
        deltas.append({
            "strategy_id": strategy_id,
            "timeframe": tf,
            "indicator": "mfi",
            "param_name": param,
            "bucket_type": "value_bin",
            "bucket_key": "value",
            "bucket_int": bucket,
            "dc": 1,
            "dp": float(pnl) if pnl is not None else 0.0,
            "dw": win,
        })
    return deltas


# 🔸 Парсинг base param_name для ADX_DMI: adx_dmi{L}_suffix → (base='adx_dmi{L}', suffix)
def _parse_adx_dmi_param_name(param_name: str) -> tuple[str | None, str | None]:
    try:
        if not param_name.startswith("adx_dmi"):
            return None, None
        idx = param_name.rfind("_")
        if idx == -1:
            return None, None
        base = param_name[:idx]
        suffix = param_name[idx+1:]
        return base, suffix
    except Exception:
        return None, None


# 🔸 Группировка снимков ADX_DMI по (timeframe, base='adx_dmi{L}')
def _group_adx_dmi(snaps):
    groups = {}
    for r in snaps:
        tf = r["timeframe"]
        param = r["param_name"]
        base, suffix = _parse_adx_dmi_param_name(param)
        if not base or not suffix:
            continue
        key = (tf, base)
        g = groups.get(key, {"adx": None, "plus_di": None, "minus_di": None})
        val = r["value_num"]
        if suffix == "adx":
            g["adx"] = val
        elif suffix == "plus_di":
            g["plus_di"] = val
        elif suffix == "minus_di":
            g["minus_di"] = val
        groups[key] = g
    return groups


# 🔸 Сбор дельт по ADX (value_bin/adx) и DMI-spread (value_bin/dmi_spread)
def _collect_adx_dmi_deltas(snaps, strategy_id: int, pnl: float):
    deltas = []
    win = 1 if pnl is not None and float(pnl) > 0 else 0
    groups = _group_adx_dmi(snaps)

    for (tf, base), vals in groups.items():
        adx = vals.get("adx")
        adx_bucket = _bucket_0_100(adx, ADX_BUCKET_STEP) if adx is not None else None
        if adx_bucket is not None:
            deltas.append({
                "strategy_id": strategy_id,
                "timeframe": tf,
                "indicator": "adx_dmi",
                "param_name": base,
                "bucket_type": "value_bin",
                "bucket_key": "adx",
                "bucket_int": adx_bucket,
                "dc": 1,
                "dp": float(pnl) if pnl is not None else 0.0,
                "dw": win,
            })

        plus_di = vals.get("plus_di")
        minus_di = vals.get("minus_di")
        if plus_di is not None and minus_di is not None:
            try:
                spread = float(plus_di) - float(minus_di)
            except Exception:
                spread = None
            bucket = _bucket_minus100_100(spread, DMI_SPREAD_STEP) if spread is not None else None
            if bucket is not None:
                deltas.append({
                    "strategy_id": strategy_id,
                    "timeframe": tf,
                    "indicator": "adx_dmi",
                    "param_name": base,
                    "bucket_type": "value_bin",
                    "bucket_key": "dmi_spread",
                    "bucket_int": bucket,
                    "dc": 1,
                    "dp": float(pnl) if pnl is not None else 0.0,
                    "dw": win,
                })

    return deltas


# 🔸 Сбор дельт по EMA (range/signed_dist_pct, шаг 0.1%, ±5%)
def _collect_ema_deltas(snaps, strategy_id: int, pnl: float, direction: str | None, entry_price) -> list:
    deltas = []
    if entry_price is None:
        return deltas
    try:
        ep = float(entry_price)
    except Exception:
        return deltas
    if not math.isfinite(ep) or ep <= 0:
        return deltas
    dir_sign = 1.0 if (direction or "").lower() == "long" else -1.0 if (direction or "").lower() == "short" else 1.0
    win = 1 if pnl is not None and float(pnl) > 0 else 0

    for s in snaps:
        tf = s["timeframe"]
        param = s["param_name"]
        val = s["value_num"]
        try:
            ema_val = float(val)
        except Exception:
            continue
        if not math.isfinite(ema_val):
            continue

        dist_pct = ((ep - ema_val) / ep) * 100.0
        signed = dist_pct * dir_sign
        rng = _pct_bin_range_ema_kama(signed)
        if not rng:
            continue
        frm, to = rng

        deltas.append({
            "strategy_id": strategy_id,
            "timeframe": tf,
            "indicator": "ema",
            "param_name": param,
            "bucket_type": "range",
            "bucket_key": "signed_dist_pct",
            "bucket_from": frm,
            "bucket_to": to,
            "dc": 1,
            "dp": float(pnl) if pnl is not None else 0.0,
            "dw": win,
        })
    return deltas


# 🔸 Сбор дельт по KAMA (range/signed_dist_pct, шаг 0.1%, ±5%)
def _collect_kama_deltas(snaps, strategy_id: int, pnl: float, direction: str | None, entry_price) -> list:
    deltas = []
    if entry_price is None:
        return deltas
    try:
        ep = float(entry_price)
    except Exception:
        return deltas
    if not math.isfinite(ep) or ep <= 0:
        return deltas
    dir_sign = 1.0 if (direction or "").lower() == "long" else -1.0 if (direction or "").lower() == "short" else 1.0
    win = 1 if pnl is not None and float(pnl) > 0 else 0

    for s in snaps:
        tf = s["timeframe"]
        param = s["param_name"]
        val = s["value_num"]
        try:
            kama_val = float(val)
        except Exception:
            continue
        if not math.isfinite(kama_val):
            continue

        dist_pct = ((ep - kama_val) / ep) * 100.0
        signed = dist_pct * dir_sign
        rng = _pct_bin_range_ema_kama(signed)
        if not rng:
            continue
        frm, to = rng

        deltas.append({
            "strategy_id": strategy_id,
            "timeframe": tf,
            "indicator": "kama",
            "param_name": param,
            "bucket_type": "range",
            "bucket_key": "signed_dist_pct",
            "bucket_from": frm,
            "bucket_to": to,
            "dc": 1,
            "dp": float(pnl) if pnl is not None else 0.0,
            "dw": win,
        })
    return deltas


# 🔸 Сбор дельт по ATR (range/atr_pct, шаг 0.1%, [0,10))
def _collect_atr_deltas(snaps, strategy_id: int, pnl: float, entry_price) -> list:
    deltas = []
    if entry_price is None:
        return deltas
    try:
        ep = float(entry_price)
    except Exception:
        return deltas
    if not math.isfinite(ep) or ep <= 0:
        return deltas
    win = 1 if pnl is not None and float(pnl) > 0 else 0

    for s in snaps:
        tf = s["timeframe"]
        param = s["param_name"]
        val = s["value_num"]
        try:
            atr_val = float(val)
        except Exception:
            continue
        if not math.isfinite(atr_val) or atr_val <= 0.0:
            continue

        atr_pct = (atr_val / ep) * 100.0
        rng = _pct_bin_range_atr(atr_pct)
        if not rng:
            continue
        frm, to = rng

        deltas.append({
            "strategy_id": strategy_id,
            "timeframe": tf,
            "indicator": "atr",
            "param_name": param,
            "bucket_type": "range",
            "bucket_key": "atr_pct",
            "bucket_from": frm,
            "bucket_to": to,
            "dc": 1,
            "dp": float(pnl) if pnl is not None else 0.0,
            "dw": win,
        })
    return deltas


# 🔸 Парсинг base/suffix для MACD: macd{F}_suffix → (base='macd{F}', suffix)
def _parse_macd_param_name(param_name: str) -> tuple[str | None, str | None]:
    try:
        if not param_name.startswith("macd"):
            return None, None
        idx = param_name.rfind("_")
        if idx == -1:
            return None, None
        base = param_name[:idx]
        suffix = param_name[idx+1:]
        return base, suffix
    except Exception:
        return None, None


# 🔸 Группировка снимков MACD по (timeframe, base='macd{F}')
def _group_macd(snaps):
    groups = {}
    for r in snaps:
        tf = r["timeframe"]
        param = r["param_name"]
        base, suffix = _parse_macd_param_name(param)
        if not base or not suffix:
            continue
        key = (tf, base)
        g = groups.get(key, {"macd": None, "signal": None, "hist": None})
        val = r["value_num"]
        if suffix == "macd":
            g["macd"] = val
        elif suffix == "macd_signal":
            g["signal"] = val
        elif suffix == "macd_hist":
            g["hist"] = val
        groups[key] = g
    return groups


# 🔸 Сбор дельт по MACD: category(hist_sign) + range(hist_pct)
def _collect_macd_deltas(snaps, strategy_id: int, pnl: float, entry_price) -> list:
    deltas = []
    if entry_price is None:
        return deltas
    try:
        ep = float(entry_price)
    except Exception:
        return deltas
    if not math.isfinite(ep) or ep <= 0:
        return deltas

    win = 1 if pnl is not None and float(pnl) > 0 else 0
    groups = _group_macd(snaps)

    for (tf, base), vals in groups.items():
        hist = vals.get("hist")
        if hist is None:
            macd_v = vals.get("macd")
            sig_v  = vals.get("signal")
            if macd_v is not None and sig_v is not None:
                try:
                    hist = float(macd_v) - float(sig_v)
                except Exception:
                    hist = None

        if hist is None or not math.isfinite(float(hist)):
            continue

        try:
            hist_f = float(hist)
        except Exception:
            continue

        hist_pct = (hist_f / ep) * 100.0
        # category: hist_sign
        if abs(hist_pct) < MACD_FLAT_EPS:
            cat = "flat"
        else:
            cat = "above" if hist_pct > 0 else "below"

        deltas.append({
            "strategy_id": strategy_id,
            "timeframe": tf,
            "indicator": "macd",
            "param_name": base,                 # 'macd{fast}'
            "bucket_type": "category",
            "bucket_key": "hist_sign",
            "bucket_text": cat,
            "dc": 1,
            "dp": float(pnl) if pnl is not None else 0.0,
            "dw": win,
        })

        # range: hist_pct
        rng = _pct_bin_range_macd(hist_pct)
        if rng:
            frm, to = rng
            deltas.append({
                "strategy_id": strategy_id,
                "timeframe": tf,
                "indicator": "macd",
                "param_name": base,
                "bucket_type": "range",
                "bucket_key": "hist_pct",
                "bucket_from": frm,
                "bucket_to": to,
                "dc": 1,
                "dp": float(pnl) if pnl is not None else 0.0,
                "dw": win,
            })

    return deltas


# 🔸 Применение дельт к таблице агрегатов (value_bin, range, category) и отметка audited
async def _apply_aggregates_and_mark_audited(pg, position_uid: str, deltas: list):
    if not deltas:
        async with pg.acquire() as conn:
            await conn.execute(
                "UPDATE positions_v4 SET audited = TRUE WHERE position_uid = $1 AND audited = FALSE",
                position_uid,
            )
        return

    async with pg.acquire() as conn:
        async with conn.transaction():
            # агрегация одинаковых ключей в пределах одной позиции
            agg = {}
            for d in deltas:
                bt = d["bucket_type"]
                if bt == "value_bin":
                    key = (d["strategy_id"], d["timeframe"], d["indicator"], d["param_name"],
                           "value_bin", d["bucket_key"], d.get("bucket_int"))
                elif bt == "range":
                    key = (d["strategy_id"], d["timeframe"], d["indicator"], d["param_name"],
                           "range", d["bucket_key"], float(d.get("bucket_from")), float(d.get("bucket_to")))
                else:  # category
                    key = (d["strategy_id"], d["timeframe"], d["indicator"], d["param_name"],
                           "category", d["bucket_key"], str(d.get("bucket_text")))
                cur = agg.get(key, {"dc": 0, "dp": 0.0, "dw": 0})
                cur["dc"] += d["dc"]
                cur["dp"] += d["dp"]
                cur["dw"] += d["dw"]
                agg[key] = cur

            for key, m in agg.items():
                strategy_id, timeframe, indicator, param_name, btype, bkey, bA, *rest = key
                dc, dp, dw = m["dc"], m["dp"], m["dw"]

                if btype == "value_bin":
                    bucket_int = bA
                    row = await conn.fetchrow(
                        """
                        SELECT id, positions_closed, pnl_sum, wins
                        FROM indicator_aggregates_v4
                        WHERE strategy_id = $1
                          AND timeframe   = $2
                          AND indicator   = $3
                          AND param_name  = $4
                          AND bucket_type = 'value_bin'
                          AND bucket_key  = $5
                          AND bucket_int  = $6
                        FOR UPDATE
                        """,
                        strategy_id, timeframe, indicator, param_name, bkey, bucket_int
                    )
                    if row:
                        new_count = int(row["positions_closed"]) + dc
                        new_pnl   = float(row["pnl_sum"]) + dp
                        new_wins  = int(row["wins"]) + dw
                        new_avg   = new_pnl / new_count if new_count else 0.0
                        new_wr    = (new_wins / new_count) if new_count else 0.0
                        await conn.execute(
                            """
                            UPDATE indicator_aggregates_v4
                            SET positions_closed = $1,
                                pnl_sum          = $2,
                                wins             = $3,
                                avg_pnl          = $4,
                                winrate          = $5,
                                updated_at       = NOW()
                            WHERE id = $6
                            """,
                            new_count, new_pnl, new_wins, new_avg, new_wr, row["id"]
                        )
                    else:
                        new_count = dc
                        new_pnl   = dp
                        new_wins  = dw
                        new_avg   = new_pnl / new_count if new_count else 0.0
                        new_wr    = (new_wins / new_count) if new_count else 0.0
                        await conn.execute(
                            """
                            INSERT INTO indicator_aggregates_v4 (
                                strategy_id, timeframe, indicator, param_name,
                                bucket_type, bucket_key, bucket_int,
                                positions_closed, pnl_sum, wins, avg_pnl, winrate, updated_at
                            ) VALUES ($1,$2,$3,$4,'value_bin',$5,$6,$7,$8,$9,$10,$11,NOW())
                            """,
                            strategy_id, timeframe, indicator, param_name,
                            bkey, bucket_int,
                            new_count, new_pnl, new_wins, new_avg, new_wr
                        )

                elif btype == "range":
                    bucket_from = bA
                    bucket_to   = rest[0]
                    row = await conn.fetchrow(
                        """
                        SELECT id, positions_closed, pnl_sum, wins
                        FROM indicator_aggregates_v4
                        WHERE strategy_id     = $1
                          AND timeframe       = $2
                          AND indicator       = $3
                          AND param_name      = $4
                          AND bucket_type     = 'range'
                          AND bucket_key      = $5
                          AND bucket_num_from = $6
                          AND bucket_num_to   = $7
                        FOR UPDATE
                        """,
                        strategy_id, timeframe, indicator, param_name, bkey, bucket_from, bucket_to
                    )
                    if row:
                        new_count = int(row["positions_closed"]) + dc
                        new_pnl   = float(row["pnl_sum"]) + dp
                        new_wins  = int(row["wins"]) + dw
                        new_avg   = new_pnl / new_count if new_count else 0.0
                        new_wr    = (new_wins / new_count) if new_count else 0.0
                        await conn.execute(
                            """
                            UPDATE indicator_aggregates_v4
                            SET positions_closed = $1,
                                pnl_sum          = $2,
                                wins             = $3,
                                avg_pnl          = $4,
                                winrate          = $5,
                                updated_at       = NOW()
                            WHERE id = $6
                            """,
                            new_count, new_pnl, new_wins, new_avg, new_wr, row["id"]
                        )
                    else:
                        new_count = dc
                        new_pnl   = dp
                        new_wins  = dw
                        new_avg   = new_pnl / new_count if new_count else 0.0
                        new_wr    = (new_wins / new_count) if new_count else 0.0
                        await conn.execute(
                            """
                            INSERT INTO indicator_aggregates_v4 (
                                strategy_id, timeframe, indicator, param_name,
                                bucket_type, bucket_key, bucket_num_from, bucket_num_to,
                                positions_closed, pnl_sum, wins, avg_pnl, winrate, updated_at
                            ) VALUES ($1,$2,$3,$4,'range',$5,$6,$7,$8,$9,$10,$11,$12,NOW())
                            """,
                            strategy_id, timeframe, indicator, param_name,
                            bkey, bucket_from, bucket_to,
                            new_count, new_pnl, new_wins, new_avg, new_wr
                        )

                else:  # category
                    bucket_text = bA
                    row = await conn.fetchrow(
                        """
                        SELECT id, positions_closed, pnl_sum, wins
                        FROM indicator_aggregates_v4
                        WHERE strategy_id  = $1
                          AND timeframe    = $2
                          AND indicator    = $3
                          AND param_name   = $4
                          AND bucket_type  = 'category'
                          AND bucket_key   = $5
                          AND bucket_text  = $6
                        FOR UPDATE
                        """,
                        strategy_id, timeframe, indicator, param_name, bkey, bucket_text
                    )
                    if row:
                        new_count = int(row["positions_closed"]) + dc
                        new_pnl   = float(row["pnl_sum"]) + dp
                        new_wins  = int(row["wins"]) + dw
                        new_avg   = new_pnl / new_count if new_count else 0.0
                        new_wr    = (new_wins / new_count) if new_count else 0.0
                        await conn.execute(
                            """
                            UPDATE indicator_aggregates_v4
                            SET positions_closed = $1,
                                pnl_sum          = $2,
                                wins             = $3,
                                avg_pnl          = $4,
                                winrate          = $5,
                                updated_at       = NOW()
                            WHERE id = $6
                            """,
                            new_count, new_pnl, new_wins, new_avg, new_wr, row["id"]
                        )
                    else:
                        new_count = dc
                        new_pnl   = dp
                        new_wins  = dw
                        new_avg   = new_pnl / new_count if new_count else 0.0
                        new_wr    = (new_wins / new_count) if new_count else 0.0
                        await conn.execute(
                            """
                            INSERT INTO indicator_aggregates_v4 (
                                strategy_id, timeframe, indicator, param_name,
                                bucket_type, bucket_key, bucket_text,
                                positions_closed, pnl_sum, wins, avg_pnl, winrate, updated_at
                            ) VALUES ($1,$2,$3,$4,'category',$5,$6,$7,$8,$9,$10,$11,NOW())
                            """,
                            strategy_id, timeframe, indicator, param_name,
                            bkey, bucket_text,
                            new_count, new_pnl, new_wins, new_avg, new_wr
                        )

            await conn.execute(
                "UPDATE positions_v4 SET audited = TRUE WHERE position_uid = $1 AND audited = FALSE",
                position_uid,
            )


# 🔸 Основной воркер: читаем закрытия, собираем дельты и пишем агрегаты
async def run_position_aggregator_worker(pg, redis):
    try:
        await redis.xgroup_create(STREAM, GROUP, id="$", mkstream=True)
        log.info(f"Группа {GROUP} создана для {STREAM}")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info(f"Группа {GROUP} уже существует")
        else:
            log.exception("Ошибка создания consumer group")
            return

    while True:
        try:
            resp = await redis.xreadgroup(
                groupname=GROUP,
                consumername=CONSUMER,
                streams={STREAM: ">"},
                count=READ_COUNT,
                block=READ_BLOCK_MS,
            )
            if not resp:
                continue

            to_ack = []
            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)
                    try:
                        status = data.get("status")
                        if status != "closed":
                            continue

                        uid = data.get("position_uid")
                        if not uid:
                            log.warning("[SKIP] closed status without position_uid")
                            continue

                        row = await _fetch_position(pg, uid)
                        if not row:
                            log.warning(f"[SKIP] position not found in DB: uid={uid}")
                            continue

                        if row["audited"]:
                            log.debug(f"[SKIP] uid={uid} already audited")
                            continue
                        if row["status"] != "closed" or row["pnl"] is None:
                            log.warning(f"[SKIP] uid={uid} post-commit status mismatch (status={row['status']}, pnl={row['pnl']})")
                            continue

                        strategy_id = row["strategy_id"]
                        pnl = float(row["pnl"]) if row["pnl"] is not None else 0.0
                        direction = row["direction"]
                        entry_price = row["entry_price"]

                        snaps_all = await _fetch_snapshots_all(pg, uid)
                        parts = _partition_snapshots_by_indicator(snaps_all)

                        deltas = []
                        if parts["rsi"]:
                            deltas += _collect_rsi_deltas(parts["rsi"], strategy_id, pnl)
                        if parts["mfi"]:
                            deltas += _collect_mfi_deltas(parts["mfi"], strategy_id, pnl)
                        if parts["adx_dmi"]:
                            deltas += _collect_adx_dmi_deltas(parts["adx_dmi"], strategy_id, pnl)
                        if parts["ema"]:
                            deltas += _collect_ema_deltas(parts["ema"], strategy_id, pnl, direction, entry_price)
                        if parts["kama"]:
                            deltas += _collect_kama_deltas(parts["kama"], strategy_id, pnl, direction, entry_price)
                        if parts["atr"]:
                            deltas += _collect_atr_deltas(parts["atr"], strategy_id, pnl, entry_price)
                        if parts["macd"]:
                            deltas += _collect_macd_deltas(parts["macd"], strategy_id, pnl, entry_price)

                        if not deltas:
                            log.info(f"[NO-AGG] uid={uid} → ставим audited=true без изменения агрегатов")
                            await _apply_aggregates_and_mark_audited(pg, uid, [])
                            continue

                        await _apply_aggregates_and_mark_audited(pg, uid, deltas)
                        log.info(f"[AGG] uid={uid} strategy={strategy_id} → записаны {len(deltas)} дельт (RSI/MFI/ADX/DMI/EMA/KAMA/ATR/MACD)")

                    except Exception:
                        log.exception("Ошибка обработки сообщения signal_log_queue")

            if to_ack:
                await redis.xack(STREAM, GROUP, *to_ack)

        except Exception as e:
            log.error(f"Ошибка в цикле IND_AGG: {e}", exc_info=True)
            await asyncio.sleep(2)