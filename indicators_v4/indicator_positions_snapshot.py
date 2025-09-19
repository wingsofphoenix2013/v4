# indicator_positions_snapshot.py — on-demand снимок индикаторов/динамик/MW при открытии позиции (m5/m15/h1)

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

# 🔸 Импорты packs и утилит
from packs.pack_utils import floor_to_bar, load_ohlcv_df
from packs.rsi_pack import build_rsi_pack
from packs.mfi_pack import build_mfi_pack
from packs.bb_pack import build_bb_pack
from packs.lr_pack import build_lr_pack
from packs.atr_pack import build_atr_pack
from packs.ema_pack import build_ema_pack
from packs.adx_dmi_pack import build_adx_dmi_pack
from packs.macd_pack import build_macd_pack
from packs.trend_pack import build_trend_pack
from packs.volatility_pack import build_volatility_pack
from packs.momentum_pack import build_momentum_pack
from packs.extremes_pack import build_extremes_pack

# 🔸 Константы стрима и параллелизма
STREAM_POSITIONS_OPEN = "positions_open_stream"
GROUP_SNAPSHOT = "pos_snap_group"
CONSUMER_SNAPSHOT = "pos_snap_1"
TF_LIST = ("m5", "m15", "h1")
MAX_CONCURRENCY = 8

# 🔸 Логгер
log = logging.getLogger("POS_SNAPSHOT")


# 🔸 Вспомогательные: кэшируемый вычислитель compute_fn на один TF/символ
def _make_compute_cached(
    compute_snapshot_values_async,
    symbol: str,
    tf: str,
    bar_open_ms: int,
    df,
    precision: int
):
    memo: Dict[Any, Dict[str, str]] = {}

    async def _compute(inst: dict) -> Dict[str, str]:
        # кэш-ключ по контракту инстанса
        key = (
            inst.get("indicator"),
            tuple(sorted((inst.get("params") or {}).items())),
            symbol,
            tf,
            bar_open_ms,
            precision,
        )
        if key in memo:
            return memo[key]
        res = await compute_snapshot_values_async(inst, symbol, df, precision)
        memo[key] = res or {}
        return memo[key]

    return _compute


# 🔸 Собрать packs для одного TF
async def _build_packs_for_tf(
    symbol: str,
    tf: str,
    now_ms: int,
    precision: int,
    redis,
    compute_fn_cached
) -> Dict[str, List[dict]]:
    # условия достаточности
    packs: Dict[str, List[dict]] = {
        "rsi": [],
        "mfi": [],
        "ema": [],
        "lr": [],
        "bb": [],
        "atr": [],
        "adx_dmi": [],
        "macd": [],
    }

    # RSI / MFI
    for L in (14, 21):
        p = await build_rsi_pack(symbol, tf, L, now_ms, precision, redis, compute_fn_cached)
        if p: packs["rsi"].append(p)
        p = await build_mfi_pack(symbol, tf, L, now_ms, precision, redis, compute_fn_cached)
        if p: packs["mfi"].append(p)

    # EMA
    for L in (21, 50, 200):
        p = await build_ema_pack(symbol, tf, L, now_ms, precision, redis, compute_fn_cached)
        if p: packs["ema"].append(p)

    # LR
    for L in (50, 100):
        p = await build_lr_pack(symbol, tf, L, now_ms, precision, redis, compute_fn_cached)
        if p: packs["lr"].append(p)

    # BB (20, 2.0)
    p = await build_bb_pack(symbol, tf, 20, 2.0, now_ms, precision, redis, compute_fn_cached)
    if p: packs["bb"].append(p)

    # ATR(14)
    p = await build_atr_pack(symbol, tf, 14, now_ms, precision, redis, compute_fn_cached)
    if p: packs["atr"].append(p)

    # ADX/DMI
    for L in (14, 21):
        p = await build_adx_dmi_pack(symbol, tf, L, now_ms, precision, redis, compute_fn_cached)
        if p: packs["adx_dmi"].append(p)

    # MACD (fast 12, 5)
    for F in (12, 5):
        p = await build_macd_pack(symbol, tf, F, now_ms, precision, redis, compute_fn_cached)
        if p: packs["macd"].append(p)

    return packs


# 🔸 Собрать MW packs (4 композита) для TF
async def _build_mw_for_tf(
    symbol: str,
    tf: str,
    now_ms: int,
    precision: int,
    redis,
    compute_fn_cached
) -> Dict[str, Optional[dict]]:
    # условия достаточности
    trend = await build_trend_pack(symbol, tf, now_ms, precision, redis, compute_fn_cached)
    vol   = await build_volatility_pack(symbol, tf, now_ms, precision, redis, compute_fn_cached)
    mom   = await build_momentum_pack(symbol, tf, now_ms, precision, redis, compute_fn_cached)
    ext   = await build_extremes_pack(symbol, tf, now_ms, precision, redis, compute_fn_cached)

    # в payload кладём только "pack"-часть
    return {
        "trend":      (trend or {}).get("pack") if trend else None,
        "volatility": (vol   or {}).get("pack") if vol   else None,
        "momentum":   (mom   or {}).get("pack") if mom   else None,
        "extremes":   (ext   or {}).get("pack") if ext   else None,
    }


# 🔸 Основная обработка одного сообщения (снимок по 3 TF)
async def _handle_snapshot_for_position(
    pg,
    redis,
    data: dict,
    get_instances_by_tf,
    get_precision,
    get_strategy_mw,
    compute_snapshot_values_async
):
    # условия достаточности
    try:
        strategy_id = int(data["strategy_id"])
        symbol      = data["symbol"]
        position_uid= data["position_uid"]
        direction   = data.get("direction")
        entry_price = data.get("entry_price")
        log_uid     = data.get("log_uid")
        route       = data.get("route")
        created_at  = data.get("created_at")
    except Exception:
        log.exception("❌ Некорректный payload positions_open_stream")
        return

    # фильтр по стратегиям с market_watcher
    if not get_strategy_mw(strategy_id):
        log.info(f"⏭️  Пропуск снимка: strategy_id={strategy_id} (market_watcher=false) pos={position_uid}")
        return

    precision = get_precision(symbol) or 8
    now_ms = int(datetime.utcnow().timestamp() * 1000)

    # по каждому TF снимаем raw + packs + MW
    for tf in TF_LIST:
        try:
            bar_open_ms = floor_to_bar(now_ms, tf)
            df = await load_ohlcv_df(redis, symbol, tf, bar_open_ms, 800)
            if df is None or df.empty:
                log.info(f"⚠️  DF пустой: {symbol}/{tf} pos={position_uid}")
                # продолжаем, всё равно зафиксируем «пустой» values/packs/mw
            compute_cached = _make_compute_cached(
                compute_snapshot_values_async, symbol, tf, bar_open_ms, df, precision
            )

            # raw значения по всем активным инстансам TF
            values: Dict[str, str] = {}
            instances = get_instances_by_tf(tf)
            for inst in instances:
                vals = await compute_cached(inst)
                if vals:
                    values.update(vals)

            # packs
            packs = await _build_packs_for_tf(symbol, tf, now_ms, precision, redis, compute_cached)

            # MW
            mw = await _build_mw_for_tf(symbol, tf, now_ms, precision, redis, compute_cached)

            # payload
            bar_open_iso = datetime.utcfromtimestamp(bar_open_ms / 1000).isoformat()
            payload = {
                "meta": {
                    "position_uid": position_uid,
                    "strategy_id": strategy_id,
                    "symbol": symbol,
                    "direction": direction,
                    "entry_price": entry_price,
                    "precision": precision,
                    "log_uid": log_uid,
                    "route": route,
                    "created_at": created_at,
                    "bar_open_iso": bar_open_iso,
                },
                "indicators": {
                    "values": values,
                    "instances": [
                        {
                            "id": iid,
                            "indicator": inst["indicator"],
                            "timeframe": inst["timeframe"],
                            "enabled_at": inst.get("enabled_at").isoformat() if inst.get("enabled_at") else None,
                            "params": inst.get("params", {}),
                        }
                        for iid, inst in getattr(instances, "items", lambda: [])()
                    ] if isinstance(instances, dict) else [
                        {
                            "id": inst_id if isinstance(inst, tuple) else inst.get("id"),
                            "indicator": (inst[1]["indicator"] if isinstance(inst, tuple) else inst["indicator"]),
                            "timeframe": (inst[1]["timeframe"] if isinstance(inst, tuple) else inst["timeframe"]),
                            "enabled_at": (
                                inst[1].get("enabled_at").isoformat()
                                if isinstance(inst, tuple) and inst[1].get("enabled_at")
                                else (inst.get("enabled_at").isoformat() if inst.get("enabled_at") else None)
                            ),
                            "params": (inst[1].get("params", {}) if isinstance(inst, tuple) else inst.get("params", {})),
                        }
                        for (inst_id, inst) in enumerate(instances)  # instances из indicators_v4_main.py — list[dict]
                    ],
                },
                "packs": {k: [p for p in v if p] for k, v in packs.items()},
                "mw": mw,
            }

            # вставка в PG (одна строка на TF)
            await pg.execute(
                """
                INSERT INTO position_open_snapshots_v1
                (position_uid, strategy_id, symbol, timeframe, bar_open_time, snapshot_time, source, payload)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                ON CONFLICT (position_uid, timeframe) DO UPDATE
                SET payload = EXCLUDED.payload, snapshot_time = EXCLUDED.snapshot_time, source = EXCLUDED.source
                """,
                position_uid,
                strategy_id,
                symbol,
                tf,
                datetime.utcfromtimestamp(bar_open_ms / 1000),
                datetime.utcnow(),
                "on_demand",
                json.dumps(payload),
            )

            # лог результата по TF
            packs_count = {k: len(v) for k, v in payload["packs"].items()}
            values_count = len(values)
            log.info(
                f"✅ SNAPSHOT pos={position_uid} {symbol}/{tf} "
                f"values={values_count} packs={packs_count} mw="
                f"{','.join([k for k,v in mw.items() if v]) or 'none'}"
            )

        except Exception:
            log.exception(f"❌ Ошибка снимка pos={position_uid} {symbol}/{tf}")


# 🔸 Обёртка для ack
async def _wrap_and_ack(
    pg,
    redis,
    record_id: str,
    data: dict,
    get_instances_by_tf,
    get_precision,
    get_strategy_mw,
    compute_snapshot_values_async
):
    try:
        await _handle_snapshot_for_position(
            pg,
            redis,
            data,
            get_instances_by_tf,
            get_precision,
            get_strategy_mw,
            compute_snapshot_values_async,
        )
    finally:
        # ack независимо от исхода — событие открытия не должно зависать
        try:
            await redis.xack(STREAM_POSITIONS_OPEN, GROUP_SNAPSHOT, record_id)
        except Exception:
            log.exception(f"❌ ACK error id={record_id}")


# 🔸 Главный воркер: слушает positions_open_stream и делает снимки
async def run_indicator_positions_snapshot(
    pg,
    redis,
    get_instances_by_tf,
    get_precision,
    get_strategy_mw,
    compute_snapshot_values_async
):
    # создать consumer group
    try:
        await redis.xgroup_create(STREAM_POSITIONS_OPEN, GROUP_SNAPSHOT, id="$", mkstream=True)
        log.debug(f"🔧 Группа {GROUP_SNAPSHOT} создана для {STREAM_POSITIONS_OPEN}")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug(f"ℹ️ Группа {GROUP_SNAPSHOT} уже существует")
        else:
            log.exception("❌ Ошибка создания Consumer Group для positions_open_stream")
            return

    # семафор параллельных снимков
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    log.debug(f"📡 Подписка через Consumer Group: {STREAM_POSITIONS_OPEN} → {GROUP_SNAPSHOT}")

    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=GROUP_SNAPSHOT,
                consumername=CONSUMER_SNAPSHOT,
                streams={STREAM_POSITIONS_OPEN: ">"},
                count=20,
                block=1000
            )

            if not entries:
                continue

            for _, records in entries:
                for record_id, data in records:
                    # условия достаточности
                    if not isinstance(data, dict):
                        try:
                            await redis.xack(STREAM_POSITIONS_OPEN, GROUP_SNAPSHOT, record_id)
                        except Exception:
                            log.exception(f"❌ ACK error (non-dict) id={record_id}")
                        continue

                    async def _run(record_id=record_id, data=data):
                        # вложенный уровень: ограничение параллельности
                        async with sem:
                            await _wrap_and_ack(
                                pg,
                                redis,
                                record_id,
                                data,
                                get_instances_by_tf,
                                get_precision,
                                get_strategy_mw,
                                compute_snapshot_values_async,
                            )

                    asyncio.create_task(_run())

        except Exception:
            log.exception("❌ Ошибка в основном цикле run_indicator_positions_snapshot")
            await asyncio.sleep(1)