# position_snapshot_worker.py — чтение positions_open_stream и on-demand срез индикаторов + EMA-status

import asyncio
import logging
import json
from datetime import datetime

import pandas as pd
from indicators.compute_and_store import compute_snapshot_values_async
from indicators_ema_status import _classify_with_prev, EPS0, EPS1  # импортируем готовую классификацию

log = logging.getLogger("IND_POS_SNAPSHOT")

STREAM   = "positions_open_stream"
GROUP    = "indicators_position_group"
CONSUMER = "ind_pos_1"

STEP_MIN = {"m5": 5, "m15": 15, "h1": 60}
REQUIRED_BARS_DEFAULT = 800

# 🔸 Флор к началу бара TF (UTC, мс)
def floor_to_bar_ms(ts_ms: int, tf: str) -> int:
    step_ms = STEP_MIN[tf] * 60_000
    return (ts_ms // step_ms) * step_ms


# 🔸 Основной воркер
async def run_position_snapshot_worker(pg, redis, get_instances_by_tf, get_precision, get_strategy_mw=lambda _sid: True):
    try:
        await redis.xgroup_create(STREAM, GROUP, id="$", mkstream=True)
        log.debug(f"Группа {GROUP} создана для {STREAM}")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug(f"Группа {GROUP} уже существует")
        else:
            log.exception("Ошибка создания consumer group")
            return

    sem = asyncio.Semaphore(4)

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
                        uid   = data.get("position_uid")
                        sym   = data.get("symbol")
                        strat = int(data.get("strategy_id"))
                        side  = data.get("direction")
                        created_iso = data.get("created_at")

                        # 🔸 фильтр по стратегиям
                        try:
                            if not get_strategy_mw(strat):
                                log.debug(f"[SKIP] uid={uid} strategy_id={strat}: market_watcher=false")
                                continue
                        except Exception:
                            log.exception(f"[SKIP] uid={uid} strategy_id={strat}: ошибка проверки market_watcher")
                            continue

                        log.debug(f"[OPENED] uid={uid} {sym} strategy={strat} dir={side} created_at={created_iso}")

                        created_dt = datetime.fromisoformat(created_iso)
                        created_ms = int(created_dt.timestamp() * 1000)

                        precision = get_precision(sym)

                        total_ind = 0
                        total_params = 0

                        for tf in ("m5", "m15", "h1"):
                            instances = get_instances_by_tf(tf)
                            if not instances:
                                continue

                            bar_open_ms = floor_to_bar_ms(created_ms, tf)
                            step_ms = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}[tf]
                            start_ts = bar_open_ms - (REQUIRED_BARS_DEFAULT - 1) * step_ms

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
                            rows = []  # батч для БД

                            # значения close_t/prev для EMA-status
                            close_t = float(pdf["c"].iloc[-1])
                            if len(pdf) > 1:
                                close_prev = float(pdf["c"].iloc[-2])
                            else:
                                close_prev = None

                            for inst in instances:
                                en = inst.get("enabled_at")
                                if en and bar_open_ms < int(en.replace(tzinfo=None).timestamp() * 1000):
                                    continue

                                async with sem:
                                    values = await compute_snapshot_values_async(inst, sym, pdf, precision)

                                if not values:
                                    continue

                                tf_inst_count += 1
                                tf_param_count += len(values)

                                # лог по инстансу
                                kv = ", ".join(f"{k}={v}" for k, v in values.items())
                                log.debug(f"[SNAPSHOT] uid={uid} TF={tf} inst={inst['id']} {kv}")

                                # формируем строки для обычных индикаторов
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
                                        bar_open_dt,  # bar_open_time
                                        enabled_at,   # enabled_at
                                        params_json   # params_json
                                    ))

                                # 🔸 Дополнительно: EMA-status
                                if inst.get("indicator") == "ema":
                                    try:
                                        L = int(inst["params"].get("length"))
                                    except Exception:
                                        continue
                                    ema_t = None
                                    ema_p = None
                                    try:
                                        if f"ema{L}" in values:
                                            ema_t = float(values[f"ema{L}"])
                                    except Exception:
                                        ema_t = None
                                    # предыдущая EMA через pdf[:-1]
                                    if len(pdf) > 1:
                                        async with sem:
                                            prev_vals = await compute_snapshot_values_async(inst, sym, pdf.iloc[:-1], precision)
                                        if prev_vals and f"ema{L}" in prev_vals:
                                            try:
                                                ema_p = float(prev_vals[f"ema{L}"])
                                            except Exception:
                                                ema_p = None

                                    # scale_t/scale_prev пока берём из std dev диапазона (BB/ATR нужно подтянуть аналогично EMA-status)
                                    scale_t = None
                                    scale_prev = None
                                    # (упрощённо: ширина high-low как scale)
                                    try:
                                        scale_t = float(pdf["h"].iloc[-1]) - float(pdf["l"].iloc[-1])
                                        if len(pdf) > 1:
                                            scale_prev = float(pdf["h"].iloc[-2]) - float(pdf["l"].iloc[-2])
                                    except Exception:
                                        pass

                                    cls = _classify_with_prev(
                                        close_t, close_prev,
                                        ema_t, ema_p,
                                        scale_t, scale_prev,
                                        EPS0, EPS1,
                                        None
                                    )
                                    if cls is not None:
                                        code, label, nd, d, delta_d = cls
                                        rows.append((
                                            uid, strat, side, tf,
                                            int(inst["id"]), f"ema{L}_status", str(code), code,
                                            bar_open_dt,
                                            enabled_at,
                                            params_json
                                        ))

                            # запись в БД по TF одним батчем
                            if rows:
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
                                            rows
                                        )
                                        # 🔸 обновляем флаг в positions_v4
                                        await conn.execute(
                                            "UPDATE positions_v4 SET emastatus_checked = true WHERE position_uid = $1",
                                            uid
                                        )

                            bar_iso = datetime.utcfromtimestamp(bar_open_ms / 1000).isoformat()
                            log.debug(f"[SUMMARY] uid={uid} TF={tf} bar={bar_iso} indicators={tf_inst_count} params={tf_param_count}")
                            total_ind += tf_inst_count
                            total_params += tf_param_count

                        log.debug(f"[SUMMARY_ALL] uid={uid} indicators_total={total_ind} params_total={total_params}")

                    except Exception:
                        log.exception("Ошибка обработки события positions_open_stream")

            if to_ack:
                await redis.xack(STREAM, GROUP, *to_ack)

        except Exception as e:
            log.error(f"Ошибка в цикле IND_POS_SNAPSHOT: {e}", exc_info=True)
            await asyncio.sleep(2)