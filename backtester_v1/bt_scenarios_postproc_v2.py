# bt_scenarios_postproc_v2.py — постпроцессинг v2 (raw_stat + daily/stat), run-aware (consumer bt:scenarios:ready_v2)

# 🔸 Базовые импорты
import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, date
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Кеши backtester_v1 (инстансы индикаторов + параметры сценария)
from backtester_config import get_all_indicator_instances, get_scenario_instance

log = logging.getLogger("BT_SCENARIOS_POSTPROC_V2")

# 🔸 Настройки Decimal
getcontext().prec = 28

# 🔸 Константы стримов (v2)
POSTPROC_STREAM_KEY = "bt:scenarios:ready_v2"
POSTPROC_CONSUMER_GROUP = "bt_scenarios_postproc_v2"
POSTPROC_CONSUMER_NAME = "bt_scenarios_postproc_v2_main"

# 🔸 Стрим готовности постпроцессинга (оставляем как в v1)
POSTPROC_READY_STREAM_KEY = "bt:postproc:ready"

# 🔸 Настройки чтения стрима
POSTPROC_STREAM_BATCH_SIZE = 10
POSTPROC_STREAM_BLOCK_MS = 5000

# 🔸 Настройки обработки позиций (raw_stat)
POSTPROC_BATCH_SIZE = 200
POSTPROC_MAX_CONCURRENCY = 10

# 🔸 Таймшаги TF (в минутах) — для подбора open_time свечи, закрытой до decision_time
TF_STEP_MINUTES = {
    "m5": 5,
    "m15": 15,
    "h1": 60,
}

# 🔸 Таблицы v2
BT_POSITIONS_V2_TABLE = "bt_scenario_positions_v2"
BT_DAILY_V2_TABLE = "bt_scenario_daily_v2"
BT_STAT_V2_TABLE = "bt_scenario_stat_v2"

# 🔸 Таблицы индикаторов
INDICATOR_VALUES_TABLE = "indicator_values_v4"

# 🔸 Таблицы runs
BT_RUNS_TABLE = "bt_signal_backfill_runs"

# 🔸 Таблица membership окна run (истина состава окна)
BT_SIGNAL_MEMBERSHIP_TABLE = "bt_signals_membership"


# 🔸 Утилита: обрезка денег/метрик до 4 знаков после запятой
def _q_money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


# 🔸 Приведение значения к Decimal безопасно
def _to_dec(x: Any, default: str = "0") -> Decimal:
    try:
        if x is None:
            return Decimal(default)
        return Decimal(str(x))
    except Exception:
        return Decimal(default)


# 🔸 Публичная точка входа: оркестратор постпроцессинга v2
async def run_bt_scenarios_postproc_v2(pg, redis) -> None:
    log.debug("BT_SCENARIOS_POSTPROC_V2: воркер постпроцессинга v2 запущен")

    await _ensure_consumer_group(redis)

    indicator_by_tf, instances_by_id = _build_indicator_instances_cache()
    log.debug(
        "BT_SCENARIOS_POSTPROC_V2: кеш индикаторов загружен — TF=%s",
        {tf: len(instances) for tf, instances in indicator_by_tf.items()},
    )

    while True:
        try:
            messages = await _read_from_stream(redis)
            if not messages:
                continue

            total_msgs = 0
            total_runs = 0

            total_positions_processed = 0
            total_positions_skipped = 0
            total_errors = 0

            total_daily_upserted = 0
            total_stat_upserted = 0

            for stream_key, entries in messages:
                if stream_key != POSTPROC_STREAM_KEY:
                    continue

                for entry_id, fields in entries:
                    total_msgs += 1

                    ctx = _parse_ready_message(fields)
                    if not ctx:
                        await redis.xack(POSTPROC_STREAM_KEY, POSTPROC_CONSUMER_GROUP, entry_id)
                        continue

                    scenario_id = ctx["scenario_id"]
                    signal_id = ctx["signal_id"]
                    run_id = ctx["run_id"]
                    finished_at = ctx["finished_at"]

                    log.debug(
                        "BT_SCENARIOS_POSTPROC_V2: scenario ready — scenario_id=%s signal_id=%s run_id=%s finished_at=%s stream_id=%s",
                        scenario_id,
                        signal_id,
                        run_id,
                        finished_at,
                        entry_id,
                    )

                    t0 = time.perf_counter()

                    # raw_stat (positions)
                    processed, skipped, errors_positions = await _process_positions_raw_stat(
                        pg=pg,
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                        run_id=run_id,
                        indicator_by_tf=indicator_by_tf,
                        instances_by_id=instances_by_id,
                    )

                    # daily + stat (всегда считаем по всем closed позициям run, независимо от postproc_v2)
                    daily_upserted, stat_upserted, errors_agg = await _process_daily_and_stat(
                        pg=pg,
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                        run_id=run_id,
                    )

                    total_ms = int((time.perf_counter() - t0) * 1000)

                    total_runs += 1
                    total_positions_processed += processed
                    total_positions_skipped += skipped
                    total_errors += (errors_positions + errors_agg)
                    total_daily_upserted += daily_upserted
                    total_stat_upserted += stat_upserted

                    # итоговый лог по run (сводка)
                    log.info(
                        "BT_SCENARIOS_POSTPROC_V2: run summary scenario_id=%s signal_id=%s run_id=%s — "
                        "positions(processed=%s skipped=%s errors=%s) agg(daily_upserted=%s stat_upserted=%s agg_errors=%s) total_ms=%s",
                        scenario_id,
                        signal_id,
                        run_id,
                        processed,
                        skipped,
                        errors_positions,
                        daily_upserted,
                        stat_upserted,
                        errors_agg,
                        total_ms,
                    )

                    # postproc ready (как в v1, без доп.полей)
                    finished_at_postproc = datetime.utcnow()
                    try:
                        await redis.xadd(
                            POSTPROC_READY_STREAM_KEY,
                            {
                                "scenario_id": str(int(scenario_id)),
                                "signal_id": str(int(signal_id)),
                                "run_id": str(int(run_id)),
                                "processed": str(int(processed)),
                                "skipped": str(int(skipped)),
                                "errors": str(int(errors_positions + errors_agg)),
                                "finished_at": finished_at_postproc.isoformat(),
                            },
                        )
                    except Exception as e:
                        log.error(
                            "BT_SCENARIOS_POSTPROC_V2: не удалось опубликовать postproc ready: scenario_id=%s signal_id=%s run_id=%s: %s",
                            scenario_id,
                            signal_id,
                            run_id,
                            e,
                            exc_info=True,
                        )

                    await redis.xack(POSTPROC_STREAM_KEY, POSTPROC_CONSUMER_GROUP, entry_id)

            log.info(
                "BT_SCENARIOS_POSTPROC_V2: пакет обработан — сообщений=%s, run=%s, processed=%s, skipped=%s, errors=%s, daily_upserted=%s, stat_upserted=%s",
                total_msgs,
                total_runs,
                total_positions_processed,
                total_positions_skipped,
                total_errors,
                total_daily_upserted,
                total_stat_upserted,
            )

        except Exception as e:
            log.error("BT_SCENARIOS_POSTPROC_V2: ошибка в основном цикле воркера: %s", e, exc_info=True)
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима постпроцессинга v2
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=POSTPROC_STREAM_KEY,
            groupname=POSTPROC_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_SCENARIOS_POSTPROC_V2: создана consumer group '%s' для стрима '%s'",
            POSTPROC_CONSUMER_GROUP,
            POSTPROC_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_SCENARIOS_POSTPROC_V2: consumer group '%s' уже существует — SETID '$' (игнор истории до старта)",
                POSTPROC_CONSUMER_GROUP,
            )
            await redis.execute_command("XGROUP", "SETID", POSTPROC_STREAM_KEY, POSTPROC_CONSUMER_GROUP, "$")
        else:
            raise


# 🔸 Чтение сообщений из стрима bt:scenarios:ready_v2
async def _read_from_stream(redis) -> List[Any]:
    try:
        entries = await redis.xreadgroup(
            groupname=POSTPROC_CONSUMER_GROUP,
            consumername=POSTPROC_CONSUMER_NAME,
            streams={POSTPROC_STREAM_KEY: ">"},
            count=POSTPROC_STREAM_BATCH_SIZE,
            block=POSTPROC_STREAM_BLOCK_MS,
        )
    except Exception as e:
        msg = str(e)
        if "NOGROUP" in msg:
            log.warning("BT_SCENARIOS_POSTPROC_V2: NOGROUP при XREADGROUP — переинициализация группы")
            await _ensure_consumer_group(redis)
            return []
        raise

    if not entries:
        return []

    parsed: List[Any] = []
    for stream_key, messages in entries:
        if isinstance(stream_key, bytes):
            stream_key = stream_key.decode("utf-8")

        stream_entries: List[Any] = []
        for msg_id, fields in messages:
            if isinstance(msg_id, bytes):
                msg_id = msg_id.decode("utf-8")

            str_fields: Dict[str, str] = {}
            for k, v in fields.items():
                key_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                val_str = v.decode("utf-8") if isinstance(v, bytes) else str(v)
                str_fields[key_str] = val_str

            stream_entries.append((msg_id, str_fields))

        parsed.append((stream_key, stream_entries))

    return parsed


# 🔸 Разбор одного сообщения из стрима bt:scenarios:ready_v2
def _parse_ready_message(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id_str = fields.get("scenario_id")
        signal_id_str = fields.get("signal_id")
        run_id_str = fields.get("run_id")
        finished_at_str = fields.get("finished_at")

        # условия достаточности
        if not (scenario_id_str and signal_id_str and run_id_str and finished_at_str):
            return None

        scenario_id = int(scenario_id_str)
        signal_id = int(signal_id_str)
        run_id = int(run_id_str)
        finished_at = datetime.fromisoformat(finished_at_str)

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "run_id": run_id,
            "finished_at": finished_at,
        }
    except Exception:
        return None


# 🔸 Построение кеша инстансов индикаторов по TF
def _build_indicator_instances_cache() -> Tuple[Dict[str, Dict[int, Dict[str, Any]]], Dict[int, Dict[str, Any]]]:
    all_instances = get_all_indicator_instances()

    indicator_by_tf: Dict[str, Dict[int, Dict[str, Any]]] = {"m5": {}, "m15": {}, "h1": {}}
    instances_by_id: Dict[int, Dict[str, Any]] = {}

    for instance_id, inst in all_instances.items():
        tf = inst.get("timeframe")
        if tf not in indicator_by_tf:
            continue

        indicator_by_tf[tf][int(instance_id)] = inst
        instances_by_id[int(instance_id)] = inst

    return indicator_by_tf, instances_by_id


# 🔸 Обработка позиций: заполнение raw_stat + postproc_v2=true (только closed, opened_run_id=run_id)
async def _process_positions_raw_stat(
    pg,
    scenario_id: int,
    signal_id: int,
    run_id: int,
    indicator_by_tf: Dict[str, Dict[int, Dict[str, Any]]],
    instances_by_id: Dict[int, Dict[str, Any]],
) -> Tuple[int, int, int]:
    processed = 0
    skipped = 0
    errors = 0

    # выбираем позиции v2 только закрытые, относящиеся к run, и ещё не прошедшие postproc_v2
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                id,
                symbol,
                decision_time,
                entry_time
            FROM {BT_POSITIONS_V2_TABLE}
            WHERE scenario_id = $1
              AND signal_id = $2
              AND opened_run_id = $3
              AND status = 'closed'
              AND postproc_v2 = false
            ORDER BY entry_time
            """,
            int(scenario_id),
            int(signal_id),
            int(run_id),
        )

    positions: List[Dict[str, Any]] = [
        {
            "id": int(r["id"]),
            "symbol": str(r["symbol"]),
            "decision_time": r["decision_time"],
            "entry_time": r["entry_time"],
        }
        for r in rows
    ]

    if not positions:
        return processed, skipped, errors

    total_positions = len(positions)

    for i in range(0, total_positions, POSTPROC_BATCH_SIZE):
        batch = positions[i : i + POSTPROC_BATCH_SIZE]
        sema = asyncio.Semaphore(POSTPROC_MAX_CONCURRENCY)
        tasks: List[asyncio.Task] = []

        for pos in batch:
            tasks.append(
                asyncio.create_task(
                    _process_position_with_semaphore(pg, pos, indicator_by_tf, instances_by_id, sema),
                    name=f"BT_SCN_POSTPROC_V2_POS_{scenario_id}_{signal_id}_{run_id}",
                )
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in results:
            if isinstance(res, Exception):
                errors += 1
                continue
            if res == "processed":
                processed += 1
            elif res == "skipped":
                skipped += 1
            elif res == "error":
                errors += 1

    log.debug(
        "BT_SCENARIOS_POSTPROC_V2: raw_stat summary scenario_id=%s signal_id=%s run_id=%s positions=%s processed=%s skipped=%s errors=%s",
        scenario_id,
        signal_id,
        run_id,
        total_positions,
        processed,
        skipped,
        errors,
    )

    return processed, skipped, errors


# 🔸 Обёртка для ограничения параллелизма по позициям
async def _process_position_with_semaphore(
    pg,
    position: Dict[str, Any],
    indicator_by_tf: Dict[str, Dict[int, Dict[str, Any]]],
    instances_by_id: Dict[int, Dict[str, Any]],
    sema: asyncio.Semaphore,
) -> str:
    async with sema:
        try:
            return await _process_single_position(pg, position, indicator_by_tf, instances_by_id)
        except Exception as e:
            log.error(
                "BT_SCENARIOS_POSTPROC_V2: ошибка постпроцессинга позиции id=%s: %s",
                position.get("id"),
                e,
                exc_info=True,
            )
            return "error"


# 🔸 Постпроцессинг одной позиции v2: сбор индикаторов по TF на момент decision_time и запись raw_stat (version=v2)
async def _process_single_position(
    pg,
    position: Dict[str, Any],
    indicator_by_tf: Dict[str, Dict[int, Dict[str, Any]]],
    instances_by_id: Dict[int, Dict[str, Any]],
) -> str:
    pos_id = int(position["id"])
    symbol = str(position["symbol"])
    decision_time = position.get("decision_time")

    # условия достаточности
    if not symbol or not isinstance(decision_time, datetime):
        return "skipped"

    open_times = await _resolve_open_times_by_decision_time(pg, symbol, decision_time)
    if not open_times:
        return "skipped"

    tf_payload: Dict[str, Dict[str, Any]] = {}

    async with pg.acquire() as conn:
        for tf in ("m5", "m15", "h1"):
            tf_instances = indicator_by_tf.get(tf) or {}
            if not tf_instances:
                continue

            tf_open_time = open_times.get(tf)
            if tf_open_time is None:
                return "skipped"

            instance_ids = list(tf_instances.keys())

            rows = await conn.fetch(
                f"""
                SELECT instance_id, param_name, value
                FROM {INDICATOR_VALUES_TABLE}
                WHERE symbol = $1
                  AND open_time = $2
                  AND instance_id = ANY($3::int[])
                """,
                symbol,
                tf_open_time,
                instance_ids,
            )

            by_instance: Dict[int, List[Dict[str, Any]]] = {}
            for r in rows:
                iid = int(r["instance_id"])
                by_instance.setdefault(iid, []).append(
                    {"param_name": r["param_name"], "value": float(r["value"])}
                )

            tf_families: Dict[str, Dict[str, float]] = {}
            for iid, records in by_instance.items():
                inst = instances_by_id.get(iid)
                if not inst:
                    continue

                family = inst.get("indicator") or "unknown"
                family_dict = tf_families.setdefault(str(family), {})

                for rec in records:
                    family_dict[str(rec["param_name"])] = float(rec["value"])

            if tf_families:
                tf_payload[tf] = {"open_time": tf_open_time.isoformat(), "indicators": tf_families}

    if not tf_payload:
        return "skipped"

    raw_stat = {"version": "v2", "tf": tf_payload}
    raw_stat_json = json.dumps(raw_stat)

    async with pg.acquire() as conn:
        cmd = await conn.execute(
            f"""
            UPDATE {BT_POSITIONS_V2_TABLE}
            SET raw_stat = $1::jsonb,
                postproc_v2 = true,
                updated_at = now()
            WHERE id = $2
              AND postproc_v2 = false
            """,
            raw_stat_json,
            int(pos_id),
        )

    # условий достаточности
    if "UPDATE" not in str(cmd):
        return "skipped"

    return "processed"


# 🔸 Определение open_time для снапшота по decision_time (m5/m15/h1)
async def _resolve_open_times_by_decision_time(
    pg,
    symbol: str,
    decision_time: datetime,
) -> Optional[Dict[str, datetime]]:
    open_times: Dict[str, datetime] = {}

    async with pg.acquire() as conn:
        for tf in ("m5", "m15", "h1"):
            table_name = _ohlcv_table_for_timeframe(tf)
            if not table_name:
                continue

            step_minutes = TF_STEP_MINUTES.get(tf)
            if step_minutes is None:
                continue

            interval_str = f"{step_minutes} minutes"

            tf_row = await conn.fetchrow(
                f"""
                SELECT max(open_time) AS open_time
                FROM {table_name}
                WHERE symbol = $1
                  AND open_time + interval '{interval_str}' <= $2
                """,
                symbol,
                decision_time,
            )

            if not tf_row or tf_row["open_time"] is None:
                return None

            open_times[tf] = tf_row["open_time"]

    required_tfs = {"m5", "m15", "h1"}
    if not required_tfs.issubset(open_times.keys()):
        return None

    return open_times


# 🔸 Заполнение daily_v2 и stat_v2 по закрытым позициям run (day = exit_time::date)
async def _process_daily_and_stat(
    pg,
    scenario_id: int,
    signal_id: int,
    run_id: int,
) -> Tuple[int, int, int]:
    errors = 0

    # депозит из параметров сценария (ROI = pnl_abs / deposit)
    deposit = _get_scenario_deposit(scenario_id)
    if deposit is None or deposit <= Decimal("0"):
        log.error(
            "BT_SCENARIOS_POSTPROC_V2: не удалось получить deposit для scenario_id=%s — daily/stat не будут записаны",
            scenario_id,
        )
        return 0, 0, 1

    # окно run (для stat_v2)
    run_window = await _load_run_window(pg, run_id)
    if not run_window:
        log.error(
            "BT_SCENARIOS_POSTPROC_V2: не найден run window для run_id=%s — stat_v2 не будет записан",
            run_id,
        )
        errors += 1
        # daily можно посчитать без окна run, но оставим консистентно: если run отсутствует — это аномалия
        return 0, 0, errors

    window_from, window_to = run_window

    # позиции в окне run (истина окна = bt_signals_membership для (run_id, signal_id))
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                p.direction,
                p.pnl_abs,
                p.max_favorable_excursion,
                p.max_adverse_excursion,
                p.exit_time
            FROM {BT_SIGNAL_MEMBERSHIP_TABLE} m
            JOIN {BT_POSITIONS_V2_TABLE} p
              ON p.signal_value_id = m.signal_value_id
             AND p.scenario_id = $1
             AND p.signal_id = $2
            WHERE m.run_id = $3
              AND m.signal_id = $2
              AND p.status = 'closed'
            """,
            int(scenario_id),
            int(signal_id),
            int(run_id),
        )

    if not rows:
        return 0, 0, errors

    # агрегаты
    daily_map: Dict[Tuple[date, str], Dict[str, Any]] = {}
    stat_map: Dict[str, Dict[str, Any]] = {}

    for r in rows:
        direction = str(r["direction"] or "").strip().lower()
        exit_time = r["exit_time"]

        # условия достаточности
        if direction not in ("long", "short") or not isinstance(exit_time, datetime):
            continue

        d = exit_time.date()

        pnl = _to_dec(r["pnl_abs"], "0")
        mfe = _to_dec(r["max_favorable_excursion"], "0")
        mae = _to_dec(r["max_adverse_excursion"], "0")

        # daily
        dk = (d, direction)
        dd = daily_map.setdefault(
            dk,
            {"trades": 0, "pnl_sum": Decimal("0"), "wins": 0, "mfe_sum": Decimal("0"), "mae_sum": Decimal("0")},
        )
        dd["trades"] += 1
        dd["pnl_sum"] += pnl
        dd["mfe_sum"] += mfe
        dd["mae_sum"] += mae
        if pnl > Decimal("0"):
            dd["wins"] += 1

        # stat (по окну run)
        sd = stat_map.setdefault(
            direction,
            {"trades": 0, "pnl_sum": Decimal("0"), "wins": 0, "mfe_sum": Decimal("0"), "mae_sum": Decimal("0")},
        )
        sd["trades"] += 1
        sd["pnl_sum"] += pnl
        sd["mfe_sum"] += mfe
        sd["mae_sum"] += mae
        if pnl > Decimal("0"):
            sd["wins"] += 1

    # upsert daily_v2
    daily_rows: List[Tuple[int, int, int, date, str, int, Decimal, Decimal, Decimal, Decimal, Decimal]] = []
    for (d, direction), agg in daily_map.items():
        trades = int(agg["trades"])
        if trades <= 0:
            continue

        pnl_sum = _q_money(Decimal(agg["pnl_sum"]))
        wins = int(agg["wins"])
        winrate = _q_money(Decimal(wins) / Decimal(trades)) if trades > 0 else Decimal("0")
        roi = _q_money(pnl_sum / deposit) if deposit > Decimal("0") else Decimal("0")

        mfe_avg = _q_money(Decimal(agg["mfe_sum"]) / Decimal(trades)) if trades > 0 else Decimal("0")
        mae_avg = _q_money(Decimal(agg["mae_sum"]) / Decimal(trades)) if trades > 0 else Decimal("0")

        daily_rows.append(
            (
                int(scenario_id),
                int(signal_id),
                int(run_id),
                d,
                direction,
                trades,
                pnl_sum,
                winrate,
                roi,
                mfe_avg,
                mae_avg,
            )
        )

    daily_upserted = 0
    if daily_rows:
        try:
            daily_upserted = await _upsert_daily_v2_bulk(pg, daily_rows)
        except Exception as e:
            errors += 1
            log.error(
                "BT_SCENARIOS_POSTPROC_V2: ошибка upsert bt_scenario_daily_v2 (scenario_id=%s signal_id=%s run_id=%s): %s",
                scenario_id,
                signal_id,
                run_id,
                e,
                exc_info=True,
            )

    # upsert stat_v2
    stat_rows: List[Tuple[int, int, int, str, datetime, datetime, int, Decimal, Decimal, Decimal, Decimal, Decimal]] = []
    for direction, agg in stat_map.items():
        trades = int(agg["trades"])
        if trades <= 0:
            continue

        pnl_sum = _q_money(Decimal(agg["pnl_sum"]))
        wins = int(agg["wins"])
        winrate = _q_money(Decimal(wins) / Decimal(trades)) if trades > 0 else Decimal("0")
        roi = _q_money(pnl_sum / deposit) if deposit > Decimal("0") else Decimal("0")

        mfe_avg = _q_money(Decimal(agg["mfe_sum"]) / Decimal(trades)) if trades > 0 else Decimal("0")
        mae_avg = _q_money(Decimal(agg["mae_sum"]) / Decimal(trades)) if trades > 0 else Decimal("0")

        stat_rows.append(
            (
                int(scenario_id),
                int(signal_id),
                int(run_id),
                direction,
                window_from,
                window_to,
                trades,
                pnl_sum,
                winrate,
                roi,
                mfe_avg,
                mae_avg,
            )
        )

    stat_upserted = 0
    if stat_rows:
        try:
            stat_upserted = await _upsert_stat_v2_bulk(pg, stat_rows)
        except Exception as e:
            errors += 1
            log.error(
                "BT_SCENARIOS_POSTPROC_V2: ошибка upsert bt_scenario_stat_v2 (scenario_id=%s signal_id=%s run_id=%s): %s",
                scenario_id,
                signal_id,
                run_id,
                e,
                exc_info=True,
            )

    return daily_upserted, stat_upserted, errors


# 🔸 Получение deposit из параметров сценария (ROI = pnl_abs / deposit)
def _get_scenario_deposit(scenario_id: int) -> Optional[Decimal]:
    scenario = get_scenario_instance(int(scenario_id))
    if not scenario:
        return None

    params = scenario.get("params") or {}
    dep_cfg = params.get("deposit")
    dep_val = (dep_cfg or {}).get("value")
    if dep_val is None:
        return None

    try:
        return Decimal(str(dep_val))
    except Exception:
        return None


# 🔸 Загрузка окна run из bt_signal_backfill_runs
async def _load_run_window(pg, run_id: int) -> Optional[Tuple[datetime, datetime]]:
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT from_time, to_time
            FROM {BT_RUNS_TABLE}
            WHERE id = $1
            """,
            int(run_id),
        )

    if not row:
        return None

    wf = row["from_time"]
    wt = row["to_time"]
    if not isinstance(wf, datetime) or not isinstance(wt, datetime):
        return None
    if wf >= wt:
        return None

    return wf, wt


# 🔸 Bulk upsert bt_scenario_daily_v2
async def _upsert_daily_v2_bulk(
    pg,
    rows: List[Tuple[int, int, int, date, str, int, Decimal, Decimal, Decimal, Decimal, Decimal]],
) -> int:
    # rows:
    # (scenario_id, signal_id, run_id, day, direction, trades, pnl_abs, winrate, roi, mfe_avg, mae_avg)
    if not rows:
        return 0

    scenario_ids: List[int] = []
    signal_ids: List[int] = []
    run_ids: List[int] = []
    days: List[date] = []
    directions: List[str] = []
    trades: List[int] = []
    pnl_abs: List[Decimal] = []
    winrate: List[Decimal] = []
    roi: List[Decimal] = []
    mfe_avg: List[Decimal] = []
    mae_avg: List[Decimal] = []

    for (scid, sgid, rid, d, direction, tr, pnl, wr, r, mfe, mae) in rows:
        scenario_ids.append(int(scid))
        signal_ids.append(int(sgid))
        run_ids.append(int(rid))
        days.append(d)
        directions.append(str(direction))
        trades.append(int(tr))
        pnl_abs.append(Decimal(pnl))
        winrate.append(Decimal(wr))
        roi.append(Decimal(r))
        mfe_avg.append(Decimal(mfe))
        mae_avg.append(Decimal(mae))

    async with pg.acquire() as conn:
        affected = await conn.fetch(
            f"""
            INSERT INTO {BT_DAILY_V2_TABLE} (
                scenario_id,
                signal_id,
                run_id,
                day,
                direction,
                trades,
                pnl_abs,
                winrate,
                roi,
                max_favorable_excursion_avg,
                max_adverse_excursion_avg,
                raw_stat,
                created_at,
                updated_at
            )
            SELECT
                u.scenario_id,
                u.signal_id,
                u.run_id,
                u.day,
                u.direction,
                u.trades,
                u.pnl_abs,
                u.winrate,
                u.roi,
                u.mfe_avg,
                u.mae_avg,
                NULL::jsonb,
                now(),
                now()
            FROM unnest(
                $1::int[],
                $2::int[],
                $3::bigint[],
                $4::date[],
                $5::text[],
                $6::int[],
                $7::numeric[],
                $8::numeric[],
                $9::numeric[],
                $10::numeric[],
                $11::numeric[]
            ) AS u(
                scenario_id,
                signal_id,
                run_id,
                day,
                direction,
                trades,
                pnl_abs,
                winrate,
                roi,
                mfe_avg,
                mae_avg
            )
            ON CONFLICT (scenario_id, signal_id, run_id, day, direction)
            DO UPDATE SET
                trades = EXCLUDED.trades,
                pnl_abs = EXCLUDED.pnl_abs,
                winrate = EXCLUDED.winrate,
                roi = EXCLUDED.roi,
                max_favorable_excursion_avg = EXCLUDED.max_favorable_excursion_avg,
                max_adverse_excursion_avg = EXCLUDED.max_adverse_excursion_avg,
                updated_at = now()
            RETURNING id
            """,
            scenario_ids,
            signal_ids,
            run_ids,
            days,
            directions,
            trades,
            pnl_abs,
            winrate,
            roi,
            mfe_avg,
            mae_avg,
        )

    return len(affected)


# 🔸 Bulk upsert bt_scenario_stat_v2
async def _upsert_stat_v2_bulk(
    pg,
    rows: List[Tuple[int, int, int, str, datetime, datetime, int, Decimal, Decimal, Decimal, Decimal, Decimal]],
) -> int:
    # rows:
    # (scenario_id, signal_id, run_id, direction, window_from, window_to, trades, pnl_abs, winrate, roi, mfe_avg, mae_avg)
    if not rows:
        return 0

    scenario_ids: List[int] = []
    signal_ids: List[int] = []
    run_ids: List[int] = []
    directions: List[str] = []
    window_froms: List[datetime] = []
    window_tos: List[datetime] = []
    trades: List[int] = []
    pnl_abs: List[Decimal] = []
    winrate: List[Decimal] = []
    roi: List[Decimal] = []
    mfe_avg: List[Decimal] = []
    mae_avg: List[Decimal] = []

    for (scid, sgid, rid, direction, wf, wt, tr, pnl, wr, r, mfe, mae) in rows:
        scenario_ids.append(int(scid))
        signal_ids.append(int(sgid))
        run_ids.append(int(rid))
        directions.append(str(direction))
        window_froms.append(wf)
        window_tos.append(wt)
        trades.append(int(tr))
        pnl_abs.append(Decimal(pnl))
        winrate.append(Decimal(wr))
        roi.append(Decimal(r))
        mfe_avg.append(Decimal(mfe))
        mae_avg.append(Decimal(mae))

    async with pg.acquire() as conn:
        affected = await conn.fetch(
            f"""
            INSERT INTO {BT_STAT_V2_TABLE} (
                scenario_id,
                signal_id,
                run_id,
                direction,
                window_from,
                window_to,
                trades,
                pnl_abs,
                winrate,
                roi,
                max_favorable_excursion_avg,
                max_adverse_excursion_avg,
                raw_stat,
                created_at,
                updated_at
            )
            SELECT
                u.scenario_id,
                u.signal_id,
                u.run_id,
                u.direction,
                u.window_from,
                u.window_to,
                u.trades,
                u.pnl_abs,
                u.winrate,
                u.roi,
                u.mfe_avg,
                u.mae_avg,
                NULL::jsonb,
                now(),
                now()
            FROM unnest(
                $1::int[],
                $2::int[],
                $3::bigint[],
                $4::text[],
                $5::timestamp[],
                $6::timestamp[],
                $7::int[],
                $8::numeric[],
                $9::numeric[],
                $10::numeric[],
                $11::numeric[],
                $12::numeric[]
            ) AS u(
                scenario_id,
                signal_id,
                run_id,
                direction,
                window_from,
                window_to,
                trades,
                pnl_abs,
                winrate,
                roi,
                mfe_avg,
                mae_avg
            )
            ON CONFLICT (scenario_id, signal_id, run_id, direction)
            DO UPDATE SET
                window_from = EXCLUDED.window_from,
                window_to = EXCLUDED.window_to,
                trades = EXCLUDED.trades,
                pnl_abs = EXCLUDED.pnl_abs,
                winrate = EXCLUDED.winrate,
                roi = EXCLUDED.roi,
                max_favorable_excursion_avg = EXCLUDED.max_favorable_excursion_avg,
                max_adverse_excursion_avg = EXCLUDED.max_adverse_excursion_avg,
                updated_at = now()
            RETURNING id
            """,
            scenario_ids,
            signal_ids,
            run_ids,
            directions,
            window_froms,
            window_tos,
            trades,
            pnl_abs,
            winrate,
            roi,
            mfe_avg,
            mae_avg,
        )

    return len(affected)


# 🔸 Определение таблицы OHLCV по TF
def _ohlcv_table_for_timeframe(timeframe: str) -> Optional[str]:
    if timeframe == "m5":
        return "ohlcv_bb_m5"
    if timeframe == "m15":
        return "ohlcv_bb_m15"
    if timeframe == "h1":
        return "ohlcv_bb_h1"
    return None