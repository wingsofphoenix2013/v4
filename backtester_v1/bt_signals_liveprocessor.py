# bt_signals_liveprocessor.py — воркер переноса live-filtered signal_sent из bt_signals_live в bt_signals_values (auto-drain backlog, без зависимости от processed=false)

import asyncio
import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Set

log = logging.getLogger("BT_SIG_LIVEPROC")

# 🔸 Redis Stream: триггер готовности live-filtered сигналов к персисту
LIVE_READY_STREAM_KEY = "bt:signals:live_ready"
LIVE_READY_CONSUMER_GROUP = "bt_signals_liveprocessor"
LIVE_READY_CONSUMER_NAME = "bt_signals_liveprocessor_main"

# 🔸 Настройки чтения стрима
LIVE_READY_STREAM_BATCH_SIZE = 100
LIVE_READY_STREAM_BLOCK_MS = 5000

# 🔸 Параметры обработки
PROCESS_BATCH_LIMIT = 500

# 🔸 Авто-дренаж: периодический подбор хвоста (чтобы история подхватилась без ручных XADD/UPDATE циклов)
DRAIN_IDLE_INTERVAL_SEC = 10

# 🔸 Защита от бесконечных циклов дренажа при отсутствии прогресса
DRAIN_MAX_ITERATIONS_PER_TICK = 200

# 🔸 RedisTimeSeries ключи (fallback: entry price из close)
BB_TS_CLOSE_KEY = "bb:ts:{symbol}:{tf}:c"

# 🔸 Таймшаги TF (в минутах) для вычисления decision_time (если отсутствует)
TF_STEP_MINUTES = {"m5": 5, "m15": 15, "h1": 60}


# 🔸 Публичная точка входа: воркер liveprocessor
async def run_bt_signals_liveprocessor(pg, redis) -> None:
    log.debug("BT_SIG_LIVEPROC: воркер liveprocessor запущен")

    await _ensure_consumer_group(redis)

    last_idle_drain_at = datetime.utcnow()

    # основной цикл
    while True:
        try:
            entries = await _read_from_stream(redis)

            total_msgs = 0
            parsed_msgs = 0
            ignored_msgs = 0

            # набор сигналов для таргетированного дренажа (если пусто — дреним всё)
            signal_ids: Set[int] = set()

            if entries:
                for stream_key, messages in entries:
                    if stream_key != LIVE_READY_STREAM_KEY:
                        continue

                    for msg_id, fields in messages:
                        total_msgs += 1

                        ctx = _parse_live_ready_message(fields)
                        if not ctx:
                            ignored_msgs += 1
                            await redis.xack(LIVE_READY_STREAM_KEY, LIVE_READY_CONSUMER_GROUP, msg_id)
                            continue

                        parsed_msgs += 1
                        signal_ids.add(ctx["signal_id"])

                        # ack сразу — обработка идемпотентная
                        await redis.xack(LIVE_READY_STREAM_KEY, LIVE_READY_CONSUMER_GROUP, msg_id)

            # условия запуска дренажа:
            # - пришли сообщения
            # - или периодический авто-дренаж (подхват истории и “молча” накопившихся signal_sent)
            now = datetime.utcnow()
            should_idle_drain = (now - last_idle_drain_at).total_seconds() >= DRAIN_IDLE_INTERVAL_SEC

            if (not signal_ids) and (not should_idle_drain):
                continue

            drain_target_ids = sorted(signal_ids) if signal_ids else []

            drained = await _drain_pending(
                pg=pg,
                redis=redis,
                signal_ids=drain_target_ids,
                max_iterations=DRAIN_MAX_ITERATIONS_PER_TICK,
            )

            last_idle_drain_at = now

            log.info(
                "BT_SIG_LIVEPROC: tick summary — msgs=%s, parsed=%s, ignored=%s, targeted_signal_ids=%s, "
                "drain_iterations=%s, rows_ready=%s, inserted=%s, duplicates=%s, marked_processed=%s, "
                "soft_skipped=%s, hard_skipped=%s, errors=%s",
                total_msgs,
                parsed_msgs,
                ignored_msgs,
                len(drain_target_ids),
                drained["iterations"],
                drained["rows_ready"],
                drained["inserted"],
                drained["duplicates"],
                drained["marked_processed"],
                drained["soft_skipped"],
                drained["hard_skipped"],
                drained["errors"],
            )

        except Exception as e:
            log.error("BT_SIG_LIVEPROC: loop error: %s", e, exc_info=True)
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=LIVE_READY_STREAM_KEY,
            groupname=LIVE_READY_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_SIG_LIVEPROC: создана consumer group '%s' для стрима '%s'",
            LIVE_READY_CONSUMER_GROUP,
            LIVE_READY_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_SIG_LIVEPROC: consumer group '%s' для стрима '%s' уже существует",
                LIVE_READY_CONSUMER_GROUP,
                LIVE_READY_STREAM_KEY,
            )
        else:
            log.error(
                "BT_SIG_LIVEPROC: ошибка при создании consumer group '%s': %s",
                LIVE_READY_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:signals:live_ready
async def _read_from_stream(redis) -> List[Any]:
    entries = await redis.xreadgroup(
        groupname=LIVE_READY_CONSUMER_GROUP,
        consumername=LIVE_READY_CONSUMER_NAME,
        streams={LIVE_READY_STREAM_KEY: ">"},
        count=LIVE_READY_STREAM_BATCH_SIZE,
        block=LIVE_READY_STREAM_BLOCK_MS,
    )

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


# 🔸 Разбор сообщения из bt:signals:live_ready
def _parse_live_ready_message(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        signal_id_str = fields.get("signal_id")
        if not signal_id_str:
            return None
        signal_id = int(signal_id_str)
        return {"signal_id": signal_id}
    except Exception:
        return None


# 🔸 Дренаж pending строк: обрабатываем батчами, пока не закончатся или пока нет прогресса
async def _drain_pending(
    pg,
    redis,
    signal_ids: List[int],
    max_iterations: int,
) -> Dict[str, int]:
    totals = {
        "iterations": 0,
        "rows_ready": 0,
        "inserted": 0,
        "duplicates": 0,
        "marked_processed": 0,
        "soft_skipped": 0,
        "hard_skipped": 0,
        "errors": 0,
    }

    # если signal_ids пустой — это означает "обработать всё"
    for _ in range(max_iterations):
        totals["iterations"] += 1

        (
            rows_ready,
            inserted,
            duplicates,
            marked_processed,
            soft_skipped,
            hard_skipped,
            errors,
        ) = await _process_one_batch(
            pg=pg,
            redis=redis,
            signal_ids=signal_ids,
        )

        totals["rows_ready"] += rows_ready
        totals["inserted"] += inserted
        totals["duplicates"] += duplicates
        totals["marked_processed"] += marked_processed
        totals["soft_skipped"] += soft_skipped
        totals["hard_skipped"] += hard_skipped
        totals["errors"] += errors

        # если в батче не было строк — всё вычищено
        if rows_ready <= 0:
            break

        # если нет прогресса — не крутимся бесконечно
        if marked_processed <= 0:
            break

    return totals


# 🔸 Обработка одного батча: берём signal_sent, которых ещё нет в bt_signals_values, пишем туда и ставим processed=true
async def _process_one_batch(
    pg,
    redis,
    signal_ids: List[int],
) -> Tuple[int, int, int, int, int, int, int]:
    rows_ready = 0
    inserted_total = 0
    duplicates_total = 0
    marked_total = 0
    soft_skipped_total = 0
    hard_skipped_total = 0
    errors_total = 0

    # выбираем строки bt_signals_live со status='signal_sent', которые ещё не представлены в bt_signals_values
    async with pg.acquire() as conn:
        if signal_ids:
            rows = await conn.fetch(
                """
                SELECT
                    l.id,
                    l.signal_id,
                    l.symbol,
                    l.timeframe,
                    l.open_time,
                    l.decision_time,
                    l.details,
                    (l.details->'signal'->>'direction') AS direction
                FROM bt_signals_live l
                WHERE l.status = 'signal_sent'
                  AND l.signal_id = ANY($1::int[])
                  AND (l.details->'signal'->>'direction') IN ('long','short')
                  AND NOT EXISTS (
                      SELECT 1
                      FROM bt_signals_values v
                      WHERE v.signal_id = l.signal_id
                        AND v.symbol    = l.symbol
                        AND v.timeframe = l.timeframe
                        AND v.open_time = l.open_time
                        AND v.direction = (l.details->'signal'->>'direction')
                  )
                ORDER BY l.open_time
                LIMIT $2
                """,
                signal_ids,
                int(PROCESS_BATCH_LIMIT),
            )
        else:
            rows = await conn.fetch(
                """
                SELECT
                    l.id,
                    l.signal_id,
                    l.symbol,
                    l.timeframe,
                    l.open_time,
                    l.decision_time,
                    l.details,
                    (l.details->'signal'->>'direction') AS direction
                FROM bt_signals_live l
                WHERE l.status = 'signal_sent'
                  AND (l.details->'signal'->>'direction') IN ('long','short')
                  AND NOT EXISTS (
                      SELECT 1
                      FROM bt_signals_values v
                      WHERE v.signal_id = l.signal_id
                        AND v.symbol    = l.symbol
                        AND v.timeframe = l.timeframe
                        AND v.open_time = l.open_time
                        AND v.direction = (l.details->'signal'->>'direction')
                  )
                ORDER BY l.open_time
                LIMIT $1
                """,
                int(PROCESS_BATCH_LIMIT),
            )

    if not rows:
        return rows_ready, inserted_total, duplicates_total, marked_total, soft_skipped_total, hard_skipped_total, errors_total

    rows_ready = len(rows)

    # нормализуем входные данные
    pending: List[Dict[str, Any]] = []
    for r in rows:
        try:
            details = r["details"]
            if details is None:
                details_obj = {}
            elif isinstance(details, dict):
                details_obj = details
            else:
                details_obj = json.loads(str(details))

            pending.append(
                {
                    "id": int(r["id"]),
                    "signal_id": int(r["signal_id"]),
                    "symbol": str(r["symbol"]),
                    "timeframe": str(r["timeframe"]).strip().lower(),
                    "open_time": r["open_time"],
                    "decision_time": r["decision_time"],
                    "direction": str(r["direction"]).strip().lower(),
                    "details": details_obj,
                }
            )
        except Exception:
            errors_total += 1

    if not pending:
        return rows_ready, inserted_total, duplicates_total, marked_total, soft_skipped_total, hard_skipped_total, errors_total

    # собираем цену из details или из Redis TS (fallback)
    need_price: List[Dict[str, Any]] = []
    for p in pending:
        details = p.get("details") or {}
        ohlcv = details.get("ohlcv") if isinstance(details.get("ohlcv"), dict) else {}
        price = None

        try:
            if isinstance(ohlcv, dict) and "close_curr" in ohlcv:
                price = float(ohlcv.get("close_curr"))
        except Exception:
            price = None

        if price is None:
            need_price.append(p)
        else:
            p["price"] = price

    # fallback: читаем close из Redis TS по open_time
    if need_price:
        pipe = redis.pipeline()
        for p in need_price:
            symbol = p["symbol"]
            tf = p["timeframe"]
            ts_ms = _to_ms_utc(p["open_time"])
            close_key = BB_TS_CLOSE_KEY.format(symbol=symbol, tf=tf)
            pipe.execute_command("TS.RANGE", close_key, ts_ms, ts_ms)

        try:
            res = await pipe.execute()
        except Exception as e:
            log.error("BT_SIG_LIVEPROC: redis TS pipeline error: %s", e, exc_info=True)
            errors_total += len(need_price)
            res = []

        for p, ts_range_result in zip(need_price, res):
            price = _extract_ts_value(ts_range_result)
            if price is None:
                continue
            p["price"] = float(price)

    # готовим вставки в bt_signals_values
    to_insert: List[Dict[str, Any]] = []
    ids_mark_processed_true: List[int] = []

    for p in pending:
        try:
            live_id = int(p["id"])
            signal_id = int(p["signal_id"])
            symbol = str(p["symbol"])
            timeframe = str(p["timeframe"]).strip().lower()
            open_time = p["open_time"]
            decision_time = p.get("decision_time")
            direction = str(p.get("direction") or "").strip().lower()
            price = p.get("price")

            details = p.get("details") or {}
            sig = details.get("signal") if isinstance(details.get("signal"), dict) else {}
            filt = details.get("filter") if isinstance(details.get("filter"), dict) else {}

            message = str((sig or {}).get("message") or "").strip()

            # условия достаточности
            if direction not in ("long", "short"):
                ids_mark_processed_true.append(live_id)
                hard_skipped_total += 1
                continue

            if not message:
                ids_mark_processed_true.append(live_id)
                hard_skipped_total += 1
                continue

            if price is None:
                # цена не найдена — не помечаем processed, чтобы попробовать позже
                soft_skipped_total += 1
                continue

            # decision_time: если нет — вычисляем по TF
            if decision_time is None:
                step_min = TF_STEP_MINUTES.get(timeframe)
                if step_min:
                    decision_time = open_time + timedelta(minutes=int(step_min))
                else:
                    decision_time = open_time

            # applied_run_id -> first_backfill_run_id
            applied_run_id = filt.get("applied_run_id") if isinstance(filt, dict) else None
            first_backfill_run_id: Optional[int] = None
            try:
                if applied_run_id is not None and str(applied_run_id).strip() != "":
                    first_backfill_run_id = int(applied_run_id)
            except Exception:
                first_backfill_run_id = None

            # mirror meta (если есть)
            mirror = sig.get("mirror") if isinstance(sig, dict) else None
            mirror_scenario_id = None
            mirror_signal_id = None
            if isinstance(mirror, dict):
                try:
                    mirror_scenario_id = int(mirror.get("scenario_id")) if mirror.get("scenario_id") is not None else None
                except Exception:
                    mirror_scenario_id = None
                try:
                    mirror_signal_id = int(mirror.get("signal_id")) if mirror.get("signal_id") is not None else None
                except Exception:
                    mirror_signal_id = None

            raw_message = {
                "signal_id": signal_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "open_time": open_time.isoformat(),
                "decision_time": decision_time.isoformat() if isinstance(decision_time, datetime) else str(decision_time),
                "direction": direction,
                "message": message,
                "price": float(price),
                "source": "backtester_v1",
                "mode": "live_filtered",
                "mirror": {
                    "scenario_id": mirror_scenario_id,
                    "signal_id": mirror_signal_id,
                } if (mirror_scenario_id is not None or mirror_signal_id is not None) else None,
                "applied_run_id": first_backfill_run_id,
            }

            to_insert.append(
                {
                    "live_id": live_id,
                    "signal_uuid": uuid.uuid4(),
                    "signal_id": signal_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "open_time": open_time,
                    "direction": direction,
                    "message": message,
                    "raw_message_json": json.dumps(raw_message, ensure_ascii=False),
                    "decision_time": decision_time,
                    "first_backfill_run_id": first_backfill_run_id,
                }
            )

        except Exception as e:
            errors_total += 1
            log.error("BT_SIG_LIVEPROC: build insert row error: %s", e, exc_info=True)

    if to_insert:
        ins_count, dup_count = await _insert_into_bt_signals_values(pg, to_insert)
        inserted_total += ins_count
        duplicates_total += dup_count

        # помечаем processed=true для всех успешно обработанных (включая dup)
        ids_mark_processed_true.extend([r["live_id"] for r in to_insert])

    # обновляем processed=true (для успешных и hard-skip)
    if ids_mark_processed_true:
        ids_mark_processed_true = sorted(set(ids_mark_processed_true))
        async with pg.acquire() as conn:
            await conn.execute(
                """
                UPDATE bt_signals_live
                SET processed = true
                WHERE id = ANY($1::int[])
                """,
                ids_mark_processed_true,
            )
        marked_total = len(ids_mark_processed_true)

    return rows_ready, inserted_total, duplicates_total, marked_total, soft_skipped_total, hard_skipped_total, errors_total


# 🔸 Вставка батчем в bt_signals_values с подсчётом inserted/duplicate
async def _insert_into_bt_signals_values(
    pg,
    rows: List[Dict[str, Any]],
) -> Tuple[int, int]:
    if not rows:
        return 0, 0

    uuids: List[uuid.UUID] = []
    signal_ids: List[int] = []
    symbols: List[str] = []
    timeframes: List[str] = []
    open_times: List[datetime] = []
    directions: List[str] = []
    messages: List[str] = []
    raw_jsons: List[str] = []
    decision_times: List[datetime] = []
    first_runs: List[Optional[int]] = []

    for r in rows:
        uuids.append(r["signal_uuid"])
        signal_ids.append(int(r["signal_id"]))
        symbols.append(str(r["symbol"]))
        timeframes.append(str(r["timeframe"]))
        open_times.append(r["open_time"])
        directions.append(str(r["direction"]))
        messages.append(str(r["message"]))
        raw_jsons.append(str(r["raw_message_json"]))
        decision_times.append(r["decision_time"])
        first_runs.append(r.get("first_backfill_run_id"))

    async with pg.acquire() as conn:
        inserted = await conn.fetch(
            """
            WITH data AS (
                SELECT *
                FROM UNNEST(
                    $1::uuid[],
                    $2::int[],
                    $3::text[],
                    $4::text[],
                    $5::timestamp[],
                    $6::text[],
                    $7::text[],
                    $8::text[],
                    $9::timestamp[],
                    $10::bigint[]
                ) AS t(
                    signal_uuid,
                    signal_id,
                    symbol,
                    timeframe,
                    open_time,
                    direction,
                    message,
                    raw_message_json,
                    decision_time,
                    first_backfill_run_id
                )
            )
            INSERT INTO bt_signals_values (
                signal_uuid,
                signal_id,
                symbol,
                timeframe,
                open_time,
                direction,
                message,
                raw_message,
                decision_time,
                first_backfill_run_id
            )
            SELECT
                signal_uuid,
                signal_id,
                symbol,
                timeframe,
                open_time,
                direction,
                message,
                raw_message_json::jsonb,
                decision_time,
                first_backfill_run_id
            FROM data
            ON CONFLICT (signal_id, symbol, timeframe, open_time, direction) DO NOTHING
            RETURNING signal_id, symbol, timeframe, open_time, direction
            """,
            uuids,
            signal_ids,
            symbols,
            timeframes,
            open_times,
            directions,
            messages,
            raw_jsons,
            decision_times,
            first_runs,
        )

    inserted_count = len(inserted)
    duplicate_count = len(rows) - inserted_count
    return inserted_count, duplicate_count


# 🔸 Naive UTC datetime → epoch ms
def _to_ms_utc(dt_naive_utc: datetime) -> int:
    return int(dt_naive_utc.replace(tzinfo=timezone.utc).timestamp() * 1000)


# 🔸 Извлечение одного значения из TS.RANGE ответа
def _extract_ts_value(ts_range_result: Any) -> Optional[float]:
    try:
        if not ts_range_result:
            return None
        point = ts_range_result[0]
        if not point or len(point) < 2:
            return None
        return float(point[1])
    except Exception:
        return None