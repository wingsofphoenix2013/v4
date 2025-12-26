# bt_signals_liveprocessor.py — воркер переноса live-filtered signal_sent из bt_signals_live в bt_signals_values (run-aware через applied_run_id)

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

# 🔸 RedisTimeSeries ключи (для получения entry price при отсутствии в details)
BB_TS_CLOSE_KEY = "bb:ts:{symbol}:{tf}:c"

# 🔸 Таймшаги TF (в минутах) для вычисления decision_time (если отсутствует)
TF_STEP_MINUTES = {"m5": 5, "m15": 15, "h1": 60}


# 🔸 Публичная точка входа: воркер liveprocessor
async def run_bt_signals_liveprocessor(pg, redis) -> None:
    log.debug("BT_SIG_LIVEPROC: воркер liveprocessor запущен")

    await _ensure_consumer_group(redis)

    # основной цикл
    while True:
        try:
            entries = await _read_from_stream(redis)

            if not entries:
                continue

            total_msgs = 0
            parsed_msgs = 0
            ignored_msgs = 0

            # набор сигналов, по которым будем искать pending rows
            signal_ids: Set[int] = set()

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

                    # ACK делаем сразу: обработка идемпотентная, а зависание pending сообщений нам не нужно
                    await redis.xack(LIVE_READY_STREAM_KEY, LIVE_READY_CONSUMER_GROUP, msg_id)

            if not signal_ids:
                log.info(
                    "BT_SIG_LIVEPROC: batch done — msgs=%s, parsed=%s, ignored=%s, signal_ids=0",
                    total_msgs,
                    parsed_msgs,
                    ignored_msgs,
                )
                continue

            # обрабатываем pending строки из bt_signals_live
            processed_total, inserted_total, duplicates_total, marked_total, soft_skipped_total, hard_skipped_total, errors_total = await _process_pending_for_signal_ids(
                pg=pg,
                redis=redis,
                signal_ids=sorted(signal_ids),
            )

            log.info(
                "BT_SIG_LIVEPROC: batch done — msgs=%s, parsed=%s, ignored=%s, signal_ids=%s, "
                "rows_ready=%s, inserted=%s, duplicates=%s, marked_processed=%s, soft_skipped=%s, hard_skipped=%s, errors=%s",
                total_msgs,
                parsed_msgs,
                ignored_msgs,
                len(signal_ids),
                processed_total,
                inserted_total,
                duplicates_total,
                marked_total,
                soft_skipped_total,
                hard_skipped_total,
                errors_total,
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


# 🔸 Обработка pending строк bt_signals_live -> вставка в bt_signals_values -> processed=true
async def _process_pending_for_signal_ids(
    pg,
    redis,
    signal_ids: List[int],
) -> Tuple[int, int, int, int, int, int, int]:
    processed_total = 0
    inserted_total = 0
    duplicates_total = 0
    marked_total = 0
    soft_skipped_total = 0
    hard_skipped_total = 0
    errors_total = 0

    if not signal_ids:
        return processed_total, inserted_total, duplicates_total, marked_total, soft_skipped_total, hard_skipped_total, errors_total

    # забираем pending строки из bt_signals_live
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                id,
                signal_id,
                symbol,
                timeframe,
                open_time,
                decision_time,
                details
            FROM bt_signals_live
            WHERE processed = false
              AND status = 'signal_sent'
              AND signal_id = ANY($1::int[])
            ORDER BY open_time
            LIMIT $2
            """,
            signal_ids,
            int(PROCESS_BATCH_LIMIT),
        )

    if not rows:
        return processed_total, inserted_total, duplicates_total, marked_total, soft_skipped_total, hard_skipped_total, errors_total

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
                    "timeframe": str(r["timeframe"]),
                    "open_time": r["open_time"],
                    "decision_time": r["decision_time"],
                    "details": details_obj,
                }
            )
        except Exception:
            errors_total += 1

    if not pending:
        return processed_total, inserted_total, duplicates_total, marked_total, soft_skipped_total, hard_skipped_total, errors_total

    processed_total = len(pending)

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

    if need_price:
        # батчем тянем close по open_time из Redis TS
        pipe = redis.pipeline()
        for p in need_price:
            symbol = p["symbol"]
            tf = str(p["timeframe"]).strip().lower()
            ts_ms = _to_ms_utc(p["open_time"])
            close_key = BB_TS_CLOSE_KEY.format(symbol=symbol, tf=tf)
            pipe.execute_command("TS.RANGE", close_key, ts_ms, ts_ms)

        try:
            res = await pipe.execute()
        except Exception as e:
            log.error("BT_SIG_LIVEPROC: redis TS pipeline error: %s", e, exc_info=True)
            errors_total += len(need_price)
            res = []

        # применяем результаты
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
            signal_id = int(p["signal_id"])
            symbol = str(p["symbol"])
            timeframe = str(p["timeframe"]).strip().lower()
            open_time = p["open_time"]
            decision_time = p.get("decision_time")

            details = p.get("details") or {}
            sig = details.get("signal") if isinstance(details.get("signal"), dict) else {}
            filt = details.get("filter") if isinstance(details.get("filter"), dict) else {}

            direction = str((sig or {}).get("direction") or "").strip().lower()
            message = str((sig or {}).get("message") or "").strip()
            price = p.get("price")

            # условия достаточности
            if direction not in ("long", "short"):
                # данные некорректны — помечаем processed=true, чтобы не зависать
                ids_mark_processed_true.append(int(p["id"]))
                hard_skipped_total += 1
                continue

            if not message:
                ids_mark_processed_true.append(int(p["id"]))
                hard_skipped_total += 1
                continue

            if price is None:
                # цена не найдена — оставляем processed=false для ретраев
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
                    "live_id": int(p["id"]),
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

    return processed_total, inserted_total, duplicates_total, marked_total, soft_skipped_total, hard_skipped_total, errors_total


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