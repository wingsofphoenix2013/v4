# packs_config/pack_io.py — consumer ind_pack_stream_core → запись ind_pack payload в PostgreSQL (state + events)

from __future__ import annotations

# 🔸 Imports
import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any


# 🔸 Константы Redis (stream источника)
IND_PACK_STREAM_CORE = "ind_pack_stream_core"
PACK_IO_GROUP = "ind_pack_io_group_v4"
PACK_IO_CONSUMER = "ind_pack_io_1"

# 🔸 Параметры чтения и параллельной обработки stream
STREAM_READ_COUNT = 500
STREAM_BLOCK_MS = 2000

# 🔸 SQL (events)
SQL_INSERT_EVENT = """
    INSERT INTO ind_pack_events_v4
        (analysis_id, scenario_id, signal_id, direction, symbol, timeframe, ok, payload_json, run_id, open_ts_ms, open_time)
    VALUES
        ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11)
"""

# 🔸 SQL (state) — pair
SQL_UPSERT_STATE_PAIR = """
    INSERT INTO ind_pack_state_v4
        (analysis_id, scenario_id, signal_id, direction, symbol, timeframe, ok, payload_json, run_id, open_ts_ms, open_time)
    VALUES
        ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11)
    ON CONFLICT (analysis_id, scenario_id, signal_id, direction, symbol, timeframe)
    WHERE scenario_id IS NOT NULL AND signal_id IS NOT NULL
    DO UPDATE SET
        ok = EXCLUDED.ok,
        payload_json = EXCLUDED.payload_json,
        run_id = EXCLUDED.run_id,
        open_ts_ms = EXCLUDED.open_ts_ms,
        open_time = EXCLUDED.open_time,
        updated_at = NOW()
"""

# 🔸 SQL (state) — static
SQL_UPSERT_STATE_STATIC = """
    INSERT INTO ind_pack_state_v4
        (analysis_id, scenario_id, signal_id, direction, symbol, timeframe, ok, payload_json, run_id, open_ts_ms, open_time)
    VALUES
        ($1, NULL, NULL, $2, $3, $4, $5, $6::jsonb, $7, $8, $9)
    ON CONFLICT (analysis_id, direction, symbol, timeframe)
    WHERE scenario_id IS NULL AND signal_id IS NULL
    DO UPDATE SET
        ok = EXCLUDED.ok,
        payload_json = EXCLUDED.payload_json,
        run_id = EXCLUDED.run_id,
        open_ts_ms = EXCLUDED.open_ts_ms,
        open_time = EXCLUDED.open_time,
        updated_at = NOW()
"""


# 🔸 Consumer-group helper
async def ensure_stream_group(redis: Any, stream: str, group: str):
    log = logging.getLogger("PACK_IO")
    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning("xgroup_create error for %s/%s: %s", stream, group, e)


# 🔸 Parse helpers
def _to_int(v: Any) -> int | None:
    try:
        if v is None:
            return None
        s = str(v).strip()
        if s == "":
            return None
        return int(s)
    except Exception:
        return None


def _to_str(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v)
    return s if s != "" else None


def _parse_open_time(v: Any) -> datetime | None:
    if v is None:
        return None
    try:
        dt = datetime.fromisoformat(str(v))
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return None


def _parse_payload(payload_json: Any) -> tuple[bool, str]:
    """
    Возвращает:
      - ok (bool)
      - payload_json_str (валидная JSON-строка для вставки в jsonb)
    """
    raw = "" if payload_json is None else str(payload_json)

    # нормальный кейс: payload_json уже строка JSON
    try:
        obj = json.loads(raw)
        ok = bool(obj.get("ok", False))
        return ok, json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        # fallback: сохраняем сырой payload, чтобы не терять данные
        obj = {"ok": False, "reason": "invalid_payload_json", "raw": raw}
        return False, json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


# 🔸 Основной воркер: stream → PG
async def run_pack_io(pg: Any, redis: Any):
    log = logging.getLogger("PACK_IO")

    # создать группу заранее
    await ensure_stream_group(redis, IND_PACK_STREAM_CORE, PACK_IO_GROUP)

    while True:
        try:
            resp = await redis.xreadgroup(
                PACK_IO_GROUP,
                PACK_IO_CONSUMER,
                streams={IND_PACK_STREAM_CORE: ">"},
                count=STREAM_READ_COUNT,
                block=STREAM_BLOCK_MS,
            )
            if not resp:
                continue

            flat: list[tuple[str, dict[str, Any]]] = []
            for _, messages in resp:
                for msg_id, data in messages:
                    flat.append((msg_id, data))
            if not flat:
                continue

            # распарсим сообщения в батч
            events_rows: list[tuple[Any, ...]] = []
            state_pair_rows: list[tuple[Any, ...]] = []
            state_static_rows: list[tuple[Any, ...]] = []
            to_ack: list[str] = []

            skipped = 0
            parse_errors = 0

            for msg_id, data in flat:
                to_ack.append(msg_id)

                kind = str(data.get("kind") or "").strip().lower()
                analysis_id = _to_int(data.get("analysis_id"))
                direction = _to_str(data.get("direction"))
                symbol = _to_str(data.get("symbol"))
                timeframe = _to_str(data.get("timeframe"))

                # условия достаточности
                if not analysis_id or not direction or not symbol or not timeframe:
                    skipped += 1
                    continue

                scenario_id = _to_int(data.get("scenario_id"))
                signal_id = _to_int(data.get("signal_id"))
                run_id = _to_int(data.get("run_id"))
                open_ts_ms = _to_int(data.get("open_ts_ms"))
                open_time_dt = _parse_open_time(data.get("open_time"))

                ok, payload_str = _parse_payload(data.get("payload_json"))
                if payload_str is None:
                    parse_errors += 1
                    continue

                # events: пишем всегда (и для static, и для pair)
                events_rows.append((
                    int(analysis_id),
                    int(scenario_id) if scenario_id is not None else None,
                    int(signal_id) if signal_id is not None else None,
                    str(direction),
                    str(symbol),
                    str(timeframe),
                    bool(ok),
                    str(payload_str),
                    int(run_id) if run_id is not None else None,
                    int(open_ts_ms) if open_ts_ms is not None else None,
                    open_time_dt,
                ))

                # state: отдельно для pair/static
                if kind == "pair" and scenario_id is not None and signal_id is not None:
                    state_pair_rows.append((
                        int(analysis_id),
                        int(scenario_id),
                        int(signal_id),
                        str(direction),
                        str(symbol),
                        str(timeframe),
                        bool(ok),
                        str(payload_str),
                        int(run_id) if run_id is not None else None,
                        int(open_ts_ms) if open_ts_ms is not None else None,
                        open_time_dt,
                    ))
                else:
                    state_static_rows.append((
                        int(analysis_id),
                        str(direction),
                        str(symbol),
                        str(timeframe),
                        bool(ok),
                        str(payload_str),
                        int(run_id) if run_id is not None else None,
                        int(open_ts_ms) if open_ts_ms is not None else None,
                        open_time_dt,
                    ))

            # если нечего писать — просто ack и дальше
            if not events_rows and to_ack:
                await redis.xack(IND_PACK_STREAM_CORE, PACK_IO_GROUP, *to_ack)
                log.info("PACK_IO: batch skipped (msgs=%s, skipped=%s)", len(flat), skipped)
                continue

            # запись в PG одной транзакцией
            async with pg.acquire() as conn:
                async with conn.transaction():
                    # events
                    if events_rows:
                        await conn.executemany(SQL_INSERT_EVENT, events_rows)

                    # state pair
                    if state_pair_rows:
                        await conn.executemany(SQL_UPSERT_STATE_PAIR, state_pair_rows)

                    # state static
                    if state_static_rows:
                        await conn.executemany(SQL_UPSERT_STATE_STATIC, state_static_rows)

            # ack после успешной записи
            if to_ack:
                await redis.xack(IND_PACK_STREAM_CORE, PACK_IO_GROUP, *to_ack)

            # суммирующий лог
            log.info(
                "PACK_IO: batch done (msgs=%s, events=%s, state_pair=%s, state_static=%s, skipped=%s, parse_errors=%s)",
                len(flat),
                len(events_rows),
                len(state_pair_rows),
                len(state_static_rows),
                skipped,
                parse_errors,
            )

        except Exception as e:
            log.error("PACK_IO loop error: %s", e, exc_info=True)
            await asyncio.sleep(2)