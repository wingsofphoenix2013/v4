# packs_config/pack_io.py — consumer ind_pack_stream_core → запись истории ind_pack в PostgreSQL (events only)

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

# 🔸 Параметры чтения stream
STREAM_READ_COUNT = 500
STREAM_BLOCK_MS = 2000

# 🔸 Таймауты, чтобы не залипать на PG
PG_STATEMENT_TIMEOUT_MS = 30000

# 🔸 SQL (events)
SQL_INSERT_EVENT = """
    INSERT INTO ind_pack_events_v4
        (analysis_id, scenario_id, signal_id, direction, symbol, timeframe, ok, payload_json, run_id, open_ts_ms, open_time)
    VALUES
        ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11)
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
    raw = "" if payload_json is None else str(payload_json)
    try:
        obj = json.loads(raw)
        ok = bool(obj.get("ok", False))
        return ok, json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        obj = {"ok": False, "reason": "invalid_payload_json", "raw": raw}
        return False, json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


# 🔸 Основной воркер: stream → PG (events only)
async def run_pack_io(pg: Any, redis: Any):
    log = logging.getLogger("PACK_IO")

    # создать группу заранее
    await ensure_stream_group(redis, IND_PACK_STREAM_CORE, PACK_IO_GROUP)

    # стартуем только с новых сообщений (не разгребаем хвост)
    try:
        await redis.execute_command("XGROUP", "SETID", IND_PACK_STREAM_CORE, PACK_IO_GROUP, "$")
        log.info("PACK_IO: %s/%s cursor set to $ (new messages only)", IND_PACK_STREAM_CORE, PACK_IO_GROUP)
    except Exception as e:
        log.warning("PACK_IO: XGROUP SETID error for %s/%s: %s", IND_PACK_STREAM_CORE, PACK_IO_GROUP, e)

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

            to_ack: list[str] = []
            events_rows: list[tuple[Any, ...]] = []

            skipped = 0

            for msg_id, data in flat:
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

                to_ack.append(msg_id)

            # если нечего писать — просто ack того, что распарсили, и дальше
            if not events_rows:
                if to_ack:
                    await redis.xack(IND_PACK_STREAM_CORE, PACK_IO_GROUP, *to_ack)
                log.debug("PACK_IO: batch skipped (msgs=%s, skipped=%s)", len(flat), skipped)
                continue

            # запись в PG одной транзакцией
            async with pg.acquire() as conn:
                async with conn.transaction():
                    # таймаут, чтобы не залипать
                    await conn.execute(f"SET LOCAL statement_timeout = '{int(PG_STATEMENT_TIMEOUT_MS)}ms'")
                    await conn.executemany(SQL_INSERT_EVENT, events_rows)

            # ack после успешной записи
            if to_ack:
                await redis.xack(IND_PACK_STREAM_CORE, PACK_IO_GROUP, *to_ack)

            log.debug(
                "PACK_IO: batch done (msgs=%s, events=%s, skipped=%s)",
                len(flat),
                len(events_rows),
                skipped,
            )

        except Exception as e:
            log.error("PACK_IO loop error: %s", e, exc_info=True)
            await asyncio.sleep(2)