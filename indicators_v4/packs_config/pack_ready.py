# packs_config/pack_ready.py — воркер готовности ind_pack: 4/4 по тикеру и m5-бару (таймаут 120с) → stream ind_pack_stream_ready + PG ind_pack_log_v4 (+ latency_ms)

from __future__ import annotations

# 🔸 Базовые импорты
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Any

# 🔸 Imports: pack runtime caches
from packs_config.cache_manager import configured_pairs_set, caches_ready

# 🔸 Константы Redis (streams)
IND_PACK_STREAM_CORE = "ind_pack_stream_core"
IND_PACK_STREAM_READY = "ind_pack_stream_ready"

READY_GROUP = "ind_pack_ready_group_v4"
READY_CONSUMER = "ind_pack_ready_1"

# 🔸 Константы Redis (keys)
READY_SET_PREFIX = "ind_pack_ready"                 # ind_pack_ready:{symbol}:{open_ts_ms} -> SET of "signal_id:scenario_id"
READY_META_PREFIX = "ind_pack_ready_meta"           # ind_pack_ready_meta:{symbol}:{open_ts_ms} -> HASH
READY_FINAL_PREFIX = "ind_pack_ready_final"         # ind_pack_ready_final:{symbol}:{open_ts_ms} -> "ok"/"error"

READY_DEADLINES_ZSET = "ind_pack_ready_deadlines"   # ZSET: score=deadline_ms, member="{symbol}:{open_ts_ms}"

# 🔸 Политики
TIMEOUT_SEC = 120
BAR_STEP_MS = 300_000  # m5 бар: 5 минут (используем для latency от закрытия)
POLL_DEADLINES_SEC = 1.0
STATE_TTL_SEC = 5 * 60  # 5 минут держим ключи состояния
READ_COUNT = 500
BLOCK_MS = 2000

# 🔸 SQL (PG)
SQL_INSERT_LOG = """
    INSERT INTO ind_pack_log_v4
        (symbol, timeframe, open_ts_ms, open_time, status, expected_count, received_count, published_at, latency_ms)
    VALUES
        ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    ON CONFLICT (symbol, timeframe, open_ts_ms) DO NOTHING
"""


# 🔸 Consumer-group helper
async def ensure_stream_group(redis: Any, stream: str, group: str):
    log = logging.getLogger("PACK_READY")
    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning("xgroup_create error for %s/%s: %s", stream, group, e)


# 🔸 Time helpers
def _now_utc_naive() -> datetime:
    return datetime.utcnow().replace(tzinfo=None)


def _now_ms() -> int:
    return int(datetime.utcnow().timestamp() * 1000)


def _parse_open_time(open_time: Any) -> datetime | None:
    if open_time is None:
        return None
    try:
        dt = datetime.fromisoformat(str(open_time))
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return None


def _parse_open_time_to_ts_ms(open_time: Any) -> int | None:
    dt = _parse_open_time(open_time)
    if dt is None:
        return None
    return int(dt.timestamp() * 1000)

def _calc_latency_ms(published_at: datetime, open_time_dt: datetime | None, open_ts_ms: int | None) -> int | None:
    """
    latency_ms считаем от ЗАКРЫТИЯ m5 бара:
      close_time = open_time + 5 минут
    """
    try:
        if open_time_dt is not None:
            close_dt = open_time_dt + timedelta(milliseconds=BAR_STEP_MS)
            ms = int((published_at - close_dt).total_seconds() * 1000)
            return max(0, ms)

        if open_ts_ms is not None:
            open_dt = datetime.utcfromtimestamp(int(open_ts_ms) / 1000).replace(tzinfo=None)
            close_dt = open_dt + timedelta(milliseconds=BAR_STEP_MS)
            ms = int((published_at - close_dt).total_seconds() * 1000)
            return max(0, ms)
    except Exception:
        return None

    return None

# 🔸 Pack pair token helper (signal:scenario)
def _pair_token(signal_id: int, scenario_id: int) -> str:
    return f"{int(signal_id)}:{int(scenario_id)}"


def _member_key(symbol: str, open_ts_ms: int) -> str:
    return f"{symbol}:{int(open_ts_ms)}"


def _ready_set_key(symbol: str, open_ts_ms: int) -> str:
    return f"{READY_SET_PREFIX}:{symbol}:{int(open_ts_ms)}"


def _ready_meta_key(symbol: str, open_ts_ms: int) -> str:
    return f"{READY_META_PREFIX}:{symbol}:{int(open_ts_ms)}"


def _ready_final_key(symbol: str, open_ts_ms: int) -> str:
    return f"{READY_FINAL_PREFIX}:{symbol}:{int(open_ts_ms)}"


# 🔸 Expected pairs snapshot (signal:scenario)
def _expected_tokens() -> list[str]:
    # configured_pairs_set хранит (scenario_id, signal_id)
    out = []
    for sc, sig in configured_pairs_set:
        out.append(_pair_token(int(sig), int(sc)))
    out.sort()
    return out


# 🔸 Insert log row to PG
async def _write_pg_log(
    pg: Any,
    symbol: str,
    open_ts_ms: int,
    open_time_dt: datetime | None,
    status: str,
    expected_count: int,
    received_count: int,
    published_at: datetime,
):
    latency_ms = _calc_latency_ms(published_at, open_time_dt, open_ts_ms)

    async with pg.acquire() as conn:
        await conn.execute(
            SQL_INSERT_LOG,
            str(symbol),
            "mtf",
            int(open_ts_ms),
            open_time_dt,
            str(status),
            int(expected_count),
            int(received_count),
            published_at,
            int(latency_ms) if latency_ms is not None else None,
        )


# 🔸 Publish ready to Redis Stream (ok only)
async def _publish_ready_stream(
    redis: Any,
    symbol: str,
    open_ts_ms: int,
    open_time_iso: str | None,
    expected_count: int,
    received_count: int,
    pairs_csv: str,
):
    fields = {
        "symbol": str(symbol),
        "timeframe": "mtf",
        "open_ts_ms": str(int(open_ts_ms)),
        "open_time": str(open_time_iso) if open_time_iso is not None else "",
        "status": "ok",
        "expected_count": str(int(expected_count)),
        "received_count": str(int(received_count)),
        "pairs": str(pairs_csv),
    }
    await redis.xadd(IND_PACK_STREAM_READY, fields)


# 🔸 Finalize OK (idempotent)
async def _finalize_ok(
    pg: Any,
    redis: Any,
    symbol: str,
    open_ts_ms: int,
    expected_count: int,
    received_count: int,
    pairs_csv: str,
    open_time_iso: str | None,
):
    final_key = _ready_final_key(symbol, open_ts_ms)
    ok_set = await redis.set(final_key, "ok", nx=True, ex=STATE_TTL_SEC)
    if not ok_set:
        return False

    published_at = _now_utc_naive()
    open_time_dt = _parse_open_time(open_time_iso)

    # PG write
    await _write_pg_log(pg, str(symbol), int(open_ts_ms), open_time_dt, "ok", int(expected_count), int(received_count), published_at)

    # Stream publish
    await _publish_ready_stream(redis, str(symbol), int(open_ts_ms), open_time_iso, int(expected_count), int(received_count), pairs_csv)

    return True


# 🔸 Finalize ERROR (idempotent; no stream publish)
async def _finalize_error(
    pg: Any,
    redis: Any,
    symbol: str,
    open_ts_ms: int,
    expected_count: int,
    received_count: int,
    open_time_iso: str | None,
):
    final_key = _ready_final_key(symbol, open_ts_ms)
    err_set = await redis.set(final_key, "error", nx=True, ex=STATE_TTL_SEC)
    if not err_set:
        return False

    published_at = _now_utc_naive()
    open_time_dt = _parse_open_time(open_time_iso)

    await _write_pg_log(pg, str(symbol), int(open_ts_ms), open_time_dt, "error", int(expected_count), int(received_count), published_at)
    return True


# 🔸 Main worker
async def run_pack_ready(pg: Any, redis: Any):
    log = logging.getLogger("PACK_READY")

    # ждём, пока pack runtime инициализируется (configured_pairs_set должен быть готов)
    while not caches_ready.get("registry", False) or not configured_pairs_set:
        log.debug("PACK_READY: waiting for pack runtime init (registry/pairs not ready yet)")
        await asyncio.sleep(1.0)

    expected = _expected_tokens()
    expected_count = len(expected)
    expected_csv = ",".join(expected)

    log.debug("PACK_READY: started — expected_pairs=%s (%s)", expected_count, expected_csv)

    # создать группу заранее
    await ensure_stream_group(redis, IND_PACK_STREAM_CORE, READY_GROUP)

    # loop tasks
    async def _consume_core_stream():
        stream = IND_PACK_STREAM_CORE
        group = READY_GROUP
        consumer = READY_CONSUMER

        while True:
            try:
                resp = await redis.xreadgroup(
                    group,
                    consumer,
                    streams={stream: ">"},
                    count=READ_COUNT,
                    block=BLOCK_MS,
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
                processed = 0
                skipped = 0
                ready_emitted = 0

                for msg_id, data in flat:
                    to_ack.append(msg_id)
                    try:
                        # условия достаточности (нужны pair + mtf + open_ts_ms)
                        if str(data.get("kind")) != "pair":
                            skipped += 1
                            continue

                        tf = str(data.get("timeframe") or "")
                        if tf != "mtf":
                            skipped += 1
                            continue

                        symbol = str(data.get("symbol") or "").strip()
                        if not symbol:
                            skipped += 1
                            continue

                        scenario_id = data.get("scenario_id")
                        signal_id = data.get("signal_id")
                        try:
                            sc = int(str(scenario_id))
                            sig = int(str(signal_id))
                        except Exception:
                            skipped += 1
                            continue

                        open_ts_ms_raw = data.get("open_ts_ms")
                        open_time_iso = data.get("open_time")

                        open_ts_ms = None
                        try:
                            if open_ts_ms_raw is not None and str(open_ts_ms_raw).strip() != "":
                                open_ts_ms = int(str(open_ts_ms_raw))
                        except Exception:
                            open_ts_ms = None

                        if open_ts_ms is None:
                            # fallback: parse open_time
                            open_ts_ms = _parse_open_time_to_ts_ms(open_time_iso)

                        if open_ts_ms is None:
                            skipped += 1
                            continue

                        # ignore if already finalized
                        final_key = _ready_final_key(symbol, open_ts_ms)
                        if await redis.exists(final_key):
                            processed += 1
                            continue

                        # set keys
                        set_key = _ready_set_key(symbol, open_ts_ms)
                        meta_key = _ready_meta_key(symbol, open_ts_ms)

                        token = _pair_token(sig, sc)

                        # first-seen: register deadline + meta
                        member = _member_key(symbol, open_ts_ms)
                        deadline_ms = _now_ms() + TIMEOUT_SEC * 1000
                        await redis.zadd(READY_DEADLINES_ZSET, {member: deadline_ms}, nx=True)

                        # meta: keep open_time if present
                        if open_time_iso is not None and str(open_time_iso).strip() != "":
                            await redis.hset(meta_key, mapping={"open_time": str(open_time_iso)})
                            await redis.expire(meta_key, STATE_TTL_SEC)

                        # add token to set
                        await redis.sadd(set_key, token)
                        await redis.expire(set_key, STATE_TTL_SEC)

                        received_count = int(await redis.scard(set_key))

                        # finalize ok when complete
                        if received_count >= expected_count:
                            pairs_csv = expected_csv
                            if await _finalize_ok(
                                pg,
                                redis,
                                symbol,
                                open_ts_ms,
                                expected_count,
                                expected_count,
                                pairs_csv,
                                str(open_time_iso) if open_time_iso else None,
                            ):
                                # cleanup redis state keys (keep final flag)
                                await redis.zrem(READY_DEADLINES_ZSET, member)
                                await redis.delete(set_key)
                                await redis.delete(meta_key)
                                ready_emitted += 1
                                log.debug(
                                    "PACK_READY: OK (symbol=%s, open_ts_ms=%s, open_time=%s, expected=%s, received=%s)",
                                    symbol,
                                    open_ts_ms,
                                    open_time_iso,
                                    expected_count,
                                    received_count,
                                )

                        processed += 1

                    except Exception as e:
                        skipped += 1
                        log.warning("PACK_READY: parse/process error: %s", e)

                if to_ack:
                    await redis.xack(stream, group, *to_ack)

                # суммирующий лог по батчу
                if processed or skipped or ready_emitted:
                    log.debug(
                        "PACK_READY: batch done (msgs=%s, processed=%s, skipped=%s, ready_ok=%s)",
                        len(flat),
                        processed,
                        skipped,
                        ready_emitted,
                    )

            except Exception as e:
                log.error("PACK_READY: core stream loop error: %s", e, exc_info=True)
                await asyncio.sleep(2)

    async def _poll_timeouts():
        while True:
            try:
                now_ms = _now_ms()
                members = await redis.zrangebyscore(READY_DEADLINES_ZSET, min="-inf", max=now_ms, start=0, num=200)
                if not members:
                    await asyncio.sleep(POLL_DEADLINES_SEC)
                    continue

                errors_logged = 0
                cleaned = 0

                for m in members:
                    try:
                        m_str = str(m)
                        if ":" not in m_str:
                            await redis.zrem(READY_DEADLINES_ZSET, m)
                            cleaned += 1
                            continue

                        symbol, open_ts_str = m_str.rsplit(":", 1)
                        try:
                            open_ts_ms = int(open_ts_str)
                        except Exception:
                            await redis.zrem(READY_DEADLINES_ZSET, m)
                            cleaned += 1
                            continue

                        # already finalized?
                        final_key = _ready_final_key(symbol, open_ts_ms)
                        if await redis.exists(final_key):
                            await redis.zrem(READY_DEADLINES_ZSET, m)
                            cleaned += 1
                            continue

                        set_key = _ready_set_key(symbol, open_ts_ms)
                        meta_key = _ready_meta_key(symbol, open_ts_ms)

                        received_count = int(await redis.scard(set_key))
                        if received_count >= expected_count:
                            # race: complete but not finalized yet
                            open_time_iso = await redis.hget(meta_key, "open_time")
                            if await _finalize_ok(pg, redis, symbol, open_ts_ms, expected_count, expected_count, expected_csv, str(open_time_iso) if open_time_iso else None):
                                log.debug(
                                    "PACK_READY: OK (race) (symbol=%s, open_ts_ms=%s, open_time=%s)",
                                    symbol,
                                    open_ts_ms,
                                    open_time_iso,
                                )
                            await redis.zrem(READY_DEADLINES_ZSET, m)
                            await redis.delete(set_key)
                            await redis.delete(meta_key)
                            cleaned += 1
                            continue

                        # timeout -> error (no stream publish)
                        open_time_iso = await redis.hget(meta_key, "open_time")
                        if await _finalize_error(pg, redis, symbol, open_ts_ms, expected_count, received_count, str(open_time_iso) if open_time_iso else None):
                            errors_logged += 1
                            log.debug(
                                "PACK_READY: ERROR timeout (symbol=%s, open_ts_ms=%s, open_time=%s, expected=%s, received=%s)",
                                symbol,
                                open_ts_ms,
                                open_time_iso,
                                expected_count,
                                received_count,
                            )

                        await redis.zrem(READY_DEADLINES_ZSET, m)
                        await redis.delete(set_key)
                        await redis.delete(meta_key)
                        cleaned += 1

                    except Exception as e:
                        log.warning("PACK_READY: timeout handler error: %s", e)
                        try:
                            await redis.zrem(READY_DEADLINES_ZSET, m)
                        except Exception:
                            pass

                if errors_logged or cleaned:
                    log.debug("PACK_READY: timeouts processed (errors=%s, cleaned=%s)", errors_logged, cleaned)

                await asyncio.sleep(POLL_DEADLINES_SEC)

            except Exception as e:
                log.error("PACK_READY: timeouts loop error: %s", e, exc_info=True)
                await asyncio.sleep(2)

    await asyncio.gather(
        _consume_core_stream(),
        _poll_timeouts(),
    )