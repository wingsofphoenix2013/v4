# position_emapattern_backfill_worker.py — бэкфилл EMA-паттернов батчами по 200

import asyncio
import logging
from decimal import Decimal

from position_emapattern_worker import (
    TIMEFRAMES, EMA_NAMES, _build_pattern_text, _get_pattern_id,
    _apply_trade_to_aggregate, _load_position, _load_position_emas, _write_redis_aggr,
)

log = logging.getLogger("IND_EMA_PATTERN_BACKFILL")

BATCH_SIZE = 200
SLEEP_SEC = 86400
LOCK_KEY = "lock:emapattern:backfill"
LOCK_TTL = 3600  # 1 час
INITIAL_DELAY = 120  # 2 минуты


async def _fetch_batch(pg):
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT position_uid
            FROM positions_v4
            WHERE status='closed' AND COALESCE(emasnapshot_checked, FALSE)=FALSE
            LIMIT $1
            """,
            BATCH_SIZE,
        )
    return [r["position_uid"] for r in rows]


async def _process_one(pg, redis, position_uid: str):
    async with pg.acquire() as conn:
        already = await conn.fetchval(
            "SELECT emasnapshot_checked FROM positions_v4 WHERE position_uid = $1",
            position_uid
        )
    if already:
        return True

    pos = await _load_position(pg, position_uid)
    if not pos or pos["entry_price"] is None or pos["direction"] is None:
        async with pg.acquire() as conn:
            await conn.execute(
                "UPDATE positions_v4 SET emasnapshot_checked = TRUE WHERE position_uid = $1",
                position_uid
            )
        log.debug(f"[BF_MARKED_BAD_POS] position_uid={position_uid}")
        return True

    strategy_id = int(pos["strategy_id"])
    direction   = pos["direction"]
    entry_price = float(pos["entry_price"])
    pnl         = float(pos["pnl"]) if pos["pnl"] is not None else 0.0

    emas_by_tf = await _load_position_emas(pg, position_uid)
    incomplete = [tf for tf in TIMEFRAMES if any(n not in emas_by_tf.get(tf, {}) for n in EMA_NAMES)]
    if incomplete:
        async with pg.acquire() as conn:
            await conn.execute(
                "UPDATE positions_v4 SET emasnapshot_checked = TRUE WHERE position_uid = $1",
                position_uid
            )
        log.debug(f"[BF_MARKED_INCOMPLETE] position_uid={position_uid} missing={incomplete}")
        return True

    last_counts = {}
    for tf in TIMEFRAMES:
        pattern_text = _build_pattern_text(entry_price, emas_by_tf[tf])
        pattern_id = await _get_pattern_id(pg, pattern_text)
        count_trades, winrate = await _apply_trade_to_aggregate(pg, strategy_id, direction, tf, pattern_id, pnl)
        last_counts[tf] = (count_trades, winrate, pattern_id)

    for tf, (ct, wr, pid) in last_counts.items():
        await _write_redis_aggr(redis, strategy_id, direction, tf, pid, ct, wr)

    async with pg.acquire() as conn:
        await conn.execute(
            "UPDATE positions_v4 SET emasnapshot_checked = TRUE WHERE position_uid = $1",
            position_uid
        )
    log.debug(f"[BF_MARKED_DONE] position_uid={position_uid}")
    return True


async def run_position_emapattern_backfill_worker(pg, redis):
    # задержка перед первым запуском
    log.info(f"Backfill стартует через {INITIAL_DELAY} секунд...")
    await asyncio.sleep(INITIAL_DELAY)

    while True:
        try:
            ok = await redis.set(LOCK_KEY, "1", nx=True, ex=LOCK_TTL)
            if not ok:
                log.debug("Лок занят другим инстансом — сплю до следующего цикла")
                await asyncio.sleep(SLEEP_SEC)
                continue

            total = 0
            while True:
                batch = await _fetch_batch(pg)
                if not batch:
                    break
                log.debug(f"[BF_BATCH_START] size={len(batch)}")
                for uid in batch:
                    try:
                        await _process_one(pg, redis, uid)
                        total += 1
                    except Exception:
                        log.exception(f"[BF_ERROR] position_uid={uid}")
                log.info(f"[BF_BATCH_DONE] processed_total={total}")

            log.info(f"[BF_CYCLE_DONE] processed_total={total}")

        except Exception as e:
            log.error(f"Ошибка в бэкфилл-цикле: {e}", exc_info=True)
        finally:
            try:
                await redis.delete(LOCK_KEY)
            except Exception:
                pass

        await asyncio.sleep(SLEEP_SEC)