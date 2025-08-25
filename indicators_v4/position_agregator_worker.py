# position_agregator_worker.py — воркер агрегации позиций (шаг 2: выборка RSI и расчёт корзин)

import asyncio
import logging
import json
import math
from datetime import datetime

log = logging.getLogger("IND_AGG")

STREAM   = "signal_log_queue"          # читаем post-commit события
GROUP    = "indicators_agg_group"
CONSUMER = "ind_agg_1"

READ_COUNT = 50
READ_BLOCK_MS = 2000

RSI_BUCKET_STEP = 5  # шаг корзинки RSI


# 🔸 Универсальный парсер ISO-строк (поддерживает 'Z' и смещения)
def _parse_iso(dt: str) -> datetime:
    s = dt.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


# 🔸 Загрузка позиции по uid из positions_v4
async def _fetch_position(pg, position_uid: str):
    async with pg.acquire() as conn:
        return await conn.fetchrow(
            """
            SELECT
                position_uid,
                strategy_id,
                symbol,
                direction,
                status,
                created_at,
                closed_at,
                pnl,
                audited
            FROM positions_v4
            WHERE position_uid = $1
            """,
            position_uid,
        )


# 🔸 Выборка RSI-снимков позиции из positions_indicators_stat
async def _fetch_rsi_snapshots(pg, position_uid: str):
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT timeframe, param_name, value_num
            FROM positions_indicators_stat
            WHERE position_uid = $1
              AND param_name ILIKE 'rsi%%'
              AND value_num IS NOT NULL
              AND value_num BETWEEN 0 AND 100
            """,
            position_uid,
        )
        return rows


# 🔸 Расчёт корзины для значения RSI
def _rsi_bucket(value: float, step: int = RSI_BUCKET_STEP) -> int:
    try:
        v = float(value)
    except Exception:
        return None
    if not math.isfinite(v):
        return None
    if v < 0:
        v = 0.0
    if v > 100:
        v = 100.0
    return int(math.floor(v / step) * step)


# 🔸 Основной воркер: читаем post-commit события, фильтруем закрытия, считаем RSI-бакеты (лог)
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
                            log.info(f"[SKIP] uid={uid} already audited")
                            continue
                        if row["status"] != "closed" or row["pnl"] is None:
                            log.warning(f"[SKIP] uid={uid} post-commit status mismatch (status={row['status']}, pnl={row['pnl']})")
                            continue

                        strategy_id = row["strategy_id"]
                        pnl = float(row["pnl"]) if row["pnl"] is not None else None
                        closed_at = row["closed_at"]

                        log.info(f"[READY] uid={uid} strategy={strategy_id} pnl={pnl} closed_at={closed_at} → считаем RSI-бакеты")

                        snaps = await _fetch_rsi_snapshots(pg, uid)
                        if not snaps:
                            log.info(f"[NO-RSI] uid={uid} нет RSI-снимков в positions_indicators_stat")
                            continue

                        for s in snaps:
                            tf = s["timeframe"]
                            param = s["param_name"]
                            value = s["value_num"]
                            bucket = _rsi_bucket(value)
                            if bucket is None:
                                log.info(f"[SKIP-RSI] uid={uid} tf={tf} param={param} value={value} → non-finite")
                                continue
                            log.info(f"[BUCKET] uid={uid} tf={tf} param={param} value={value:.4f} → bucket={bucket}")

                        # на этом шаге только логируем бакеты; запись агрегатов будет на шаге 3

                    except Exception:
                        log.exception("Ошибка обработки сообщения signal_log_queue")

            if to_ack:
                await redis.xack(STREAM, GROUP, *to_ack)

        except Exception as e:
            log.error(f"Ошибка в цикле IND_AGG: {e}", exc_info=True)
            await asyncio.sleep(2)