# 🔸 Агрегатор итогов лаборатории: читает сигналы завершения ранa и пишет свод по стратегии

import os
import asyncio
import logging
from decimal import Decimal, ROUND_HALF_UP

import laboratory_v4_infra as infra

# 🔸 Логгер и настройки чтения стрима
log = logging.getLogger("LAB_RESULTS_AGG")

GROUP_NAME    = os.getenv("LAB_RESULTS_GROUP",    "lab_results_aggregator")
CONSUMER_NAME = os.getenv("LAB_RESULTS_CONSUMER", "lab_results_aggregator_1")
XREAD_COUNT   = int(os.getenv("LAB_RESULTS_COUNT",    "50"))
XREAD_BLOCKMS = int(os.getenv("LAB_RESULTS_BLOCK_MS", "1000"))


# 🔸 Идемпотентное создание consumer-group
async def _ensure_group():
    try:
        await infra.redis_client.xgroup_create(infra.FINISH_STREAM, GROUP_NAME, id="$", mkstream=True)
        log.debug("Создана consumer group '%s' на стриме '%s'", GROUP_NAME, infra.FINISH_STREAM)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug("Consumer group '%s' уже существует", GROUP_NAME)
        else:
            log.exception("Ошибка создания consumer group: %s", e)
            raise


# 🔸 Чтение депозита стратегии
async def _load_strategy_deposit(strategy_id: int) -> Decimal:
    async with infra.pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT deposit FROM strategies_v4 WHERE id=$1",
            strategy_id,
        )
    if not row or row["deposit"] is None:
        return Decimal("0")
    return Decimal(str(row["deposit"]))


# 🔸 Подсчёт метрик по approved-позициям рана
async def _aggregate_run(lab_id: int, strategy_id: int, run_id: int):
    async with infra.pg_pool.acquire() as conn:
        # approved-позиции данного рана
        row = await conn.fetchrow(
            """
            WITH approved AS (
              SELECT position_uid
              FROM laboratory_results_v4
              WHERE run_id=$1 AND lab_id=$2 AND strategy_id=$3 AND test_result='approved'
            )
            SELECT
              COUNT(p.position_uid)                                   AS approved_trades,
              SUM(CASE WHEN p.pnl > 0 THEN 1 ELSE 0 END)              AS won_trades,
              COALESCE(SUM(p.pnl), 0)                                 AS pnl_sum
            FROM positions_v4 p
            JOIN approved a ON a.position_uid = p.position_uid
            """,
            run_id, lab_id, strategy_id,
        )

    approved_trades = int(row["approved_trades"] or 0)
    won_trades      = int(row["won_trades"] or 0)
    pnl_sum_raw     = Decimal(str(row["pnl_sum"] or "0"))

    if approved_trades > 0:
        winrate = (Decimal(won_trades) / Decimal(approved_trades)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    else:
        winrate = Decimal("0")

    pnl_sum = pnl_sum_raw.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    deposit = await _load_strategy_deposit(strategy_id)
    if deposit > 0:
        roi = (pnl_sum / deposit).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    else:
        roi = Decimal("0")

    return approved_trades, pnl_sum, winrate, roi


# 🔸 UPSERT результатов в laboratory_strategy_results_v4
async def _upsert_strategy_results(lab_id: int, strategy_id: int, run_id: int,
                                   pnl_sum: Decimal, winrate: Decimal, roi: Decimal):
    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO laboratory_strategy_results_v4
              (run_id, lab_id, strategy_id, pnl_sum_approved, winrate_approved, roi_approved, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, NOW())
            ON CONFLICT (run_id, lab_id, strategy_id)
            DO UPDATE SET
              pnl_sum_approved = EXCLUDED.pnl_sum_approved,
              winrate_approved = EXCLUDED.winrate_approved,
              roi_approved     = EXCLUDED.roi_approved,
              created_at       = NOW()
            """,
            run_id, lab_id, strategy_id,
            str(pnl_sum), str(winrate), str(roi),
        )


# 🔸 Обработка одного сообщения из стрима
async def _handle_message(fields: dict):
    try:
        lab_id      = int(fields.get("lab_id"))
        strategy_id = int(fields.get("strategy_id"))
        run_id      = int(fields.get("run_id"))
    except Exception:
        log.error("Неверные поля сообщения: %s", fields)
        return

    approved_cnt, pnl_sum, winrate, roi = await _aggregate_run(lab_id, strategy_id, run_id)
    await _upsert_strategy_results(lab_id, strategy_id, run_id, pnl_sum, winrate, roi)

    log.debug(
        "AGG DONE lab=%s strategy=%s run_id=%s: approved=%s pnl_sum=%s winrate=%s roi=%s",
        lab_id, strategy_id, run_id, approved_cnt, str(pnl_sum), str(winrate), str(roi)
    )


# 🔸 Главный цикл аггрегатора результатов
async def run_laboratory_results_aggregator():
    await _ensure_group()
    log.debug("Слушаем стрим '%s' (group=%s, consumer=%s)", infra.FINISH_STREAM, GROUP_NAME, CONSUMER_NAME)

    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={infra.FINISH_STREAM: ">"},
                count=XREAD_COUNT,
                block=XREAD_BLOCKMS,
            )
            if not resp:
                continue

            to_ack = []
            for _, records in resp:
                for msg_id, data in records:
                    try:
                        await _handle_message(data)
                        to_ack.append(msg_id)
                    except Exception as e:
                        log.exception("Ошибка обработки сообщения %s: %s", msg_id, e)
                        to_ack.append(msg_id)

            if to_ack:
                await infra.redis_client.xack(infra.FINISH_STREAM, GROUP_NAME, *to_ack)

        except asyncio.CancelledError:
            log.debug("Агрегатор результатов остановлен по сигналу")
            raise
        except Exception as e:
            log.exception("Ошибка в XREADGROUP цикле: %s", e)
            await asyncio.sleep(1)