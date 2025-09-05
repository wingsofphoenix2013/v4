# 🔸 Агрегатор итогов лаборатории: читает сигналы завершения ранa, пишет свод по стратегии и очищает per-position результаты

import os
import asyncio
import logging
from decimal import Decimal, ROUND_HALF_UP

import laboratory_v4_infra as infra

# 🔸 Логгер и настройки чтения стрима
log = logging.getLogger("LAB_RESULTS_AGG")

STREAM_NAME   = infra.FINISH_STREAM
GROUP_NAME    = os.getenv("LAB_RESULTS_GROUP",    "lab_results_aggregator")
CONSUMER_NAME = os.getenv("LAB_RESULTS_CONSUMER", "lab_results_aggregator_1")
# фиксируем «ширину глотка» и таймауты (можно переопределить ENV при желании)
XREAD_COUNT   = int(os.getenv("LAB_RESULTS_COUNT",    "250"))   # сколько сообщений забираем за тик
XREAD_BLOCKMS = int(os.getenv("LAB_RESULTS_BLOCK_MS", "200"))   # блокировка XREADGROUP, мс
RESET_GROUP   = os.getenv("LAB_RESULTS_RESET_GROUP", "false").lower() == "true"

# 🔸 Параллелизм агрегации и параметры очистки
AGG_CONCURRENCY = int(os.getenv("LAB_RESULTS_AGG_CONCURRENCY", "6"))  # одновременно агрегируемых run’ов
PURGE_AFTER_AGG = os.getenv("LAB_PURGE_RESULTS_AFTER_AGG", "true").lower() == "true"
PURGE_CHUNK     = int(os.getenv("LAB_PURGE_CHUNK", "100000"))  # размер одного чанка DELETE

# 🔸 Семафор для ограничения параллелизма
_sem = asyncio.Semaphore(AGG_CONCURRENCY)


# 🔸 Инициализация consumer-group (вариант A: reset-группы по ENV)
async def _ensure_group():
    try:
        if RESET_GROUP:
            try:
                await infra.redis_client.xgroup_destroy(STREAM_NAME, GROUP_NAME)
                log.info("Сброс consumer-group '%s' на стриме '%s'", GROUP_NAME, STREAM_NAME)
            except Exception:
                pass
        await infra.redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, id="$", mkstream=True)
        log.info("Создана consumer-group '%s' на стриме '%s' (id=$)", GROUP_NAME, STREAM_NAME)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug("Consumer-group '%s' уже существует", GROUP_NAME)
        else:
            log.exception("Ошибка создания consumer-group: %s", e)
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


# 🔸 Подсчёт метрик по approved-позициям рана + масштаб выборки
async def _aggregate_run(lab_id: int, strategy_id: int, run_id: int):
    async with infra.pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            WITH all_rows AS (
              SELECT position_uid
              FROM laboratory_results_v4
              WHERE run_id=$1 AND lab_id=$2 AND strategy_id=$3
            ),
            approved AS (
              SELECT position_uid
              FROM laboratory_results_v4
              WHERE run_id=$1 AND lab_id=$2 AND strategy_id=$3 AND test_result='approved'
            )
            SELECT
              (SELECT COUNT(*) FROM all_rows)   AS raw_positions,
              (SELECT COUNT(*) FROM approved)   AS approved_positions,
              COUNT(p.position_uid)             AS approved_trades,   -- по join с positions_v4
              SUM(CASE WHEN p.pnl > 0 THEN 1 ELSE 0 END) AS won_trades,
              COALESCE(SUM(p.pnl), 0)           AS pnl_sum
            FROM positions_v4 p
            JOIN approved a ON a.position_uid = p.position_uid
            """,
            run_id, lab_id, strategy_id,
        )

    raw_positions      = int(row["raw_positions"] or 0)
    approved_positions = int(row["approved_positions"] or 0)
    won_trades         = int(row["won_trades"] or 0)
    pnl_sum_raw        = Decimal(str(row["pnl_sum"] or "0"))

    if approved_positions > 0:
        winrate = (Decimal(won_trades) / Decimal(approved_positions)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    else:
        winrate = Decimal("0")

    pnl_sum = pnl_sum_raw.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    deposit = await _load_strategy_deposit(strategy_id)
    if deposit > 0:
        roi = (pnl_sum / deposit).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    else:
        roi = Decimal("0")

    return raw_positions, approved_positions, pnl_sum, winrate, roi


# 🔸 UPSERT результатов в laboratory_strategy_results_v4
async def _upsert_strategy_results(lab_id: int, strategy_id: int, run_id: int,
                                   raw_positions: int, approved_positions: int,
                                   pnl_sum: Decimal, winrate: Decimal, roi: Decimal):
    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO laboratory_strategy_results_v4
              (run_id, lab_id, strategy_id,
               pnl_sum_approved, winrate_approved, roi_approved,
               raw_positions, approved_positions,
               created_at)
            VALUES ($1, $2, $3,
                    $4, $5, $6,
                    $7, $8,
                    NOW())
            ON CONFLICT (run_id, lab_id, strategy_id)
            DO UPDATE SET
              pnl_sum_approved   = EXCLUDED.pnl_sum_approved,
              winrate_approved   = EXCLUDED.winrate_approved,
              roi_approved       = EXCLUDED.roi_approved,
              raw_positions      = EXCLUDED.raw_positions,
              approved_positions = EXCLUDED.approved_positions,
              created_at         = NOW()
            """,
            run_id, lab_id, strategy_id,
            str(pnl_sum), str(winrate), str(roi),
            int(raw_positions), int(approved_positions),
        )


# 🔸 Проверка наличия run перед апдейтом
async def _run_exists(run_id: int) -> bool:
    async with infra.pg_pool.acquire() as conn:
        row = await conn.fetchval("SELECT 1 FROM laboratory_runs_v4 WHERE id=$1", run_id)
    return bool(row)


# 🔸 Пакетная очистка per-position результатов по run_id
async def _purge_run_results(run_id: int):
    total = 0
    while True:
        async with infra.pg_pool.acquire() as conn:
            async with conn.transaction():
                status = await conn.execute(
                    """
                    DELETE FROM laboratory_results_v4
                    WHERE ctid IN (
                        SELECT ctid
                        FROM laboratory_results_v4
                        WHERE run_id = $1
                        LIMIT $2
                    )
                    """,
                    run_id, PURGE_CHUNK,
                )
        try:
            deleted = int(status.split()[-1])
        except Exception:
            deleted = 0
        total += deleted
        if deleted == 0:
            break
        await asyncio.sleep(0)
    log.debug("PURGE DONE run_id=%s removed=%s rows", run_id, total)


# 🔸 Обработка одного сообщения (одного run_id)
async def _handle_message(fields: dict):
    try:
        lab_id      = int(fields.get("lab_id"))
        strategy_id = int(fields.get("strategy_id"))
        run_id      = int(fields.get("run_id"))
    except Exception:
        log.error("Неверные поля сообщения: %s", fields)
        return

    if not await _run_exists(run_id):
        log.debug("Пропуск сообщения: run_id=%s отсутствует в laboratory_runs_v4", run_id)
        return

    raw_cnt, approved_cnt, pnl_sum, winrate, roi = await _aggregate_run(lab_id, strategy_id, run_id)
    await _upsert_strategy_results(lab_id, strategy_id, run_id,
                                   raw_cnt, approved_cnt,
                                   pnl_sum, winrate, roi)

    if PURGE_AFTER_AGG:
        try:
            await _purge_run_results(run_id)
        except Exception as e:
            log.exception("Ошибка очистки per-position результатов для run_id=%s: %s", run_id, e)

    log.debug(
        "AGG DONE lab=%s strategy=%s run_id=%s: raw=%s approved=%s pnl_sum=%s winrate=%s roi=%s",
        lab_id, strategy_id, run_id, raw_cnt, approved_cnt, str(pnl_sum), str(winrate), str(roi)
    )


# 🔸 Обработчик с ограничением параллелизма
async def _handle_message_guarded(fields: dict):
    async with _sem:
        await _handle_message(fields)


# 🔸 Главный цикл аггрегатора результатов
async def run_laboratory_results_aggregator():
    await _ensure_group()
    log.debug("Слушаем стрим '%s' (group=%s, consumer=%s)", STREAM_NAME, GROUP_NAME, CONSUMER_NAME)

    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={STREAM_NAME: ">"},
                count=XREAD_COUNT,
                block=XREAD_BLOCKMS,
            )
            if not resp:
                continue

            to_ack = []
            tasks: list[asyncio.Task] = []

            for _, records in resp:
                for msg_id, data in records:
                    tasks.append(asyncio.create_task(_handle_message_guarded(data)))
                    to_ack.append(msg_id)

            if tasks:
                # Дождаться завершения пачки; исключения уже залогируются внутри обработчика
                await asyncio.gather(*tasks, return_exceptions=True)

            if to_ack:
                await infra.redis_client.xack(STREAM_NAME, GROUP_NAME, *to_ack)

        except asyncio.CancelledError:
            log.debug("Агрегатор результатов остановлен по сигналу")
            raise
        except Exception as e:
            log.exception("Ошибка в XREADGROUP цикле: %s", e)
            await asyncio.sleep(1)