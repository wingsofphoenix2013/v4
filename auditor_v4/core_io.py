# core_io.py

import asyncio
import logging
from datetime import datetime

import infra

# 🔸 Логгер для PostgreSQL операций
log = logging.getLogger("CORE_IO")

# 🔸 Параметры
MAX_PARALLEL_TASKS = 20

TF_SECONDS = {
    "m1": 60,
    "m5": 300,
    "m15": 900,
}


# 🔸 Вычисление open_time предыдущей закрытой свечи
def get_last_closed_open_time(created_at: datetime, tf: str) -> datetime:
    tf_sec = TF_SECONDS[tf]
    ts = int(created_at.timestamp())
    return datetime.fromtimestamp(ts - (ts % tf_sec) - tf_sec)


# 🔸 Загрузка неаудированных закрытых позиций
async def load_unprocessed_positions(limit: int = 100) -> list[dict]:
    log.info("📥 Загрузка неаудированных позиций из базы...")
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT *
            FROM positions_v4
            WHERE status = 'closed' AND audited = false
            ORDER BY created_at
            LIMIT $1
        """, limit)
    log.info(f"📊 Загружено {len(rows)} позиций на аудит")
    return [dict(r) for r in rows]
# 🔸 Обработка одной позиции
async def process_position(position: dict):
    uid = position["position_uid"]
    symbol = position["symbol"]
    strategy_id = position["strategy_id"]
    created_at = position["created_at"]

    strategy = infra.enabled_strategies.get(strategy_id)
    if not strategy:
        log.warning(f"⚠️ Стратегия {strategy_id} не найдена — позиция {uid} пропущена")
        return

    strategy_name = strategy.get("name")
    base_tf = strategy.get("timeframe", "").lower()

    tf_order = ["m1", "m5", "m15"]
    if base_tf not in tf_order:
        log.warning(f"⚠️ Неизвестный таймфрейм '{base_tf}' — позиция {uid} пропущена")
        return

    base_idx = tf_order.index(base_tf)
    allowed_tfs = tf_order[:base_idx + 1]

    # 🔸 Фильтрация индикаторов только по допустимым таймфреймам
    indicators = [
        i for i in infra.enabled_indicators.values()
        if i.get("timeframe") in allowed_tfs
    ]

    if not indicators:
        log.info(f"ℹ️ Нет индикаторов для позиции {uid}")
        return

    snapshot_rows = []

    async with infra.pg_pool.acquire() as conn:
        for ind in indicators:
            tf = ind["timeframe"]
            ot = get_last_closed_open_time(created_at, tf)

            rows = await conn.fetch("""
                SELECT param_name, value, open_time
                FROM indicator_values_v4
                WHERE instance_id = $1 AND symbol = $2 AND open_time = $3
            """, ind["id"], symbol, ot)

            if not rows:
                log.debug(f"🔸 Нет значений для индикатора {ind['id']} на {ot} ({tf})")
                continue

            for row in rows:
                snapshot_rows.append({
                    "position_uid": uid,
                    "strategy_id": strategy_id,
                    "strategy_name": strategy_name,
                    "indicator_id": ind["id"],
                    "param_name": row["param_name"],
                    "value": row["value"],
                    "timeframe": tf,
                    "source_time": row["open_time"]
                })

    if snapshot_rows:
        await insert_ind_snapshot(snapshot_rows)
        await mark_position_audited(uid)
        log.info(f"✅ Позиция {uid} обработана ({len(snapshot_rows)} значений)")
    else:
        log.warning(f"⚠️ Позиция {uid} — ни одного значения индикатора не найдено")
# 🔸 Вставка слепков индикаторов
async def insert_ind_snapshot(snapshot_rows: list[dict]):
    async with infra.pg_pool.acquire() as conn:
        await conn.executemany("""
            INSERT INTO position_ind_stat_v4 (
                position_uid, strategy_id, strategy_name,
                indicator_id, param_name, value,
                timeframe, source_time
            )
            VALUES (
                $1, $2, $3,
                $4, $5, $6,
                $7, $8
            )
            ON CONFLICT DO NOTHING
        """, [
            (
                row["position_uid"],
                row["strategy_id"],
                row["strategy_name"],
                row["indicator_id"],
                row["param_name"],
                row["value"],
                row["timeframe"],
                row["source_time"]
            ) for row in snapshot_rows
        ])


# 🔸 Отметка позиции как обработанной
async def mark_position_audited(position_uid: str):
    async with infra.pg_pool.acquire() as conn:
        await conn.execute("""
            UPDATE positions_v4
            SET audited = true
            WHERE position_uid = $1
        """, position_uid)


# 🔸 Обёртка с семафором
async def process_with_semaphore(position: dict, semaphore: asyncio.Semaphore):
    async with semaphore:
        try:
            await process_position(position)
        except Exception:
            log.exception(f"❌ Ошибка при обработке позиции {position['position_uid']}")


# 🔸 Основной воркер PostgreSQL
async def pg_task():
    log.info("🔁 [pg_task] стартует")

    try:
        while True:
            try:
                log.info("🔁 Начало аудиторского прохода")
                positions = await load_unprocessed_positions()

                if not positions:
                    log.info("✅ Нет новых позиций для аудита — пауза")
                    await asyncio.sleep(60)
                    continue

                semaphore = asyncio.Semaphore(MAX_PARALLEL_TASKS)
                tasks = [
                    process_with_semaphore(pos, semaphore)
                    for pos in positions
                ]
                await asyncio.gather(*tasks, return_exceptions=True)

                log.info("⏸ Пауза до следующего цикла")
                await asyncio.sleep(60)

            except Exception:
                log.exception("❌ Ошибка в pg_task — продолжаем выполнение")
                await asyncio.sleep(5)

    except Exception:
        log.exception("🔥 Ошибка вне цикла в pg_task — выясняем причину")
        await asyncio.sleep(5)
# 🔸 Финмониторинг позиций
async def finmonitor_task():
    log = logging.getLogger("FINMONITOR")
    log.info("🔁 [finmonitor_task] стартует")

    while True:
        try:
            async with infra.pg_pool.acquire() as conn:
                # 1. Загружаем стратегии, у которых разрешён аудит
                strategy_rows = await conn.fetch("""
                    SELECT id FROM strategies_v4
                    WHERE auditor_enabled = true
                """)
                strategy_ids = [r["id"] for r in strategy_rows]

                if not strategy_ids:
                    log.info("ℹ️ Нет стратегий с включённым аудитом")
                    await asyncio.sleep(60)
                    continue

                # 2. Загружаем позиции с finmonitor = false
                position_rows = await conn.fetch("""
                    SELECT strategy_id, position_uid, symbol,
                           created_at, closed_at, pnl
                    FROM positions_v4
                    WHERE status = 'closed'
                      AND finmonitor = false
                      AND strategy_id = ANY($1::int[])
                """, strategy_ids)

                if not position_rows:
                    log.info("✅ Нет новых позиций для финмониторинга — пауза")
                    await asyncio.sleep(60)
                    continue

                # 3. Формируем строки для вставки
                insert_data = []
                mark_done = []

                for row in position_rows:
                    created = row["created_at"]
                    closed = row["closed_at"]
                    duration = int((closed - created).total_seconds() // 60)

                    insert_data.append((
                        row["strategy_id"],
                        row["position_uid"],
                        row["symbol"],
                        created,
                        closed,
                        duration,
                        "win" if row["pnl"] > 0 else "loss",
                        row["pnl"]
                    ))
                    mark_done.append(row["position_uid"])

                # 4. Вставляем в strategies_finmonitor_v4
                await conn.executemany("""
                    INSERT INTO strategies_finmonitor_v4 (
                        strategy_id, position_uid, symbol,
                        created_at, closed_at, duration,
                        result, pnl
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                    ON CONFLICT DO NOTHING
                """, insert_data)

                # 5. Обновляем positions_v4
                await conn.executemany("""
                    UPDATE positions_v4
                    SET finmonitor = true
                    WHERE position_uid = $1
                """, [(uid,) for uid in mark_done])

                log.info(f"✅ Обработано финмониторингом {len(mark_done)} позиций")

        except Exception:
            log.exception("❌ Ошибка в finmonitor_task")

        await asyncio.sleep(60)