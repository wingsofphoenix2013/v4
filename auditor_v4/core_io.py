# core_io.py

import os
import aiohttp
import asyncio
import logging
from datetime import datetime
from decimal import Decimal

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
    allowed_tfs = tf_order[base_idx:]  # 👈 Изменено: от base_tf и выше

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

                # 2. Загружаем закрытые позиции с finmonitor = false
                position_rows = await conn.fetch("""
                    SELECT strategy_id, position_uid, symbol,
                           direction,
                           created_at, closed_at, pnl,
                           entry_price, exit_price
                    FROM positions_v4
                    WHERE status = 'closed'
                      AND finmonitor = false
                      AND strategy_id = ANY($1::int[])
                    ORDER BY closed_at
                    LIMIT 100
                """, strategy_ids)

                if not position_rows:
                    log.info("✅ Нет новых позиций для финмониторинга — пауза")
                    await asyncio.sleep(60)
                    continue

                insert_data = []
                mark_done = []

                for row in position_rows:
                    created = row["created_at"]
                    closed = row["closed_at"]
                    duration = int((closed - created).total_seconds() // 60)
                    result = "win" if row["pnl"] > 0 else "loss"

                    insert_data.append((
                        row["strategy_id"],
                        row["position_uid"],
                        row["symbol"],
                        created,
                        closed,
                        duration,
                        result,
                        row["pnl"]
                    ))
                    mark_done.append(row["position_uid"])

                    # 🔸 Форматируем время и направление
                    created_fmt = created.strftime("%Y-%m-%d %H:%M UTC")
                    closed_fmt = closed.strftime("%Y-%m-%d %H:%M UTC")
                    dir_label = "🟢 long" if row["direction"] == "long" else "🔴 short"

                    # 🔸 Формируем сообщение для Telegram
                    if row["pnl"] > 0:
                        msg = (
                            "🟢 <b>Nice one! We bagged some profit 💰</b>\n\n"
                            f"⚔️ Took a {dir_label} shot on <b>{row['symbol']}</b>\n"
                            f"🎯 Entry: <code>{row['entry_price']}</code>\n"
                            f"🏁 Exit: <code>{row['exit_price']}</code>\n"
                            f"💵 PnL: <b>+{row['pnl']}</b>\n"
                            f"🕓 It took us {duration} minutes of glorious trading 🧠\n"
                            f"⏳ {created_fmt} → {closed_fmt}"
                        )
                    else:
                        msg = (
                            "🔴 <b>Not our proudest moment... but hey, we tried 😅</b>\n\n"
                            f"⚔️ Took a {dir_label} shot on <b>{row['symbol']}</b>\n"
                            f"🎯 Entry: <code>{row['entry_price']}</code>\n"
                            f"🏁 Exit: <code>{row['exit_price']}</code>\n"
                            f"💸 PnL: <b>{row['pnl']}</b>\n"
                            f"🕓 We fought bravely for {duration} minutes… and then accepted fate 🙃\n"
                            f"⏳ {created_fmt} → {closed_fmt}"
                        )

                    await send_telegram_message(msg)

                # 3. Вставляем в финмониторинг
                await conn.executemany("""
                    INSERT INTO strategies_finmonitor_v4 (
                        strategy_id, position_uid, symbol,
                        created_at, closed_at, duration,
                        result, pnl
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                    ON CONFLICT DO NOTHING
                """, insert_data)

                # 4. Обновляем пометки в positions_v4
                await conn.executemany("""
                    UPDATE positions_v4
                    SET finmonitor = true
                    WHERE position_uid = $1
                """, [(uid,) for uid in mark_done])

                log.info(f"📌 Обработано в финмониторинге: {len(mark_done)} позиций")

        except Exception:
            log.exception("❌ Ошибка в finmonitor_task")

        await asyncio.sleep(60)
# 🔸 Казначейская обработка
async def treasury_task():
    log = logging.getLogger("TREASURY")
    log.info("🔁 [treasury_task] стартует")

    while True:
        try:
            async with infra.pg_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT strategy_id, position_uid, pnl
                    FROM strategies_finmonitor_v4
                    WHERE treasurised = false
                    ORDER BY closed_at
                    LIMIT 100
                """)

                if not rows:
                    log.info("✅ Нет новых позиций для казначейства — пауза")
                    await asyncio.sleep(60)
                    continue

                for r in rows:
                    strategy_id = r["strategy_id"]
                    position_uid = r["position_uid"]
                    pnl = Decimal(r["pnl"])

                    async with conn.transaction():
                        await conn.execute("""
                            INSERT INTO strategies_treasury_v4 (strategy_id, strategy_deposit)
                            SELECT id, deposit FROM strategies_v4 WHERE id = $1
                            ON CONFLICT DO NOTHING
                        """, strategy_id)

                        treasury = await conn.fetchrow("""
                            SELECT pnl_total, pnl_operational, pnl_insurance
                            FROM strategies_treasury_v4
                            WHERE strategy_id = $1
                            FOR UPDATE
                        """, strategy_id)

                        op = Decimal(treasury["pnl_operational"])
                        ins = Decimal(treasury["pnl_insurance"])

                        delta_op = Decimal("0.00")
                        delta_ins = Decimal("0.00")

                        if pnl > 0:
                            # сначала покрываем минус в страховом фонде
                            if ins < 0:
                                cover = min(pnl, abs(ins))
                                delta_ins = cover
                                ins += cover
                                remainder = pnl - cover
                                if remainder > 0:
                                    delta_op = remainder
                                    op += remainder
                                comment = (
                                    f"Прибыльная позиция: +{pnl:.2f} → покрыт минус страхового фонда {cover:.2f}"
                                )
                                if remainder > 0:
                                    comment += f", остаток {remainder:.2f} отправлен в кассу"
                            else:
                                delta_op = (pnl * Decimal("0.9")).quantize(Decimal("0.01"))
                                delta_ins = (pnl * Decimal("0.1")).quantize(Decimal("0.01"))
                                op += delta_op
                                ins += delta_ins
                                comment = (
                                    f"Прибыльная позиция: +{pnl:.2f} → распределено "
                                    f"{delta_op:.2f} в кассу, {delta_ins:.2f} в страховой фонд"
                                )
                        else:
                            loss = abs(pnl)
                            from_op = min(loss, op)
                            from_ins = loss - from_op

                            delta_op = -from_op
                            delta_ins = -from_ins

                            op -= from_op
                            ins -= from_ins

                            if from_op == 0 and from_ins == 0:
                                comment = (
                                    f"Убыточная позиция: {pnl:.2f} → касса пуста, "
                                    f"страховой фонд пуст, убыток записан в страховой фонд: {pnl:.2f}"
                                )
                            elif from_op > 0 and from_ins == 0:
                                comment = (
                                    f"Убыточная позиция: {pnl:.2f} → списано {from_op:.2f} из кассы"
                                )
                            elif from_op == 0 and from_ins > 0:
                                comment = (
                                    f"Убыточная позиция: {pnl:.2f} → касса пуста, "
                                    f"списано {from_ins:.2f} из страхового фонда"
                                )
                            else:
                                comment = (
                                    f"Убыточная позиция: {pnl:.2f} → списано {from_op:.2f} из кассы, "
                                    f"{from_ins:.2f} из страхового фонда"
                                )

                        comment += f" (касса: {op:.2f}, фонд: {ins:.2f})"

                        await conn.execute("""
                            UPDATE strategies_treasury_v4
                            SET pnl_total = pnl_total + $2,
                                pnl_operational = $3,
                                pnl_insurance = $4,
                                updated_at = now()
                            WHERE strategy_id = $1
                        """, strategy_id, pnl, op, ins)

                        await conn.execute("""
                            INSERT INTO strategies_treasury_log_v4 (
                                strategy_id, position_uid, timestamp,
                                operation_type, pnl, delta_operational,
                                delta_insurance, comment
                            )
                            VALUES ($1, $2, now(), $3, $4, $5, $6, $7)
                        """, strategy_id, position_uid,
                             "income" if pnl > 0 else "loss",
                             pnl, delta_op, delta_ins, comment)

                        await conn.execute("""
                            UPDATE strategies_finmonitor_v4
                            SET treasurised = true
                            WHERE position_uid = $1
                        """, position_uid)

                        log.info(f"💰 Позиция {position_uid} обработана")

        except Exception:
            log.exception("❌ Ошибка в treasury_task")

        await asyncio.sleep(60)
# 🔸 Отправка сообщения в Telegram
async def send_telegram_message(text: str):
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        log.warning("❌ TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не заданы — сообщение не отправлено")
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=payload) as resp:
                if resp.status == 200:
                    log.debug("📤 Telegram: сообщение успешно отправлено")
                else:
                    body = await resp.text()
                    log.warning(f"❌ Telegram API вернул ошибку {resp.status}: {body}")
    except Exception as e:
        log.exception(f"❌ Ошибка отправки Telegram-сообщения: {e}")