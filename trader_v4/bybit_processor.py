# bybit_processor.py — воркер исполнения: читает positions_bybit_orders (только новые), pre-flight (live), dry-run заполнение entry по last price, строит карту TP/SL и пишет в БД

# 🔸 Импорты
import os
import json
import asyncio
import logging
import hashlib
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Tuple, Optional, Any, List

import httpx

from trader_infra import infra
from trader_config import config  # берём политики стратегий из in-memory кэша

# 🔸 Логгер
log = logging.getLogger("BYBIT_PROCESSOR")

# 🔸 Константы стримов/CG
ORDERS_STREAM = "positions_bybit_orders"
BYBIT_PROC_CG = "bybit_processor_cg"
BYBIT_PROC_CONSUMER = os.getenv("BYBIT_PROC_CONSUMER", "bybit-proc-1")
AUDIT_STREAM = "positions_bybit_audit"

# 🔸 Параллелизм
MAX_PARALLEL_TASKS = int(os.getenv("BYBIT_PROC_MAX_TASKS", "200"))
LOCK_TTL_SEC = int(os.getenv("BYBIT_PROC_LOCK_TTL", "30"))

# 🔸 BYBIT ENV
API_KEY = os.getenv("BYBIT_API_KEY", "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com")
RECV_WINDOW = os.getenv("BYBIT_RECV_WINDOW", "5000")
ACCOUNT_TYPE = os.getenv("BYBIT_ACCOUNT_TYPE", "UNIFIED")        # не используется здесь, но оставим для будущего
CATEGORY = "linear"
MARGIN_MODE = os.getenv("BYBIT_MARGIN_MODE", "isolated")         # always isolated по ТЗ
POSITION_MODE = os.getenv("BYBIT_POSITION_MODE", "oneway")

# 🔸 Режим исполнения
TRADER_ORDER_MODE = os.getenv("TRADER_ORDER_MODE", "dry_run")    # dry_run | live

# 🔸 Локальные мьютексы по ключу (strategy_id, symbol)
_local_locks: Dict[Tuple[int, str], asyncio.Lock] = {}


# 🔸 Основной запуск воркера
async def run_bybit_processor():
    redis = infra.redis_client

    # создание CG (id="$" — только новые записи)
    try:
        await redis.xgroup_create(ORDERS_STREAM, BYBIT_PROC_CG, id="$", mkstream=True)
        log.info("📡 Создана CG %s для стрима %s", BYBIT_PROC_CG, ORDERS_STREAM)
    except Exception:
        # группа уже существует
        pass

    # сброс offset CG на '$' — читаем строго только новые записи после старта
    try:
        await redis.execute_command("XGROUP", "SETID", ORDERS_STREAM, BYBIT_PROC_CG, "$")
        log.info("⏩ CG %s для %s сброшена на $ (только новые)", BYBIT_PROC_CG, ORDERS_STREAM)
    except Exception:
        log.exception("❌ Не удалось сбросить CG %s для %s на $", BYBIT_PROC_CG, ORDERS_STREAM)

    sem = asyncio.Semaphore(MAX_PARALLEL_TASKS)

    # чтение из стрима в вечном цикле
    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=BYBIT_PROC_CG,
                consumername=BYBIT_PROC_CONSUMER,
                streams={ORDERS_STREAM: ">"},
                count=100,
                block=1000,  # мс
            )
            if not entries:
                continue

            tasks = []
            for _, records in entries:
                for entry_id, fields in records:
                    tasks.append(asyncio.create_task(_handle_order_entry(sem, entry_id, fields)))

            await asyncio.gather(*tasks)

        except Exception:
            log.exception("❌ Ошибка чтения/обработки из стрима %s", ORDERS_STREAM)
            await asyncio.sleep(1)


# 🔸 Обработка одной записи из positions_bybit_orders
async def _handle_order_entry(sem: asyncio.Semaphore, entry_id: str, fields: Dict[str, Any]):
    async with sem:
        redis = infra.redis_client

        # парсим payload
        try:
            data_raw = fields.get("data")
            if isinstance(data_raw, bytes):
                data_raw = data_raw.decode("utf-8", errors="ignore")
            payload = json.loads(data_raw or "{}")
        except Exception:
            log.exception("❌ Некорректный payload (id=%s) — ACK", entry_id)
            try:
                await redis.xack(ORDERS_STREAM, BYBIT_PROC_CG, entry_id)
            except Exception:
                pass
            return

        # ключевые поля
        order_link_id = payload.get("order_link_id")
        position_uid = payload.get("position_uid")
        sid = int(payload.get("strategy_id"))
        stype = payload.get("strategy_type")  # plain|reverse
        symbol = payload.get("symbol")
        direction = payload.get("direction")  # long|short
        side = payload.get("side")            # Buy|Sell
        leverage = Decimal(str(payload.get("leverage", "0")))
        qty = Decimal(str(payload.get("qty", "0")))
        size_mode = payload.get("size_mode")  # 'pct_of_virtual'
        size_pct = Decimal(str(payload.get("size_pct", "0")))
        margin_plan = Decimal(str(payload.get("margin_plan", "0")))
        order_mode = payload.get("order_mode", TRADER_ORDER_MODE)
        source_stream_id = payload.get("source_stream_id")
        ts = payload.get("ts")
        ts_ms = payload.get("ts_ms")

        # сериализация по ключу (strategy_id, symbol)
        key = (sid, symbol)
        lock = _local_locks.setdefault(key, asyncio.Lock())

        async with lock:
            # распределённый замок в Redis
            gate_key = f"tv4:gate:{sid}:{symbol}"
            owner = f"{BYBIT_PROC_CONSUMER}-{entry_id}"
            if not await _acquire_dist_lock(gate_key, owner, LOCK_TTL_SEC):
                # короткий локальный ретрай без ACK — вернёмся позже
                for _ in range(10):
                    await asyncio.sleep(0.2)
                    if await _acquire_dist_lock(gate_key, owner, LOCK_TTL_SEC):
                        break
                else:
                    log.info("⏳ Не взят замок %s — отложено (id=%s)", gate_key, entry_id)
                    return

            try:
                # карточка entry в БД (planned)
                await _insert_entry_order_card(
                    position_uid=position_uid,
                    strategy_id=sid,
                    strategy_type=stype,
                    symbol=symbol,
                    direction=direction,
                    side=side,
                    order_mode=order_mode,
                    source_stream_id=source_stream_id,
                    orders_stream_id=entry_id,
                    order_link_id=_suffix_link(order_link_id, "e"),
                    qty=qty,
                    leverage=leverage,
                )

                # dry_run: запрашиваем last price и считаем полное исполнение
                if order_mode == "dry_run":
                    last_price = await _get_last_price_linear(symbol)
                    if last_price is None or last_price <= 0:
                        # фолбэк: попробуем взять цену из Redis bb:price:{symbol}
                        last_price = await _get_price_from_redis(symbol)
                    if last_price is None or last_price <= 0:
                        # в крайнем случае считаем "1", но это только чтобы не падать
                        last_price = Decimal("1")

                    filled_qty = qty
                    avg_price = last_price

                    # апдейт карточки entry фактами fill + commit
                    await _update_entry_filled_and_commit(
                        position_uid=position_uid,
                        order_link_id=_suffix_link(order_link_id, "e"),
                        filled_qty=filled_qty,
                        avg_price=avg_price,
                        commit_criterion="dry_run",
                        late_tail_delta=None,
                    )

                    # аудит: entry_filled (dry_run)
                    await _publish_audit(
                        event="entry_filled",
                        data={
                            "criterion": "dry_run",
                            "order_link_id": _suffix_link(order_link_id, "e"),
                            "position_uid": position_uid,
                            "symbol": symbol,
                            "filled_qty": str(filled_qty),
                            "filled_pct": "100",
                            "avg_price": str(avg_price),
                            "source_stream_id": source_stream_id,
                            "ts": ts,
                            "ts_ms": ts_ms,
                            "mode": order_mode,
                        },
                    )

                    # сформировать и записать карту TP/SL (без ATR, только percent)
                    await _build_tp_sl_cards_after_entry(
                        position_uid=position_uid,
                        strategy_id=sid,
                        strategy_type=stype,
                        symbol=symbol,
                        direction=direction,
                        filled_qty=filled_qty,
                        entry_price=avg_price,
                        order_mode=order_mode,
                        source_stream_id=source_stream_id,
                        base_link=order_link_id,
                    )

                    # можно скорректировать журналы (опционально): статус в trader_positions_log/trader_signals
                    await _touch_journals_after_entry(
                        source_stream_id=source_stream_id,
                        note=f"entry dry-run filled @ {avg_price}",
                        processing_status="processing",
                    )

                    # ACK записи очереди
                    await infra.redis_client.xack(ORDERS_STREAM, BYBIT_PROC_CG, entry_id)
                    log.info("✅ ENTRY dry-run filled & TP/SL planned (sid=%s %s %s qty=%s @ %s) [id=%s]",
                             sid, symbol, direction, filled_qty, avg_price, entry_id)
                    return

                # live-режим (пока только pre-flight; правила fill обсудим и включим позже)
                await _preflight_symbol_settings(symbol=symbol, leverage=leverage)

                # TODO: LIVE create-order + watcher условий 95%/5s и 75%/60s — реализуем на следующем шаге

                # ACK даже в live-прототипе, пока без реальной отправки
                await infra.redis_client.xack(ORDERS_STREAM, BYBIT_PROC_CG, entry_id)
                log.info("🟡 LIVE preflight done (sid=%s %s %s qty=%s) [id=%s] — отправка/ожидание fill будет добавлено",
                         sid, symbol, direction, qty, entry_id)

            except Exception:
                log.exception("❌ Ошибка обработки задачи bybit_processor (sid=%s %s id=%s)", sid, symbol, entry_id)
                # не ACK — вернёмся ретраем
            finally:
                await _release_dist_lock(gate_key, owner)


# 🔸 Вставка карточки entry в trader_position_orders
async def _insert_entry_order_card(
    *,
    position_uid: str,
    strategy_id: int,
    strategy_type: str,
    symbol: str,
    direction: str,
    side: str,
    order_mode: str,
    source_stream_id: str,
    orders_stream_id: str,
    order_link_id: str,
    qty: Decimal,
    leverage: Decimal,
):
    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO trader_position_orders (
                position_uid, strategy_id, strategy_type, symbol, direction, side, order_mode,
                source_stream_id, orders_stream_id,
                kind, level, activation, activation_tp_level, is_active,
                reduce_only, tif, qty, price,
                order_link_id, exchange_order_id, status, filled_qty, avg_price, note,
                committed_qty, entry_commit_criterion, late_tail_qty_total,
                created_at, updated_at, extras
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                $8, $9,
                'entry', NULL, 'immediate', NULL, false,
                false, 'IOC', $10, NULL,
                $11, NULL, 'planned', 0, NULL, $12,
                NULL, NULL, NULL,
                now(), now(), jsonb_build_object('leverage', $13::text)
            )
            ON CONFLICT (order_link_id) DO NOTHING
            """,
            position_uid,
            strategy_id,
            strategy_type,
            symbol,
            direction,
            side,
            order_mode,
            source_stream_id,
            orders_stream_id,
            str(qty),
            order_link_id,
            f"entry planned qty={qty}",
            str(leverage),
        )
        log.info("📝 entry planned: uid=%s sid=%s %s qty=%s link=%s",
                 position_uid, strategy_id, symbol, qty, order_link_id)


# 🔸 Обновление карточки entry фактами fill + commit
async def _update_entry_filled_and_commit(
    *,
    position_uid: str,
    order_link_id: str,
    filled_qty: Decimal,
    avg_price: Decimal,
    commit_criterion: str,
    late_tail_delta: Optional[Decimal],
):
    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE trader_position_orders
            SET status = 'filled',
                filled_qty = $3,
                avg_price = $4,
                committed_qty = $3,
                entry_commit_criterion = $5,
                late_tail_qty_total = COALESCE(late_tail_qty_total, 0) + COALESCE($6, 0),
                updated_at = now(),
                note = 'entry filled & committed'
            WHERE position_uid = $1
              AND order_link_id = $2
              AND kind = 'entry'
            """,
            position_uid,
            order_link_id,
            str(filled_qty),
            str(avg_price),
            commit_criterion,
            str(late_tail_delta) if late_tail_delta is not None else None,
        )
        log.info("✅ entry filled & committed: uid=%s qty=%s @ %s", position_uid, filled_qty, avg_price)

# 🔸 Построение карты TP/SL (percent-only) и запись в БД (в dry-run — без реального размещения на бирже)
async def _build_tp_sl_cards_after_entry(
    *,
    position_uid: str,
    strategy_id: int,
    strategy_type: str,
    symbol: str,
    direction: str,
    filled_qty: Decimal,
    entry_price: Decimal,
    order_mode: str,
    source_stream_id: str,
    base_link: str,
):
    # получить политику стратегии из кэша
    policy = config.strategy_policies.get(strategy_id, {})
    tp_levels: List[dict] = policy.get("tp_levels", [])
    initial_sl = policy.get("initial_sl")  # {'mode': 'percent', 'value': Decimal} | None

    # параметры тикера
    t_rules = await _fetch_ticker_rules(symbol)
    step_qty = t_rules["step_qty"]
    min_qty = t_rules["min_qty"]
    step_price = t_rules["step_price"]

    # распределяем qty по TP-уровням (percent, volume_percent)
    placed_tp = 0
    level_num = 0
    for lvl in tp_levels:
        if (lvl.get("tp_type") or "").lower() != "percent":
            continue  # ATR/другое не используем
        level_num += 1
        vol_pct = _as_decimal(lvl.get("volume_percent")) or Decimal("0")
        if vol_pct <= 0:
            continue

        # объём на уровень
        q_raw = (filled_qty * (vol_pct / Decimal("100")))
        q_plan = _quant_down(q_raw, step_qty)
        if q_plan is None or q_plan <= 0 or q_plan < min_qty:
            continue

        # цена уровня
        p_pct = _as_decimal(lvl.get("tp_value")) or Decimal("0")
        price = _price_percent(entry=entry_price, pct=p_pct, direction=direction, is_tp=True)
        p_plan = _quant_down(price, step_price)

        # orderLinkId для TP
        link = _suffix_link(base_link, f"t{level_num}")

        # запись в БД (как «поставленный» локально; на live — здесь же будет реальная отправка)
        await _insert_tp_card(
            position_uid=position_uid,
            strategy_id=strategy_id,
            strategy_type=strategy_type,
            symbol=symbol,
            direction=direction,
            order_mode=order_mode,
            source_stream_id=source_stream_id,
            kind="tp",
            level=level_num,
            qty=q_plan,
            price=p_plan,
            order_link_id=link,
            is_active=True,  # «активный» в нашей карте
            status="sent" if order_mode == "dry_run" else "planned",  # dry_run считаем «локально отправленным»
            note="tp planned (percent)",
        )
        placed_tp += 1

        # SL-после-TP (переносы) — только карточки, без размещения
        sl_mode = (lvl.get("sl_mode") or "").lower()
        sl_val = _as_decimal(lvl.get("sl_value"))
        if sl_mode in ("entry", "percent"):
            sl_price = entry_price if sl_mode == "entry" else _price_percent(
                entry_price, sl_val or Decimal("0"), direction, is_tp=False
            )
            sl_price = _quant_down(sl_price, step_price)
            await _insert_sl_card(
                position_uid=position_uid,
                strategy_id=strategy_id,
                strategy_type=strategy_type,
                symbol=symbol,
                direction=direction,
                order_mode=order_mode,
                source_stream_id=source_stream_id,
                kind="sl",
                level=level_num,
                activation="on_tp",
                activation_tp_level=level_num,
                qty=None,  # будет рассчитываться по остаткам при активации
                price=sl_price if sl_price and sl_price > 0 else None,
                order_link_id=_suffix_link(base_link, f"sl{level_num}"),
                is_active=False,
                status="planned_offchain",
                note="sl replacement planned (on TP)",
            )

    # стартовый SL (если включён)
    if initial_sl and (initial_sl.get("mode") or "").lower() == "percent":
        slp = _as_decimal(initial_sl.get("value")) or Decimal("0")
        if slp > 0:
            sl_price0 = _price_percent(entry=entry_price, pct=slp, direction=direction, is_tp=False)
            sl_price0 = _quant_down(sl_price0, step_price)
            await _insert_sl_card(
                position_uid=position_uid,
                strategy_id=strategy_id,
                strategy_type=strategy_type,
                symbol=symbol,
                direction=direction,
                order_mode=order_mode,
                source_stream_id=source_stream_id,
                kind="sl",
                level=0,
                activation="immediate",
                activation_tp_level=None,
                qty=filled_qty,  # на всю позицию
                price=sl_price0,
                order_link_id=_suffix_link(base_link, "sl0"),
                is_active=True,
                status="sent" if order_mode == "dry_run" else "planned",
                note="initial SL planned",
            )

    # reverse: TP signal (виртуальный) + sl_protect_entry (виртуальный)
    if (strategy_type or "").lower() == "reverse":
        await _insert_virtual_tp_signal(
            position_uid=position_uid,
            strategy_id=strategy_id,
            strategy_type=strategy_type,
            symbol=symbol,
            direction=direction,
            order_mode=order_mode,
            source_stream_id=source_stream_id,
            order_link_id=_suffix_link(base_link, "tsig"),
            note="tp_signal (virtual, no price)",
        )

        await _insert_sl_protect_entry(
            position_uid=position_uid,
            strategy_id=strategy_id,
            strategy_type=strategy_type,
            symbol=symbol,
            direction=direction,
            order_mode=order_mode,
            source_stream_id=source_stream_id,
            order_link_id=_suffix_link(base_link, "slprot"),
            note="sl_protect_entry (virtual)",
        )

    log.info("🧩 TP/SL карта создана: sid=%s %s placed_tp=%s", strategy_id, symbol, placed_tp)

# 🔸 Запись TP карточки
async def _insert_tp_card(
    *,
    position_uid: str,
    strategy_id: int,
    strategy_type: str,
    symbol: str,
    direction: str,
    order_mode: str,
    source_stream_id: str,
    kind: str,
    level: int,
    qty: Decimal,
    price: Decimal,
    order_link_id: str,
    is_active: bool,
    status: str,
    note: str,
):
    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO trader_position_orders (
                position_uid, strategy_id, strategy_type, symbol, direction, side, order_mode,
                source_stream_id,
                kind, level, activation, activation_tp_level, is_active,
                reduce_only, tif, qty, price,
                order_link_id, status, note, created_at, updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, CASE WHEN $5='long' THEN 'Sell' ELSE 'Buy' END, $6,
                $7,
                $8, $9, 'immediate', NULL, $10,
                true, 'GTC', $11, $12,
                $13, $14, $15, now(), now()
            )
            ON CONFLICT (order_link_id) DO NOTHING
            """,
            position_uid, strategy_id, strategy_type, symbol, direction, order_mode,
            source_stream_id,
            kind, level, is_active,
            str(qty), str(price),
            order_link_id, status, note,
        )
        log.info("📝 TP planned: uid=%s sid=%s %s L#%s qty=%s price=%s",
                 position_uid, strategy_id, symbol, level, qty, price)

# 🔸 Запись SL карточки (immediate или on_tp), без реального размещения в dry-run
async def _insert_sl_card(
    *,
    position_uid: str,
    strategy_id: int,
    strategy_type: str,
    symbol: str,
    direction: str,
    order_mode: str,
    source_stream_id: str,
    kind: str,
    level: int,
    activation: str,
    activation_tp_level: Optional[int],
    qty: Optional[Decimal],
    price: Optional[Decimal],
    order_link_id: str,
    is_active: bool,
    status: str,
    note: str,
):
    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO trader_position_orders (
                position_uid, strategy_id, strategy_type, symbol, direction, side, order_mode,
                source_stream_id,
                kind, level, activation, activation_tp_level, is_active,
                reduce_only, tif, qty, price,
                order_link_id, status, note, created_at, updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, CASE WHEN $5='long' THEN 'Sell' ELSE 'Buy' END, $6,
                $7,
                $8, $9, $10, $11, $12,
                true, 'GTC', $13, $14,
                $15, $16, $17, now(), now()
            )
            ON CONFLICT (order_link_id) DO NOTHING
            """,
            position_uid, strategy_id, strategy_type, symbol, direction, order_mode,
            source_stream_id,
            kind, level, activation, activation_tp_level, is_active,
            str(qty) if qty is not None else "0", str(price) if price is not None else None,
            order_link_id, status, note,
        )
        log.info("📝 SL planned: uid=%s sid=%s %s mode=%s L#%s price=%s",
                 position_uid, strategy_id, symbol, activation, level, price)


# 🔸 Виртуальный TP signal (никогда не уходит на биржу)
async def _insert_virtual_tp_signal(
    *,
    position_uid: str,
    strategy_id: int,
    strategy_type: str,
    symbol: str,
    direction: str,
    order_mode: str,
    source_stream_id: str,
    order_link_id: str,
    note: str,
):
    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO trader_position_orders (
                position_uid, strategy_id, strategy_type, symbol, direction, side, order_mode,
                source_stream_id,
                kind, level, activation, activation_tp_level, is_active,
                reduce_only, tif, qty, price,
                order_link_id, status, note, created_at, updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, CASE WHEN $5='long' THEN 'Buy' ELSE 'Sell' END, $6,
                $7,
                'tp_signal', NULL, 'immediate', NULL, false,
                true, NULL, 0, NULL,
                $8, 'virtual', $9, now(), now()
            )
            ON CONFLICT (order_link_id) DO NOTHING
            """,
            position_uid, strategy_id, strategy_type, symbol, direction, order_mode,
            source_stream_id,
            order_link_id, note,
        )
        log.info("📝 TP signal (virtual): uid=%s sid=%s %s", position_uid, strategy_id, symbol)


# 🔸 Виртуальная карточка sl_protect_entry (ранний перенос SL на entry до TP)
async def _insert_sl_protect_entry(
    *,
    position_uid: str,
    strategy_id: int,
    strategy_type: str,
    symbol: str,
    direction: str,
    order_mode: str,
    source_stream_id: str,
    order_link_id: str,
    note: str,
):
    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO trader_position_orders (
                position_uid, strategy_id, strategy_type, symbol, direction, side, order_mode,
                source_stream_id,
                kind, level, activation, activation_tp_level, is_active,
                reduce_only, tif, qty, price,
                order_link_id, status, note, created_at, updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, CASE WHEN $5='long' THEN 'Sell' ELSE 'Buy' END, $6,
                $7,
                'sl_protect_entry', NULL, 'on_protect', NULL, false,
                true, NULL, 0, NULL,
                $8, 'planned_offchain', $9, now(), now()
            )
            ON CONFLICT (order_link_id) DO NOTHING
            """,
            position_uid, strategy_id, strategy_type, symbol, direction, order_mode,
            source_stream_id,
            order_link_id, note,
        )
        log.info("📝 SL protect-entry (virtual): uid=%s sid=%s %s", position_uid, strategy_id, symbol)


# 🔸 Обновления журналов (необязательная косметика)
async def _touch_journals_after_entry(*, source_stream_id: str, note: str, processing_status: str):
    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE trader_positions_log
            SET status = 'processing',
                updated_at = now(),
                note = COALESCE(note,'') || CASE WHEN COALESCE(note,'')='' THEN '' ELSE '; ' END || $2
            WHERE source_stream_id = $1
            """,
            source_stream_id,
            note,
        )
        await conn.execute(
            """
            UPDATE trader_signals
            SET processing_status = $2,
                processed_at = now(),
                processing_note = $3
            WHERE stream_id = $1
            """,
            source_stream_id,
            processing_status,
            note,
        )
        log.info("🧾 journals updated: stream_id=%s → %s", source_stream_id, processing_status)


# 🔸 Pre-flight для символа (live): плечо/режимы — с кэшированием, чтобы не дёргать лишний раз
async def _preflight_symbol_settings(*, symbol: str, leverage: Decimal):
    # кэш в Redis: bybit:preflight:linear:{symbol} = json {leverage, margin_mode, position_mode}
    key = f"bybit:preflight:linear:{symbol}"
    try:
        cached = await infra.redis_client.get(key)
        if cached:
            # условия достаточности: если тот же левередж и зафиксированные режимы — пропуск
            return
    except Exception:
        pass

    # здесь будет реальный вызов в live (set-leverage / switch-isolated / position-mode),
    # сейчас — просто лог и отметка в кэше
    await infra.redis_client.set(key, json.dumps({
        "leverage": str(leverage),
        "margin_mode": MARGIN_MODE,
        "position_mode": POSITION_MODE,
    }), ex=12 * 60 * 60)
    log.info("🛫 preflight cached: %s leverage=%s margin=%s posmode=%s", symbol, leverage, MARGIN_MODE, POSITION_MODE)


# 🔸 Аудит-событие
async def _publish_audit(event: str, data: dict):
    payload = {"event": event, **(data or {})}
    sid = await infra.redis_client.xadd(AUDIT_STREAM, {"data": json.dumps(payload)})
    log.info("📜 audit %s → %s: %s", event, AUDIT_STREAM, payload)
    return sid


# 🔸 Получение last price (Bybit) для категории linear
async def _get_last_price_linear(symbol: str) -> Optional[Decimal]:
    url = f"{BASE_URL}/v5/market/tickers?category={CATEGORY}&symbol={symbol}"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
            lst = (data.get("result") or {}).get("list") or []
            head = lst[0] if lst else {}
            lp = head.get("lastPrice")
            return _as_decimal(lp)
    except Exception:
        log.exception("❌ Ошибка получения last price для %s", symbol)
        return None


# 🔸 Фолбэк цена из Redis (bb:price:{symbol})
async def _get_price_from_redis(symbol: str) -> Optional[Decimal]:
    try:
        v = await infra.redis_client.get(f"bb:price:{symbol}")
        return _as_decimal(v)
    except Exception:
        return None


# 🔸 Получить параметры тикера из БД
async def _fetch_ticker_rules(symbol: str) -> dict:
    row = await infra.pg_pool.fetchrow(
        """
        SELECT COALESCE(precision_price,0) AS pprice,
               COALESCE(precision_qty,0)   AS pqty,
               COALESCE(min_qty,0)         AS min_qty,
               COALESCE(ticksize,0)        AS ticksize
        FROM tickers_bb
        WHERE symbol = $1
        """,
        symbol,
    )
    pprice = int(row["pprice"]) if row else 0
    pqty = int(row["pqty"]) if row else 0
    min_qty = _as_decimal(row["min_qty"]) if row else Decimal("0")
    ticksize = _as_decimal(row["ticksize"]) if row else Decimal("0")

    step_qty = Decimal("1").scaleb(-pqty) if pqty > 0 else Decimal("1")
    step_price = ticksize if (ticksize and ticksize > 0) else (Decimal("1").scaleb(-pprice) if pprice > 0 else Decimal("0.00000001"))

    return {"step_qty": step_qty, "min_qty": min_qty, "step_price": step_price}


# 🔸 Квантование вниз к шагу
def _quant_down(value: Decimal, step: Decimal) -> Optional[Decimal]:
    try:
        if value is None or step is None or step <= 0:
            return None
        return (value / step).to_integral_value(rounding=ROUND_DOWN) * step
    except Exception:
        return None


# 🔸 Цена по проценту (без ATR)
def _price_percent(entry: Decimal, pct: Decimal, direction: str, is_tp: bool) -> Decimal:
    # для TP: long ↑, short ↓; для SL: long ↓, short ↑
    sgn = Decimal("1") if (is_tp and direction == "long") or ((not is_tp) and direction == "short") else Decimal("-1")
    return entry * (Decimal("1") + (sgn * (pct / Decimal("100"))))


# 🔸 Формирование короткого order_link_id с суффиксом (<=36)
def _suffix_link(base: str, suffix: str) -> str:
    core = f"{base}-{suffix}"
    if len(core) <= 36:
        return core
    h = hashlib.sha1(core.encode("utf-8")).hexdigest()[:36 - 4]  # учтём 'tv4-' ниже не нужно, base уже с 'tv4-'
    return h  # уже ограничен


# 🔸 Распределённый замок (SET NX EX)
async def _acquire_dist_lock(key: str, value: str, ttl: int) -> bool:
    try:
        ok = await infra.redis_client.set(key, value, ex=ttl, nx=True)
        return bool(ok)
    except Exception:
        log.exception("❌ Ошибка acquire lock %s", key)
        return False


# 🔸 Освобождение замка по владельцу (Lua check-and-del)
async def _release_dist_lock(key: str, value: str):
    if not key:
        return
    try:
        lua = """
        if redis.call('get', KEYS[1]) == ARGV[1] then
            return redis.call('del', KEYS[1])
        else
            return 0
        end
        """
        await infra.redis_client.eval(lua, 1, key, value)
    except Exception:
        log.debug("lock release fallback (key=%s)", key)


# 🔸 Утилиты
def _as_decimal(v) -> Optional[Decimal]:
    try:
        if v is None:
            return None
        if isinstance(v, Decimal):
            return v
        return Decimal(str(v))
    except Exception:
        return None