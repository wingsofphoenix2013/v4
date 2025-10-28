# bybit_processor.py — воркер исполнения: читает positions_bybit_orders (только новые),
# dry_run: entry по last price (100% fill), строит карту TP/SL (percent-only) и пишет в БД

# 🔸 Импорты
import os
import json
import asyncio
import logging
import hashlib
import time
import hmac
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Tuple, Optional, Any, List

import httpx

from trader_infra import infra
from trader_config import config  # политики стратегий из in-memory кэша

# 🔸 Логгер
log = logging.getLogger("BYBIT_PROCESSOR")

# 🔸 Константы стримов/CG
ORDERS_STREAM = "positions_bybit_orders"
BYBIT_PROC_CG = "bybit_processor_cg"
BYBIT_PROC_CONSUMER = os.getenv("BYBIT_PROC_CONSUMER", "bybit-proc-1")
AUDIT_STREAM = "positions_bybit_audit"

# 🔸 Параметры ожидания entry в live
ENTRY_POLL_INTERVAL_SEC = 1.0
ENTRY_POLL_TIMEOUT_SEC = 10.0

# 🔸 TIF для live-ордеров
TP_TIF = "GTC"
SL_TIF = "GTC"

# 🔸 Параллелизм и замки
MAX_PARALLEL_TASKS = int(os.getenv("BYBIT_PROC_MAX_TASKS", "200"))
LOCK_TTL_SEC = int(os.getenv("BYBIT_PROC_LOCK_TTL", "30"))

# 🔸 BYBIT ENV (часть нужна позже для live)
API_KEY = os.getenv("BYBIT_API_KEY", "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com")
RECV_WINDOW = os.getenv("BYBIT_RECV_WINDOW", "5000")
ACCOUNT_TYPE = os.getenv("BYBIT_ACCOUNT_TYPE", "UNIFIED")
CATEGORY = "linear"
MARGIN_MODE = os.getenv("BYBIT_MARGIN_MODE", "isolated")
POSITION_MODE = os.getenv("BYBIT_POSITION_MODE", "oneway")

# 🔸 Режим исполнения
TRADER_ORDER_MODE = os.getenv("TRADER_ORDER_MODE", "dry_run")  # dry_run | live

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
        try:
            order_link_id   = payload.get("order_link_id")
            position_uid    = payload.get("position_uid")
            sid             = int(payload.get("strategy_id"))
            strategy_type   = (payload.get("strategy_type") or "").lower()  # plain|reverse
            symbol          = payload.get("symbol")
            direction       = payload.get("direction")  # long|short
            side            = payload.get("side")       # Buy|Sell
            leverage        = _as_decimal(payload.get("leverage")) or Decimal("0")
            qty             = _as_decimal(payload.get("qty")) or Decimal("0")
            size_mode       = payload.get("size_mode")  # 'pct_of_virtual'
            size_pct        = _as_decimal(payload.get("size_pct")) or Decimal("0")
            margin_plan     = _as_decimal(payload.get("margin_plan")) or Decimal("0")
            order_mode      = payload.get("order_mode", TRADER_ORDER_MODE)
            source_stream_id= payload.get("source_stream_id")
            ts              = payload.get("ts")
            ts_ms           = payload.get("ts_ms")
        except Exception:
            log.exception("❌ Ошибка парсинга полей payload (id=%s) — ACK", entry_id)
            try:
                await redis.xack(ORDERS_STREAM, BYBIT_PROC_CG, entry_id)
            except Exception:
                pass
            return

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
                    strategy_type=strategy_type,
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

                # ─────────────────────────────────────────────────────────────
                # DRY-RUN ВЕТКА
                # ─────────────────────────────────────────────────────────────
                if order_mode == "dry_run":
                    # last price → 100% fill (бумажный)
                    last_price = await _get_last_price_linear(symbol)
                    if last_price is None or last_price <= 0:
                        last_price = await _get_price_from_redis(symbol)
                    if last_price is None or last_price <= 0:
                        last_price = Decimal("1")

                    filled_qty = qty
                    avg_price  = last_price

                    # зафиксировать fill + commit
                    await _update_entry_filled_and_commit(
                        position_uid=position_uid,
                        order_link_id=_suffix_link(order_link_id, "e"),
                        filled_qty=filled_qty,
                        avg_price=avg_price,
                        commit_criterion="dry_run",
                        late_tail_delta=None,
                    )

                    # аудит
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

                    # построить карту TP/SL (в БД) — percent-only
                    await _build_tp_sl_cards_after_entry(
                        position_uid=position_uid,
                        strategy_id=sid,
                        strategy_type=strategy_type,
                        symbol=symbol,
                        direction=direction,
                        filled_qty=filled_qty,
                        entry_price=avg_price,
                        order_mode=order_mode,
                        source_stream_id=source_stream_id,
                        base_link=order_link_id,
                    )

                    # журналы
                    await _touch_journals_after_entry(
                        source_stream_id=source_stream_id,
                        note=f"entry dry-run filled @ {avg_price}",
                        processing_status="processing",
                    )

                    # ACK и выход
                    await infra.redis_client.xack(ORDERS_STREAM, BYBIT_PROC_CG, entry_id)
                    log.info(
                        "✅ ENTRY dry-run filled & TP/SL planned (sid=%s %s %s qty=%s @ %s) [id=%s]",
                        sid, symbol, direction, filled_qty, avg_price, entry_id
                    )
                    return

                # ─────────────────────────────────────────────────────────────
                # LIVE ВЕТКА: preflight → market IOC → короткий watcher → commit
                # ─────────────────────────────────────────────────────────────
                await _preflight_symbol_settings(symbol=symbol, leverage=leverage)

                # создать market IOC
                link_e = _suffix_link(order_link_id, "e")
                create_resp = await _create_market_order(symbol, side, qty, link_e)
                log.info("🟢 LIVE entry create sent: link=%s resp=%s", link_e, (create_resp or {}).get("retMsg"))

                # короткий опрос состояния (до 10с)
                filled_qty = Decimal("0")
                avg_price  = None
                t0 = time.time()
                while time.time() - t0 < ENTRY_POLL_TIMEOUT_SEC:
                    await asyncio.sleep(ENTRY_POLL_INTERVAL_SEC)
                    state = await _get_order_realtime_by_link(link_e)
                    lst = (state.get("result") or {}).get("list") or []
                    head = lst[0] if lst else {}
                    fq  = _as_decimal(head.get("cumExecQty")) or Decimal("0")
                    ap  = _as_decimal(head.get("avgPrice"))
                    filled_qty = fq
                    avg_price  = ap
                    # если есть хоть что-то — выходим (минимальный watcher)
                    if filled_qty > 0:
                        break

                if filled_qty > 0 and avg_price:
                    # зафиксировать commit входа
                    await _update_entry_filled_and_commit(
                        position_uid=position_uid,
                        order_link_id=link_e,
                        filled_qty=filled_qty,
                        avg_price=avg_price,
                        commit_criterion="live_minimal",
                        late_tail_delta=None,
                    )
                    await _touch_journals_after_entry(
                        source_stream_id=source_stream_id,
                        note=f"entry live filled (minimal) qty={filled_qty} @ {avg_price}",
                        processing_status="processing",
                        ext_status="open",
                    )
                    log.info("✅ LIVE entry filled (minimal): %s qty=%s @ %s", link_e, filled_qty, avg_price)

                    # построить карту TP/SL в БД (как в dry-run, но с order_mode='live')
                    await _build_tp_sl_cards_after_entry(
                        position_uid=position_uid,
                        strategy_id=sid,
                        strategy_type=strategy_type,
                        symbol=symbol,
                        direction=direction,
                        filled_qty=filled_qty,
                        entry_price=avg_price,
                        order_mode="live",
                        source_stream_id=source_stream_id,
                        base_link=order_link_id,
                    )

                    # выставить «немедленные» TP/SL на биржу по карточкам (tp price + sl level=0)
                    await _place_immediate_orders_for_position(position_uid, symbol, direction)

                else:
                    # ничего не успело исполниться за окно — считаем 'sent'
                    async with infra.pg_pool.acquire() as conn:
                        await conn.execute(
                            """
                            UPDATE trader_position_orders
                            SET status = 'sent', updated_at = now(),
                                note = COALESCE(note,'') || CASE WHEN COALESCE(note,'')='' THEN '' ELSE '; ' END || 'live entry sent (no fill yet)'
                            WHERE position_uid = $1 AND order_link_id = $2 AND kind = 'entry'
                            """,
                            position_uid, link_e,
                        )
                    log.info("🟡 LIVE entry sent (no fill yet): %s", link_e)

                # ACK — завершаем обработку записи (в live TP/SL уже поставлены/или пока нет fill)
                await infra.redis_client.xack(ORDERS_STREAM, BYBIT_PROC_CG, entry_id)
                return

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
                'entry', NULL, 'immediate', NULL, true,
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
    strategy_type: str,  # 'plain' | 'reverse'
    symbol: str,
    direction: str,      # 'long' | 'short'
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

    # общий доступный объём в кратных шага количества
    q_total = _quant_down(filled_qty, step_qty) or Decimal("0")
    if q_total <= 0:
        log.info("ℹ️ q_total=0 — TP/SL карта не строится (uid=%s)", position_uid)
        return

    # подготовить массив ценовых TP (percent) в исходном порядке
    percent_levels: List[dict] = []
    for lvl in tp_levels:
        if (lvl.get("tp_type") or "").lower() != "percent":
            continue
        vol_pct = _as_decimal(lvl.get("volume_percent")) or Decimal("0")
        if vol_pct <= 0:
            continue
        percent_levels.append(lvl)

    # распределение TP: для plain — остаток в последний ценовой TP; для reverse — остаток в tp_signal
    assign_residual_to = "last_price_tp" if strategy_type == "plain" else "tp_signal"
    tp_qtys = _allocate_tp_quantities(q_total, step_qty, percent_levels, assign_residual_to)

    placed_tp = 0
    cum_qty = Decimal("0")
    level_num = 0

    # цикл по ценовым TP
    for i, lvl in enumerate(percent_levels):
        level_num += 1
        q_plan = tp_qtys[i] if i < len(tp_qtys) else Decimal("0")
        if q_plan <= 0:
            continue  # не создаём «нулевые» уровни

        # цена уровня
        p_pct = _as_decimal(lvl.get("tp_value")) or Decimal("0")
        price = _price_percent(entry=entry_price, pct=p_pct, direction=direction, is_tp=True)
        p_plan = _quant_down(price, step_price)

        # orderLinkId для TP
        link = _suffix_link(base_link, f"t{level_num}")

        # запись TP (side противоположная направлению позиции, reduce_only=true)
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
            is_active=True,  # релевантен
            status="sent" if order_mode == "dry_run" else "planned",
            note="tp planned (percent)",
        )
        placed_tp += 1
        cum_qty += q_plan

        # SL-после-TP (переносы) — только карточки, без размещения
        sl_mode = (lvl.get("sl_mode") or "").lower()
        sl_val = _as_decimal(lvl.get("sl_value"))
        if sl_mode in ("entry", "percent"):
            # цена SL-замены
            sl_price = entry_price if sl_mode == "entry" else _price_percent(
                entry_price, sl_val or Decimal("0"), direction, is_tp=False
            )
            sl_price = _quant_down(sl_price, step_price)

            # объём SL после срабатывания TP-k — остаток после кумулятивных TP
            qty_sl_after_k = _quant_down(q_total - cum_qty, step_qty) or Decimal("0")
            if qty_sl_after_k > 0:
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
                    qty=qty_sl_after_k,
                    price=sl_price if sl_price and sl_price > 0 else None,
                    order_link_id=_suffix_link(base_link, f"sl{level_num}"),
                    is_active=True,  # релевантен
                    status="planned_offchain",
                    note="sl replacement planned (on TP)",
                )

    # стартовый SL (всегда на весь q_total)
    slp_ok = initial_sl and (initial_sl.get("mode") or "").lower() == "percent"
    if slp_ok:
        slp = _as_decimal(initial_sl.get("value")) or Decimal("0")
        if slp > 0 and q_total > 0:
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
                qty=q_total,          # вся позиция
                price=sl_price0,
                order_link_id=_suffix_link(base_link, "sl0"),
                is_active=True,
                status="sent" if order_mode == "dry_run" else "planned",
                note="initial SL planned",
            )

    # reverse: TP signal (виртуальный) + sl_protect_entry (виртуальный)
    if strategy_type == "reverse":
        # последний числовой TP-уровень (если есть)
        max_level = len(percent_levels)

        # остаток после всей лестницы TP (идёт в tp_signal)
        qty_after_all_tp = _quant_down(q_total - (sum(tp_qtys) if tp_qtys else Decimal("0")), step_qty) or Decimal("0")
        if qty_after_all_tp > 0 and max_level > 0:
            await _insert_virtual_tp_signal(
                position_uid=position_uid,
                strategy_id=strategy_id,
                strategy_type=strategy_type,
                symbol=symbol,
                direction=direction,
                order_mode=order_mode,
                source_stream_id=source_stream_id,
                order_link_id=_suffix_link(base_link, "tsig"),
                activation_tp_level=max_level,
                qty=qty_after_all_tp,
                note="tp_signal (virtual, on_tp)",
            )

        # карточка sl_protect_entry (ранний перенос SL на entry ДО TP) — qty = вся позиция
        await _insert_sl_protect_entry(
            position_uid=position_uid,
            strategy_id=strategy_id,
            strategy_type=strategy_type,
            symbol=symbol,
            direction=direction,
            order_mode=order_mode,
            source_stream_id=source_stream_id,
            order_link_id=_suffix_link(base_link, "slprot"),
            qty=q_total,
            note="sl_protect_entry (virtual)",
        )

    log.info("🧩 TP/SL карта создана: sid=%s %s placed_tp=%s", strategy_id, symbol, placed_tp)


# 🔸 Распределение количества по TP
# режим:
#   assign_residual_to='last_price_tp'  → для plain: первые n−1 по процентам, n-й = остаток
#   assign_residual_to='tp_signal'      → для reverse: все n по процентам, остаток уходит в tp_signal
def _allocate_tp_quantities(
    q_total: Decimal,
    step_qty: Decimal,
    percent_levels: List[dict],
    assign_residual_to: str,
) -> List[Decimal]:
    # условия достаточности
    if q_total is None or q_total <= 0 or step_qty is None or step_qty <= 0:
        return []
    n = len(percent_levels)
    if n == 0:
        return []

    qtys: List[Decimal] = []
    sum_prev = Decimal("0")

    if assign_residual_to == "last_price_tp":
        # первые n−1 по процентам
        for i in range(n - 1):
            vol_pct = _as_decimal(percent_levels[i].get("volume_percent")) or Decimal("0")
            q_i = _quant_down(q_total * (vol_pct / Decimal("100")), step_qty) or Decimal("0")
            qtys.append(q_i)
            sum_prev += q_i
        # последний — остаток
        q_last = _quant_down(q_total - sum_prev, step_qty) or Decimal("0")
        qtys.append(q_last)
    else:
        # все n по процентам; остаток пойдёт в tp_signal (считается снаружи)
        for i in range(n):
            vol_pct = _as_decimal(percent_levels[i].get("volume_percent")) or Decimal("0")
            q_i = _quant_down(q_total * (vol_pct / Decimal("100")), step_qty) or Decimal("0")
            qtys.append(q_i)

    return qtys


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


# 🔸 Запись SL карточки (immediate или on_tp)
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
    qty: Decimal,
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
            str(qty), str(price) if price is not None else None,
            order_link_id, status, note,
        )
        log.info("📝 SL planned: uid=%s sid=%s %s mode=%s L#%s qty=%s price=%s",
                 position_uid, strategy_id, symbol, activation, level, qty, price)

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
    activation_tp_level: Optional[int],
    qty: Decimal,
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
                $1, $2, $3, $4, $5,
                CASE WHEN $5='long' THEN 'Sell' ELSE 'Buy' END, $6,
                $7,
                'tp_signal', NULL, 'on_tp', $8, true,
                true, NULL, $9, NULL,
                $10, 'virtual', $11, now(), now()
            )
            ON CONFLICT (order_link_id) DO NOTHING
            """,
            position_uid, strategy_id, strategy_type, symbol, direction, order_mode,
            source_stream_id,
            activation_tp_level, str(qty),
            order_link_id, note,
        )
        log.info("📝 TP signal (virtual): uid=%s sid=%s %s qty=%s level=%s",
                 position_uid, strategy_id, symbol, qty, activation_tp_level)

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
    qty: Decimal,
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
                $1, $2, $3, $4, $5,
                CASE WHEN $5='long' THEN 'Sell' ELSE 'Buy' END, $6,
                $7,
                'sl_protect_entry', NULL, 'on_protect', NULL, true,
                true, 'GTC', $8, NULL,
                $9, 'planned_offchain', $10, now(), now()
            )
            ON CONFLICT (order_link_id) DO NOTHING
            """,
            position_uid, strategy_id, strategy_type, symbol, direction, order_mode,
            source_stream_id,
            str(qty),
            order_link_id, note,
        )
        log.info("📝 SL protect-entry (virtual): uid=%s sid=%s %s qty=%s",
                 position_uid, strategy_id, symbol, qty)

# 🔸 Обновления журналов (косметика)
async def _touch_journals_after_entry(
    *,
    source_stream_id: str,
    note: str,
    processing_status: str,
    ext_status: Optional[str] = None,
):
    async with infra.pg_pool.acquire() as conn:
        if ext_status:
            await conn.execute(
                """
                UPDATE trader_positions_log
                SET status = 'processing',
                    ext_status = $2,
                    updated_at = now(),
                    note = COALESCE(note,'') || CASE WHEN COALESCE(note,'')='' THEN '' ELSE '; ' END || $3
                WHERE source_stream_id = $1
                """,
                source_stream_id, ext_status, note,
            )
        else:
            await conn.execute(
                """
                UPDATE trader_positions_log
                SET status = 'processing',
                    updated_at = now(),
                    note = COALESCE(note,'') || CASE WHEN COALESCE(note,'')='' THEN '' ELSE '; ' END || $2
                WHERE source_stream_id = $1
                """,
                source_stream_id, note,
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
        log.info("🧾 journals updated: stream_id=%s → %s%s",
                 source_stream_id,
                 processing_status,
                 f", ext_status={ext_status}" if ext_status else "")

# 🔸 Pre-flight для символа (live): плечо/режимы — с кэшированием, чтобы не дёргать лишний раз
async def _preflight_symbol_settings(*, symbol: str, leverage: Decimal):
    # кэш в Redis: bybit:preflight:linear:{symbol} = json {leverage, margin_mode, position_mode}
    key = f"bybit:preflight:linear:{symbol}"
    try:
        cached = await infra.redis_client.get(key)
        if cached:
            # если запись уже есть — пропускаем (дальше добавим проверки на изменение плеча)
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

    # шаги
    step_qty = Decimal("1").scaleb(-pqty) if pqty > 0 else Decimal("1")
    step_price = ticksize if (ticksize and ticksize > 0) else (Decimal("1").scaleb(-pprice) if pprice > 0 else Decimal("0.00000001"))

    return {"step_qty": step_qty, "min_qty": min_qty, "step_price": step_price}

# 🔸 Подпись приватных запросов Bybit v5
def _rest_sign(timestamp_ms: int, query_or_body: str) -> str:
    payload = f"{timestamp_ms}{API_KEY}{RECV_WINDOW}{query_or_body}"
    return hmac.new(API_SECRET.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()

def _private_headers(ts_ms: int, signed: str) -> dict:
    return {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-TIMESTAMP": str(ts_ms),
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
        "X-BAPI-SIGN": signed,
        "Content-Type": "application/json",
    }

# 🔸 Создание market IOC ордера (reduceOnly=false)
async def _create_market_order(symbol: str, side: str, qty: Decimal, order_link_id: str) -> dict:
    # квантование и проверка min_qty
    rules = await _fetch_ticker_rules(symbol)
    q = _quant_down(qty, rules["step_qty"]) or Decimal("0")
    if q <= 0 or q < (rules["min_qty"] or Decimal("0")):
        raise ValueError(f"qty below min_qty after quantization: q={q}, min={rules['min_qty']}")

    body = {
        "category": "linear",
        "symbol": symbol,
        "side": side,                       # Buy | Sell
        "orderType": "Market",
        "qty": _to_fixed_str(q),            # фиксированный вид
        "timeInForce": "IOC",
        "reduceOnly": False,
        "orderLinkId": order_link_id,
    }
    url = f"{BASE_URL}/v5/order/create"
    ts = int(time.time() * 1000)
    body_json = json.dumps(body, separators=(",", ":"))
    sign = _rest_sign(ts, body_json)
    headers = _private_headers(ts, sign)
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(url, headers=headers, content=body_json)
        r.raise_for_status()
        return r.json()

# 🔸 Создание LIMIT reduceOnly GTC (TP)
async def _create_limit_ro_order(symbol: str, side: str, qty: Decimal, price: Decimal, order_link_id: str) -> dict:
    rules = await _fetch_ticker_rules(symbol)
    q = _quant_down(qty, rules["step_qty"]) or Decimal("0")
    p = _quant_down(price, rules["step_price"]) or Decimal("0")
    if q <= 0 or q < (rules["min_qty"] or Decimal("0")) or p <= 0:
        raise ValueError(f"invalid TP qty/price: q={q} (min={rules['min_qty']}), p={p}")

    body = {
        "category": "linear",
        "symbol": symbol,
        "side": side,                           # закрывающая сторона
        "orderType": "Limit",
        "qty": _to_fixed_str(q),
        "price": _to_fixed_str(p),
        "timeInForce": TP_TIF,                  # GTC
        "reduceOnly": True,
        "orderLinkId": order_link_id,
    }
    url = f"{BASE_URL}/v5/order/create"
    ts = int(time.time() * 1000)
    body_json = json.dumps(body, separators=(",", ":"))
    sign = _rest_sign(ts, body_json)
    headers = _private_headers(ts, sign)
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(url, headers=headers, content=body_json)
        r.raise_for_status()
        return r.json()

# 🔸 Создание STOP-MARKET reduceOnly GTC (SL)
async def _create_stop_ro_order(symbol: str, side: str, qty: Decimal, trigger_price: Decimal, order_link_id: str) -> dict:
    rules = await _fetch_ticker_rules(symbol)
    q  = _quant_down(qty, rules["step_qty"]) or Decimal("0")
    tr = _quant_down(trigger_price, rules["step_price"]) or Decimal("0")
    if q <= 0 or q < (rules["min_qty"] or Decimal("0")) or tr <= 0:
        raise ValueError(f"invalid SL qty/trigger: q={q} (min={rules['min_qty']}), trigger={tr}")

    # Bybit v5: стоп-маркет через order/create с triggerPrice + stopOrderType/triggerBy
    body = {
        "category": "linear",
        "symbol": symbol,
        "side": side,                               # закрывающая сторона
        "orderType": "Market",
        "qty": _to_fixed_str(q),
        "timeInForce": SL_TIF,                      # GTC
        "reduceOnly": True,
        "triggerPrice": _to_fixed_str(tr),
        "stopOrderType": "Stop",
        "triggerBy": "LastPrice",
        "orderLinkId": order_link_id,
    }
    url = f"{BASE_URL}/v5/order/create"
    ts = int(time.time() * 1000)
    body_json = json.dumps(body, separators=(",", ":"))
    sign = _rest_sign(ts, body_json)
    headers = _private_headers(ts, sign)
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(url, headers=headers, content=body_json)
        r.raise_for_status()
        return r.json()

# 🔸 Позиционный стоп-лосс для позиции (Bybit v5 /v5/position/trading-stop)
async def _set_position_stop_loss(
    symbol: str,
    trigger_price: Decimal,
    *,
    trigger_by: str = "LastPrice",
    position_idx: int = 0
) -> dict:
    # условия достаточности
    rules = await _fetch_ticker_rules(symbol)
    step_price = rules["step_price"]
    p = _quant_down(trigger_price, step_price) if trigger_price is not None else None
    if p is None or p <= 0:
        raise ValueError("invalid SL trigger price")

    # формирование тела запроса
    body = {
        "category": CATEGORY,       # 'linear'
        "symbol": symbol,
        "positionIdx": position_idx,  # oneway → 0
        "stopLoss": _to_fixed_str(p),
        "slTriggerBy": trigger_by,    # 'LastPrice'
    }

    # подпись и отправка
    url = f"{BASE_URL}/v5/position/trading-stop"
    ts = int(time.time() * 1000)
    body_json = json.dumps(body, separators=(",", ":"))
    signed = _rest_sign(ts, body_json)
    headers = _private_headers(ts, signed)

    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(url, headers=headers, content=body_json)
        r.raise_for_status()
        return r.json()

# 🔸 Постановка live-ордеров по «немедленным» карточкам (tp/sl level=0) из БД
async def _place_immediate_orders_for_position(position_uid: str, symbol: str, direction: str):
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, kind, level, side, qty, price, order_link_id
            FROM trader_position_orders
            WHERE position_uid = $1
              AND activation = 'immediate'
              AND is_active = true
              AND status = 'planned'
              AND kind IN ('tp','sl')
            ORDER BY kind, level
            """,
            position_uid,
        )

    for r in rows:
        rid   = int(r["id"])
        kind  = r["kind"]
        level = r["level"]
        side  = r["side"]                          # уже «закрывающая» сторона
        qty   = _as_decimal(r["qty"]) or Decimal("0")
        price = _as_decimal(r["price"]) if r["price"] is not None else None
        link  = r["order_link_id"]

        try:
            if kind == "tp":
                # лимитный TP reduceOnly (как было)
                resp = await _create_limit_ro_order(symbol, side, qty, price, link)

                ret_code = (resp or {}).get("retCode", 0)
                ret_msg  = (resp or {}).get("retMsg")
                exch_id  = ((resp or {}).get("result") or {}).get("orderId")

                if ret_code == 0 and exch_id:
                    async with infra.pg_pool.acquire() as conn:
                        await conn.execute(
                            """
                            UPDATE trader_position_orders
                            SET status = 'sent',
                                exchange_order_id = COALESCE($2, exchange_order_id),
                                updated_at = now(),
                                note = COALESCE(note,'') || CASE WHEN COALESCE(note,'')='' THEN '' ELSE '; ' END || 'live placed'
                            WHERE id = $1
                            """,
                            rid, exch_id,
                        )
                    await _publish_audit(
                        event="tp_placed",
                        data={
                            "position_uid": position_uid,
                            "symbol": symbol,
                            "level": level,
                            "qty": _to_fixed_str(qty),
                            "price": _to_fixed_str(price) if price is not None else None,
                            "order_link_id": link,
                            "exchange_order_id": exch_id,
                        },
                    )
                    log.info("📤 live placed: tp L#%s link=%s exch_id=%s qty=%s price=%s",
                             level, link, exch_id, qty, price)
                else:
                    async with infra.pg_pool.acquire() as conn:
                        await conn.execute(
                            """
                            UPDATE trader_position_orders
                            SET status = 'error',
                                updated_at = now(),
                                note = COALESCE(note,'') || CASE WHEN COALESCE(note,'')='' THEN '' ELSE '; ' END ||
                                       ('live place failed: retCode=' || $2::text || ' msg=' || COALESCE($3,''))
                            WHERE id = $1
                            """,
                            rid, str(ret_code), ret_msg,
                        )
                    await _publish_audit(
                        event="tp_place_failed",
                        data={
                            "position_uid": position_uid,
                            "symbol": symbol,
                            "level": level,
                            "qty": _to_fixed_str(qty),
                            "price": _to_fixed_str(price) if price is not None else None,
                            "order_link_id": link,
                            "retCode": ret_code,
                            "retMsg": ret_msg,
                        },
                    )
                    log.info("❗ live place failed: tp L#%s link=%s ret=%s %s",
                             level, link, ret_code, ret_msg)

            else:
                # стартовый SL (level=0) — позиционный стоп через /v5/position/trading-stop
                # цена должна быть; квантируем к шагу цены на всякий случай
                rules = await _fetch_ticker_rules(symbol)
                p_plan = _quant_down(price, rules["step_price"]) if price is not None else None

                # условия достаточности
                if p_plan is None or p_plan <= 0:
                    raise ValueError("invalid SL trigger price")

                # подготовка запроса trading-stop
                # body: category=linear, positionIdx=0 (oneway), stopLoss, slTriggerBy=LastPrice
                body = {
                    "category": "linear",
                    "symbol": symbol,
                    "positionIdx": 0,
                    "stopLoss": _to_fixed_str(p_plan),
                    "slTriggerBy": "LastPrice",
                }
                url = f"{BASE_URL}/v5/position/trading-stop"
                ts = int(time.time() * 1000)
                body_json = json.dumps(body, separators=(",", ":"))
                sign = _rest_sign(ts, body_json)
                headers = _private_headers(ts, sign)

                async with httpx.AsyncClient(timeout=10) as client:
                    resp = await client.post(url, headers=headers, content=body_json)
                    resp.raise_for_status()
                    data = resp.json()

                ret_code = (data or {}).get("retCode", 0)
                ret_msg  = (data or {}).get("retMsg")

                if ret_code == 0:
                    # успех позиционного SL — помечаем карточку как отправленную (exchange_order_id отсутствует)
                    async with infra.pg_pool.acquire() as conn:
                        await conn.execute(
                            """
                            UPDATE trader_position_orders
                            SET status = 'sent',
                                updated_at = now(),
                                note = COALESCE(note,'') || CASE WHEN COALESCE(note,'')='' THEN '' ELSE '; ' END || 'live position stop set'
                            WHERE id = $1
                            """,
                            rid,
                        )
                    await _publish_audit(
                        event="sl_position_set",
                        data={
                            "position_uid": position_uid,
                            "symbol": symbol,
                            "level": level,
                            "qty": _to_fixed_str(qty),
                            "price": _to_fixed_str(p_plan),
                            "order_link_id": link,
                        },
                    )
                    log.info("📤 live position stop set: sl L#%s link=%s qty=%s price=%s",
                             level, link, qty, p_plan)
                else:
                    # ошибка установки позиционного SL
                    async with infra.pg_pool.acquire() as conn:
                        await conn.execute(
                            """
                            UPDATE trader_position_orders
                            SET status = 'error',
                                updated_at = now(),
                                note = COALESCE(note,'') || CASE WHEN COALESCE(note,'')='' THEN '' ELSE '; ' END ||
                                       ('live position stop failed: retCode=' || $2::text || ' msg=' || COALESCE($3,''))
                            WHERE id = $1
                            """,
                            rid, str(ret_code), ret_msg,
                        )
                    await _publish_audit(
                        event="sl_position_set_failed",
                        data={
                            "position_uid": position_uid,
                            "symbol": symbol,
                            "level": level,
                            "qty": _to_fixed_str(qty),
                            "price": _to_fixed_str(p_plan),
                            "order_link_id": link,
                            "retCode": ret_code,
                            "retMsg": ret_msg,
                        },
                    )
                    log.info("❗ live position stop failed: sl L#%s link=%s ret=%s %s",
                             level, link, ret_code, ret_msg)

        except ValueError as ve:
            # ниже min_qty / некорректные параметры — помечаем как офчейн/ошибка
            async with infra.pg_pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE trader_position_orders
                    SET status = 'planned_offchain',
                        updated_at = now(),
                        note = COALESCE(note,'') || CASE WHEN COALESCE(note,'')='' THEN '' ELSE '; ' END ||
                               ('skipped: ' || $2)
                    WHERE id = $1
                    """,
                    rid, str(ve),
                )
            await _publish_audit(
                event="tp_skipped_below_min_qty" if kind == "tp" else "sl_skipped_below_min_qty",
                data={
                    "position_uid": position_uid,
                    "symbol": symbol,
                    "level": level,
                    "qty": _to_fixed_str(qty),
                    "price": _to_fixed_str(price) if price is not None else None,
                    "order_link_id": link,
                    "reason": str(ve),
                },
            )
            log.info("ℹ️ skipped %s L#%s (reason: %s)", kind, level, ve)

        except Exception:
            # прочие ошибки — карточку в error и аудит
            async with infra.pg_pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE trader_position_orders
                    SET status = 'error',
                        updated_at = now(),
                        note = COALESCE(note,'') || CASE WHEN COALESCE(note,'')='' THEN '' ELSE '; ' END || 'exception on place'
                    WHERE id = $1
                    """,
                    rid,
                )
            await _publish_audit(
                event="tp_place_failed" if kind == "tp" else "sl_position_set_failed",
                data={
                    "position_uid": position_uid,
                    "symbol": symbol,
                    "level": level,
                    "qty": _to_fixed_str(qty),
                    "price": _to_fixed_str(price) if price is not None else None,
                    "order_link_id": link,
                    "reason": "exception",
                },
            )
            log.exception("❌ live place failed (exception): %s L#%s link=%s", kind, level, link)
            
# 🔸 Получить актуальное состояние ордера по orderLinkId
async def _get_order_realtime_by_link(order_link_id: str) -> dict:
    query = f"category=linear&orderLinkId={order_link_id}"
    url = f"{BASE_URL}/v5/order/realtime?{query}"
    ts = int(time.time() * 1000)
    sign = _rest_sign(ts, query)
    headers = _private_headers(ts, sign)
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(url, headers=headers)
        r.raise_for_status()
        return r.json()

# 🔸 Строка без экспоненты, с обрезкой хвостовых нулей
def _to_fixed_str(d: Decimal) -> str:
    s = format(d, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s or "0"
    
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
    # если длинно — берём sha1 и обрезаем под лимит
    h = hashlib.sha1(core.encode("utf-8")).hexdigest()[:36]
    return h


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