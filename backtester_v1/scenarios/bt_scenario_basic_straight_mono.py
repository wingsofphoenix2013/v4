# bt_scenario_basic_straight_mono.py — базовый straight-сценарий (mono) для backtester_v1

import asyncio
import logging
import uuid
import json
from datetime import datetime, timedelta, date
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Dict, Any, List, Tuple, Optional, Set

# 🔸 Кеши backtester_v1
from backtester_config import get_signal_instance, get_ticker_info

log = logging.getLogger("BT_SCENARIO_BASIC_MONO")

# 🔸 Настройки Decimal
getcontext().prec = 28

# 🔸 Константы стримов
BT_SCENARIOS_READY_STREAM = "bt:scenarios:ready"


# 🔸 Утилита: обрезка денег/метрик до 4 знаков после запятой
def _q_money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


# 🔸 Обрезка по precision_price (без глобального 0.0001)
def _quant_price(value: Decimal, precision_price: Optional[int]) -> Decimal:
    if precision_price is None:
        precision_price = 8
    try:
        p_dec = int(precision_price)
    except Exception:
        p_dec = 8
    quant = Decimal("1").scaleb(-p_dec)
    return value.quantize(quant, rounding=ROUND_DOWN)


# 🔸 Приведение цены к precision_price и ticksize
def _round_price(
    price: Decimal,
    precision_price: Optional[int],
    ticksize: Optional[Decimal],
) -> Decimal:
    # сначала обрезка по precision_price
    price = _quant_price(price, precision_price)

    # затем обрезка по ticksize, если есть
    if ticksize is not None and ticksize > Decimal("0"):
        steps = (price / ticksize).to_integral_value(rounding=ROUND_DOWN)
        price = steps * ticksize

    # после снапа к тиксайзу ещё раз приводим к precision_price
    price = _quant_price(price, precision_price)
    return price


# 🔸 Публичная точка входа: backfill для сценария basic_straight_mono по одному окну сигнала
async def run_basic_straight_mono_backfill(
    scenario: Dict[str, Any],
    signal_ctx: Dict[str, Any],
    pg,
    redis,  # оставляем для совместимости сигнатур
) -> None:
    scenario_id = scenario.get("id")
    scenario_key = scenario.get("key")
    scenario_type = scenario.get("type")
    params = scenario.get("params") or {}

    signal_id = signal_ctx.get("signal_id")
    from_time = signal_ctx.get("from_time")
    to_time = signal_ctx.get("to_time")

    # базовые параметры сценария
    try:
        direction_mode = (params["direction"]["value"] or "").strip().lower()
        deposit = Decimal(str(params["deposit"]["value"]))
        leverage = Decimal(str(params["leverage"]["value"]))
        sl_type = (params["sl_type"]["value"] or "").strip().lower()
        sl_value = Decimal(str(params["sl_value"]["value"]))
        tp_type = (params["tp_type"]["value"] or "").strip().lower()
        tp_value = Decimal(str(params["tp_value"]["value"]))
        position_limit = Decimal(str(params["position_limit"]["value"]))
    except Exception as e:
        log.error(
            f"BT_SCENARIO_BASIC_MONO: сценарий id={scenario_id} — некорректные параметры: {e}",
            exc_info=True,
        )
        return

    if direction_mode != "mono":
        log.warning(
            f"BT_SCENARIO_BASIC_MONO: сценарий id={scenario_id} ожидает direction='mono', "
            f"получено '{direction_mode}'"
        )

    if sl_type != "percent" or tp_type != "percent":
        log.error(
            f"BT_SCENARIO_BASIC_MONO: сценарий id={scenario_id} поддерживает только sl_type/tp_type='percent', "
            f"получено sl_type='{sl_type}', tp_type='{tp_type}' — сценарий не будет выполнен"
        )
        return

    signal_instance = get_signal_instance(signal_id)
    if not signal_instance:
        log.error(
            f"BT_SCENARIO_BASIC_MONO: не найден инстанс сигнала id={signal_id} в кеше, сценарий id={scenario_id}"
        )
        return

    timeframe = signal_instance.get("timeframe")
    if timeframe not in ("m5", "m15", "h1"):
        log.error(
            f"BT_SCENARIO_BASIC_MONO: сценарий id={scenario_id}, signal_id={signal_id} — "
            f"неподдерживаемый timeframe='{timeframe}'"
        )
        return

    log.debug(
        f"BT_SCENARIO_BASIC_MONO: старт обработки сценария id={scenario_id} (key={scenario_key}, type={scenario_type}) "
        f"для signal_id={signal_id}, TF={timeframe}, окно=[{from_time} .. {to_time}], "
        f"deposit={deposit}, leverage={leverage}, position_limit={position_limit}, "
        f"SL={sl_value}% TP={tp_value}%"
    )

    # грузим сигналы для данного signal_id/TF/окна, которые ещё не обрабатывались этим сценарием
    signals = await _load_signals_for_scenario(pg, scenario_id, signal_id, timeframe, from_time, to_time)
    if not signals:
        log.debug(
            f"BT_SCENARIO_BASIC_MONO: сценарий id={scenario_id}, signal_id={signal_id} — "
            f"актуальных сигналов для обработки не найдено"
        )
        # всё равно публикуем сообщение о готовности сценария, чтобы цепочка стримов была консистентной
        finished_at = datetime.utcnow()
        try:
            await redis.xadd(
                BT_SCENARIOS_READY_STREAM,
                {
                    "scenario_id": str(scenario_id),
                    "signal_id": str(signal_id),
                    "finished_at": finished_at.isoformat(),
                },
            )
            log.debug(
                f"BT_SCENARIO_BASIC_MONO: опубликовано событие готовности сценария в стрим "
                f"'{BT_SCENARIOS_READY_STREAM}' для scenario_id={scenario_id}, signal_id={signal_id}, "
                f"finished_at={finished_at}"
            )
        except Exception as e:
            log.error(
                f"BT_SCENARIO_BASIC_MONO: не удалось опубликовать событие в стрим "
                f"'{BT_SCENARIOS_READY_STREAM}' для scenario_id={scenario_id}, signal_id={signal_id}: {e}",
                exc_info=True,
            )
        return

    positions_to_insert: List[Tuple[Any, ...]] = []
    logs_to_insert: List[Tuple[Any, ...]] = []
    affected_days: Set[date] = set()

    total_signals_processed = 0
    total_positions_opened = 0
    total_skipped = 0
    total_alive = 0

    # обрабатываем long и short как две независимые вселенные с одинаковым deposit
    # и независимыми позициями в разрезе (scenario_id, signal_id, direction)
    for direction in ("long", "short"):
        # существующие исторические позиции по этому направлению и signal_id
        existing_positions = await _load_existing_positions(pg, scenario_id, signal_id, timeframe, direction)
        new_positions: List[Dict[str, Any]] = []

        # фильтруем сигналы по направлению
        dir_signals = [s for s in signals if s["direction"] == direction]
        if not dir_signals:
            continue

        log.debug(
            f"BT_SCENARIO_BASIC_MONO: сценарий id={scenario_id}, signal_id={signal_id}, direction={direction} — "
            f"для обработки сигналов={len(dir_signals)}"
        )

        # сортировка по времени сигнала
        dir_signals.sort(key=lambda s: s["open_time"])

        for s_row in dir_signals:
            total_signals_processed += 1

            symbol = s_row["symbol"]
            open_time = s_row["open_time"]
            signal_uuid = s_row["signal_uuid"]
            raw_message = s_row["raw_message"]

            # активные позиции на момент сигнала (entry_time <= T < exit_time)
            active_positions = _get_active_positions(existing_positions, new_positions, open_time)

            # тикер уже в позиции по этому направлению?
            if any(p["symbol"] == symbol for p in active_positions):
                logs_to_insert.append(
                    (
                        signal_uuid,
                        scenario_id,
                        None,
                        f"skipped: ticker already in position (symbol={symbol}, direction={direction})",
                    )
                )
                total_skipped += 1
                continue

            # маржа, занятая активными позициями (ТОЛЬКО по этому направлению и signal_id)
            used_margin_now = sum(p["margin_used"] for p in active_positions)
            free_margin = deposit - used_margin_now

            if free_margin <= Decimal("0"):
                logs_to_insert.append(
                    (
                        signal_uuid,
                        scenario_id,
                        None,
                        "skipped: no free margin",
                    )
                )
                total_skipped += 1
                continue

            # ограничение маржи на одну позицию
            max_margin_per_position = position_limit
            max_margin_for_trade = free_margin if free_margin < max_margin_per_position else max_margin_per_position
            if max_margin_for_trade <= Decimal("0"):
                logs_to_insert.append(
                    (
                        signal_uuid,
                        scenario_id,
                        None,
                        "skipped: no per-position margin available",
                    )
                )
                total_skipped += 1
                continue

            # получаем цену входа из raw_message
            try:
                if isinstance(raw_message, dict):
                    entry_price_val = raw_message.get("price")
                else:
                    raw_dict = json.loads(raw_message)
                    entry_price_val = raw_dict.get("price")

                entry_price = Decimal(str(entry_price_val))
            except Exception as e:
                log.error(
                    f"BT_SCENARIO_BASIC_MONO: сценарий id={scenario_id}, signal_id={signal_id}, "
                    f"symbol={symbol} — ошибка извлечения цены входа из raw_message: {e}",
                    exc_info=True,
                )
                logs_to_insert.append(
                    (
                        signal_uuid,
                        scenario_id,
                        None,
                        "skipped: invalid raw_message price",
                    )
                )
                total_skipped += 1
                continue

            if entry_price <= Decimal("0"):
                logs_to_insert.append(
                    (
                        signal_uuid,
                        scenario_id,
                        None,
                        "skipped: non-positive entry price",
                    )
                )
                total_skipped += 1
                continue

            # загрузка настроек тикера
            ticker_info = get_ticker_info(symbol) or {}
            min_qty_val = ticker_info.get("min_qty")
            precision_qty = ticker_info.get("precision_qty")
            precision_price = ticker_info.get("precision_price")
            ticksize_val = ticker_info.get("ticksize")

            try:
                min_qty = Decimal(str(min_qty_val)) if min_qty_val is not None else Decimal("0")
            except Exception:
                min_qty = Decimal("0")

            try:
                ticksize = Decimal(str(ticksize_val)) if ticksize_val is not None else None
            except Exception:
                ticksize = None

            # выравниваем цену входа по тикеру
            entry_price = _round_price(entry_price, precision_price, ticksize)

            # максимально допустимый notional под эту сделку
            max_notional_for_trade = max_margin_for_trade * leverage

            # теоретическое количество
            qty_raw = max_notional_for_trade / entry_price

            if precision_qty is not None:
                try:
                    q_dec = int(precision_qty)
                except Exception:
                    q_dec = 0
                quant = Decimal("1").scaleb(-q_dec)
                qty = qty_raw.quantize(quant, rounding=ROUND_DOWN)
            else:
                qty = qty_raw

            if qty <= Decimal("0"):
                logs_to_insert.append(
                    (
                        signal_uuid,
                        scenario_id,
                        None,
                        "skipped: qty <= 0 after rounding",
                    )
                )
                total_skipped += 1
                continue

            if qty < min_qty:
                logs_to_insert.append(
                    (
                        signal_uuid,
                        scenario_id,
                        None,
                        f"skipped: qty below min_qty (qty={qty}, min_qty={min_qty})",
                    )
                )
                total_skipped += 1
                continue

            entry_qty = qty

            # notional и маржа
            entry_notional = entry_price * entry_qty
            if entry_notional <= Decimal("0"):
                logs_to_insert.append(
                    (
                        signal_uuid,
                        scenario_id,
                        None,
                        "skipped: notional <= 0 after rounding",
                    )
                )
                total_skipped += 1
                continue

            margin_used = entry_notional / leverage

            # обрезаем деньги
            entry_notional = _q_money(entry_notional)
            margin_used = _q_money(margin_used)

            if margin_used > max_margin_for_trade:
                margin_used = _q_money(max_margin_for_trade)

            # расчёт уровней SL/TP в процентах
            sl_price, tp_price = _calc_sl_tp_percent(
                entry_price=entry_price,
                sl_percent=sl_value,
                tp_percent=tp_value,
                direction=direction,
            )

            # приводим цены SL/TP к precision_price и ticksize
            sl_price = _round_price(sl_price, precision_price, ticksize)
            tp_price = _round_price(tp_price, precision_price, ticksize)

            if sl_price <= Decimal("0") or tp_price <= Decimal("0"):
                logs_to_insert.append(
                    (
                        signal_uuid,
                        scenario_id,
                        None,
                        "skipped: invalid SL/TP price after rounding",
                    )
                )
                total_skipped += 1
                continue

            # моделируем жизнь сделки ДО to_time:
            # если TP/SL нет, позиция "жива"
            sim_result = await _simulate_trade(
                pg=pg,
                symbol=symbol,
                timeframe=timeframe,
                direction=direction,
                entry_time=open_time,
                entry_price=entry_price,
                entry_qty=entry_qty,
                entry_notional=entry_notional,
                sl_price=sl_price,
                tp_price=tp_price,
                to_time=to_time,
            )

            if sim_result is None:
                # позиция открыта и на момент to_time остаётся живой
                logs_to_insert.append(
                    (
                        signal_uuid,
                        scenario_id,
                        None,
                        "position opened and still alive",
                    )
                )
                # для учёта маржи внутри текущего прогона считаем, что она жива до to_time
                new_positions.append(
                    {
                        "symbol": symbol,
                        "entry_time": open_time,
                        "exit_time": to_time,
                        "margin_used": margin_used,
                    }
                )
                total_alive += 1
                continue

            (
                exit_time,
                exit_price,
                exit_reason,
                pnl_abs,
                duration,
                max_fav_pct,
                max_adv_pct,
            ) = sim_result

            # формируем позицию
            position_uid = uuid.uuid4()

            positions_to_insert.append(
                (
                    str(position_uid),
                    scenario_id,
                    signal_id,
                    signal_uuid,
                    symbol,
                    timeframe,
                    direction,
                    open_time,
                    entry_price,             # цена уже приведена к precision_price/ticksize
                    entry_qty,               # qty уже приведено к precision_qty
                    entry_notional,          # деньги обрезаны до 4 знаков
                    margin_used,             # деньги обрезаны до 4 знаков
                    sl_price,
                    tp_price,
                    exit_time,
                    exit_price,
                    exit_reason,
                    pnl_abs,
                    duration,
                    max_fav_pct,             # MFE в процентах от цены входа
                    max_adv_pct,             # MAE в процентах от цены входа
                    False,                   # postproc: ещё не обработана постпроцессором
                )
            )

            logs_to_insert.append(
                (
                    signal_uuid,
                    scenario_id,
                    str(position_uid),
                    "position opened",
                )
            )

            new_positions.append(
                {
                    "symbol": symbol,
                    "entry_time": open_time,
                    "exit_time": exit_time,
                    "margin_used": margin_used,
                }
            )

            affected_days.add(open_time.date())
            total_positions_opened += 1

    # вставляем позиции и логи в БД
    if positions_to_insert:
        async with pg.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO bt_scenario_positions (
                    position_uid,
                    scenario_id,
                    signal_id,
                    signal_uuid,
                    symbol,
                    timeframe,
                    direction,
                    entry_time,
                    entry_price,
                    entry_qty,
                    entry_notional,
                    margin_used,
                    sl_price,
                    tp_price,
                    exit_time,
                    exit_price,
                    exit_reason,
                    pnl_abs,
                    duration,
                    max_favorable_excursion,
                    max_adverse_excursion,
                    postproc,
                    created_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7,
                    $8, $9, $10, $11, $12, $13, $14,
                    $15, $16, $17, $18, $19, $20, $21, $22, now()
                )
                """,
                positions_to_insert,
            )

    if logs_to_insert:
        async with pg.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO bt_signals_log (
                    signal_uuid,
                    scenario_id,
                    position_uid,
                    report,
                    created_at
                )
                VALUES ($1, $2, $3, $4, now())
                """,
                logs_to_insert,
            )

    log.info(
        f"BT_SCENARIO_BASIC_MONO: сценарий id={scenario_id}, signal_id={signal_id} — "
        f"обработано сигналов={total_signals_processed}, позиций открыто={total_positions_opened}, "
        f"пропущено={total_skipped}, живых позиций={total_alive}"
    )

    # пересчёт суточной статистики и общей статистики по сценарию+сигналу
    if positions_to_insert:
        await _recalc_daily_stats(pg, scenario_id, signal_id, deposit, affected_days)
        await _recalc_total_stats(pg, scenario_id, signal_id, deposit)

    # отправляем уведомление в Redis Stream о завершении обработки сценария
    finished_at = datetime.utcnow()
    try:
        await redis.xadd(
            BT_SCENARIOS_READY_STREAM,
            {
                "scenario_id": str(scenario_id),
                "signal_id": str(signal_id),
                "finished_at": finished_at.isoformat(),
            },
        )
        log.debug(
            f"BT_SCENARIO_BASIC_MONO: опубликовано событие готовности сценария в стрим "
            f"'{BT_SCENARIOS_READY_STREAM}' для scenario_id={scenario_id}, signal_id={signal_id}, "
            f"finished_at={finished_at}"
        )
    except Exception as e:
        log.error(
            f"BT_SCENARIO_BASIC_MONO: не удалось опубликовать событие в стрим "
            f"'{BT_SCENARIOS_READY_STREAM}' для scenario_id={scenario_id}, signal_id={signal_id}: {e}",
            exc_info=True,
        )


# 🔸 Загрузка сигналов для сценария (без уже обработанных)
async def _load_signals_for_scenario(
    pg,
    scenario_id: int,
    signal_id: int,
    timeframe: str,
    from_time: datetime,
    to_time: datetime,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                v.signal_uuid,
                v.symbol,
                v.timeframe,
                v.open_time,
                v.direction,
                v.raw_message
            FROM bt_signals_values v
            LEFT JOIN bt_signals_log l
              ON l.signal_uuid = v.signal_uuid
             AND l.scenario_id = $2
            WHERE v.signal_id = $1
              AND v.timeframe = $3
              AND v.open_time BETWEEN $4 AND $5
              AND l.id IS NULL
            ORDER BY v.open_time
            """,
            signal_id,
            scenario_id,
            timeframe,
            from_time,
            to_time,
        )

    signals: List[Dict[str, Any]] = []
    for r in rows:
        signals.append(
            {
                "signal_uuid": r["signal_uuid"],
                "symbol": r["symbol"],
                "timeframe": r["timeframe"],
                "open_time": r["open_time"],
                "direction": r["direction"],
                "raw_message": r["raw_message"],
            }
        )

    log.debug(
        f"BT_SCENARIO_BASIC_MONO: загружено сигналов для scenario_id={scenario_id}, "
        f"signal_id={signal_id}, TF={timeframe} в окне [{from_time} .. {to_time}]: {len(signals)}"
    )
    return signals


# 🔸 Загрузка всех существующих позиций сценария+сигнала по TF/направлению
async def _load_existing_positions(
    pg,
    scenario_id: int,
    signal_id: int,
    timeframe: str,
    direction: str,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT symbol, entry_time, exit_time, margin_used
            FROM bt_scenario_positions
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND timeframe   = $3
              AND direction   = $4
            ORDER BY entry_time
            """,
            scenario_id,
            signal_id,
            timeframe,
            direction,
        )

    positions: List[Dict[str, Any]] = []
    for r in rows:
        positions.append(
            {
                "symbol": r["symbol"],
                "entry_time": r["entry_time"],
                "exit_time": r["exit_time"],
                "margin_used": Decimal(str(r["margin_used"])),
            }
        )

    log.debug(
        f"BT_SCENARIO_BASIC_MONO: загружены существующие позиции для scenario_id={scenario_id}, "
        f"signal_id={signal_id}, TF={timeframe}, direction={direction}: позиций={len(positions)}"
    )
    return positions


# 🔸 Получение активных позиций на момент T (entry_time <= T < exit_time)
def _get_active_positions(
    existing_positions: List[Dict[str, Any]],
    new_positions: List[Dict[str, Any]],
    current_time: datetime,
) -> List[Dict[str, Any]]:
    active: List[Dict[str, Any]] = []

    for p in existing_positions:
        if p["entry_time"] <= current_time < p["exit_time"]:
            active.append(p)

    for p in new_positions:
        if p["entry_time"] <= current_time < p["exit_time"]:
            active.append(p)

    return active


# 🔸 Расчёт SL/TP в процентах от цены входа
def _calc_sl_tp_percent(
    entry_price: Decimal,
    sl_percent: Decimal,
    tp_percent: Decimal,
    direction: str,
) -> Tuple[Decimal, Decimal]:
    if direction == "long":
        sl_price = entry_price * (Decimal("1") - sl_percent / Decimal("100"))
        tp_price = entry_price * (Decimal("1") + tp_percent / Decimal("100"))
    else:
        sl_price = entry_price * (Decimal("1") + sl_percent / Decimal("100"))
        tp_price = entry_price * (Decimal("1") - tp_percent / Decimal("100"))

    return sl_price, tp_price


# 🔸 Симуляция сделки: TP/SL + PnL, duration, MFE/MAE (в процентах от цены входа)
async def _simulate_trade(
    pg,
    symbol: str,
    timeframe: str,
    direction: str,
    entry_time: datetime,
    entry_price: Decimal,
    entry_qty: Decimal,
    entry_notional: Decimal,
    sl_price: Decimal,
    tp_price: Decimal,
    to_time: datetime,
) -> Optional[Tuple[datetime, Decimal, str, Decimal, timedelta, Decimal, Decimal]]:
    table_name = _ohlcv_table_for_timeframe(timeframe)
    if not table_name:
        return None

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT open_time, high, low, close
            FROM {table_name}
            WHERE symbol = $1
              AND open_time > $2
              AND open_time <= $3
            ORDER BY open_time
            """,
            symbol,
            entry_time,
            to_time,
        )

    if not rows:
        return None

    max_fav = Decimal("0")
    max_adv = Decimal("0")

    exit_time: Optional[datetime] = None
    exit_price: Optional[Decimal] = None
    exit_reason: Optional[str] = None

    for r in rows:
        otime = r["open_time"]
        high = Decimal(str(r["high"]))
        low = Decimal(str(r["low"]))
        close = Decimal(str(r["close"]))

        # вычисляем благоприятное и неблагоприятное движение в абсолютных ценовых шагах
        if direction == "long":
            fav_move = high - entry_price
            adv_move = low - entry_price
            if fav_move > max_fav:
                max_fav = fav_move
            if adv_move < max_adv:
                max_adv = adv_move

            touched_sl = low <= sl_price
            touched_tp = high >= tp_price

            if touched_sl and touched_tp:
                exit_time = otime
                exit_price = sl_price
                exit_reason = "sl_after_tp"
                break
            elif touched_sl:
                exit_time = otime
                exit_price = sl_price
                exit_reason = "full_sl_hit"
                break
            elif touched_tp:
                exit_time = otime
                exit_price = tp_price
                exit_reason = "full_tp_hit"
                break
        else:
            fav_move = entry_price - low
            adv_move = entry_price - high
            if fav_move > max_fav:
                max_fav = fav_move
            if adv_move < max_adv:
                max_adv = adv_move

            touched_sl = high >= sl_price
            touched_tp = low <= tp_price

            if touched_sl and touched_tp:
                exit_time = otime
                exit_price = sl_price
                exit_reason = "sl_after_tp"
                break
            elif touched_sl:
                exit_time = otime
                exit_price = sl_price
                exit_reason = "full_sl_hit"
                break
            elif touched_tp:
                exit_time = otime
                exit_price = tp_price
                exit_reason = "full_tp_hit"
                break

    # если ни TP, ни SL не были задеты в окне до to_time — считаем позицию "живой"
    if exit_time is None or exit_price is None or exit_reason is None:
        return None

    if direction == "long":
        raw_pnl = (exit_price - entry_price) * entry_qty
    else:
        raw_pnl = (entry_price - exit_price) * entry_qty

    raw_pnl = _q_money(raw_pnl)

    commission_rate = Decimal("0.0015")  # 0.15% вход+выход
    commission = _q_money(entry_notional * commission_rate)

    pnl_abs = raw_pnl - commission
    pnl_abs = _q_money(pnl_abs)

    duration = exit_time - entry_time

    if entry_price > Decimal("0"):
        max_fav_pct = (max_fav / entry_price) * Decimal("100")
        max_adv_pct = (max_adv / entry_price) * Decimal("100")
    else:
        max_fav_pct = Decimal("0")
        max_adv_pct = Decimal("0")

    max_fav_pct = _q_money(max_fav_pct)
    max_adv_pct = _q_money(max_adv_pct)

    return exit_time, exit_price, exit_reason, pnl_abs, duration, max_fav_pct, max_adv_pct


# 🔸 Определение таблицы OHLCV по TF
def _ohlcv_table_for_timeframe(timeframe: str) -> Optional[str]:
    if timeframe == "m5":
        return "ohlcv_bb_m5"
    if timeframe == "m15":
        return "ohlcv_bb_m15"
    if timeframe == "h1":
        return "ohlcv_bb_h1"
    return None


# 🔸 Пересчёт суточной статистики по затронутым дням (per scenario_id + signal_id + direction)
async def _recalc_daily_stats(
    pg,
    scenario_id: int,
    signal_id: int,
    deposit: Decimal,
    days: Set[date],
) -> None:
    if not days:
        return

    async with pg.acquire() as conn:
        for d in sorted(days):
            for direction in ("long", "short"):
                row = await conn.fetchrow(
                    """
                    SELECT
                        COUNT(*)                                         AS trades,
                        COUNT(*) FILTER (WHERE pnl_abs > 0)              AS wins,
                        COALESCE(SUM(pnl_abs), 0)                        AS pnl_abs_total,
                        COALESCE(AVG(max_favorable_excursion), 0)        AS mfe_avg,
                        COALESCE(AVG(max_adverse_excursion), 0)          AS mae_avg
                    FROM bt_scenario_positions
                    WHERE scenario_id = $1
                      AND signal_id   = $2
                      AND entry_time::date = $3
                      AND direction   = $4
                    """,
                    scenario_id,
                    signal_id,
                    d,
                    direction,
                )

                if not row or row["trades"] == 0:
                    continue

                trades = row["trades"]
                wins = row["wins"]
                pnl_abs_total = Decimal(str(row["pnl_abs_total"]))
                mfe_avg = Decimal(str(row["mfe_avg"]))
                mae_avg = Decimal(str(row["mae_avg"]))

                if trades > 0:
                    winrate = _q_money(Decimal(wins) / Decimal(trades))
                else:
                    winrate = Decimal("0")

                roi = _q_money(pnl_abs_total / deposit) if deposit != 0 else Decimal("0")

                await conn.execute(
                    """
                    INSERT INTO bt_scenario_daily (
                        scenario_id,
                        signal_id,
                        day,
                        direction,
                        trades,
                        pnl_abs,
                        winrate,
                        roi,
                        max_favorable_excursion_avg,
                        max_adverse_excursion_avg,
                        raw_stat,
                        created_at
                    )
                    VALUES (
                        $1, $2, $3, $4,
                        $5, $6, $7, $8,
                        $9, $10,
                        NULL,
                        now()
                    )
                    ON CONFLICT (scenario_id, signal_id, day, direction) DO UPDATE
                    SET
                        trades                      = EXCLUDED.trades,
                        pnl_abs                     = EXCLUDED.pnl_abs,
                        winrate                     = EXCLUDED.winrate,
                        roi                         = EXCLUDED.roi,
                        max_favorable_excursion_avg = EXCLUDED.max_favorable_excursion_avg,
                        max_adverse_excursion_avg   = EXCLUDED.max_adverse_excursion_avg,
                        updated_at                  = now()
                    """,
                    scenario_id,
                    signal_id,
                    d,
                    direction,
                    trades,
                    _q_money(pnl_abs_total),
                    winrate,
                    roi,
                    _q_money(mfe_avg),
                    _q_money(mae_avg),
                )

    log.debug(
        f"BT_SCENARIO_BASIC_MONO: пересчитана суточная статистика для scenario_id={scenario_id}, "
        f"signal_id={signal_id}, дней={len(days)}"
    )


# 🔸 Пересчёт общей статистики по сценарию и сигналу
async def _recalc_total_stats(
    pg,
    scenario_id: int,
    signal_id: int,
    deposit: Decimal,
) -> None:
    async with pg.acquire() as conn:
        for direction in ("long", "short"):
            row = await conn.fetchrow(
                """
                SELECT
                    COUNT(*)                                         AS trades,
                    COUNT(*) FILTER (WHERE pnl_abs > 0)              AS wins,
                    COALESCE(SUM(pnl_abs), 0)                        AS pnl_abs_total,
                    COALESCE(AVG(max_favorable_excursion), 0)        AS mfe_avg,
                    COALESCE(AVG(max_adverse_excursion), 0)          AS mae_avg
                FROM bt_scenario_positions
                WHERE scenario_id = $1
                  AND signal_id   = $2
                  AND direction   = $3
                """,
                scenario_id,
                signal_id,
                direction,
            )

            if not row or row["trades"] == 0:
                continue

            trades = row["trades"]
            wins = row["wins"]
            pnl_abs_total = Decimal(str(row["pnl_abs_total"]))
            mfe_avg = Decimal(str(row["mfe_avg"]))
            mae_avg = Decimal(str(row["mae_avg"]))

            if trades > 0:
                winrate = _q_money(Decimal(wins) / Decimal(trades))
            else:
                winrate = Decimal("0")

            roi = _q_money(pnl_abs_total / deposit) if deposit != 0 else Decimal("0")

            await conn.execute(
                """
                INSERT INTO bt_scenario_stat (
                    scenario_id,
                    signal_id,
                    direction,
                    trades,
                    pnl_abs,
                    winrate,
                    roi,
                    max_favorable_excursion_avg,
                    max_adverse_excursion_avg,
                    raw_stat,
                    created_at
                )
                VALUES (
                    $1, $2, $3,
                    $4, $5, $6, $7,
                    $8, $9,
                    NULL,
                    now()
                )
                ON CONFLICT (scenario_id, signal_id, direction) DO UPDATE
                SET
                    trades                      = EXCLUDED.trades,
                    pnl_abs                     = EXCLUDED.pnl_abs,
                    winrate                     = EXCLUDED.winrate,
                    roi                         = EXCLUDED.roi,
                    max_favorable_excursion_avg = EXCLUDED.max_favorable_excursion_avg,
                    max_adverse_excursion_avg   = EXCLUDED.max_adverse_excursion_avg,
                    updated_at                  = now()
                """,
                scenario_id,
                signal_id,
                direction,
                trades,
                _q_money(pnl_abs_total),
                winrate,
                roi,
                _q_money(mfe_avg),
                _q_money(mae_avg),
            )

    log.debug(
        f"BT_SCENARIO_BASIC_MONO: пересчитана итоговая статистика для scenario_id={scenario_id}, "
        f"signal_id={signal_id}"
    )