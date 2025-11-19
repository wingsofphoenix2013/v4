# bt_scenario_basic_straight_mono.py — базовый straight-сценарий (mono) для backtester_v1

import asyncio
import logging
import uuid
import json
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Dict, Any, List, Tuple, Optional

# 🔸 Кеши backtester_v1
from backtester_config import get_signal_instance, get_ticker_info

log = logging.getLogger("BT_SCENARIO_BASIC_MONO")

# 🔸 Настройки Decimal
getcontext().prec = 28


# 🔸 Утилита: обрезка до 4 знаков после запятой
def _q4(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


# 🔸 Публичная точка входа: backfill для сценария basic_straight_mono по одному окну сигнала
async def run_basic_straight_mono_backfill(
    scenario: Dict[str, Any],
    signal_ctx: Dict[str, Any],
    pg,
    redis,
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
        log.warning(
            f"BT_SCENARIO_BASIC_MONO: сценарий id={scenario_id} поддерживает только sl_type/tp_type='percent', "
            f"получено sl_type='{sl_type}', tp_type='{tp_type}' — сигналы будут пропускаться"
        )

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

    log.info(
        f"BT_SCENARIO_BASIC_MONO: старт обработки сценария id={scenario_id} (key={scenario_key}, type={scenario_type}) "
        f"для signal_id={signal_id}, TF={timeframe}, окно=[{from_time} .. {to_time}], "
        f"deposit={deposit}, leverage={leverage}, position_limit={position_limit}, "
        f"SL={sl_value}% TP={tp_value}%"
    )

    # грузим сигналы для данного signal_id/TF/окна, которые ещё не обрабатывались этим сценарием
    signals = await _load_signals_for_scenario(pg, scenario_id, signal_id, timeframe, from_time, to_time)
    if not signals:
        log.info(
            f"BT_SCENARIO_BASIC_MONO: сценарий id={scenario_id}, signal_id={signal_id} — "
            f"актуальных сигналов для обработки не найдено"
        )
        return

    # инициализация состояния по марже и последним позициям
    # used_margin учитывает только позиции, открытые в рамках текущего прогона
    used_margin = Decimal("0")

    # последние позиции по символу/направлению — по данным БД (исторические)
    last_exit_long = await _load_last_exit_times(pg, scenario_id, timeframe, "long")
    last_exit_short = await _load_last_exit_times(pg, scenario_id, timeframe, "short")

    # локальные обновления last_exit_* для позиций, открытых в текущем прогоне
    local_last_exit_long: Dict[str, datetime] = {}
    local_last_exit_short: Dict[str, datetime] = {}

    # списки для вставки позиций и логов
    positions_to_insert: List[Tuple[Any, ...]] = []
    logs_to_insert: List[Tuple[Any, ...]] = []

    # обрабатываем сначала long, потом short (направления независимы, но общая маржа одна)
    total_signals_processed = 0
    total_positions_opened = 0
    total_skipped = 0

    # обрабатываем только одно направление за проход, но для mono нам нужны оба (long и short)
    for direction in ("long", "short"):
        # начальное состояние last_exit по направлению
        if direction == "long":
            last_exit = dict(last_exit_long)
            local_last_exit = local_last_exit_long
        else:
            last_exit = dict(last_exit_short)
            local_last_exit = local_last_exit_short

        # обновим last_exit локальными изменениями из предыдущего направления, если они есть
        # (на случай, если позже решим учитывать перекрытие между направлениями)
        for sym, dt in local_last_exit.items():
            last_exit[sym] = dt

        # фильтруем сигналы по направлению
        dir_signals = [s for s in signals if s["direction"] == direction]
        if not dir_signals:
            continue

        log.info(
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

            # проверка существующей позиции по этому символу/направлению (берём ТОЛЬКО последнюю)
            # сначала из истории, затем из локальных данных текущего прогона
            last_exit_time = local_last_exit.get(symbol) or last_exit.get(symbol)

            if last_exit_time and open_time <= last_exit_time:
                # тикер уже был/есть в позиции в момент этого сигнала — пропускаем
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

            # проверка свободной маржи сценария
            free_margin = deposit - used_margin
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

            # вычисляем максимально допустимый notional под эту сделку
            max_notional_for_trade = max_margin_for_trade * leverage

            # считаем теоретическое количество и приводим к precision_qty
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

            entry_qty = _q4(qty)

            # пересчитываем notional и маржу по итоговым qty/цене
            entry_notional = _q4(entry_price * entry_qty)
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

            margin_used = _q4(entry_notional / leverage)
            if margin_used > max_margin_for_trade:
                # теоретически не должно быть, т.к. мы режем вниз, но на всякий случай
                margin_used = _q4(max_margin_for_trade)

            # расчёт уровней SL/TP в процентах
            if sl_type != "percent" or tp_type != "percent":
                logs_to_insert.append(
                    (
                        signal_uuid,
                        scenario_id,
                        None,
                        "skipped: unsupported sl_type/tp_type for basic_straight_mono",
                    )
                )
                total_skipped += 1
                continue

            sl_price, tp_price = _calc_sl_tp_percent(
                entry_price=entry_price,
                sl_percent=sl_value,
                tp_percent=tp_value,
                direction=direction,
            )

            # приводим цены к precision_price и ticksize
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

            # моделируем жизнь сделки: поиск первого касания TP/SL
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
            )

            if sim_result is None:
                logs_to_insert.append(
                    (
                        signal_uuid,
                        scenario_id,
                        None,
                        "skipped: not enough ohlcv data for simulation",
                    )
                )
                total_skipped += 1
                continue

            (
                exit_time,
                exit_price,
                exit_reason,
                pnl_abs,
                duration,
                max_fav,
                max_adv,
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
                    _q4(entry_price),
                    _q4(entry_qty),
                    _q4(entry_notional),
                    _q4(margin_used),
                    _q4(sl_price),
                    _q4(tp_price),
                    exit_time,
                    _q4(exit_price),
                    exit_reason,
                    _q4(pnl_abs),
                    duration,
                    _q4(max_fav),
                    _q4(max_adv),
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

            # обновляем состояние маржи и последнюю позицию для символа/направления
            used_margin += margin_used
            local_last_exit[symbol] = exit_time
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
                    created_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7,
                    $8, $9, $10, $11, $12, $13, $14,
                    $15, $16, $17, $18, $19, $20, $21, now()
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
        f"пропущено={total_skipped}, итоговая использованная маржа={_q4(used_margin)}"
    )


# 🔸 Загрузка сигналов для сценария (без уже залогированных)
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

    log.info(
        f"BT_SCENARIO_BASIC_MONO: загружено сигналов для scenario_id={scenario_id}, "
        f"signal_id={signal_id}, TF={timeframe} в окне [{from_time} .. {to_time}]: {len(signals)}"
    )
    return signals


# 🔸 Загрузка последних exit_time по символам/направлению для сценария
async def _load_last_exit_times(
    pg,
    scenario_id: int,
    timeframe: str,
    direction: str,
) -> Dict[str, datetime]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (symbol)
                symbol,
                exit_time
            FROM bt_scenario_positions
            WHERE scenario_id = $1
              AND timeframe = $2
              AND direction = $3
            ORDER BY symbol, entry_time DESC
            """,
            scenario_id,
            timeframe,
            direction,
        )

    result: Dict[str, datetime] = {}
    for r in rows:
        result[r["symbol"]] = r["exit_time"]

    log.info(
        f"BT_SCENARIO_BASIC_MONO: загружены последние позиции для scenario_id={scenario_id}, "
        f"TF={timeframe}, direction={direction}: символов={len(result)}"
    )
    return result


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
        # short
        sl_price = entry_price * (Decimal("1") + sl_percent / Decimal("100"))
        tp_price = entry_price * (Decimal("1") - tp_percent / Decimal("100"))

    return sl_price, tp_price


# 🔸 Приведение цены к precision_price и ticksize
def _round_price(
    price: Decimal,
    precision_price: Optional[int],
    ticksize: Optional[Decimal],
) -> Decimal:
    # сначала обрезка по precision_price
    if precision_price is not None:
        try:
            p_dec = int(precision_price)
        except Exception:
            p_dec = 0
        quant = Decimal("1").scaleb(-p_dec)
        price = price.quantize(quant, rounding=ROUND_DOWN)

    # затем обрезка по ticksize, если есть
    if ticksize is not None and ticksize > Decimal("0"):
        steps = (price / ticksize).to_integral_value(rounding=ROUND_DOWN)
        price = steps * ticksize

    return _q4(price)


# 🔸 Симуляция сделки: поиск первого касания TP/SL + PnL, duration, MFE/MAE
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
            ORDER BY open_time
            """,
            symbol,
            entry_time,
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

        if direction == "long":
            # обновляем MFE/MAE
            fav_move = high - entry_price
            adv_move = low - entry_price
            if fav_move > max_fav:
                max_fav = fav_move
            if adv_move < max_adv:
                max_adv = adv_move

            touched_sl = low <= sl_price
            touched_tp = high >= tp_price

            if touched_sl and touched_tp:
                # консервативно считаем, что первым сработал SL
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
            # short
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

    # если ни TP, ни SL не были задеты — закрываем по последней свече (timeout_closed)
    if exit_time is None or exit_price is None or exit_reason is None:
        last = rows[-1]
        exit_time = last["open_time"]
        last_close = Decimal(str(last["close"]))
        exit_price = last_close
        exit_reason = "timeout_closed"

        # обновляем MFE/MAE для последнего бара, если ещё не учли
        # (формально мы проходили все бары, но на всякий случай)
        if direction == "long":
            high = Decimal(str(last["high"]))
            low = Decimal(str(last["low"]))
            fav_move = high - entry_price
            adv_move = low - entry_price
            if fav_move > max_fav:
                max_fav = fav_move
            if adv_move < max_adv:
                max_adv = adv_move
        else:
            high = Decimal(str(last["high"]))
            low = Decimal(str(last["low"]))
            fav_move = entry_price - low
            adv_move = entry_price - high
            if fav_move > max_fav:
                max_fav = fav_move
            if adv_move < max_adv:
                max_adv = adv_move

    # расчёт PnL и комиссии
    if direction == "long":
        raw_pnl = (exit_price - entry_price) * entry_qty
    else:
        raw_pnl = (entry_price - exit_price) * entry_qty

    raw_pnl = _q4(raw_pnl)

    commission_rate = Decimal("0.0015")  # 0.15% вход+выход
    commission = _q4(entry_notional * commission_rate)

    pnl_abs = raw_pnl - commission
    pnl_abs = _q4(pnl_abs)

    duration = exit_time - entry_time

    # MFE/MAE уже в дельтах цены; обрезаем до 4 знаков
    max_fav = _q4(max_fav)
    max_adv = _q4(max_adv)

    return exit_time, exit_price, exit_reason, pnl_abs, duration, max_fav, max_adv


# 🔸 Определение таблицы OHLCV по TF
def _ohlcv_table_for_timeframe(timeframe: str) -> Optional[str]:
    if timeframe == "m5":
        return "ohlcv_bb_m5"
    if timeframe == "m15":
        return "ohlcv_bb_m15"
    if timeframe == "h1":
        return "ohlcv_bb_h1"
    return None