# bt_scenario_tp_optimizer.py — воркер перебора TP1/долей для оптимизации сценария по ROI

import asyncio
import logging
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Any, Dict, List, Optional, Tuple, Set

log = logging.getLogger("BT_SCENARIO_TP_OPT")

# 🔸 Настройки Decimal
getcontext().prec = 28

# 🔸 Константы стрима postproc-результатов
POSTPROC_STREAM_KEY = "bt:postproc:ready"
POSTPROC_CONSUMER_GROUP = "bt_scenario_tp_optimizer"
POSTPROC_CONSUMER_NAME = "bt_scenario_tp_optimizer_main"

# 🔸 Базовый сценарий и сигналы, для которых запускаем оптимизацию
BASE_SCENARIO_ID = 1
OPTIMIZER_SIGNAL_IDS: Set[int] = {72, 73}

# 🔸 Константы для SL/TP2 (в процентах от цены входа)
SL_PERCENT = Decimal("1.0")   # -1%
TP2_PERCENT = Decimal("1.0")  # +1%

# 🔸 Сетка значений TP1 и долей на TP1
TP1_VALUES = [
    Decimal("0.5"),
    Decimal("0.6"),
    Decimal("0.7"),
    Decimal("0.8"),
    Decimal("0.9"),
]  # проценты движения цены

TP1_SHARE_PERCENTS = [
    Decimal("50"),
    Decimal("60"),
    Decimal("70"),
    Decimal("80"),
    Decimal("90"),
]  # проценты от позиции

# 🔸 Комиссия (упрощённо, как в сценариях)
COMMISSION_RATE = Decimal("0.0015")  # 0.15% вход+выход

# 🔸 Таймшаги TF (в минутах) — на будущее, если пригодится
TF_STEP_MINUTES = {
    "m5": 5,
    "m15": 15,
    "h1": 60,
}

# 🔸 Параллелизм загрузки OHLC по позициям
OPTIMIZER_LOAD_CONCURRENCY = 10


# 🔸 Обрезка денег/метрик до 4 знаков после запятой
def _q_money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


# 🔸 Публичная точка входа: воркер оптимизации TP по результатам postproc
async def run_bt_scenario_tp_optimizer(pg, redis) -> None:
    log.debug("BT_SCENARIO_TP_OPT: воркер оптимизации TP запущен")

    await _ensure_consumer_group(redis)

    # основной цикл чтения стрима bt:postproc:ready
    while True:
        try:
            messages = await _read_from_stream(redis)

            if not messages:
                continue

            total_msgs = 0
            total_pairs_processed = 0

            for stream_key, entries in messages:
                if stream_key != POSTPROC_STREAM_KEY:
                    continue

                for entry_id, fields in entries:
                    total_msgs += 1

                    ctx = _parse_postproc_message(fields)
                    if not ctx:
                        await redis.xack(POSTPROC_STREAM_KEY, POSTPROC_CONSUMER_GROUP, entry_id)
                        continue

                    scenario_id = ctx["scenario_id"]
                    signal_id = ctx["signal_id"]

                    # оптимизируем только базовый сценарий и только выбранные сигналы
                    if scenario_id != BASE_SCENARIO_ID or signal_id not in OPTIMIZER_SIGNAL_IDS:
                        log.debug(
                            "BT_SCENARIO_TP_OPT: сообщение stream_id=%s пропущено "
                            "(scenario_id=%s, signal_id=%s не входят в область оптимизации)",
                            entry_id,
                            scenario_id,
                            signal_id,
                        )
                        await redis.xack(POSTPROC_STREAM_KEY, POSTPROC_CONSUMER_GROUP, entry_id)
                        continue

                    log.info(
                        "BT_SCENARIO_TP_OPT: старт оптимизации для base_scenario_id=%s, signal_id=%s, stream_id=%s",
                        scenario_id,
                        signal_id,
                        entry_id,
                    )

                    await _run_optimizer_for_pair(pg, scenario_id, signal_id)
                    total_pairs_processed += 1

                    await redis.xack(POSTPROC_STREAM_KEY, POSTPROC_CONSUMER_GROUP, entry_id)

            if total_pairs_processed > 0:
                log.info(
                    "BT_SCENARIO_TP_OPT: пакет сообщений обработан — сообщений=%s, пар scenario/signal=%s",
                    total_msgs,
                    total_pairs_processed,
                )

        except Exception as e:
            log.error(
                "BT_SCENARIO_TP_OPT: ошибка в основном цикле воркера: %s",
                e,
                exc_info=True,
            )
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима postproc
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=POSTPROC_STREAM_KEY,
            groupname=POSTPROC_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_SCENARIO_TP_OPT: создана consumer group '%s' для стрима '%s'",
            POSTPROC_CONSUMER_GROUP,
            POSTPROC_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_SCENARIO_TP_OPT: consumer group '%s' для стрима '%s' уже существует",
                POSTPROC_CONSUMER_GROUP,
                POSTPROC_STREAM_KEY,
            )
        else:
            log.error(
                "BT_SCENARIO_TP_OPT: ошибка при создании consumer group '%s': %s",
                POSTPROC_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:postproc:ready
async def _read_from_stream(redis) -> List[Any]:
    entries = await redis.xreadgroup(
        groupname=POSTPROC_CONSUMER_GROUP,
        consumername=POSTPROC_CONSUMER_NAME,
        streams={POSTPROC_STREAM_KEY: ">"},
        count=10,
        block=5000,
    )

    if not entries:
        return []

    parsed: List[Any] = []
    for stream_key, messages in entries:
        if isinstance(stream_key, bytes):
            stream_key = stream_key.decode("utf-8")

        stream_entries: List[Any] = []
        for msg_id, fields in messages:
            if isinstance(msg_id, bytes):
                msg_id = msg_id.decode("utf-8")

            str_fields: Dict[str, str] = {}
            for k, v in fields.items():
                key_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                val_str = v.decode("utf-8") if isinstance(v, bytes) else str(v)
                str_fields[key_str] = val_str

            stream_entries.append((msg_id, str_fields))

        parsed.append((stream_key, stream_entries))

    return parsed


# 🔸 Разбор одного сообщения из стрима bt:postproc:ready
def _parse_postproc_message(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id_str = fields.get("scenario_id")
        signal_id_str = fields.get("signal_id")
        finished_at_str = fields.get("finished_at")

        if not (scenario_id_str and signal_id_str and finished_at_str):
            return None

        scenario_id = int(scenario_id_str)
        signal_id = int(signal_id_str)
        finished_at = datetime.fromisoformat(finished_at_str)

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "finished_at": finished_at,
        }
    except Exception as e:
        log.error(
            "BT_SCENARIO_TP_OPT: ошибка разбора сообщения bt:postproc:ready: %s, fields=%s",
            e,
            fields,
            exc_info=True,
        )
        return None


# 🔸 Запуск оптимизации для пары (base_scenario_id, signal_id)
async def _run_optimizer_for_pair(
    pg,
    base_scenario_id: int,
    signal_id: int,
) -> None:
    # грузим все позиции базового сценария для данного сигнала
    positions = await _load_base_positions(pg, base_scenario_id, signal_id)
    if not positions:
        log.debug(
            "BT_SCENARIO_TP_OPT: нет позиций для base_scenario_id=%s, signal_id=%s — оптимизация пропущена",
            base_scenario_id,
            signal_id,
        )
        return

    # депо для расчёта ROI
    deposit = await _get_deposit_for_scenario(pg, base_scenario_id)
    if deposit <= Decimal("0"):
        log.error(
            "BT_SCENARIO_TP_OPT: некорректный deposit (%s) для base_scenario_id=%s — оптимизация пропущена",
            deposit,
            base_scenario_id,
        )
        return

    # группируем позиции по TF
    positions_by_tf: Dict[str, List[Dict[str, Any]]] = {}
    for p in positions:
        tf = p["timeframe"]
        positions_by_tf.setdefault(tf, []).append(p)

    total_rows_written = 0

    # сначала загружаем OHLC для всех позиций (один раз на позицию), с параллелизмом
    for timeframe, tf_positions in positions_by_tf.items():
        sema = asyncio.Semaphore(OPTIMIZER_LOAD_CONCURRENCY)

        # для загрузки OHLC используем пул pg, каждый таск берёт свой conn
        tasks = [
            _load_ohlcv_for_position_with_semaphore(
                pg=pg,
                position=p,
                timeframe=timeframe,
                sema=sema,
            )
            for p in tf_positions
        ]
        await asyncio.gather(*tasks)

        # оставляем только те позиции, для которых удалось загрузить OHLC
        tf_positions_effective = [p for p in tf_positions if p.get("ohlc")]
        if not tf_positions_effective:
            log.debug(
                "BT_SCENARIO_TP_OPT: base_scenario_id=%s, signal_id=%s, TF=%s — "
                "нет позиций с доступными OHLC, оптимизация по TF пропущена",
                base_scenario_id,
                signal_id,
                timeframe,
            )
            continue

        # перебираем все комбинации TP1 и долей
        async with pg.acquire() as conn:
            for tp1_percent in TP1_VALUES:
                for tp1_share_percent in TP1_SHARE_PERCENTS:
                    tp1_share_frac = tp1_share_percent / Decimal("100")

                    trades = 0
                    wins = 0
                    pnl_total = Decimal("0")
                    mfe_sum = Decimal("0")
                    mae_sum = Decimal("0")

                    # считаем PnL для каждой сделки по новой схеме (чисто в памяти)
                    for pos in tf_positions_effective:
                        sim_result = _simulate_trade_double_on_rows(
                            rows=pos["ohlc"],
                            direction=pos["direction"],
                            entry_time=pos["entry_time"],
                            entry_price=pos["entry_price"],
                            entry_qty=pos["entry_qty"],
                            entry_notional=pos["entry_notional"],
                            tp1_percent=tp1_percent,
                            tp2_percent=TP2_PERCENT,
                            sl_percent=SL_PERCENT,
                            tp1_share_frac=tp1_share_frac,
                        )

                        if sim_result is None:
                            continue

                        pnl_abs, max_fav_pct, max_adv_pct = sim_result

                        trades += 1
                        if pnl_abs > Decimal("0"):
                            wins += 1
                        pnl_total += pnl_abs
                        mfe_sum += max_fav_pct
                        mae_sum += max_adv_pct

                    if trades == 0:
                        log.debug(
                            "BT_SCENARIO_TP_OPT: base_scenario_id=%s, signal_id=%s, TF=%s, tp1=%s, share=%s%% — "
                            "подходящих сделок нет (trades=0), строка не будет записана",
                            base_scenario_id,
                            signal_id,
                            timeframe,
                            tp1_percent,
                            tp1_share_percent,
                        )
                        continue

                    winrate = _q_money(Decimal(wins) / Decimal(trades))
                    roi = _q_money(pnl_total / deposit)
                    mfe_avg = _q_money(mfe_sum / Decimal(trades))
                    mae_avg = _q_money(mae_sum / Decimal(trades))

                    await conn.execute(
                        """
                        INSERT INTO bt_scenario_tp_optimizer (
                            base_scenario_id,
                            signal_id,
                            timeframe,
                            tp1_value,
                            tp1_share_percent,
                            tp2_value,
                            sl_value,
                            trades,
                            wins,
                            pnl_abs,
                            roi,
                            winrate,
                            mfe_avg,
                            mae_avg,
                            raw_stat,
                            created_at,
                            updated_at
                        )
                        VALUES (
                            $1, $2, $3,
                            $4, $5, $6, $7,
                            $8, $9, $10, $11, $12, $13, $14,
                            NULL,
                            now(),
                            NULL
                        )
                        ON CONFLICT (base_scenario_id, signal_id, timeframe, tp1_value, tp1_share_percent) DO UPDATE
                        SET
                            trades    = EXCLUDED.trades,
                            wins      = EXCLUDED.wins,
                            pnl_abs   = EXCLUDED.pnl_abs,
                            roi       = EXCLUDED.roi,
                            winrate   = EXCLUDED.winrate,
                            mfe_avg   = EXCLUDED.mfe_avg,
                            mae_avg   = EXCLUDED.mae_avg,
                            updated_at = now()
                        """,
                        base_scenario_id,
                        signal_id,
                        timeframe,
                        tp1_percent,
                        tp1_share_percent,
                        TP2_PERCENT,
                        SL_PERCENT,
                        trades,
                        wins,
                        _q_money(pnl_total),
                        roi,
                        winrate,
                        mfe_avg,
                        mae_avg,
                    )
                    total_rows_written += 1

    log.info(
        "BT_SCENARIO_TP_OPT: оптимизация завершена для base_scenario_id=%s, signal_id=%s — "
        "позиций=%s, комбинаций записано=%s",
        base_scenario_id,
        signal_id,
        len(positions),
        total_rows_written,
    )


# 🔸 Загрузка позиций базового сценария/сигнала
async def _load_base_positions(
    pg,
    base_scenario_id: int,
    signal_id: int,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                id,
                symbol,
                timeframe,
                direction,
                entry_time,
                entry_price,
                entry_qty,
                entry_notional,
                exit_time
            FROM bt_scenario_positions
            WHERE scenario_id = $1
              AND signal_id   = $2
            ORDER BY entry_time
            """,
            base_scenario_id,
            signal_id,
        )

    positions: List[Dict[str, Any]] = []
    for r in rows:
        positions.append(
            {
                "id": r["id"],
                "symbol": r["symbol"],
                "timeframe": r["timeframe"],
                "direction": r["direction"],
                "entry_time": r["entry_time"],
                "entry_price": Decimal(str(r["entry_price"])),
                "entry_qty": Decimal(str(r["entry_qty"])),
                "entry_notional": Decimal(str(r["entry_notional"])),
                "exit_time": r["exit_time"],
                "ohlc": [],  # будет заполнено позже
            }
        )

    log.debug(
        "BT_SCENARIO_TP_OPT: загружены позиции базового сценария=%s, signal_id=%s: позиций=%s",
        base_scenario_id,
        signal_id,
        len(positions),
    )
    return positions


# 🔸 Получение депозита базового сценария из bt_scenario_parameters
async def _get_deposit_for_scenario(pg, base_scenario_id: int) -> Decimal:
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT param_value
            FROM bt_scenario_parameters
            WHERE scenario_id = $1
              AND param_name  = 'deposit'
            LIMIT 1
            """,
            base_scenario_id,
        )

    if not row:
        log.error(
            "BT_SCENARIO_TP_OPT: не найден параметр 'deposit' для base_scenario_id=%s, "
            "будет использовано значение по умолчанию 1000",
            base_scenario_id,
        )
        return Decimal("1000")

    try:
        deposit = Decimal(str(row["param_value"]))
    except Exception as e:
        log.error(
            "BT_SCENARIO_TP_OPT: некорректное значение deposit '%s' для base_scenario_id=%s: %s, "
            "будет использовано значение по умолчанию 1000",
            row["param_value"],
            base_scenario_id,
            e,
            exc_info=True,
        )
        return Decimal("1000")

    return deposit


# 🔸 Обёртка загрузки OHLC с семафором (каждая задача сама берёт conn из пула)
async def _load_ohlcv_for_position_with_semaphore(
    pg,
    position: Dict[str, Any],
    timeframe: str,
    sema: asyncio.Semaphore,
) -> None:
    async with sema:
        try:
            async with pg.acquire() as conn:
                position["ohlc"] = await _load_ohlcv_for_position(conn, position, timeframe)
        except Exception as e:
            log.error(
                "BT_SCENARIO_TP_OPT: ошибка загрузки OHLC для позиции id=%s: %s",
                position.get("id"),
                e,
                exc_info=True,
            )
            position["ohlc"] = []


# 🔸 Загрузка OHLC для одной позиции (один раз)
async def _load_ohlcv_for_position(
    conn,
    position: Dict[str, Any],
    timeframe: str,
) -> List[Tuple[datetime, Decimal, Decimal, Decimal]]:
    symbol = position["symbol"]
    entry_time = position["entry_time"]
    exit_time_limit = position["exit_time"]

    table_name = _ohlcv_table_for_timeframe(timeframe)
    if not table_name:
        return []

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
        exit_time_limit,
    )

    ohlc_rows: List[Tuple[datetime, Decimal, Decimal, Decimal]] = []
    for r in rows:
        try:
            ohlc_rows.append(
                (
                    r["open_time"],
                    Decimal(str(r["high"])),
                    Decimal(str(r["low"])),
                    Decimal(str(r["close"])),
                )
            )
        except Exception:
            continue

    return ohlc_rows


# 🔸 Симуляция сделки с двумя тейками для одного входа по заранее загруженным OHLC
def _simulate_trade_double_on_rows(
    rows: List[Tuple[datetime, Decimal, Decimal, Decimal]],
    direction: str,
    entry_time: datetime,
    entry_price: Decimal,
    entry_qty: Decimal,
    entry_notional: Decimal,
    tp1_percent: Decimal,
    tp2_percent: Decimal,
    sl_percent: Decimal,
    tp1_share_frac: Decimal,
) -> Optional[Tuple[Decimal, Decimal, Decimal]]:
    if not rows:
        return None

    # уровни SL/TP1/TP2
    if direction == "long":
        sl_price = entry_price * (Decimal("1") - sl_percent / Decimal("100"))
        tp1_price = entry_price * (Decimal("1") + tp1_percent / Decimal("100"))
        tp2_price = entry_price * (Decimal("1") + tp2_percent / Decimal("100"))
    else:
        sl_price = entry_price * (Decimal("1") + sl_percent / Decimal("100"))
        tp1_price = entry_price * (Decimal("1") - tp1_percent / Decimal("100"))
        tp2_price = entry_price * (Decimal("1") - tp2_percent / Decimal("100"))

    # объёмы под TP1/TP2
    qty1_raw = entry_qty * tp1_share_frac
    qty1 = qty1_raw.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
    qty2 = entry_qty - qty1

    if qty1 <= Decimal("0") or qty2 <= Decimal("0"):
        return None

    max_fav = Decimal("0")
    max_adv = Decimal("0")

    leg1_open = True
    leg2_open = True

    pnl_leg1 = Decimal("0")
    pnl_leg2 = Decimal("0")

    # логика конфликтов такая же, как в double-сценарии:
    # TP1+SL → full_sl_hit, TP2+SL → sl_after_tp, TP1+TP2 → full_tp_hit
    for otime, high, low, close in rows:
        if direction == "long":
            fav_move = high - entry_price
            adv_move = low - entry_price
        else:
            fav_move = entry_price - low
            adv_move = entry_price - high

        if fav_move > max_fav:
            max_fav = fav_move
        if adv_move < max_adv:
            max_adv = adv_move

        if direction == "long":
            touched_sl = low <= sl_price
            touched_tp1 = high >= tp1_price
            touched_tp2 = high >= tp2_price
        else:
            touched_sl = high >= sl_price
            touched_tp1 = low <= tp1_price
            touched_tp2 = low <= tp2_price

        # оба плеча ещё открыты
        if leg1_open and leg2_open:
            # чистый SL
            if touched_sl and not touched_tp2 and not touched_tp1:
                if direction == "long":
                    pnl_full = (sl_price - entry_price) * (qty1 + qty2)
                else:
                    pnl_full = (entry_price - sl_price) * (qty1 + qty2)
                pnl_leg1 = pnl_full
                pnl_leg2 = Decimal("0")
                break

            # TP1 + SL на одной свече — считаем, что TP1 не было, полный SL
            if touched_sl and touched_tp1 and not touched_tp2:
                if direction == "long":
                    pnl_full = (sl_price - entry_price) * (qty1 + qty2)
                else:
                    pnl_full = (entry_price - sl_price) * (qty1 + qty2)
                pnl_leg1 = pnl_full
                pnl_leg2 = Decimal("0")
                break

            # TP1 + TP2 без SL — полный TP
            if not touched_sl and touched_tp2:
                if direction == "long":
                    pnl_leg1 = (tp1_price - entry_price) * qty1
                    pnl_leg2 = (tp2_price - entry_price) * qty2
                else:
                    pnl_leg1 = (entry_price - tp1_price) * qty1
                    pnl_leg2 = (entry_price - tp2_price) * qty2
                break

            # TP2 + SL на одной свече — SL после TP
            if touched_sl and touched_tp2:
                if direction == "long":
                    pnl_leg1 = (tp1_price - entry_price) * qty1
                    pnl_leg2 = (sl_price - entry_price) * qty2
                else:
                    pnl_leg1 = (entry_price - tp1_price) * qty1
                    pnl_leg2 = (entry_price - sl_price) * qty2
                break

            # только TP1 — закрывается первая часть, вторая живёт дальше
            if touched_tp1 and not touched_sl and not touched_tp2:
                leg1_open = False
                if direction == "long":
                    pnl_leg1 = (tp1_price - entry_price) * qty1
                else:
                    pnl_leg1 = (entry_price - tp1_price) * qty1
                continue

        # первая нога уже закрыта по TP1, жива только вторая
        if not leg1_open and leg2_open:
            # SL по остаточной позиции
            if touched_sl:
                if direction == "long":
                    pnl_leg2 = (sl_price - entry_price) * qty2
                else:
                    pnl_leg2 = (entry_price - sl_price) * qty2
                break

            # TP2 по остаточной позиции
            if touched_tp2:
                if direction == "long":
                    pnl_leg2 = (tp2_price - entry_price) * qty2
                else:
                    pnl_leg2 = (entry_price - tp2_price) * qty2
                break

    # если ни TP2, ни SL не были задеты — считаем позицию "живой", optimizer её не учитывает
    raw_pnl = pnl_leg1 + pnl_leg2
    if raw_pnl == Decimal("0"):
        return None

    raw_pnl = _q_money(raw_pnl)

    commission = _q_money(entry_notional * COMMISSION_RATE)
    pnl_abs = raw_pnl - commission
    pnl_abs = _q_money(pnl_abs)

    if entry_price > Decimal("0"):
        max_fav_pct = (max_fav / entry_price) * Decimal("100")
        max_adv_pct = (max_adv / entry_price) * Decimal("100")
    else:
        max_fav_pct = Decimal("0")
        max_adv_pct = Decimal("0")

    max_fav_pct = _q_money(max_fav_pct)
    max_adv_pct = _q_money(max_adv_pct)

    return pnl_abs, max_fav_pct, max_adv_pct


# 🔸 Определение таблицы OHLCV по TF
def _ohlcv_table_for_timeframe(timeframe: str) -> Optional[str]:
    tf = (timeframe or "").lower()
    if tf == "m5":
        return "ohlcv_bb_m5"
    if tf == "m15":
        return "ohlcv_bb_m15"
    if tf == "h1":
        return "ohlcv_bb_h1"
    return None