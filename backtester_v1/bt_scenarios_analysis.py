# bt_scenarios_analysis.py — аналитика позиций сценариев (биновые фичи по ATR/ADX/Supertrend/EMA/RSI/LR)

import asyncio
import logging
import json
from datetime import datetime
from decimal import Decimal, getcontext
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("BT_SCENARIOS_ANALYSIS")

# 🔸 Настройки Decimal
getcontext().prec = 28

# 🔸 Константы стрима аналитики
ANALYSIS_STREAM_KEY = "bt:postproc:ready"
ANALYSIS_CONSUMER_GROUP = "bt_scenarios_analysis"
ANALYSIS_CONSUMER_NAME = "bt_scenarios_analysis_main"

# 🔸 Настройки чтения стрима
ANALYSIS_STREAM_BATCH_SIZE = 10
ANALYSIS_STREAM_BLOCK_MS = 5000

# 🔸 Бины для ATR% на m5 (проценты)
ATR_BINS: List[Tuple[Optional[float], Optional[float], str]] = [
    (0.0, 0.25, "0.00-0.25"),
    (0.25, 0.50, "0.25-0.50"),
    (0.50, 0.75, "0.50-0.75"),
    (0.75, 1.00, "0.75-1.00"),
    (1.00, None, ">=1.00"),
]

# 🔸 Бины для ADX (m15/h1)
ADX_BINS: List[Tuple[Optional[float], Optional[float], str]] = [
    (0.0, 10.0, "0-10"),
    (10.0, 20.0, "10-20"),
    (20.0, 30.0, "20-30"),
    (30.0, None, ">=30"),
]

# 🔸 Бины для расстояния до EMA200 (в процентах)
# dist = (price - ema200) / price * 100
DIST_EMA_BINS: List[Tuple[Optional[float], Optional[float], str]] = [
    (None, -2.0, "<=-2"),   # очень ниже EMA200
    (-2.0, 0.0, "-2-0"),
    (0.0, 2.0, "0-2"),
    (2.0, None, ">=2"),     # сильно выше EMA200
]

# 🔸 Бины для RSI14
RSI_BINS: List[Tuple[Optional[float], Optional[float], str]] = [
    (0.0, 30.0, "0-30"),
    (30.0, 50.0, "30-50"),
    (50.0, 70.0, "50-70"),
    (70.0, None, ">=70"),
]

# 🔸 Порог для классификации LR угла
LR_ANGLE_EPS = 0.005  # небольшой порог для "flat"


# 🔸 Публичная точка входа: оркестратор аналитики сценариев
async def run_bt_scenarios_analysis(pg, redis) -> None:
    log.info("BT_SCENARIOS_ANALYSIS: воркер аналитики сценариев запущен")

    await _ensure_consumer_group(redis)

    # основной цикл чтения стрима и запуска анализа
    while True:
        try:
            messages = await _read_from_stream(redis)

            if not messages:
                continue

            total_msgs = 0
            total_scenarios_processed = 0

            for stream_key, entries in messages:
                if stream_key != ANALYSIS_STREAM_KEY:
                    # на всякий случай игнорируем чужие стримы
                    continue

                for entry_id, fields in entries:
                    total_msgs += 1

                    ctx = _parse_postproc_message(fields)
                    if not ctx:
                        # некорректное сообщение — ACK и пропускаем
                        await redis.xack(ANALYSIS_STREAM_KEY, ANALYSIS_CONSUMER_GROUP, entry_id)
                        continue

                    scenario_id = ctx["scenario_id"]
                    signal_id = ctx["signal_id"]
                    finished_at = ctx["finished_at"]
                    processed = ctx.get("processed")
                    skipped = ctx.get("skipped")
                    errors = ctx.get("errors")

                    log.info(
                        "BT_SCENARIOS_ANALYSIS: получено сообщение о завершении постпроцессинга "
                        "scenario_id=%s, signal_id=%s, processed=%s, skipped=%s, errors=%s, finished_at=%s, stream_id=%s",
                        scenario_id,
                        signal_id,
                        processed,
                        skipped,
                        errors,
                        finished_at,
                        entry_id,
                    )

                    # запускаем анализ по этому сценарию/сигналу
                    await _run_analysis_for_scenario(pg, scenario_id, signal_id)
                    total_scenarios_processed += 1

                    # помечаем сообщение как обработанное
                    await redis.xack(ANALYSIS_STREAM_KEY, ANALYSIS_CONSUMER_GROUP, entry_id)

            log.info(
                "BT_SCENARIOS_ANALYSIS: пакет сообщений обработан — сообщений=%s, сценариев=%s",
                total_msgs,
                total_scenarios_processed,
            )

        except Exception as e:
            log.error(
                "BT_SCENARIOS_ANALYSIS: ошибка в основном цикле воркера: %s",
                e,
                exc_info=True,
            )
            # небольшая пауза, чтобы не крутить CPU при постоянной ошибке
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима аналитики
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=ANALYSIS_STREAM_KEY,
            groupname=ANALYSIS_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.info(
            "BT_SCENARIOS_ANALYSIS: создана consumer group '%s' для стрима '%s'",
            ANALYSIS_CONSUMER_GROUP,
            ANALYSIS_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.info(
                "BT_SCENARIOS_ANALYSIS: consumer group '%s' для стрима '%s' уже существует",
                ANALYSIS_CONSUMER_GROUP,
                ANALYSIS_STREAM_KEY,
            )
        else:
            log.error(
                "BT_SCENARIOS_ANALYSIS: ошибка при создании consumer group '%s': %s",
                ANALYSIS_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:postproc:ready
async def _read_from_stream(redis) -> List[Any]:
    entries = await redis.xreadgroup(
        groupname=ANALYSIS_CONSUMER_GROUP,
        consumername=ANALYSIS_CONSUMER_NAME,
        streams={ANALYSIS_STREAM_KEY: ">"},
        count=ANALYSIS_STREAM_BATCH_SIZE,
        block=ANALYSIS_STREAM_BLOCK_MS,
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

        ctx: Dict[str, Any] = {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "finished_at": finished_at,
        }

        # необязательные поля
        for key in ("processed", "skipped", "errors"):
            if key in fields:
                try:
                    ctx[key] = int(fields[key])
                except Exception:
                    ctx[key] = None

        return ctx
    except Exception as e:
        log.error(
            "BT_SCENARIOS_ANALYSIS: ошибка разбора сообщения стрима bt:postproc:ready: %s, fields=%s",
            e,
            fields,
            exc_info=True,
        )
        return None


# 🔸 Запуск анализа для одного сценария/сигнала
async def _run_analysis_for_scenario(pg, scenario_id: int, signal_id: int) -> None:
    log.info(
        "BT_SCENARIOS_ANALYSIS: старт анализа для scenario_id=%s, signal_id=%s",
        scenario_id,
        signal_id,
    )

    # получаем депозит сценария
    deposit = await _load_scenario_deposit(pg, scenario_id)
    if deposit is None or deposit <= Decimal("0"):
        log.error(
            "BT_SCENARIOS_ANALYSIS: не удалось получить корректный deposit для scenario_id=%s",
            scenario_id,
        )
        return

    # загружаем все позиции с postproc=true
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                id,
                direction,
                entry_price,
                pnl_abs,
                raw_stat
            FROM bt_scenario_positions
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND postproc    = true
            """,
            scenario_id,
            signal_id,
        )

    if not rows:
        log.info(
            "BT_SCENARIOS_ANALYSIS: нет позиций для анализа (scenario_id=%s, signal_id=%s)",
            scenario_id,
            signal_id,
        )
        return

    total_positions = len(rows)
    log.info(
        "BT_SCENARIOS_ANALYSIS: загружено позиций для анализа: %s (scenario_id=%s, signal_id=%s)",
        total_positions,
        scenario_id,
        signal_id,
    )

    # агрегация по (direction, feature_name, bin_label)
    stats: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    skipped_positions = 0

    for r in rows:
        pos_id = r["id"]
        direction = r["direction"]
        entry_price_val = r["entry_price"]
        pnl_abs_val = r["pnl_abs"]
        raw_stat_val = r["raw_stat"]

        # работаем только с long/short
        if direction not in ("long", "short"):
            continue

        # приводим типы
        try:
            entry_price = Decimal(str(entry_price_val))
            pnl_abs = Decimal(str(pnl_abs_val))
        except Exception as e:
            log.warning(
                "BT_SCENARIOS_ANALYSIS: позиция id=%s — ошибка приведения типов entry_price/pnl_abs: %s",
                pos_id,
                e,
            )
            skipped_positions += 1
            continue

        if entry_price <= Decimal("0"):
            skipped_positions += 1
            continue

        # парсим raw_stat (jsonb)
        raw = _ensure_dict_from_json(raw_stat_val)
        if not raw:
            skipped_positions += 1
            continue

        tf_block = raw.get("tf") or {}
        m5_block = tf_block.get("m5") or {}
        m15_block = tf_block.get("m15") or {}
        h1_block = tf_block.get("h1") or {}

        m5_ind = m5_block.get("indicators") or {}
        m15_ind = m15_block.get("indicators") or {}
        h1_ind = h1_block.get("indicators") or {}

        # 🔸 ATR% m5
        atr_agg = m5_ind.get("atr") or {}
        atr14_m5 = _safe_get_float(atr_agg, "atr14")
        if atr14_m5 is not None:
            atr_pct_m5 = float((Decimal(str(atr14_m5)) / entry_price) * Decimal("100"))
            _accumulate_numeric_feature(
                stats,
                direction=direction,
                feature_name="atr_pct_m5",
                value=atr_pct_m5,
                bins=ATR_BINS,
                pnl_abs=pnl_abs,
            )

        # 🔸 ADX14 m15
        adx_m15_agg = m15_ind.get("adx_dmi") or {}
        adx14_m15 = _safe_get_float(adx_m15_agg, "adx_dmi14_adx")
        if adx14_m15 is not None:
            _accumulate_numeric_feature(
                stats,
                direction=direction,
                feature_name="adx14_m15",
                value=adx14_m15,
                bins=ADX_BINS,
                pnl_abs=pnl_abs,
            )

        # 🔸 ADX14 h1
        adx_h1_agg = h1_ind.get("adx_dmi") or {}
        adx14_h1 = _safe_get_float(adx_h1_agg, "adx_dmi14_adx")
        if adx14_h1 is not None:
            _accumulate_numeric_feature(
                stats,
                direction=direction,
                feature_name="adx14_h1",
                value=adx14_h1,
                bins=ADX_BINS,
                pnl_abs=pnl_abs,
            )

        # 🔸 Тренд Supertrend m15
        super_m15_agg = m15_ind.get("supertrend") or {}
        super_m15_trend = _safe_get_float(super_m15_agg, "supertrend10_3_0_trend")
        if super_m15_trend is not None:
            label_m15 = _trend_label(direction, super_m15_trend)
        else:
            label_m15 = "none"
        _accumulate_categorical_feature(
            stats,
            direction=direction,
            feature_name="trend_super_m15",
            bin_label=label_m15,
            pnl_abs=pnl_abs,
        )

        # 🔸 Тренд Supertrend h1
        super_h1_agg = h1_ind.get("supertrend") or {}
        super_h1_trend = _safe_get_float(super_h1_agg, "supertrend10_3_0_trend")
        if super_h1_trend is not None:
            label_h1 = _trend_label(direction, super_h1_trend)
        else:
            label_h1 = "none"
        _accumulate_categorical_feature(
            stats,
            direction=direction,
            feature_name="trend_super_h1",
            bin_label=label_h1,
            pnl_abs=pnl_abs,
        )

        # 🔸 Distance to EMA200 (m5/m15/h1)
        ema_m5_agg = m5_ind.get("ema") or {}
        ema_m15_agg = m15_ind.get("ema") or {}
        ema_h1_agg = h1_ind.get("ema") or {}

        ema200_m5 = _safe_get_float(ema_m5_agg, "ema200")
        ema200_m15 = _safe_get_float(ema_m15_agg, "ema200")
        ema200_h1 = _safe_get_float(ema_h1_agg, "ema200")

        if ema200_m5 is not None:
            dist_m5 = float((Decimal(str(ema200_m5)) - entry_price) / entry_price * Decimal("100"))
            _accumulate_numeric_feature(
                stats,
                direction=direction,
                feature_name="dist_ema200_m5",
                value=dist_m5,
                bins=DIST_EMA_BINS,
                pnl_abs=pnl_abs,
            )

        if ema200_m15 is not None:
            dist_m15 = float((Decimal(str(ema200_m15)) - entry_price) / entry_price * Decimal("100"))
            _accumulate_numeric_feature(
                stats,
                direction=direction,
                feature_name="dist_ema200_m15",
                value=dist_m15,
                bins=DIST_EMA_BINS,
                pnl_abs=pnl_abs,
            )

        if ema200_h1 is not None:
            dist_h1 = float((Decimal(str(ema200_h1)) - entry_price) / entry_price * Decimal("100"))
            _accumulate_numeric_feature(
                stats,
                direction=direction,
                feature_name="dist_ema200_h1",
                value=dist_h1,
                bins=DIST_EMA_BINS,
                pnl_abs=pnl_abs,
            )

        # 🔸 RSI14 (m5/m15)
        rsi_m5_agg = m5_ind.get("rsi") or {}
        rsi_m15_agg = m15_ind.get("rsi") or {}

        rsi14_m5 = _safe_get_float(rsi_m5_agg, "rsi14")
        if rsi14_m5 is not None:
            _accumulate_numeric_feature(
                stats,
                direction=direction,
                feature_name="rsi14_m5",
                value=rsi14_m5,
                bins=RSI_BINS,
                pnl_abs=pnl_abs,
            )

        rsi14_m15 = _safe_get_float(rsi_m15_agg, "rsi14")
        if rsi14_m15 is not None:
            _accumulate_numeric_feature(
                stats,
                direction=direction,
                feature_name="rsi14_m15",
                value=rsi14_m15,
                bins=RSI_BINS,
                pnl_abs=pnl_abs,
            )

        # 🔸 LR50 angle (m15/h1) — категориально: flat / up / down
        lr_m15_agg = m15_ind.get("lr") or {}
        lr_m15_angle = _safe_get_float(lr_m15_agg, "lr50_angle")
        if lr_m15_angle is not None:
            label_lr_m15 = _lr_angle_label(lr_m15_angle)
            _accumulate_categorical_feature(
                stats,
                direction=direction,
                feature_name="lr50_angle_m15",
                bin_label=label_lr_m15,
                pnl_abs=pnl_abs,
            )

        lr_h1_agg = h1_ind.get("lr") or {}
        lr_h1_angle = _safe_get_float(lr_h1_agg, "lr50_angle")
        if lr_h1_angle is not None:
            label_lr_h1 = _lr_angle_label(lr_h1_angle)
            _accumulate_categorical_feature(
                stats,
                direction=direction,
                feature_name="lr50_angle_h1",
                bin_label=label_lr_h1,
                pnl_abs=pnl_abs,
            )

    log.info(
        "BT_SCENARIOS_ANALYSIS: анализ завершён для scenario_id=%s, signal_id=%s — "
        "позиций всего=%s, пропущено=%s, бинов=%s",
        scenario_id,
        signal_id,
        total_positions,
        skipped_positions,
        len(stats),
    )

    # пересчёт winrate/roi и запись в таблицу
    await _write_feature_bins(pg, scenario_id, signal_id, deposit, stats)


# 🔸 Получение депозита сценария
async def _load_scenario_deposit(pg, scenario_id: int) -> Optional[Decimal]:
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT param_value
            FROM bt_scenario_parameters
            WHERE scenario_id = $1
              AND param_name  = 'deposit'
            """,
            scenario_id,
        )

    if not row:
        return None

    try:
        return Decimal(str(row["param_value"]))
    except Exception as e:
        log.error(
            "BT_SCENARIOS_ANALYSIS: ошибка приведения deposit для scenario_id=%s: %s",
            scenario_id,
            e,
        )
        return None


# 🔸 Преобразование raw_stat (jsonb) в dict
def _ensure_dict_from_json(raw_stat_val: Any) -> Dict[str, Any]:
    if raw_stat_val is None:
        return {}
    if isinstance(raw_stat_val, dict):
        return raw_stat_val
    try:
        # asyncpg может вернуть str для jsonb
        return json.loads(raw_stat_val)
    except Exception:
        return {}


# 🔸 Безопасное извлечение float из словаря
def _safe_get_float(d: Dict[str, Any], key: str) -> Optional[float]:
    try:
        v = d.get(key)
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


# 🔸 Определение метки тренда относительно направления сделки
def _trend_label(direction: str, trend_value: float) -> str:
    # direction: 'long' или 'short'
    if trend_value == 0:
        return "none"

    if direction == "long":
        if trend_value > 0:
            return "with_trend"
        if trend_value < 0:
            return "against_trend"
    elif direction == "short":
        if trend_value < 0:
            return "with_trend"
        if trend_value > 0:
            return "against_trend"

    return "none"


# 🔸 Метка LR-угла: flat / up / down
def _lr_angle_label(angle: float) -> str:
    if angle > LR_ANGLE_EPS:
        return "up"
    if angle < -LR_ANGLE_EPS:
        return "down"
    return "flat"


# 🔸 Агрегация числовой фичи по бинам
def _accumulate_numeric_feature(
    stats: Dict[Tuple[str, str, str], Dict[str, Any]],
    direction: str,
    feature_name: str,
    value: float,
    bins: List[Tuple[Optional[float], Optional[float], str]],
    pnl_abs: Decimal,
) -> None:
    bin_label = None
    bin_from: Optional[float] = None
    bin_to: Optional[float] = None

    for b_from, b_to, label in bins:
        # нижняя граница
        if b_from is not None and value < b_from:
            continue
        # верхняя граница
        if b_to is not None and value >= b_to:
            continue

        bin_label = label
        bin_from = b_from
        bin_to = b_to
        break

    if bin_label is None:
        return

    key = (direction, feature_name, bin_label)
    bucket = stats.setdefault(
        key,
        {
            "bin_from": bin_from,
            "bin_to": bin_to,
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "pnl_abs_total": Decimal("0"),
        },
    )

    bucket["trades"] += 1
    if pnl_abs > Decimal("0"):
        bucket["wins"] += 1
    elif pnl_abs < Decimal("0"):
        bucket["losses"] += 1

    bucket["pnl_abs_total"] += pnl_abs


# 🔸 Агрегация категориальной фичи
def _accumulate_categorical_feature(
    stats: Dict[Tuple[str, str, str], Dict[str, Any]],
    direction: str,
    feature_name: str,
    bin_label: str,
    pnl_abs: Decimal,
) -> None:
    key = (direction, feature_name, bin_label)
    bucket = stats.setdefault(
        key,
        {
            "bin_from": None,
            "bin_to": None,
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "pnl_abs_total": Decimal("0"),
        },
    )

    bucket["trades"] += 1
    if pnl_abs > Decimal("0"):
        bucket["wins"] += 1
    elif pnl_abs < Decimal("0"):
        bucket["losses"] += 1

    bucket["pnl_abs_total"] += pnl_abs


# 🔸 Запись агрегатов в bt_scenario_feature_bins
async def _write_feature_bins(
    pg,
    scenario_id: int,
    signal_id: int,
    deposit: Decimal,
    stats: Dict[Tuple[str, str, str], Dict[str, Any]],
) -> None:
    if not stats:
        log.info(
            "BT_SCENARIOS_ANALYSIS: нет данных для записи в bt_scenario_feature_bins (scenario_id=%s, signal_id=%s)",
            scenario_id,
            signal_id,
        )
        return

    rows_to_insert: List[Tuple[Any, ...]] = []

    for (direction, feature_name, bin_label), data in stats.items():
        trades = data["trades"]
        wins = data["wins"]
        losses = data["losses"]
        pnl_abs_total: Decimal = data["pnl_abs_total"]
        bin_from = data["bin_from"]
        bin_to = data["bin_to"]

        if trades > 0:
            winrate = (Decimal(wins) / Decimal(trades)).quantize(Decimal("0.0001"))
        else:
            winrate = Decimal("0")

        if deposit != 0:
            roi = (pnl_abs_total / deposit).quantize(Decimal("0.0001"))
        else:
            roi = Decimal("0")

        rows_to_insert.append(
            (
                scenario_id,
                signal_id,
                direction,
                feature_name,
                bin_label,
                bin_from,
                bin_to,
                trades,
                wins,
                losses,
                pnl_abs_total,
                winrate,
                roi,
            )
        )

    async with pg.acquire() as conn:
        # удаляем старые записи для этого сценария/сигнала
        await conn.execute(
            """
            DELETE FROM bt_scenario_feature_bins
            WHERE scenario_id = $1
              AND signal_id   = $2
            """,
            scenario_id,
            signal_id,
        )

        # вставляем новые
        await conn.executemany(
            """
            INSERT INTO bt_scenario_feature_bins (
                scenario_id,
                signal_id,
                direction,
                feature_name,
                bin_label,
                bin_from,
                bin_to,
                trades,
                wins,
                losses,
                pnl_abs_total,
                winrate,
                roi,
                created_at
            )
            VALUES (
                $1, $2, $3,
                $4, $5,
                $6, $7,
                $8, $9, $10,
                $11, $12, $13,
                now()
            )
            """,
            rows_to_insert,
        )

    log.info(
        "BT_SCENARIOS_ANALYSIS: записано строк в bt_scenario_feature_bins=%s для scenario_id=%s, signal_id=%s",
        len(rows_to_insert),
        scenario_id,
        signal_id,
    )