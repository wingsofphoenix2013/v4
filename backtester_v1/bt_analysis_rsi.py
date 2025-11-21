# bt_analysis_rsi.py — анализатор фич семейства RSI для backtester_v1

import json
import logging
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Any, Dict, List, Tuple, Optional

# 🔸 Кеши backtester_v1 (для доступа к параметрам сценария)
from backtester_config import get_scenario_instance

# 🔸 Настройки Decimal
getcontext().prec = 28

log = logging.getLogger("BT_ANALYSIS_RSI")


# 🔸 Квантование чисел до 4 знаков
def _q4(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


# 🔸 Безопасное деление
def _safe_div(n: Decimal, d: Decimal) -> Decimal:
    if d == 0:
        return Decimal("0")
    return n / d

# 🔸 Бины по абсолютному значению RSI (0–100)
def _default_rsi_value_bins() -> List[Tuple[float, float, str]]:
    return [
        (0.0, 20.0, "RSI_0_20"),
        (20.0, 30.0, "RSI_20_30"),
        (30.0, 40.0, "RSI_30_40"),
        (40.0, 50.0, "RSI_40_50"),
        (50.0, 60.0, "RSI_50_60"),
        (60.0, 70.0, "RSI_60_70"),
        (70.0, 100.0001, "RSI_70_100"),
    ]

# 🔸 Поиск бина для конкретного значения RSI
def _find_bin(value: float, bins: List[Tuple[float, float, str]]) -> Optional[Tuple[float, float, str]]:
    for b_from, b_to, label in bins:
        if b_from <= value < b_to:
            return b_from, b_to, label
    return None


# 🔸 Извлечение значения RSI из raw_stat с учётом TF и ключа
def _extract_rsi_value(
    raw_stat: Any,
    timeframe: str,
    source_key: str,
) -> Optional[float]:
    # если raw_stat пришёл как JSON-строка — разбираем
    if isinstance(raw_stat, str):
        try:
            raw_stat = json.loads(raw_stat)
        except Exception:
            return None

    if not isinstance(raw_stat, dict):
        return None

    tf_map = raw_stat.get("tf")
    if not isinstance(tf_map, dict):
        return None

    # приводим ключи TF к lower()
    tf_lower: Dict[str, Any] = {str(k).lower(): v for k, v in tf_map.items()}
    tf_block = tf_lower.get(timeframe.lower())
    if not isinstance(tf_block, dict):
        return None

    indicators = tf_block.get("indicators")
    if not isinstance(indicators, dict):
        return None

    # приводим семьи индикаторов к lower()
    indicators_lower: Dict[str, Any] = {str(k).lower(): v for k, v in indicators.items()}
    rsi_block_raw = indicators_lower.get("rsi")
    if not isinstance(rsi_block_raw, dict):
        return None

    # приводим ключи внутри RSI к lower()
    rsi_block: Dict[str, Any] = {str(k).lower(): v for k, v in rsi_block_raw.items()}
    rsi_val_raw = rsi_block.get(source_key.lower())
    if rsi_val_raw is None:
        return None

    try:
        return float(rsi_val_raw)
    except Exception:
        return None


# 🔸 Публичная точка входа: анализ семейства RSI для одного сценария+сигнала
async def run_analysis_rsi(
    scenario_id: int,
    signal_id: int,
    analysis_instances: List[Dict[str, Any]],
    pg,
) -> None:
    log.info(
        "BT_ANALYSIS_RSI: старт анализа RSI для scenario_id=%s, signal_id=%s, инстансов=%s",
        scenario_id,
        signal_id,
        len(analysis_instances),
    )

    if not analysis_instances:
        log.debug(
            "BT_ANALYSIS_RSI: для scenario_id=%s, signal_id=%s нет инстансов анализа RSI",
            scenario_id,
            signal_id,
        )
        return

    # загружаем сценарий, чтобы взять deposit для расчёта ROI
    scenario = get_scenario_instance(scenario_id)
    deposit: Optional[Decimal] = None

    if scenario:
        params = scenario.get("params") or {}
        deposit_cfg = params.get("deposit")
        if deposit_cfg is not None:
            try:
                deposit = Decimal(str(deposit_cfg.get("value")))
            except Exception:
                deposit = None

    # грузим все позиции этого сценария/сигнала, уже прошедшие постпроцессинг
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                id,
                direction,
                timeframe,
                raw_stat,
                pnl_abs
            FROM bt_scenario_positions
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND postproc    = true
            """,
            scenario_id,
            signal_id,
        )

    if not rows:
        log.debug(
            "BT_ANALYSIS_RSI: для scenario_id=%s, signal_id=%s нет позиций с postproc=true",
            scenario_id,
            signal_id,
        )
        return

    log.info(
        "BT_ANALYSIS_RSI: для scenario_id=%s, signal_id=%s загружено позиций=%s",
        scenario_id,
        signal_id,
        len(rows),
    )

    # обрабатываем каждый инстанс анализа независимо
    for inst in analysis_instances:
        family_key = inst.get("family_key")
        key = inst.get("key")
        inst_id = inst.get("id")
        params = inst.get("params") or {}

        # пока поддерживаем только rsi_value
        if family_key != "rsi" or key != "rsi_value":
            log.debug(
                "BT_ANALYSIS_RSI: inst_id=%s (family_key=%s, key=%s) пока не поддерживается",
                inst_id,
                family_key,
                key,
            )
            continue

        # извлекаем параметры инстанса
        tf_cfg = params.get("timeframe")
        source_cfg = params.get("source_key")

        timeframe = str(tf_cfg.get("value")).strip() if tf_cfg is not None else "m5"
        source_key = str(source_cfg.get("value")).strip() if source_cfg is not None else "rsi14"

        if not timeframe or not source_key:
            log.warning(
                "BT_ANALYSIS_RSI: inst_id=%s — некорректные параметры timeframe/source_key "
                "(timeframe=%s, source_key=%s)",
                inst_id,
                timeframe,
                source_key,
            )
            continue

        feature_name = f"rsi_value_{timeframe}_{source_key}"

        log.info(
            "BT_ANALYSIS_RSI: inst_id=%s — старт расчёта feature_name=%s, timeframe=%s, source_key=%s",
            inst_id,
            feature_name,
            timeframe,
            source_key,
        )

        # подготовка бинов
        bins = _default_rsi_value_bins()

        # агрегаты по (direction, bin_label)
        agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

        # проходим по всем позициям
        for r in rows:
            direction = r["direction"]
            raw_stat = r["raw_stat"]
            pnl_abs_raw = r["pnl_abs"]

            # условия достаточности
            if direction is None or raw_stat is None or pnl_abs_raw is None:
                continue

            try:
                pnl_abs = Decimal(str(pnl_abs_raw))
            except Exception:
                continue

            # извлекаем RSI из raw_stat по нужному TF и ключу
            rsi_val = _extract_rsi_value(
                raw_stat=raw_stat,
                timeframe=timeframe,
                source_key=source_key,
            )
            if rsi_val is None:
                continue

            bin_def = _find_bin(rsi_val, bins)
            if bin_def is None:
                continue

            b_from, b_to, bin_label = bin_def
            key_tuple = (direction, bin_label)

            bin_stat = agg.get(key_tuple)
            if bin_stat is None:
                bin_stat = {
                    "bin_from": b_from,
                    "bin_to": b_to,
                    "trades": 0,
                    "wins": 0,
                    "losses": 0,
                    "pnl_abs_total": Decimal("0"),
                }
                agg[key_tuple] = bin_stat

            bin_stat["trades"] += 1
            if pnl_abs > 0:
                bin_stat["wins"] += 1
            elif pnl_abs < 0:
                bin_stat["losses"] += 1
            bin_stat["pnl_abs_total"] += pnl_abs

        # если по инстансу не набралось ни одного бина — очищаем старые и выходим
        if not agg:
            log.info(
                "BT_ANALYSIS_RSI: inst_id=%s, feature_name=%s — нет данных для записи (agg пустой), "
                "очищаем старые бины",
                inst_id,
                feature_name,
            )
            async with pg.acquire() as conn:
                await conn.execute(
                    """
                    DELETE FROM bt_scenario_feature_bins
                    WHERE scenario_id  = $1
                      AND signal_id    = $2
                      AND timeframe    = $3
                      AND feature_name = $4
                    """,
                    scenario_id,
                    signal_id,
                    timeframe,
                    feature_name,
                )
            continue

        # подготавливаем строки для вставки
        rows_to_insert: List[Tuple[Any, ...]] = []

        for (direction, bin_label), stat in agg.items():
            trades = stat["trades"]
            wins = stat["wins"]
            losses = stat["losses"]
            pnl_abs_total = stat["pnl_abs_total"]

            if trades <= 0:
                continue

            winrate = _safe_div(Decimal(wins), Decimal(trades))
            if deposit is not None and deposit != 0:
                roi = _safe_div(pnl_abs_total, deposit)
            else:
                roi = Decimal("0")

            rows_to_insert.append(
                (
                    scenario_id,                 # scenario_id
                    signal_id,                   # signal_id
                    direction,                   # direction
                    timeframe,                   # timeframe
                    feature_name,                # feature_name
                    bin_label,                   # bin_label
                    stat["bin_from"],            # bin_from
                    stat["bin_to"],              # bin_to
                    trades,                      # trades
                    wins,                        # wins
                    losses,                      # losses
                    _q4(pnl_abs_total),          # pnl_abs_total
                    _q4(winrate),                # winrate
                    _q4(roi),                    # roi
                )
            )

        async with pg.acquire() as conn:
            # сначала удаляем старые бины по этой фиче/TF для данного сценария/сигнала
            await conn.execute(
                """
                DELETE FROM bt_scenario_feature_bins
                WHERE scenario_id  = $1
                  AND signal_id    = $2
                  AND timeframe    = $3
                  AND feature_name = $4
                """,
                scenario_id,
                signal_id,
                timeframe,
                feature_name,
            )

            if rows_to_insert:
                await conn.executemany(
                    """
                    INSERT INTO bt_scenario_feature_bins (
                        scenario_id,
                        signal_id,
                        direction,
                        timeframe,
                        feature_name,
                        bin_label,
                        bin_from,
                        bin_to,
                        trades,
                        wins,
                        losses,
                        pnl_abs_total,
                        winrate,
                        roi
                    )
                    VALUES (
                        $1, $2, $3, $4, $5,
                        $6, $7, $8, $9, $10,
                        $11, $12, $13, $14
                    )
                    """,
                    rows_to_insert,
                )

        log.info(
            "BT_ANALYSIS_RSI: inst_id=%s, feature_name=%s, timeframe=%s — бинов записано=%s",
            inst_id,
            feature_name,
            timeframe,
            len(rows_to_insert),
        )

    log.info(
        "BT_ANALYSIS_RSI: анализ RSI завершён для scenario_id=%s, signal_id=%s",
        scenario_id,
        signal_id,
    )