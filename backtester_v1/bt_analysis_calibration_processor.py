# bt_analysis_calibration_processor.py — построение адаптивных бин-конфигов по сырым фичам

import asyncio
import logging
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Кеши backtester_v1 (анализаторы)
from backtester_config import get_analysis_instance

# 🔸 Утилиты анализа фич
from bt_analysis_utils import resolve_feature_name

# 🔸 Настройки Decimal
getcontext().prec = 28

log = logging.getLogger("BT_ANALYSIS_CALIB_PROC")

# 🔸 Константы стримов
CALIB_READY_STREAM_KEY = "bt:analysis:calibration:ready"
ADAPTIVE_READY_STREAM_KEY = "bt:analysis:adaptive:ready"
CALIB_PROC_CONSUMER_GROUP = "bt_analysis_calib_proc"
CALIB_PROC_CONSUMER_NAME = "bt_analysis_calib_proc_main"

# 🔸 Настройки чтения стрима bt:analysis:calibration:ready
CALIB_PROC_STREAM_BATCH_SIZE = 10
CALIB_PROC_STREAM_BLOCK_MS = 5000

# 🔸 Версия адаптивных бин-конфигов
ADAPTIVE_VERSION = "v2"

# 🔸 Параметры калибровки
NUM_BINS = 5                # сколько бинов строим по квантилям
MIN_TRADES_FOR_CALIB = 100  # минимальное количество сделок для попытки калибровки

# 🔸 Поддерживаемые семейства для калибровки адаптивных бинов
SUPPORTED_FAMILIES_CALIB = {"rsi", "adx", "ema", "atr", "supertrend"}

# 🔸 Квантование до 4 знаков
def _q4(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


# 🔸 Безопасное деление
def _safe_div(n: Decimal, d: Decimal) -> Decimal:
    if d == 0:
        return Decimal("0")
    return n / d


# 🔸 Публичная точка входа: воркер построения адаптивных бин-конфигов
async def run_bt_analysis_calibration_processor(pg, redis):
    log.debug("BT_ANALYSIS_CALIB_PROC: воркер построения адаптивных бин-конфигов запущен")

    # подготавливаем consumer group для стрима bt:analysis:calibration:ready
    await _ensure_consumer_group(redis)

    # основной цикл чтения стрима и обработки
    while True:
        try:
            messages = await _read_from_stream(redis)

            if not messages:
                continue

            total_msgs = 0
            total_pairs = 0
            total_features_calibrated = 0

            for stream_key, entries in messages:
                if stream_key != CALIB_READY_STREAM_KEY:
                    continue

                for entry_id, fields in entries:
                    total_msgs += 1

                    ctx = _parse_calib_message(fields)
                    if not ctx:
                        await redis.xack(CALIB_READY_STREAM_KEY, CALIB_PROC_CONSUMER_GROUP, entry_id)
                        continue

                    scenario_id = ctx["scenario_id"]
                    signal_id = ctx["signal_id"]
                    family_key = ctx["family_key"]
                    analysis_ids = ctx["analysis_ids"]
                    rows_written = ctx["rows_written"]

                    log.debug(
                        "BT_ANALYSIS_CALIB_PROC: получено сообщение о готовности сырых фич "
                        "scenario_id=%s, signal_id=%s, family=%s, analysis_ids=%s, rows_written=%s, stream_id=%s",
                        scenario_id,
                        signal_id,
                        family_key,
                        analysis_ids,
                        rows_written,
                        entry_id,
                    )

                    if family_key not in SUPPORTED_FAMILIES_CALIB or not analysis_ids:
                        await redis.xack(CALIB_READY_STREAM_KEY, CALIB_PROC_CONSUMER_GROUP, entry_id)
                        continue

                    features_calibrated = await _process_family_calibration(
                        pg=pg,
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                        family_key=family_key,
                        analysis_ids=analysis_ids,
                    )
                    total_pairs += 1
                    total_features_calibrated += features_calibrated

                    # публикуем событие о готовности адаптивного бин-конфига
                    finished_at = datetime.utcnow()
                    try:
                        await redis.xadd(
                            ADAPTIVE_READY_STREAM_KEY,
                            {
                                "scenario_id": str(scenario_id),
                                "signal_id": str(signal_id),
                                "family_key": str(family_key),
                                "analysis_ids": ",".join(str(a) for a in analysis_ids),
                                "version": ADAPTIVE_VERSION,
                                "finished_at": finished_at.isoformat(),
                            },
                        )
                        log.debug(
                            "BT_ANALYSIS_CALIB_PROC: опубликовано событие в '%s' для scenario_id=%s, signal_id=%s, "
                            "family=%s, analysis_ids=%s, version=%s, finished_at=%s",
                            ADAPTIVE_READY_STREAM_KEY,
                            scenario_id,
                            signal_id,
                            family_key,
                            analysis_ids,
                            ADAPTIVE_VERSION,
                            finished_at,
                        )
                    except Exception as e:
                        log.error(
                            "BT_ANALYSIS_CALIB_PROC: не удалось опубликовать событие в стрим '%s' "
                            "для scenario_id=%s, signal_id=%s, family=%s: %s",
                            ADAPTIVE_READY_STREAM_KEY,
                            scenario_id,
                            signal_id,
                            family_key,
                            e,
                            exc_info=True,
                        )

                    await redis.xack(CALIB_READY_STREAM_KEY, CALIB_PROC_CONSUMER_GROUP, entry_id)

                    log.debug(
                        "BT_ANALYSIS_CALIB_PROC: сообщение stream_id=%s для scenario_id=%s, signal_id=%s "
                        "обработано, фич откалибровано=%s",
                        entry_id,
                        scenario_id,
                        signal_id,
                        features_calibrated,
                    )

            log.debug(
                "BT_ANALYSIS_CALIB_PROC: пакет сообщений обработан — сообщений=%s, пар_сценарий_сигнал=%s, "
                "фич_откалибровано=%s",
                total_msgs,
                total_pairs,
                total_features_calibrated,
            )

        except Exception as e:
            log.error(
                "BT_ANALYSIS_CALIB_PROC: ошибка в основном цикле воркера: %s",
                e,
                exc_info=True,
            )
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима bt:analysis:calibration:ready
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=CALIB_READY_STREAM_KEY,
            groupname=CALIB_PROC_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_ANALYSIS_CALIB_PROC: создана consumer group '%s' для стрима '%s'",
            CALIB_PROC_CONSUMER_GROUP,
            CALIB_READY_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_ANALYSIS_CALIB_PROC: consumer group '%s' для стрима '%s' уже существует",
                CALIB_PROC_CONSUMER_GROUP,
                CALIB_READY_STREAM_KEY,
            )
        else:
            log.error(
                "BT_ANALYSIS_CALIB_PROC: ошибка при создании consumer group '%s': %s",
                CALIB_PROC_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:analysis:calibration:ready
async def _read_from_stream(redis) -> List[Any]:
    entries = await redis.xreadgroup(
        groupname=CALIB_PROC_CONSUMER_GROUP,
        consumername=CALIB_PROC_CONSUMER_NAME,
        streams={CALIB_READY_STREAM_KEY: ">"},
        count=CALIB_PROC_STREAM_BATCH_SIZE,
        block=CALIB_PROC_STREAM_BLOCK_MS,
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


# 🔸 Разбор одного сообщения из стрима bt:analysis:calibration:ready
def _parse_calib_message(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id_str = fields.get("scenario_id")
        signal_id_str = fields.get("signal_id")
        family_key = fields.get("family_key")
        analysis_ids_str = fields.get("analysis_ids") or ""
        rows_written_str = fields.get("rows_written") or "0"
        finished_at_str = fields.get("finished_at")

        if not (scenario_id_str and signal_id_str and family_key and finished_at_str):
            return None

        scenario_id = int(scenario_id_str)
        signal_id = int(signal_id_str)
        finished_at = datetime.fromisoformat(finished_at_str)

        raw_ids = [s.strip() for s in analysis_ids_str.split(",") if s.strip()]
        analysis_ids: List[int] = []
        for s in raw_ids:
            try:
                analysis_ids.append(int(s))
            except Exception:
                continue

        try:
            rows_written = int(rows_written_str)
        except Exception:
            rows_written = 0

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "family_key": family_key,
            "analysis_ids": analysis_ids,
            "rows_written": rows_written,
            "finished_at": finished_at,
        }
    except Exception as e:
        log.error(
            "BT_ANALYSIS_CALIB_PROC: ошибка разбора сообщения стрима bt:analysis:calibration:ready: %s, fields=%s",
            e,
            fields,
            exc_info=True,
        )
        return None


# 🔸 Калибровка одного семейства анализаторов для пары scenario_id/signal_id
async def _process_family_calibration(
    pg,
    scenario_id: int,
    signal_id: int,
    family_key: str,
    analysis_ids: List[int],
) -> int:
    features_calibrated = 0

    # обрабатываем каждый анализатор независимо
    for aid in analysis_ids:
        inst = get_analysis_instance(aid)
        if not inst:
            log.warning(
                "BT_ANALYSIS_CALIB_PROC: analysis_id=%s не найден в кеше, scenario_id=%s, signal_id=%s",
                aid,
                scenario_id,
                signal_id,
            )
            continue

        inst_family = inst.get("family_key")
        key = inst.get("key")
        params = inst.get("params") or {}

        if inst_family != family_key:
            continue

        tf_cfg = params.get("timeframe")
        source_cfg = params.get("source_key")

        timeframe = str(tf_cfg.get("value")).strip() if tf_cfg is not None else "m5"
        source_key = str(source_cfg.get("value")).strip() if source_cfg is not None else ""

        feature_name = resolve_feature_name(
            family_key=family_key,
            key=key,
            timeframe=timeframe,
            source_key=source_key,
        )

        log.debug(
            "BT_ANALYSIS_CALIB_PROC: калибровка фичи для analysis_id=%s, family=%s, key=%s, "
            "feature_name=%s, timeframe=%s, source_key=%s, scenario_id=%s, signal_id=%s",
            aid,
            family_key,
            key,
            feature_name,
            timeframe,
            source_key,
            scenario_id,
            signal_id,
        )

        # загружаем сырые значения фичи
        async with pg.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT feature_value, is_win::int AS win
                FROM bt_position_features_raw
                WHERE scenario_id  = $1
                  AND signal_id    = $2
                  AND family_key   = $3
                  AND key          = $4
                  AND feature_name = $5
                """,
                scenario_id,
                signal_id,
                family_key,
                key,
                feature_name,
            )

        if not rows:
            log.debug(
                "BT_ANALYSIS_CALIB_PROC: нет сырых значений для feature_name=%s, "
                "scenario_id=%s, signal_id=%s",
                feature_name,
                scenario_id,
                signal_id,
            )
            continue

        values: List[float] = []
        wins: List[int] = []

        for r in rows:
            try:
                v = float(r["feature_value"])
            except Exception:
                continue
            values.append(v)
            wins.append(int(r["win"]))

        total_trades = len(values)
        if total_trades < max(NUM_BINS, MIN_TRADES_FOR_CALIB):
            log.debug(
                "BT_ANALYSIS_CALIB_PROC: недостаточно данных для калибровки feature_name=%s "
                "(trades=%s, min_required=%s)",
                feature_name,
                total_trades,
                max(NUM_BINS, MIN_TRADES_FOR_CALIB),
            )
            continue

        # строим квантильные бины
        bins = _build_quantile_bins(values, wins, num_bins=NUM_BINS)

        if not bins:
            log.debug(
                "BT_ANALYSIS_CALIB_PROC: не удалось построить бины для feature_name=%s "
                "(scenario_id=%s, signal_id=%s)",
                feature_name,
                scenario_id,
                signal_id,
            )
            continue

        # чистим старые бины для этой фичи/версии
        async with pg.acquire() as conn:
            await conn.execute(
                """
                DELETE FROM bt_position_features_bins
                WHERE family_key = $1
                  AND key        = $2
                  AND timeframe  = $3
                  AND source_key = $4
                  AND version    = $5
                """,
                family_key,
                key,
                timeframe,
                source_key,
                ADAPTIVE_VERSION,
            )

            # записываем новые бины
            rows_to_insert = []
            for order_index, b in enumerate(bins):
                from_val = b["from"]
                to_val = b["to"]
                trades = b["trades"]
                wins_bin = b["wins"]
                winrate = b["winrate"]
                coverage = b["coverage"]
                bin_label = f"Q{order_index + 1}"

                rows_to_insert.append(
                    (
                        family_key,
                        key,
                        timeframe,
                        source_key,
                        ADAPTIVE_VERSION,
                        bin_label,
                        from_val,
                        to_val,
                        order_index,
                        scenario_id,
                        signal_id,
                        trades,
                        _q4(winrate),
                        _q4(coverage),
                    )
                )

            await conn.executemany(
                """
                INSERT INTO bt_position_features_bins (
                    family_key,
                    key,
                    timeframe,
                    source_key,
                    version,
                    bin_label,
                    from_value,
                    to_value,
                    order_index,
                    calib_scenario_id,
                    calib_signal_id,
                    calib_trades,
                    calib_winrate,
                    calib_coverage,
                    created_at
                )
                VALUES (
                    $1, $2, $3, $4, $5,
                    $6, $7, $8, $9,
                    $10, $11, $12, $13, $14,
                    now()
                )
                """,
                rows_to_insert,
            )

        features_calibrated += 1

        log.debug(
            "BT_ANALYSIS_CALIB_PROC: записаны адаптивные бины для feature_name=%s "
            "(analysis_id=%s, scenario_id=%s, signal_id=%s, version=%s, bins=%s)",
            feature_name,
            aid,
            scenario_id,
            signal_id,
            ADAPTIVE_VERSION,
            len(bins),
        )

    return features_calibrated


# 🔸 Построение квантильных бинов для одной фичи
def _build_quantile_bins(
    values: List[float],
    wins: List[int],
    num_bins: int,
) -> List[Dict[str, Any]]:
    n = len(values)
    if n == 0 or num_bins <= 0:
        return []

    pairs = sorted(zip(values, wins), key=lambda x: x[0])
    bins: List[Dict[str, Any]] = []

    for i in range(num_bins):
        start = int(i * n / num_bins)
        end = int((i + 1) * n / num_bins)
        if end <= start:
            continue

        slice_pairs = pairs[start:end]
        trades = len(slice_pairs)
        wins_bin = sum(w for _, w in slice_pairs)
        winrate = _safe_div(Decimal(wins_bin), Decimal(trades))
        v_from = slice_pairs[0][0]
        v_to = slice_pairs[-1][0]
        coverage = _safe_div(Decimal(trades), Decimal(n))

        bins.append(
            {
                "from": v_from,
                "to": v_to,
                "trades": trades,
                "wins": wins_bin,
                "winrate": winrate,
                "coverage": coverage,
            }
        )

    return bins