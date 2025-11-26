# bt_analysis_calibration_raw.py — оркестратор калибровки сырых фич анализаторов

# 🔸 Импорты стандартной библиотеки
import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

# 🔸 Импорты семейств калибровки сырых фич
from bt_analysis_calibration_rsi import run_calibration_rsi_raw
from bt_analysis_calibration_adx import run_calibration_adx_raw
from bt_analysis_calibration_ema import run_calibration_ema_raw
from bt_analysis_calibration_atr import run_calibration_atr_raw
from bt_analysis_calibration_supertrend import run_calibration_supertrend_raw

log = logging.getLogger("BT_ANALYSIS_CALIB_RAW")

# 🔸 Константы стримов анализа
ANALYSIS_READY_STREAM_KEY = "bt:analysis:ready"
CALIB_READY_STREAM_KEY = "bt:analysis:calibration:ready"
CALIB_CONSUMER_GROUP = "bt_analysis_calib_raw"
CALIB_CONSUMER_NAME = "bt_analysis_calib_raw_main"

# 🔸 Настройки чтения стрима bt:analysis:ready
CALIB_STREAM_BATCH_SIZE = 10
CALIB_STREAM_BLOCK_MS = 5000

# 🔸 Регистр обработчиков семейств для калибровки
FAMILY_CALIBRATION_HANDLERS = {
    "rsi": run_calibration_rsi_raw,
    "adx": run_calibration_adx_raw,
    "ema": run_calibration_ema_raw,
    "atr": run_calibration_atr_raw,
    "supertrend": run_calibration_supertrend_raw,
}


# 🔸 Публичная точка входа: воркер калибровки сырых фич (оркестратор семейств)
async def run_bt_analysis_calibration_raw(pg, redis):
    log.debug("BT_ANALYSIS_CALIB_RAW: воркер калибровки сырых фич запущен")

    # подготавливаем consumer group для стрима bt:analysis:ready
    await _ensure_consumer_group(redis)

    # основной цикл чтения стрима и обработки
    while True:
        try:
            messages = await _read_from_stream(redis)

            if not messages:
                continue

            total_msgs = 0
            total_pairs = 0
            total_rows_written = 0

            for stream_key, entries in messages:
                if stream_key != ANALYSIS_READY_STREAM_KEY:
                    continue

                for entry_id, fields in entries:
                    total_msgs += 1

                    ctx = _parse_ready_message(fields)
                    if not ctx:
                        await redis.xack(ANALYSIS_READY_STREAM_KEY, CALIB_CONSUMER_GROUP, entry_id)
                        continue

                    scenario_id = ctx["scenario_id"]
                    signal_id = ctx["signal_id"]
                    family_key = ctx["family_key"]
                    analysis_ids = ctx["analysis_ids"]
                    version = ctx["version"]

                    log.debug(
                        "BT_ANALYSIS_CALIB_RAW: получено сообщение о готовности анализа "
                        "scenario_id=%s, signal_id=%s, family=%s, version=%s, analysis_ids=%s, stream_id=%s",
                        scenario_id,
                        signal_id,
                        family_key,
                        version,
                        analysis_ids,
                        entry_id,
                    )

                    # пока работаем только с известными семействами и только для v1
                    handler = FAMILY_CALIBRATION_HANDLERS.get(family_key)
                    if handler is None or not analysis_ids or version != "v1":
                        await redis.xack(ANALYSIS_READY_STREAM_KEY, CALIB_CONSUMER_GROUP, entry_id)
                        continue

                    # выполняем калибровку сырых фич для данного семейства
                    rows_written = await _process_family_raw(
                        pg=pg,
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                        family_key=family_key,
                        analysis_ids=analysis_ids,
                        handler=handler,
                    )
                    total_pairs += 1
                    total_rows_written += rows_written

                    # 🔸 Публикуем событие готовности калибровки в bt:analysis:calibration:ready
                    finished_at = datetime.utcnow()
                    try:
                        await redis.xadd(
                            CALIB_READY_STREAM_KEY,
                            {
                                "scenario_id": str(scenario_id),
                                "signal_id": str(signal_id),
                                "family_key": str(family_key),
                                "analysis_ids": ",".join(str(a) for a in analysis_ids),
                                "rows_written": str(rows_written),
                                "finished_at": finished_at.isoformat(),
                            },
                        )
                        log.debug(
                            "BT_ANALYSIS_CALIB_RAW: опубликовано событие в '%s' для scenario_id=%s, signal_id=%s, "
                            "family=%s, analysis_ids=%s, rows_written=%s, finished_at=%s",
                            CALIB_READY_STREAM_KEY,
                            scenario_id,
                            signal_id,
                            family_key,
                            analysis_ids,
                            rows_written,
                            finished_at,
                        )
                    except Exception as e:
                        log.error(
                            "BT_ANALYSIS_CALIB_RAW: не удалось опубликовать событие в стрим '%s' "
                            "для scenario_id=%s, signal_id=%s, family=%s: %s",
                            CALIB_READY_STREAM_KEY,
                            scenario_id,
                            signal_id,
                            family_key,
                            e,
                            exc_info=True,
                        )

                    await redis.xack(ANALYSIS_READY_STREAM_KEY, CALIB_CONSUMER_GROUP, entry_id)

                    log.debug(
                        "BT_ANALYSIS_CALIB_RAW: сообщение stream_id=%s для scenario_id=%s, signal_id=%s "
                        "обработано, строк в bt_position_features_raw записано=%s",
                        entry_id,
                        scenario_id,
                        signal_id,
                        rows_written,
                    )

            log.debug(
                "BT_ANALYSIS_CALIB_RAW: пакет сообщений обработан — сообщений=%s, пар_сценарий_сигнал=%s, "
                "строк_в_bt_position_features_raw=%s",
                total_msgs,
                total_pairs,
                total_rows_written,
            )

        except Exception as e:
            log.error(
                "BT_ANALYSIS_CALIB_RAW: ошибка в основном цикле воркера: %s",
                e,
                exc_info=True,
            )
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима bt:analysis:ready
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=ANALYSIS_READY_STREAM_KEY,
            groupname=CALIB_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_ANALYSIS_CALIB_RAW: создана consumer group '%s' для стрима '%s'",
            CALIB_CONSUMER_GROUP,
            ANALYSIS_READY_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_ANALYSIS_CALIB_RAW: consumer group '%s' для стрима '%s' уже существует",
                CALIB_CONSUMER_GROUP,
                ANALYSIS_READY_STREAM_KEY,
            )
        else:
            log.error(
                "BT_ANALYSIS_CALIB_RAW: ошибка при создании consumer group '%s': %s",
                CALIB_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:analysis:ready
async def _read_from_stream(redis) -> List[Any]:
    entries = await redis.xreadgroup(
        groupname=CALIB_CONSUMER_GROUP,
        consumername=CALIB_CONSUMER_NAME,
        streams={ANALYSIS_READY_STREAM_KEY: ">"},
        count=CALIB_STREAM_BATCH_SIZE,
        block=CALIB_STREAM_BLOCK_MS,
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


# 🔸 Разбор одного сообщения из стрима bt:analysis:ready
def _parse_ready_message(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id_str = fields.get("scenario_id")
        signal_id_str = fields.get("signal_id")
        family_key = fields.get("family_key")
        analysis_ids_str = fields.get("analysis_ids") or ""
        finished_at_str = fields.get("finished_at")
        version = fields.get("version") or "v1"

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

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "family_key": family_key,
            "analysis_ids": analysis_ids,
            "version": version,
            "finished_at": finished_at,
        }
    except Exception as e:
        log.error(
            "BT_ANALYSIS_CALIB_RAW: ошибка разбора сообщения стрима bt:analysis:ready: %s, fields=%s",
            e,
            fields,
            exc_info=True,
        )
        return None


# 🔸 Обработка одного семейства анализаторов для пары scenario_id/signal_id
async def _process_family_raw(
    pg,
    scenario_id: int,
    signal_id: int,
    family_key: str,
    analysis_ids: List[int],
    handler,
) -> int:
    log.debug(
        "BT_ANALYSIS_CALIB_RAW: старт обработки семейства family=%s для scenario_id=%s, signal_id=%s, "
        "analysis_ids=%s",
        family_key,
        scenario_id,
        signal_id,
        analysis_ids,
    )

    # загружаем позиции этого сценария/сигнала, прошедшие постпроцессинг
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                id,
                symbol,
                direction,
                timeframe,
                entry_time,
                raw_stat,
                pnl_abs,
                entry_price,
                sl_price
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
            "BT_ANALYSIS_CALIB_RAW: нет позиций с postproc=true для scenario_id=%s, signal_id=%s",
            scenario_id,
            signal_id,
        )
        return 0

    positions: List[Dict[str, Any]] = []
    for r in rows:
        positions.append(
            {
                "id": r["id"],
                "symbol": r["symbol"],
                "direction": r["direction"],
                "timeframe": r["timeframe"],
                "entry_time": r["entry_time"],
                "raw_stat": r["raw_stat"],
                "pnl_abs": r["pnl_abs"],
                "entry_price": float(r["entry_price"]) if r["entry_price"] is not None else None,
                "sl_price": float(r["sl_price"]) if r["sl_price"] is not None else None,
            }
        )

    # 🔸 Полная очистка сырых фич для этой связки и семейства
    async with pg.acquire() as conn:
        await conn.execute(
            """
            DELETE FROM bt_position_features_raw
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND family_key  = $3
            """,
            scenario_id,
            signal_id,
            family_key,
        )

    # вызываем конкретный калибровщик семейства
    rows_written = await handler(
        pg=pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        analysis_ids=analysis_ids,
        positions=positions,
    )

    log.info(
        "BT_ANALYSIS_CALIB_RAW: семейство=%s, scenario_id=%s, signal_id=%s — сырых строк записано=%s",
        family_key,
        scenario_id,
        signal_id,
        rows_written,
    )

    return rows_written