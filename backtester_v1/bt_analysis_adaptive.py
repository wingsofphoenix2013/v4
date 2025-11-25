# bt_analysis_adaptive.py — адаптивный анализатор фич (versioned bins) для backtester_v1

import asyncio
import logging
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Any, Dict, List, Tuple, Optional

# 🔸 Кеши backtester_v1
from backtester_config import get_scenario_instance, get_analysis_instance

# 🔸 Утилиты анализа фич
from bt_analysis_utils import resolve_feature_name

# 🔸 Настройки Decimal
getcontext().prec = 28

log = logging.getLogger("BT_ANALYSIS_ADAPTIVE")

# 🔸 Константы стримов
ADAPTIVE_READY_STREAM_KEY = "bt:analysis:adaptive:ready"
ANALYSIS_READY_STREAM_KEY = "bt:analysis:ready"
ADAPTIVE_CONSUMER_GROUP = "bt_analysis_adaptive"
ADAPTIVE_CONSUMER_NAME = "bt_analysis_adaptive_main"

# 🔸 Настройки чтения стрима bt:analysis:adaptive:ready
ADAPTIVE_STREAM_BATCH_SIZE = 10
ADAPTIVE_STREAM_BLOCK_MS = 5000

# 🔸 Поддерживаемые семейства для адаптивного анализа
SUPPORTED_FAMILIES_ADAPTIVE = {"rsi", "adx"}


# 🔸 Квантование чисел до 4 знаков
def _q4(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


# 🔸 Безопасное деление
def _safe_div(n: Decimal, d: Decimal) -> Decimal:
    if d == 0:
        return Decimal("0")
    return n / d


# 🔸 Поиск бина для значения feature_value в списке [from_value, to_value)
def _find_bin_for_value(
    v: float,
    bins: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not bins:
        return None

    # предполагаем, что bins отсортированы по order_index
    last_bin = bins[-1]
    for b in bins:
        b_from = b["from_value"]
        b_to = b["to_value"]

        # для всех, кроме последнего, используем [from, to)
        if b is not last_bin:
            if b_from <= v < b_to:
                return b
        else:
            # последний бин считаем [from, to] включительно
            if b_from <= v <= b_to:
                return b

    return None


# 🔸 Публичная точка входа: воркер адаптивного анализа (слушает bt:analysis:adaptive:ready)
async def run_bt_analysis_adaptive_worker(pg, redis):
    log.debug("BT_ANALYSIS_ADAPTIVE: воркер адаптивного анализа запущен")

    # подготавливаем consumer group для стрима bt:analysis:adaptive:ready
    await _ensure_consumer_group(redis)

    while True:
        try:
            messages = await _read_from_stream(redis)

            if not messages:
                continue

            total_msgs = 0
            total_pairs = 0

            for stream_key, entries in messages:
                if stream_key != ADAPTIVE_READY_STREAM_KEY:
                    continue

                for entry_id, fields in entries:
                    total_msgs += 1

                    ctx = _parse_adaptive_message(fields)
                    if not ctx:
                        await redis.xack(ADAPTIVE_READY_STREAM_KEY, ADAPTIVE_CONSUMER_GROUP, entry_id)
                        continue

                    scenario_id = ctx["scenario_id"]
                    signal_id = ctx["signal_id"]
                    family_key = ctx["family_key"]
                    analysis_ids = ctx["analysis_ids"]
                    version = ctx["version"]

                    log.debug(
                        "BT_ANALYSIS_ADAPTIVE: получено сообщение о готовности бин-конфигов "
                        "scenario_id=%s, signal_id=%s, family=%s, version=%s, analysis_ids=%s, stream_id=%s",
                        scenario_id,
                        signal_id,
                        family_key,
                        version,
                        analysis_ids,
                        entry_id,
                    )

                    # работаем только с поддерживаемыми семействами и непустым списком анализаторов
                    if family_key not in SUPPORTED_FAMILIES_ADAPTIVE or not analysis_ids:
                        await redis.xack(ADAPTIVE_READY_STREAM_KEY, ADAPTIVE_CONSUMER_GROUP, entry_id)
                        continue

                    # собираем analysis_instances по analysis_ids
                    analysis_instances: List[Dict[str, Any]] = []
                    for aid in analysis_ids:
                        inst = get_analysis_instance(aid)
                        if not inst:
                            log.warning(
                                "BT_ANALYSIS_ADAPTIVE: analysis_id=%s не найден в кеше, scenario_id=%s, signal_id=%s",
                                aid,
                                scenario_id,
                                signal_id,
                            )
                            continue
                        if inst.get("family_key") != family_key:
                            continue
                        analysis_instances.append(inst)

                    if not analysis_instances:
                        log.debug(
                            "BT_ANALYSIS_ADAPTIVE: нет валидных инстансов анализа для "
                            "scenario_id=%s, signal_id=%s, family=%s",
                            scenario_id,
                            signal_id,
                            family_key,
                        )
                        await redis.xack(ADAPTIVE_READY_STREAM_KEY, ADAPTIVE_CONSUMER_GROUP, entry_id)
                        continue

                    # запускаем адаптивный анализ по этой связке
                    await run_analysis_adaptive(
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                        family_key=family_key,
                        analysis_instances=analysis_instances,
                        version=version,
                        pg=pg,
                        redis=redis,
                    )
                    total_pairs += 1

                    # публикуем событие готовности адаптивного анализа в bt:analysis:ready
                    finished_at = datetime.utcnow()
                    try:
                        await redis.xadd(
                            ANALYSIS_READY_STREAM_KEY,
                            {
                                "scenario_id": str(scenario_id),
                                "signal_id": str(signal_id),
                                "family_key": str(family_key),
                                "analysis_ids": ",".join(str(a) for a in analysis_ids),
                                "version": str(version),
                                "finished_at": finished_at.isoformat(),
                            },
                        )
                        log.debug(
                            "BT_ANALYSIS_ADAPTIVE: опубликовано событие в '%s' для scenario_id=%s, signal_id=%s, "
                            "family=%s, version=%s, analysis_ids=%s, finished_at=%s",
                            ANALYSIS_READY_STREAM_KEY,
                            scenario_id,
                            signal_id,
                            family_key,
                            version,
                            analysis_ids,
                            finished_at,
                        )
                    except Exception as e:
                        log.error(
                            "BT_ANALYSIS_ADAPTIVE: не удалось опубликовать событие в стрим '%s' "
                            "для scenario_id=%s, signal_id=%s, family=%s, version=%s: %s",
                            ANALYSIS_READY_STREAM_KEY,
                            scenario_id,
                            signal_id,
                            family_key,
                            version,
                            e,
                            exc_info=True,
                        )

                    await redis.xack(ADAPTIVE_READY_STREAM_KEY, ADAPTIVE_CONSUMER_GROUP, entry_id)

            log.debug(
                "BT_ANALYSIS_ADAPTIVE: пакет сообщений обработан — сообщений=%s, пар_сценарий_сигнал=%s",
                total_msgs,
                total_pairs,
            )

        except Exception as e:
            log.error(
                "BT_ANALYSIS_ADAPTIVE: ошибка в основном цикле воркера: %s",
                e,
                exc_info=True,
            )
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима bt:analysis:adaptive:ready
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=ADAPTIVE_READY_STREAM_KEY,
            groupname=ADAPTIVE_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_ANALYSIS_ADAPTIVE: создана consumer group '%s' для стрима '%s'",
            ADAPTIVE_CONSUMER_GROUP,
            ADAPTIVE_READY_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_ANALYSIS_ADAPTIVE: consumer group '%s' для стрима '%s' уже существует",
                ADAPTIVE_CONSUMER_GROUP,
                ADAPTIVE_READY_STREAM_KEY,
            )
        else:
            log.error(
                "BT_ANALYSIS_ADAPTIVE: ошибка при создании consumer group '%s': %s",
                ADAPTIVE_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:analysis:adaptive:ready
async def _read_from_stream(redis) -> List[Any]:
    entries = await redis.xreadgroup(
        groupname=ADAPTIVE_CONSUMER_GROUP,
        consumername=ADAPTIVE_CONSUMER_NAME,
        streams={ADAPTIVE_READY_STREAM_KEY: ">"},
        count=ADAPTIVE_STREAM_BATCH_SIZE,
        block=ADAPTIVE_STREAM_BLOCK_MS,
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


# 🔸 Разбор одного сообщения из стрима bt:analysis:adaptive:ready
def _parse_adaptive_message(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id_str = fields.get("scenario_id")
        signal_id_str = fields.get("signal_id")
        family_key = fields.get("family_key")
        analysis_ids_str = fields.get("analysis_ids") or ""
        version = fields.get("version") or "v2"
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
            "BT_ANALYSIS_ADAPTIVE: ошибка разбора сообщения стрима bt:analysis:adaptive:ready: %s, fields=%s",
            e,
            fields,
            exc_info=True,
        )
        return None


# 🔸 Адаптивный анализ для одного сценария+сигнала и набора анализаторов
async def run_analysis_adaptive(
    scenario_id: int,
    signal_id: int,
    family_key: str,
    analysis_instances: List[Dict[str, Any]],
    version: str,
    pg,
    redis,  # оставляем для совместимости сигнатур, но здесь не используем
) -> None:
    log.debug(
        "BT_ANALYSIS_ADAPTIVE: старт адаптивного анализа для scenario_id=%s, signal_id=%s, "
        "family=%s, инстансов=%s, version=%s",
        scenario_id,
        signal_id,
        family_key,
        len(analysis_instances),
        version,
    )

    if not analysis_instances:
        log.debug(
            "BT_ANALYSIS_ADAPTIVE: для scenario_id=%s, signal_id=%s, family=%s нет инстансов анализа",
            scenario_id,
            signal_id,
            family_key,
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

    # обрабатываем каждый инстанс анализа независимо
    for inst in analysis_instances:
        inst_family = inst.get("family_key")
        key = inst.get("key")
        inst_id = inst.get("id")
        params = inst.get("params") or {}

        if inst_family != family_key:
            continue

        tf_cfg = params.get("timeframe")
        source_cfg = params.get("source_key")

        timeframe = str(tf_cfg.get("value")).strip() if tf_cfg is not None else "m5"
        source_key = str(source_cfg.get("value")).strip() if source_cfg is not None else "rsi14"

        feature_name = resolve_feature_name(
            family_key=family_key,
            key=key,
            timeframe=timeframe,
            source_key=source_key,
        )

        log.debug(
            "BT_ANALYSIS_ADAPTIVE: inst_id=%s — анализ key=%s, timeframe=%s, source_key=%s, "
            "feature_name=%s, version=%s",
            inst_id,
            key,
            timeframe,
            source_key,
            feature_name,
            version,
        )

        # загружаем адаптивные бины для этой фичи/версии
        async with pg.acquire() as conn:
            bin_rows = await conn.fetch(
                """
                SELECT bin_label,
                       from_value,
                       to_value,
                       order_index
                FROM bt_position_features_bins
                WHERE family_key = $1
                  AND key        = $2
                  AND timeframe  = $3
                  AND source_key = $4
                  AND version    = $5
                ORDER BY order_index
                """,
                family_key,
                key,
                timeframe,
                source_key,
                version,
            )

        if not bin_rows:
            log.debug(
                "BT_ANALYSIS_ADAPTIVE: inst_id=%s, feature_name=%s — нет адаптивных бинов для version=%s, "
                "scenario_id=%s, signal_id=%s, family=%s",
                inst_id,
                feature_name,
                version,
                scenario_id,
                signal_id,
                family_key,
            )
            continue

        bins_cfg: List[Dict[str, Any]] = []
        for r in bin_rows:
            bins_cfg.append(
                {
                    "bin_label": r["bin_label"],
                    "from_value": float(r["from_value"]),
                    "to_value": float(r["to_value"]),
                    "order_index": int(r["order_index"]),
                }
            )

        # загружаем сырые значения фичи для этого сценария/сигнала
        async with pg.acquire() as conn:
            raw_rows = await conn.fetch(
                """
                SELECT
                    direction,
                    feature_value,
                    pnl_abs,
                    is_win
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

        if not raw_rows:
            log.debug(
                "BT_ANALYSIS_ADAPTIVE: inst_id=%s, feature_name=%s — нет сырых значений для "
                "scenario_id=%s, signal_id=%s, family=%s",
                inst_id,
                feature_name,
                scenario_id,
                signal_id,
                family_key,
            )
            continue

        # агрегаты по (direction, bin_label)
        agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

        for r in raw_rows:
            direction = r["direction"]
            try:
                v = float(r["feature_value"])
            except Exception:
                continue

            pnl_abs_raw = r["pnl_abs"]
            is_win = bool(r["is_win"])

            if direction is None or pnl_abs_raw is None:
                continue

            try:
                pnl_abs = Decimal(str(pnl_abs_raw))
            except Exception:
                continue

            # находим бин
            b = _find_bin_for_value(v, bins_cfg)
            if b is None:
                continue

            bin_label = b["bin_label"]
            key_tuple = (direction, bin_label)

            bin_stat = agg.get(key_tuple)
            if bin_stat is None:
                bin_stat = {
                    "bin_from": b["from_value"],
                    "bin_to": b["to_value"],
                    "trades": 0,
                    "wins": 0,
                    "losses": 0,
                    "pnl_abs_total": Decimal("0"),
                }
                agg[key_tuple] = bin_stat

            bin_stat["trades"] += 1
            if is_win:
                bin_stat["wins"] += 1
            elif pnl_abs < 0:
                bin_stat["losses"] += 1
            bin_stat["pnl_abs_total"] += pnl_abs

        if not agg:
            log.debug(
                "BT_ANALYSIS_ADAPTIVE: inst_id=%s, feature_name=%s, version=%s, family=%s — нет данных "
                "для записи (agg пустой)",
                inst_id,
                feature_name,
                version,
                family_key,
            )
            # на всякий случай чистим старые бины по этой фиче/TF/версии
            async with pg.acquire() as conn:
                await conn.execute(
                    """
                    DELETE FROM bt_scenario_feature_bins
                    WHERE scenario_id  = $1
                      AND signal_id    = $2
                      AND timeframe    = $3
                      AND feature_name = $4
                      AND version      = $5
                    """,
                    scenario_id,
                    signal_id,
                    timeframe,
                    feature_name,
                    version,
                )
            continue

        # готовим строки для вставки
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
                    version,                     # version
                )
            )

        # записываем в bt_scenario_feature_bins с заданной версией (обычно v2)
        async with pg.acquire() as conn:
            # сначала удаляем старые бины по этой фиче/TF/версии для данного сценария/сигнала
            await conn.execute(
                """
                DELETE FROM bt_scenario_feature_bins
                WHERE scenario_id  = $1
                  AND signal_id    = $2
                  AND timeframe    = $3
                  AND feature_name = $4
                  AND version      = $5
                """,
                scenario_id,
                signal_id,
                timeframe,
                feature_name,
                version,
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
                        roi,
                        version
                    )
                    VALUES (
                        $1, $2, $3, $4, $5,
                        $6, $7, $8, $9, $10,
                        $11, $12, $13, $14,
                        $15
                    )
                    """,
                    rows_to_insert,
                )

        log.debug(
            "BT_ANALYSIS_ADAPTIVE: inst_id=%s, feature_name=%s, timeframe=%s, version=%s, family=%s — бинов записано=%s",
            inst_id,
            feature_name,
            timeframe,
            version,
            family_key,
            len(rows_to_insert),
        )

    log.debug(
        "BT_ANALYSIS_ADAPTIVE: адаптивный анализ завершён для scenario_id=%s, signal_id=%s, "
        "family=%s, version=%s",
        scenario_id,
        signal_id,
        family_key,
        version,
    )