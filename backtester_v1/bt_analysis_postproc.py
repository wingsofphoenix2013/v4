# bt_analysis_postproc.py — постпроцессор анализа фич (оценка силы анализаторов)

import asyncio
import logging
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Any, Dict, List, Optional

# 🔸 Кеши backtester_v1 (анализаторы и сценарии)
from backtester_config import get_analysis_instance, get_scenario_instance

# 🔸 Утилиты анализа фич
from bt_analysis_utils import resolve_feature_name

# 🔸 Настройки Decimal
getcontext().prec = 28

log = logging.getLogger("BT_ANALYSIS_POSTPROC")

# 🔸 Константы стримов готовности анализа
ANALYSIS_READY_STREAM_KEY = "bt:analysis:ready"
ANALYSIS_POSTPROC_READY_STREAM_KEY = "bt:analysis:postproc:ready"
ANALYSIS_POSTPROC_CONSUMER_GROUP = "bt_analysis_postproc"
ANALYSIS_POSTPROC_CONSUMER_NAME = "bt_analysis_postproc_main"

# 🔸 Настройки чтения стрима bt:analysis:ready
ANALYSIS_POSTPROC_STREAM_BATCH_SIZE = 10
ANALYSIS_POSTPROC_STREAM_BLOCK_MS = 5000

# 🔸 Параметры отбора бинов (тюнингуются при необходимости)
MIN_COVERAGE = Decimal("0.20")              # минимальная доля сделок (20% от базовых)
MIN_WINRATE_IMPROVEMENT = Decimal("0.01")   # минимальное улучшение winrate (1%)

# 🔸 Поддерживаемые семейства анализаторов
SUPPORTED_FAMILIES = {"rsi", "adx", "ema", "atr"}

# 🔸 Квантование до 4 знаков
def _q4(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


# 🔸 Безопасное деление
def _safe_div(n: Decimal, d: Decimal) -> Decimal:
    if d == 0:
        return Decimal("0")
    return n / d


# 🔸 Публичная точка входа: оркестратор пост-анализа bt_scenario_feature_bins
async def run_bt_analysis_postproc(pg, redis):
    log.info("BT_ANALYSIS_POSTPROC: воркер пост-анализа запущен")

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
            total_stats_written = 0

            for stream_key, entries in messages:
                if stream_key != ANALYSIS_READY_STREAM_KEY:
                    # защищаемся от чужих стримов
                    continue

                for entry_id, fields in entries:
                    total_msgs += 1

                    ctx = _parse_ready_message(fields)
                    if not ctx:
                        # не удалось корректно распарсить сообщение — ACK и пропускаем
                        await redis.xack(ANALYSIS_READY_STREAM_KEY, ANALYSIS_POSTPROC_CONSUMER_GROUP, entry_id)
                        continue

                    scenario_id = ctx["scenario_id"]
                    signal_id = ctx["signal_id"]
                    family_key = ctx["family_key"]
                    analysis_ids = ctx["analysis_ids"]
                    version = ctx["version"]

                    log.info(
                        "BT_ANALYSIS_POSTPROC: получено сообщение о готовности анализа "
                        "scenario_id=%s, signal_id=%s, family=%s, version=%s, analysis_ids=%s, stream_id=%s",
                        scenario_id,
                        signal_id,
                        family_key,
                        version,
                        analysis_ids,
                        entry_id,
                    )

                    # пост-анализ только для известных семейств
                    if family_key not in SUPPORTED_FAMILIES:
                        log.debug(
                            "BT_ANALYSIS_POSTPROC: family_key=%s пока не поддерживается, "
                            "scenario_id=%s, signal_id=%s",
                            family_key,
                            scenario_id,
                            signal_id,
                        )
                        await redis.xack(ANALYSIS_READY_STREAM_KEY, ANALYSIS_POSTPROC_CONSUMER_GROUP, entry_id)
                        continue

                    if not analysis_ids:
                        log.debug(
                            "BT_ANALYSIS_POSTPROC: для scenario_id=%s, signal_id=%s, family=%s нет analysis_ids",
                            scenario_id,
                            signal_id,
                            family_key,
                        )
                        await redis.xack(ANALYSIS_READY_STREAM_KEY, ANALYSIS_POSTPROC_CONSUMER_GROUP, entry_id)
                        continue

                    # выполняем пост-анализ для связки scenario+signal по всем указанным анализаторам семьи и версии
                    stats_written = await _process_analysis_family(
                        pg=pg,
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                        family_key=family_key,
                        analysis_ids=analysis_ids,
                        version=version,
                    )
                    total_pairs += 1
                    total_stats_written += stats_written

                    # публикуем событие о завершении пост-анализа в bt:analysis:postproc:ready
                    finished_at_postproc = datetime.utcnow()
                    try:
                        await redis.xadd(
                            ANALYSIS_POSTPROC_READY_STREAM_KEY,
                            {
                                "scenario_id": str(scenario_id),
                                "signal_id": str(signal_id),
                                "family_key": str(family_key),
                                "analysis_ids": ",".join(str(a) for a in analysis_ids),
                                "version": str(version),
                                "stats_written": str(stats_written),
                                "finished_at": finished_at_postproc.isoformat(),
                            },
                        )
                        log.info(
                            "BT_ANALYSIS_POSTPROC: опубликовано событие готовности пост-анализа в стрим '%s' "
                            "для scenario_id=%s, signal_id=%s, family=%s, version=%s, analysis_ids=%s, "
                            "stats_written=%s, finished_at=%s",
                            ANALYSIS_POSTPROC_READY_STREAM_KEY,
                            scenario_id,
                            signal_id,
                            family_key,
                            version,
                            analysis_ids,
                            stats_written,
                            finished_at_postproc,
                        )
                    except Exception as e:
                        log.error(
                            "BT_ANALYSIS_POSTPROC: не удалось опубликовать событие в стрим '%s' "
                            "для scenario_id=%s, signal_id=%s, family=%s, version=%s: %s",
                            ANALYSIS_POSTPROC_READY_STREAM_KEY,
                            scenario_id,
                            signal_id,
                            family_key,
                            version,
                            e,
                            exc_info=True,
                        )

                    # помечаем сообщение как обработанное
                    await redis.xack(ANALYSIS_READY_STREAM_KEY, ANALYSIS_POSTPROC_CONSUMER_GROUP, entry_id)

                    log.info(
                        "BT_ANALYSIS_POSTPROC: сообщение stream_id=%s для scenario_id=%s, signal_id=%s, version=%s "
                        "обработано, записано строк в bt_analysis_stat=%s",
                        entry_id,
                        scenario_id,
                        signal_id,
                        version,
                        stats_written,
                    )

            log.info(
                "BT_ANALYSIS_POSTPROC: пакет сообщений обработан — сообщений=%s, пар_сценарий_сигнал=%s, "
                "строк_в_bt_analysis_stat=%s",
                total_msgs,
                total_pairs,
                total_stats_written,
            )

        except Exception as e:
            log.error(
                "BT_ANALYSIS_POSTPROC: ошибка в основном цикле воркера: %s",
                e,
                exc_info=True,
            )
            # небольшая пауза перед повторной попыткой, чтобы не крутить CPU при постоянной ошибке
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима bt:analysis:ready
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=ANALYSIS_READY_STREAM_KEY,
            groupname=ANALYSIS_POSTPROC_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_ANALYSIS_POSTPROC: создана consumer group '%s' для стрима '%s'",
            ANALYSIS_POSTPROC_CONSUMER_GROUP,
            ANALYSIS_READY_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.info(
                "BT_ANALYSIS_POSTPROC: consumer group '%s' для стрима '%s' уже существует",
                ANALYSIS_POSTPROC_CONSUMER_GROUP,
                ANALYSIS_READY_STREAM_KEY,
            )
        else:
            log.error(
                "BT_ANALYSIS_POSTPROC: ошибка при создании consumer group '%s': %s",
                ANALYSIS_POSTPROC_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:analysis:ready
async def _read_from_stream(redis) -> List[Any]:
    entries = await redis.xreadgroup(
        groupname=ANALYSIS_POSTPROC_CONSUMER_GROUP,
        consumername=ANALYSIS_POSTPROC_CONSUMER_NAME,
        streams={ANALYSIS_READY_STREAM_KEY: ">"},
        count=ANALYSIS_POSTPROC_STREAM_BATCH_SIZE,
        block=ANALYSIS_POSTPROC_STREAM_BLOCK_MS,
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
        # если версия не передана (старый v1-пайплайн) — считаем версию v1
        version = fields.get("version") or "v1"

        if not (scenario_id_str and signal_id_str and family_key and finished_at_str):
            return None

        scenario_id = int(scenario_id_str)
        signal_id = int(signal_id_str)
        finished_at = datetime.fromisoformat(finished_at_str)

        # парсим список id анализаторов из строки через запятую
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
            "BT_ANALYSIS_POSTPROC: ошибка разбора сообщения стрима bt:analysis:ready: %s, fields=%s",
            e,
            fields,
            exc_info=True,
        )
        return None


# 🔸 Пост-анализ одного семейства анализаторов для пары scenario_id/signal_id
async def _process_analysis_family(
    pg,
    scenario_id: int,
    signal_id: int,
    family_key: str,
    analysis_ids: List[int],
    version: str,
) -> int:
    stats_written = 0

    # загружаем базовую статистику по сценарию+сигналу для всех направлений
    async with pg.acquire() as conn:
        base_rows = await conn.fetch(
            """
            SELECT direction, trades, winrate, pnl_abs
            FROM bt_scenario_stat
            WHERE scenario_id = $1
              AND signal_id   = $2
            """,
            scenario_id,
            signal_id,
        )

    if not base_rows:
        log.debug(
            "BT_ANALYSIS_POSTPROC: нет базовой статистики в bt_scenario_stat для scenario_id=%s, signal_id=%s",
            scenario_id,
            signal_id,
        )
        return 0

    # приводим базовую статистику к dict по direction
    base_by_dir: Dict[str, Dict[str, Any]] = {}
    for r in base_rows:
        direction = r["direction"]
        if direction is None:
            continue
        base_by_dir[direction] = {
            "trades": int(r["trades"]),
            "winrate": Decimal(str(r["winrate"])),
            "pnl_abs": Decimal(str(r["pnl_abs"])),
        }

    if not base_by_dir:
        return 0

    # загружаем депозит сценария для расчёта ROI
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

    if deposit is None or deposit <= 0:
        log.error(
            "BT_ANALYSIS_POSTPROC: не удалось получить корректный deposit для scenario_id=%s, "
            "ROI base/selected будет равен 0",
            scenario_id,
        )
        deposit = None

    # 🔸 Полная очистка старых записей для этой связки/версии и этих анализаторов
    async with pg.acquire() as conn:
        await conn.execute(
            """
            DELETE FROM bt_analysis_stat
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND analysis_id = ANY($3::int[])
              AND version     = $4
            """,
            scenario_id,
            signal_id,
            analysis_ids,
            version,
        )

    # обрабатываем каждый анализатор по каждому направлению отдельно
    for aid in analysis_ids:
        inst = get_analysis_instance(aid)
        if not inst:
            log.warning(
                "BT_ANALYSIS_POSTPROC: analysis_id=%s не найден в кеше, scenario_id=%s, signal_id=%s",
                aid,
                scenario_id,
                signal_id,
            )
            continue

        inst_family = inst.get("family_key")
        key = inst.get("key")
        params = inst.get("params") or {}

        if inst_family != family_key:
            # защитный фильтр
            continue

        # определяем timeframe и feature_name так же, как в bt_analysis_rsi
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
            "BT_ANALYSIS_POSTPROC: анализ postproc для analysis_id=%s, family=%s, key=%s, "
            "feature_name=%s, timeframe=%s, version=%s, scenario_id=%s, signal_id=%s",
            aid,
            family_key,
            key,
            feature_name,
            timeframe,
            version,
            scenario_id,
            signal_id,
        )

        # по каждому направлению считаем uplift, coverage и ROI
        for direction, base_stat in base_by_dir.items():
            base_trades = int(base_stat["trades"])
            base_winrate = base_stat["winrate"]
            base_pnl_abs = base_stat["pnl_abs"]

            # если нет базовых сделок — смысла нет
            if base_trades <= 0:
                continue

            # базовый ROI (по направлению) относительно депозита сценария
            if deposit is not None and deposit != 0:
                base_roi = _safe_div(base_pnl_abs, deposit)
            else:
                base_roi = Decimal("0")

            # загружаем все бины для этой фичи/TF/направления/версии
            async with pg.acquire() as conn:
                bin_rows = await conn.fetch(
                    """
                    SELECT trades, wins, winrate, pnl_abs_total
                    FROM bt_scenario_feature_bins
                    WHERE scenario_id  = $1
                      AND signal_id    = $2
                      AND direction    = $3
                      AND timeframe    = $4
                      AND feature_name = $5
                      AND version      = $6
                    """,
                    scenario_id,
                    signal_id,
                    direction,
                    timeframe,
                    feature_name,
                    version,
                )

            if not bin_rows:
                continue

            # отбор бинов, winrate которых выше базовой линии + порог улучшения
            selected_trades = 0
            selected_wins = 0
            selected_pnl_abs_total = Decimal("0")

            for r in bin_rows:
                bin_trades = int(r["trades"])
                bin_wins = int(r["wins"])
                bin_winrate = Decimal(str(r["winrate"]))
                bin_pnl_abs_total = Decimal(str(r["pnl_abs_total"]))

                # условия достаточности и улучшения
                if bin_trades <= 0:
                    continue

                # улучшение winrate как минимум на MIN_WINRATE_IMPROVEMENT
                if bin_winrate < base_winrate + MIN_WINRATE_IMPROVEMENT:
                    continue

                selected_trades += bin_trades
                selected_wins += bin_wins
                selected_pnl_abs_total += bin_pnl_abs_total

            if selected_trades <= 0:
                # нет бинов, которые дают заметное улучшение
                continue

            # coverage: доля сделок, попавших в отобранные бины
            coverage = _safe_div(Decimal(selected_trades), Decimal(base_trades))
            if coverage < MIN_COVERAGE:
                # фильтр слишком узкий — мало сделок
                log.debug(
                    "BT_ANALYSIS_POSTPROC: analysis_id=%s, feature=%s, direction=%s, version=%s — "
                    "coverage=%.4f < %.4f (selected_trades=%s, base_trades=%s)",
                    aid,
                    feature_name,
                    direction,
                    version,
                    float(coverage),
                    float(MIN_COVERAGE),
                    selected_trades,
                    base_trades,
                )
                continue

            # считаем winrate по отобранным
            selected_winrate = _safe_div(Decimal(selected_wins), Decimal(selected_trades))

            # считаем ROI по отобранным бинам
            if deposit is not None and deposit != 0:
                selected_roi = _safe_div(selected_pnl_abs_total, deposit)
            else:
                selected_roi = Decimal("0")

            # пока окна анализа не реализованы — оставляем analysis_window = None (NULL в БД)
            analysis_window: Optional[str] = None

            # пишем строку в bt_analysis_stat (старые мы уже удалили выше)
            async with pg.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO bt_analysis_stat (
                        scenario_id,
                        signal_id,
                        analysis_id,
                        family_key,
                        key,
                        direction,
                        timeframe,
                        base_trades,
                        base_winrate,
                        selected_trades,
                        selected_winrate,
                        base_roi,
                        selected_roi,
                        version,
                        analysis_window,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        $1, $2, $3, $4, $5,
                        $6, $7, $8, $9, $10, $11,
                        $12, $13, $14, $15,
                        now(), NULL
                    )
                    """,
                    scenario_id,
                    signal_id,
                    aid,
                    inst_family,
                    key,
                    direction,
                    timeframe,
                    base_trades,
                    _q4(base_winrate),
                    selected_trades,
                    _q4(selected_winrate),
                    _q4(base_roi),
                    _q4(selected_roi),
                    version,
                    analysis_window,
                )

            stats_written += 1

            log.debug(
                "BT_ANALYSIS_POSTPROC: записана строка в bt_analysis_stat: "
                "scenario_id=%s, signal_id=%s, analysis_id=%s, direction=%s, timeframe=%s, version=%s, "
                "base_trades=%s, base_winrate=%.4f, base_roi=%.4f, "
                "selected_trades=%s, selected_winrate=%.4f, selected_roi=%.4f, coverage=%.4f",
                scenario_id,
                signal_id,
                aid,
                direction,
                timeframe,
                version,
                base_trades,
                float(base_winrate),
                float(base_roi),
                selected_trades,
                float(selected_winrate),
                float(selected_roi),
                float(coverage),
            )

    return stats_written