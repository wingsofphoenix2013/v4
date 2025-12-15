# bt_analysis_main.py — оркестратор анализаторов backtester_v1

import asyncio
import logging
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any, List, Optional, Callable, Awaitable, Tuple

# 🔸 Конфиг и кеши backtester_v1
from backtester_config import (
    get_analysis_connections_for_scenario_signal,
    get_analysis_instance,
)

# 🔸 Тип обработчика анализатора:
#    (analysis_cfg, analysis_ctx, pg_pool, redis_client) -> {"rows": [...], "summary": {...}}
AnalysisHandler = Callable[
    [Dict[str, Any], Dict[str, Any], Any, Any],
    Awaitable[Dict[str, Any]]
]

# 🔸 Воркеры анализаторов (из пакета analysis/)
from analysis.bt_analysis_rsi_bin import run_rsi_bin_analysis
from analysis.bt_analysis_mfi_bin import run_mfi_bin_analysis
from analysis.bt_analysis_adx_bin import run_adx_bin_analysis
from analysis.bt_analysis_bb_band_bin import run_bb_band_bin_analysis
from analysis.bt_analysis_lr_band_bin import run_lr_band_bin_analysis
from analysis.bt_analysis_lr_angle_bin import run_lr_angle_bin_analysis
from analysis.bt_analysis_atr_bin import run_atr_bin_analysis
from analysis.bt_analysis_dmigap_bin import run_dmigap_bin_analysis
from analysis.bt_analysis_supertrend_bin import run_supertrend_bin_analysis
from analysis.bt_analysis_lr_angle_mtf import run_lr_angle_mtf_analysis
from analysis.bt_analysis_rsimfi_mtf import run_rsimfi_mtf_analysis
from analysis.bt_analysis_rsi_mtf import run_rsi_mtf_analysis
from analysis.bt_analysis_mfi_mtf import run_mfi_mtf_analysis
from analysis.bt_analysis_lr_mtf import run_lr_mtf_analysis
from analysis.bt_analysis_bb_mtf import run_bb_mtf_analysis

# 🔸 Реестр анализаторов: (family_key, key) → handler
ANALYSIS_HANDLERS: Dict[Tuple[str, str], AnalysisHandler] = {
    ("rsi", "rsi_bin"): run_rsi_bin_analysis,
    ("mfi", "mfi_bin"): run_mfi_bin_analysis,
    ("adx_dmi", "adx_bin"): run_adx_bin_analysis,
    ("bb", "bb_band_bin"): run_bb_band_bin_analysis,
    ("lr", "lr_band_bin"): run_lr_band_bin_analysis,
    ("lr", "lr_angle_bin"): run_lr_angle_bin_analysis,
    ("atr", "atr_bin"): run_atr_bin_analysis,
    ("adx_dmi", "dmigap_bin"): run_dmigap_bin_analysis,
    ("supertrend", "supertrend_bin"): run_supertrend_bin_analysis,
    ("lr", "lr_angle_mtf"): run_lr_angle_mtf_analysis,
    ("rsimfi", "rsimfi_mtf"): run_rsimfi_mtf_analysis,
    ("rsi", "rsi_mtf"): run_rsi_mtf_analysis,
    ("mfi", "mfi_mtf"): run_mfi_mtf_analysis,
    ("lr", "lr_mtf"): run_lr_mtf_analysis,
    ("bb", "bb_mtf"): run_bb_mtf_analysis,
}

# 🔸 Константы стрима анализа
ANALYSIS_STREAM_KEY = "bt:postproc:ready"
ANALYSIS_CONSUMER_GROUP = "bt_analysis"
ANALYSIS_CONSUMER_NAME = "bt_analysis_main"

# 🔸 Стрим готовности анализа
ANALYSIS_READY_STREAM_KEY = "bt:analysis:ready"

# 🔸 Настройки чтения стрима
ANALYSIS_STREAM_BATCH_SIZE = 10
ANALYSIS_STREAM_BLOCK_MS = 5000

# 🔸 Ограничение параллелизма анализаторов
ANALYSIS_MAX_CONCURRENCY = 12

# 🔸 Кеш последних finished_at по (scenario_id, signal_id) для отсечки дублей
_last_postproc_finished_at: Dict[Tuple[int, int], datetime] = {}

log = logging.getLogger("BT_ANALYSIS_MAIN")


# 🔸 Публичная точка входа: оркестратор анализаторов
async def run_bt_analysis_orchestrator(pg, redis):
    log.debug("BT_ANALYSIS_MAIN: оркестратор анализаторов запущен")

    await _ensure_consumer_group(redis)

    # общий семафор для всех анализаторов
    analysis_sema = asyncio.Semaphore(ANALYSIS_MAX_CONCURRENCY)

    # основной цикл чтения стрима и запуска анализаторов
    while True:
        try:
            messages = await _read_from_stream(redis)

            if not messages:
                continue

            total_msgs = 0
            total_pairs = 0
            total_analyses_planned = 0
            total_analyses_ok = 0
            total_analyses_failed = 0
            total_rows_inserted = 0
            total_bins_rows = 0

            # сводка по очисткам
            total_cleanup_raw = 0
            total_cleanup_bins = 0
            total_cleanup_model = 0
            total_cleanup_labels = 0
            total_cleanup_postproc = 0
            total_cleanup_scenario_stat = 0
            total_cleanup_total = 0

            for stream_key, entries in messages:
                if stream_key != ANALYSIS_STREAM_KEY:
                    # на всякий случай игнорируем чужие стримы
                    continue

                for entry_id, fields in entries:
                    total_msgs += 1

                    ctx = _parse_postproc_message(fields)
                    if not ctx:
                        # если не удалось корректно распарсить поля — ACK и пропускаем
                        await redis.xack(ANALYSIS_STREAM_KEY, ANALYSIS_CONSUMER_GROUP, entry_id)
                        continue

                    scenario_id = ctx["scenario_id"]
                    signal_id = ctx["signal_id"]
                    finished_at = ctx["finished_at"]

                    pair_key = (scenario_id, signal_id)
                    last_finished = _last_postproc_finished_at.get(pair_key)

                    # отсечка дублей по равному finished_at
                    if last_finished is not None and last_finished == finished_at:
                        log.debug(
                            "BT_ANALYSIS_MAIN: дубликат сообщения для scenario_id=%s, signal_id=%s, "
                            "finished_at=%s, stream_id=%s — анализаторы не запускаются",
                            scenario_id,
                            signal_id,
                            finished_at,
                            entry_id,
                        )
                        await redis.xack(ANALYSIS_STREAM_KEY, ANALYSIS_CONSUMER_GROUP, entry_id)
                        continue

                    _last_postproc_finished_at[pair_key] = finished_at
                    total_pairs += 1

                    log.debug(
                        "BT_ANALYSIS_MAIN: получено сообщение постпроцессинга "
                        "scenario_id=%s, signal_id=%s, finished_at=%s, stream_id=%s",
                        scenario_id,
                        signal_id,
                        finished_at,
                        entry_id,
                    )

                    # очистка всего контура анализаторов по паре (scenario_id, signal_id) перед прогоном "с чистого листа"
                    try:
                        cleanup = await _cleanup_analysis_tables_for_pair(pg, scenario_id, signal_id)
                        total_cleanup_raw += cleanup["raw"]
                        total_cleanup_bins += cleanup["bins"]
                        total_cleanup_model += cleanup["model_opt"]
                        total_cleanup_labels += cleanup["bins_labels"]
                        total_cleanup_postproc += cleanup["positions_postproc"]
                        total_cleanup_scenario_stat += cleanup["scenario_stat"]
                        total_cleanup_total += cleanup["total"]

                        log.info(
                            "BT_ANALYSIS_MAIN: cleanup перед анализом scenario_id=%s, signal_id=%s — "
                            "deleted_raw=%s, deleted_bins=%s, deleted_model_opt=%s, deleted_bins_labels=%s, "
                            "deleted_positions_postproc=%s, deleted_scenario_stat=%s, deleted_total=%s",
                            scenario_id,
                            signal_id,
                            cleanup["raw"],
                            cleanup["bins"],
                            cleanup["model_opt"],
                            cleanup["bins_labels"],
                            cleanup["positions_postproc"],
                            cleanup["scenario_stat"],
                            cleanup["total"],
                        )
                    except Exception as e:
                        log.error(
                            "BT_ANALYSIS_MAIN: ошибка cleanup перед анализом scenario_id=%s, signal_id=%s: %s",
                            scenario_id,
                            signal_id,
                            e,
                            exc_info=True,
                        )

                    # получаем все связки сценарий ↔ сигнал ↔ анализатор
                    links = get_analysis_connections_for_scenario_signal(scenario_id, signal_id)
                    if not links:
                        log.debug(
                            "BT_ANALYSIS_MAIN: для scenario_id=%s, signal_id=%s нет активных связок анализаторов, "
                            "сообщение %s будет помечено как обработанное",
                            scenario_id,
                            signal_id,
                            entry_id,
                        )
                        # публикуем пустое событие готовности анализа
                        await _publish_analysis_ready(
                            redis=redis,
                            scenario_id=scenario_id,
                            signal_id=signal_id,
                            analyses_total=0,
                            analyses_ok=0,
                            analyses_failed=0,
                            rows_inserted=0,
                            bins_rows=0,
                        )
                        await redis.xack(ANALYSIS_STREAM_KEY, ANALYSIS_CONSUMER_GROUP, entry_id)
                        continue

                    # формируем список анализаторов для запуска
                    analyses_to_run: List[Dict[str, Any]] = []
                    for link in links:
                        analysis_id = link.get("analysis_id")
                        analysis_cfg = get_analysis_instance(analysis_id)
                        if not analysis_cfg:
                            log.warning(
                                "BT_ANALYSIS_MAIN: анализатор id=%s не найден в кеше, "
                                "scenario_id=%s, signal_id=%s, сообщение=%s",
                                analysis_id,
                                scenario_id,
                                signal_id,
                                entry_id,
                            )
                            continue

                        if not analysis_cfg.get("enabled"):
                            log.debug(
                                "BT_ANALYSIS_MAIN: анализатор id=%s отключён, "
                                "scenario_id=%s, signal_id=%s, сообщение=%s",
                                analysis_id,
                                scenario_id,
                                signal_id,
                                entry_id,
                            )
                            continue

                        analyses_to_run.append(analysis_cfg)

                    if not analyses_to_run:
                        log.debug(
                            "BT_ANALYSIS_MAIN: для scenario_id=%s, signal_id=%s нет активных анализаторов "
                            "(после фильтра по кешу), сообщение %s будет помечено как обработанное",
                            scenario_id,
                            signal_id,
                            entry_id,
                        )
                        await _publish_analysis_ready(
                            redis=redis,
                            scenario_id=scenario_id,
                            signal_id=signal_id,
                            analyses_total=0,
                            analyses_ok=0,
                            analyses_failed=0,
                            rows_inserted=0,
                            bins_rows=0,
                        )
                        await redis.xack(ANALYSIS_STREAM_KEY, ANALYSIS_CONSUMER_GROUP, entry_id)
                        continue

                    # запускаем все анализаторы для данной пары с ограничением по семафору
                    tasks: List[asyncio.Task] = []
                    for analysis in analyses_to_run:
                        task = asyncio.create_task(
                            _run_analysis_with_semaphore(
                                analysis=analysis,
                                scenario_id=scenario_id,
                                signal_id=signal_id,
                                pg=pg,
                                redis=redis,
                                sema=analysis_sema,
                            ),
                            name=f"BT_ANALYSIS_{analysis.get('id')}_SC_{scenario_id}_SIG_{signal_id}",
                        )
                        tasks.append(task)

                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    analyses_total = len(analyses_to_run)
                    analyses_ok = 0
                    analyses_failed = 0
                    rows_inserted = 0
                    bins_rows_for_pair = 0

                    for res in results:
                        if isinstance(res, Exception):
                            analyses_failed += 1
                            continue

                        status = res.get("status")
                        inserted = res.get("rows_inserted", 0)
                        bins_rows = res.get("bins_rows", 0)

                        if status == "ok":
                            analyses_ok += 1
                        elif status == "skipped":
                            # считаем как "успешно, но без данных"
                            analyses_ok += 1
                        else:
                            analyses_failed += 1

                        rows_inserted += inserted
                        bins_rows_for_pair += bins_rows

                    total_analyses_planned += analyses_total
                    total_analyses_ok += analyses_ok
                    total_analyses_failed += analyses_failed
                    total_rows_inserted += rows_inserted
                    total_bins_rows += bins_rows_for_pair

                    log.info(
                        "BT_ANALYSIS_MAIN: scenario_id=%s, signal_id=%s — анализаторов всего=%s, "
                        "успешно=%s, с ошибками=%s, строк в raw=%s, строк в bins_stat=%s",
                        scenario_id,
                        signal_id,
                        analyses_total,
                        analyses_ok,
                        analyses_failed,
                        rows_inserted,
                        bins_rows_for_pair,
                    )

                    # публикуем агрегированное событие готовности анализа в bt:analysis:ready
                    await _publish_analysis_ready(
                        redis=redis,
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                        analyses_total=analyses_total,
                        analyses_ok=analyses_ok,
                        analyses_failed=analyses_failed,
                        rows_inserted=rows_inserted,
                        bins_rows=bins_rows_for_pair,
                    )

                    # помечаем сообщение как обработанное после завершения всех анализаторов
                    await redis.xack(ANALYSIS_STREAM_KEY, ANALYSIS_CONSUMER_GROUP, entry_id)

            log.debug(
                "BT_ANALYSIS_MAIN: пакет сообщений обработан — сообщений=%s, пар=%s, "
                "cleanup_total=%s, анализаторов_планировалось=%s, успехов=%s, ошибок=%s, строк_raw=%s, строк_bins=%s",
                total_msgs,
                total_pairs,
                total_cleanup_total,
                total_analyses_planned,
                total_analyses_ok,
                total_analyses_failed,
                total_rows_inserted,
                total_bins_rows,
            )
            log.info(
                "BT_ANALYSIS_MAIN: итог по пакету — сообщений=%s, пар=%s, "
                "cleanup_raw=%s, cleanup_bins=%s, cleanup_model_opt=%s, cleanup_bins_labels=%s, cleanup_positions_postproc=%s, cleanup_scenario_stat=%s, cleanup_total=%s, "
                "анализаторов всего=%s, успешно=%s, с ошибками=%s, строк в raw=%s, строк в bins_stat=%s",
                total_msgs,
                total_pairs,
                total_cleanup_raw,
                total_cleanup_bins,
                total_cleanup_model,
                total_cleanup_labels,
                total_cleanup_postproc,
                total_cleanup_scenario_stat,
                total_cleanup_total,
                total_analyses_planned,
                total_analyses_ok,
                total_analyses_failed,
                total_rows_inserted,
                total_bins_rows,
            )

        except Exception as e:
            log.error(
                "BT_ANALYSIS_MAIN: ошибка в основном цикле оркестратора: %s",
                e,
                exc_info=True,
            )
            # небольшая пауза перед повторной попыткой, чтобы не крутить CPU при постоянной ошибке
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима анализа
async def _ensure_consumer_group(redis) -> None:
    try:
        # пытаемся создать группу; MKSTREAM создаст стрим, если его ещё нет
        await redis.xgroup_create(
            name=ANALYSIS_STREAM_KEY,
            groupname=ANALYSIS_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_ANALYSIS_MAIN: создана consumer group '%s' для стрима '%s'",
            ANALYSIS_CONSUMER_GROUP,
            ANALYSIS_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_ANALYSIS_MAIN: consumer group '%s' для стрима '%s' уже существует",
                ANALYSIS_CONSUMER_GROUP,
                ANALYSIS_STREAM_KEY,
            )
        else:
            log.error(
                "BT_ANALYSIS_MAIN: ошибка при создании consumer group '%s': %s",
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
            # не хватает обязательных полей
            return None

        scenario_id = int(scenario_id_str)
        signal_id = int(signal_id_str)
        finished_at = datetime.fromisoformat(finished_at_str)

        # дополнительные поля (processed/skipped/errors) сейчас не используем, но можем залогировать при необходимости
        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "finished_at": finished_at,
        }
    except Exception as e:
        log.error(
            "BT_ANALYSIS_MAIN: ошибка разбора сообщения стрима bt:postproc:ready: %s, fields=%s",
            e,
            fields,
            exc_info=True,
        )
        return None


# 🔸 Обёртка для запуска одного анализатора с семафором
async def _run_analysis_with_semaphore(
    analysis: Dict[str, Any],
    scenario_id: int,
    signal_id: int,
    pg,
    redis,
    sema: asyncio.Semaphore,
) -> Dict[str, Any]:
    async with sema:
        try:
            return await _run_single_analysis(
                analysis=analysis,
                scenario_id=scenario_id,
                signal_id=signal_id,
                pg=pg,
                redis=redis,
            )
        except Exception as e:
            analysis_id = analysis.get("id")
            family_key = analysis.get("family_key")
            key = analysis.get("key")
            log.error(
                "BT_ANALYSIS_MAIN: ошибка выполнения анализатора id=%s (family=%s, key=%s) "
                "для scenario_id=%s, signal_id=%s: %s",
                analysis_id,
                family_key,
                key,
                scenario_id,
                signal_id,
                e,
                exc_info=True,
            )
            return {
                "analysis_id": analysis_id,
                "status": "error",
                "rows_inserted": 0,
                "bins_rows": 0,
            }


# 🔸 Запуск одного анализатора: очистка результатов, запуск воркера, запись raw и пересчёт bin-статистики
async def _run_single_analysis(
    analysis: Dict[str, Any],
    scenario_id: int,
    signal_id: int,
    pg,
    redis,
) -> Dict[str, Any]:
    analysis_id = analysis.get("id")
    family_key = str(analysis.get("family_key") or "").strip()
    analysis_key = str(analysis.get("key") or "").strip()
    name = analysis.get("name")
    params = analysis.get("params") or {}

    handler = ANALYSIS_HANDLERS.get((family_key, analysis_key))
    if handler is None:
        log.debug(
            "BT_ANALYSIS_MAIN: анализатор id=%s (family=%s, key=%s, name=%s) пока не поддерживается реестром анализаторов",
            analysis_id,
            family_key,
            analysis_key,
            name,
        )
        return {
            "analysis_id": analysis_id,
            "status": "skipped",
            "rows_inserted": 0,
            "bins_rows": 0,
        }

    # indicator_param — например rsi14 / rsi21 / lr50_angle и т.п.
    indicator_param_cfg = params.get("param_name")
    if indicator_param_cfg is not None:
        indicator_param = str(indicator_param_cfg.get("value") or "").strip() or None
    else:
        indicator_param = None

    log.debug(
        "BT_ANALYSIS_MAIN: запуск анализатора id=%s (family=%s, key=%s, name=%s, indicator_param=%s) "
        "для scenario_id=%s, signal_id=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        indicator_param,
        scenario_id,
        signal_id,
    )

    # очистка предыдущих результатов для данного анализатора и пары (сценарий, сигнал)
    async with pg.acquire() as conn:
        await conn.execute(
            """
            DELETE FROM bt_analysis_positions_raw
            WHERE analysis_id = $1
              AND scenario_id = $2
              AND signal_id = $3
            """,
            analysis_id,
            scenario_id,
            signal_id,
        )
        await conn.execute(
            """
            DELETE FROM bt_analysis_bins_stat
            WHERE analysis_id = $1
              AND scenario_id = $2
              AND signal_id = $3
            """,
            analysis_id,
            scenario_id,
            signal_id,
        )

    # контекст для анализатора
    analysis_ctx: Dict[str, Any] = {
        "scenario_id": scenario_id,
        "signal_id": signal_id,
    }

    # запускаем бизнес-логику анализатора
    result: Dict[str, Any] = await handler(analysis, analysis_ctx, pg, redis)
    rows: List[Dict[str, Any]] = (result or {}).get("rows") or []

    if not rows:
        log.debug(
            "BT_ANALYSIS_MAIN: анализатор id=%s (family=%s, key=%s) для scenario_id=%s, signal_id=%s "
            "не вернул строк для вставки (raw)",
            analysis_id,
            family_key,
            analysis_key,
            scenario_id,
            signal_id,
        )
        return {
            "analysis_id": analysis_id,
            "status": "ok",
            "rows_inserted": 0,
            "bins_rows": 0,
        }

    # подготовка данных для массовой вставки в bt_analysis_positions_raw
    to_insert: List[Tuple[Any, ...]] = []
    for row in rows:
        position_uid = row.get("position_uid")
        timeframe = row.get("timeframe")
        direction = row.get("direction")
        bin_name = row.get("bin_name")
        value = row.get("value")
        pnl_abs = row.get("pnl_abs")

        if not position_uid or timeframe is None or direction is None or bin_name is None:
            # пропускаем некорректные строки
            log.debug(
                "BT_ANALYSIS_MAIN: анализатор id=%s (family=%s, key=%s) вернул некорректную строку, "
                "она будет пропущена: %s",
                analysis_id,
                family_key,
                analysis_key,
                row,
            )
            continue

        to_insert.append(
            (
                analysis_id,
                position_uid,
                scenario_id,
                signal_id,
                family_key,
                analysis_key,
                timeframe,
                direction,
                bin_name,
                value,
                pnl_abs,
            )
        )

    inserted = 0
    bins_rows = 0

    if to_insert:
        async with pg.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO bt_analysis_positions_raw (
                    analysis_id,
                    position_uid,
                    scenario_id,
                    signal_id,
                    family_key,
                    "key",
                    timeframe,
                    direction,
                    bin_name,
                    value,
                    pnl_abs
                )
                VALUES (
                    $1, $2, $3, $4,
                    $5, $6, $7, $8,
                    $9, $10, $11
                )
                """,
                to_insert,
            )
        inserted = len(to_insert)

        # пересчёт статистики по биннам для данного анализатора и пары
        bins_rows, trades_total = await _recalc_bins_stat(
            pg=pg,
            analysis_id=analysis_id,
            scenario_id=scenario_id,
            signal_id=signal_id,
            indicator_param=indicator_param,
        )

        log.info(
            "BT_ANALYSIS_MAIN: анализатор id=%s (family=%s, key=%s, name=%s) "
            "для scenario_id=%s, signal_id=%s записал raw строк=%s и bins строк=%s (trades_total=%s)",
            analysis_id,
            family_key,
            analysis_key,
            name,
            scenario_id,
            signal_id,
            inserted,
            bins_rows,
            trades_total,
        )
    else:
        log.debug(
            "BT_ANALYSIS_MAIN: анализатор id=%s (family=%s, key=%s, name=%s) для scenario_id=%s, signal_id=%s "
            "не сформировал валидных строк raw после фильтрации",
            analysis_id,
            family_key,
            analysis_key,
            name,
            scenario_id,
            signal_id,
        )

    return {
        "analysis_id": analysis_id,
        "status": "ok",
        "rows_inserted": inserted,
        "bins_rows": bins_rows,
    }


# 🔸 Пересчёт статистики по биннам в bt_analysis_bins_stat
async def _recalc_bins_stat(
    pg,
    analysis_id: int,
    scenario_id: int,
    signal_id: int,
    indicator_param: Optional[str],
) -> Tuple[int, int]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                timeframe,
                direction,
                bin_name,
                COUNT(*)                                         AS trades,
                COUNT(*) FILTER (WHERE pnl_abs > 0)              AS wins,
                COALESCE(SUM(pnl_abs), 0)                        AS pnl_abs_total
            FROM bt_analysis_positions_raw
            WHERE analysis_id = $1
              AND scenario_id = $2
              AND signal_id   = $3
            GROUP BY timeframe, direction, bin_name
            """,
            analysis_id,
            scenario_id,
            signal_id,
        )

        if not rows:
            log.debug(
                "BT_ANALYSIS_MAIN: для анализа id=%s, scenario_id=%s, signal_id=%s нет строк raw для пересчёта bins_stat",
                analysis_id,
                scenario_id,
                signal_id,
            )
            return 0, 0

        # удаляем старые строки bins_stat для этого анализа и пары
        await conn.execute(
            """
            DELETE FROM bt_analysis_bins_stat
            WHERE analysis_id = $1
              AND scenario_id = $2
              AND signal_id   = $3
            """,
            analysis_id,
            scenario_id,
            signal_id,
        )

        to_insert: List[Tuple[Any, ...]] = []
        total_trades = 0

        for r in rows:
            timeframe = r["timeframe"]
            direction = r["direction"]
            bin_name = r["bin_name"]
            trades = int(r["trades"])
            wins = int(r["wins"])
            pnl_abs_total = Decimal(str(r["pnl_abs_total"]))

            total_trades += trades

            if trades > 0:
                winrate = Decimal(wins) / Decimal(trades)
            else:
                winrate = Decimal("0")

            # лёгкая квантизация для предсказуемой точности
            pnl_abs_q = pnl_abs_total.quantize(Decimal("0.0001"))
            winrate_q = winrate.quantize(Decimal("0.0001"))

            to_insert.append(
                (
                    analysis_id,
                    scenario_id,
                    signal_id,
                    indicator_param,
                    timeframe,
                    direction,
                    bin_name,
                    trades,
                    pnl_abs_q,
                    winrate_q,
                )
            )

        await conn.executemany(
            """
            INSERT INTO bt_analysis_bins_stat (
                analysis_id,
                scenario_id,
                signal_id,
                indicator_param,
                timeframe,
                direction,
                bin_name,
                trades,
                pnl_abs,
                winrate
            )
            VALUES (
                $1, $2, $3, $4,
                $5, $6, $7,
                $8, $9, $10
            )
            """,
            to_insert,
        )

    bins_count = len(to_insert)

    log.debug(
        "BT_ANALYSIS_MAIN: пересчитана статистика bins_stat для analysis_id=%s, scenario_id=%s, signal_id=%s — "
        "бинов=%s, trades_total=%s",
        analysis_id,
        scenario_id,
        signal_id,
        bins_count,
        total_trades,
    )
    return bins_count, total_trades


# 🔸 Публикация агрегированного события готовности анализа в bt:analysis:ready
async def _publish_analysis_ready(
    redis,
    scenario_id: int,
    signal_id: int,
    analyses_total: int,
    analyses_ok: int,
    analyses_failed: int,
    rows_inserted: int,
    bins_rows: int,
) -> None:
    finished_at = datetime.utcnow()

    try:
        await redis.xadd(
            ANALYSIS_READY_STREAM_KEY,
            {
                "scenario_id": str(scenario_id),
                "signal_id": str(signal_id),
                "analyses_total": str(analyses_total),
                "analyses_ok": str(analyses_ok),
                "analyses_failed": str(analyses_failed),
                "rows_raw": str(rows_inserted),
                "rows_bins": str(bins_rows),
                "finished_at": finished_at.isoformat(),
            },
        )
        log.debug(
            "BT_ANALYSIS_MAIN: опубликовано событие готовности анализа в стрим '%s' "
            "для scenario_id=%s, signal_id=%s, analyses_total=%s, analyses_ok=%s, "
            "analyses_failed=%s, rows_raw=%s, rows_bins=%s, finished_at=%s",
            ANALYSIS_READY_STREAM_KEY,
            scenario_id,
            signal_id,
            analyses_total,
            analyses_ok,
            analyses_failed,
            rows_inserted,
            bins_rows,
            finished_at,
        )
    except Exception as e:
        log.error(
            "BT_ANALYSIS_MAIN: не удалось опубликовать событие в стрим '%s' "
            "для scenario_id=%s, signal_id=%s: %s",
            ANALYSIS_READY_STREAM_KEY,
            scenario_id,
            signal_id,
            e,
            exc_info=True,
        )

# 🔸 Очистка всех результатов контура анализаторов по паре (scenario_id, signal_id)
async def _cleanup_analysis_tables_for_pair(pg, scenario_id: int, signal_id: int) -> Dict[str, int]:
    deleted_raw = 0
    deleted_bins = 0
    deleted_model = 0
    deleted_labels = 0
    deleted_postproc = 0
    deleted_scenario_stat = 0
    deleted_adaptive_bins = 0

    async with pg.acquire() as conn:
        # транзакция очистки
        async with conn.transaction():
            # сначала labels (на случай отсутствия ON DELETE CASCADE по model_id)
            res_labels = await conn.execute(
                """
                DELETE FROM bt_analysis_bins_labels
                WHERE scenario_id = $1
                  AND signal_id   = $2
                """,
                scenario_id,
                signal_id,
            )
            deleted_labels = _parse_pg_execute_count(res_labels)

            # затем модели (и всё, что может быть связано каскадом)
            res_model = await conn.execute(
                """
                DELETE FROM bt_analysis_model_opt
                WHERE scenario_id = $1
                  AND signal_id   = $2
                """,
                scenario_id,
                signal_id,
            )
            deleted_model = _parse_pg_execute_count(res_model)

            # адаптивный словарь биннов (пересчитывается на каждый проход)
            res_adapt = await conn.execute(
                """
                DELETE FROM bt_analysis_bin_dict_adaptive
                WHERE scenario_id = $1
                  AND signal_id   = $2
                """,
                scenario_id,
                signal_id,
            )
            deleted_adaptive_bins = _parse_pg_execute_count(res_adapt)

            # финальный контейнер позиций
            res_postproc = await conn.execute(
                """
                DELETE FROM bt_analysis_positions_postproc
                WHERE scenario_id = $1
                  AND signal_id   = $2
                """,
                scenario_id,
                signal_id,
            )
            deleted_postproc = _parse_pg_execute_count(res_postproc)

            # финальная статистика по сценарию
            res_sc_stat = await conn.execute(
                """
                DELETE FROM bt_analysis_scenario_stat
                WHERE scenario_id = $1
                  AND signal_id   = $2
                """,
                scenario_id,
                signal_id,
            )
            deleted_scenario_stat = _parse_pg_execute_count(res_sc_stat)

            # сырые результаты и агрегаты по биннам
            res_bins = await conn.execute(
                """
                DELETE FROM bt_analysis_bins_stat
                WHERE scenario_id = $1
                  AND signal_id   = $2
                """,
                scenario_id,
                signal_id,
            )
            deleted_bins = _parse_pg_execute_count(res_bins)

            res_raw = await conn.execute(
                """
                DELETE FROM bt_analysis_positions_raw
                WHERE scenario_id = $1
                  AND signal_id   = $2
                """,
                scenario_id,
                signal_id,
            )
            deleted_raw = _parse_pg_execute_count(res_raw)

    return {
        "raw": deleted_raw,
        "bins": deleted_bins,
        "model_opt": deleted_model,
        "bins_labels": deleted_labels,
        "positions_postproc": deleted_postproc,
        "scenario_stat": deleted_scenario_stat,
        "adaptive_bins": deleted_adaptive_bins,
        "total": (
            deleted_raw
            + deleted_bins
            + deleted_model
            + deleted_labels
            + deleted_postproc
            + deleted_scenario_stat
            + deleted_adaptive_bins
        ),
    }

# 🔸 Парсинг результата asyncpg conn.execute вида "DELETE 123"
def _parse_pg_execute_count(res: Any) -> int:
    try:
        return int(str(res).split()[-1])
    except Exception:
        return 0