# bt_analysis_main.py — оркестратор анализаторов backtester_v1 (run-aware, без очистки, строго по окну run)

import asyncio
import logging
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any, List, Optional, Callable, Awaitable, Tuple, Set

# 🔸 Конфиг и кеши backtester_v1
from backtester_config import (
    get_analysis_connections_for_scenario_signal,
    get_analysis_instance,
    get_signal_instance,
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

from analysis.bt_analysis_supertrend_mtf import run_supertrend_mtf_analysis
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
    ("supertrend", "supertrend_mtf"): run_supertrend_mtf_analysis,
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

# 🔸 Таблицы (v2)
BT_SCENARIO_POSITIONS_V2_TABLE = "bt_scenario_positions_v2"
BT_SIGNALS_MEMBERSHIP_TABLE = "bt_signals_membership"
BT_SIGNAL_RUNS_TABLE = "bt_signal_backfill_runs"

# 🔸 Кеш последних finished_at по (scenario_id, signal_id, run_id) для отсечки дублей
_last_postproc_finished_at: Dict[Tuple[int, int, int], datetime] = {}

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
            total_rows_filtered_out = 0
            total_bins_rows = 0

            for stream_key, entries in messages:
                if stream_key != ANALYSIS_STREAM_KEY:
                    continue

                for entry_id, fields in entries:
                    total_msgs += 1

                    ctx = _parse_postproc_message(fields)
                    if not ctx:
                        await redis.xack(ANALYSIS_STREAM_KEY, ANALYSIS_CONSUMER_GROUP, entry_id)
                        continue

                    scenario_id = ctx["scenario_id"]
                    signal_id = ctx["signal_id"]
                    run_id = ctx["run_id"]
                    finished_at = ctx["finished_at"]

                    pair_key = (scenario_id, signal_id, run_id)
                    last_finished = _last_postproc_finished_at.get(pair_key)

                    # отсечка дублей по равному finished_at в рамках (scenario, signal, run)
                    if last_finished is not None and last_finished == finished_at:
                        log.debug(
                            "BT_ANALYSIS_MAIN: дубликат сообщения для scenario_id=%s, signal_id=%s, run_id=%s, "
                            "finished_at=%s, stream_id=%s — анализаторы не запускаются",
                            scenario_id,
                            signal_id,
                            run_id,
                            finished_at,
                            entry_id,
                        )
                        await redis.xack(ANALYSIS_STREAM_KEY, ANALYSIS_CONSUMER_GROUP, entry_id)
                        continue

                    _last_postproc_finished_at[pair_key] = finished_at
                    total_pairs += 1

                    # окно run (источник истины)
                    run_info = await _load_run_info(pg, run_id)
                    if not run_info:
                        log.error(
                            "BT_ANALYSIS_MAIN: не найден run_id=%s в bt_signal_backfill_runs (scenario_id=%s, signal_id=%s), stream_id=%s",
                            run_id,
                            scenario_id,
                            signal_id,
                            entry_id,
                        )
                        await redis.xack(ANALYSIS_STREAM_KEY, ANALYSIS_CONSUMER_GROUP, entry_id)
                        continue

                    run_signal_id = int(run_info["signal_id"])
                    window_from = run_info["from_time"]
                    window_to = run_info["to_time"]

                    # sanity-check: run должен соответствовать этому signal_id
                    # исключение: производный stream-сигнал может использовать parent run_id
                    if run_signal_id != signal_id:
                        parent_ok = False

                        # допускаем многослойные derived сигналы: signal -> parent -> parent -> ... -> root
                        cur_signal_id = int(signal_id)
                        for _depth in range(6):
                            sig_inst = get_signal_instance(int(cur_signal_id))
                            if not sig_inst:
                                break

                            sig_params = sig_inst.get("params") or {}
                            p_cfg = sig_params.get("parent_signal_id")

                            try:
                                parent_signal_id = int((p_cfg or {}).get("value") or 0)
                            except Exception:
                                parent_signal_id = 0

                            if parent_signal_id <= 0:
                                break

                            if parent_signal_id == run_signal_id:
                                parent_ok = True
                                break

                            cur_signal_id = int(parent_signal_id)

                        if not parent_ok:
                            log.error(
                                "BT_ANALYSIS_MAIN: run_id=%s принадлежит signal_id=%s, но пришло событие для signal_id=%s (scenario_id=%s). stream_id=%s",
                                run_id,
                                run_signal_id,
                                signal_id,
                                scenario_id,
                                entry_id,
                            )
                            await redis.xack(ANALYSIS_STREAM_KEY, ANALYSIS_CONSUMER_GROUP, entry_id)
                            continue

                        log.debug(
                            "BT_ANALYSIS_MAIN: derived signal допускает parent run_id — run_id=%s run.signal_id=%s, derived_signal_id=%s (scenario_id=%s)",
                            run_id,
                            run_signal_id,
                            signal_id,
                            scenario_id,
                        )

                    # множество позиций окна (защита на уровне storage: чтобы не хранить вне окна)
                    window_position_uids = await _load_window_position_uids(
                        pg=pg,
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                        run_id=run_id,
                        run_signal_id=run_signal_id,
                    )

                    log.debug(
                        "BT_ANALYSIS_MAIN: postproc_ready scenario_id=%s, signal_id=%s, run_id=%s, window=[%s..%s], positions_in_window=%s, finished_at=%s, stream_id=%s",
                        scenario_id,
                        signal_id,
                        run_id,
                        window_from,
                        window_to,
                        len(window_position_uids),
                        finished_at,
                        entry_id,
                    )

                    # получаем все связки сценарий ↔ сигнал ↔ анализатор
                    links = get_analysis_connections_for_scenario_signal(scenario_id, signal_id)
                    if not links:
                        await _publish_analysis_ready(
                            redis=redis,
                            scenario_id=scenario_id,
                            signal_id=signal_id,
                            run_id=run_id,
                            window_from=window_from,
                            window_to=window_to,
                            analyses_total=0,
                            analyses_ok=0,
                            analyses_failed=0,
                            rows_inserted=0,
                            rows_filtered_out=0,
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
                                "BT_ANALYSIS_MAIN: анализатор id=%s не найден в кеше (scenario_id=%s, signal_id=%s, run_id=%s, stream_id=%s)",
                                analysis_id,
                                scenario_id,
                                signal_id,
                                run_id,
                                entry_id,
                            )
                            continue

                        if not analysis_cfg.get("enabled"):
                            continue

                        analyses_to_run.append(analysis_cfg)

                    if not analyses_to_run:
                        await _publish_analysis_ready(
                            redis=redis,
                            scenario_id=scenario_id,
                            signal_id=signal_id,
                            run_id=run_id,
                            window_from=window_from,
                            window_to=window_to,
                            analyses_total=0,
                            analyses_ok=0,
                            analyses_failed=0,
                            rows_inserted=0,
                            rows_filtered_out=0,
                            bins_rows=0,
                        )
                        await redis.xack(ANALYSIS_STREAM_KEY, ANALYSIS_CONSUMER_GROUP, entry_id)
                        continue

                    # контекст для анализаторов (run-aware + окно)
                    analysis_ctx_base: Dict[str, Any] = {
                        "scenario_id": scenario_id,
                        "signal_id": signal_id,
                        "run_id": run_id,
                        "window_from": window_from,
                        "window_to": window_to,
                        "window_position_uids": window_position_uids,
                        "run_finished_at": run_info.get("finished_at"),
                    }

                    # запускаем все анализаторы для данной пары с ограничением по семафору
                    tasks: List[asyncio.Task] = []
                    for analysis in analyses_to_run:
                        task = asyncio.create_task(
                            _run_analysis_with_semaphore(
                                analysis=analysis,
                                analysis_ctx=analysis_ctx_base,
                                pg=pg,
                                redis=redis,
                                sema=analysis_sema,
                            ),
                            name=f"BT_ANALYSIS_{analysis.get('id')}_SC_{scenario_id}_SIG_{signal_id}_RUN_{run_id}",
                        )
                        tasks.append(task)

                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    analyses_total = len(analyses_to_run)
                    analyses_ok = 0
                    analyses_failed = 0
                    rows_inserted = 0
                    rows_filtered_out = 0
                    bins_rows_for_pair = 0

                    for res in results:
                        if isinstance(res, Exception):
                            analyses_failed += 1
                            continue

                        status = res.get("status")
                        inserted = int(res.get("rows_inserted", 0) or 0)
                        filtered_out = int(res.get("rows_filtered_out", 0) or 0)
                        bins_rows = int(res.get("bins_rows", 0) or 0)

                        if status in ("ok", "skipped"):
                            analyses_ok += 1
                        else:
                            analyses_failed += 1

                        rows_inserted += inserted
                        rows_filtered_out += filtered_out
                        bins_rows_for_pair += bins_rows

                    total_analyses_planned += analyses_total
                    total_analyses_ok += analyses_ok
                    total_analyses_failed += analyses_failed
                    total_rows_inserted += rows_inserted
                    total_rows_filtered_out += rows_filtered_out
                    total_bins_rows += bins_rows_for_pair

                    log.debug(
                        "BT_ANALYSIS_MAIN: pair done — scenario_id=%s, signal_id=%s, run_id=%s, window=[%s..%s], "
                        "analyses_total=%s, ok=%s, failed=%s, raw_inserted=%s, raw_filtered_out=%s, bins_rows=%s",
                        scenario_id,
                        signal_id,
                        run_id,
                        window_from,
                        window_to,
                        analyses_total,
                        analyses_ok,
                        analyses_failed,
                        rows_inserted,
                        rows_filtered_out,
                        bins_rows_for_pair,
                    )

                    await _publish_analysis_ready(
                        redis=redis,
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                        run_id=run_id,
                        window_from=window_from,
                        window_to=window_to,
                        analyses_total=analyses_total,
                        analyses_ok=analyses_ok,
                        analyses_failed=analyses_failed,
                        rows_inserted=rows_inserted,
                        rows_filtered_out=rows_filtered_out,
                        bins_rows=bins_rows_for_pair,
                    )

                    await redis.xack(ANALYSIS_STREAM_KEY, ANALYSIS_CONSUMER_GROUP, entry_id)

            log.debug(
                "BT_ANALYSIS_MAIN: batch summary — msgs=%s, pairs=%s, analyses_planned=%s, ok=%s, failed=%s, "
                "raw_inserted=%s, raw_filtered_out=%s, bins_rows=%s",
                total_msgs,
                total_pairs,
                total_analyses_planned,
                total_analyses_ok,
                total_analyses_failed,
                total_rows_inserted,
                total_rows_filtered_out,
                total_bins_rows,
            )

        except Exception as e:
            log.error(
                "BT_ANALYSIS_MAIN: ошибка в основном цикле оркестратора: %s",
                e,
                exc_info=True,
            )
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима анализа
async def _ensure_consumer_group(redis) -> None:
    try:
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
                "BT_ANALYSIS_MAIN: consumer group '%s' уже существует — сдвигаем курсор группы на '$' (SETID) для игнора истории до старта",
                ANALYSIS_CONSUMER_GROUP,
            )

            await redis.execute_command(
                "XGROUP",
                "SETID",
                ANALYSIS_STREAM_KEY,
                ANALYSIS_CONSUMER_GROUP,
                "$",
            )

            log.debug(
                "BT_ANALYSIS_MAIN: consumer group '%s' SETID='$' для стрима '%s' выполнен",
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
    try:
        entries = await redis.xreadgroup(
            groupname=ANALYSIS_CONSUMER_GROUP,
            consumername=ANALYSIS_CONSUMER_NAME,
            streams={ANALYSIS_STREAM_KEY: ">"},
            count=ANALYSIS_STREAM_BATCH_SIZE,
            block=ANALYSIS_STREAM_BLOCK_MS,
        )
    except Exception as e:
        msg = str(e)
        if "NOGROUP" in msg:
            log.warning(
                "BT_ANALYSIS_MAIN: NOGROUP при XREADGROUP — переинициализируем группу и продолжаем",
            )
            await _ensure_consumer_group(redis)
            return []
        raise

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


# 🔸 Разбор одного сообщения из стрима bt:postproc:ready (run-aware)
def _parse_postproc_message(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id_str = fields.get("scenario_id")
        signal_id_str = fields.get("signal_id")
        run_id_str = fields.get("run_id")
        finished_at_str = fields.get("finished_at")

        if not (scenario_id_str and signal_id_str and run_id_str and finished_at_str):
            return None

        scenario_id = int(scenario_id_str)
        signal_id = int(signal_id_str)
        run_id = int(run_id_str)
        finished_at = datetime.fromisoformat(finished_at_str)

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "run_id": run_id,
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
    analysis_ctx: Dict[str, Any],
    pg,
    redis,
    sema: asyncio.Semaphore,
) -> Dict[str, Any]:
    async with sema:
        try:
            return await _run_single_analysis(
                analysis=analysis,
                analysis_ctx=analysis_ctx,
                pg=pg,
                redis=redis,
            )
        except Exception as e:
            analysis_id = analysis.get("id")
            family_key = analysis.get("family_key")
            key = analysis.get("key")
            log.error(
                "BT_ANALYSIS_MAIN: ошибка выполнения анализатора id=%s (family=%s, key=%s) "
                "для scenario_id=%s, signal_id=%s, run_id=%s: %s",
                analysis_id,
                family_key,
                key,
                analysis_ctx.get("scenario_id"),
                analysis_ctx.get("signal_id"),
                analysis_ctx.get("run_id"),
                e,
                exc_info=True,
            )
            return {
                "analysis_id": analysis_id,
                "status": "error",
                "rows_inserted": 0,
                "rows_filtered_out": 0,
                "bins_rows": 0,
            }


# 🔸 Запуск одного анализатора: запуск воркера, запись raw и пересчёт bin-статистики (run-aware)
async def _run_single_analysis(
    analysis: Dict[str, Any],
    analysis_ctx: Dict[str, Any],
    pg,
    redis,
) -> Dict[str, Any]:
    analysis_id = int(analysis.get("id") or 0)
    family_key = str(analysis.get("family_key") or "").strip()
    analysis_key = str(analysis.get("key") or "").strip()
    name = analysis.get("name")
    params = analysis.get("params") or {}

    scenario_id = int(analysis_ctx.get("scenario_id") or 0)
    signal_id = int(analysis_ctx.get("signal_id") or 0)
    run_id = int(analysis_ctx.get("run_id") or 0)

    window_position_uids: Set[Any] = analysis_ctx.get("window_position_uids") or set()

    handler = ANALYSIS_HANDLERS.get((family_key, analysis_key))
    if handler is None:
        log.debug(
            "BT_ANALYSIS_MAIN: анализатор id=%s (family=%s, key=%s, name=%s) пока не поддерживается реестром",
            analysis_id,
            family_key,
            analysis_key,
            name,
        )
        return {"analysis_id": analysis_id, "status": "skipped", "rows_inserted": 0, "rows_filtered_out": 0, "bins_rows": 0}

    # indicator_param — например rsi14 / rsi21 / lr50_angle и т.п.
    indicator_param_cfg = params.get("param_name")
    if indicator_param_cfg is not None:
        indicator_param = str(indicator_param_cfg.get("value") or "").strip() or None
    else:
        indicator_param = None

    # вызываем бизнес-логику анализатора
    result: Dict[str, Any] = await handler(analysis, analysis_ctx, pg, redis)
    rows: List[Dict[str, Any]] = (result or {}).get("rows") or []

    if not rows:
        return {"analysis_id": analysis_id, "status": "ok", "rows_inserted": 0, "rows_filtered_out": 0, "bins_rows": 0}

    to_insert: List[Tuple[Any, ...]] = []
    filtered_out = 0

    for row in rows:
        position_uid = row.get("position_uid")
        timeframe = row.get("timeframe")
        direction = row.get("direction")
        bin_name = row.get("bin_name")
        value = row.get("value")
        pnl_abs = row.get("pnl_abs")

        if not position_uid or timeframe is None or direction is None or bin_name is None:
            continue

        # фильтр по окну run на уровне storage (позиции должны быть из window_position_uids)
        if window_position_uids and position_uid not in window_position_uids:
            filtered_out += 1
            continue

        to_insert.append(
            (
                run_id,
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

    if not to_insert:
        return {"analysis_id": analysis_id, "status": "ok", "rows_inserted": 0, "rows_filtered_out": filtered_out, "bins_rows": 0}

    async with pg.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO bt_analysis_positions_raw (
                run_id,
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
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9,
                $10, $11, $12
            )
            ON CONFLICT (run_id, analysis_id, position_uid) DO NOTHING
            """,
            to_insert,
        )

    inserted = len(to_insert)

    bins_rows, trades_total = await _recalc_bins_stat(
        pg=pg,
        run_id=run_id,
        analysis_id=analysis_id,
        scenario_id=scenario_id,
        signal_id=signal_id,
        indicator_param=indicator_param,
    )

    log.debug(
        "BT_ANALYSIS_MAIN: analysis done id=%s (family=%s, key=%s, name=%s) scenario_id=%s signal_id=%s run_id=%s — raw=%s, filtered_out=%s, bins=%s (trades_total=%s)",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        run_id,
        inserted,
        filtered_out,
        bins_rows,
        trades_total,
    )

    return {"analysis_id": analysis_id, "status": "ok", "rows_inserted": inserted, "rows_filtered_out": filtered_out, "bins_rows": bins_rows}


# 🔸 Пересчёт статистики по биннам в bt_analysis_bins_stat (run-aware)
async def _recalc_bins_stat(
    pg,
    run_id: int,
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
            WHERE run_id     = $1
              AND analysis_id = $2
              AND scenario_id = $3
              AND signal_id   = $4
            GROUP BY timeframe, direction, bin_name
            """,
            run_id,
            analysis_id,
            scenario_id,
            signal_id,
        )

        if not rows:
            return 0, 0

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

            winrate = (Decimal(wins) / Decimal(trades)) if trades > 0 else Decimal("0")

            pnl_abs_q = pnl_abs_total.quantize(Decimal("0.0001"))
            winrate_q = winrate.quantize(Decimal("0.0001"))

            to_insert.append(
                (
                    run_id,
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
                run_id,
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
                $5, $6, $7, $8,
                $9, $10, $11
            )
            ON CONFLICT (run_id, analysis_id, scenario_id, signal_id, indicator_param, timeframe, direction, bin_name)
            DO UPDATE SET
                trades = EXCLUDED.trades,
                pnl_abs = EXCLUDED.pnl_abs,
                winrate = EXCLUDED.winrate,
                updated_at = now()
            """,
            to_insert,
        )

    return len(to_insert), total_trades


# 🔸 Публикация агрегированного события готовности анализа в bt:analysis:ready (run-aware)
async def _publish_analysis_ready(
    redis,
    scenario_id: int,
    signal_id: int,
    run_id: int,
    window_from: datetime,
    window_to: datetime,
    analyses_total: int,
    analyses_ok: int,
    analyses_failed: int,
    rows_inserted: int,
    rows_filtered_out: int,
    bins_rows: int,
) -> None:
    finished_at = datetime.utcnow()

    try:
        await redis.xadd(
            ANALYSIS_READY_STREAM_KEY,
            {
                "scenario_id": str(scenario_id),
                "signal_id": str(signal_id),
                "run_id": str(run_id),
                "window_from": window_from.isoformat(),
                "window_to": window_to.isoformat(),
                "analyses_total": str(analyses_total),
                "analyses_ok": str(analyses_ok),
                "analyses_failed": str(analyses_failed),
                "rows_raw": str(rows_inserted),
                "rows_raw_filtered_out": str(rows_filtered_out),
                "rows_bins": str(bins_rows),
                "finished_at": finished_at.isoformat(),
            },
        )
        log.debug(
            "BT_ANALYSIS_MAIN: published bt:analysis:ready scenario_id=%s signal_id=%s run_id=%s analyses_total=%s ok=%s failed=%s raw=%s filtered_out=%s bins=%s",
            scenario_id,
            signal_id,
            run_id,
            analyses_total,
            analyses_ok,
            analyses_failed,
            rows_inserted,
            rows_filtered_out,
            bins_rows,
        )
    except Exception as e:
        log.error(
            "BT_ANALYSIS_MAIN: не удалось опубликовать событие в '%s' scenario_id=%s signal_id=%s run_id=%s: %s",
            ANALYSIS_READY_STREAM_KEY,
            scenario_id,
            signal_id,
            run_id,
            e,
            exc_info=True,
        )


# 🔸 Загрузка run-окна из bt_signal_backfill_runs
async def _load_run_info(pg, run_id: int) -> Optional[Dict[str, Any]]:
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, signal_id, from_time, to_time, status, finished_at
            FROM bt_signal_backfill_runs
            WHERE id = $1
            """,
            int(run_id),
        )
    if not row:
        return None
    return {
        "id": int(row["id"]),
        "signal_id": int(row["signal_id"]),
        "from_time": row["from_time"],
        "to_time": row["to_time"],
        "status": row["status"],
        "finished_at": row["finished_at"],
    }


# 🔸 Загрузка position_uid в окне run (v2, membership-aware, строго в рамках run.to_time)
async def _load_window_position_uids(
    pg,
    scenario_id: int,
    signal_id: int,
    run_id: int,
    run_signal_id: int,
) -> Set[Any]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT DISTINCT p.position_uid
            FROM {BT_SIGNALS_MEMBERSHIP_TABLE} m
            JOIN {BT_SIGNAL_RUNS_TABLE} r
              ON r.id = $3
            JOIN {BT_SCENARIO_POSITIONS_V2_TABLE} p
              ON p.signal_value_id = m.signal_value_id
             AND p.scenario_id = $1
             AND p.signal_id = $2
            WHERE
                (
                    (m.run_id = $3 AND m.signal_id = $2)
                    OR
                    (m.parent_run_id = $3 AND m.parent_signal_id = $4 AND m.signal_id = $2)
                )
              AND p.status = 'closed'
              AND p.postproc_v2 = true
              AND p.exit_time <= r.to_time
            """,
            int(scenario_id),
            int(signal_id),
            int(run_id),
            int(run_signal_id),
        )

    return {r["position_uid"] for r in rows}