# bt_signals_lr_universal_level2.py — stream-backfill воркер уровня 2: создаёт собственный run, формирует membership по good bins (v2)

import asyncio
import logging
import os
import sys
from datetime import datetime
from typing import Dict, Any, Optional, Set, Tuple, List, Callable


# 🔸 Path bootstrap для signals_plugins (директория в корне проекта)
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

# 🔸 Плагины биннинга (универсальные по инстансам анализаторов)
from signals_plugins.lr_mtf import (
    init_lr_mtf_plugin_context,
    prepare_symbol_series as prepare_lr_mtf_symbol_series,
    compute_lr_mtf_bin_name,
)
from signals_plugins.lr_angle_mtf import (
    init_lr_angle_mtf_plugin_context,
    prepare_symbol_series as prepare_lr_angle_mtf_symbol_series,
    compute_lr_angle_mtf_bin_name,
)
from signals_plugins.bb_mtf import (
    init_bb_mtf_plugin_context,
    prepare_symbol_series as prepare_bb_mtf_symbol_series,
    compute_bb_mtf_bin_name,
)

from signals_plugins.emastate import (
    init_emastate_mtf_plugin_context,
    prepare_symbol_series as prepare_emastate_mtf_symbol_series,
    compute_emastate_mtf_bin_name,
)

# 🔸 Кеши backtester_v1
from backtester_config import (
    get_all_ticker_symbols,
    get_analysis_instance,
)

# 🔸 Логгер модуля
log = logging.getLogger("BT_SIG_LR_UNI_L2")

# 🔸 Стримы
BT_POSTPROC_READY_STREAM_V2 = "bt:analysis:postproc_ready_v2"
BT_SIGNALS_READY_STREAM_V2 = "bt:signals:ready_v2"

# 🔸 Таблицы
BT_LABELS_V2_TABLE = "bt_analysis_bins_labels_v2"
BT_MEMBERSHIP_TABLE = "bt_signals_membership"
BT_EVENTS_TABLE = "bt_signals_values"
BT_RUNS_TABLE = "bt_signal_backfill_runs"

# 🔸 Ограничение параллелизма по тикерам
SYMBOL_MAX_CONCURRENCY = 5


# 🔸 Парсер сообщения bt:analysis:postproc_ready_v2
def _parse_postproc_ready_v2(fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id = int(str(fields.get("scenario_id") or "").strip())
        signal_id = int(str(fields.get("signal_id") or "").strip())
        run_id = int(str(fields.get("run_id") or "").strip())

        winner_analysis_id = int(str(fields.get("winner_analysis_id") or "0").strip() or 0)
        winner_param = str(fields.get("winner_param") or "").strip()
        score_version = str(fields.get("score_version") or "v1").strip()

        finished_at_raw = str(fields.get("finished_at") or "").strip()
        finished_at = datetime.fromisoformat(finished_at_raw) if finished_at_raw else None

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "run_id": run_id,
            "winner_analysis_id": winner_analysis_id,
            "winner_param": winner_param,
            "score_version": score_version,
            "finished_at": finished_at,
        }
    except Exception:
        return None


# 🔸 Загрузка окна parent-run из bt_signal_backfill_runs
async def _load_run_info(pg, run_id: int) -> Optional[Dict[str, Any]]:
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT id, signal_id, from_time, to_time, finished_at, status
            FROM {BT_RUNS_TABLE}
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
        "finished_at": row["finished_at"],
        "status": row["status"],
    }


# 🔸 Создание (или получение) собственного run для level2/refilter по origin_msg_id (идемпотентно)
async def _create_or_get_stream_run(
    pg,
    signal_id: int,
    from_time: datetime,
    to_time: datetime,
    origin_msg_id: str,
) -> int:
    async with pg.acquire() as conn:
        rid = await conn.fetchval(
            f"""
            INSERT INTO {BT_RUNS_TABLE}
                (signal_id, from_time, to_time, started_at, status, origin_msg_id)
            VALUES ($1, $2, $3, NOW(), 'running', $4)
            ON CONFLICT (signal_id, origin_msg_id)
            DO UPDATE SET started_at = {BT_RUNS_TABLE}.started_at
            RETURNING id
            """,
            int(signal_id),
            from_time,
            to_time,
            str(origin_msg_id),
        )

    return int(rid)


# 🔸 Завершение собственного run
async def _finish_run(pg, run_id: int, status: str, error: Optional[str]) -> None:
    async with pg.acquire() as conn:
        await conn.execute(
            f"""
            UPDATE {BT_RUNS_TABLE}
               SET finished_at = NOW(),
                   status = $2,
                   error = $3
             WHERE id = $1
            """,
            int(run_id),
            str(status),
            error,
        )


# 🔸 Загрузка whitelist good bins из bt_analysis_bins_labels_v2 (строго по parent_run_id)
async def _load_good_bins_v2(
    pg,
    parent_run_id: int,
    scenario_id: int,
    parent_signal_id: int,
    direction: str,
    score_version: str,
    analysis_id: int,
    winner_param: str,
) -> Set[str]:
    async with pg.acquire() as conn:
        # если winner_param задан — считаем, что это indicator_param
        if winner_param:
            rows = await conn.fetch(
                f"""
                SELECT bin_name
                FROM {BT_LABELS_V2_TABLE}
                WHERE run_id       = $1
                  AND scenario_id  = $2
                  AND signal_id    = $3
                  AND direction    = $4
                  AND score_version= $5
                  AND analysis_id  = $6
                  AND indicator_param = $7
                  AND state        = 'good'
                """,
                int(parent_run_id),
                int(scenario_id),
                int(parent_signal_id),
                str(direction),
                str(score_version),
                int(analysis_id),
                str(winner_param),
            )
        else:
            rows = await conn.fetch(
                f"""
                SELECT bin_name
                FROM {BT_LABELS_V2_TABLE}
                WHERE run_id       = $1
                  AND scenario_id  = $2
                  AND signal_id    = $3
                  AND direction    = $4
                  AND score_version= $5
                  AND analysis_id  = $6
                  AND indicator_param IS NULL
                  AND state        = 'good'
                """,
                int(parent_run_id),
                int(scenario_id),
                int(parent_signal_id),
                str(direction),
                str(score_version),
                int(analysis_id),
            )

    bins: Set[str] = set()
    for r in rows:
        bn = r["bin_name"]
        if bn is not None:
            bins.add(str(bn))

    return bins


# 🔸 Загрузка parent кандидатов через membership(parent_run_id,parent_signal_id) JOIN events (по символу)
async def _load_parent_candidates_for_symbol(
    pg,
    parent_run_id: int,
    parent_signal_id: int,
    symbol: str,
    timeframe: str,
    direction: str,
    window_from: datetime,
    window_to: datetime,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                e.id            AS signal_value_id,
                e.open_time     AS open_time,
                e.decision_time AS decision_time,
                e.direction     AS direction,
                e.price         AS price
            FROM {BT_MEMBERSHIP_TABLE} m
            JOIN {BT_EVENTS_TABLE} e
              ON e.id = m.signal_value_id
            WHERE m.run_id = $1
              AND m.signal_id = $2
              AND e.symbol = $3
              AND e.timeframe = $4
              AND e.direction = $5
              AND e.open_time BETWEEN $6 AND $7
            ORDER BY e.open_time
            """,
            int(parent_run_id),
            int(parent_signal_id),
            str(symbol),
            str(timeframe),
            str(direction),
            window_from,
            window_to,
        )

    out: List[Dict[str, Any]] = []
    for r in rows:
        # условия достаточности
        if r["decision_time"] is None:
            continue

        out.append(
            {
                "signal_value_id": int(r["signal_value_id"]),
                "symbol": str(symbol),
                "open_time": r["open_time"],
                "decision_time": r["decision_time"],
                "direction": str(direction),
                "price": r["price"],
            }
        )

    return out


# 🔸 Вставка membership для текущего run (идемпотентно)
async def _insert_membership(pg, rows: List[Tuple[Any, ...]]) -> int:
    # условия достаточности
    if not rows:
        return 0

    run_ids: List[int] = []
    signal_ids: List[int] = []
    value_ids: List[int] = []
    scenario_ids: List[int] = []
    parent_run_ids: List[int] = []
    parent_signal_ids: List[int] = []
    winner_ids: List[int] = []
    score_versions: List[str] = []
    winner_params: List[Optional[str]] = []
    bin_names: List[str] = []
    plugins: List[Optional[str]] = []
    plugin_params: List[Optional[str]] = []
    lr_prefixes: List[Optional[str]] = []
    lengths: List[int] = []
    pipeline_modes: List[str] = []

    for r in rows:
        run_ids.append(int(r[0]))
        signal_ids.append(int(r[1]))
        value_ids.append(int(r[2]))
        scenario_ids.append(int(r[3]))
        parent_run_ids.append(int(r[4]))
        parent_signal_ids.append(int(r[5]))
        winner_ids.append(int(r[6]))
        score_versions.append(str(r[7]))
        winner_params.append(None if r[8] is None else str(r[8]))
        bin_names.append(str(r[9]))
        plugins.append(None if r[10] is None else str(r[10]))
        plugin_params.append(None if r[11] is None else str(r[11]))
        lr_prefixes.append(None if r[12] is None else str(r[12]))
        lengths.append(int(r[13]))
        pipeline_modes.append(str(r[14]))

    async with pg.acquire() as conn:
        inserted_rows = await conn.fetch(
            f"""
            INSERT INTO {BT_MEMBERSHIP_TABLE}
                (run_id, signal_id, signal_value_id,
                 scenario_id, parent_run_id, parent_signal_id,
                 winner_analysis_id, score_version, winner_param,
                 bin_name, plugin, plugin_param_name, lr_prefix, length, pipeline_mode)
            SELECT
                u.run_id,
                u.signal_id,
                u.signal_value_id,
                u.scenario_id,
                u.parent_run_id,
                u.parent_signal_id,
                u.winner_analysis_id,
                u.score_version,
                u.winner_param,
                u.bin_name,
                u.plugin,
                u.plugin_param_name,
                u.lr_prefix,
                u.length,
                u.pipeline_mode
            FROM unnest(
                $1::bigint[],
                $2::int[],
                $3::int[],
                $4::int[],
                $5::bigint[],
                $6::int[],
                $7::int[],
                $8::text[],
                $9::text[],
                $10::text[],
                $11::text[],
                $12::text[],
                $13::text[],
                $14::int[],
                $15::text[]
            ) AS u(
                run_id,
                signal_id,
                signal_value_id,
                scenario_id,
                parent_run_id,
                parent_signal_id,
                winner_analysis_id,
                score_version,
                winner_param,
                bin_name,
                plugin,
                plugin_param_name,
                lr_prefix,
                length,
                pipeline_mode
            )
            ON CONFLICT (run_id, signal_id, signal_value_id) DO NOTHING
            RETURNING id
            """,
            run_ids,
            signal_ids,
            value_ids,
            scenario_ids,
            parent_run_ids,
            parent_signal_ids,
            winner_ids,
            score_versions,
            winner_params,
            bin_names,
            plugins,
            plugin_params,
            lr_prefixes,
            lengths,
            pipeline_modes,
        )

    return len(inserted_rows)

# 🔸 Публичная точка входа: stream-backfill сигнал уровня 2
async def run_lr_universal_level2_stream_backfill(
    signal: Dict[str, Any],
    msg_ctx: Dict[str, Any],
    pg,
    redis,
) -> None:
    signal_id = int(signal.get("id") or 0)
    signal_key = str(signal.get("key") or "").strip()
    name = signal.get("name")
    timeframe = str(signal.get("timeframe") or "").strip().lower()
    params = signal.get("params") or {}

    # условия достаточности
    if signal_id <= 0 or timeframe != "m5":
        return

    # сообщение должно приходить из bt:analysis:postproc_ready_v2
    stream_key = str((msg_ctx or {}).get("stream_key") or "")
    fields = (msg_ctx or {}).get("fields") or {}
    origin_msg_id = str((msg_ctx or {}).get("msg_id") or "").strip()

    if stream_key != BT_POSTPROC_READY_STREAM_V2:
        return
    if not origin_msg_id:
        return

    evt = _parse_postproc_ready_v2(fields)
    if not evt:
        return

    msg_scenario_id = evt["scenario_id"]
    msg_parent_signal_id = evt["signal_id"]
    parent_run_id = evt["run_id"]
    winner_analysis_id = int(evt["winner_analysis_id"])
    winner_param = str(evt.get("winner_param") or "")
    score_version = str(evt.get("score_version") or "v1")

    # parent_signal_id / parent_scenario_id из параметров инстанса
    parent_sig_cfg = params.get("parent_signal_id")
    parent_sc_cfg = params.get("parent_scenario_id")
    dir_cfg = params.get("direction_mask")

    # режим пайплайна: generate|refilter
    pipeline_mode_cfg = params.get("pipeline_mode")
    pipeline_mode = str((pipeline_mode_cfg or {}).get("value") or "generate").strip().lower()

    try:
        configured_parent_signal_id = int((parent_sig_cfg or {}).get("value") or 0)
    except Exception:
        configured_parent_signal_id = 0

    try:
        configured_parent_scenario_id = int((parent_sc_cfg or {}).get("value") or 0)
    except Exception:
        configured_parent_scenario_id = 0

    direction = str((dir_cfg or {}).get("value") or "").strip().lower()

    # условия достаточности
    if configured_parent_signal_id <= 0 or configured_parent_scenario_id <= 0:
        return
    if direction not in ("long", "short"):
        return
    if pipeline_mode not in ("generate", "refilter"):
        pipeline_mode = "generate"

    # сообщение должно относиться к нашей связке
    if msg_parent_signal_id != configured_parent_signal_id or msg_scenario_id != configured_parent_scenario_id:
        return

    parent_signal_id = configured_parent_signal_id
    scenario_id = configured_parent_scenario_id

    # окно parent run — источник истины
    parent_run_info = await _load_run_info(pg, parent_run_id)
    if not parent_run_info:
        log.warning(
            "BT_SIG_LR_UNI_L2: parent run not found — level2_signal_id=%s parent_signal_id=%s scenario_id=%s parent_run_id=%s",
            signal_id,
            parent_signal_id,
            scenario_id,
            parent_run_id,
        )
        return

    window_from: datetime = parent_run_info["from_time"]
    window_to: datetime = parent_run_info["to_time"]

    # winner analysis cfg (для выбора плагина)
    analysis_cfg = get_analysis_instance(int(winner_analysis_id))
    if not analysis_cfg:
        log.debug(
            "BT_SIG_LR_UNI_L2: winner analysis not in cache — skip (winner_analysis_id=%s)",
            winner_analysis_id,
        )
        return

    family_key = str(analysis_cfg.get("family_key") or "").strip().lower()
    analysis_key = str(analysis_cfg.get("key") or "").strip().lower()

    # выбор плагина
    plugin_name: str = ""
    init_plugin: Optional[Callable[..., Any]] = None
    prepare_series_fn: Optional[Callable[..., Any]] = None
    compute_bin_fn: Optional[Callable[..., Any]] = None

    if family_key == "lr" and analysis_key == "lr_mtf":
        plugin_name = "lr_mtf"
        init_plugin = init_lr_mtf_plugin_context
        prepare_series_fn = prepare_lr_mtf_symbol_series
        compute_bin_fn = compute_lr_mtf_bin_name

    elif family_key == "lr" and analysis_key == "lr_angle_mtf":
        plugin_name = "lr_angle_mtf"
        init_plugin = init_lr_angle_mtf_plugin_context
        prepare_series_fn = prepare_lr_angle_mtf_symbol_series
        compute_bin_fn = compute_lr_angle_mtf_bin_name

    elif family_key == "bb" and analysis_key == "bb_mtf":
        plugin_name = "bb_mtf"
        init_plugin = init_bb_mtf_plugin_context
        prepare_series_fn = prepare_bb_mtf_symbol_series
        compute_bin_fn = compute_bb_mtf_bin_name

    elif family_key == "ema" and analysis_key == "emastate_mtf":
        plugin_name = "emastate_mtf"
        init_plugin = init_emastate_mtf_plugin_context
        prepare_series_fn = prepare_emastate_mtf_symbol_series
        compute_bin_fn = compute_emastate_mtf_bin_name

    else:
        log.debug(
            "BT_SIG_LR_UNI_L2: winner plugin not supported — winner_analysis_id=%s family=%s key=%s winner_param='%s'",
            winner_analysis_id,
            family_key,
            analysis_key,
            str(winner_param),
        )
        return

    # загружаем good bins (строго по parent_run_id)
    good_bins = await _load_good_bins_v2(
        pg=pg,
        parent_run_id=int(parent_run_id),
        scenario_id=int(scenario_id),
        parent_signal_id=int(parent_signal_id),
        direction=str(direction),
        score_version=str(score_version),
        analysis_id=int(winner_analysis_id),
        winner_param=str(winner_param),
    )

    # условий достаточности
    if not good_bins:
        log.info(
            "BT_SIG_LR_UNI_L2: no good bins — skip (level2_signal_id=%s parent_signal_id=%s scenario_id=%s parent_run_id=%s winner=%s dir=%s score_version=%s winner_param='%s')",
            signal_id,
            parent_signal_id,
            scenario_id,
            parent_run_id,
            winner_analysis_id,
            direction,
            score_version,
            winner_param,
        )
        return

    # init plugin context (run-aware, run_id = parent_run_id)
    plugin_ctx = await init_plugin(
        pg=pg,
        run_id=int(parent_run_id),
        scenario_id=int(scenario_id),
        parent_signal_id=int(parent_signal_id),
        direction=str(direction),
        analysis_id=int(winner_analysis_id),
    )

    # создаём собственный run для level2 (один run на одно сообщение)
    level2_run_id = await _create_or_get_stream_run(
        pg=pg,
        signal_id=int(signal_id),
        from_time=window_from,
        to_time=window_to,
        origin_msg_id=str(origin_msg_id),
    )

    # список тикеров
    symbols = get_all_ticker_symbols()
    if not symbols:
        return

    log.debug(
        "BT_SIG_LR_UNI_L2: start — level2_signal_id=%s name='%s' pipeline_mode=%s scenario_id=%s parent_signal_id=%s parent_run_id=%s "
        "level2_run_id=%s winner_analysis_id=%s score_version=%s winner_param='%s' dir=%s plugin=%s window=[%s..%s] tickers=%s good_bins=%s origin_msg_id=%s",
        signal_id,
        name,
        pipeline_mode,
        scenario_id,
        parent_signal_id,
        parent_run_id,
        level2_run_id,
        winner_analysis_id,
        score_version,
        winner_param,
        direction,
        plugin_name,
        window_from,
        window_to,
        len(symbols),
        len(good_bins),
        origin_msg_id,
    )

    sema = asyncio.Semaphore(SYMBOL_MAX_CONCURRENCY)
    tasks: List[asyncio.Task] = []

    for symbol in symbols:
        tasks.append(
            asyncio.create_task(
                _process_symbol(
                    pg=pg,
                    sema=sema,
                    symbol=str(symbol),
                    timeframe="m5",
                    direction=str(direction),
                    # окна
                    window_from=window_from,
                    window_to=window_to,
                    # parent dataset
                    parent_run_id=int(parent_run_id),
                    parent_signal_id=int(parent_signal_id),
                    # target dataset
                    level2_run_id=int(level2_run_id),
                    level2_signal_id=int(signal_id),
                    scenario_id=int(scenario_id),
                    pipeline_mode=str(pipeline_mode),
                    # winner
                    winner_analysis_id=int(winner_analysis_id),
                    score_version=str(score_version),
                    winner_param=str(winner_param),
                    # plugin
                    plugin_name=str(plugin_name),
                    plugin_ctx=plugin_ctx,
                    prepare_series_fn=prepare_series_fn,
                    compute_bin_fn=compute_bin_fn,
                    good_bins=good_bins,
                ),
                name=f"BT_SIG_LR_UNI_L2_{pipeline_mode}_{signal_id}_{symbol}",
            )
        )

    status = "success"
    error_text: Optional[str] = None

    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)

        candidates_total = 0
        candidates_with_bin = 0
        candidates_good = 0
        membership_inserted = 0
        skipped_no_data = 0
        skipped_no_bin = 0
        skipped_not_good = 0

        for res in results:
            if isinstance(res, Exception):
                continue
            (
                c_total,
                c_with_bin,
                c_good,
                mem_inserted,
                s_no_data,
                s_no_bin,
                s_not_good,
            ) = res

            candidates_total += c_total
            candidates_with_bin += c_with_bin
            candidates_good += c_good
            membership_inserted += mem_inserted
            skipped_no_data += s_no_data
            skipped_no_bin += s_no_bin
            skipped_not_good += s_not_good

        log.info(
            "BT_SIG_LR_UNI_L2: done — level2_signal_id=%s level2_run_id=%s parent_signal_id=%s parent_run_id=%s scenario_id=%s dir=%s pipeline_mode=%s "
            "winner=%s score_version=%s winner_param='%s' plugin=%s good_bins=%s candidates=%s with_bin=%s good=%s membership_inserted=%s "
            "skipped_no_data=%s skipped_no_bin=%s skipped_not_good=%s",
            signal_id,
            level2_run_id,
            parent_signal_id,
            parent_run_id,
            scenario_id,
            direction,
            pipeline_mode,
            winner_analysis_id,
            score_version,
            winner_param,
            plugin_name,
            len(good_bins),
            candidates_total,
            candidates_with_bin,
            candidates_good,
            membership_inserted,
            skipped_no_data,
            skipped_no_bin,
            skipped_not_good,
        )

    except Exception as e:
        status = "error"
        error_text = str(e)
        log.error(
            "BT_SIG_LR_UNI_L2: failed — level2_signal_id=%s level2_run_id=%s err=%s",
            signal_id,
            level2_run_id,
            e,
            exc_info=True,
        )

    # фиксируем статус run
    try:
        await _finish_run(pg, int(level2_run_id), status=status, error=error_text)
    except Exception:
        pass

    # публикуем готовность датасета level2 (membership-датасет)
    finished_at = datetime.utcnow()
    try:
        await redis.xadd(
            BT_SIGNALS_READY_STREAM_V2,
            {
                "signal_id": str(int(signal_id)),
                "run_id": str(int(level2_run_id)),
                "from_time": window_from.isoformat(),
                "to_time": window_to.isoformat(),
                "finished_at": finished_at.isoformat(),
                "dataset_kind": "membership",
                "scenario_id": str(int(scenario_id)),
                "parent_run_id": str(int(parent_run_id)),
                "parent_signal_id": str(int(parent_signal_id)),
                "origin_msg_id": str(origin_msg_id),
                "signal_key": str(signal_key),
                "pipeline_mode": str(pipeline_mode),
                "winner_analysis_id": str(int(winner_analysis_id)),
                "score_version": str(score_version),
                "winner_param": str(winner_param),
            },
        )
    except Exception as e:
        log.error(
            "BT_SIG_LR_UNI_L2: failed to publish ready_v2 — signal_id=%s run_id=%s err=%s",
            signal_id,
            level2_run_id,
            e,
            exc_info=True,
        )


# 🔸 Обработка по символу: кандидаты из parent membership -> bin -> good_bins -> insert membership
async def _process_symbol(
    pg,
    sema: asyncio.Semaphore,
    symbol: str,
    timeframe: str,
    direction: str,
    window_from: datetime,
    window_to: datetime,
    parent_run_id: int,
    parent_signal_id: int,
    level2_run_id: int,
    level2_signal_id: int,
    scenario_id: int,
    pipeline_mode: str,
    winner_analysis_id: int,
    score_version: str,
    winner_param: str,
    plugin_name: str,
    plugin_ctx: Dict[str, Any],
    prepare_series_fn: Callable[..., Any],
    compute_bin_fn: Callable[..., Any],
    good_bins: Set[str],
) -> Tuple[int, int, int, int, int, int, int]:
    async with sema:
        # подготовка серий плагина (на основе symbol + окна)
        symbol_series = await prepare_series_fn(
            pg=pg,
            plugin_ctx=plugin_ctx,
            symbol=str(symbol),
            window_from=window_from,
            window_to=window_to,
        )

        # условий достаточности
        if not symbol_series:
            return 0, 0, 0, 0, 1, 0, 0

        # кандидаты = parent membership join events
        candidates = await _load_parent_candidates_for_symbol(
            pg=pg,
            parent_run_id=int(parent_run_id),
            parent_signal_id=int(parent_signal_id),
            symbol=str(symbol),
            timeframe=str(timeframe),
            direction=str(direction),
            window_from=window_from,
            window_to=window_to,
        )

        # условий достаточности
        if not candidates:
            return 0, 0, 0, 0, 0, 0, 0

        to_membership: List[Tuple[Any, ...]] = []

        with_bin = 0
        good = 0
        skipped_no_bin = 0
        skipped_not_good = 0

        for cand in candidates:
            bin_name = compute_bin_fn(
                plugin_ctx=plugin_ctx,
                symbol_series=symbol_series,
                candidate=cand,
            )

            if not bin_name:
                skipped_no_bin += 1
                continue

            with_bin += 1

            if bin_name not in good_bins:
                skipped_not_good += 1
                continue

            good += 1

            to_membership.append(
                (
                    int(level2_run_id),
                    int(level2_signal_id),
                    int(cand["signal_value_id"]),
                    int(scenario_id),
                    int(parent_run_id),
                    int(parent_signal_id),
                    int(winner_analysis_id),
                    str(score_version),
                    str(winner_param) if winner_param else None,
                    str(bin_name),
                    str(plugin_name),
                    str(plugin_ctx.get("param_name") or ""),
                    str(plugin_ctx.get("lr_prefix") or ""),
                    int(plugin_ctx.get("length") or 0),
                    str(pipeline_mode),
                )
            )

        mem_inserted = 0
        if to_membership:
            mem_inserted = await _insert_membership(pg, to_membership)

        return len(candidates), with_bin, good, mem_inserted, 0, skipped_no_bin, skipped_not_good