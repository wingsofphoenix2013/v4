# 🔸 auditor_mwstat_worker.py — воркер анализа MW-фильтров (m5) по тренду/воле/моментуму

# 🔸 Импорты
import logging
import datetime as dt
from collections import defaultdict
from typing import Dict, Any, Tuple

import auditor_infra as infra


# 🔸 Логгер
log = logging.getLogger("AUD_MWSTAT")


# 🔸 Константы воркера
TIMEFRAME = "m5"
BATCH_SIZE = 200

FILTER_TYPES = (
    "ban_trend_m5",
    "ban_vola_m5",
    "ban_momalign_m5",
    "ban_momentum_m5",
    "mw_m5_combo",
)


# 🔸 Вспомогательные функции

def _calc_winrate(wins: int, total: int) -> float | None:
    if total <= 0:
        return None
    return wins / total


def _compute_bans(
    trend_state: str | None,
    vola_state: str | None,
    mom_align_state: str | None,
    momentum_state: str | None,
) -> Tuple[int, int, int, int, int]:
    # ban_trend_m5 = 1, если trend_state_m5 = 'sideways'
    ban_trend_m5 = 1 if trend_state == "sideways" else 0

    # ban_vola_m5 = 1, если vola_state_m5 = 'low_squeeze'
    ban_vola_m5 = 1 if vola_state == "low_squeeze" else 0

    # ban_momalign_m5 = 1, если mom_align_state_m5 = 'countertrend'
    ban_momalign_m5 = 1 if mom_align_state == "countertrend" else 0

    # ban_momentum_m5 = 1, если momentum_state_m5 = 'divergence_flat'
    ban_momentum_m5 = 1 if momentum_state == "divergence_flat" else 0

    # allow_m5_combo = 1, если все баны = 0, иначе 0
    if ban_trend_m5 or ban_vola_m5 or ban_momalign_m5 or ban_momentum_m5:
        allow_m5_combo = 0
    else:
        allow_m5_combo = 1

    return ban_trend_m5, ban_vola_m5, ban_momalign_m5, ban_momentum_m5, allow_m5_combo


def _init_counters_for_strategy() -> Tuple[
    Dict[tuple, Dict[str, Any]],
    Dict[tuple, Dict[str, Any]],
    Dict[tuple, Dict[str, Any]],
]:
    # детальные счётчики: по MW-состояниям + filter_type
    detailed_counters: dict[
        tuple[int, str, str, str, str, str, str, str],
        Dict[str, Any],
    ] = defaultdict(lambda: {
        "total": 0,
        "passed": 0,
        "sum_before": 0,
        "sum_after": 0,
        "wins_before": 0,
        "wins_after": 0,
    })

    # агрегированные счётчики: по фильтру в целом
    aggregated_counters: dict[
        tuple[int, str, str, str],
        Dict[str, Any],
    ] = defaultdict(lambda: {
        "total": 0,
        "passed": 0,
        "sum_before": 0,
        "sum_after": 0,
        "wins_before": 0,
        "wins_after": 0,
    })

    # суточные счётчики: по дате закрытия сделки и фильтру
    daily_counters: dict[
        tuple[int, str, dt.date, str, str],
        Dict[str, Any],
    ] = defaultdict(lambda: {
        "total": 0,
        "passed": 0,
        "sum_before": 0,
        "sum_after": 0,
        "wins_before": 0,
        "wins_after": 0,
    })

    return detailed_counters, aggregated_counters, daily_counters


def _filter_applies(filter_type: str,
                    ban_trend_m5: int,
                    ban_vola_m5: int,
                    ban_momalign_m5: int,
                    ban_momentum_m5: int,
                    allow_m5_combo: int) -> bool:
    # Проверка, запрещает ли конкретный фильтр эту сделку
    if filter_type == "ban_trend_m5":
        return ban_trend_m5 == 1
    if filter_type == "ban_vola_m5":
        return ban_vola_m5 == 1
    if filter_type == "ban_momalign_m5":
        return ban_momalign_m5 == 1
    if filter_type == "ban_momentum_m5":
        return ban_momentum_m5 == 1
    if filter_type == "mw_m5_combo":
        return allow_m5_combo == 0
    return False


def _update_stats_for_position(
    pos: Dict[str, Any],
    mw_state: Dict[str, str],
    trade_date: dt.date,
    detailed_counters: Dict[tuple, Dict[str, Any]],
    aggregated_counters: Dict[tuple, Dict[str, Any]],
    daily_counters: Dict[tuple, Dict[str, Any]],
) -> None:
    strategy_id = int(pos["strategy_id"])
    pos_direction = str(pos["direction"])
    pnl = pos["pnl"] if pos["pnl"] is not None else 0
    is_win = pnl > 0

    trend_state = mw_state.get("trend_state_m5")
    vola_state = mw_state.get("vola_state_m5")
    mom_align_state = mw_state.get("mom_align_state_m5")
    momentum_state = mw_state.get("momentum_state_m5")

    # условия достаточности: все четыре состояния должны присутствовать
    if not (trend_state and vola_state and mom_align_state and momentum_state):
        return

    ban_trend_m5, ban_vola_m5, ban_momalign_m5, ban_momentum_m5, allow_m5_combo = _compute_bans(
        trend_state,
        vola_state,
        mom_align_state,
        momentum_state,
    )

    # обход по всем фильтрам
    for filter_type in FILTER_TYPES:
        filtered = _filter_applies(
            filter_type,
            ban_trend_m5,
            ban_vola_m5,
            ban_momalign_m5,
            ban_momentum_m5,
            allow_m5_combo,
        )
        passed = not filtered

        # детальный ключ
        det_key = (
            strategy_id,
            pos_direction,
            TIMEFRAME,
            filter_type,
            trend_state,
            vola_state,
            mom_align_state,
            momentum_state,
        )
        det = detailed_counters[det_key]
        det["total"] += 1
        det["sum_before"] += pnl
        if is_win:
            det["wins_before"] += 1
        if passed:
            det["passed"] += 1
            det["sum_after"] += pnl
            if is_win:
                det["wins_after"] += 1

        # агрегированный ключ
        agg_key = (strategy_id, pos_direction, TIMEFRAME, filter_type)
        agg = aggregated_counters[agg_key]
        agg["total"] += 1
        det["sum_before"]
        agg["sum_before"] += pnl
        if is_win:
            agg["wins_before"] += 1
        if passed:
            agg["passed"] += 1
            agg["sum_after"] += pnl
            if is_win:
                agg["wins_after"] += 1

        # суточный ключ
        day_key = (strategy_id, pos_direction, trade_date, TIMEFRAME, filter_type)
        day = daily_counters[day_key]
        day["total"] += 1
        day["sum_before"] += pnl
        if is_win:
            day["wins_before"] += 1
        if passed:
            day["passed"] += 1
            day["sum_after"] += pnl
            if is_win:
                day["wins_after"] += 1


# 🔸 Загрузка MW-стратегий (market_watcher = true)
async def _load_mw_strategies(conn) -> Dict[int, Dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT id, deposit
        FROM strategies_v4
        WHERE enabled = true
          AND (archived IS NOT TRUE)
          AND market_watcher = true
        """
    )

    strategies: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        sid = int(r["id"])
        strategies[sid] = {
            "id": sid,
            "deposit": r["deposit"],
        }

    log.info("🔍 AUD_MWSTAT: найдено MW-стратегий для анализа: %d", len(strategies))
    return strategies


# 🔸 Обработка позиций одной стратегии батчами
async def _process_strategy_positions(
    conn,
    strategy_id: int,
    detailed_counters: Dict[tuple, Dict[str, Any]],
    aggregated_counters: Dict[tuple, Dict[str, Any]],
    daily_counters: Dict[tuple, Dict[str, Any]],
) -> Tuple[int, int]:
    last_id = 0
    total_positions = 0
    used_positions = 0

    while True:
        rows = await conn.fetch(
            """
            SELECT id, position_uid, direction, pnl, closed_at
            FROM positions_v4
            WHERE status = 'closed'
              AND strategy_id = $1
              AND id > $2
            ORDER BY id
            LIMIT $3
            """,
            strategy_id,
            last_id,
            BATCH_SIZE,
        )

        if not rows:
            break

        positions_batch = []
        position_uids: list[str] = []

        for r in rows:
            pid = int(r["id"])
            position_uid = str(r["position_uid"])
            pos_direction = str(r["direction"])
            pnl = r["pnl"]
            closed_at = r["closed_at"]

            positions_batch.append(
                {
                    "id": pid,
                    "position_uid": position_uid,
                    "direction": pos_direction,
                    "pnl": pnl,
                    "strategy_id": strategy_id,
                    "closed_at": closed_at,
                }
            )
            position_uids.append(position_uid)

            if pid > last_id:
                last_id = pid

        total_positions += len(positions_batch)

        if not position_uids:
            continue

        # загрузка MW-состояний по батчу позиций
        ind_rows = await conn.fetch(
            """
            SELECT position_uid, param_base, param_name, value_text
            FROM indicator_position_stat
            WHERE position_uid = ANY($1::text[])
              AND param_type = 'marketwatch'
              AND timeframe = 'm5'
              AND status = 'ok'
              AND param_base IN ('trend','volatility','mom_align','momentum')
              AND param_name = 'state'
            """,
            position_uids,
        )

        # структура {position_uid: {trend_state_m5: ..., vola_state_m5: ..., ...}}
        mw_map: Dict[str, Dict[str, str]] = {}

        for r in ind_rows:
            puid = str(r["position_uid"])
            param_base = str(r["param_base"])
            value_text = str(r["value_text"]) if r["value_text"] is not None else None

            entry = mw_map.setdefault(puid, {})

            if param_base == "trend":
                entry["trend_state_m5"] = value_text
            elif param_base == "volatility":
                entry["vola_state_m5"] = value_text
            elif param_base == "mom_align":
                entry["mom_align_state_m5"] = value_text
            elif param_base == "momentum":
                entry["momentum_state_m5"] = value_text

        # обход позиций батча
        for pos in positions_batch:
            puid = pos["position_uid"]
            mw_state = mw_map.get(puid)

            if not mw_state:
                continue

            # проверяем наличие всех четырёх состояний
            if not all(
                key in mw_state
                for key in ("trend_state_m5", "vola_state_m5", "mom_align_state_m5", "momentum_state_m5")
            ):
                continue

            closed_at = pos["closed_at"]
            if not closed_at:
                continue

            if isinstance(closed_at, dt.datetime):
                trade_date = closed_at.date()
            else:
                trade_date = closed_at

            used_positions += 1
            _update_stats_for_position(
                pos,
                mw_state,
                trade_date,
                detailed_counters,
                aggregated_counters,
                daily_counters,
            )

    log.info(
        "🔍 AUD_MWSTAT: стратегия %d — позиций всего=%d, с полным MW m5=%d",
        strategy_id,
        total_positions,
        used_positions,
    )

    return total_positions, used_positions


# 🔸 Построение строк для вставки по одной стратегии
def _build_rows_for_strategy(
    strategy_id: int,
    detailed_counters: Dict[tuple, Dict[str, Any]],
    aggregated_counters: Dict[tuple, Dict[str, Any]],
    daily_counters: Dict[tuple, Dict[str, Any]],
    strategies: Dict[int, Dict[str, Any]],
    calc_at: dt.datetime,
) -> tuple[list[tuple], list[tuple], list[tuple]]:
    detailed_rows: list[tuple] = []
    aggregated_rows: list[tuple] = []
    daily_rows: list[tuple] = []

    deposit = strategies.get(strategy_id, {}).get("deposit")

    # детальные строки
    for key, det in detailed_counters.items():
        sid, direction, timeframe, filter_type, trend_state, vola_state, mom_align_state, momentum_state = key
        if sid != strategy_id:
            continue

        total = det["total"]
        if total <= 0:
            continue

        passed = det["passed"]
        filtered = total - passed

        wins_before = det["wins_before"]
        wins_after = det["wins_after"]
        sum_before = det["sum_before"]
        sum_after = det["sum_after"]

        winrate_before = _calc_winrate(wins_before, total)
        winrate_after = _calc_winrate(wins_after, passed)

        ban_trend_m5, ban_vola_m5, ban_momalign_m5, ban_momentum_m5, allow_m5_combo = _compute_bans(
            trend_state,
            vola_state,
            mom_align_state,
            momentum_state,
        )

        if not deposit or deposit == 0:
            roi_before = None
            roi_after = None
        else:
            roi_before = sum_before / deposit
            roi_after = sum_after / deposit

        detailed_rows.append(
            (
                calc_at,
                strategy_id,
                direction,
                timeframe,
                filter_type,
                trend_state,
                vola_state,
                mom_align_state,
                momentum_state,
                ban_trend_m5,
                ban_vola_m5,
                ban_momalign_m5,
                ban_momentum_m5,
                allow_m5_combo,
                total,
                filtered,
                passed,
                winrate_before,
                winrate_after,
                sum_before,
                sum_after,
                roi_before,
                roi_after,
            )
        )

    # агрегированные строки
    for key, agg in aggregated_counters.items():
        sid, direction, timeframe, filter_type = key
        if sid != strategy_id:
            continue

        total = agg["total"]
        if total <= 0:
            continue

        passed = agg["passed"]
        filtered = total - passed

        wins_before = agg["wins_before"]
        wins_after = agg["wins_after"]
        sum_before = agg["sum_before"]
        sum_after = agg["sum_after"]

        winrate_before = _calc_winrate(wins_before, total)
        winrate_after = _calc_winrate(wins_after, passed)

        if not deposit or deposit == 0:
            roi_before = None
            roi_after = None
        else:
            roi_before = sum_before / deposit
            roi_after = sum_after / deposit

        aggregated_rows.append(
            (
                calc_at,
                strategy_id,
                direction,
                timeframe,
                filter_type,
                total,
                filtered,
                passed,
                winrate_before,
                winrate_after,
                sum_before,
                sum_after,
                roi_before,
                roi_after,
            )
        )

    # суточные строки
    for key, day in daily_counters.items():
        sid, direction, trade_date, timeframe, filter_type = key
        if sid != strategy_id:
            continue

        total = day["total"]
        if total <= 0:
            continue

        passed = day["passed"]
        filtered = total - passed

        wins_before = day["wins_before"]
        wins_after = day["wins_after"]
        sum_before = day["sum_before"]
        sum_after = day["sum_after"]

        winrate_before = _calc_winrate(wins_before, total)
        winrate_after = _calc_winrate(wins_after, passed)

        if not deposit or deposit == 0:
            roi_before = None
            roi_after = None
        else:
            roi_before = sum_before / deposit
            roi_after = sum_after / deposit

        daily_rows.append(
            (
                calc_at,
                strategy_id,
                direction,
                timeframe,
                filter_type,
                trade_date,
                total,
                filtered,
                passed,
                winrate_before,
                winrate_after,
                sum_before,
                sum_after,
                roi_before,
                roi_after,
            )
        )

    return detailed_rows, aggregated_rows, daily_rows


# 🔸 Запись результатов в БД
async def _insert_detailed_rows(conn, rows: list[tuple]) -> None:
    if not rows:
        return

    await conn.executemany(
        """
        INSERT INTO auditor_mwstat_detailed (
            calc_at,
            strategy_id,
            direction,
            timeframe,
            filter_type,
            trend_state_m5,
            vola_state_m5,
            mom_align_state_m5,
            momentum_state_m5,
            ban_trend_m5,
            ban_vola_m5,
            ban_momalign_m5,
            ban_momentum_m5,
            allow_m5_combo,
            total_trades,
            filtered_trades,
            passed_trades,
            winrate_before,
            winrate_after,
            sum_pnl_before,
            sum_pnl_after,
            roi_before,
            roi_after
        )
        VALUES (
            $1,$2,$3,$4,$5,
            $6,$7,$8,$9,
            $10,$11,$12,$13,$14,
            $15,$16,$17,
            $18,$19,
            $20,$21,
            $22,$23
        )
        """,
        rows,
    )


async def _insert_aggregated_rows(conn, rows: list[tuple]) -> None:
    if not rows:
        return

    await conn.executemany(
        """
        INSERT INTO auditor_mwstat_aggregated (
            calc_at,
            strategy_id,
            direction,
            timeframe,
            filter_type,
            total_trades,
            filtered_trades,
            passed_trades,
            winrate_before,
            winrate_after,
            sum_pnl_before,
            sum_pnl_after,
            roi_before,
            roi_after
        )
        VALUES (
            $1,$2,$3,$4,$5,
            $6,$7,$8,
            $9,$10,
            $11,$12,
            $13,$14
        )
        """,
        rows,
    )


async def _insert_daily_rows(conn, rows: list[tuple]) -> None:
    if not rows:
        return

    await conn.executemany(
        """
        INSERT INTO auditor_mwstat_daily (
            calc_at,
            strategy_id,
            direction,
            timeframe,
            filter_type,
            trade_date,
            total_trades,
            filtered_trades,
            passed_trades,
            winrate_before,
            winrate_after,
            sum_pnl_before,
            sum_pnl_after,
            roi_before,
            roi_after
        )
        VALUES (
            $1,$2,$3,$4,$5,$6,
            $7,$8,$9,
            $10,$11,
            $12,$13,
            $14,$15
        )
        """,
        rows,
    )


# 🔸 Основная корутина воркера
async def run_mwstat_worker():
    if infra.pg_pool is None:
        log.info("❌ AUD_MWSTAT: пропуск воркера — PG не инициализирован")
        return

    calc_at = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    log.info("🚀 AUD_MWSTAT: старт расчёта MW-фильтров (calc_at=%s)", calc_at)

    async with infra.pg_pool.acquire() as conn:
        strategies = await _load_mw_strategies(conn)
        if not strategies:
            log.info("❌ AUD_MWSTAT: нет MW-стратегий для анализа — выход")
            return

        total_positions_all = 0
        used_positions_all = 0
        total_detailed_rows_all = 0
        total_aggregated_rows_all = 0
        total_daily_rows_all = 0

        for strategy_id in sorted(strategies.keys()):
            detailed_counters, aggregated_counters, daily_counters = _init_counters_for_strategy()

            log.info("🔧 AUD_MWSTAT: стратегия %d — старт обработки", strategy_id)

            total_pos, used_pos = await _process_strategy_positions(
                conn,
                strategy_id,
                detailed_counters,
                aggregated_counters,
                daily_counters,
            )

            detailed_rows, aggregated_rows, daily_rows = _build_rows_for_strategy(
                strategy_id,
                detailed_counters,
                aggregated_counters,
                daily_counters,
                strategies,
                calc_at,
            )

            await _insert_detailed_rows(conn, detailed_rows)
            await _insert_aggregated_rows(conn, aggregated_rows)
            await _insert_daily_rows(conn, daily_rows)

            log.info(
                "✅ AUD_MWSTAT: стратегия %d — позиций_всего=%d, позиций_с_MW=%d, "
                "детальных строк=%d, агрегированных строк=%d, суточных строк=%d",
                strategy_id,
                total_pos,
                used_pos,
                len(detailed_rows),
                len(aggregated_rows),
                len(daily_rows),
            )

            total_positions_all += total_pos
            used_positions_all += used_pos
            total_detailed_rows_all += len(detailed_rows)
            total_aggregated_rows_all += len(aggregated_rows)
            total_daily_rows_all += len(daily_rows)

    log.info(
        "✅ AUD_MWSTAT: завершено — стратегий=%d, позиций_всего=%d, позиций_с_MW=%d, "
        "детальных строк=%d, агрегированных строк=%d, суточных строк=%d",
        len(strategies),
        total_positions_all,
        used_positions_all,
        total_detailed_rows_all,
        total_aggregated_rows_all,
        total_daily_rows_all,
    )