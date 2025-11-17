# 🔸 auditor_mw_state_worker.py — воркер анализа market_state по MW-стратегиям

# 🔸 Импорты
import logging
import datetime as dt
from collections import defaultdict
from typing import Dict, Any, Tuple

import auditor_infra as infra


# 🔸 Логгер
log = logging.getLogger("AUD_MW_STATE")


# 🔸 Константы воркера
TIMEFRAMES = ("m5", "m15", "h1")
CHECK_TYPES = ("solo_straight", "solo_combo")
BATCH_SIZE = 200


# 🔸 Вспомогательные функции

def _is_passed(check_type: str, ms_direction: str, ms_quality: str | None, pos_direction: str) -> bool:
    # базовая проверка направления
    if ms_direction == "both":
        allowed_dir = True
    elif ms_direction == "long_only":
        allowed_dir = (pos_direction == "long")
    elif ms_direction == "short_only":
        allowed_dir = (pos_direction == "short")
    else:
        allowed_dir = False

    # простой режим: только направление
    if check_type == "solo_straight":
        return allowed_dir

    # комбинированный режим: направление + вето quality
    if ms_quality == "avoid":
        return False

    return allowed_dir


def _calc_winrate(wins: int, total: int) -> float | None:
    # защита от деления на ноль
    if total <= 0:
        return None
    return wins / total


def _init_counters_for_strategy() -> Tuple[
    Dict[tuple, Dict[str, Any]],
    Dict[tuple, Dict[str, Any]],
]:
    # детальные счётчики: по варианту market_state
    detailed_counters: dict[
        tuple[int, str, str, str, str, str | None],
        Dict[str, Any],
    ] = defaultdict(lambda: {
        "total": 0,
        "passed": 0,
        "sum_before": 0.0,
        "sum_after": 0.0,
        "wins_before": 0,
        "wins_after": 0,
    })

    # агрегированные счётчики: по фильтру в целом (без разбиения на варианты)
    aggregated_counters: dict[
        tuple[int, str, str, str],
        Dict[str, Any],
    ] = defaultdict(lambda: {
        "total": 0,
        "passed": 0,
        "sum_before": 0.0,
        "sum_after": 0.0,
        "wins_before": 0,
        "wins_after": 0,
    })

    return detailed_counters, aggregated_counters


def _update_stats_for_position(
    pos: Dict[str, Any],
    ms_by_tf: Dict[str, Dict[str, str]],
    detailed_counters: Dict[tuple, Dict[str, Any]],
    aggregated_counters: Dict[tuple, Dict[str, Any]],
) -> None:
    strategy_id = int(pos["strategy_id"])
    pos_direction = str(pos["direction"])
    pnl = pos["pnl"] if pos["pnl"] is not None else 0
    is_win = pnl > 0

    # обход по ТФ
    for timeframe in TIMEFRAMES:
        tf_ms = ms_by_tf.get(timeframe) or {}
        ms_direction = tf_ms.get("direction")
        ms_quality = tf_ms.get("quality")

        # пропуск, если чего-то не хватает
        if not ms_direction:
            continue

        # обход по типам проверки
        for check_type in CHECK_TYPES:
            passed = _is_passed(check_type, ms_direction, ms_quality, pos_direction)

            # для solo_straight quality в детальной строке не фиксируем
            eff_ms_quality = ms_quality if check_type == "solo_combo" else None

            # ключ детальной статистики
            det_key = (strategy_id, pos_direction, timeframe, check_type, ms_direction, eff_ms_quality)
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

            # ключ агрегированной статистики
            agg_key = (strategy_id, pos_direction, timeframe, check_type)
            agg = aggregated_counters[agg_key]
            agg["total"] += 1
            agg["sum_before"] += pnl
            if is_win:
                agg["wins_before"] += 1
            if passed:
                agg["passed"] += 1
                agg["sum_after"] += pnl
                if is_win:
                    agg["wins_after"] += 1


# 🔸 Загрузка MW-стратегий (deposit нужен для ROI)
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

    log.info("🔍 AUD_MW_STATE: найдено MW-стратегий для анализа: %d", len(strategies))
    return strategies


# 🔸 Обработка позиций одной стратегии батчами
async def _process_strategy_positions(
    conn,
    strategy_id: int,
    detailed_counters: Dict[tuple, Dict[str, Any]],
    aggregated_counters: Dict[tuple, Dict[str, Any]],
) -> Tuple[int, int]:
    last_id = 0
    total_positions = 0
    used_positions = 0

    # обработка батчами по id
    while True:
        rows = await conn.fetch(
            """
            SELECT id, position_uid, direction, pnl
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

        # подготовка батча позиций
        for r in rows:
            pid = int(r["id"])
            position_uid = str(r["position_uid"])
            pos_direction = str(r["direction"])
            pnl = r["pnl"]

            positions_batch.append(
                {
                    "id": pid,
                    "position_uid": position_uid,
                    "direction": pos_direction,
                    "pnl": pnl,
                    "strategy_id": strategy_id,
                }
            )
            position_uids.append(position_uid)

            if pid > last_id:
                last_id = pid

        total_positions += len(positions_batch)

        # защита от пустого батча
        if not position_uids:
            continue

        # загрузка market_state по батчу позицій
        ind_rows = await conn.fetch(
            """
            SELECT position_uid, timeframe, param_name, value_text
            FROM indicator_position_stat
            WHERE position_uid = ANY($1::text[])
              AND param_type = 'marketwatch'
              AND param_base = 'market_state'
              AND status = 'ok'
              AND timeframe IN ('m5','m15','h1')
              AND param_name IN ('direction','quality')
            """,
            position_uids,
        )

        # укладка market_state в структуру {position_uid: {tf: {param_name: value}}}
        ms_map: Dict[str, Dict[str, Dict[str, str]]] = {}

        for r in ind_rows:
            puid = str(r["position_uid"])
            timeframe = str(r["timeframe"])
            param_name = str(r["param_name"])
            value_text = str(r["value_text"]) if r["value_text"] is not None else None

            ms_map.setdefault(puid, {}).setdefault(timeframe, {})[param_name] = value_text

        # обход позиций батча с проверкой полноты market_state
        for pos in positions_batch:
            puid = pos["position_uid"]
            ms_by_tf = ms_map.get(puid)

            # если нет вообще записей по этой позиции — пропуск
            if not ms_by_tf:
                continue

            complete = True
            for tf in TIMEFRAMES:
                tf_ms = ms_by_tf.get(tf)
                if not tf_ms or "direction" not in tf_ms or "quality" not in tf_ms:
                    complete = False
                    break

            if not complete:
                continue

            # позиция с полным market_state — учитываем в статистике
            used_positions += 1
            _update_stats_for_position(pos, ms_by_tf, detailed_counters, aggregated_counters)

    log.info(
        "🔍 AUD_MW_STATE: стратегия %d — позиций всего=%d, с полным market_state=%d",
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
    strategies: Dict[int, Dict[str, Any]],
    calc_at: dt.datetime,
) -> tuple[list[tuple], list[tuple]]:
    detailed_rows: list[tuple] = []
    aggregated_rows: list[tuple] = []

    deposit = strategies.get(strategy_id, {}).get("deposit")

    # формирование детальных строк только по этой стратегии
    for key, det in detailed_counters.items():
        sid, direction, timeframe, check_type, ms_direction, ms_quality = key
        if sid != strategy_id:
            continue

        total = det["total"]
        passed = det["passed"]
        filtered = total - passed

        wins_before = det["wins_before"]
        wins_after = det["wins_after"]
        sum_before = det["sum_before"]
        sum_after = det["sum_after"]

        winrate_before = _calc_winrate(wins_before, total)
        winrate_after = _calc_winrate(wins_after, passed)

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
                check_type,
                ms_direction,
                ms_quality,
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

    # формирование агрегированных строк только по этой стратегии
    for key, agg in aggregated_counters.items():
        sid, direction, timeframe, check_type = key
        if sid != strategy_id:
            continue

        total = agg["total"]
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
                check_type,
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

    return detailed_rows, aggregated_rows


# 🔸 Запись результатов в БД
async def _insert_detailed_rows(conn, rows: list[tuple]) -> None:
    # условия достаточности
    if not rows:
        return

    await conn.executemany(
        """
        INSERT INTO auditor_mw_state_detailed (
            calc_at,
            strategy_id,
            direction,
            timeframe,
            check_type,
            ms_direction,
            ms_quality,
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
            $1,$2,$3,$4,$5,$6,$7,
            $8,$9,$10,
            $11,$12,
            $13,$14,
            $15,$16
        )
        """,
        rows,
    )


async def _insert_aggregated_rows(conn, rows: list[tuple]) -> None:
    # условия достаточности
    if not rows:
        return

    await conn.executemany(
        """
        INSERT INTO auditor_mw_state_aggregated (
            calc_at,
            strategy_id,
            direction,
            timeframe,
            check_type,
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


# 🔸 Основная корутина воркера
async def run_mw_state_worker():
    # условия достаточности
    if infra.pg_pool is None:
        log.info("❌ AUD_MW_STATE: пропуск воркера — PG не инициализирован")
        return

    calc_at = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    log.info("🚀 AUD_MW_STATE: старт расчёта market_state (calc_at=%s)", calc_at)

    async with infra.pg_pool.acquire() as conn:
        # загрузка стратегий
        strategies = await _load_mw_strategies(conn)
        if not strategies:
            log.info("❌ AUD_MW_STATE: нет MW-стратегий для анализа — выход")
            return

        total_positions_all = 0
        used_positions_all = 0
        total_detailed_rows_all = 0
        total_aggregated_rows_all = 0

        # обход стратегий по одной
        for strategy_id in sorted(strategies.keys()):
            # инициализация счётчиков для стратегии
            detailed_counters, aggregated_counters = _init_counters_for_strategy()

            log.info("🔧 AUD_MW_STATE: стратегия %d — старт обработки", strategy_id)

            total_pos, used_pos = await _process_strategy_positions(
                conn,
                strategy_id,
                detailed_counters,
                aggregated_counters,
            )

            # построение строк для конкретной стратегии
            detailed_rows, aggregated_rows = _build_rows_for_strategy(
                strategy_id,
                detailed_counters,
                aggregated_counters,
                strategies,
                calc_at,
            )

            # запись в БД
            await _insert_detailed_rows(conn, detailed_rows)
            await _insert_aggregated_rows(conn, aggregated_rows)

            log.info(
                "✅ AUD_MW_STATE: стратегия %d — позиций_всего=%d, позиций_с_полным_ms=%d, "
                "детальных строк=%d, агрегированных строк=%d",
                strategy_id,
                total_pos,
                used_pos,
                len(detailed_rows),
                len(aggregated_rows),
            )

            total_positions_all += total_pos
            used_positions_all += used_pos
            total_detailed_rows_all += len(detailed_rows)
            total_aggregated_rows_all += len(aggregated_rows)

    log.info(
        "✅ AUD_MW_STATE: завершено — стратегий=%d, позиций_всего=%d, позиций_с_полным_ms=%d, "
        "детальных строк=%d, агрегированных строк=%d",
        len(strategies),
        total_positions_all,
        used_positions_all,
        total_detailed_rows_all,
        total_aggregated_rows_all,
    )