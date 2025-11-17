# 🔸 auditor_mw_state_worker.py — воркер анализа market_state по MW-стратегиям

# 🔸 Импорты
import logging
import datetime as dt
from collections import defaultdict
from typing import Dict, Any

import auditor_infra as infra


# 🔸 Логгер
log = logging.getLogger("AUD_MW_STATE")


# 🔸 Константы воркера
TIMEFRAMES = ("m5", "m15", "h1")
CHECK_TYPES = ("solo_straight", "solo_combo")


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
        # неизвестное состояние трактуем как запрет
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


# 🔸 Загрузка позиций с полным комплектом market_state (6 строк на позицию)
async def _load_positions_with_market_state(conn, sid_list: list[int]) -> Dict[str, Dict[str, Any]]:
    # условия достаточности
    if not sid_list:
        return {}

    rows = await conn.fetch(
        """
        WITH base_pos AS (
            SELECT
                p.position_uid,
                p.strategy_id,
                p.direction,
                p.pnl
            FROM positions_v4 p
            WHERE p.status = 'closed'
              AND p.strategy_id = ANY($1::int4[])
        ),
        complete_pos AS (
            SELECT
                s.position_uid
            FROM indicator_position_stat s
            JOIN base_pos bp ON bp.position_uid = s.position_uid
            WHERE s.param_type = 'marketwatch'
              AND s.param_base = 'market_state'
              AND s.status = 'ok'
              AND s.timeframe IN ('m5','m15','h1')
              AND s.param_name IN ('direction','quality')
            GROUP BY s.position_uid
            HAVING COUNT(*) = 6
        )
        SELECT
            bp.position_uid,
            bp.strategy_id,
            bp.direction AS pos_direction,
            bp.pnl,
            s.timeframe,
            s.param_name,
            s.value_text
        FROM base_pos bp
        JOIN complete_pos cp ON cp.position_uid = bp.position_uid
        JOIN indicator_position_stat s ON s.position_uid = bp.position_uid
        WHERE s.param_type = 'marketwatch'
          AND s.param_base = 'market_state'
          AND s.status = 'ok'
          AND s.timeframe IN ('m5','m15','h1')
          AND s.param_name IN ('direction','quality')
        ORDER BY bp.position_uid, s.timeframe, s.param_name
        """,
        sid_list,
    )

    positions: Dict[str, Dict[str, Any]] = {}

    for r in rows:
        position_uid = str(r["position_uid"])
        strategy_id = int(r["strategy_id"])
        pos_direction = str(r["pos_direction"])
        pnl = r["pnl"]

        timeframe = str(r["timeframe"])
        param_name = str(r["param_name"])
        value_text = str(r["value_text"]) if r["value_text"] is not None else None

        # инициализация записи позиции
        if position_uid not in positions:
            positions[position_uid] = {
                "position_uid": position_uid,
                "strategy_id": strategy_id,
                "direction": pos_direction,
                "pnl": pnl,
                "market_state": {tf: {} for tf in TIMEFRAMES},
            }

        # запись market_state по ТФ
        positions[position_uid]["market_state"][timeframe][param_name] = value_text

    log.info("🔍 AUD_MW_STATE: найдено позиций с полным market_state: %d", len(positions))
    return positions


# 🔸 Построение статистики (детальной и агрегированной)
def _build_stats(
    positions: Dict[str, Dict[str, Any]],
    strategies: Dict[int, Dict[str, Any]],
    calc_at: dt.datetime,
) -> tuple[list[tuple], list[tuple]]:
    # детальные счётчики: по варианту market_state
    detailed_counters: dict[
        tuple[int, str, str, str, str, str | None],
        Dict[str, Any],
    ] = defaultdict(lambda: {
        "total": 0,
        "passed": 0,
        "sum_before": 0,
        "sum_after": 0,
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
        "sum_before": 0,
        "sum_after": 0,
        "wins_before": 0,
        "wins_after": 0,
    })

    # обход всех позиций
    for pos in positions.values():
        strategy_id = int(pos["strategy_id"])
        pos_direction = str(pos["direction"])
        pnl = pos["pnl"] if pos["pnl"] is not None else 0
        is_win = pnl > 0
        ms_all = pos["market_state"]

        # обход по ТФ
        for timeframe in TIMEFRAMES:
            ms = ms_all.get(timeframe) or {}
            ms_direction = ms.get("direction")
            ms_quality = ms.get("quality")

            # пропуск, если чего-то не хватает (на всякий случай, хотя SQL уже отфильтровал)
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

    detailed_rows: list[tuple] = []
    aggregated_rows: list[tuple] = []

    # формирование детальных строк
    for key, det in detailed_counters.items():
        strategy_id, direction, timeframe, check_type, ms_direction, ms_quality = key
        total = det["total"]
        passed = det["passed"]
        filtered = total - passed

        wins_before = det["wins_before"]
        wins_after = det["wins_after"]
        sum_before = det["sum_before"]
        sum_after = det["sum_after"]

        winrate_before = _calc_winrate(wins_before, total)
        winrate_after = _calc_winrate(wins_after, passed)

        deposit = strategies.get(strategy_id, {}).get("deposit")
        if deposit is None or deposit == 0:
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

    # формирование агрегированных строк
    for key, agg in aggregated_counters.items():
        strategy_id, direction, timeframe, check_type = key
        total = agg["total"]
        passed = agg["passed"]
        filtered = total - passed

        wins_before = agg["wins_before"]
        wins_after = agg["wins_after"]
        sum_before = agg["sum_before"]
        sum_after = agg["sum_after"]

        winrate_before = _calc_winrate(wins_before, total)
        winrate_after = _calc_winrate(wins_after, passed)

        deposit = strategies.get(strategy_id, {}).get("deposit")
        if deposit is None or deposit == 0:
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

        sid_list = list(strategies.keys())

        # загрузка позиций с полным market_state
        positions = await _load_positions_with_market_state(conn, sid_list)
        if not positions:
            log.info("❌ AUD_MW_STATE: нет позиций с полным market_state — выход")
            return

        # расчёт статистики
        detailed_rows, aggregated_rows = _build_stats(positions, strategies, calc_at)

        # запись в БД
        await _insert_detailed_rows(conn, detailed_rows)
        await _insert_aggregated_rows(conn, aggregated_rows)

    log.info(
        "✅ AUD_MW_STATE: завершено — стратегий=%d, позиций=%d, детальных строк=%d, агрегированных строк=%d",
        len(strategies),
        len(positions),
        len(detailed_rows),
        len(aggregated_rows),
    )