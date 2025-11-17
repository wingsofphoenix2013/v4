# 🔸 auditor_ema21short_worker.py — воркер анализа PACK ema21 (m5) для шорт-фильтра

# 🔸 Импорты
import logging
import datetime as dt
from collections import defaultdict
from typing import Dict, Any, Tuple

import auditor_infra as infra


# 🔸 Логгер
log = logging.getLogger("AUD_EMA21_SHORT")


# 🔸 Константы воркера
TIMEFRAME = "m5"
FILTER_TYPE = "ema21_short"
PACK_BASE = "ema21"
BATCH_SIZE = 200


# 🔸 Вспомогательные функции

def _calc_winrate(wins: int, total: int) -> float | None:
    # условия достаточности
    if total <= 0:
        return None
    return wins / total


def _init_counters_for_strategy() -> Tuple[
    Dict[tuple, Dict[str, Any]],
    Dict[tuple, Dict[str, Any]],
    Dict[tuple, Dict[str, Any]],
]:
    # детальные счётчики: по состояниям PACK ema21 (side + dynamic_smooth)
    detailed_counters: dict[
        tuple[int, str, str, str, str, str],
        Dict[str, Any],
    ] = defaultdict(lambda: {
        "total": 0,
        "passed": 0,
        "sum_before": 0,   # int + Decimal -> Decimal
        "sum_after": 0,
        "wins_before": 0,
        "wins_after": 0,
    })

    # агрегированные счётчики: по фильтру ema21_short в целом
    aggregated_counters: dict[
        tuple[int, str, str, str, str],
        Dict[str, Any],
    ] = defaultdict(lambda: {
        "total": 0,
        "passed": 0,
        "sum_before": 0,
        "sum_after": 0,
        "wins_before": 0,
        "wins_after": 0,
    })

    # суточные счётчики: по дате закрытия сделки
    daily_counters: dict[
        tuple[int, str, dt.date, str, str, str],
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


def _is_short_allowed(side: str | None, dyn_smooth: str | None) -> bool:
    # условия достаточности
    if side is None or dyn_smooth is None:
        return False

    # разрешённые состояния:
    # side ∈ {'above', 'equal'}
    # dynamic_smooth ∈ {
    #   'above_approaching', 'above_stable',
    #   'equal_approaching', 'equal_stable'
    # }
    if side not in ("above", "equal"):
        return False

    allowed_dyn = {
        "above_approaching",
        "above_stable",
        "equal_approaching",
        "equal_stable",
    }

    return dyn_smooth in allowed_dyn


def _update_stats_for_position(
    pos: Dict[str, Any],
    ema_state: Dict[str, str],
    trade_date: dt.date,
    detailed_counters: Dict[tuple, Dict[str, Any]],
    aggregated_counters: Dict[tuple, Dict[str, Any]],
    daily_counters: Dict[tuple, Dict[str, Any]],
) -> None:
    strategy_id = int(pos["strategy_id"])
    pos_direction = str(pos["direction"])
    pnl = pos["pnl"] if pos["pnl"] is not None else 0
    is_win = pnl > 0

    side = ema_state.get("side")
    dyn_smooth = ema_state.get("dynamic_smooth")

    # условия достаточности: работаем только с полным пакетом
    if not side or not dyn_smooth:
        return

    # фильтр только для шортов; для лонгов статистику не считаем
    if pos_direction != "short":
        return

    passed = _is_short_allowed(side, dyn_smooth)

    # 🔹 детальная статистика по состоянию ema21
    det_key = (strategy_id, pos_direction, TIMEFRAME, FILTER_TYPE, side, dyn_smooth)
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

    # 🔹 агрегированная статистика по фильтру в целом
    agg_key = (strategy_id, pos_direction, TIMEFRAME, FILTER_TYPE, PACK_BASE)
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

    # 🔹 суточная статистика по дате закрытия
    day_key = (strategy_id, pos_direction, trade_date, TIMEFRAME, FILTER_TYPE, PACK_BASE)
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


# 🔸 Загрузка MW-стратегий (используем те же market_watcher=true)
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

    log.info("🔍 AUD_EMA21_SHORT: найдено MW-стратегий для анализа: %d", len(strategies))
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

    # обработка батчами по id
    while True:
        rows = await conn.fetch(
            """
            SELECT id, position_uid, direction, pnl, closed_at
            FROM positions_v4
            WHERE status = 'closed'
              AND strategy_id = $1
              AND direction = 'short'
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

        # защита от пустого батча
        if not position_uids:
            continue

        # загрузка PACK ema21 по батчу позиций
        ind_rows = await conn.fetch(
            """
            SELECT position_uid, param_name, value_text
            FROM indicator_position_stat
            WHERE position_uid = ANY($1::text[])
              AND param_type = 'pack'
              AND timeframe = 'm5'
              AND param_base = $2
              AND status = 'ok'
              AND param_name IN ('side','dynamic_smooth')
            """,
            position_uids,
            PACK_BASE,
        )

        # укладка ema21 в структуру {position_uid: {side: ..., dynamic_smooth: ...}}
        ema_map: Dict[str, Dict[str, str]] = {}

        for r in ind_rows:
            puid = str(r["position_uid"])
            param_name = str(r["param_name"])
            value_text = str(r["value_text"]) if r["value_text"] is not None else None

            ema_map.setdefault(puid, {})[param_name] = value_text

        # обход позиций батча с проверкой полноты PACK ema21
        for pos in positions_batch:
            puid = pos["position_uid"]
            ema_state = ema_map.get(puid)

            # если нет вообще записей по этой позиции — пропуск
            if not ema_state:
                continue

            # должны быть и side, и dynamic_smooth
            if "side" not in ema_state or "dynamic_smooth" not in ema_state:
                continue

            closed_at = pos["closed_at"]
            if not closed_at:
                continue

            # преобразование в date
            if isinstance(closed_at, dt.datetime):
                trade_date = closed_at.date()
            else:
                # на всякий случай, если уже date
                trade_date = closed_at

            # позиция с полным PACK ema21 — учитываем в статистике
            used_positions += 1
            _update_stats_for_position(
                pos,
                ema_state,
                trade_date,
                detailed_counters,
                aggregated_counters,
                daily_counters,
            )

    log.info(
        "🔍 AUD_EMA21_SHORT: стратегия %d — шорт-позиций всего=%d, с полным PACK ema21=%d",
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

    # 🔹 детальные строки
    for key, det in detailed_counters.items():
        sid, direction, timeframe, filter_type, side, dyn_smooth = key
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
                PACK_BASE,
                side,
                dyn_smooth,
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

    # 🔹 агрегированные строки
    for key, agg in aggregated_counters.items():
        sid, direction, timeframe, filter_type, pack_base = key
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
                pack_base,
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

    # 🔹 суточные строки
    for key, day in daily_counters.items():
        sid, direction, trade_date, timeframe, filter_type, pack_base = key
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
                pack_base,
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
    # условия достаточности
    if not rows:
        return

    await conn.executemany(
        """
        INSERT INTO auditor_ema21_state_detailed (
            calc_at,
            strategy_id,
            direction,
            timeframe,
            filter_type,
            pack_base,
            side,
            dynamic_smooth,
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
            $1,$2,$3,$4,$5,$6,$7,$8,
            $9,$10,$11,
            $12,$13,
            $14,$15,
            $16,$17
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
        INSERT INTO auditor_ema21_state_aggregated (
            calc_at,
            strategy_id,
            direction,
            timeframe,
            filter_type,
            pack_base,
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


async def _insert_daily_rows(conn, rows: list[tuple]) -> None:
    # условия достаточности
    if not rows:
        return

    await conn.executemany(
        """
        INSERT INTO auditor_ema21_state_daily (
            calc_at,
            strategy_id,
            direction,
            timeframe,
            filter_type,
            pack_base,
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
            $1,$2,$3,$4,$5,$6,$7,
            $8,$9,$10,
            $11,$12,
            $13,$14,
            $15,$16
        )
        """,
        rows,
    )


# 🔸 Основная корутина воркера
async def run_ema21short_worker():
    # условия достаточности
    if infra.pg_pool is None:
        log.info("❌ AUD_EMA21_SHORT: пропуск воркера — PG не инициализирован")
        return

    calc_at = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    log.info("🚀 AUD_EMA21_SHORT: старт расчёта PACK ema21 шорт-фильтра (calc_at=%s)", calc_at)

    async with infra.pg_pool.acquire() as conn:
        # загрузка стратегий
        strategies = await _load_mw_strategies(conn)
        if not strategies:
            log.info("❌ AUD_EMA21_SHORT: нет MW-стратегий для анализа — выход")
            return

        total_positions_all = 0
        used_positions_all = 0
        total_detailed_rows_all = 0
        total_aggregated_rows_all = 0
        total_daily_rows_all = 0

        # обход стратегий по одной
        for strategy_id in sorted(strategies.keys()):
            # инициализация счётчиков для стратегии
            detailed_counters, aggregated_counters, daily_counters = _init_counters_for_strategy()

            log.info("🔧 AUD_EMA21_SHORT: стратегия %d — старт обработки", strategy_id)

            total_pos, used_pos = await _process_strategy_positions(
                conn,
                strategy_id,
                detailed_counters,
                aggregated_counters,
                daily_counters,
            )

            # построение строк для конкретной стратегии
            detailed_rows, aggregated_rows, daily_rows = _build_rows_for_strategy(
                strategy_id,
                detailed_counters,
                aggregated_counters,
                daily_counters,
                strategies,
                calc_at,
            )

            # запись в БД
            await _insert_detailed_rows(conn, detailed_rows)
            await _insert_aggregated_rows(conn, aggregated_rows)
            await _insert_daily_rows(conn, daily_rows)

            log.info(
                "✅ AUD_EMA21_SHORT: стратегия %d — шорт-позиций_всего=%d, шорт-позиций_с_ema21=%d, "
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
        "✅ AUD_EMA21_SHORT: завершено — стратегий=%d, шорт-позиций_всего=%d, шорт-позиций_с_ema21=%d, "
        "детальных строк=%d, агрегированных строк=%d, суточных строк=%d",
        len(strategies),
        total_positions_all,
        used_positions_all,
        total_detailed_rows_all,
        total_aggregated_rows_all,
        total_daily_rows_all,
    )