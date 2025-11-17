# 🔸 auditor_retrace_ema921_grid_worker.py — подбор порогов retrace-фильтра EMA9/21 по сетке

# 🔸 Импорты
import logging
import datetime as dt
from typing import Dict, Any, List, Tuple

import auditor_infra as infra


# 🔸 Логгер
log = logging.getLogger("AUD_RETRACE_EMA921_GRID")


# 🔸 Константы грид-серча
# Диапазоны можно потом менять прямо в коде
LOWER_ATR_GRID = [0.05, 0.10, 0.15, 0.20, 0.25]
LOWER_IMP_GRID = [0.05, 0.10, 0.15, 0.20]
UPPER_ATR_GRID = [0.8, 1.0, 1.2, 1.5]
UPPER_IMP_GRID = [0.3, 0.4, 0.5, 0.6]


# 🔸 Типы

class Trade:
    __slots__ = ("strategy_id", "direction", "pnl", "ratio_atr", "ratio_imp")

    def __init__(self, strategy_id: int, direction: str, pnl: float,
                 ratio_atr: float | None, ratio_imp: float | None):
        self.strategy_id = strategy_id
        self.direction = direction
        self.pnl = pnl
        self.ratio_atr = ratio_atr
        self.ratio_imp = ratio_imp


# 🔸 Вспомогательные функции

def _calc_winrate(wins: int, total: int) -> float | None:
    if total <= 0:
        return None
    return wins / total


async def _load_trades(conn) -> List[Trade]:
    """
    Загружаем все сделки, для которых уже посчитаны retracement_ratio_atr / retracement_ratio_impulse.
    Источник:
      - auditor_retrace_ema921 (ratio_*),
      - positions_v4 (pnl, direction)
    """
    rows = await conn.fetch(
        """
        SELECT
            a.strategy_id,
            p.direction,
            p.pnl,
            a.retracement_ratio_atr,
            a.retracement_ratio_impulse
        FROM auditor_retrace_ema921 a
        JOIN positions_v4 p
          ON p.position_uid = a.position_uid
        WHERE a.retracement_ratio_impulse IS NOT NULL
        """
    )

    trades: List[Trade] = []
    for r in rows:
        strategy_id = int(r["strategy_id"])
        direction = str(r["direction"])
        pnl = float(r["pnl"] or 0.0)
        ratio_atr = r["retracement_ratio_atr"]
        ratio_imp = r["retracement_ratio_impulse"]
        trades.append(
            Trade(
                strategy_id=strategy_id,
                direction=direction,
                pnl=pnl,
                ratio_atr=float(ratio_atr) if ratio_atr is not None else None,
                ratio_imp=float(ratio_imp),
            )
        )

    log.info("🔍 AUD_RETRACE_EMA921_GRID: загружено сделок с retrace-метриками: %d", len(trades))
    return trades


async def _load_deposits(conn) -> Dict[int, float]:
    """
    Депозиты стратегий, чтобы считать ROI.
    Берём все enabled, не archived.
    """
    rows = await conn.fetch(
        """
        SELECT id, deposit
        FROM strategies_v4
        WHERE enabled = true
          AND (archived IS NOT TRUE)
        """
    )

    deposits: Dict[int, float] = {}
    for r in rows:
        sid = int(r["id"])
        dep = r["deposit"]
        deposits[sid] = float(dep) if dep is not None else 0.0

    log.info("🔍 AUD_RETRACE_EMA921_GRID: загружено депозитов стратегий: %d", len(deposits))
    return deposits


def _simulate_config_for_trades(
    trades: List[Trade],
    lower_atr: float,
    lower_imp: float,
    upper_atr: float,
    upper_imp: float,
    deposits: Dict[int, float],
) -> List[Tuple]:
    """
    Прогон одной конфигурации порогов по всем сделкам.
    Возвращает список строк для вставки в auditor_retrace_ema921_grid.
    """

    # counters[(strategy_id, direction)] = { ... }
    counters: Dict[Tuple[int, str], Dict[str, Any]] = {}

    for t in trades:
        key = (t.strategy_id, t.direction)
        c = counters.get(key)
        if c is None:
            c = {
                "total": 0,
                "filtered": 0,
                "passed": 0,
                "wins_before": 0,
                "wins_after": 0,
                "sum_before": 0.0,
                "sum_after": 0.0,
            }
            counters[key] = c

        # пропускаем сделки, где нет ratio_imp (без них фильтр бессмысленен)
        if t.ratio_imp is None:
            continue

        ratio_atr = t.ratio_atr
        ratio_imp = t.ratio_imp

        c["total"] += 1
        c["sum_before"] += t.pnl
        if t.pnl > 0:
            c["wins_before"] += 1

        # --- определяем бан по "малый" ---
        small = False
        if ratio_atr is not None and ratio_atr < lower_atr:
            small = True
        if ratio_imp < lower_imp:
            small = True

        # --- определяем бан по "глубокий" ---
        deep = False
        if ratio_atr is not None and ratio_atr > upper_atr:
            deep = True
        if ratio_imp > upper_imp:
            deep = True

        banned = small or deep
        if banned:
            c["filtered"] += 1
        else:
            c["passed"] += 1
            c["sum_after"] += t.pnl
            if t.pnl > 0:
                c["wins_after"] += 1

    # формируем строки для вставки
    rows: List[Tuple] = []
    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)

    for (strategy_id, direction), c in counters.items():
        total = c["total"]
        if total == 0:
            continue

        filtered = c["filtered"]
        passed = c["passed"]

        wins_before = c["wins_before"]
        wins_after = c["wins_after"]
        sum_before = c["sum_before"]
        sum_after = c["sum_after"]

        winrate_before = _calc_winrate(wins_before, total)
        winrate_after = _calc_winrate(wins_after, passed)

        deposit = deposits.get(strategy_id, 0.0)
        if deposit > 0:
            roi_before = sum_before / deposit
            roi_after = sum_after / deposit
        else:
            roi_before = None
            roi_after = None

        rows.append(
            (
                now,
                strategy_id,
                direction,
                lower_atr,
                lower_imp,
                upper_atr,
                upper_imp,
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

    return rows


async def _insert_grid_rows(conn, rows: List[Tuple]) -> None:
    if not rows:
        return

    await conn.executemany(
        """
        INSERT INTO auditor_retrace_ema921_grid (
            calc_at,
            strategy_id,
            direction,
            lower_atr_threshold,
            lower_imp_threshold,
            upper_atr_threshold,
            upper_imp_threshold,
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
            $1,$2,$3,
            $4,$5,$6,$7,
            $8,$9,$10,
            $11,$12,
            $13,$14,
            $15,$16
        )
        """,
        rows,
    )


# 🔸 Основная корутина воркера
async def run_retrace_ema921_grid_worker():
    if infra.pg_pool is None:
        log.info("❌ AUD_RETRACE_EMA921_GRID: пропуск воркера — PG не инициализирован")
        return

    log.info("🚀 AUD_RETRACE_EMA921_GRID: старт грид-серча порогов")

    async with infra.pg_pool.acquire() as conn:
        trades = await _load_trades(conn)
        if not trades:
            log.info("❌ AUD_RETRACE_EMA921_GRID: нет сделок для анализа — выход")
            return

        deposits = await _load_deposits(conn)

        total_configs = 0
        total_rows = 0

        for lower_atr in LOWER_ATR_GRID:
            for lower_imp in LOWER_IMP_GRID:
                for upper_atr in UPPER_ATR_GRID:
                    for upper_imp in UPPER_IMP_GRID:
                        total_configs += 1
                        log.info(
                            "🔧 AUD_RETRACE_EMA921_GRID: конфиг #%d — LA=%.3f, LI=%.3f, UA=%.3f, UI=%.3f",
                            total_configs, lower_atr, lower_imp, upper_atr, upper_imp,
                        )

                        rows = _simulate_config_for_trades(
                            trades,
                            lower_atr,
                            lower_imp,
                            upper_atr,
                            upper_imp,
                            deposits,
                        )

                        await _insert_grid_rows(conn, rows)
                        total_rows += len(rows)

        log.info(
            "✅ AUD_RETRACE_EMA921_GRID: завершено — конфигов=%d, строк записано=%d",
            total_configs,
            total_rows,
        )