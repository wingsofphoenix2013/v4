# 🔸 auditor_cross_strength.py — аудит «силы кросса» EMA9/EMA21: bins по abs(ema9-ema21)/atr14 и метрики (WR, ΣPnL, ROI=ΣPnL/deposit) по TF (m5/m15/h1), окнам (7d/14d/28d/total) и направлению (long/short)

# 🔸 Импорты
import asyncio
import logging
import datetime as dt
from typing import Dict, List, Tuple, Optional, Iterable, Any

import auditor_infra as infra
from auditor_config import load_active_mw_strategies

# 🔸 Логгер
log = logging.getLogger("AUD_XSTR")

# 🔸 Константы аудита
WINDOWS: List[Tuple[str, Optional[int]]] = [("7d", 7), ("14d", 14), ("28d", 28), ("total", None)]
TIMEFRAMES: Tuple[str, ...] = ("m5", "m15", "h1")
MIN_SAMPLE_PER_CELL = 50          # пометка в логах, если наблюдений меньше
INITIAL_DELAY_SEC = 60            # стартовая задержка воркера
SLEEP_BETWEEN_RUNS_SEC = 3 * 60 * 60  # сон между проходами (3 часа)


# 🔸 Точка входа воркера
async def run_auditor_cross_strength():
    # условия достаточности
    if infra.pg_pool is None:
        log.info("❌ Пропуск auditor_cross_strength: PG не инициализирован")
        return

    # стартовая задержка
    if INITIAL_DELAY_SEC > 0:
        log.info("⏳ AUD_XSTR: ожидание %d сек перед первым запуском", INITIAL_DELAY_SEC)
        await asyncio.sleep(int(INITIAL_DELAY_SEC))

    # основной цикл
    while True:
        try:
            await _run_once()
        except asyncio.CancelledError:
            log.info("⏹️ AUD_XSTR: остановлено по сигналу")
            raise
        except Exception:
            log.exception("❌ AUD_XSTR: ошибка прохода — пауза 5 секунд")
            await asyncio.sleep(5)

        # сон до следующего запуска
        log.info("😴 AUD_XSTR: пауза %d сек до следующего запуска", SLEEP_BETWEEN_RUNS_SEC)
        await asyncio.sleep(int(SLEEP_BETWEEN_RUNS_SEC))


# 🔸 Один проход аудита по всем активным MW-стратегиям
async def _run_once():
    # загрузка активных MW-стратегий
    strategies = await load_active_mw_strategies()
    log.info("📦 AUD_XSTR: найдено активных MW-стратегий: %d", len(strategies))
    if not strategies:
        return

    # фиксируем "сейчас" (aware→naive UTC, совместимо с хранением в БД)
    now_utc = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    win_bounds = {
        "7d": now_utc - dt.timedelta(days=7),
        "14d": now_utc - dt.timedelta(days=14),
        "28d": now_utc - dt.timedelta(days=28),
        "total": None,
    }
    log.info(
        "🕒 AUD_XSTR: окна — now=%s; 7d>=%s; 14d>=%s; 28d>=%s",
        now_utc, win_bounds["7d"], win_bounds["14d"], win_bounds["28d"]
    )

    # последовательный проход по стратегиям (как договорились)
    for sid, meta in strategies.items():
        await _process_strategy_cross_strength(sid, meta, win_bounds)


# 🔸 Обработка одной стратегии: выборка данных, расчёт, логирование
async def _process_strategy_cross_strength(sid: int, meta: Dict[str, Any], win_bounds: Dict[str, Optional[dt.datetime]]):
    name = meta.get("name") or f"sid_{sid}"
    human = meta.get("human_name") or ""
    title = f'{sid} "{name}"' if not human else f'{sid} "{name}" ({human})'

    # получить депозит стратегии
    deposit = await _load_strategy_deposit(sid)
    has_deposit = deposit is not None and float(deposit) > 0.0
    if not has_deposit:
        log.info('⚠️ AUD_XSTR: sid=%s — депозит отсутствует или равен 0; ROI будет "n/a"', sid)

    # выбрать все закрытые позиции стратегии (total)
    positions = await _load_closed_positions_for_strategy(sid)
    if not positions:
        log.info("ℹ️ AUD_XSTR: %s — закрытых позиций нет", title)
        return

    # построить карту окон: для каждой позиции — принадлежность окнам
    for p in positions:
        closed_at = p["closed_at"]
        p["in_window"] = {
            "7d": (closed_at is not None and closed_at >= win_bounds["7d"]),
            "14d": (closed_at is not None and closed_at >= win_bounds["14d"]),
            "28d": (closed_at is not None and closed_at >= win_bounds["28d"]),
            "total": True,
        }

    # подтянуть снапшоты индикаторов по TF (ema9/ema21/atr14)
    pos_uids = [p["position_uid"] for p in positions]
    snaps = await _load_indicator_snapshots_for_positions(pos_uids)

    # для каждого TF, окна, направления — собрать значения cross_strength и pnl
    # структура: data[tf][window][direction][symbol] -> list of (cs, pnl)
    data: Dict[str, Dict[str, Dict[str, Dict[str, List[Tuple[float, float]]]]]] = {
        tf: {w: {"long": {}, "short": {}} for w, _ in WINDOWS} for tf in TIMEFRAMES
    }

    # вычисляем cross_strength на основе снапшотов; раскладываем по корзинам
    for p in positions:
        symbol = p["symbol"]
        direction = p["direction"]
        pnl = float(p["pnl"] or 0.0)

        for tf in TIMEFRAMES:
            # условия достаточности
            s = snaps.get((p["position_uid"], tf))
            if not s:
                continue
            ema9 = s.get("ema9"); ema21 = s.get("ema21"); atr14 = s.get("atr14")
            if ema9 is None or ema21 is None or atr14 is None:
                continue
            if atr14 <= 0:
                continue

            cs = abs(float(ema9) - float(ema21)) / float(atr14)

            for w, _days in WINDOWS:
                if p["in_window"][w]:
                    bucket = data[tf][w][direction].setdefault(symbol, [])
                    bucket.append((cs, pnl))

    # посчитать бины и метрики; напечатать логи
    for tf in TIMEFRAMES:
        for w, _days in WINDOWS:
            for direction in ("long", "short"):
                _log_bins_for_cell(
                    title=title, tf=tf, window=w, direction=direction,
                    symbol_series=data[tf][w][direction],
                    deposit=deposit if has_deposit else None
                )


# 🔸 Загрузка депозита стратегии
async def _load_strategy_deposit(sid: int) -> Optional[float]:
    async with infra.pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT deposit FROM strategies_v4 WHERE id = $1",
            int(sid)
        )
    if not row:
        return None
    val = row["deposit"]
    return float(val) if val is not None else None


# 🔸 Выборка закрытых позиций стратегии (total)
async def _load_closed_positions_for_strategy(sid: int) -> List[Dict[str, Any]]:
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT position_uid, symbol, direction, pnl, notional_value, created_at, closed_at
            FROM positions_v4
            WHERE strategy_id = $1
              AND status = 'closed'
              AND direction IN ('long','short')
            """,
            int(sid)
        )
    # нормализация
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            "position_uid": str(r["position_uid"]),
            "symbol": str(r["symbol"]),
            "direction": str(r["direction"]),
            "pnl": float(r["pnl"] or 0.0),
            "notional_value": float(r["notional_value"] or 0.0),
            "created_at": r["created_at"],
            "closed_at": r["closed_at"],
        })
    return out


# 🔸 Подтянуть снапшоты ema9/ema21/atr14 для позиций по всем TF
async def _load_indicator_snapshots_for_positions(pos_uids: List[str]) -> Dict[Tuple[str, str], Dict[str, float]]:
    snaps: Dict[Tuple[str, str], Dict[str, float]] = {}
    if not pos_uids:
        return snaps

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT position_uid, timeframe,
                   MAX(value_num) FILTER (WHERE param_type='indicator' AND param_base='ema' AND param_name='ema9')  AS ema9,
                   MAX(value_num) FILTER (WHERE param_type='indicator' AND param_base='ema' AND param_name='ema21') AS ema21,
                   MAX(value_num) FILTER (WHERE param_type='indicator' AND param_base='atr' AND param_name='atr14') AS atr14
            FROM indicator_position_stat
            WHERE position_uid = ANY($1)
              AND status = 'ok'
              AND param_type = 'indicator'
              AND timeframe IN ('m5','m15','h1')
            GROUP BY position_uid, timeframe
            """,
            pos_uids
        )

    for r in rows:
        key = (str(r["position_uid"]), str(r["timeframe"]))
        snaps[key] = {
            "ema9": _to_float_or_none(r["ema9"]),
            "ema21": _to_float_or_none(r["ema21"]),
            "atr14": _to_float_or_none(r["atr14"]),
        }
    return snaps


# 🔸 Логирование бинов и метрик по ячейке TF×window×direction
def _log_bins_for_cell(
    title: str,
    tf: str,
    window: str,
    direction: str,
    symbol_series: Dict[str, List[Tuple[float, float]]],
    deposit: Optional[float],
):
    # собрать плоский список для проверки объёма (до биннинга)
    total_n = sum(len(v) for v in symbol_series.values())
    if total_n == 0:
        # нет данных — тихо выходим
        return

    # расчёт квантилей и присвоение бинов по каждому символу отдельно
    # агрегируем по всем символам в 5 бинов
    bin_totals = {
        1: {"N": 0, "wins": 0, "pnl_sum": 0.0},
        2: {"N": 0, "wins": 0, "pnl_sum": 0.0},
        3: {"N": 0, "wins": 0, "pnl_sum": 0.0},
        4: {"N": 0, "wins": 0, "pnl_sum": 0.0},
        5: {"N": 0, "wins": 0, "pnl_sum": 0.0},
    }

    for symbol, pairs in symbol_series.items():
        if not pairs:
            continue
        xs = [cs for (cs, _p) in pairs]
        edges = _quantile_edges(xs, (0.2, 0.4, 0.6, 0.8))

        for cs, pnl in pairs:
            b = _assign_bin(cs, edges)
            rec = bin_totals[b]
            rec["N"] += 1
            rec["wins"] += 1 if pnl >= 0 else 0
            rec["pnl_sum"] += float(pnl)

    # заголовок ячейки
    warn = " (N<50)" if total_n < MIN_SAMPLE_PER_CELL else ""
    log.info('📈 AUD_XSTR | %s | TF=%s | dir=%s | window=%s — bins by cross_strength%s',
             title, tf, direction, window, warn)

    # печатаем бины и накапливаем крайние для дельт
    first = bin_totals[1]
    last = bin_totals[5]

    for idx in (1, 2, 3, 4, 5):
        rec = bin_totals[idx]
        N = rec["N"]
        WR = (rec["wins"] / N * 100.0) if N > 0 else 0.0
        pnl_sum = rec["pnl_sum"]
        roi = None
        if deposit and deposit > 0:
            roi = (pnl_sum / float(deposit)) * 100.0

        # в лог: N, WR, ΣPnL, ROI (как ΣPnL/deposit)
        if roi is None:
            log.info("  B%d: N=%d, WR=%.2f%%, ΣPnL=%.6f, ROI=n/a", idx, N, WR, pnl_sum)
        else:
            log.info("  B%d: N=%d, WR=%.2f%%, ΣPnL=%.6f, ROI=%.4f%%", idx, N, WR, pnl_sum, roi)

    # итог по ячейке — дельты между B5 и B1
    d_wr = _delta_wr(first, last)
    d_roi = _delta_roi(first, last, deposit)
    if d_roi is None:
        log.info("  ΔWR(B5−B1)=%.2f pp, ΔROI(B5−B1)=n/a", d_wr)
    else:
        log.info("  ΔWR(B5−B1)=%.2f pp, ΔROI(B5−B1)=%.4f pp", d_wr, d_roi)


# условия достаточности для дельт WR/ROI
def _delta_wr(b1: Dict[str, Any], b5: Dict[str, Any]) -> float:
    n1 = max(b1["N"], 1)
    n5 = max(b5["N"], 1)
    wr1 = (b1["wins"] / n1) * 100.0
    wr5 = (b5["wins"] / n5) * 100.0
    return wr5 - wr1


def _delta_roi(b1: Dict[str, Any], b5: Dict[str, Any], deposit: Optional[float]) -> Optional[float]:
    if not deposit or deposit <= 0:
        return None
    roi1 = (b1["pnl_sum"] / deposit) * 100.0
    roi5 = (b5["pnl_sum"] / deposit) * 100.0
    return roi5 - roi1


# 🔸 Утилиты квантилей/бинов/преобразований

def _quantile_edges(values: List[float], probs: Iterable[float]) -> Tuple[float, float, float, float]:
    """
    Возвращает 4 квантильные границы по values для указанных probs (ожидаем 0.2,0.4,0.6,0.8).
    Метод: "nearest-rank" по индексу round(p*(n-1)).
    """
    arr = sorted(float(x) for x in values)
    n = len(arr)
    if n == 0:
        return (0.0, 0.0, 0.0, 0.0)
    edges: List[float] = []
    for p in probs:
        idx = int(round(p * (n - 1)))
        idx = min(max(idx, 0), n - 1)
        edges.append(arr[idx])
    # гарантируем невозрастающую монотонность (на случай одинаковых значений)
    e1, e2, e3, e4 = edges
    if e1 > e2: e2 = e1
    if e2 > e3: e3 = e2
    if e3 > e4: e4 = e3
    return (e1, e2, e3, e4)


def _assign_bin(x: float, edges: Tuple[float, float, float, float]) -> int:
    """
    Присваивает бин 1..5 по значениям x и границам (q20,q40,q60,q80).
    """
    q20, q40, q60, q80 = edges
    if x <= q20:
        return 1
    elif x <= q40:
        return 2
    elif x <= q60:
        return 3
    elif x <= q80:
        return 4
    else:
        return 5


def _to_float_or_none(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None