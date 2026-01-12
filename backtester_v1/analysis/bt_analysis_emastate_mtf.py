# bt_analysis_emastate_mtf.py — анализатор MTF-состояний EMA относительно close (H1/M15/M5) по истории 12 свечей (prefetch по диапазону)

import logging
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal, InvalidOperation

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_EMASTATE_MTF")

# 🔸 Дефолтные параметры анализатора (согласованы с принятыми решениями)
DEFAULT_N_BARS = 12
DEFAULT_EPS_K = Decimal("0.0001")          # 0.01% от цены
DEFAULT_MIN_SHARE = Decimal("0.01")

# 🔸 Таблицы OHLCV по TF
OHLCV_TABLES_BY_TF: Dict[str, str] = {
    "m5": "ohlcv_bb_m5",
    "m15": "ohlcv_bb_m15",
    "h1": "ohlcv_bb_h1",
}

# 🔸 Шаг времени TF
TF_STEP: Dict[str, timedelta] = {
    "m5": timedelta(minutes=5),
    "m15": timedelta(minutes=15),
    "h1": timedelta(hours=1),
}

# 🔸 Каталог соответствий состояния -> bin_idx (0..6)
STATE_TO_BIN_IDX: Dict[str, int] = {
    "ABOVE_AWAY": 0,
    "ABOVE_STAG": 1,
    "ABOVE_APPROACH": 2,
    "NEAR": 3,
    "BELOW_APPROACH": 4,
    "BELOW_STAG": 5,
    "BELOW_AWAY": 6,
}


# 🔸 Публичная точка входа анализатора EMA state MTF (батч-подход: prefetch по диапазону)
async def run_emastate_mtf_analysis(
    analysis: Dict[str, Any],
    analysis_ctx: Dict[str, Any],
    pg,
    redis,  # оставляем для совместимости сигнатур, здесь не используется
) -> Dict[str, Any]:
    analysis_id = analysis.get("id")
    family_key = str(analysis.get("family_key") or "").strip()
    analysis_key = str(analysis.get("key") or "").strip()
    name = analysis.get("name")

    params = analysis.get("params") or {}
    scenario_id = analysis_ctx.get("scenario_id")
    signal_id = analysis_ctx.get("signal_id")

    # параметры анализатора (в bt_analysis_parameters)
    instance_id_m5 = _get_int_param(params, "instance_id_m5", 0)
    instance_id_m15 = _get_int_param(params, "instance_id_m15", 0)
    instance_id_h1 = _get_int_param(params, "instance_id_h1", 0)
    param_name = _get_str_param(params, "param_name", "").strip()

    n_bars = _get_int_param(params, "n_bars", DEFAULT_N_BARS)
    eps_k = _get_decimal_param(params, "eps_k", DEFAULT_EPS_K)
    min_share = _get_decimal_param(params, "min_share", DEFAULT_MIN_SHARE)

    if analysis_id is None or scenario_id is None or signal_id is None:
        log.debug(
            "BT_ANALYSIS_EMASTATE_MTF: анализ пропущен (нет обязательных id) analysis_id=%s, scenario_id=%s, signal_id=%s",
            analysis_id,
            scenario_id,
            signal_id,
        )
        return {
            "rows": [],
            "summary": {
                "positions_total": 0,
                "positions_used": 0,
                "positions_skipped": 0,
                "skipped_reason": "missing_ids",
            },
        }

    # условия достаточности
    if not param_name or instance_id_m5 <= 0 or instance_id_m15 <= 0 or instance_id_h1 <= 0:
        log.debug(
            "BT_ANALYSIS_EMASTATE_MTF: анализ пропущен (нет обязательных параметров) analysis_id=%s, scenario_id=%s, signal_id=%s, "
            "param_name=%s, instance_id_m5=%s, instance_id_m15=%s, instance_id_h1=%s",
            analysis_id,
            scenario_id,
            signal_id,
            param_name,
            instance_id_m5,
            instance_id_m15,
            instance_id_h1,
        )
        return {
            "rows": [],
            "summary": {
                "positions_total": 0,
                "positions_used": 0,
                "positions_skipped": 0,
                "skipped_reason": "missing_params",
            },
        }

    # окно run
    window_from = analysis_ctx.get("window_from")
    window_to = analysis_ctx.get("window_to")

    log.debug(
        "BT_ANALYSIS_EMASTATE_MTF: старт анализа id=%s (family=%s, key=%s, name=%s) scenario_id=%s, signal_id=%s — "
        "param_name=%s, n_bars=%s, eps_k=%s, min_share=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        param_name,
        n_bars,
        eps_k,
        min_share,
    )

    # загружаем позиции окна run (status=closed + postproc_v2=true) — строго в границах window_from/window_to
    positions = await _load_positions_for_analysis(pg, int(scenario_id), int(signal_id), window_from, window_to)
    if not positions:
        log.info(
            "BT_ANALYSIS_EMASTATE_MTF: нет позиций для анализа analysis_id=%s scenario_id=%s signal_id=%s",
            analysis_id,
            scenario_id,
            signal_id,
        )
        return {
            "rows": [],
            "summary": {
                "positions_total": 0,
                "positions_used": 0,
                "positions_skipped": 0,
            },
        }

    # предварительная нарезка по symbol + парсинг anchor_open_time по TF
    positions_total = 0
    positions_skipped = 0

    positions_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    for p in positions:
        positions_total += 1

        position_uid = p["position_uid"]
        symbol = str(p["symbol"] or "").strip()
        direction = str(p["direction"] or "").strip().lower()
        pnl_abs = p["pnl_abs"]
        raw_stat = p["raw_stat"]

        if not position_uid or not symbol:
            positions_skipped += 1
            continue

        anchor_h1 = _extract_anchor_open_time(raw_stat, "h1")
        anchor_m15 = _extract_anchor_open_time(raw_stat, "m15")
        anchor_m5 = _extract_anchor_open_time(raw_stat, "m5")

        # если не удалось извлечь полный набор якорей — позицию пропускаем
        if anchor_h1 is None or anchor_m15 is None or anchor_m5 is None:
            positions_skipped += 1
            continue

        positions_by_symbol.setdefault(symbol, []).append(
            {
                "position_uid": position_uid,
                "symbol": symbol,
                "direction": direction,
                "pnl_abs": pnl_abs,
                "anchor_h1": anchor_h1,
                "anchor_m15": anchor_m15,
                "anchor_m5": anchor_m5,
            }
        )

    # базовый список (без tail): для каждой позиции — индексы биннов на H1/M15/M5
    base_list: List[Dict[str, Any]] = []

    # счётчики batched-прохода
    symbols_total = len(positions_by_symbol)
    symbols_prefetched = 0
    symbols_skipped = 0

    # основной проход по symbol: prefetch данных по диапазону и расчёт состояний в памяти
    for symbol, plist in positions_by_symbol.items():
        if not plist:
            continue

        # условия достаточности
        if symbol == "":
            symbols_skipped += 1
            continue

        try:
            # считаем диапазоны времени для prefetch по каждому TF
            t_from_h1, t_to_h1 = _compute_prefetch_range(plist, "anchor_h1", "h1", n_bars)
            t_from_m15, t_to_m15 = _compute_prefetch_range(plist, "anchor_m15", "m15", n_bars)
            t_from_m5, t_to_m5 = _compute_prefetch_range(plist, "anchor_m5", "m5", n_bars)

            # prefetch OHLCV close и EMA по каждому TF
            closes_h1 = await _prefetch_close_map(pg, "h1", symbol, t_from_h1, t_to_h1)
            closes_m15 = await _prefetch_close_map(pg, "m15", symbol, t_from_m15, t_to_m15)
            closes_m5 = await _prefetch_close_map(pg, "m5", symbol, t_from_m5, t_to_m5)

            emas_h1 = await _prefetch_ema_map(pg, instance_id_h1, symbol, param_name, t_from_h1, t_to_h1)
            emas_m15 = await _prefetch_ema_map(pg, instance_id_m15, symbol, param_name, t_from_m15, t_to_m15)
            emas_m5 = await _prefetch_ema_map(pg, instance_id_m5, symbol, param_name, t_from_m5, t_to_m5)

            symbols_prefetched += 1

        except Exception as e:
            log.error(
                "BT_ANALYSIS_EMASTATE_MTF: ошибка prefetch symbol=%s analysis_id=%s scenario_id=%s signal_id=%s: %s",
                symbol,
                analysis_id,
                scenario_id,
                signal_id,
                e,
                exc_info=True,
            )
            symbols_skipped += 1
            continue

        # считаем состояние для каждой позиции данного symbol
        for rec in plist:
            position_uid = rec["position_uid"]
            direction = rec["direction"]
            pnl_abs = rec["pnl_abs"]

            anchor_h1 = rec["anchor_h1"]
            anchor_m15 = rec["anchor_m15"]
            anchor_m5 = rec["anchor_m5"]

            h1_state = _calc_tf_state_from_maps(
                tf="h1",
                anchor_open_time=anchor_h1,
                ema_map=emas_h1,
                close_map=closes_h1,
                n_bars=n_bars,
                eps_k=eps_k,
            )
            m15_state = _calc_tf_state_from_maps(
                tf="m15",
                anchor_open_time=anchor_m15,
                ema_map=emas_m15,
                close_map=closes_m15,
                n_bars=n_bars,
                eps_k=eps_k,
            )
            m5_state = _calc_tf_state_from_maps(
                tf="m5",
                anchor_open_time=anchor_m5,
                ema_map=emas_m5,
                close_map=closes_m5,
                n_bars=n_bars,
                eps_k=eps_k,
            )

            # если хотя бы один TF не смог дать состояние — позицию пропускаем
            if h1_state is None or m15_state is None or m5_state is None:
                positions_skipped += 1
                continue

            h1_idx = STATE_TO_BIN_IDX.get(h1_state)
            m15_idx = STATE_TO_BIN_IDX.get(m15_state)
            m5_idx = STATE_TO_BIN_IDX.get(m5_state)

            if h1_idx is None or m15_idx is None or m5_idx is None:
                positions_skipped += 1
                continue

            base_list.append(
                {
                    "position_uid": position_uid,
                    "direction": direction,
                    "pnl_abs": pnl_abs,
                    "h1_idx": int(h1_idx),
                    "m15_idx": int(m15_idx),
                    "m5_idx": int(m5_idx),
                }
            )

    positions_used = len(base_list)

    if positions_used == 0:
        log.info(
            "BT_ANALYSIS_EMASTATE_MTF: после фильтрации нет позиций analysis_id=%s scenario_id=%s signal_id=%s (total=%s skipped=%s)",
            analysis_id,
            scenario_id,
            signal_id,
            positions_total,
            positions_skipped,
        )
        return {
            "rows": [],
            "summary": {
                "positions_total": positions_total,
                "positions_used": 0,
                "positions_skipped": positions_skipped,
            },
        }

    total_for_share = Decimal(positions_used)

    # группируем по H1_bin
    by_h1: Dict[int, List[Dict[str, Any]]] = {}
    for rec in base_list:
        by_h1.setdefault(int(rec["h1_idx"]), []).append(rec)

    rows: List[Dict[str, Any]] = []

    # счётчики результатов
    tail_h1_rows = 0
    tail_m15_rows = 0
    full_rows = 0

    # проходим по H1-группам и применяем tail (H1 → M15 → M5)
    for h1_idx, group_h in by_h1.items():
        group_n_h = len(group_h)
        share_h = Decimal(group_n_h) / total_for_share

        # если доля H1-группы < min_share — складываем в хвост H1_bin_X|M15_0|M5_0
        if share_h < min_share:
            bin_name = f"H1_bin_{h1_idx}|M15_0|M5_0"
            for rec in group_h:
                rows.append(
                    {
                        "position_uid": rec["position_uid"],
                        "timeframe": "mtf",
                        "direction": rec["direction"],
                        "bin_name": bin_name,
                        "value": 0,
                        "pnl_abs": rec["pnl_abs"],
                    }
                )
                tail_h1_rows += 1
            continue

        # иначе — внутри H1-группы применяем tail по M15 (доля считается от total_for_share)
        by_m15: Dict[int, List[Dict[str, Any]]] = {}
        for rec in group_h:
            by_m15.setdefault(int(rec["m15_idx"]), []).append(rec)

        for m15_idx, group_m15 in by_m15.items():
            group_n_m15 = len(group_m15)
            share_m15 = Decimal(group_n_m15) / total_for_share

            # если доля (H1,M15) < min_share — хвост H1_bin_X|M15_bin_Y|M5_0
            if share_m15 < min_share:
                bin_name = f"H1_bin_{h1_idx}|M15_bin_{m15_idx}|M5_0"
                for rec in group_m15:
                    rows.append(
                        {
                            "position_uid": rec["position_uid"],
                            "timeframe": "mtf",
                            "direction": rec["direction"],
                            "bin_name": bin_name,
                            "value": 0,
                            "pnl_abs": rec["pnl_abs"],
                        }
                    )
                    tail_m15_rows += 1
                continue

            # иначе — полноценная MTF-матрица: H1_bin_X|M15_bin_Y|M5_bin_Z
            for rec in group_m15:
                m5_idx = int(rec["m5_idx"])
                bin_name = f"H1_bin_{h1_idx}|M15_bin_{m15_idx}|M5_bin_{m5_idx}"
                rows.append(
                    {
                        "position_uid": rec["position_uid"],
                        "timeframe": "mtf",
                        "direction": rec["direction"],
                        "bin_name": bin_name,
                        "value": 0,
                        "pnl_abs": rec["pnl_abs"],
                    }
                )
                full_rows += 1

    # итоговый summary
    summary = {
        "positions_total": positions_total,
        "positions_used": positions_used,
        "positions_skipped": positions_skipped,
        "rows": len(rows),
        "tail_h1_rows": tail_h1_rows,
        "tail_m15_rows": tail_m15_rows,
        "full_rows": full_rows,
        "h1_groups": len(by_h1),
        "n_bars": n_bars,
        "eps_k": str(eps_k),
        "min_share": str(min_share),
        "param_name": param_name,
        "symbols_total": symbols_total,
        "symbols_prefetched": symbols_prefetched,
        "symbols_skipped": symbols_skipped,
    }

    log.info(
        "BT_ANALYSIS_EMASTATE_MTF: done analysis_id=%s (family=%s, key=%s, name=%s) scenario_id=%s signal_id=%s — "
        "param=%s n_bars=%s eps_k=%s min_share=%s pos_total=%s used=%s skipped=%s rows=%s (tail_h1=%s tail_m15=%s full=%s) "
        "symbols_total=%s prefetched=%s skipped=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        param_name,
        n_bars,
        eps_k,
        min_share,
        positions_total,
        positions_used,
        positions_skipped,
        len(rows),
        tail_h1_rows,
        tail_m15_rows,
        full_rows,
        symbols_total,
        symbols_prefetched,
        symbols_skipped,
    )

    return {"rows": rows, "summary": summary}


# 🔸 Расчёт диапазона prefetch по списку позиций (min_anchor - (N-1)*step .. max_anchor)
def _compute_prefetch_range(
    positions: List[Dict[str, Any]],
    anchor_key: str,
    tf: str,
    n_bars: int,
) -> Tuple[datetime, datetime]:
    step = TF_STEP.get(tf)
    if step is None:
        raise RuntimeError(f"Unsupported tf: {tf}")

    anchors: List[datetime] = []
    for p in positions:
        t = p.get(anchor_key)
        if isinstance(t, datetime):
            anchors.append(t)

    if not anchors:
        raise RuntimeError(f"No anchors for tf={tf} key={anchor_key}")

    t_min = min(anchors)
    t_max = max(anchors)

    # захватываем N-1 бар назад, включая якорь
    lookback = step * max(int(n_bars) - 1, 0)
    t_from = t_min - lookback
    t_to = t_max

    return t_from, t_to


# 🔸 Prefetch: загрузка close для symbol+tf в диапазоне времени
async def _prefetch_close_map(
    pg,
    tf: str,
    symbol: str,
    time_from: datetime,
    time_to: datetime,
) -> Dict[datetime, Decimal]:
    table = OHLCV_TABLES_BY_TF.get(tf)
    if not table:
        return {}

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT open_time, "close"
            FROM {table}
            WHERE symbol = $1
              AND open_time BETWEEN $2 AND $3
            ORDER BY open_time
            """,
            symbol,
            time_from,
            time_to,
        )

    out: Dict[datetime, Decimal] = {}
    for r in rows:
        out[r["open_time"]] = _safe_decimal(r["close"])
    return out


# 🔸 Prefetch: загрузка EMA для instance_id+symbol+param_name в диапазоне времени
async def _prefetch_ema_map(
    pg,
    instance_id: int,
    symbol: str,
    param_name: str,
    time_from: datetime,
    time_to: datetime,
) -> Dict[datetime, Decimal]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT open_time, value
            FROM indicator_values_v4
            WHERE instance_id = $1
              AND symbol      = $2
              AND param_name  = $3
              AND open_time BETWEEN $4 AND $5
            ORDER BY open_time
            """,
            int(instance_id),
            symbol,
            param_name,
            time_from,
            time_to,
        )

    out: Dict[datetime, Decimal] = {}
    for r in rows:
        out[r["open_time"]] = _safe_decimal(r["value"])
    return out


# 🔸 Расчёт состояния на одном TF по заранее загруженным series map'ам (EMA/close)
def _calc_tf_state_from_maps(
    tf: str,
    anchor_open_time: datetime,
    ema_map: Dict[datetime, Decimal],
    close_map: Dict[datetime, Decimal],
    n_bars: int,
    eps_k: Decimal,
) -> Optional[str]:
    step = TF_STEP.get(tf)
    if step is None:
        return None

    # собираем список времен последних N баров включая anchor
    times: List[datetime] = []
    for i in range(int(n_bars) - 1, -1, -1):
        times.append(anchor_open_time - (step * i))

    # условия достаточности: все времена должны присутствовать и в EMA, и в close
    ema_series: List[Decimal] = []
    close_series: List[Decimal] = []

    for t in times:
        e = ema_map.get(t)
        c = close_map.get(t)
        if e is None or c is None:
            return None
        ema_series.append(e)
        close_series.append(c)

    ema_last = ema_series[-1]
    close_last = close_series[-1]

    d_last = ema_last - close_last
    a_last = _abs(d_last)

    eps = _safe_decimal(close_last) * _safe_decimal(eps_k)

    # near имеет приоритет над всем
    if a_last <= eps:
        return "NEAR"

    # ряд расстояний |ema-close| по окну
    a_series: List[Decimal] = []
    for i in range(len(ema_series)):
        a_series.append(_abs(ema_series[i] - close_series[i]))

    slope = _linreg_slope(a_series)

    # порог значимости движения (дефолтная схема): thr = eps / N
    thr = eps / Decimal(max(int(n_bars), 1))

    # классификация тренда расстояния
    if slope <= -thr:
        motion = "APPROACH"
    elif slope >= thr:
        motion = "AWAY"
    else:
        motion = "STAG"

    # над/под ценой определяется по последней точке окна
    if d_last > 0:
        return f"ABOVE_{motion}"
    else:
        return f"BELOW_{motion}"


# 🔸 Загрузка позиций сценария/сигнала (postproc_v2=true) в окне времени
async def _load_positions_for_analysis(
    pg,
    scenario_id: int,
    signal_id: int,
    window_from: Optional[Any],
    window_to: Optional[Any],
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        # условия достаточности
        if window_from is None or window_to is None:
            return []

        rows = await conn.fetch(
            """
            SELECT
                position_uid,
                symbol,
                direction,
                pnl_abs,
                raw_stat
            FROM bt_scenario_positions_v2
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND status      = 'closed'
              AND postproc_v2 = true
              AND entry_time BETWEEN $3 AND $4
            ORDER BY entry_time
            """,
            scenario_id,
            signal_id,
            window_from,
            window_to,
        )

    out: List[Dict[str, Any]] = []
    for r in rows:
        raw = r["raw_stat"]

        # приводим jsonb к dict, если он пришёл строкой
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                raw = None

        out.append(
            {
                "position_uid": r["position_uid"],
                "symbol": r["symbol"],
                "direction": r["direction"],
                "pnl_abs": _safe_decimal(r["pnl_abs"]),
                "raw_stat": raw,
            }
        )

    log.debug(
        "BT_ANALYSIS_EMASTATE_MTF: загружено позиций scenario_id=%s signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(out),
    )
    return out


# 🔸 Извлечение якорного open_time по TF из raw_stat
def _extract_anchor_open_time(raw_stat: Any, tf: str) -> Optional[datetime]:
    if raw_stat is None:
        return None

    if isinstance(raw_stat, str):
        try:
            raw_stat = json.loads(raw_stat)
        except Exception:
            return None

    if not isinstance(raw_stat, dict):
        return None

    tf_block = (raw_stat.get("tf") or {}).get(tf)
    if not isinstance(tf_block, dict):
        return None

    open_time_str = tf_block.get("open_time")
    if not open_time_str:
        return None

    try:
        return datetime.fromisoformat(str(open_time_str))
    except Exception:
        return None


# 🔸 Наклон линейной регрессии y(t) по t=0..n-1 (в единицах y на бар)
def _linreg_slope(values: List[Decimal]) -> Decimal:
    n = len(values)
    if n <= 1:
        return Decimal("0")

    xs: List[Decimal] = [Decimal(i) for i in range(n)]
    mean_x = sum(xs) / Decimal(n)
    mean_y = sum(values) / Decimal(n)

    cov = Decimal("0")
    var = Decimal("0")

    for i in range(n):
        dx = xs[i] - mean_x
        dy = values[i] - mean_y
        cov += dx * dy
        var += dx * dx

    if var == 0:
        return Decimal("0")

    return cov / var


# 🔸 Абсолютное значение Decimal
def _abs(x: Decimal) -> Decimal:
    return x if x >= 0 else -x


# 🔸 Вспомогательная функция: безопасное приведение к Decimal
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


# 🔸 Вспомогательная функция: безопасное чтение Decimal-параметра
def _get_decimal_param(params: Dict[str, Any], name: str, default: Decimal) -> Decimal:
    cfg = params.get(name)
    if cfg is None:
        return default

    raw = cfg.get("value")
    if raw is None:
        return default

    try:
        return Decimal(str(raw))
    except (InvalidOperation, TypeError, ValueError):
        return default


# 🔸 Вспомогательная функция: безопасное чтение int-параметра
def _get_int_param(params: Dict[str, Any], name: str, default: int) -> int:
    cfg = params.get(name)
    if cfg is None:
        return default

    raw = cfg.get("value")
    if raw is None:
        return default

    try:
        return int(str(raw))
    except (TypeError, ValueError):
        return default


# 🔸 Вспомогательная функция: безопасное чтение str-параметра
def _get_str_param(params: Dict[str, Any], name: str, default: str) -> str:
    cfg = params.get(name)
    if cfg is None:
        return default

    raw = cfg.get("value")
    if raw is None:
        return default

    return str(raw).strip()