# 🔸 Маршруты торгов

import logging
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from collections import defaultdict
from decimal import Decimal
from datetime import datetime, time, timedelta

from main import KYIV_TZ, get_kyiv_day_bounds, get_kyiv_range_backwards

router = APIRouter()
log = logging.getLogger("TRADES")

# 🔸 Внешние зависимости (инициализируются из init_dependencies)
pg_pool = None
templates = Jinja2Templates(directory="templates")


# 🔸 Страница /trades — список активных стратегий с фильтрацией
@router.get("/trades", response_class=HTMLResponse)
async def trades_page(request: Request, filter: str = "today", series: str = None):
    strategies = await get_trading_summary(filter)

    if series:
        prefix = f"strategy_{series}"
        strategies = [s for s in strategies if s["name"].startswith(prefix)]

    return templates.TemplateResponse("trades.html", {
        "request": request,
        "strategies": strategies,
        "filter": filter,
        "series": series,
    })


# 🔸 Расчёт статистики стратегий под /trades
async def get_trading_summary(filter: str) -> list[dict]:
    async with pg_pool.acquire() as conn:
        strategies = await conn.fetch("""
            SELECT id, name, human_name, deposit
            FROM strategies_v4
            WHERE enabled = true
            ORDER BY id
        """)

        if filter == "today":
            start, end = get_kyiv_day_bounds(0)
        elif filter == "yesterday":
            start, end = get_kyiv_day_bounds(1)
        elif filter == "7days":
            start, end = get_kyiv_range_backwards(7)
        else:
            start, end = None, None

        if start and end:
            start = start.replace(tzinfo=None)
            end = end.replace(tzinfo=None)

        result = []

        for strat in strategies:
            sid = strat["id"]
            deposit = strat["deposit"]

            if start and end:
                closed_rows = await conn.fetch("""
                    SELECT pnl FROM positions_v4
                    WHERE strategy_id = $1 AND status = 'closed'
                      AND closed_at BETWEEN $2 AND $3
                """, sid, start, end)
            else:
                closed_rows = await conn.fetch("""
                    SELECT pnl FROM positions_v4
                    WHERE strategy_id = $1 AND status = 'closed'
                """, sid)

            pnl_list = [r["pnl"] for r in closed_rows if r["pnl"] is not None]
            closed_count = len(pnl_list)
            win_count = sum(1 for pnl in pnl_list if pnl >= 0)
            pnl_sum = sum(pnl_list)

            winrate = round(win_count / closed_count * 100, 2) if closed_count > 0 else None
            roi = round(pnl_sum / deposit * 100, 2) if deposit else None

            if filter == "today":
                open_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM positions_v4
                    WHERE strategy_id = $1 AND status = 'open'
                      AND created_at BETWEEN $2 AND $3
                """, sid, start, end)
            else:
                open_count = 0

            result.append({
                "id": sid,
                "name": strat["name"],
                "human_name": strat["human_name"],
                "open": open_count,
                "closed": closed_count,
                "winrate": winrate,
                "roi": roi
            })

        result.sort(key=lambda r: (r["roi"] is not None, r["roi"]), reverse=True)
        return result
@router.get("/trades/details/{strategy_name}", response_class=HTMLResponse)
async def strategy_detail_page(
    request: Request,
    strategy_name: str,
    filter: str = None,
    series: str = None,
    page: int = 1
):
    async with pg_pool.acquire() as conn:
        # Стратегия
        strategy = await conn.fetchrow("""
            SELECT s.*, sig.name AS signal_name
            FROM strategies_v4 s
            LEFT JOIN signals_v4 sig ON sig.id = s.signal_id
            WHERE s.name = $1
        """, strategy_name)

        if not strategy:
            raise HTTPException(status_code=404, detail="Стратегия не найдена")

        strategy_id = strategy["id"]

        # Открытые позиции
        open_positions_raw = await conn.fetch("""
            SELECT *
            FROM positions_v4
            WHERE strategy_id = $1 AND status = 'open'
            ORDER BY created_at DESC
        """, strategy_id)

        open_positions = [
            {
                **dict(p),
                "created_at": p["created_at"].astimezone(KYIV_TZ) if p["created_at"] else None
            }
            for p in open_positions_raw
        ]

        # TP/SL цели
        position_uids = [p["position_uid"] for p in open_positions]
        targets_raw = await conn.fetch("""
            SELECT *
            FROM position_targets_v4
            WHERE position_uid = ANY($1::text[])
              AND hit = false AND canceled = false
        """, position_uids)

        targets_by_uid = {}
        for t in targets_raw:
            uid = t["position_uid"]
            targets_by_uid.setdefault(uid, []).append(dict(t))

        tp_sl_by_uid = {}
        for uid, targets in targets_by_uid.items():
            tp = sorted((t for t in targets if t["type"] == "tp"), key=lambda x: x["level"])
            sl = [t for t in targets if t["type"] == "sl"]
            tp_sl_by_uid[uid] = {
                "tp": tp[0] if tp else None,
                "sl": sl[0] if sl else None
            }

        # Закрытые позиции (история)
        page_size = 50
        offset = (page - 1) * page_size

        closed_positions_raw = await conn.fetch("""
            SELECT *
            FROM positions_v4
            WHERE strategy_id = $1 AND status = 'closed'
            ORDER BY closed_at DESC
            LIMIT $2 OFFSET $3
        """, strategy_id, page_size, offset)

        closed_positions = [
            {
                **dict(p),
                "created_at": p["created_at"].astimezone(KYIV_TZ) if p["created_at"] else None,
                "closed_at": p["closed_at"].astimezone(KYIV_TZ) if p["closed_at"] else None,
            }
            for p in closed_positions_raw
        ]

        # Общее количество закрытых сделок
        total_closed = await conn.fetchval("""
            SELECT COUNT(*)
            FROM positions_v4
            WHERE strategy_id = $1 AND status = 'closed'
        """, strategy_id)

        total_pages = (total_closed + page_size - 1) // page_size

        # Статистика по 10 дням (включая сегодня)
        days = 10
        daily_stats = defaultdict(lambda: {
            "count": 0,
            "positive": 0,
            "negative": 0,
            "pnl": Decimal("0.0")
        })

        for i in range(days):
            day_start, day_end = get_kyiv_day_bounds(i)
            rows = await conn.fetch("""
                SELECT pnl
                FROM positions_v4
                WHERE strategy_id = $1 AND status = 'closed'
                  AND closed_at BETWEEN $2 AND $3
            """, strategy_id, day_start, day_end)

            date_key = day_start.strftime('%Y-%m-%d')
            for row in rows:
                pnl = row["pnl"]
                daily_stats[date_key]["count"] += 1
                daily_stats[date_key]["pnl"] += pnl
                if pnl >= 0:
                    daily_stats[date_key]["positive"] += 1
                else:
                    daily_stats[date_key]["negative"] += 1

        total_stats = {
            "count": sum(d["count"] for d in daily_stats.values()),
            "positive": sum(d["positive"] for d in daily_stats.values()),
            "negative": sum(d["negative"] for d in daily_stats.values()),
            "pnl": sum(d["pnl"] for d in daily_stats.values())
        }

        deposit = strategy["deposit"] or 0
        roi = (total_stats["pnl"] / deposit * 100) if deposit else None

        stat_dates = [
            get_kyiv_day_bounds(i)[0].strftime('%Y-%m-%d')
            for i in reversed(range(days))
        ]
        today_key = stat_dates[-1]
        now = datetime.now(KYIV_TZ)

    return templates.TemplateResponse("strategy_detail.html", {
        "request": request,
        "strategy": dict(strategy),
        "open_positions": open_positions,
        "tp_sl_by_uid": tp_sl_by_uid,
        "closed_positions": closed_positions,
        "current_page": page,
        "total_pages": total_pages,
        "filter": filter,
        "series": series,
        "now": now,
        "stat_dates": stat_dates,
        "daily_stats": daily_stats,
        "total_stats": total_stats,
        "roi": roi,
        "today_key": today_key,
    })
 # 🔸 Страница статистики по стратегиям
@router.get("/trades/details/{strategy_name}/stats", response_class=HTMLResponse)
async def strategy_stats_overview(
    request: Request,
    strategy_name: str,
    filter: str = None,
    series: str = None
):
    async with pg_pool.acquire() as conn:
        strategy = await conn.fetchrow("""
            SELECT *
            FROM strategies_v4
            WHERE name = $1
        """, strategy_name)

        if not strategy:
            raise HTTPException(status_code=404, detail="Стратегия не найдена")

        # 🔸 Статистика по тикерам
        ticker_stats = await conn.fetch("""
            SELECT
                symbol,
                COUNT(*) AS total,
                SUM(CASE WHEN pnl >= 0 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN direction = 'long' THEN 1 ELSE 0 END) AS long_total,
                SUM(CASE WHEN direction = 'long' AND pnl >= 0 THEN 1 ELSE 0 END) AS long_win,
                SUM(CASE WHEN direction = 'short' THEN 1 ELSE 0 END) AS short_total,
                SUM(CASE WHEN direction = 'short' AND pnl >= 0 THEN 1 ELSE 0 END) AS short_win
            FROM positions_v4
            WHERE strategy_id = $1 AND status = 'closed'
            GROUP BY symbol
            ORDER BY total DESC
        """, strategy["id"])

        summary = {
            "total": 0,
            "wins": 0,
            "long_total": 0,
            "long_win": 0,
            "short_total": 0,
            "short_win": 0
        }

        tickers = []

        for row in ticker_stats:
            row = dict(row)
            symbol = row["symbol"]
            total = row["total"]
            wins = row["wins"]
            long_total = row["long_total"]
            long_win = row["long_win"]
            short_total = row["short_total"]
            short_win = row["short_win"]

            tickers.append({
                "symbol": symbol,
                "total": total,
                "total_pct": None,  # будет позже
                "winrate": round(wins / total * 100, 1) if total else 0,
                "long_total": long_total,
                "long_winrate": round(long_win / long_total * 100, 1) if long_total else 0,
                "short_total": short_total,
                "short_winrate": round(short_win / short_total * 100, 1) if short_total else 0,
            })

            summary["total"] += total
            summary["wins"] += wins
            summary["long_total"] += long_total
            summary["long_win"] += long_win
            summary["short_total"] += short_total
            summary["short_win"] += short_win

        for row in tickers:
            row["total_pct"] = round(row["total"] / summary["total"] * 100, 1) if summary["total"] else 0

        summary_row = {
            "symbol": "ИТОГО",
            "total": summary["total"],
            "total_pct": 100,
            "winrate": round(summary["wins"] / summary["total"] * 100, 1) if summary["total"] else 0,
            "long_total": summary["long_total"],
            "long_winrate": round(summary["long_win"] / summary["long_total"] * 100, 1) if summary["long_total"] else 0,
            "short_total": summary["short_total"],
            "short_winrate": round(summary["short_win"] / summary["short_total"] * 100, 1) if summary["short_total"] else 0,
        }

    return templates.TemplateResponse("strategy_stats.html", {
        "request": request,
        "strategy": dict(strategy),
        "filter": filter,
        "series": series,
        "tickers": tickers,
        "summary_row": summary_row
    })
# 🔸 Статистика стратегии по индикатору RSI
RSI_BINS = [(0, 20), (20, 30), (30, 40), (40, 50),
            (50, 60), (60, 70), (70, 80), (80, float("inf"))]
RSI_INF = float("inf")

def rsi_bin_index(value: float) -> int:
    for i, (lo, hi) in enumerate(RSI_BINS):
        if lo <= value < hi:
            return i
    return len(RSI_BINS) - 1

@router.get("/trades/details/{strategy_name}/stats/rsi", response_class=HTMLResponse)
async def strategy_rsi_stats(
    request: Request,
    strategy_name: str,
    filter: str = None,
    series: str = None
):
    log = logging.getLogger("RSI_STATS")

    async with pg_pool.acquire() as conn:
        strategy = await conn.fetchrow("""
            SELECT * FROM strategies_v4
            WHERE name = $1
        """, strategy_name)

        if not strategy:
            raise HTTPException(status_code=404, detail="Стратегия не найдена")

        tf = strategy["timeframe"]
        log.info(f"[RSI] Стратегия: {strategy_name} | таймфрейм: {tf}")

        positions = await conn.fetch("""
            SELECT position_uid, pnl, direction
            FROM positions_v4
            WHERE strategy_id = $1 AND status = 'closed'
        """, strategy["id"])

        position_map = {
            p["position_uid"]: {
                "pnl": p["pnl"],
                "direction": p["direction"]
            }
            for p in positions
        }

        log.info(f"[RSI] Закрытых сделок: {len(position_map)}")

        rsi_data = await conn.fetch("""
            SELECT position_uid, value
            FROM position_ind_stat_v4
            WHERE param_name = 'rsi14'
              AND timeframe = $2
              AND position_uid = ANY($1)
        """, list(position_map.keys()), tf)

        log.info(f"[RSI] RSI-записей по {tf}: {len(rsi_data)}")

        result = {
            "success_long": {"main": [0]*8},
            "success_short": {"main": [0]*8},
            "fail_long": {"main": [0]*8},
            "fail_short": {"main": [0]*8},
        }

        summary = {
            "success": [0]*8,
            "fail": [0]*8,
        }

        for row in rsi_data:
            uid = row["position_uid"]
            if uid not in position_map:
                continue

            rsi = float(row["value"])
            info = position_map[uid]
            pnl = info["pnl"]
            direction = info["direction"]
            idx = rsi_bin_index(rsi)

            if pnl >= 0:
                summary["success"][idx] += 1
                if direction == "long":
                    result["success_long"]["main"][idx] += 1
                elif direction == "short":
                    result["success_short"]["main"][idx] += 1
            else:
                summary["fail"][idx] += 1
                if direction == "long":
                    result["fail_long"]["main"][idx] += 1
                elif direction == "short":
                    result["fail_short"]["main"][idx] += 1

    return templates.TemplateResponse("strategy_stats_rsi.html", {
        "request": request,
        "strategy": dict(strategy),
        "filter": filter,
        "series": series,
        "rsi_distribution": result,
        "rsi_summary": summary,
        "rsi_bins": RSI_BINS,
        "rsi_inf": RSI_INF,
    })
# 🔸 Статистика стратегии по индикатору ADX
ADX_BINS = [(0, 10), (10, 15), (15, 20), (20, 25), (25, 30), (30, 35), (35, 40), (40, float("inf"))]
ADX_INF = float("inf")  # для шаблона

def bin_index(adx_value: float) -> int:
    for i, (lo, hi) in enumerate(ADX_BINS):
        if lo <= adx_value < hi:
            return i
    return len(ADX_BINS) - 1

@router.get("/trades/details/{strategy_name}/stats/adx", response_class=HTMLResponse)
async def strategy_adx_stats(
    request: Request,
    strategy_name: str,
    filter: str = None,
    series: str = None
):
    log = logging.getLogger("ADX_STATS")

    async with pg_pool.acquire() as conn:
        # Стратегия
        strategy = await conn.fetchrow("""
            SELECT * FROM strategies_v4
            WHERE name = $1
        """, strategy_name)

        if not strategy:
            raise HTTPException(status_code=404, detail="Стратегия не найдена")

        tf = strategy["timeframe"]
        log.info(f"[ADX] Стратегия: {strategy_name} | таймфрейм: {tf}")

        # Закрытые сделки
        positions = await conn.fetch("""
            SELECT position_uid, pnl, direction
            FROM positions_v4
            WHERE strategy_id = $1 AND status = 'closed'
        """, strategy["id"])

        position_map = {
            p["position_uid"]: {
                "pnl": p["pnl"],
                "direction": p["direction"]
            }
            for p in positions
        }

        log.info(f"[ADX] Найдено закрытых сделок: {len(position_map)}")

        # ADX по таймфрейму стратегии
        adx_data = await conn.fetch("""
            SELECT position_uid, value
            FROM position_ind_stat_v4
            WHERE param_name = 'adx_dmi14_adx'
              AND timeframe = $2
              AND position_uid = ANY($1)
        """, list(position_map.keys()), tf)

        log.info(f"[ADX] Записей ADX по таймфрейму {tf}: {len(adx_data)}")

        for i, row in enumerate(adx_data[:5]):
            uid = row["position_uid"]
            val = float(row["value"])
            info = position_map.get(uid)
            if info:
                log.info(f"[ADX] → {uid} | ADX={val:.2f} | pnl={info['pnl']} | {info['direction']}")

        # Инициализация структуры
        result = {
            "success_long": {"main": [0]*8},
            "success_short": {"main": [0]*8},
            "fail_long": {"main": [0]*8},
            "fail_short": {"main": [0]*8},
        }

        adx_summary = {
            "success": [0]*8,
            "fail": [0]*8,
        }

        for row in adx_data:
            uid = row["position_uid"]
            if uid not in position_map:
                continue

            adx = float(row["value"])
            info = position_map[uid]
            pnl = info["pnl"]
            direction = info["direction"]
            idx = bin_index(adx)

            if pnl >= 0:
                adx_summary["success"][idx] += 1
                if direction == "long":
                    result["success_long"]["main"][idx] += 1
                elif direction == "short":
                    result["success_short"]["main"][idx] += 1
            else:
                adx_summary["fail"][idx] += 1
                if direction == "long":
                    result["fail_long"]["main"][idx] += 1
                elif direction == "short":
                    result["fail_short"]["main"][idx] += 1

    return templates.TemplateResponse("strategy_stats_adx.html", {
        "request": request,
        "strategy": dict(strategy),
        "filter": filter,
        "series": series,
        "adx_distribution": result,
        "adx_summary": adx_summary,
        "adx_bins": ADX_BINS,
        "adx_inf": ADX_INF,
    })
# 🔸 Статистика стратегии по Bollinger Bands
BB_ZONES = 6
BB_SETS = ["2_5", "2_0", "1_5", "1_0"]

def classify_zone(entry, upper, center, lower) -> int:
    mid_upper = (center + upper) / 2
    mid_lower = (center + lower) / 2
    if entry > upper:
        return 0
    elif entry > mid_upper:
        return 1
    elif entry > center:
        return 2
    elif entry > mid_lower:
        return 3
    elif entry > lower:
        return 4
    else:
        return 5

@router.get("/trades/details/{strategy_name}/stats/bb", response_class=HTMLResponse)
async def strategy_bb_stats(
    request: Request,
    strategy_name: str,
    filter: str = None,
    series: str = None
):
    log = logging.getLogger("BB_STATS")

    async with pg_pool.acquire() as conn:
        strategy = await conn.fetchrow("""
            SELECT * FROM strategies_v4
            WHERE name = $1
        """, strategy_name)

        if not strategy:
            raise HTTPException(status_code=404, detail="Стратегия не найдена")

        tf = strategy["timeframe"]
        log.info(f"[BB] Стратегия: {strategy_name} | таймфрейм: {tf}")

        positions = await conn.fetch("""
            SELECT position_uid, entry_price, pnl, direction
            FROM positions_v4
            WHERE strategy_id = $1 AND status = 'closed'
        """, strategy["id"])

        position_map = {
            p["position_uid"]: {
                "entry_price": float(p["entry_price"]),
                "pnl": p["pnl"],
                "direction": p["direction"]
            }
            for p in positions
        }

        bb_data_raw = await conn.fetch("""
            SELECT position_uid, param_name, value
            FROM position_ind_stat_v4
            WHERE timeframe = $2
              AND position_uid = ANY($1)
              AND (
                param_name LIKE 'bb20_2_5_%' OR
                param_name LIKE 'bb20_2_0_%' OR
                param_name LIKE 'bb20_1_5_%' OR
                param_name LIKE 'bb20_1_0_%'
              )
        """, list(position_map.keys()), tf)

        # Сборка BB-наборов по каждой позиции и каждому std
        bb_full_data = {
            std: defaultdict(dict) for std in BB_SETS
        }

        for row in bb_data_raw:
            uid = row["position_uid"]
            param = row["param_name"]
            for std in BB_SETS:
                if f"bb20_{std}_" in param:
                    base = f"bb20_{std}_"
                    bb_full_data[std][uid][param[len(base):]] = float(row["value"])

        bb_distribution = {
            std: {
                "success_long": [0]*BB_ZONES,
                "success_short": [0]*BB_ZONES,
                "fail_long": [0]*BB_ZONES,
                "fail_short": [0]*BB_ZONES,
            } for std in BB_SETS
        }

        for std in BB_SETS:
            for uid, bb in bb_full_data[std].items():
                if uid not in position_map:
                    continue
                if not all(k in bb for k in ["upper", "center", "lower"]):
                    continue

                info = position_map[uid]
                entry = info["entry_price"]
                pnl = info["pnl"]
                direction = info["direction"]

                zone = classify_zone(entry, bb["upper"], bb["center"], bb["lower"])

                key = (
                    "success_" if pnl >= 0 else "fail_"
                ) + direction

                bb_distribution[std][key][zone] += 1

    return templates.TemplateResponse("strategy_stats_bb.html", {
        "request": request,
        "strategy": dict(strategy),
        "filter": filter,
        "series": series,
        "bb_distribution": bb_distribution,
    })