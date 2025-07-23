# 🔸 Маршруты торгов

import logging
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from collections import defaultdict
from decimal import Decimal
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from main import KYIV_TZ, get_kyiv_day_bounds, get_kyiv_range_backwards

router = APIRouter()
log = logging.getLogger("TRADES")

# 🔸 Внешние зависимости (инициализируются из init_dependencies)
pg_pool = None
templates = Jinja2Templates(directory="templates")


# 🔸 Страница /trades — список активных стратегий с фильтрацией
@router.get("/trades", response_class=HTMLResponse)
async def trades_page(request: Request, filter: str = "24h", series: str = None):
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
        # Все стратегии
        strategies = await conn.fetch("""
            SELECT id, name, human_name, deposit
            FROM strategies_v4
            WHERE enabled = true
            ORDER BY id
        """)

        # 👑 Получаем последнюю запись из strategies_active_v4
        active_row = await conn.fetchrow("""
            SELECT * FROM strategies_active_v4
            ORDER BY ts DESC
            LIMIT 1
        """)
        current_king_id = active_row["strategy_id"] if active_row else None
        previous_king_id = active_row["previous_strategy_id"] if active_row else None

        # 🔁 Диапазон по фильтру
        if filter == "24h":
            end = datetime.utcnow()
            start = end - timedelta(hours=24)
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

            # Закрытые позиции
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

            # Открытые позиции
            if filter == "24h":
                open_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM positions_v4
                    WHERE strategy_id = $1 AND status = 'open'
                      AND created_at BETWEEN $2 AND $3
                """, sid, start, end)
            else:
                open_count = 0

            # 👑 и 🤡
            is_king = sid == current_king_id
            was_king = sid == previous_king_id

            result.append({
                "id": sid,
                "name": strat["name"],
                "human_name": strat["human_name"],
                "open": open_count,
                "closed": closed_count,
                "winrate": winrate,
                "roi": roi,
                "is_king": is_king,
                "was_king": was_king,
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

            date_key = day_start.replace(tzinfo=ZoneInfo("UTC")).astimezone(KYIV_TZ).strftime('%Y-%m-%d')
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
            get_kyiv_day_bounds(i)[0]
                .replace(tzinfo=ZoneInfo("UTC"))
                .astimezone(KYIV_TZ)
                .strftime('%Y-%m-%d')
            for i in reversed(range(days))
        ]
        today_key = stat_dates[-1]
        now = datetime.now(KYIV_TZ)

        # 🔍 Отладочный лог
        log.debug(f"[STATS] Сегодня: {get_kyiv_day_bounds(0)[0].replace(tzinfo=ZoneInfo('UTC')).astimezone(KYIV_TZ).strftime('%Y-%m-%d')}")
        log.debug(f"[STATS] Список дат: {stat_dates}")
        log.debug(f"[TIME] now() = {datetime.now()}")
        log.debug(f"[TIME] now(KYIV) = {datetime.now(KYIV_TZ)}")

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
            SELECT * FROM strategies_v4
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
                "total_pct": None,
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

        # 🔸 EMA анализ
        positions = await conn.fetch("""
            SELECT position_uid, entry_price, pnl, direction
            FROM positions_v4
            WHERE strategy_id = $1 AND status = 'closed'
        """, strategy["id"])

        position_map = {
            p["position_uid"]: {
                "entry": float(p["entry_price"]),
                "pnl": p["pnl"],
                "dir": p["direction"]
            }
            for p in positions
        }

        ema_keys = ["ema9", "ema14", "ema21", "ema50", "ema200"]
        tf = strategy["timeframe"]

        ema_raw = await conn.fetch(f"""
            SELECT position_uid, param_name, value
            FROM position_ind_stat_v4
            WHERE strategy_id = $1
              AND timeframe = $2
              AND param_name = ANY($3)
        """, strategy["id"], tf, ema_keys)

        ema_distribution = {
            k: {
                "success_long": [0, 0],
                "success_short": [0, 0],
                "fail_long": [0, 0],
                "fail_short": [0, 0],
            } for k in ema_keys
        }

        for row in ema_raw:
            uid = row["position_uid"]
            param = row["param_name"]
            ema = float(row["value"])

            if uid not in position_map:
                continue

            entry = position_map[uid]["entry"]
            pnl = position_map[uid]["pnl"]
            direction = position_map[uid]["dir"]

            above = 0 if entry > ema else 1
            key = ("success_" if pnl >= 0 else "fail_") + direction
            ema_distribution[param][key][above] += 1

    return templates.TemplateResponse("strategy_stats.html", {
        "request": request,
        "strategy": dict(strategy),
        "filter": filter,
        "series": series,
        "tickers": tickers,
        "summary_row": summary_row,
        "ema_distribution": ema_distribution
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
        log.debug(f"[RSI] Стратегия: {strategy_name} | таймфрейм: {tf}")

        positions = await conn.fetch("""
            SELECT position_uid, pnl, direction
            FROM positions_v4
            WHERE strategy_id = $1 AND status = 'closed'
        """, strategy["id"])

        position_map = {
            p["position_uid"]: {
                "pnl": float(p["pnl"] or 0.0),
                "direction": p["direction"]
            }
            for p in positions
        }

        log.debug(f"[RSI] Закрытых сделок: {len(position_map)}")

        rsi_data = await conn.fetch("""
            SELECT position_uid, value
            FROM position_ind_stat_v4
            WHERE param_name = 'rsi14'
              AND timeframe = $2
              AND position_uid = ANY($1)
        """, list(position_map.keys()), tf)

        log.debug(f"[RSI] RSI-записей по {tf}: {len(rsi_data)}")

        rsi_distribution = {
            "success_long": [0]*8,
            "success_short": [0]*8,
            "fail_long": [0]*8,
            "fail_short": [0]*8,
        }

        rsi_pnl_distribution = {
            "long": [[0.0]*8, [0.0]*8],  # [успешные, неуспешные]
            "short": [[0.0]*8, [0.0]*8],
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

            is_success = pnl >= 0
            group_key = ("success_" if is_success else "fail_") + direction
            rsi_distribution[group_key][idx] += 1

            if direction in ("long", "short"):
                i = 0 if is_success else 1
                rsi_pnl_distribution[direction][i][idx] += pnl

    return templates.TemplateResponse("strategy_stats_rsi.html", {
        "request": request,
        "strategy": dict(strategy),
        "filter": filter,
        "series": series,
        "rsi_distribution": rsi_distribution,
        "rsi_pnl_distribution": rsi_pnl_distribution,
        "rsi_bins": RSI_BINS,
        "rsi_inf": RSI_INF,
    })
# 🔸 Статистика стратегии по индикатору ADX
ADX_BINS = [(0, 10), (10, 15), (15, 20), (20, 25), (25, 30), (30, 35), (35, 40), (40, float("inf"))]
ADX_INF = float("inf")  # для шаблона

GAP_ZONES = [-float("inf"), -15, -5, 5, 10, 15, 20, 25, 30, float("inf")]
GAP_LABELS = ["≤-15", "-15–-5", "-5–+5", "+5–10", "+10–15", "+15–20", "+20–25", "+25–30", ">30"]

def bin_index(adx_value: float) -> int:
    for i, (lo, hi) in enumerate(ADX_BINS):
        if lo <= adx_value < hi:
            return i
    return len(ADX_BINS) - 1

def gap_bin_index(val: float) -> int:
    for i in range(len(GAP_ZONES) - 1):
        if GAP_ZONES[i] <= val < GAP_ZONES[i + 1]:
            return i
    return len(GAP_ZONES) - 2

@router.get("/trades/details/{strategy_name}/stats/adx", response_class=HTMLResponse)
async def strategy_adx_stats(
    request: Request,
    strategy_name: str,
    filter: str = None,
    series: str = None
):
    log = logging.getLogger("ADX_STATS")

    async with pg_pool.acquire() as conn:
        strategy = await conn.fetchrow("""
            SELECT * FROM strategies_v4
            WHERE name = $1
        """, strategy_name)

        if not strategy:
            raise HTTPException(status_code=404, detail="Стратегия не найдена")

        tf = strategy["timeframe"]
        log.debug(f"[ADX] Стратегия: {strategy_name} | таймфрейм: {tf}")

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

        log.debug(f"[ADX] Найдено закрытых сделок: {len(position_map)}")

        ind_data = await conn.fetch("""
            SELECT position_uid, param_name, value
            FROM position_ind_stat_v4
            WHERE position_uid = ANY($1)
              AND timeframe = $2
              AND param_name IN (
                'adx_dmi14_adx',
                'adx_dmi14_plus_di',
                'adx_dmi14_minus_di'
              )
        """, list(position_map.keys()), tf)

        adx_values = {}
        plus_di = {}
        minus_di = {}

        for row in ind_data:
            uid = row["position_uid"]
            val = float(row["value"])
            if row["param_name"] == "adx_dmi14_adx":
                adx_values[uid] = val
            elif row["param_name"] == "adx_dmi14_plus_di":
                plus_di[uid] = val
            elif row["param_name"] == "adx_dmi14_minus_di":
                minus_di[uid] = val

        log.debug(f"[ADX] Найдено ADX: {len(adx_values)} | +DI: {len(plus_di)} | -DI: {len(minus_di)}")

        # 🔸 Распределение по ADX-зонам
        adx_distribution = {
            "success_long": {"main": [0]*8},
            "success_short": {"main": [0]*8},
            "fail_long": {"main": [0]*8},
            "fail_short": {"main": [0]*8},
        }

        adx_summary = {
            "success": [0]*8,
            "fail": [0]*8,
        }

        # 🔸 Распределение по signed_gap
        signed_gap_success = {"long": [0]*9, "short": [0]*9}
        signed_gap_fail = {"long": [0]*9, "short": [0]*9}

        for uid, info in position_map.items():
            pnl = info["pnl"]
            direction = info["direction"]

            # --- ADX ---
            if uid in adx_values:
                adx = adx_values[uid]
                idx = bin_index(adx)

                if pnl >= 0:
                    adx_summary["success"][idx] += 1
                    adx_distribution[f"success_{direction}"]["main"][idx] += 1
                else:
                    adx_summary["fail"][idx] += 1
                    adx_distribution[f"fail_{direction}"]["main"][idx] += 1

            # --- Signed GAP ---
            if uid in plus_di and uid in minus_di:
                if direction == "long":
                    gap = plus_di[uid] - minus_di[uid]
                else:
                    gap = minus_di[uid] - plus_di[uid]

                gidx = gap_bin_index(gap)

                if pnl >= 0:
                    signed_gap_success[direction][gidx] += 1
                else:
                    signed_gap_fail[direction][gidx] += 1

    return templates.TemplateResponse("strategy_stats_adx.html", {
        "request": request,
        "strategy": dict(strategy),
        "filter": filter,
        "series": series,
        "adx_distribution": adx_distribution,
        "adx_summary": adx_summary,
        "adx_bins": ADX_BINS,
        "adx_inf": ADX_INF,
        "signed_gap_success": signed_gap_success,
        "signed_gap_fail": signed_gap_fail,
        "gap_labels": GAP_LABELS
    })
# 🔸 Статистика стратегии по Bollinger Bands
BB_ZONES = 6
BB_SETS = ["2_5", "2_0"]

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
        log.debug(f"[BB] Стратегия: {strategy_name} | таймфрейм: {tf}")

        positions = await conn.fetch("""
            SELECT position_uid, entry_price, pnl, direction
            FROM positions_v4
            WHERE strategy_id = $1 AND status = 'closed'
        """, strategy["id"])

        position_map = {
            p["position_uid"]: {
                "entry_price": float(p["entry_price"]),
                "pnl": float(p["pnl"] or 0.0),
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
                param_name LIKE 'bb20_2_0_%'
              )
        """, list(position_map.keys()), tf)

        bb_full_data = {std: defaultdict(dict) for std in BB_SETS}

        for row in bb_data_raw:
            uid = row["position_uid"]
            param = row["param_name"]
            for std in BB_SETS:
                base = f"bb20_{std}_"
                if param.startswith(base):
                    bb_full_data[std][uid][param[len(base):]] = float(row["value"])

        bb_distribution = {
            std: {
                "success_long": [0]*BB_ZONES,
                "success_short": [0]*BB_ZONES,
                "fail_long": [0]*BB_ZONES,
                "fail_short": [0]*BB_ZONES,
            } for std in BB_SETS
        }

        bb_pnl_distribution = {
            std: {
                "long": [[0.0]*BB_ZONES, [0.0]*BB_ZONES],   # [успешные, неуспешные]
                "short": [[0.0]*BB_ZONES, [0.0]*BB_ZONES],
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
                is_success = pnl >= 0
                group_key = ("success_" if is_success else "fail_") + direction

                bb_distribution[std][group_key][zone] += 1

                if direction in ("long", "short"):
                    index = 0 if is_success else 1
                    bb_pnl_distribution[std][direction][index][zone] += pnl

    return templates.TemplateResponse("strategy_stats_bb.html", {
        "request": request,
        "strategy": dict(strategy),
        "filter": filter,
        "series": series,
        "bb_distribution": bb_distribution,
        "bb_pnl_distribution": bb_pnl_distribution,
    })

# 🔸 Статистика стратегии по MFI
MFI_BINS = [(0, 10), (10, 20), (20, 30), (30, 40), (40, 50),
            (50, 60), (60, 70), (70, 80), (80, 90), (90, float("inf"))]

def mfi_bin_index(value: float) -> int:
    for i, (lo, hi) in enumerate(MFI_BINS):
        if lo <= value < hi:
            return i
    return len(MFI_BINS) - 1

@router.get("/trades/details/{strategy_name}/stats/mfi", response_class=HTMLResponse)
async def strategy_mfi_stats(
    request: Request,
    strategy_name: str,
    filter: str = None,
    series: str = None
):
    log = logging.getLogger("MFI_STATS")

    async with pg_pool.acquire() as conn:
        strategy = await conn.fetchrow("""
            SELECT * FROM strategies_v4
            WHERE name = $1
        """, strategy_name)

        if not strategy:
            raise HTTPException(status_code=404, detail="Стратегия не найдена")

        tf = strategy["timeframe"]
        log.debug(f"[MFI] Стратегия: {strategy_name} | таймфрейм: {tf}")

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

        mfi_data = await conn.fetch("""
            SELECT position_uid, value
            FROM position_ind_stat_v4
            WHERE param_name = 'mfi14'
              AND timeframe = $2
              AND position_uid = ANY($1)
        """, list(position_map.keys()), tf)

        log.debug(f"[MFI] Найдено записей: {len(mfi_data)}")

        result = {
            "success_long": [0]*10,
            "success_short": [0]*10,
            "fail_long": [0]*10,
            "fail_short": [0]*10,
        }

        summary = {
            "success": [0]*10,
            "fail": [0]*10,
        }

        for row in mfi_data:
            uid = row["position_uid"]
            if uid not in position_map:
                continue

            mfi = float(row["value"])
            info = position_map[uid]
            pnl = info["pnl"]
            direction = info["direction"]
            idx = mfi_bin_index(mfi)

            if pnl >= 0:
                summary["success"][idx] += 1
                if direction == "long":
                    result["success_long"][idx] += 1
                elif direction == "short":
                    result["success_short"][idx] += 1
            else:
                summary["fail"][idx] += 1
                if direction == "long":
                    result["fail_long"][idx] += 1
                elif direction == "short":
                    result["fail_short"][idx] += 1

    return templates.TemplateResponse("strategy_stats_mfi.html", {
        "request": request,
        "strategy": dict(strategy),
        "filter": filter,
        "series": series,
        "mfi_distribution": result,
        "mfi_summary": summary,
        "mfi_bins": MFI_BINS
    })
# 🔸 Статистика стратегии по LR (угол наклона)
LR_BINS = [
    (-float("inf"), -0.02),
    (-0.02, -0.015),
    (-0.015, -0.01),
    (-0.01, -0.005),
    (-0.005, 0),
    (0, 0),  # строго 0
    (0, 0.005),
    (0.005, 0.01),
    (0.01, 0.015),
    (0.015, 0.02),
    (0.02, float("inf"))
]

LR_LABELS = [
    "< -0.02", "-0.02–-0.015", "-0.015–-0.01", "-0.01–-0.005",
    "-0.005–0", "0", "0–0.005", "0.005–0.01", "0.01–0.015", "0.015–0.02", "> 0.02"
]

def lr_bin_index(angle: float) -> int:
    if angle == 0:
        return 5
    elif angle < -0.02:
        return 0
    elif angle < -0.015:
        return 1
    elif angle < -0.01:
        return 2
    elif angle < -0.005:
        return 3
    elif angle < 0:
        return 4
    elif angle <= 0.005:
        return 6
    elif angle <= 0.01:
        return 7
    elif angle <= 0.015:
        return 8
    elif angle <= 0.02:
        return 9
    else:
        return 10

@router.get("/trades/details/{strategy_name}/stats/lr", response_class=HTMLResponse)
async def strategy_lr_stats(
    request: Request,
    strategy_name: str,
    filter: str = None,
    series: str = None
):
    log = logging.getLogger("LR_STATS")

    async with pg_pool.acquire() as conn:
        strategy = await conn.fetchrow("""
            SELECT * FROM strategies_v4
            WHERE name = $1
        """, strategy_name)

        if not strategy:
            raise HTTPException(status_code=404, detail="Стратегия не найдена")

        tf = strategy["timeframe"]

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

        lr_data = await conn.fetch("""
            SELECT position_uid, value
            FROM position_ind_stat_v4
            WHERE param_name = 'lr50_angle'
              AND timeframe = $2
              AND position_uid = ANY($1)
        """, list(position_map.keys()), tf)

        log.debug(f"[LR] Найдено: {len(lr_data)} записей")

        result = {
            "success_long": [0]*11,
            "success_short": [0]*11,
            "fail_long": [0]*11,
            "fail_short": [0]*11,
        }

        summary = {
            "success": [0]*11,
            "fail": [0]*11,
        }

        for row in lr_data:
            uid = row["position_uid"]
            if uid not in position_map:
                continue

            angle = float(row["value"])
            info = position_map[uid]
            pnl = info["pnl"]
            direction = info["direction"]
            idx = lr_bin_index(angle)

            if pnl >= 0:
                summary["success"][idx] += 1
                result[f"success_{direction}"][idx] += 1
            else:
                summary["fail"][idx] += 1
                result[f"fail_{direction}"][idx] += 1

    return templates.TemplateResponse("strategy_stats_lr.html", {
        "request": request,
        "strategy": dict(strategy),
        "filter": filter,
        "series": series,
        "lr_distribution": result,
        "lr_summary": summary,
        "lr_labels": LR_LABELS
    })
# 🔸 Статистика стратегии по MACD (включая направление тренда)
MACD_CATEGORIES_EXT = [
    "↓↓ hist←", "↓↓ hist→",
    "↑↓ hist←", "↑↓ hist→",
    "←hist ↓↑", "→hist ↓↑",
    "←hist ↑↑", "→hist ↑↑"
]

def classify_macd_full(macd, signal, hist, plus_di, minus_di):
    if macd <= signal:
        base = "↓↓ hist" if macd < hist else "↑↓ hist"
        arrow = "→" if minus_di >= plus_di else "←"
        return f"{base}{arrow}"
    else:
        base = "hist ↓↑" if macd < hist else "hist ↑↑"
        arrow = "→" if plus_di > minus_di else "←"
        return f"{arrow}{base}"

@router.get("/trades/details/{strategy_name}/stats/macd", response_class=HTMLResponse)
async def strategy_macd_stats(
    request: Request,
    strategy_name: str,
    filter: str = None,
    series: str = None
):
    log = logging.getLogger("MACD_STATS")

    async with pg_pool.acquire() as conn:
        strategy = await conn.fetchrow("""
            SELECT * FROM strategies_v4
            WHERE name = $1
        """, strategy_name)

        if not strategy:
            raise HTTPException(status_code=404, detail="Стратегия не найдена")

        tf = strategy["timeframe"]

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

        ind_rows = await conn.fetch("""
            SELECT position_uid, param_name, value
            FROM position_ind_stat_v4
            WHERE position_uid = ANY($1)
              AND timeframe = $2
              AND param_name IN (
                'macd12_macd', 'macd12_macd_signal', 'macd12_macd_hist',
                'adx_dmi14_plus_di', 'adx_dmi14_minus_di'
              )
        """, list(position_map.keys()), tf)

        ind_map = defaultdict(dict)
        for row in ind_rows:
            ind_map[row["position_uid"]][row["param_name"]] = float(row["value"])

        result = {
            "success_long": {k: 0 for k in MACD_CATEGORIES_EXT},
            "success_short": {k: 0 for k in MACD_CATEGORIES_EXT},
            "fail_long": {k: 0 for k in MACD_CATEGORIES_EXT},
            "fail_short": {k: 0 for k in MACD_CATEGORIES_EXT},
        }

        for uid, info in position_map.items():
            data = ind_map.get(uid, {})
            if not all(k in data for k in [
                "macd12_macd", "macd12_macd_signal", "macd12_macd_hist",
                "adx_dmi14_plus_di", "adx_dmi14_minus_di"
            ]):
                continue

            macd = data["macd12_macd"]
            signal = data["macd12_macd_signal"]
            hist = data["macd12_macd_hist"]
            plus_di = data["adx_dmi14_plus_di"]
            minus_di = data["adx_dmi14_minus_di"]

            category = classify_macd_full(macd, signal, hist, plus_di, minus_di)

            key = f"{'success' if info['pnl'] >= 0 else 'fail'}_{info['direction']}"
            result[key][category] += 1

    return templates.TemplateResponse("strategy_stats_macd.html", {
        "request": request,
        "strategy": dict(strategy),
        "filter": filter,
        "series": series,
        "macd_distribution": result,
        "macd_categories_ext": MACD_CATEGORIES_EXT
    })