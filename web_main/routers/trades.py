# 🔸 Маршруты торгов

import logging
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from main import get_kyiv_day_bounds, get_kyiv_range_backwards

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