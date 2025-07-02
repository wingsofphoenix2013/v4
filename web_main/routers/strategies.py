# 🔸 Маршруты стратегий (strategies)

import logging

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette import status

router = APIRouter()
log = logging.getLogger("STRATEGIES")

# 🔸 Внешние зависимости (инициализируются из main.py)
pg_pool = None
redis_client = None
templates = Jinja2Templates(directory="templates")

# 🔸 Страница со списком стратегий
@router.get("/strategies", response_class=HTMLResponse)
async def strategies_page(request: Request, filter: str = "all"):
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT s.id, s.name, s.human_name, s.timeframe, s.enabled,
                   COALESCE(sig.name, '-') AS signal_name
            FROM strategies_v4 s
            LEFT JOIN signals_v4 sig ON sig.id = s.signal_id
            ORDER BY s.id
        """)
    
    enabled_list = []
    disabled_list = []

    for r in rows:
        strategy = {
            "id": r["id"],
            "name": r["name"],
            "human_name": r["human_name"],
            "signal_name": r["signal_name"],
            "timeframe": r["timeframe"].upper(),
            "enabled": r["enabled"]
        }
        (enabled_list if r["enabled"] else disabled_list).append(strategy)

    return templates.TemplateResponse("strategies.html", {
        "request": request,
        "enabled_strategies": enabled_list,
        "disabled_strategies": disabled_list,
        "filter": filter
    })
# 🔸 POST: включение стратегии
@router.post("/strategies/{strategy_id}/enable")
async def enable_strategy(strategy_id: int, filter: str = Form("all")):
    await update_strategy_status(strategy_id, True)
    return RedirectResponse(url=f"/strategies?filter={filter}", status_code=status.HTTP_303_SEE_OTHER)

# 🔸 POST: отключение стратегии
@router.post("/strategies/{strategy_id}/disable")
async def disable_strategy(strategy_id: int, filter: str = Form("all")):
    await update_strategy_status(strategy_id, False)
    return RedirectResponse(url=f"/strategies?filter={filter}", status_code=status.HTTP_303_SEE_OTHER)

# 🔸 Обновление статуса стратегии и публикация события
async def update_strategy_status(strategy_id: int, new_value: bool):
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "UPDATE strategies_v4 SET enabled = $1 WHERE id = $2",
            new_value, strategy_id
        )

    event = {
        "id": strategy_id,
        "type": "enabled",
        "action": str(new_value).lower(),
        "source": "web_ui"
    }

    await redis_client.publish("strategies_v4_events", json.dumps(event))
    log.info(f"[PubSub] {event}")

# 🔸 Форма создания стратегии
@router.get("/strategies/create", response_class=HTMLResponse)
async def strategies_create_form(request: Request):
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name FROM signals_v4 ORDER BY id")
        signals = [{"id": r["id"], "name": r["name"]} for r in rows]

    return templates.TemplateResponse("strategies_create.html", {
        "request": request,
        "signals": signals,
        "error": None
    })

# 🔸 Сигналы по таймфрейму (AJAX)
@router.get("/strategies/signals_by_timeframe")
async def get_signals_by_tf(tf: str):
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, name FROM signals_v4
            WHERE enabled = true AND LOWER(timeframe) = LOWER($1)
            ORDER BY name
        """, tf)
        return [{"id": r["id"], "name": r["name"]} for r in rows]
# 🔸 POST: создание стратегии (без TP/SL/тикеров)
@router.post("/strategies/create", response_class=HTMLResponse)
async def create_strategy(
    request: Request,
    name: str = Form(...),
    human_name: str = Form(...),
    description: str = Form(...),
    signal_id: int = Form(...),
    deposit: int = Form(...),
    position_limit: int = Form(...),
    leverage: int = Form(...),
    max_risk: int = Form(...),
    timeframe: str = Form(...),
    sl_type: str = Form(...),
    sl_value: str = Form(...),  # сохраняется как строка, потом оборачивается в Decimal
    reverse: bool = Form(False),
    sl_protection: bool = Form(False)
):
    enabled_bool = False
    if reverse:
        sl_protection = True

    form_data = await request.form()
    use_all_flag = form_data.get("use_all_tickers")
    use_all_tickers = use_all_flag == "on"

    async with pg_pool.acquire() as conn:
        # Проверка на уникальность
        exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM strategies_v4 WHERE name = $1)",
            name
        )
        if exists:
            rows = await conn.fetch("SELECT id, name, enabled FROM signals_v4 ORDER BY id")
            signals = [{"id": r["id"], "name": r["name"], "enabled": bool(r["enabled"])} for r in rows]
            return templates.TemplateResponse("strategies_create.html", {
                "request": request,
                "signals": signals,
                "error": f"Стратегия с кодом '{name}' уже существует"
            })

        # Вставка стратегии
        await conn.execute("""
            INSERT INTO strategies_v4 (
                name, human_name, description, signal_id,
                deposit, position_limit, leverage, max_risk,
                timeframe, enabled, reverse, sl_protection,
                archived, use_all_tickers, allow_open,
                use_stoploss, sl_type, sl_value,
                created_at
            )
            VALUES (
                $1, $2, $3, $4,
                $5, $6, $7, $8,
                $9, $10, $11, $12,
                false, $13, true,
                true, $14, $15,
                NOW()
            )
        """, name, human_name, description, signal_id,
             deposit, position_limit, leverage, max_risk,
             timeframe.lower(), enabled_bool, reverse, sl_protection,
             use_all_tickers, sl_type, Decimal(sl_value))
        # Получаем ID вставленной стратегии
        result = await conn.fetchrow("SELECT id FROM strategies_v4 WHERE name = $1", name)
        strategy_id = result["id"]

        # 🔸 TP уровни
        tp_level_ids = []
        level = 1
        while f"tp_{level}_volume" in form_data:
            volume = int(form_data.get(f"tp_{level}_volume"))
            tp_type = form_data.get(f"tp_{level}_type")
            tp_value = form_data.get(f"tp_{level}_value")
            value = Decimal(tp_value) if tp_type != "signal" else None

            row = await conn.fetchrow("""
                INSERT INTO strategy_tp_levels_v4 (
                    strategy_id, level, tp_type, tp_value, volume_percent, created_at
                )
                VALUES ($1, $2, $3, $4, $5, NOW())
                RETURNING id
            """, strategy_id, level, tp_type, value, volume)
            tp_level_ids.append(row["id"])
            level += 1

        # 🔸 SL-настройки для TP уровней
        for i in range(1, len(tp_level_ids)):
            mode = form_data.get(f"sl_tp_{i}_mode")
            val = form_data.get(f"sl_tp_{i}_value")
            sl_val = Decimal(val) if mode in ("percent", "atr") else None

            await conn.execute("""
                INSERT INTO strategy_tp_sl_v4 (
                    strategy_id, tp_level_id, sl_mode, sl_value, created_at
                )
                VALUES ($1, $2, $3, $4, NOW())
            """, strategy_id, tp_level_ids[i - 1], mode, sl_val)

        # 🔸 Привязка тикеров
        if not use_all_tickers:
            selected_ids = form_data.getlist("ticker_id[]")
            for tid in selected_ids:
                await conn.execute("""
                    INSERT INTO strategy_tickers_v4 (strategy_id, ticker_id, enabled)
                    VALUES ($1, $2, true)
                """, strategy_id, int(tid))
                
    return RedirectResponse(url="/strategies", status_code=status.HTTP_303_SEE_OTHER)
# 🔸 GET: список тикеров со статусом 'enabled' (для стратегии)
@router.get("/tickers/enabled")
async def get_enabled_tickers():
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, symbol
            FROM tickers_v4
            WHERE status = 'enabled'
            ORDER BY symbol
        """)
        return [{"id": r["id"], "symbol": r["symbol"]} for r in rows]

# 🔸 GET: проверка уникальности имени стратегии (AJAX от UI)
@router.get("/strategies/check_name")
async def check_strategy_name(name: str):
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM strategies_v4 WHERE name = $1",
            name
        )
    return {"exists": row is not None}
# 🔸 Детали стратегии по name
@router.get("/strategies/details/{strategy_name}", response_class=HTMLResponse)
async def strategy_details(strategy_name: str, request: Request, filter: str = "all"):
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT s.*, COALESCE(sig.name, '-') AS signal_name
            FROM strategies_v4 s
            LEFT JOIN signals_v4 sig ON sig.id = s.signal_id
            WHERE s.name = $1
        """, strategy_name)

    if not row:
        return HTMLResponse(content="Стратегия не найдена", status_code=404)

    strategy = dict(row)

    return templates.TemplateResponse("strategy_details.html", {
        "request": request,
        "strategy": strategy,
        "filter": filter
    })