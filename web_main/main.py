import os
import json
import logging
from decimal import Decimal
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict

import asyncpg
import redis.asyncio as aioredis

from fastapi import FastAPI, Request, Form, HTTPException, status, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response
from fastapi.templating import Jinja2Templates
from starlette.status import HTTP_303_SEE_OTHER

from prometheus_client import (
    Counter,
    Gauge,
    generate_latest,
    CONTENT_TYPE_LATEST
)

from infra import setup_logging, init_pg_pool, init_redis_client, pg_pool, redis_client

# 🔸 FastAPI и шаблоны
app = FastAPI()
templates = Jinja2Templates(directory="templates")

# 🔸 Временная зона и фильтрация по локальному времени (Киев)
KYIV_TZ = ZoneInfo("Europe/Kyiv")

def get_kyiv_day_bounds(days_ago: int = 0) -> tuple[datetime, datetime]:
    """
    Возвращает границы суток по Киеву в naive-UTC формате (для SQL через asyncpg).
    days_ago = 0 → сегодня, 1 → вчера и т.д.
    """
    now_kyiv = datetime.now(KYIV_TZ)
    target_day = now_kyiv.date() - timedelta(days=days_ago)

    start_kyiv = datetime.combine(target_day, time.min, tzinfo=KYIV_TZ)
    end_kyiv = datetime.combine(target_day, time.max, tzinfo=KYIV_TZ)

    return (
        start_kyiv.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
        end_kyiv.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    )

def get_kyiv_range_backwards(days: int) -> tuple[datetime, datetime]:
    """
    Возвращает диапазон последних N суток по Киеву — в naive-UTC формате (для SQL).
    """
    now_kyiv = datetime.now(KYIV_TZ)
    start_kyiv = now_kyiv - timedelta(days=days)

    return (
        start_kyiv.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
        now_kyiv.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    )
# 🔸 Инициализация пула при запуске приложения
@app.on_event("startup")
async def startup():
    setup_logging()
    await init_pg_pool()
    init_redis_client()

# 🔸 Получение всех тикеров из базы
async def get_all_tickers():
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, symbol, status, tradepermission,
                   precision_price, precision_qty, min_qty
            FROM tickers_v4
            ORDER BY id
        """)
        return [dict(row) for row in rows]

# 🔸 Добавление нового тикера в базу
async def add_new_ticker(data: dict):
    async with pg_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO tickers_v4 (symbol, status, tradepermission,
              precision_price, precision_qty, min_qty, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, NOW())
        """, data['symbol'], data['status'], data['tradepermission'],
              data['precision_price'], data['precision_qty'], data['min_qty'])

# 🔸 Проверка существования символа
async def ticker_exists(symbol: str) -> bool:
    async with pg_pool.acquire() as conn:
        result = await conn.fetchval("""
            SELECT EXISTS(SELECT 1 FROM tickers_v4 WHERE symbol = $1)
        """, symbol)
        return result

# 🔸 Главная страница интерфейса
@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# 🔸 Страница тикеров
@app.get("/tickers", response_class=HTMLResponse)
async def tickers_page(request: Request):
    tickers = await get_all_tickers()
    return templates.TemplateResponse("tickers.html", {"request": request, "tickers": tickers})

# 🔸 POST: Включение/выключение статуса и торговли
@app.post("/tickers/{ticker_id}/enable_status")
async def enable_status(ticker_id: int):
    await update_ticker_and_notify(ticker_id, field="status", new_value="enabled")
    return RedirectResponse(url="/tickers", status_code=HTTP_303_SEE_OTHER)

@app.post("/tickers/{ticker_id}/disable_status")
async def disable_status(ticker_id: int):
    await update_ticker_and_notify(ticker_id, field="status", new_value="disabled")
    return RedirectResponse(url="/tickers", status_code=HTTP_303_SEE_OTHER)

@app.post("/tickers/{ticker_id}/enable_trade")
async def enable_trade(ticker_id: int):
    await update_ticker_and_notify(ticker_id, field="tradepermission", new_value="enabled")
    return RedirectResponse(url="/tickers", status_code=HTTP_303_SEE_OTHER)

@app.post("/tickers/{ticker_id}/disable_trade")
async def disable_trade(ticker_id: int):
    await update_ticker_and_notify(ticker_id, field="tradepermission", new_value="disabled")
    return RedirectResponse(url="/tickers", status_code=HTTP_303_SEE_OTHER)

# 🔸 Форма создания тикера
@app.get("/tickers/create", response_class=HTMLResponse)
async def create_ticker_form(request: Request):
    return templates.TemplateResponse("tickers_create.html", {"request": request, "error": None})

# 🔸 Обработка создания тикера
@app.post("/tickers/create", response_class=HTMLResponse)
async def create_ticker(
    request: Request,
    symbol: str = Form(...),
    status: str = Form(...),
    tradepermission: str = Form(...),
    precision_price: int = Form(...),
    precision_qty: int = Form(...),
    min_qty: Decimal = Form(...)
):
    symbol_upper = symbol.upper()
    if await ticker_exists(symbol_upper):
        return templates.TemplateResponse("tickers_create.html", {
            "request": request,
            "error": f"Тикер '{symbol_upper}' уже существует"
        })

    await add_new_ticker({
        "symbol": symbol_upper,
        "status": status,
        "tradepermission": tradepermission,
        "precision_price": precision_price,
        "precision_qty": precision_qty,
        "min_qty": min_qty
    })
    return RedirectResponse(url="/tickers", status_code=HTTP_303_SEE_OTHER)
# 🔸 Обновление поля тикера и отправка уведомления в Redis
log = logging.getLogger("TICKERS")

async def update_ticker_and_notify(ticker_id: int, field: str, new_value: str):
    async with pg_pool.acquire() as conn:
        # 🔹 Обновление основного поля
        await conn.execute(
            f"UPDATE tickers_v4 SET {field} = $1 WHERE id = $2",
            new_value, ticker_id
        )

        # 🔹 Дополнительно: управление activated_at
        if field == "status":
            if new_value == "enabled":
                await conn.execute(
                    "UPDATE tickers_v4 SET activated_at = NOW() WHERE id = $1",
                    ticker_id
                )
            elif new_value == "disabled":
                await conn.execute(
                    "UPDATE tickers_v4 SET activated_at = NULL WHERE id = $1",
                    ticker_id
                )

        # 🔹 Получение символа тикера
        symbol = await conn.fetchval("SELECT symbol FROM tickers_v4 WHERE id = $1", ticker_id)

    # 🔹 Публикация события
    event = {
        "type": field,
        "action": new_value,
        "symbol": symbol,
        "source": "web_ui"
    }

    await redis_client.publish("tickers_v4_events", json.dumps(event))
    log.info(f"[PubSub] {event}")
# 🔸 Страница со списком всех расчётов индикаторов и их параметрами
@app.get("/indicators", response_class=HTMLResponse)
async def indicators_page(request: Request):
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT i.id, i.indicator, i.timeframe, i.enabled, i.stream_publish,
                   COALESCE(json_object_agg(p.param, p.value), '{}') AS parameters
            FROM indicator_instances_v4 i
            LEFT JOIN indicator_parameters_v4 p ON p.instance_id = i.id
            GROUP BY i.id
            ORDER BY i.id
        """)
        indicators = []
        for row in rows:
            indicators.append({
                "id": row["id"],
                "indicator": row["indicator"],
                "timeframe": row["timeframe"],
                "enabled": row["enabled"],
                "stream_publish": row["stream_publish"],
                "params": json.loads(row["parameters"]) if isinstance(row["parameters"], str) else row["parameters"]
            })
        return templates.TemplateResponse("indicators.html", {
            "request": request,
            "indicators": indicators
        })
# 🔸 POST: Включение/выключение расчёта индикатора и публикации
@app.post("/indicators/{indicator_id}/enable_status")
async def enable_indicator_status(indicator_id: int):
    await update_indicator_and_notify(indicator_id, field="enabled", new_value="true")
    return RedirectResponse(url="/indicators", status_code=HTTP_303_SEE_OTHER)

@app.post("/indicators/{indicator_id}/disable_status")
async def disable_indicator_status(indicator_id: int):
    await update_indicator_and_notify(indicator_id, field="enabled", new_value="false")
    return RedirectResponse(url="/indicators", status_code=HTTP_303_SEE_OTHER)

@app.post("/indicators/{indicator_id}/enable_stream")
async def enable_indicator_stream(indicator_id: int):
    await update_indicator_and_notify(indicator_id, field="stream_publish", new_value="true")
    return RedirectResponse(url="/indicators", status_code=HTTP_303_SEE_OTHER)

@app.post("/indicators/{indicator_id}/disable_stream")
async def disable_indicator_stream(indicator_id: int):
    await update_indicator_and_notify(indicator_id, field="stream_publish", new_value="false")
    return RedirectResponse(url="/indicators", status_code=HTTP_303_SEE_OTHER)
# 🔸 Обновление поля индикатора и отправка уведомления в Redis
log = logging.getLogger("INDICATORS")

async def update_indicator_and_notify(indicator_id: int, field: str, new_value: str):
    async with pg_pool.acquire() as conn:
        await conn.execute(
            f"UPDATE indicator_instances_v4 SET {field} = $1 WHERE id = $2",
            new_value == "true",  # преобразуем в bool
            indicator_id
        )

    event = {
        "id": indicator_id,
        "type": field,
        "action": new_value,
        "source": "web_ui"
    }

    await redis_client.publish("indicators_v4_events", json.dumps(event))
    log.info(f"[PubSub] {event}")
# 🔸 GET: отрисовка формы создания нового индикатора
@app.get("/indicators/create", response_class=HTMLResponse)
async def indicators_create_form(request: Request):
    return templates.TemplateResponse("indicators_create.html", {"request": request})
# 🔸 POST: создание нового расчёта индикатора и параметров
@app.post("/indicators/create")
async def create_indicator(
    request: Request,
    indicator: str = Form(...),
    status: str = Form(...),
    stream_publish: str = Form(...),
    timeframe: str = Form(...),
    param_count: int = Form(...),
):
    async with pg_pool.acquire() as conn:
        # Вставка в indicator_instances_v4 (без поля symbol)
        result = await conn.fetchrow(
            """
            INSERT INTO indicator_instances_v4 (indicator, timeframe, enabled, stream_publish, created_at)
            VALUES ($1, $2, $3, $4, NOW())
            RETURNING id
            """,
            indicator.lower(),
            timeframe,
            status == "enabled",
            stream_publish == "enabled"
        )
        instance_id = result["id"]

        # Чтение всех параметров из формы
        form = await request.form()
        for i in range(1, param_count + 1):
            param = form.get(f"param_{i}_name")
            value = form.get(f"param_{i}_value")
            await conn.execute(
                """
                INSERT INTO indicator_parameters_v4 (instance_id, param, value)
                VALUES ($1, $2, $3)
                """,
                instance_id, param, value
            )

    return RedirectResponse(url="/indicators", status_code=HTTP_303_SEE_OTHER)
# 🔸 Страница сигналов
@app.get("/signals", response_class=HTMLResponse)
async def signals_page(request: Request):
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, name, timeframe, long_phrase, short_phrase, description, enabled, source
            FROM signals_v4
            ORDER BY id
        """)
        signals = []
        for row in rows:
            signals.append({
                "id": row["id"],
                "name": row["name"],
                "description": row["description"],
                "phrase": f"{row['long_phrase']}\n{row['short_phrase']}",
                "timeframe": row["timeframe"].upper(),
                "source": row["source"],
                "enabled": row["enabled"],
            })
    return templates.TemplateResponse("signals.html", {"request": request, "signals": signals})
# 🔸 POST: включение/отключение сигнала
@app.post("/signals/{signal_id}/enable")
async def enable_signal(signal_id: int):
    await update_signal_status(signal_id, True)
    return RedirectResponse(url="/signals", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/signals/{signal_id}/disable")
async def disable_signal(signal_id: int):
    await update_signal_status(signal_id, False)
    return RedirectResponse(url="/signals", status_code=status.HTTP_303_SEE_OTHER)
# 🔸 Обновление статуса сигнала и отправка уведомления в Redis
log = logging.getLogger("SIGNALS")

async def update_signal_status(signal_id: int, new_value: bool):
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "UPDATE signals_v4 SET enabled = $1 WHERE id = $2",
            new_value, signal_id
        )

    event = {
        "id": signal_id,
        "type": "enabled",
        "action": str(new_value).lower(),
        "source": "web_ui"
    }

    await redis_client.publish("signals_v4_events", json.dumps(event))
    log.info(f"[PubSub] {event}")
# 🔸 GET: форма создания нового сигнала
@app.get("/signals/create", response_class=HTMLResponse)
async def signals_create_form(request: Request):
    return templates.TemplateResponse("signals_create.html", {"request": request, "error": None})
# 🔸 POST: создание нового сигнала
@app.post("/signals/create", response_class=HTMLResponse)
async def create_signal(
    request: Request,
    name: str = Form(...),
    long_phrase: str = Form(...),
    short_phrase: str = Form(...),
    timeframe: str = Form(...),
    source: str = Form(...),
    description: str = Form(...),
    enabled: str = Form(...)
):
    name = name.upper()
    long_phrase = long_phrase.upper()
    short_phrase = short_phrase.upper()
    timeframe = timeframe.lower()
    enabled_bool = enabled == "enabled"

    async with pg_pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM signals_v4 WHERE name = $1)", name
        )
        if exists:
            return templates.TemplateResponse("signals_create.html", {
                "request": request,
                "error": f"Сигнал с именем '{name}' уже существует"
            })

        await conn.execute("""
            INSERT INTO signals_v4 (name, long_phrase, short_phrase, timeframe, source, description, enabled, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
        """, name, long_phrase, short_phrase, timeframe, source, description, enabled_bool)

    return RedirectResponse(url="/signals", status_code=status.HTTP_303_SEE_OTHER)
# 🔸 Приём сигналов от TradingView (формат JSON, v4)
log = logging.getLogger("WEBHOOK")

@app.post("/webhook_v4")
async def webhook_v4(request: Request):
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    message = payload.get("message")
    symbol = payload.get("symbol")
    bar_time = payload.get("time")
    sent_at = payload.get("sent_at")

    if not message or not symbol:
        raise HTTPException(status_code=422, detail="Missing 'message' or 'symbol'")

    # 🔹 Очистка тикера от постфикса .P
    if symbol.endswith(".P"):
        symbol = symbol[:-2]

    received_at = datetime.utcnow().isoformat()

    # 🔹 Отладочный лог сигнала
    log.info(f"{message} | {symbol} | bar_time={bar_time} | sent_at={sent_at}")

    # 🔹 Публикация в Redis Stream с источником
    await redis_client.xadd("signals_stream", {
        "message": message,
        "symbol": symbol,
        "bar_time": bar_time or "",
        "sent_at": sent_at or "",
        "received_at": received_at,
        "source": "external_signal"
    })

    return JSONResponse({"status": "ok", "received_at": received_at})
# 🔸 Страница стратегий
@app.get("/strategies", response_class=HTMLResponse)
async def strategies_page(request: Request):
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT s.id, s.name, s.human_name, s.timeframe, s.enabled,
                   COALESCE(sig.name, '-') AS signal_name
            FROM strategies_v4 s
            LEFT JOIN signals_v4 sig ON sig.id = s.signal_id
            ORDER BY s.id
        """)
        strategies = []
        for r in rows:
            strategies.append({
                "id": r["id"],
                "name": r["name"],
                "human_name": r["human_name"],
                "signal_name": r["signal_name"],
                "timeframe": r["timeframe"].upper(),
                "enabled": r["enabled"]
            })

    return templates.TemplateResponse("strategies.html", {
        "request": request,
        "strategies": strategies
    })
# 🔸 POST: включение стратегии
@app.post("/strategies/{strategy_id}/enable")
async def enable_strategy(strategy_id: int):
    await update_strategy_status(strategy_id, True)
    return RedirectResponse(url="/strategies", status_code=status.HTTP_303_SEE_OTHER)

# 🔸 POST: отключение стратегии
@app.post("/strategies/{strategy_id}/disable")
async def disable_strategy(strategy_id: int):
    await update_strategy_status(strategy_id, False)
    return RedirectResponse(url="/strategies", status_code=status.HTTP_303_SEE_OTHER)
# 🔸 Обновление статуса стратегии и отправка уведомления в Redis
log = logging.getLogger("STRATEGIES")

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
# 🔸 GET: форма создания новой стратегии
@app.get("/strategies/create", response_class=HTMLResponse)
async def strategies_create_form(request: Request):
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name FROM signals_v4 ORDER BY id")
        signals = [{"id": r["id"], "name": r["name"]} for r in rows]

    return templates.TemplateResponse("strategies_create.html", {
        "request": request,
        "signals": signals,
        "error": None
    })
# 🔸 GET: сигналы по таймфрейму
@app.get("/strategies/signals_by_timeframe")
async def get_signals_by_tf(tf: str):
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, name FROM signals_v4
            WHERE enabled = true AND LOWER(timeframe) = LOWER($1)
            ORDER BY name
        """, tf)
        return [{"id": r["id"], "name": r["name"]} for r in rows]
# 🔸 POST: создание стратегии + TP + SL-настройки + привязка тикеров
@app.post("/strategies/create", response_class=HTMLResponse)
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
    sl_value: str = Form(...),  # важно: сохраняем как строку, чтобы обернуть в Decimal вручную
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
        exists = await conn.fetchval("SELECT EXISTS(SELECT 1 FROM strategies_v4 WHERE name = $1)", name)
        if exists:
            rows = await conn.fetch("SELECT id, name, enabled FROM signals_v4 ORDER BY id")
            signals = [{"id": r["id"], "name": r["name"], "enabled": bool(r["enabled"])} for r in rows]
            return templates.TemplateResponse("strategies_create.html", {
                "request": request,
                "signals": signals,
                "error": f"Стратегия с кодом '{name}' уже существует"
            })

        result = await conn.fetchrow("""
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
            RETURNING id
        """, name, human_name, description, signal_id,
             deposit, position_limit, leverage, max_risk,
             timeframe.lower(), enabled_bool, reverse, sl_protection,
             use_all_tickers, sl_type, Decimal(sl_value))  # ✅ точная вставка

        strategy_id = result['id']

        # TP уровни
        tp_level_ids = []
        level = 1
        while f"tp_{level}_volume" in form_data:
            volume = int(form_data.get(f"tp_{level}_volume"))
            tp_type = form_data.get(f"tp_{level}_type")
            tp_value = form_data.get(f"tp_{level}_value")
            value = Decimal(tp_value) if tp_type != 'signal' else None

            row = await conn.fetchrow("""
                INSERT INTO strategy_tp_levels_v4 (
                    strategy_id, level, tp_type, tp_value, volume_percent, created_at
                )
                VALUES ($1, $2, $3, $4, $5, NOW())
                RETURNING id
            """, strategy_id, level, tp_type, value, volume)
            tp_level_ids.append(row["id"])
            level += 1

        # SL-настройки для TP уровней
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

        # тикеры
        if not use_all_tickers:
            selected_ids = form_data.getlist("ticker_id[]")
            for tid in selected_ids:
                await conn.execute("""
                    INSERT INTO strategy_tickers_v4 (strategy_id, ticker_id, enabled)
                    VALUES ($1, $2, true)
                """, strategy_id, int(tid))

    return RedirectResponse(url="/strategies", status_code=status.HTTP_303_SEE_OTHER)
# 🔸 GET: список тикеров со статусом 'enabled' (для стратегии)
@app.get("/tickers/enabled")
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
@app.get("/strategies/check_name")
async def check_strategy_name(name: str):
    """
    Проверка уникальности кода стратегии (name) — вызывается из UI через AJAX
    """
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT 1 FROM strategies_v4 WHERE name = $1", name)
    return {"exists": row is not None}
@app.get("/testsignals", response_class=HTMLResponse)
async def testsignals_page(request: Request):
    async with pg_pool.acquire() as conn:
        # Все тикеры с включённым статусом
        tickers_all = await conn.fetch("""
            SELECT symbol FROM tickers_v4
            WHERE status = 'enabled'
        """)

        # Только активные тикеры (status=enabled и tradepermission=enabled)
        tickers_active = await conn.fetch("""
            SELECT symbol FROM tickers_v4
            WHERE status = 'enabled' AND tradepermission = 'enabled'
        """)

        # Все сигналы
        signals_all = await conn.fetch("""
            SELECT id, name, long_phrase, short_phrase FROM signals_v4
        """)

        # Только активные сигналы
        signals_active = await conn.fetch("""
            SELECT id, name, long_phrase, short_phrase FROM signals_v4
            WHERE enabled = true
        """)

    return templates.TemplateResponse("testsignals.html", {
        "request": request,
        "tickers_all": [r["symbol"] for r in tickers_all],
        "tickers_active": [r["symbol"] for r in tickers_active],
        "signals_all": [
            {
                "id": r["id"],
                "name": r["name"],
                "long_phrase": r["long_phrase"],
                "short_phrase": r["short_phrase"]
            } for r in signals_all
        ],
        "signals_active": [
            {
                "id": r["id"],
                "name": r["name"],
                "long_phrase": r["long_phrase"],
                "short_phrase": r["short_phrase"]
            } for r in signals_active
        ]
    })
from datetime import datetime  # убедись, что импорт добавлен выше

# 🔸 POST: сохранение тестового сигнала в журнал
log = logging.getLogger("TESTSIGNALS")

def to_naive_utc(dt_str):
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).replace(tzinfo=None)

@app.post("/testsignals/save")
async def save_testsignal(request: Request):
    data = await request.json()

    symbol = data.get("symbol")
    message = data.get("message")
    time_raw = data.get("time")
    sent_raw = data.get("sent_at")
    mode = data.get("mode")

    if not all([symbol, message, time_raw, sent_raw, mode]):
        raise HTTPException(status_code=400, detail="Missing required fields")

    time = to_naive_utc(time_raw)
    sent_at = to_naive_utc(sent_raw)

    async with pg_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO testsignals_v4 (symbol, message, time, sent_at, mode)
            VALUES ($1, $2, $3, $4, $5)
        """, symbol, message, time, sent_at, mode)

    log.info(f"Тестовый сигнал записан: {symbol} | {message} | {mode}")
    return JSONResponse({"status": "ok"})

# 🔸 Страница /trades — список активных стратегий с фильтрацией
@app.get("/trades", response_class=HTMLResponse)
async def trades_page(request: Request, filter: str = "today", series: str = None):
    """
    Выводит таблицу активных стратегий и статистику по фильтру:
    - filter = today / yesterday / 7days / all
    - series = "2", "3", "4", ... (по первой цифре кода стратегии, например strategy_2XX)
    """
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
    """
    Возвращает список активных стратегий и их статистику:
    - открытые/закрытые сделки
    - winrate (в %) и roi (в %) — по закрытым сделкам
    """
    async with pg_pool.acquire() as conn:
        strategies = await conn.fetch("""
            SELECT id, name, human_name, deposit
            FROM strategies_v4
            WHERE enabled = true
            ORDER BY id
        """)

        # 🔹 Временные рамки (UTC → naive для SQL)
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

            # 🔹 Закрытые сделки — общее число и победные
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

            # 🔹 Winrate (%)
            if closed_count > 0:
                winrate = round(win_count / closed_count * 100, 2)
            else:
                winrate = None

            # 🔹 ROI (%)
            if deposit and deposit != 0:
                roi = round(pnl_sum / deposit * 100, 2)
            else:
                roi = None

            # 🔹 Открытые сделки — только для "today"
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

        # 🔹 Сортировка по ROI (по убыванию, None — в конец)
        result.sort(key=lambda r: (r["roi"] is not None, r["roi"]), reverse=True)
        return result
# 🔸 Прометей-метрики
signals_processed_total = Counter(
    "signals_processed_total", "Обработано сигналов (всего)"
)
signals_dispatched_total = Counter(
    "signals_dispatched_total", "Отправлено в стратегии"
)
signals_ignored_total = Counter(
    "signals_ignored_total", "Проигнорировано"
)
processing_latency = Gauge(
    "processing_latency_ms", "Задержка обработки сигнала (мс)"
)

# 🔸 Эндпоинт Prometheus метрик
@app.get("/metrics")
async def metrics():
    stats = await redis_client.hgetall("metrics:signals")

    try:
        signals_processed_total._value.set(
            int(stats.get("signals_processed_total", 0))
        )
        signals_dispatched_total._value.set(
            int(stats.get("signals_dispatched_total", 0))
        )
        signals_ignored_total._value.set(
            int(stats.get("signals_ignored_total", 0))
        )
        processing_latency.set(
            float(stats.get("processing_latency_ms", 0))
        )
    except Exception:
        logging.getLogger("METRICS").warning("Ошибка при обновлении метрик")

    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
# 🔸 Эндпоинт: статус signals_v4
@app.get("/status", response_class=HTMLResponse)
async def status_page(request: Request):
    stats = await redis_client.hgetall("metrics:signals")

    # 🔹 Подстановка значений по умолчанию
    for key in [
        "signals_processed_total",
        "signals_dispatched_total",
        "signals_ignored_total",
        "processing_latency_ms"
    ]:
        stats.setdefault(key, "0")

    # 🔹 Отдаём шаблон status.html
    return templates.TemplateResponse(
        "status.html",
        {
            "request": request,
            "stats": stats
        }
    )
@app.get("/trades/details/{strategy_name}", response_class=HTMLResponse)
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
@app.get("/trades/details/{strategy_name}/stats", response_class=HTMLResponse)
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

    return templates.TemplateResponse("strategy_stats.html", {
        "request": request,
        "strategy": dict(strategy),
        "filter": filter,
        "series": series
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

@app.get("/trades/details/{strategy_name}/stats/rsi", response_class=HTMLResponse)
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

@app.get("/trades/details/{strategy_name}/stats/adx", response_class=HTMLResponse)
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

@app.get("/trades/details/{strategy_name}/stats/bb", response_class=HTMLResponse)
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