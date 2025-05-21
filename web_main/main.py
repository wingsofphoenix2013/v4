# main.py — стартовая точка FastAPI приложения для UI движка v4

import os
from decimal import Decimal
import redis.asyncio as aioredis
import json
import logging
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.status import HTTP_303_SEE_OTHER
import asyncpg

# 🔸 Настройка логирования
logging.basicConfig(level=logging.INFO)

# 🔸 Переменные окружения
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_USE_TLS = os.getenv("REDIS_USE_TLS", "false").lower() == "true"

# 🔸 Подключение к PostgreSQL (асинхронный пул)
pg_pool: asyncpg.Pool = None

async def init_pg_pool():
    return await asyncpg.create_pool(DATABASE_URL)

# 🔸 Подключение к Redis
redis_client: aioredis.Redis = None

def init_redis_client():
    protocol = "rediss" if REDIS_USE_TLS else "redis"
    return aioredis.from_url(
        f"{protocol}://{REDIS_HOST}:{REDIS_PORT}",
        password=REDIS_PASSWORD,
        decode_responses=True
    )

# 🔸 FastAPI и шаблоны
app = FastAPI()
templates = Jinja2Templates(directory="templates")

# 🔸 Инициализация пула при запуске приложения
@app.on_event("startup")
async def startup():
    global pg_pool, redis_client
    pg_pool = await init_pg_pool()
    redis_client = init_redis_client()

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
async def update_ticker_and_notify(ticker_id: int, field: str, new_value: str):
    # Обновление поля в PostgreSQL
    async with pg_pool.acquire() as conn:
        await conn.execute(
            f"UPDATE tickers_v4 SET {field} = $1 WHERE id = $2",
            new_value, ticker_id
        )
        symbol = await conn.fetchval("SELECT symbol FROM tickers_v4 WHERE id = $1", ticker_id)

    # Формирование события
    event = {
        "type": field,
        "action": new_value,
        "symbol": symbol,
        "source": "web_ui"
    }

    # Отправка в Redis Pub/Sub
    await redis_client.publish("tickers_v4_events", json.dumps(event))
    logging.info(f"[PubSub] {event}")

    # Отправка в Redis Stream
    stream_name = f"tickers_{field}_stream"
    await redis_client.xadd(stream_name, event)
    logging.info(f"[Stream:{stream_name}] {event}")
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
async def update_indicator_and_notify(indicator_id: int, field: str, new_value: str):
    # Обновление поля в базе
    async with pg_pool.acquire() as conn:
        await conn.execute(
            f"UPDATE indicator_instances_v4 SET {field} = $1 WHERE id = $2",
            new_value == "true",  # преобразуем в bool
            indicator_id
        )

    # Формирование события
    event = {
        "id": indicator_id,
        "type": field,
        "action": new_value,
        "source": "web_ui"
    }

    # Отправка в Redis Pub/Sub
    await redis_client.publish("indicators_v4_events", json.dumps(event))
    logging.info(f"[PubSub] {event}")

    # Отправка в Redis Stream
    stream_name = f"indicators_{field}_stream"
    await redis_client.xadd(stream_name, event)
    logging.info(f"[Stream:{stream_name}] {event}")
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
            indicator.upper(),
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