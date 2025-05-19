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
    return aioredis.from_url(
        f"redis://{REDIS_HOST}:{REDIS_PORT}",
        password=REDIS_PASSWORD,
        ssl=REDIS_USE_TLS,
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