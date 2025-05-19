# main.py — стартовая точка FastAPI приложения для UI движка v4

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.status import HTTP_303_SEE_OTHER
from decimal import Decimal
import os

# Создание экземпляра FastAPI приложения
app = FastAPI()

# Настройка шаблонов Jinja2 (директория с HTML-файлами)
templates = Jinja2Templates(directory="templates")

# Временное хранилище тикеров до подключения PostgreSQL
in_memory_tickers = [
    {"id": 1, "symbol": "BTCUSDT", "status": "enabled", "tradepermission": "enabled"},
    {"id": 2, "symbol": "ETHUSDT", "status": "disabled", "tradepermission": "enabled"},
    {"id": 3, "symbol": "XRPUSDT", "status": "enabled", "tradepermission": "disabled"},
]

# Маршрут для главной страницы интерфейса
@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    # Отдаёт HTML-шаблон index.html с объектом запроса
    return templates.TemplateResponse("index.html", {"request": request})

# Маршрут для страницы тикеров
@app.get("/tickers", response_class=HTMLResponse)
async def tickers_page(request: Request):
    # Отдаёт HTML-шаблон tickers.html с данными тикеров
    return templates.TemplateResponse("tickers.html", {"request": request, "tickers": in_memory_tickers})

# Обработчик включения статуса тикера
@app.post("/tickers/{ticker_id}/enable_status")
async def enable_status(ticker_id: int):
    print(f"[DEBUG] Включить статус тикера ID={ticker_id}")
    return RedirectResponse(url="/tickers", status_code=HTTP_303_SEE_OTHER)

# Обработчик выключения статуса тикера
@app.post("/tickers/{ticker_id}/disable_status")
async def disable_status(ticker_id: int):
    print(f"[DEBUG] Выключить статус тикера ID={ticker_id}")
    return RedirectResponse(url="/tickers", status_code=HTTP_303_SEE_OTHER)

# Обработчик включения разрешения на торговлю
@app.post("/tickers/{ticker_id}/enable_trade")
async def enable_trade(ticker_id: int):
    print(f"[DEBUG] Включить торговлю для тикера ID={ticker_id}")
    return RedirectResponse(url="/tickers", status_code=HTTP_303_SEE_OTHER)

# Обработчик выключения разрешения на торговлю
@app.post("/tickers/{ticker_id}/disable_trade")
async def disable_trade(ticker_id: int):
    print(f"[DEBUG] Выключить торговлю для тикера ID={ticker_id}")
    return RedirectResponse(url="/tickers", status_code=HTTP_303_SEE_OTHER)

# Маршрут для отображения формы создания нового тикера
@app.get("/tickers/create", response_class=HTMLResponse)
async def create_ticker_form(request: Request):
    # Отдаёт HTML-шаблон tickers_create.html с пустым контекстом
    return templates.TemplateResponse("tickers_create.html", {"request": request, "error": None})

# Обработчик создания нового тикера (POST)
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
    # Проверка на уникальность symbol (в upper-case)
    symbol_upper = symbol.upper()
    for t in in_memory_tickers:
        if t["symbol"] == symbol_upper:
            # Если уже есть — возвращаем форму с ошибкой
            return templates.TemplateResponse("tickers_create.html", {
                "request": request,
                "error": f"Тикер '{symbol_upper}' уже существует"
            })

    # Создание нового тикера и добавление в список
    new_ticker = {
        "id": len(in_memory_tickers) + 1,
        "symbol": symbol_upper,
        "status": status,
        "tradepermission": tradepermission,
        "precision_price": precision_price,
        "precision_qty": precision_qty,
        "min_qty": min_qty
    }
    in_memory_tickers.append(new_ticker)

    return RedirectResponse(url="/tickers", status_code=HTTP_303_SEE_OTHER)
