# main.py — стартовая точка FastAPI приложения для UI движка v4

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.status import HTTP_303_SEE_OTHER
import os

# Создание экземпляра FastAPI приложения
app = FastAPI()

# Настройка шаблонов Jinja2 (директория с HTML-файлами)
templates = Jinja2Templates(directory="templates")

# Маршрут для главной страницы интерфейса
@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    # Отдаёт HTML-шаблон index.html с объектом запроса
    return templates.TemplateResponse("index.html", {"request": request})

# Маршрут для страницы тикеров
@app.get("/tickers", response_class=HTMLResponse)
async def tickers_page(request: Request):
    # Временные тестовые данные тикеров
    tickers = [
        {"id": 1, "symbol": "BTCUSDT", "status": "enabled", "tradepermission": "enabled"},
        {"id": 2, "symbol": "ETHUSDT", "status": "disabled", "tradepermission": "enabled"},
        {"id": 3, "symbol": "XRPUSDT", "status": "enabled", "tradepermission": "disabled"},
    ]
    # Отдаёт HTML-шаблон tickers.html с данными тикеров
    return templates.TemplateResponse("tickers.html", {"request": request, "tickers": tickers})

# Обработчик включения статуса тикера
@app.post("/tickers/{ticker_id}/enable_status")
async def enable_status(ticker_id: int):
    # Здесь будет логика включения статуса тикера
    print(f"[DEBUG] Включить статус тикера ID={ticker_id}")
    return RedirectResponse(url="/tickers", status_code=HTTP_303_SEE_OTHER)

# Обработчик выключения статуса тикера
@app.post("/tickers/{ticker_id}/disable_status")
async def disable_status(ticker_id: int):
    # Здесь будет логика выключения статуса тикера
    print(f"[DEBUG] Выключить статус тикера ID={ticker_id}")
    return RedirectResponse(url="/tickers", status_code=HTTP_303_SEE_OTHER)

# Обработчик включения разрешения на торговлю
@app.post("/tickers/{ticker_id}/enable_trade")
async def enable_trade(ticker_id: int):
    # Здесь будет логика включения торговли для тикера
    print(f"[DEBUG] Включить торговлю для тикера ID={ticker_id}")
    return RedirectResponse(url="/tickers", status_code=HTTP_303_SEE_OTHER)

# Обработчик выключения разрешения на торговлю
@app.post("/tickers/{ticker_id}/disable_trade")
async def disable_trade(ticker_id: int):
    # Здесь будет логика выключения торговли для тикера
    print(f"[DEBUG] Выключить торговлю для тикера ID={ticker_id}")
    return RedirectResponse(url="/tickers", status_code=HTTP_303_SEE_OTHER)
