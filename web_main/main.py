# main.py — стартовая точка FastAPI приложения для UI движка v4

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import os

# Создание экземпляра FastAPI приложения
app = FastAPI()

# Настройка шаблонов Jinja2 (директория с HTML-файлами)
templates = Jinja2Templates(directory="web_main/templates")

# Маршрут для главной страницы интерфейса
@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    # Отдаёт HTML-шаблон index.html с объектом запроса
    return templates.TemplateResponse("index.html", {"request": request})
