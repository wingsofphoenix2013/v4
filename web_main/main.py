import os
import json
import logging
from decimal import Decimal
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo
from collections import defaultdict
import re
from markupsafe import Markup, escape

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

# 🔸 Переменные окружения
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_USE_TLS = os.getenv("REDIS_USE_TLS", "false").lower() == "true"
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"

# 🔸 Централизованная настройка логирования
def setup_logging():
    """
    Централизованная настройка логирования.
    DEBUG_MODE=True → debug/info/warning/error
    DEBUG_MODE=False → info/warning/error
    """
    level = logging.DEBUG if DEBUG_MODE else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

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

# 🔸 Фильтр Jinja: форматирование даты по-русски
MONTHS_RU = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря"
}

def format_date_ru(dt):
    if not dt:
        return ""
    return f"{dt.day:02d} {MONTHS_RU.get(dt.month, '')} {dt.year} года"

templates.env.filters["format_date_ru"] = format_date_ru

# 🔸 Фильтр Jinja: выделение сумм жирным
def highlight_amounts(text):
    if not text:
        return ""
    # Подсвечивает целые и дробные числа с необязательным знаком и сравнением
    pattern = r"(\b[><=]?\s?[+-]?\d+(?:[.,]\d{2})?\b)"
    highlighted = re.sub(pattern, r"<strong>\1</strong>", text)
    return Markup(highlighted)

templates.env.filters["highlight_amounts"] = highlight_amounts

# 🔸 Временная зона и фильтрация по локальному времени (Киев)
KYIV_TZ = ZoneInfo("Europe/Kyiv")

def get_kyiv_day_bounds(days_ago: int = 0) -> tuple[datetime, datetime]:
    """
    Возвращает границы суток по Киеву в naive-UTC формате (для SQL через asyncpg).
    days_ago = 0 → сегодня, 1 → вчера и т.д.
    """
    # Получаем "текущий момент" по UTC и преобразуем в Киев
    now_kyiv = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC")).astimezone(KYIV_TZ)
    target_day = now_kyiv.date() - timedelta(days=days_ago)

    # Создаём границы дня в Киевском времени
    start_kyiv = datetime.combine(target_day, time.min).replace(tzinfo=KYIV_TZ)
    end_kyiv = datetime.combine(target_day, time.max).replace(tzinfo=KYIV_TZ)

    # Преобразуем в UTC и убираем tzinfo (для SQL)
    return (
        start_kyiv.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
        end_kyiv.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    )

def get_kyiv_range_backwards(days: int) -> tuple[datetime, datetime]:
    """
    Возвращает диапазон последних N суток по Киеву в naive-UTC формате (для SQL).
    """
    # Получаем "текущий момент" по UTC и переводим в Киев
    now_kyiv = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC")).astimezone(KYIV_TZ)
    end_kyiv = now_kyiv

    # Начало диапазона: (N дней назад, в 00:00:00 по Киеву)
    start_day = (now_kyiv - timedelta(days=days)).date()
    start_kyiv = datetime.combine(start_day, time.min).replace(tzinfo=KYIV_TZ)

    return (
        start_kyiv.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
        end_kyiv.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    )

# 🔸 Инициализация пула при запуске приложения
@app.on_event("startup")
async def startup():
    setup_logging()
    global pg_pool, redis_client
    pg_pool = await init_pg_pool()
    redis_client = init_redis_client()

    # 🔸 Передаём зависимости в роутеры
    from routers import init_dependencies
    init_dependencies(pg_pool, redis_client, templates)

# 🔸 Подключаем все маршруты
from routers import routers
for router in routers:
    app.include_router(router)

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
# 🔸 Приём сигналов от TradingView (формат JSON, v4)
log = logging.getLogger("WEBHOOK")

# 🔹 Преобразование значения в ISO-8601 с 'Z' и без микросекунд
def normalize_iso_utc_z(value: str) -> str:
    try:
        match = re.search(r"\d{10}", value)
        if match:
            ts = int(match.group(0))
            dt = datetime.utcfromtimestamp(ts).replace(microsecond=0)
        else:
            # Удаляем 'Z', если есть, и разбираем как ISO
            dt = datetime.fromisoformat(value.replace("Z", "")).replace(microsecond=0)
        return dt.isoformat() + "Z"
    except Exception:
        return value  # fallback: оставить как есть

@app.post("/webhook_v4")
async def webhook_v4(request: Request):
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    message   = payload.get("message")
    symbol    = payload.get("symbol")
    bar_time  = payload.get("time")
    sent_at   = payload.get("sent_at")

    if not message or not symbol:
        raise HTTPException(status_code=422, detail="Missing 'message' or 'symbol'")

    # 🔹 Очистка тикера от постфикса .P
    if symbol.endswith(".P"):
        symbol = symbol[:-2]

    # 🔹 Приведение bar_time и sent_at к ISO с 'Z'
    bar_time = normalize_iso_utc_z(bar_time) if bar_time else ""
    sent_at  = normalize_iso_utc_z(sent_at)  if sent_at else ""

    # 🔹 Время получения — UTC с микросекундами, без 'Z'
    received_at = datetime.now(timezone.utc).isoformat()

    # 🔹 Отладочный лог сигнала
    log.info(f"{message} | {symbol} | bar_time={bar_time} | sent_at={sent_at}")

    # 🔹 Публикация в Redis Stream с источником
    await redis_client.xadd("signals_stream", {
        "message": message,
        "symbol": symbol,
        "bar_time": bar_time,
        "sent_at": sent_at,
        "received_at": received_at,
        "source": "external_signal"
    })

    return JSONResponse({"status": "ok", "received_at": received_at})
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
