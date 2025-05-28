import os
import json
import logging
from decimal import Decimal
from datetime import datetime

import asyncpg
import redis.asyncio as aioredis

from fastapi import FastAPI, Request, Form, HTTPException, status, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from starlette.status import HTTP_303_SEE_OTHER

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

# 🔸 Инициализация пула при запуске приложения
@app.on_event("startup")
async def startup():
    setup_logging()
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
log = logging.getLogger("TICKERS")

async def update_ticker_and_notify(ticker_id: int, field: str, new_value: str):
    async with pg_pool.acquire() as conn:
        await conn.execute(
            f"UPDATE tickers_v4 SET {field} = $1 WHERE id = $2",
            new_value, ticker_id
        )
        symbol = await conn.fetchval("SELECT symbol FROM tickers_v4 WHERE id = $1", ticker_id)

    event = {
        "type": field,
        "action": new_value,
        "symbol": symbol,
        "source": "web_ui"
    }

    # 🔹 Публикация события
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
    log.debug(f"{message} | {symbol} | bar_time={bar_time} | sent_at={sent_at}")

    # 🔹 Публикация в Redis Stream
    await redis_client.xadd("signals_stream", {
        "message": message,
        "symbol": symbol,
        "bar_time": bar_time or "",
        "sent_at": sent_at or "",
        "received_at": received_at
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
# 🔸 POST: создание стратегии + TP + SL-настройки
# 🔸 POST: создание стратегии + TP + SL-настройки
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
    sl_value: float = Form(...),
    reverse: bool = Form(False),
    sl_protection: bool = Form(False)
):
    enabled_bool = False
    if reverse:
        sl_protection = True

    form_data = await request.form()

    async with pg_pool.acquire() as conn:
        # Проверка имени
        exists = await conn.fetchval("SELECT EXISTS(SELECT 1 FROM strategies_v4 WHERE name = $1)", name)
        if exists:
            rows = await conn.fetch("SELECT id, name, enabled FROM signals_v4 ORDER BY id")
            signals = [{"id": r["id"], "name": r["name"], "enabled": bool(r["enabled"])} for r in rows]
            return templates.TemplateResponse("strategies_create.html", {
                "request": request,
                "signals": signals,
                "error": f"Стратегия с кодом '{name}' уже существует"
            })

        # Сохраняем стратегию
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
                false, true, true,
                true, $13, $14,
                NOW()
            )
            RETURNING id
        """, name, human_name, description, signal_id,
             deposit, position_limit, leverage, max_risk,
             timeframe.lower(), enabled_bool, reverse, sl_protection,
             sl_type, sl_value)

        strategy_id = result['id']

        # Вставка TP-уровней
        tp_level_ids = []
        level = 1
        while f"tp_{level}_volume" in form_data:
            volume = int(form_data.get(f"tp_{level}_volume"))
            tp_type = form_data.get(f"tp_{level}_type")
            tp_value = form_data.get(f"tp_{level}_value")
            value = float(tp_value) if tp_type != 'signal' else None

            row = await conn.fetchrow("""
                INSERT INTO strategy_tp_levels_v4 (
                    strategy_id, level, tp_type, tp_value, volume_percent, created_at
                )
                VALUES ($1, $2, $3, $4, $5, NOW())
                RETURNING id
            """, strategy_id, level, tp_type, value, volume)
            tp_level_ids.append(row["id"])
            level += 1

        # Вставка SL-настроек для TP уровней (начиная с 1..n-1)
        for i in range(1, len(tp_level_ids)):
            mode = form_data.get(f"sl_tp_{i}_mode")
            val = form_data.get(f"sl_tp_{i}_value")
            sl_val = float(val) if mode in ("percent", "atr") else None

            await conn.execute("""
                INSERT INTO strategy_tp_sl_v4 (
                    strategy_id, tp_level_id, sl_mode, sl_value, created_at
                )
                VALUES ($1, $2, $3, $4, NOW())
            """, strategy_id, tp_level_ids[i - 1], mode, sl_val)

    return RedirectResponse(url="/strategies", status_code=status.HTTP_303_SEE_OTHER)
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