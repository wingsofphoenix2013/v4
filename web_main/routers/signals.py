# 🔸 Маршруты сигналов (signals)

import json
import logging

from fastapi import APIRouter, Request, Form, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()
log = logging.getLogger("SIGNALS")

# 🔸 Внешние зависимости (инициализируются извне)
pg_pool = None
redis_client = None
templates = Jinja2Templates(directory="templates")


# 🔸 Страница сигналов
@router.get("/signals", response_class=HTMLResponse)
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
@router.post("/signals/{signal_id}/enable")
async def enable_signal(signal_id: int):
    await update_signal_status(signal_id, True)
    return RedirectResponse(url="/signals", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/signals/{signal_id}/disable")
async def disable_signal(signal_id: int):
    await update_signal_status(signal_id, False)
    return RedirectResponse(url="/signals", status_code=status.HTTP_303_SEE_OTHER)


# 🔸 Обновление статуса сигнала и отправка уведомления в Redis
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
@router.get("/signals/create", response_class=HTMLResponse)
async def signals_create_form(request: Request):
    async with pg_pool.acquire() as conn:
        rules = await conn.fetch("SELECT name, description FROM signal_rules_v4 ORDER BY name")
    return templates.TemplateResponse("signals_create.html", {
        "request": request,
        "error": None,
        "rules": rules
    })


# 🔸 POST: создание нового сигнала
@router.post("/signals/create", response_class=HTMLResponse)
async def create_signal(
    request: Request,
    name: str = Form(...),
    long_phrase: str = Form(...),
    short_phrase: str = Form(...),
    timeframe: str = Form(...),
    source: str = Form(...),
    description: str = Form(...),
    enabled: str = Form(...),
    rule: str = Form(None)
):
    name = name.upper()
    long_phrase = long_phrase.upper()
    short_phrase = short_phrase.upper()
    timeframe = timeframe.lower()
    enabled_bool = enabled == "enabled"

    async with pg_pool.acquire() as conn:
        rules = await conn.fetch("SELECT name, description FROM signal_rules_v4 ORDER BY name")

        if source == "generator":
            valid_rule_names = {r["name"] for r in rules}
            if not rule or rule not in valid_rule_names:
                return templates.TemplateResponse("signals_create.html", {
                    "request": request,
                    "error": "Для источника 'generator' необходимо выбрать корректное правило",
                    "rules": rules
                })

        exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM signals_v4 WHERE name = $1)", name
        )
        if exists:
            return templates.TemplateResponse("signals_create.html", {
                "request": request,
                "error": f"Сигнал с именем '{name}' уже существует",
                "rules": rules
            })

        await conn.execute("""
            INSERT INTO signals_v4 (name, long_phrase, short_phrase, timeframe, source, rule, description, enabled, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
        """, name, long_phrase, short_phrase, timeframe, source, rule, description, enabled_bool)

    return RedirectResponse(url="/signals", status_code=status.HTTP_303_SEE_OTHER)
# 🔸 GET: страница подробностей сигнала
@router.get("/signals/{signal_id}", response_class=HTMLResponse)
async def signal_detail_page(request: Request, signal_id: int):
    async with pg_pool.acquire() as conn:
        signal = await conn.fetchrow("""
            SELECT id, name, description, long_phrase, short_phrase,
                   timeframe, source, rule, enabled
            FROM signals_v4
            WHERE id = $1
        """, signal_id)

        if not signal:
            raise HTTPException(status_code=404, detail="Сигнал не найден")

    return templates.TemplateResponse("signals_detail.html", {
        "request": request,
        "signal": signal,
        "logs": []  # пока без логов, добавим позже
    })