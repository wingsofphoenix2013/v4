# 🔸 Маршруты сигналов (signals)

import json
import logging

from fastapi import APIRouter, Request, Form, status
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from main import KYIV_TZ

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
# 🔸 GET: страница подробностей сигнала с логами
@router.get("/signals/{signal_id}", response_class=HTMLResponse)
async def signal_detail_page(request: Request, signal_id: int, page: int = 1):
    page_size = 25
    offset = (page - 1) * page_size

    async with pg_pool.acquire() as conn:
        signal = await conn.fetchrow("""
            SELECT id, name, description, long_phrase, short_phrase,
                   timeframe, source, rule, enabled
            FROM signals_v4
            WHERE id = $1
        """, signal_id)

        if not signal:
            raise HTTPException(status_code=404, detail="Сигнал не найден")

        log_rows = await conn.fetch("""
            SELECT uid, symbol, direction, received_at, raw_message
            FROM signals_v4_log
            WHERE signal_id = $1
            ORDER BY logged_at DESC
            LIMIT $2 OFFSET $3
        """, signal_id, page_size + 1, offset)

        # 🔸 Загружаем статусы стратегий (opened/closed) по UID
        uids = [row["uid"] for row in log_rows[:page_size]]
        strategy_status_map = {}
        if uids:
            placeholders = ','.join(f"${i+1}" for i in range(len(uids)))
            query = f"""
                SELECT log_uid, strategy_id, status
                FROM signal_log_entries_v4
                WHERE log_uid IN ({placeholders})
                  AND status IN ('opened', 'closed')
            """
            status_rows = await conn.fetch(query, *uids)
            for r in status_rows:
                uid = r["log_uid"]
                sid = r["strategy_id"]
                status = r["status"]
                strategy_status_map.setdefault(uid, {}).setdefault(sid, set()).add(status)

    # 🔸 Сборка логов для отображения
    logs = []
    for row in log_rows[:page_size]:
        try:
            raw = json.loads(row["raw_message"])
            strategy_ids = sorted(raw.get("strategies", []))
        except Exception:
            strategy_ids = []

        uid = row["uid"]
        statuses_by_sid = strategy_status_map.get(uid, {})

        rendered = []
        for sid in strategy_ids:
            style = ""
            statuses = statuses_by_sid.get(sid, set())

            if "opened" in statuses:
                style = "text-green-600 font-semibold"
            elif "closed" in statuses:
                style = "text-red-600 font-semibold"

            if style:
                rendered.append(f'<span class="{style}">{sid}</span>')
            else:
                rendered.append(str(sid))

        logs.append({
            "uid": uid[:8] + "...",
            "full_uid": uid,
            "symbol": row["symbol"],
            "direction": row["direction"],
            "received_at": row["received_at"].astimezone(KYIV_TZ).strftime("%Y-%m-%d %H:%M"),
            "strategies": ", ".join(rendered)
        })

    has_next_page = len(log_rows) > page_size

    return templates.TemplateResponse("signals_detail.html", {
        "request": request,
        "signal": signal,
        "logs": logs,
        "page": page,
        "has_next_page": has_next_page
    })
# 🔸 API: детали обработки по UID
@router.get("/api/signal-log-details/{uid}")
async def get_signal_log_details(uid: str):
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT s.id, st.name, s.status, s.note, s.position_uid, s.logged_at
            FROM signal_log_entries_v4 s
            LEFT JOIN strategies_v4 st ON s.strategy_id = st.id
            JOIN signals_v4_log l ON s.log_uid = l.uid
            WHERE s.log_uid = $1
              AND s.logged_at <= l.received_at + interval '1 minute'
            ORDER BY st.name
        """, uid)

    result = []
    for row in rows:
        result.append({
            "id": row["id"],
            "name": row["name"],
            "status": row["status"],
            "note": row["note"],
            "position_uid": row["position_uid"][:8] + "..." if row["position_uid"] else "",
            "logged_at": row["logged_at"].astimezone(KYIV_TZ).strftime("%Y-%m-%d %H:%M")
        })

    return JSONResponse(result)