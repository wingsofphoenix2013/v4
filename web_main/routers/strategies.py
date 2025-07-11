# 🔸 Маршруты стратегий (strategies)

import logging
import json

from fastapi import APIRouter, Request, Form, Body
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette import status
from decimal import Decimal, ROUND_DOWN
from datetime import datetime
from zoneinfo import ZoneInfo
from pydantic import BaseModel, condecimal

from main import KYIV_TZ

# 🔸 Инициализация
router = APIRouter()
log = logging.getLogger("STRATEGIES")

# 🔸 Внешние зависимости (инициализируются из main.py)
pg_pool = None
redis_client = None
templates = None  # будет присвоено в main.py

# 🔸 Страница со списком стратегий
@router.get("/strategies", response_class=HTMLResponse)
async def strategies_page(request: Request, filter: str = "all"):
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT s.id, s.name, s.human_name, s.timeframe, s.enabled,
                   COALESCE(sig.name, '-') AS signal_name
            FROM strategies_v4 s
            LEFT JOIN signals_v4 sig ON sig.id = s.signal_id
            ORDER BY s.id
        """)
    
    enabled_list = []
    disabled_list = []

    for r in rows:
        strategy = {
            "id": r["id"],
            "name": r["name"],
            "human_name": r["human_name"],
            "signal_name": r["signal_name"],
            "timeframe": r["timeframe"].upper(),
            "enabled": r["enabled"]
        }
        (enabled_list if r["enabled"] else disabled_list).append(strategy)

    return templates.TemplateResponse("strategies.html", {
        "request": request,
        "enabled_strategies": enabled_list,
        "disabled_strategies": disabled_list,
        "filter": filter
    })
# 🔸 POST: включение стратегии
@router.post("/strategies/{strategy_id}/enable")
async def enable_strategy(strategy_id: int, filter: str = Form("all")):
    await update_strategy_status(strategy_id, True)
    return RedirectResponse(url=f"/strategies?filter={filter}", status_code=status.HTTP_303_SEE_OTHER)

# 🔸 POST: отключение стратегии
@router.post("/strategies/{strategy_id}/disable")
async def disable_strategy(strategy_id: int, filter: str = Form("all")):
    await update_strategy_status(strategy_id, False)
    return RedirectResponse(url=f"/strategies?filter={filter}", status_code=status.HTTP_303_SEE_OTHER)

# 🔸 Обновление статуса стратегии и публикация события
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

# 🔸 Форма создания стратегии
@router.get("/strategies/create", response_class=HTMLResponse)
async def strategies_create_form(request: Request):
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name FROM signals_v4 ORDER BY id")
        signals = [{"id": r["id"], "name": r["name"]} for r in rows]

    return templates.TemplateResponse("strategies_create.html", {
        "request": request,
        "signals": signals,
        "error": None
    })

# 🔸 Сигналы по таймфрейму (AJAX)
@router.get("/strategies/signals_by_timeframe")
async def get_signals_by_tf(tf: str):
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, name FROM signals_v4
            WHERE enabled = true AND LOWER(timeframe) = LOWER($1)
            ORDER BY name
        """, tf)
        return [{"id": r["id"], "name": r["name"]} for r in rows]
# 🔸 POST: создание стратегии (без TP/SL/тикеров)
@router.post("/strategies/create", response_class=HTMLResponse)
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
    sl_value: str = Form(...),  # сохраняется как строка, потом оборачивается в Decimal
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
        # Проверка на уникальность
        exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM strategies_v4 WHERE name = $1)",
            name
        )
        if exists:
            rows = await conn.fetch("SELECT id, name, enabled FROM signals_v4 ORDER BY id")
            signals = [{"id": r["id"], "name": r["name"], "enabled": bool(r["enabled"])} for r in rows]
            return templates.TemplateResponse("strategies_create.html", {
                "request": request,
                "signals": signals,
                "error": f"Стратегия с кодом '{name}' уже существует"
            })

        # Вставка стратегии
        await conn.execute("""
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
        """, name, human_name, description, signal_id,
             deposit, position_limit, leverage, max_risk,
             timeframe.lower(), enabled_bool, reverse, sl_protection,
             use_all_tickers, sl_type, Decimal(sl_value))
        # Получаем ID вставленной стратегии
        result = await conn.fetchrow("SELECT id FROM strategies_v4 WHERE name = $1", name)
        strategy_id = result["id"]

        # 🔸 TP уровни
        tp_level_ids = []
        level = 1
        while f"tp_{level}_volume" in form_data:
            volume = int(form_data.get(f"tp_{level}_volume"))
            tp_type = form_data.get(f"tp_{level}_type")
            tp_value = form_data.get(f"tp_{level}_value")
            value = Decimal(tp_value) if tp_type != "signal" else None

            row = await conn.fetchrow("""
                INSERT INTO strategy_tp_levels_v4 (
                    strategy_id, level, tp_type, tp_value, volume_percent, created_at
                )
                VALUES ($1, $2, $3, $4, $5, NOW())
                RETURNING id
            """, strategy_id, level, tp_type, value, volume)
            tp_level_ids.append(row["id"])
            level += 1

        # 🔸 SL-настройки для TP уровней
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

        # 🔸 Привязка тикеров
        if not use_all_tickers:
            selected_ids = form_data.getlist("ticker_id[]")
            for tid in selected_ids:
                await conn.execute("""
                    INSERT INTO strategy_tickers_v4 (strategy_id, ticker_id, enabled)
                    VALUES ($1, $2, true)
                """, strategy_id, int(tid))
                
    return RedirectResponse(url="/strategies", status_code=status.HTTP_303_SEE_OTHER)
# 🔸 GET: список тикеров со статусом 'enabled' (для стратегии)
@router.get("/tickers/enabled")
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
@router.get("/strategies/check_name")
async def check_strategy_name(name: str):
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM strategies_v4 WHERE name = $1",
            name
        )
    return {"exists": row is not None}
# 🔸 Детали стратегии по name
@router.get("/strategies/details/{strategy_name}", response_class=HTMLResponse)
async def strategy_details(
    strategy_name: str,
    request: Request,
    filter: str = "all",
    page: int = 1,
    trade_page: int = 1
):
    async with pg_pool.acquire() as conn:
        # 🔹 Получение стратегии
        row = await conn.fetchrow("""
            SELECT s.*, COALESCE(sig.name, '-') AS signal_name
            FROM strategies_v4 s
            LEFT JOIN signals_v4 sig ON sig.id = s.signal_id
            WHERE s.name = $1
        """, strategy_name)

        if not row:
            return HTMLResponse(content="Стратегия не найдена", status_code=404)

        strategy = dict(row)

        # 🔹 Казначейство
        treasury_row = await conn.fetchrow("""
            SELECT pnl_total, pnl_operational, pnl_insurance, updated_at
            FROM strategies_treasury_v4
            WHERE strategy_id = $1
        """, strategy["id"])
        treasury = dict(treasury_row) if treasury_row else None

        # 🔹 Резерв под рост депозита
        reserve_required = None
        if treasury and strategy.get("deposit"):
            deposit = strategy["deposit"]
            reserve_required = Decimal(deposit) * Decimal((1.01 ** 7) - 1)

        # 🔹 Пагинация: казначейский лог
        log_limit = 10
        log_offset = max((page - 1), 0) * log_limit

        treasury_log = await conn.fetch("""
            SELECT timestamp, scenario, comment
            FROM strategies_treasury_meta_log_v4
            WHERE strategy_id = $1
            ORDER BY timestamp DESC
            LIMIT $2 OFFSET $3
        """, strategy["id"], log_limit, log_offset)

        log_total = await conn.fetchval("""
            SELECT COUNT(*) FROM strategies_treasury_meta_log_v4
            WHERE strategy_id = $1
        """, strategy["id"])

        # 🔹 Пагинация: Торговля
        trade_limit = 25
        trade_offset = max((trade_page - 1), 0) * trade_limit

        trades = await conn.fetch("""
            SELECT f.position_uid,
                   f.symbol,
                   f.created_at,
                   f.closed_at,
                   f.duration,
                   f.result,
                   COALESCE(t.comment, '-') AS comment
            FROM strategies_finmonitor_v4 f
            LEFT JOIN strategies_treasury_log_v4 t ON t.position_uid = f.position_uid
            WHERE f.strategy_id = $1
            ORDER BY f.closed_at DESC
            LIMIT $2 OFFSET $3
        """, strategy["id"], trade_limit, trade_offset)

        trade_total = await conn.fetchval("""
            SELECT COUNT(*) FROM strategies_finmonitor_v4
            WHERE strategy_id = $1
        """, strategy["id"])

        # 🔹 Конвертация времени в Europe/Kyiv
        trades = [dict(row) for row in trades]
        for row in trades:
            row["created_at"] = row["created_at"].replace(tzinfo=ZoneInfo("UTC")).astimezone(KYIV_TZ)
            row["closed_at"] = row["closed_at"].replace(tzinfo=ZoneInfo("UTC")).astimezone(KYIV_TZ)

    return templates.TemplateResponse("strategy_details.html", {
        "request": request,
        "strategy": strategy,
        "treasury": treasury,
        "reserve_required": reserve_required,
        "filter": filter,
        "page": page,
        "treasury_log": treasury_log,
        "log_total": log_total,
        "log_limit": log_limit,
        "trades": trades,
        "trade_page": trade_page,
        "trade_total": trade_total,
        "trade_limit": trade_limit,
    })
# 🔸 POST: Снятие средств из кассы

class WithdrawRequest(BaseModel):
    amount: float

@router.post("/strategies/details/{strategy_name}/withdraw")
async def withdraw_from_cash(strategy_name: str, payload: WithdrawRequest):
    amount = payload.amount

    async with pg_pool.acquire() as conn:
        async with conn.transaction():
            # 🔹 Получение ID стратегии и текущего баланса кассы
            row = await conn.fetchrow("""
                SELECT s.id AS strategy_id, t.pnl_operational
                FROM strategies_v4 s
                JOIN strategies_treasury_v4 t ON t.strategy_id = s.id
                WHERE s.name = $1
            """, strategy_name)

            if not row:
                raise HTTPException(status_code=404, detail="Стратегия не найдена")

            strategy_id = row["strategy_id"]
            current_cash = row["pnl_operational"]

            # 🔹 Проверка лимита
            if Decimal(amount) > current_cash:
                raise HTTPException(status_code=400, detail="Недостаточно средств")

            new_cash = current_cash - Decimal(amount)

            # 🔹 Обновление состояния кассы
            await conn.execute("""
                UPDATE strategies_treasury_v4
                SET pnl_operational = $1, updated_at = now()
                WHERE strategy_id = $2
            """, new_cash, strategy_id)

            # 🔹 Логирование события
            await conn.execute("""
                INSERT INTO strategies_treasury_meta_log_v4 (
                    strategy_id, timestamp, scenario, comment
                )
                VALUES ($1, $2, 'Снятие из кассы', $3)
            """, strategy_id, datetime.utcnow(),
                f"Снято из кассы ${float(amount):.2f}. Остаток в кассе ${float(new_cash):.2f}")

    return {"status": "ok"}
# 🔸 POST: Перевод между кассой и депозитом
class TransferRequest(BaseModel):
    amount: condecimal(gt=Decimal("-100000000"), lt=Decimal("100000000"))  # строго Decimal, безопасный диапазон

@router.post("/strategies/details/{strategy_name}/transfer")
async def transfer_cash_to_deposit(strategy_name: str, payload: TransferRequest):
    # 🔹 Округление суммы вниз до целых десятков
    amount = payload.amount.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    if amount == 0:
        raise HTTPException(status_code=400, detail="Сумма должна быть отлична от нуля")

    rounded = (abs(amount) // Decimal("10")) * Decimal("10")
    if amount < 0:
        rounded *= Decimal("-1")

    async with pg_pool.acquire() as conn:
        async with conn.transaction():
            # 🔹 Получение текущих значений по стратегии и казначейству
            row = await conn.fetchrow("""
                SELECT s.id AS strategy_id,
                       s.deposit,
                       s.position_limit,
                       t.pnl_operational
                FROM strategies_v4 s
                JOIN strategies_treasury_v4 t ON t.strategy_id = s.id
                WHERE s.name = $1
            """, strategy_name)

            if not row:
                raise HTTPException(status_code=404, detail="Стратегия не найдена")

            strategy_id = row["strategy_id"]
            deposit = row["deposit"]
            position_limit = row["position_limit"]
            cash = row["pnl_operational"]

            # 🔹 Обработка направления перевода
            if rounded > 0:
                # ➕ Перевод из кассы в депозит
                if rounded > cash:
                    raise HTTPException(status_code=400, detail="Недостаточно средств в кассе")

                new_cash = cash - rounded
                new_deposit = deposit + rounded
                new_limit = int(position_limit + rounded / Decimal("10"))

                scenario = "Перевод в депозит"
                comment = (
                    f"Переведено {rounded:.2f} из кассы в депозит. "
                    f"Новый депозит: {new_deposit:.2f}, лимит: {new_limit}"
                )

            else:
                # ➖ Обратный перевод из депозита в кассу
                rounded_abs = abs(rounded)

                if rounded_abs > deposit:
                    raise HTTPException(status_code=400, detail="Недостаточно средств в депозите")

                if position_limit < rounded_abs / Decimal("10"):
                    raise HTTPException(status_code=400, detail="Ограничение по лимиту")

                new_cash = cash + rounded_abs
                new_deposit = deposit - rounded_abs
                new_limit = int(position_limit - rounded_abs / Decimal("10"))

                scenario = "Перевод в кассу"
                comment = (
                    f"Возврат {rounded_abs:.2f} из депозита в кассу. "
                    f"Новый депозит: {new_deposit:.2f}, лимит: {new_limit}"
                )

            # 🔹 Обновление баланса казначейства
            await conn.execute("""
                UPDATE strategies_treasury_v4
                SET pnl_operational = $1, updated_at = now()
                WHERE strategy_id = $2
            """, new_cash, strategy_id)

            # 🔹 Обновление стратегии
            await conn.execute("""
                UPDATE strategies_v4
                SET deposit = $1, position_limit = $2
                WHERE id = $3
            """, new_deposit, new_limit, strategy_id)

            # 🔹 Логирование события
            await conn.execute("""
                INSERT INTO strategies_treasury_meta_log_v4 (
                    strategy_id, timestamp, scenario, comment
                )
                VALUES ($1, $2, $3, $4)
            """, strategy_id, datetime.utcnow(), scenario, comment)

            # 🔹 Публикация события в Redis
            await redis_client.xadd("strategy_update_stream", {
                "id": str(strategy_id),
                "type": "strategy",
                "action": "update",
                "source": "ui_event"
            })

    return {"status": "ok"}
# 🔸 POST: Изменение депозита вручную

class AdjustDepositRequest(BaseModel):
    amount: condecimal(gt=Decimal("-100000000"), lt=Decimal("100000000"))

@router.post("/strategies/details/{strategy_name}/adjust")
async def adjust_deposit(strategy_name: str, payload: AdjustDepositRequest):
    # 🔹 Округление до десятков
    amount = payload.amount.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    if amount == 0:
        raise HTTPException(status_code=400, detail="Сумма должна быть отлична от нуля")

    rounded = (abs(amount) // Decimal("10")) * Decimal("10")
    if amount < 0:
        rounded *= Decimal("-1")

    async with pg_pool.acquire() as conn:
        async with conn.transaction():
            # 🔹 Получение данных стратегии
            row = await conn.fetchrow("""
                SELECT id, deposit, position_limit
                FROM strategies_v4
                WHERE name = $1
            """, strategy_name)

            if not row:
                raise HTTPException(status_code=404, detail="Стратегия не найдена")

            strategy_id = row["id"]
            deposit = row["deposit"]
            limit = row["position_limit"]

            if rounded > 0:
                # ➕ Пополнение депозита
                new_deposit = deposit + rounded
                new_limit = int(limit + rounded / Decimal("10"))

                comment = (
                    f"Депозит пополнен на {rounded:.2f}. "
                    f"Новый депозит: {new_deposit:.2f}, лимит: {new_limit}"
                )
                scenario = "Пополнение депозита"

            else:
                # ➖ Изъятие депозита
                rounded_abs = abs(rounded)

                if rounded_abs > deposit:
                    raise HTTPException(status_code=400, detail="Недостаточно средств в депозите")

                new_deposit = deposit - rounded_abs
                new_limit = int(limit - rounded_abs / Decimal("10"))

                comment = (
                    f"Депозит уменьшен на {rounded_abs:.2f}. "
                    f"Новый депозит: {new_deposit:.2f}, лимит: {new_limit}"
                )
                scenario = "Изъятие депозита"

            # 🔹 Обновление стратегии
            await conn.execute("""
                UPDATE strategies_v4
                SET deposit = $1, position_limit = $2
                WHERE id = $3
            """, new_deposit, new_limit, strategy_id)

            # 🔹 Запись в лог
            await conn.execute("""
                INSERT INTO strategies_treasury_meta_log_v4 (
                    strategy_id, timestamp, scenario, comment
                )
                VALUES ($1, $2, $3, $4)
            """, strategy_id, datetime.utcnow(), scenario, comment)

            # 🔹 Уведомление в Redis
            await redis_client.xadd("strategy_update_stream", {
                "id": str(strategy_id),
                "type": "strategy",
                "action": "update",
                "source": "ui_event"
            })

    return {"status": "ok"}
@router.post("/strategies/{strategy_name}/toggle-auditor")
async def toggle_auditor(strategy_name: str):
    async with pg_pool.acquire() as conn:
        # Получаем текущее значение
        row = await conn.fetchrow("""
            SELECT id, auditor_enabled
            FROM strategies_v4
            WHERE name = $1
        """, strategy_name)

        if not row:
            raise HTTPException(status_code=404, detail="Стратегия не найдена")

        new_value = not row["auditor_enabled"]

        await conn.execute("""
            UPDATE strategies_v4
            SET auditor_enabled = $1
            WHERE id = $2
        """, new_value, row["id"])

    return RedirectResponse(url=f"/strategies/details/{strategy_name}", status_code=303)
# 🔸 Переключение флага Binance-торговли
@router.post("/strategies/{strategy_name}/toggle-binance")
async def toggle_binance(strategy_name: str):
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT id, binance_enabled
            FROM strategies_v4
            WHERE name = $1
        """, strategy_name)

        if not row:
            raise HTTPException(status_code=404, detail="Стратегия не найдена")

        strategy_id = row["id"]
        new_value = not row["binance_enabled"]

        await conn.execute("""
            UPDATE strategies_v4
            SET binance_enabled = $1
            WHERE id = $2
        """, new_value, strategy_id)

        # Публикация в Redis Pub/Sub
        payload = json.dumps({
            "strategy_id": strategy_id,
            "binance_enabled": new_value
        })

        await redis_client.publish("binance_strategy_updates", payload)

    return RedirectResponse(url=f"/strategies/details/{strategy_name}", status_code=303)
@router.post("/strategies/bulk-create", response_class=HTMLResponse)
async def bulk_create_strategies(request: Request):
    form = await request.form()
    tsv_input = form.get("tsv_input", "").strip()

    if not tsv_input:
        return templates.TemplateResponse("strategies_bulk_create.html", {
            "request": request,
            "error": "Пустой ввод TSV"
        })

    # 🔹 Общие параметры
    try:
        signal_id = int(form.get("signal_id"))
        deposit = int(form.get("deposit"))
        position_limit = int(form.get("position_limit"))
        leverage = int(form.get("leverage"))
        max_risk = int(form.get("max_risk"))
        timeframe = form.get("timeframe").lower()
        sl_type = form.get("sl_type")
        main_sl_value = Decimal(form.get("sl_value"))
        reverse = "reverse" in form
        sl_protection = reverse or ("sl_protection" in form)
        use_all_tickers = "use_all_tickers" in form
    except Exception as e:
        return templates.TemplateResponse("strategies_bulk_create.html", {
            "request": request,
            "error": f"Ошибка чтения параметров формы: {e}"
        })

    # 🔹 TP уровни
    tp_levels = []
    i = 1
    while f"tp_{i}_volume" in form:
        try:
            volume = int(form.get(f"tp_{i}_volume"))
            tp_type = form.get(f"tp_{i}_type")
            tp_value = form.get(f"tp_{i}_value") or None
            value = Decimal(tp_value) if tp_type != "signal" and tp_value else None
            tp_levels.append({
                "level": i,
                "type": tp_type,
                "value": value,
                "volume": volume
            })
            i += 1
        except Exception as e:
            return templates.TemplateResponse("strategies_bulk_create.html", {
                "request": request,
                "error": f"Ошибка разбора TP-уровней: TP{i}: {e}"
            })

    # 🔹 SL после TP
    sl_after_tp = []
    for j in range(1, len(tp_levels)):
        mode = form.get(f"sl_tp_{j}_mode")
        val_raw = form.get(f"sl_tp_{j}_value", "").strip()

        if mode in ("percent", "atr"):
            if not val_raw:
                return templates.TemplateResponse("strategies_bulk_create.html", {
                    "request": request,
                    "error": f"SL-значение обязательно для TP {j} при режиме '{mode}'"
                })
            try:
                tp_sl_value = Decimal(val_raw)
            except Exception:
                return templates.TemplateResponse("strategies_bulk_create.html", {
                    "request": request,
                    "error": f"Неверный формат SL-значения на TP {j}: '{val_raw}'"
                })
        else:
            tp_sl_value = None

        sl_after_tp.append({
            "tp_level_index": j - 1,
            "mode": mode,
            "value": tp_sl_value
        })

    # 🔹 Тикеры
    ticker_ids = []
    if not use_all_tickers:
        try:
            ticker_ids = [int(tid) for tid in form.getlist("ticker_id[]")]
        except Exception as e:
            return templates.TemplateResponse("strategies_bulk_create.html", {
                "request": request,
                "error": f"Ошибка разбора тикеров: {e}"
            })

    # 🔹 Разбор TSV
    strategies = []
    lines = tsv_input.strip().splitlines()
    for lineno, line in enumerate(lines, start=1):
        parts = line.strip().split('\t')
        if len(parts) != 3:
            return templates.TemplateResponse("strategies_bulk_create.html", {
                "request": request,
                "error": f"Неверный формат TSV на строке {lineno}: должно быть 3 колонки"
            })
        name, human_name, description = [p.strip() for p in parts]
        if not name:
            return templates.TemplateResponse("strategies_bulk_create.html", {
                "request": request,
                "error": f"Пустой код стратегии на строке {lineno}"
            })
        strategies.append({
            "name": name,
            "human_name": human_name,
            "description": description
        })

    # 🔹 Вставка в БД
    async with pg_pool.acquire() as conn:
        async with conn.transaction():
            for s in strategies:
                exists = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM strategies_v4 WHERE name = $1)",
                    s["name"]
                )
                if exists:
                    raise ValueError(f"Стратегия '{s['name']}' уже существует")

            for s in strategies:
                row = await conn.fetchrow("""
                    INSERT INTO strategies_v4 (
                        name, human_name, description, signal_id,
                        deposit, position_limit, leverage, max_risk,
                        timeframe, enabled, reverse, sl_protection,
                        archived, use_all_tickers, allow_open,
                        use_stoploss, sl_type, sl_value,
                        auditor_enabled, binance_enabled, created_at
                    )
                    VALUES (
                        $1, $2, $3, $4,
                        $5, $6, $7, $8,
                        $9, false, $10, $11,
                        false, $12, true,
                        true, $13, $14,
                        false, false, NOW()
                    )
                    RETURNING id
                """, s["name"], s["human_name"], s["description"], signal_id,
                     deposit, position_limit, leverage, max_risk,
                     timeframe, reverse, sl_protection,
                     use_all_tickers, sl_type, main_sl_value)
                strategy_id = row["id"]

                # TP уровни
                tp_level_ids = []
                for tp in tp_levels:
                    r = await conn.fetchrow("""
                        INSERT INTO strategy_tp_levels_v4 (
                            strategy_id, level, tp_type, tp_value, volume_percent, created_at
                        )
                        VALUES ($1, $2, $3, $4, $5, NOW())
                        RETURNING id
                    """, strategy_id, tp["level"], tp["type"], tp["value"], tp["volume"])
                    tp_level_ids.append(r["id"])

                # SL после TP
                for j, sl in enumerate(sl_after_tp):
                    await conn.execute("""
                        INSERT INTO strategy_tp_sl_v4 (
                            strategy_id, tp_level_id, sl_mode, sl_value, created_at
                        )
                        VALUES ($1, $2, $3, $4, NOW())
                    """, strategy_id, tp_level_ids[sl["tp_level_index"]], sl["mode"], sl["value"])

                # Тикеры
                if not use_all_tickers:
                    for tid in ticker_ids:
                        await conn.execute("""
                            INSERT INTO strategy_tickers_v4 (strategy_id, ticker_id, enabled)
                            VALUES ($1, $2, true)
                        """, strategy_id, tid)

    return RedirectResponse(url="/strategies", status_code=303)
@router.get("/strategies/bulk-create", response_class=HTMLResponse)
async def strategies_bulk_create_form(request: Request):
    return templates.TemplateResponse("strategies_bulk_create.html", {
        "request": request,
        "error": None
    })