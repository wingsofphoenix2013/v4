# trader_tg_notifier.py — Telegram-уведомления через Redis Stream positions_bybit_audit (OPEN/CLOSE) + direction/TP/SL из БД и PnL (executions → Bybit closed-pnl fallback)

# 🔸 Импорты
import os
import json
import time
import asyncio
import logging
import random
from decimal import Decimal
from datetime import datetime
from typing import Optional, Iterable, Any, Dict, Tuple, List

import httpx

from trader_infra import infra

# 🔸 Логгер телеграм-уведомлений
log = logging.getLogger("TRADER_TG")

# 🔸 Конфигурация (берём из ENV)
_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # для каналов иногда отрицательное число

# 🔸 Режим отправки (ENV TRADER_TG_MODE: off|on)
def _normalize_mode(v: Optional[str]) -> str:
    # приводим к одному из: "off" | "on"
    s = (v or "").strip().lower()
    if s in ("off", "false", "0", "no", "disable", "disabled"):
        return "off"
    return "on"

_TG_MODE = _normalize_mode(os.getenv("TRADER_TG_MODE"))

# 🔸 Redis Streams (источник событий)
AUDIT_STREAM = "positions_bybit_audit"         # события системы (entry_filled, position_closed_*)

# 🔸 Consumer Group для TG-воркера
TG_CG = "trader_tg_cg"
TG_CONSUMER = os.getenv("TRADER_TG_CONSUMER", "tg-1")

# 🔸 Параметры воркера
MAX_PARALLEL_TASKS = int(os.getenv("TRADER_TG_MAX_TASKS", "50"))
DEDUP_TTL_SEC = int(os.getenv("TRADER_TG_DEDUP_TTL_SEC", "604800"))                # TTL ключа дедупликации (7 дней)
CLOSE_DEDUP_TTL_SEC = int(os.getenv("TRADER_TG_CLOSE_DEDUP_TTL_SEC", "604800"))    # TTL дедупа закрытия (7 дней)

# 🔸 Наборы заголовков (ротируются случайно)
_OPEN_HEADERS = [
    "🚀 We’re in — fresh entry on the board",
    "🎯 Button pressed. Position live.",
    "🧭 New position deployed — let’s navigate",
    "⚙️ Switch flipped — trade engaged",
    "🥷 Silent entry — let’s hunt",
    "🧠 Thesis locked — sending it",
    "🧩 Setup aligned — taking the shot",
    "🛰️ Signal pinged — we answered",
    "🧯 Risk set, breathe in — execute",
    "🪙 Coins on the table — let’s dance",
    "🦾 Machine says go — we go",
    "🧊 Calm entry — hot market",
    "🧪 Test passed — now we trade",
    "🧱 Brick placed — building position",
    "🎮 New level unlocked — position entered",
]

_WIN_HEADERS = [
    "🟢 Profit secured — the market blinked first 😎",
    "🟢 Winner winner, crypto dinner 🍽️",
    "🟢 Green ink day — we got paid 💚",
    "🟢 That exit slapped — bag secured 💰",
    "🟢 Trend befriended, wallet defended 🛡️",
    "🟢 Clean close — no drama, just numbers ✅",
    "🟢 Took what we came for — out we go 🚪",
    "🟢 Smooth landing — nice one ✈️",
    "🟢 Cash register noise intensifies 🧾",
    "🟢 Market donated — we accepted 🎁",
    "🟢 Green is a nice color today 🎨",
    "🟢 Another brick in the vault 🧱💰",
    "🟢 Secured. Next. 🔁",
]

_LOSS_HEADERS = [
    "🔴 Ouch. Market said “nope.” Moving on. 🧊",
    "🔴 Tuition paid to Mr. Market. Class dismissed. 📉",
    "🔴 We took one on the chin — next one’s ours 👊",
    "🔴 Red day, cool head. Reset and reload 🔁",
    "🔴 Loss logged, ego intact. Back to the lab 🧪",
    "🔴 Took the L — kept the plan ✅",
    "🔴 Small cut, big lesson 🩹",
    "🔴 Stop hit — discipline kept 🧭",
    "🔴 Not our wave — paddle back 🏄",
    "🔴 Market 1 — Us 0. Rematch soon 🥊",
    "🔴 A scratch, not a scar 🗒️",
    "🔴 Wrong door — we don’t live there 🚪",
    "🔴 Data collected — onward 📚",
]

_NEUTRAL_HEADERS = [
    "⚪ Position closed",
    "⚪ Close event",
]

# 🔸 Основной воркер: читаем AUDIT_STREAM и отправляем TG
async def run_trader_tg_notifier():
    redis = infra.redis_client

    # создание CG (id="$" — только новые записи)
    try:
        await redis.xgroup_create(AUDIT_STREAM, TG_CG, id="$", mkstream=True)
        log.info("📡 TG CG created: %s for %s", TG_CG, AUDIT_STREAM)
    except Exception:
        # группа уже существует
        pass

    # сброс offset CG на '$' — читать строго только новые записи после старта
    try:
        await redis.execute_command("XGROUP", "SETID", AUDIT_STREAM, TG_CG, "$")
        log.info("⏩ TG CG %s for %s set to $ (only new)", TG_CG, AUDIT_STREAM)
    except Exception:
        log.exception("❌ TG CG SETID failed: %s for %s", TG_CG, AUDIT_STREAM)

    # режим и готовность
    log.info(
        "🚀 TG notifier started: mode=%s chat_id=%s token=%s consumer=%s",
        _TG_MODE,
        _CHAT_ID if _CHAT_ID else "<none>",
        "set" if _BOT_TOKEN else "none",
        TG_CONSUMER,
    )

    sem = asyncio.Semaphore(MAX_PARALLEL_TASKS)

    while True:
        try:
            batch = await redis.xreadgroup(
                groupname=TG_CG,
                consumername=TG_CONSUMER,
                streams={AUDIT_STREAM: ">"},
                count=100,
                block=1000,  # мс
            )
            if not batch:
                continue

            tasks = []
            for _, records in batch:
                for entry_id, fields in records:
                    tasks.append(asyncio.create_task(_handle_audit_entry(sem, entry_id, fields)))

            await asyncio.gather(*tasks)

        except Exception:
            log.exception("❌ TG worker loop failed")
            await asyncio.sleep(1)

# 🔸 Обработка одной записи из positions_bybit_audit
async def _handle_audit_entry(sem: asyncio.Semaphore, entry_id: str, fields: Dict[str, Any]):
    async with sem:
        redis = infra.redis_client

        # дедуп (на случай ретраев/рестартов)
        # условия достаточности: если ключ уже есть — просто ACK
        dedup_key = f"tv4:tg:sent:{entry_id}"
        try:
            ok = await redis.set(dedup_key, "1", nx=True, ex=DEDUP_TTL_SEC)
            if not ok:
                await _ack_ok(entry_id)
                return
        except Exception:
            # если Redis дёрнулся — всё равно попробуем обработать, но без дедупа
            pass

        # парсинг payload
        try:
            data_raw = fields.get("data")
            if isinstance(data_raw, bytes):
                data_raw = data_raw.decode("utf-8", errors="ignore")
            payload = json.loads(data_raw or "{}")
        except Exception:
            log.exception("❌ TG: bad payload — ACK (id=%s)", entry_id)
            await _ack_ok(entry_id)
            return

        event = (payload.get("event") or "").strip()

        # интересующие события
        if event == "entry_filled":
            await _handle_open_event(payload, entry_id)
            await _ack_ok(entry_id)
            return

        if event in ("position_closed_by_closer", "position_closed_by_sl"):
            await _handle_close_event(payload, entry_id, close_event=event)
            await _ack_ok(entry_id)
            return

        # прочее — не интересует
        await _ack_ok(entry_id)

# 🔸 OPEN: entry_filled → direction + entry + TP/SL
async def _handle_open_event(payload: dict, entry_id: str):
    # условия достаточности
    symbol = payload.get("symbol")
    position_uid = payload.get("position_uid")
    avg_price = _as_decimal(payload.get("avg_price"))
    filled_qty = _as_decimal(payload.get("filled_qty"))

    if not symbol or not position_uid:
        log.info("TG OPEN skipped: missing symbol/position_uid (id=%s)", entry_id)
        return

    # достаём direction и created_at
    direction, created_at, _ = await _load_position_basics(position_uid)
    if not direction:
        direction = None

    # попытка подтянуть TP/SL цены
    tp_targets, sl_targets = await _load_tp_sl_targets(position_uid)

    hdr = random.choice(_OPEN_HEADERS)
    text = build_open_message(
        header=hdr,
        symbol=symbol,
        direction=direction,
        entry_price=avg_price,
        created_at=created_at or datetime.utcnow(),
        tp_targets=tp_targets,
        sl_targets=sl_targets,
        filled_qty=filled_qty,
    )

    await tg_send(text, disable_notification=False)
    log.info(
        "📨 TG OPEN sent: %s %s entry=%s qty=%s tp=%s sl=%s",
        symbol,
        direction or "?",
        _fmt_money(avg_price),
        _fmt_money(filled_qty),
        len(tp_targets or []),
        len(sl_targets or []),
    )

# 🔸 CLOSE: position_closed_by_* → direction + pnl (executions → closed-pnl fallback) + held
async def _handle_close_event(payload: dict, entry_id: str, *, close_event: str):
    # условия достаточности
    symbol = payload.get("symbol")
    position_uid = payload.get("position_uid")

    if not symbol or not position_uid:
        log.info("TG CLOSE skipped: missing symbol/position_uid (id=%s)", entry_id)
        return

    # дедуп закрытия по позиции (аудитор может прислать одно и то же закрытие через разные ветки)
    redis = infra.redis_client
    close_dedup_key = f"tv4:tg:close:{position_uid}"
    try:
        ok = await redis.set(close_dedup_key, "1", nx=True, ex=CLOSE_DEDUP_TTL_SEC)
        if not ok:
            log.info("↷ TG CLOSE duplicate suppressed: uid=%s event=%s", position_uid, close_event)
            return
    except Exception:
        # мягкий фолбэк — если Redis недоступен, всё равно попробуем
        pass

    direction, created_at, source_stream_id = await _load_position_basics(position_uid)
    closed_at = await _load_position_closed_at(position_uid)

    # PnL: отключено (не показываем и не считаем)
    pnl = None

    # заголовок: нейтральный (без попыток классифицировать результат)
    hdr = random.choice(_NEUTRAL_HEADERS)

    text = build_closed_message(
        header=hdr,
        symbol=symbol,
        direction=direction,
        pnl=pnl,
        created_at=created_at,
        closed_at=closed_at or datetime.utcnow(),
        close_event=close_event,
    )

    await tg_send(text, disable_notification=False)
    log.info(
        "📨 TG CLOSE sent: %s %s pnl=%s event=%s",
        symbol,
        direction or "?",
        _fmt_signed(pnl),
        close_event,
    )

# 🔸 Telegram: базовая отправка текста (HTML)
async def tg_send(text: str, *, disable_notification: bool = False) -> None:
    # режим off: не отправляем
    if _TG_MODE != "on":
        return

    # отсутствие токена/чата — пропускаем
    if not _BOT_TOKEN or not _CHAT_ID:
        log.info("ℹ️ TG skipped: TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set")
        return

    url = f"https://api.telegram.org/bot{_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": _CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
        "disable_notification": disable_notification,
    }

    try:
        # контролируемо: TG не должен зависеть от прокси Bybit
        async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
            r = await client.post(url, json=payload)
            if r.status_code != 200:
                log.warning("⚠️ TG send failed: %s %s", r.status_code, r.text)
            else:
                log.debug("TG sent ok")
    except Exception:
        log.exception("❌ TG send exception")

# 🔸 Загрузка базовой информации по позиции (direction, created_at, source_stream_id)
async def _load_position_basics(position_uid: str) -> Tuple[Optional[str], Optional[datetime], Optional[str]]:
    row = await infra.pg_pool.fetchrow(
        """
        SELECT direction, created_at, source_stream_id
        FROM trader_positions_log
        WHERE position_uid = $1
        """,
        position_uid,
    )
    if not row:
        return None, None, None
    direction = (row["direction"] or "").strip().lower() if row["direction"] else None
    created_at = row["created_at"] if row["created_at"] else None
    source_stream_id = row["source_stream_id"] if row["source_stream_id"] else None
    return direction, created_at, source_stream_id

# 🔸 Закрытие позиции: берём updated_at как время close (в текущей схеме reconcile это обновляет)
async def _load_position_closed_at(position_uid: str) -> Optional[datetime]:
    row = await infra.pg_pool.fetchrow(
        """
        SELECT updated_at
        FROM trader_positions_log
        WHERE position_uid = $1
        """,
        position_uid,
    )
    if not row:
        return None
    return row["updated_at"] if row["updated_at"] else None

# 🔸 TP/SL уровни: вытягиваем из trader_position_orders
async def _load_tp_sl_targets(position_uid: str) -> Tuple[List[dict], List[dict]]:
    rows = await infra.pg_pool.fetch(
        """
        SELECT kind, level, activation, activation_tp_level, price, qty, status, is_active
        FROM trader_position_orders
        WHERE position_uid = $1
          AND kind IN ('tp','sl')
          AND is_active = true
        ORDER BY kind, level, COALESCE(activation_tp_level, -1), updated_at DESC
        """,
        position_uid,
    )

    tps: List[dict] = []
    sls: List[dict] = []

    for r in rows or []:
        kind = r["kind"]
        price = _as_decimal(r["price"]) if r["price"] is not None else None
        qty = _as_decimal(r["qty"]) if r["qty"] is not None else None
        level = int(r["level"]) if r["level"] is not None else None
        item = {"level": level, "price": price, "qty": qty, "kind": kind}
        if kind == "tp":
            if price is not None and price > 0:
                tps.append(item)
        else:
            if price is not None and price > 0:
                sls.append(item)

    # TP: максимум первые 3
    tps = sorted(tps, key=lambda x: (x.get("level") or 10**9))[:3]

    # SL: предпочтительно level=0, иначе первый по level
    sl0 = [s for s in sls if (s.get("level") == 0)]
    if sl0:
        sls = [sl0[0]]
    else:
        sls = sorted(sls, key=lambda x: (x.get("level") or 10**9))
        sls = [sls[0]] if sls else []

    return tps, sls

# 🔸 ACK helper
async def _ack_ok(entry_id: str):
    try:
        await infra.redis_client.xack(AUDIT_STREAM, TG_CG, entry_id)
    except Exception:
        pass

# 🔸 Форматтеры
def _fmt_money(x: Optional[Decimal], max_prec: int = 8) -> str:
    if x is None:
        return "—"
    try:
        s = f"{x:.{max_prec}f}".rstrip("0").rstrip(".")
        return s if s else "0"
    except Exception:
        return str(x)

def _fmt_signed(x: Optional[Decimal], max_prec: int = 8) -> str:
    if x is None:
        return "—"
    try:
        sign = "+" if x >= 0 else ""
        return f"{sign}{_fmt_money(x, max_prec)}"
    except Exception:
        return str(x)

def _fmt_dt_utc(dt: Optional[datetime]) -> str:
    if not dt:
        return "—"
    return dt.strftime("%Y-%m-%d %H:%M") + " UTC"

def _side_arrow_and_word(direction: Optional[str]) -> tuple[str, str]:
    d = (direction or "").lower()
    return ("⬆️", "LONG") if d == "long" else ("⬇️", "SHORT") if d == "short" else ("↕️", "DIR?")

def _format_tp_section(tp_targets: Optional[Iterable[Any]], max_items: int = 3) -> str:
    if not tp_targets:
        return ""
    try:
        tps = sorted(tp_targets, key=lambda t: (int(t.get("level")) if isinstance(t, dict) and t.get("level") is not None else 10**9))
    except Exception:
        tps = list(tp_targets)

    lines = []
    shown = 0
    for t in tps:
        if shown >= max_items:
            break
        lvl = t.get("level") if isinstance(t, dict) else None
        price = t.get("price") if isinstance(t, dict) else None
        lvl_txt = f"TP{lvl}" if lvl is not None else "TP"
        lines.append(f"🎯 {lvl_txt}: <code>{_fmt_money(_as_decimal(price))}</code>")
        shown += 1

    more = len(tps) - shown
    suffix = f"\n➕ ... and {more} more TP" if more > 0 else ""
    return ("\n".join(lines)) + suffix

def _format_sl_section(sl_targets: Optional[Iterable[Any]]) -> str:
    if not sl_targets:
        return ""
    sl = None
    for s in sl_targets:
        sl = s
        break
    price = sl.get("price") if isinstance(sl, dict) else None
    return f"🛡️ SL: <code>{_fmt_money(_as_decimal(price))}</code>"

# 🔸 Конструкторы сообщений
def build_open_message(
    *,
    header: str,
    symbol: str,
    direction: Optional[str],
    entry_price: Optional[Decimal],
    created_at: datetime,
    tp_targets: Optional[Iterable[Any]] = None,
    sl_targets: Optional[Iterable[Any]] = None,
    filled_qty: Optional[Decimal] = None,
) -> str:
    arrow, side = _side_arrow_and_word(direction)
    tp_block = _format_tp_section(tp_targets)
    sl_block = _format_sl_section(sl_targets)

    parts = [
        f"{header}",
        "",
        f"{arrow} {side} on <b>{symbol}</b>",
        "",
        f"🎯 Entry: <code>{_fmt_money(entry_price)}</code>",
    ]

    # условия достаточности
    if filled_qty is not None:
        parts.append(f"📦 Qty: <code>{_fmt_money(filled_qty)}</code>")

    if tp_block:
        parts.append(tp_block)
    if sl_block:
        parts.append(sl_block)

    parts += [
        "",
        f"⏳ {_fmt_dt_utc(created_at)}",
    ]

    text = "\n".join(parts)
    while "\n\n\n" in text:
        text = text.replace("\n\n\n", "\n\n")
    return text

def build_closed_message(
    *,
    header: str,
    symbol: str,
    direction: Optional[str],
    pnl: Optional[Decimal],
    created_at: Optional[datetime],
    closed_at: Optional[datetime],
    close_event: str,
) -> str:
    arrow, side = _side_arrow_and_word(direction)

    # длительность удержания (минуты)
    held_line = "🕓 Held: —"
    if created_at and closed_at:
        try:
            minutes = int((closed_at - created_at).total_seconds() // 60)
            held_line = f"🕓 Held: {minutes} minutes"
        except Exception:
            pass

    lines = [
        f"{header}",
        "",
        f"{arrow} {side} on <b>{symbol}</b>",
        "",
        held_line,
        "",
        f"⏳ {_fmt_dt_utc(closed_at)}",
    ]
    return "\n".join(lines)

# 🔸 Утилиты
def _as_decimal(v) -> Optional[Decimal]:
    try:
        if v is None:
            return None
        if isinstance(v, Decimal):
            return v
        return Decimal(str(v))
    except Exception:
        return None