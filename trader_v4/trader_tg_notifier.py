# trader_tg_notifier.py — асинхронные уведомления в Telegram (open/close), с ротируемыми заголовками и стрелками направления

# 🔸 Импорты
import os
import logging
import random
from decimal import Decimal
from datetime import datetime
from typing import Optional
import httpx

# 🔸 Логгер телеграм-уведомлений
log = logging.getLogger("TRADER_TG")

# 🔸 Конфигурация (берём из ENV)
_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # для каналов обычно отрицательное число

# 🔸 Наборы заголовков (ротируются случайно)
_OPEN_HEADERS = [
    "🚀 We’re in — fresh entry on the board",
    "🎯 Button pressed. Position live.",
    "🧭 New position deployed — let’s navigate",
    "⚙️ Switch flipped — trade engaged",
    "🥷 Silent entry — let’s hunt",
]

_WIN_HEADERS = [
    "🟢 Profit secured — the market blinked first 😎",
    "🟢 Winner winner, crypto dinner 🍽️",
    "🟢 Green ink day — we got paid 💚",
    "🟢 That exit slapped — bag secured 💰",
    "🟢 Trend befriended, wallet defended 🛡️",
]

_LOSS_HEADERS = [
    "🔴 Ouch. Market said “nope.” Moving on. 🧊",
    "🔴 Tuition paid to Mr. Market. Class dismissed. 📉",
    "🔴 We took one on the chin — next one’s ours 👊",
    "🔴 Red day, cool head. Reset and reload 🔁",
    "🔴 Loss logged, ego intact. Back to the lab 🧪",
]

# 🔸 Базовая отправка текста (HTML)
async def tg_send(text: str, *, disable_notification: bool = False) -> None:
    if not _BOT_TOKEN or not _CHAT_ID:
        log.info("ℹ️ TG: пропуск — TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID не заданы")
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
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(url, json=payload)
            if r.status_code != 200:
                log.warning("⚠️ TG: %s %s", r.status_code, r.text)
            else:
                log.info("📨 TG: отправлено")
    except Exception:
        log.exception("❌ TG: ошибка отправки")

# 🔸 Публичные отправители: open/close (под ключи/формат системы)

async def send_open_notification(
    *,
    symbol: str,
    direction: Optional[str],
    entry_price: Optional[Decimal],
    margin_used: Decimal,
    strategy_id: int,
    group_id: int,
    created_at: datetime,
    header: Optional[str] = None,
    silent: bool = False,
) -> None:
    # выбираем заголовок (без 🟢/🔴 для открытий)
    hdr = header or random.choice(_OPEN_HEADERS)
    text = build_open_message(
        header=hdr,
        symbol=symbol,
        direction=direction,
        entry_price=entry_price,
        margin_used=margin_used,
        strategy_id=strategy_id,
        group_id=group_id,
        created_at=created_at,
    )
    await tg_send(text, disable_notification=silent)

async def send_closed_notification(
    *,
    symbol: str,
    direction: Optional[str],
    entry_price: Optional[Decimal],
    exit_price: Optional[Decimal],
    pnl: Optional[Decimal],
    created_at: Optional[datetime],
    closed_at: Optional[datetime],
    header: Optional[str] = None,
    silent: bool = False,
) -> None:
    # выбираем заголовок: win или loss (с 🟢/🔴 в заголовке)
    if header:
        hdr = header
    else:
        is_win = (pnl or Decimal("0")) >= 0
        hdr = random.choice(_WIN_HEADERS if is_win else _LOSS_HEADERS)

    text = build_closed_message(
        header=hdr,
        symbol=symbol,
        direction=direction,
        entry_price=entry_price,
        exit_price=exit_price,
        pnl=pnl,
        created_at=created_at,
        closed_at=closed_at,
    )
    await tg_send(text, disable_notification=silent)

# 🔸 Форматтеры и конструкторы сообщений

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
    # timestamps в БД наивные UTC — помечаем явно
    return dt.strftime("%Y-%m-%d %H:%M") + " UTC"

def _side_arrow_and_word(direction: Optional[str]) -> tuple[str, str]:
    # ⬆️ long, ⬇️ short
    d = (direction or "").lower()
    return ("⬆️", "LONG") if d == "long" else ("⬇️", "SHORT")

# 🔸 Конструктор сообщения об открытии
def build_open_message(
    *,
    header: str,
    symbol: str,
    direction: Optional[str],
    entry_price: Optional[Decimal],
    margin_used: Decimal,
    strategy_id: int,
    group_id: int,
    created_at: datetime,
) -> str:
    arrow, side = _side_arrow_and_word(direction)
    return (
        f"{header}\n\n"
        f"{arrow} {side} on <b>{symbol}</b>\n"
        f"🎯 Entry: <code>{_fmt_money(entry_price)}</code>\n"
        f"💼 Margin used: <code>{_fmt_money(margin_used)}</code>\n"
        f"🏷️ sid={strategy_id}, group={group_id}\n"
        f"⏳ {_fmt_dt_utc(created_at)}"
    )

# 🔸 Конструктор сообщения о закрытии
def build_closed_message(
    *,
    header: str,
    symbol: str,
    direction: Optional[str],
    entry_price: Optional[Decimal],
    exit_price: Optional[Decimal],
    pnl: Optional[Decimal],
    created_at: Optional[datetime],
    closed_at: Optional[datetime],
) -> str:
    arrow, side = _side_arrow_and_word(direction)

    # длительность
    dur = "—"
    if created_at and closed_at:
        try:
            minutes = int((closed_at - created_at).total_seconds() // 60)
            dur = f"{minutes} minutes"
        except Exception:
            pass

    return (
        f"{header}\n\n"
        f"{arrow} {side} on <b>{symbol}</b>\n"
        f"🎯 Entry: <code>{_fmt_money(entry_price)}</code>\n"
        f"🏁 Exit: <code>{_fmt_money(exit_price)}</code>\n"
        f"💵 PnL: <b>{_fmt_signed(pnl)}</b>\n"
        f"🕓 Held: {dur}\n"
        f"⏳ {_fmt_dt_utc(created_at)} → {_fmt_dt_utc(closed_at)}"
    )