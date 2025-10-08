# trader_tg_notifier.py — асинхронные уведомления в Telegram (open/close),
# с ротируемыми заголовками, стрелками направления, аккуратными переносами строк,
# TP/SL в открытии и портфельными метриками (24h/TOTAL ROI & Winrate) в закрытии.
#
# ⚠️ Важно: в строке стратегии используем ТОЛЬКО strategies_v4.name (не human_name).

# 🔸 Импорты
import os
import logging
import random
from decimal import Decimal
from datetime import datetime
from typing import Optional, Iterable, Any
import httpx

# 🔸 Логгер телеграм-уведомлений
log = logging.getLogger("TRADER_TG")

# 🔸 Конфигурация (берём из ENV)
_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # для каналов иногда отрицательное число

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
        log.debug("ℹ️ TG: пропуск — TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID не заданы")
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
                log.debug("📨 TG: отправлено")
    except Exception:
        log.exception("❌ TG: ошибка отправки")

# 🔸 Публичные отправители: open/close

async def send_open_notification(
    *,
    symbol: str,
    direction: Optional[str],
    entry_price: Optional[Decimal],
    strategy_name: str,         # ← ТОЛЬКО strategies_v4.name
    created_at: datetime,
    tp_targets: Optional[Iterable[Any]] = None,  # список TP (dict/tuple)
    sl_targets: Optional[Iterable[Any]] = None,  # список SL (dict/tuple)
    header: Optional[str] = None,
    silent: bool = False,
) -> None:
    # заголовок (без 🟢/🔴 для открытий)
    hdr = header or random.choice(_OPEN_HEADERS)
    text = build_open_message(
        header=hdr,
        symbol=symbol,
        direction=direction,
        entry_price=entry_price,
        created_at=created_at,
        strategy_name=strategy_name,
        tp_targets=tp_targets,
        sl_targets=sl_targets,
    )
    await tg_send(text, disable_notification=silent)

async def send_closed_notification(
    *,
    symbol: str,
    direction: Optional[str],
    pnl: Optional[Decimal],
    strategy_name: str,          # ← ТОЛЬКО strategies_v4.name
    created_at: Optional[datetime],  # для Held (минуты)
    closed_at: Optional[datetime],   # для Held (минуты)
    roi_24h: Optional[Decimal] = None,   # портфельный 24h ROI (доля)
    roi_total: Optional[Decimal] = None, # портфельный TOTAL ROI (доля)
    wr_24h: Optional[Decimal] = None,    # портфельный 24h Winrate (доля 0..1)
    wr_total: Optional[Decimal] = None,  # портфельный TOTAL Winrate (доля 0..1)
    header: Optional[str] = None,
    silent: bool = False,
) -> None:
    # заголовок: win или loss (с 🟢/🔴)
    if header:
        hdr = header
    else:
        is_win = (pnl or Decimal("0")) >= 0
        hdr = random.choice(_WIN_HEADERS if is_win else _LOSS_HEADERS)

    text = build_closed_message(
        header=hdr,
        symbol=symbol,
        direction=direction,
        pnl=pnl,
        strategy_name=strategy_name,
        created_at=created_at,
        closed_at=closed_at,
        roi_24h=roi_24h,
        roi_total=roi_total,
        wr_24h=wr_24h,
        wr_total=wr_total,
    )
    await tg_send(text, disable_notification=silent)

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

def _fmt_pct(x: Optional[Decimal], max_prec: int = 2) -> str:
    if x is None:
        return "—"
    try:
        val = x * Decimal("100")  # доля → проценты
        sign = "+" if val >= 0 else ""
        s = f"{val:.{max_prec}f}".rstrip("0").rstrip(".")
        return f"{sign}{s}%"
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

def _level_from(obj: Any) -> Optional[int]:
    try:
        if isinstance(obj, dict):
            return int(obj.get("level")) if obj.get("level") is not None else None
        if isinstance(obj, (tuple, list)) and len(obj) >= 1:
            return int(obj[0]) if obj[0] is not None else None
    except Exception:
        return None
    return None

def _price_from(obj: Any) -> Optional[Decimal]:
    try:
        if isinstance(obj, dict):
            v = obj.get("price")
        else:
            v = obj[1] if isinstance(obj, (tuple, list)) and len(obj) >= 2 else None
        if v is None:
            return None
        if isinstance(v, Decimal):
            return v
        return Decimal(str(v))
    except Exception:
        return None

def _format_tp_section(tp_targets: Optional[Iterable[Any]], max_items: int = 3) -> str:
    """TP-блок без количества — только цены (по заданию)."""
    if not tp_targets:
        return ""
    try:
        tps = sorted(tp_targets, key=lambda t: (_level_from(t) or 10**9))
    except Exception:
        tps = list(tp_targets)

    lines = []
    shown = 0
    for t in tps:
        if shown >= max_items:
            break
        lvl = _level_from(t)
        price = _price_from(t)
        lvl_txt = f"TP{lvl}" if lvl is not None else "TP"
        price_txt = _fmt_money(price)
        lines.append(f"🎯 {lvl_txt}: <code>{price_txt}</code>")
        shown += 1

    more = len(tps) - shown
    suffix = f"\n➕ ... and {more} more TP" if more > 0 else ""
    # добавим завершающий перевод строки, если есть что показать
    return ("\n".join(lines)) + suffix + ("\n" if lines or suffix else "")

def _format_sl_section(sl_targets: Optional[Iterable[Any]]) -> str:
    if not sl_targets:
        return ""
    # берём первый (обычно единственный) SL
    sl = None
    for s in sl_targets:
        sl = s
        break
    price = _price_from(sl)
    price_txt = _fmt_money(price)
    return f"🛡️ SL: <code>{price_txt}</code>\n"

# 🔸 Конструкторы сообщений

def build_open_message(
    *,
    header: str,
    symbol: str,
    direction: Optional[str],
    entry_price: Optional[Decimal],
    created_at: datetime,
    strategy_name: str,
    tp_targets: Optional[Iterable[Any]] = None,
    sl_targets: Optional[Iterable[Any]] = None,
) -> str:
    """
    Итоговый формат:
    <header>

    ⬆️ LONG on <symbol>

    🎯 Entry: <entry_price>
    🎯 TP1: <price>
    🛡️ SL: <price>

    🧠 Strategy: <name>

    ⏳ <created_at UTC>
    """
    arrow, side = _side_arrow_and_word(direction)
    tp_block = _format_tp_section(tp_targets)
    sl_block = _format_sl_section(sl_targets)

    parts = [
        f"{header}",
        "",
        f"{arrow} {side} on <b>{symbol}</b>",
        "",
        f"🎯 Entry: <code>{_fmt_money(entry_price)}</code>",
        tp_block.rstrip("\n"),
        sl_block.rstrip("\n"),
        "",
        f"🧠 Strategy: {strategy_name}",
        "",
        f"⏳ {_fmt_dt_utc(created_at)}",
    ]
    # уберём лишние пустые строки от возможных пустых TP/SL блоков
    text = "\n".join([line for line in parts if line is not None])
    while "\n\n\n" in text:
        text = text.replace("\n\n\n", "\n\n")
    return text

def build_closed_message(
    *,
    header: str,
    symbol: str,
    direction: Optional[str],
    pnl: Optional[Decimal],
    strategy_name: str,
    created_at: Optional[datetime],
    closed_at: Optional[datetime],
    roi_24h: Optional[Decimal] = None,   # доля (0.0123 → 1.23%)
    roi_total: Optional[Decimal] = None, # доля
    wr_24h: Optional[Decimal] = None,    # доля
    wr_total: Optional[Decimal] = None,  # доля
) -> str:
    """
    Итоговый формат:
    <win/loss header>

    ⬆️ LONG on <symbol>
    🧠 Strategy: <name>

    💵 PnL: +...

    📈 24h ROI: +..%
    📊 TOTAL ROI: +..%

    🥇 24h Winrate: ..%
    🏆 TOTAL Winrate: ..%

    🕓 Held: X minutes
    """
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
        f"🧠 Strategy: {strategy_name}",
        "",
        f"💵 PnL: <b>{_fmt_signed(pnl)}</b>",
        "",
        f"📈 24h ROI: <b>{_fmt_pct(roi_24h)}</b>",
        f"📊 TOTAL ROI: <b>{_fmt_pct(roi_total)}</b>",
        "",
        f"🥇 24h Winrate: <b>{_fmt_pct(wr_24h)}</b>",
        f"🏆 TOTAL Winrate: <b>{_fmt_pct(wr_total)}</b>",
        "",
        held_line,
    ]
    return "\n".join(lines)