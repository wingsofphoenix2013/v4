# trader_sl_handler.py — обработчик SL-protect:
# слушает СИСТЕМНЫЕ события из positions_bybit_status и при необходимости инициирует перестановку биржевого SL на entry.
# FIX #2: не переносить SL на бирже из-за системного tp_hit — двойные гейты:
#   (1) недавний system tp_hit (TTL), (2) есть активные priced TP на бирже.

# 🔸 Импорты
import asyncio
import logging
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Optional, Dict, Tuple

import httpx

from trader_infra import infra
from trader_config import config

# 🔸 Логгер
log = logging.getLogger("TRADER_SL_HANDLER")

# 🔸 Константы стримов / CG
STATUS_STREAM = "positions_bybit_status"          # источник событий системы
CG_NAME = "trader_sl_handler_group"
CONSUMER_NAME = "trader_sl_handler_1"

MAINTAINER_STREAM = "trader_maintainer_events"    # куда отправляем команду на перестановку SL

# 🔸 Bybit public REST для получения lastPrice
BYBIT_PUBLIC_BASE = "https://api.bybit.com"
BYBIT_CATEGORY = "linear"  # USDT-perp

# 🔸 Кэш недавних system tp_hit (гейт №1)
_RECENT_TP_HIT: Dict[str, int] = {}   # uid -> ts (epoch sec)
_TP_HIT_TTL_SEC = 10

def _now_ts() -> int:
    return int(time.time())

def _gc_recent_tp() -> None:
    # очистка устаревших записей из кэша tp_hit
    now = _now_ts()
    for k, ts in list(_RECENT_TP_HIT.items()):
        if now - ts > _TP_HIT_TTL_SEC:
            _RECENT_TP_HIT.pop(k, None)


# 🔸 Основной воркер
async def run_trader_sl_handler_loop():
    redis = infra.redis_client

    # создаём CG
    try:
        await redis.xgroup_create(STATUS_STREAM, CG_NAME, id="$", mkstream=True)
        log.info("📡 CG создана: %s → %s", STATUS_STREAM, CG_NAME)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info("ℹ️ CG уже существует: %s", CG_NAME)
        else:
            log.exception("❌ Ошибка создания CG для %s", STATUS_STREAM)
            return

    log.info("🚦 TRADER_SL_HANDLER запущен (слушаем tp_hit и sl_replaced из %s)", STATUS_STREAM)

    # основной цикл чтения
    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=CG_NAME,
                consumername=CONSUMER_NAME,
                streams={STATUS_STREAM: ">"},
                count=50,
                block=1000
            )
            if not entries:
                continue

            for _, records in entries:
                for record_id, raw in records:
                    try:
                        await _handle_status_event(raw)
                    except Exception:
                        log.exception("❌ Ошибка обработки записи sl_handler")
                    finally:
                        # ack в любом случае
                        try:
                            await redis.xack(STATUS_STREAM, CG_NAME, record_id)
                        except Exception:
                            log.exception("❌ Не удалось ACK запись sl_handler")
        except Exception:
            log.exception("❌ Ошибка в цикле TRADER_SL_HANDLER")
            await asyncio.sleep(1.0)


# 🔸 Обработка одного события из positions_bybit_status
async def _handle_status_event(raw: Dict[str, Any]) -> None:
    event = _g(raw, "event")

    # отмечаем system tp_hit в кэше (гейт №1) и выходим
    if event == "tp_hit":
        uid = _g(raw, "position_uid")
        if uid:
            _gc_recent_tp()
            _RECENT_TP_HIT[uid] = _now_ts()
            log.debug("SL_HANDLER: noted system tp_hit (uid=%s)", uid)
        return

    # интересует только sl_replaced (tp_hit уже учтён выше)
    if event != "sl_replaced":
        return

    uid = _g(raw, "position_uid")
    sid = _to_int(_g(raw, "strategy_id"))
    direction = (_g(raw, "direction") or "").lower()

    # условия достаточности: базовые поля
    if not uid or sid is None or direction not in ("long", "short"):
        log.debug("⚠️ SL_HANDLER: пропуск (некорректные поля) uid=%s sid=%s dir=%s", uid, sid, direction)
        return

    # фильтр winners
    if sid not in config.trader_winners:
        log.debug("⏭️ SL_HANDLER: sid=%s не в trader_winner, пропуск uid=%s", sid, uid)
        return

    # гейт №1: недавно был system tp_hit → не двигаем SL на бирже
    _gc_recent_tp()
    if _RECENT_TP_HIT.get(uid):
        log.debug("SL_HANDLER: skip sl_move_to_entry — recent system tp_hit (uid=%s)", uid)
        return

    # позиция у трейдера должна быть «open»
    tp_row = await infra.pg_pool.fetchrow(
        "SELECT symbol, status FROM public.trader_positions WHERE position_uid = $1",
        uid
    )
    if not tp_row:
        log.debug("ℹ️ SL_HANDLER: позиция uid=%s не отслеживается у трейдера", uid)
        return
    if (tp_row["status"] or "").lower() != "open":
        log.debug("ℹ️ SL_HANDLER: позиция uid=%s уже закрыта в портфеле", uid)
        return

    symbol = str(tp_row["symbol"])

    # остаток фактический
    left_qty = await _calc_left_qty(uid)
    if not left_qty or left_qty <= Decimal("0"):
        log.debug("ℹ️ SL_HANDLER: остаток по uid=%s уже 0", uid)
        return

    # гейт №2: если есть активные priced TP на бирже — не двигаем SL (ждём фактического биржевого TP)
    if await _has_active_priced_tp_on_exchange(uid):
        log.debug("SL_HANDLER: skip sl_move_to_entry — active priced TP on exchange (uid=%s)", uid)
        return

    # биржевая «цена входа»
    entry_avg = await _fetch_entry_avg_fill(uid)
    if entry_avg is None or entry_avg <= Decimal("0"):
        log.debug("⚠️ SL_HANDLER: нет avg_fill для uid=%s", uid)
        return

    # текущая lastPrice
    last_price = await _fetch_bybit_last_price(symbol)
    if last_price is None:
        log.debug("⚠️ SL_HANDLER: не получили last price для %s", symbol)
        return

    # «цена лучше входа» по направлению
    better = (last_price > entry_avg) if direction == "long" else (last_price < entry_avg)
    if not better:
        log.debug("ℹ️ SL_HANDLER: last=%s не лучше entry=%s для dir=%s (uid=%s)", _fmt(last_price), _fmt(entry_avg), direction, uid)
        return

    # берём текущий активный SL (для объёма)
    sl_qty = await _fetch_active_sl_qty(uid)
    if sl_qty is None:
        log.debug("ℹ️ SL_HANDLER: активный SL не найден для uid=%s — пропуск", uid)
        return
    if sl_qty <= Decimal("0"):
        log.debug("ℹ️ SL_HANDLER: активный SL имеет нулевой объём (uid=%s)", uid)
        return

    # нормализуем цену entry к ticksize
    ticksize = _to_dec((config.tickers.get(symbol) or {}).get("ticksize"))
    trigger_price = _round_price(entry_avg, ticksize)

    # отправляем команду в maintainer: переставить SL на entry с тем же объёмом
    try:
        await infra.redis_client.xadd(MAINTAINER_STREAM, {
            "type": "sl_move_to_entry",
            "position_uid": uid,
            "strategy_id": str(sid),
            "symbol": symbol,
            "direction": direction,
            "trigger_price": str(trigger_price),
            "qty": str(sl_qty),
            "ts": datetime.utcnow().isoformat(timespec="milliseconds"),
            "dedupe": f"{uid}:sl:to_entry",
        })
        log.info("📤 SL_HANDLER → sl_move_to_entry: uid=%s %s trigger=%s qty=%s", uid, symbol, _fmt(trigger_price), _fmt(sl_qty))
    except Exception:
        log.exception("❌ Не удалось отправить sl_move_to_entry для uid=%s", uid)


# 🔸 Вспомогательные
def _g(d: Dict[str, Any], key: str) -> Optional[str]:
    v = d.get(key) if key in d else d.get(key.encode(), None)
    return v.decode() if isinstance(v, (bytes, bytearray)) else (v if isinstance(v, str) else None)

def _to_dec(v: Any) -> Optional[Decimal]:
    try:
        if v is None:
            return None
        if isinstance(v, Decimal):
            return v
        return Decimal(str(v))
    except Exception:
        return None

def _to_int(s: Optional[str]) -> Optional[int]:
    try:
        return int(s) if s not in (None, "", "None") else None
    except Exception:
        return None

def _fmt(x: Optional[Decimal], max_prec: int = 8) -> str:
    if x is None:
        return "—"
    try:
        s = f"{x:.{max_prec}f}".rstrip("0").rstrip(".")
        return s if s else "0"
    except Exception:
        return str(x)

def _round_price(price: Decimal, ticksize: Optional[Decimal]) -> Decimal:
    if ticksize is None or ticksize <= 0:
        return price
    try:
        return price.quantize(ticksize, rounding=ROUND_HALF_UP)
    except Exception:
        return price

async def _calc_left_qty(uid: str) -> Optional[Decimal]:
    row = await infra.pg_pool.fetchrow(
        """
        WITH e AS (
          SELECT COALESCE(MAX(filled_qty),0) AS fq
          FROM public.trader_position_orders
          WHERE position_uid=$1 AND kind='entry'
        ),
        t AS (
          SELECT COALESCE(SUM(filled_qty),0) AS fq
          FROM public.trader_position_orders
          WHERE position_uid=$1 AND kind='tp'
        ),
        s AS (
          SELECT COALESCE(SUM(filled_qty),0) AS fq
          FROM public.trader_position_orders
          WHERE position_uid=$1 AND kind='sl'
        ),
        c AS (
          SELECT COALESCE(SUM(filled_qty),0) AS fq
          FROM public.trader_position_orders
          WHERE position_uid=$1 AND kind='close'
        )
        SELECT e.fq - t.fq - s.fq - c.fq AS left_qty FROM e,t,s,c
        """,
        uid
    )
    try:
        return Decimal(str(row["left_qty"])) if row and row["left_qty"] is not None else None
    except Exception:
        return None

async def _fetch_entry_avg_fill(uid: str) -> Optional[Decimal]:
    row = await infra.pg_pool.fetchrow(
        """
        SELECT avg_fill_price
        FROM public.trader_position_orders
        WHERE position_uid = $1 AND kind='entry'
        ORDER BY id DESC LIMIT 1
        """,
        uid
    )
    return _to_dec(row["avg_fill_price"]) if row and row["avg_fill_price"] is not None else None

async def _fetch_active_sl_qty(uid: str) -> Optional[Decimal]:
    row = await infra.pg_pool.fetchrow(
        """
        SELECT qty
        FROM public.trader_position_orders
        WHERE position_uid = $1 AND kind='sl'
          AND ext_status IN ('submitted','accepted','partially_filled')
        ORDER BY id DESC LIMIT 1
        """,
        uid
    )
    return _to_dec(row["qty"]) if row and row["qty"] is not None else None

async def _has_active_priced_tp_on_exchange(uid: str) -> bool:
    row = await infra.pg_pool.fetchrow(
        """
        SELECT 1
        FROM public.trader_position_orders
        WHERE position_uid = $1
          AND kind = 'tp'
          AND "type" = 'limit'
          AND ext_status IN ('submitted','accepted','partially_filled')
        LIMIT 1
        """,
        uid
    )
    return bool(row)

async def _fetch_bybit_last_price(symbol: str) -> Optional[Decimal]:
    try:
        url = f"{BYBIT_PUBLIC_BASE}/v5/market/tickers?category={BYBIT_CATEGORY}&symbol={symbol}"
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
        lst = (data.get("result") or {}).get("list") or []
        last = lst[0].get("lastPrice") if lst else None
        return _to_dec(last)
    except Exception:
        return None