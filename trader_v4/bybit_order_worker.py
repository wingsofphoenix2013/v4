# bybit_order_worker.py — фоновый воркер: читает intents из стрима и отправляет на Bybit (entry + TP + SL), фиксирует в БД

# 🔸 Импорты
import asyncio
import json
import logging
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from trader_infra import infra
from trader_config import config
from bybit_http_client import BybitHttpClient, BybitAPIError
from bybit_order_router import route_entry_with_tpsl

# 🔸 Логгер
log = logging.getLogger("BYBIT_ORDER")

# 🔸 Константы стрима/CG
ORDER_INTENTS_STREAM = "bybit_order_intents"
CG_NAME = "bybit_order_group"
CONSUMER = "bybit_order_1"

# 🔸 Тайминги
XREAD_BLOCK_MS = 1000
RETRY_SLEEP_SEC = 2.0


# 🔸 Основной цикл воркера
async def run_bybit_order_worker_loop():
    redis = infra.redis_client

    # создаём Consumer Group (идём по новым сообщениям)
    try:
        await redis.xgroup_create(ORDER_INTENTS_STREAM, CG_NAME, id="$", mkstream=True)
        log.info("BYBIT_ORDER: Consumer Group создана: %s → %s", ORDER_INTENTS_STREAM, CG_NAME)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info("BYBIT_ORDER: Consumer Group уже существует: %s", CG_NAME)
        else:
            log.exception("BYBIT_ORDER: ошибка создания Consumer Group")
            return

    # единый HTTP-клиент Bybit на весь цикл
    client = BybitHttpClient()

    log.info("BYBIT_ORDER: запущен (последовательная обработка)")
    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=CG_NAME,
                consumername=CONSUMER,
                streams={ORDER_INTENTS_STREAM: ">"},
                count=1,
                block=XREAD_BLOCK_MS,
            )
            if not entries:
                continue

            for _, records in entries:
                for record_id, data in records:
                    try:
                        await _handle_intent(record_id, data, client)
                    except Exception:
                        log.exception("BYBIT_ORDER: ошибка обработки intent id=%s", record_id)
                        await redis.xack(ORDER_INTENTS_STREAM, CG_NAME, record_id)
                        await asyncio.sleep(RETRY_SLEEP_SEC)
                    else:
                        await redis.xack(ORDER_INTENTS_STREAM, CG_NAME, record_id)

        except Exception:
            log.exception("BYBIT_ORDER: сбой основного цикла")
            await asyncio.sleep(RETRY_SLEEP_SEC)


# 🔸 Обработка одного intent из стрима
async def _handle_intent(record_id: str, data: Dict[str, Any], client: BybitHttpClient) -> None:
    action = _as_str(data.get("action") or data.get(b"action"))
    if action == "":
        log.info("BYBIT_ORDER: пропуск пустого action (id=%s)", record_id)
        return

    # базовая маршрутизация по action
    if action == "create_entry_tpsl":
        await _handle_create_entry_tpsl(data, client)
        return

    if action == "amend_sl":
        await _handle_amend_sl(data, client)
        return

    if action == "close_market":
        await _handle_close_market(data, client)
        return

    if action.startswith("cancel_"):
        await _handle_cancel_generic(data, client)
        return

    log.info("BYBIT_ORDER: неизвестный action=%s (id=%s) — пропуск", action, record_id)


# 🔸 Обработка: вход + TP + SL (единый intent)
async def _handle_create_entry_tpsl(data: Dict[str, Any], client: BybitHttpClient) -> None:
    # чтение обязательных полей
    position_uid = _as_str(data.get("position_uid"))
    strategy_id = _as_int(data.get("strategy_id"))
    symbol = _as_str(data.get("symbol"))
    direction = _as_str(data.get("direction"))
    qty_internal = _as_decimal(data.get("qty_internal"))

    if not position_uid or not strategy_id or not symbol or not direction or qty_internal is None:
        log.info("BYBIT_ORDER: пропуск (неполные поля) uid=%s sid=%s %s %s qi=%s",
                 position_uid, strategy_id, symbol, direction, qty_internal)
        return

    # TP/SL цели — JSON-строки в стриме
    tp_targets = _json_list(data.get("tp_targets"))
    sl_targets = _json_list(data.get("sl_targets"))

    # плечо (опционально)
    leverage = _as_decimal(data.get("leverage"))
    ensure_leverage = _as_bool(data.get("ensure_leverage"))

    # проверка live-флага и допуска стратегии (из кэша)
    if not config.is_live_trading():
        log.info("BYBIT_ORDER: live_trading=FALSE — пропуск uid=%s", position_uid)
        return
    if strategy_id not in config.trader_winners or strategy_id not in config.trader_bybit_enabled:
        log.info("BYBIT_ORDER: стратегия не допущена (winner&bybit) uid=%s sid=%s", position_uid, strategy_id)
        return

    # отправляем на Bybit (entry + tp + sl)
    try:
        res = await route_entry_with_tpsl(
            client=client,
            position_uid=position_uid,
            strategy_id=strategy_id,
            symbol=symbol,
            direction=direction,
            qty_internal=qty_internal,
            tp_targets=tp_targets,
            sl_targets=sl_targets,
            leverage=leverage,
            ensure_leverage=bool(ensure_leverage),
        )
    except BybitAPIError as e:
        log.info("BYBIT_ORDER: ошибка create route (uid=%s sym=%s): %s", position_uid, symbol, e)
        await _db_set_error_for_position(position_uid, f"create route error: {e}")
        return
    except Exception as e:
        log.exception("BYBIT_ORDER: неожиданный сбой при создании заявки (uid=%s)", position_uid)
        await _db_set_error_for_position(position_uid, f"unexpected: {e}")
        return

    # отражаем «вход» в БД
    try:
        entry = res.get("entry") or {}
        entry_order_id = _as_str(entry.get("order_id"))
        await _db_update_entry_after_create(
            position_uid=position_uid,
            exchange="BYBIT",
            order_link_id=position_uid,
            order_id=entry_order_id,
            ext_status="submitted",
        )
    except Exception:
        log.exception("BYBIT_ORDER: БД-обновление entry не удалось uid=%s", position_uid)

    # отражаем TP «дети» в БД
    try:
        tp_list: List[Dict[str, Any]] = res.get("tp") or []
        side_close = _side_close_from_dir(direction)
        for tp in tp_list:
            level = tp.get("level")
            tp_link = _as_str(tp.get("order_link_id"))
            tp_order_id = _as_str(tp.get("order_id"))
            # цена/qty из исходной цели (масштаб уже применён в router — на бирже), но для аудита положим исходные
            src = _find_tp_source(tp_targets, int(level) if level is not None else None)
            price_src = _as_decimal((src or {}).get("price"))
            qty_src = _as_decimal((src or {}).get("quantity"))
            await _db_upsert_child_order(
                position_uid=position_uid,
                kind="tp",
                level=int(level) if level is not None else None,
                exchange="BYBIT",
                symbol=symbol,
                side=side_close,
                type_="Limit",
                tif="GTC",
                reduce_only=True,
                price=price_src,
                qty=qty_src,
                order_link_id=tp_link,
                order_id=tp_order_id,
                ext_status="submitted",
            )
    except Exception:
        log.exception("BYBIT_ORDER: БД-обновление TP не удалось uid=%s", position_uid)

    # SL (trading-stop) — как событие; опционально фиксируем «виртуальный» слейв-запрос
    try:
        sl_price = _first_price(sl_targets)
        if sl_price is not None:
            await _db_upsert_child_order(
                position_uid=position_uid,
                kind="sl",
                level=None,
                exchange="BYBIT",
                symbol=symbol,
                side=None,
                type_="Stop",
                tif="GTC",
                reduce_only=True,
                price=sl_price,
                qty=None,
                order_link_id=f"{position_uid}:sl",
                order_id=None,
                ext_status="submitted",
            )
    except Exception:
        log.exception("BYBIT_ORDER: БД-отражение SL(trading-stop) не удалось uid=%s", position_uid)

    log.info("BYBIT_ORDER: создано entry+TP+SL для uid=%s sym=%s", position_uid, symbol)


# 🔸 Обработка: amend SL (position-level trading stop)
async def _handle_amend_sl(data: Dict[str, Any], client: BybitHttpClient) -> None:
    position_uid = _as_str(data.get("position_uid"))
    symbol = _as_str(data.get("symbol"))
    new_sl_price = _as_decimal(data.get("new_sl_price"))

    if not position_uid or not symbol or new_sl_price is None:
        log.info("BYBIT_ORDER: amend_sl пропуск (поля) uid=%s sym=%s", position_uid, symbol)
        return

    if not config.is_live_trading():
        log.info("BYBIT_ORDER: live_trading=FALSE — amend_sl пропуск uid=%s", position_uid)
        return

    try:
        await client.set_trading_stop(category="linear", symbol=symbol, stop_loss=str(new_sl_price))
        await _db_update_child_sl_price(position_uid, new_sl_price)
        log.info("BYBIT_ORDER: amend_sl OK uid=%s new=%s", position_uid, new_sl_price)
    except Exception as e:
        log.exception("BYBIT_ORDER: amend_sl FAIL uid=%s err=%s", position_uid, e)
        await _db_set_error_for_position(position_uid, f"amend_sl: {e}")


# 🔸 Обработка: close remainder (reduceOnly Market)
async def _handle_close_market(data: Dict[str, Any], client: BybitHttpClient) -> None:
    position_uid = _as_str(data.get("position_uid"))
    symbol = _as_str(data.get("symbol"))
    direction = _as_str(data.get("direction"))
    qty_internal = _as_decimal(data.get("qty_internal"))

    if not position_uid or not symbol or not direction or qty_internal is None:
        log.info("BYBIT_ORDER: close_market пропуск (поля) uid=%s", position_uid)
        return

    if not config.is_live_trading():
        log.info("BYBIT_ORDER: live_trading=FALSE — close_market пропуск uid=%s", position_uid)
        return

    # масштабируем qty так же, как для входа
    tick = config.tickers.get(symbol)
    if not tick:
        log.info("BYBIT_ORDER: close_market нет меты тикера sym=%s", symbol)
        return

    R = _ratio_from_env()
    qty_ex = _scale_qty_with_tick(qty_internal, R, tick)
    if qty_ex is None:
        log.info("BYBIT_ORDER: close_market qty слишком мал uid=%s", position_uid)
        return

    side_close = _side_close_from_dir(direction)
    link_id = f"{position_uid}:close"

    try:
        await client.create_order(
            category="linear",
            symbol=symbol,
            side=side_close,
            order_type="Market",
            qty=str(qty_ex),
            reduce_only=True,
            order_link_id=link_id,
        )
        await _db_upsert_child_order(
            position_uid=position_uid,
            kind="close",
            level=None,
            exchange="BYBIT",
            symbol=symbol,
            side=side_close,
            type_="Market",
            tif="GTC",
            reduce_only=True,
            price=None,
            qty=qty_ex,
            order_link_id=link_id,
            order_id=None,
            ext_status="submitted",
            )
        log.info("BYBIT_ORDER: close_market отправлен uid=%s", position_uid)
    except Exception as e:
        log.exception("BYBIT_ORDER: close_market FAIL uid=%s err=%s", position_uid, e)
        await _db_set_error_for_position(position_uid, f"close_market: {e}")


# 🔸 Обработка: отмены (универсальная заглушка)
async def _handle_cancel_generic(data: Dict[str, Any], client: BybitHttpClient) -> None:
    symbol = _as_str(data.get("symbol"))
    order_link_id = _as_str(data.get("order_link_id"))
    if not symbol or not order_link_id:
        log.info("BYBIT_ORDER: cancel_* пропуск (поля)")
        return
    try:
        await client.cancel_order(category="linear", symbol=symbol, order_link_id=order_link_id)
        await _db_update_child_status_by_link(order_link_id, "canceled")
        log.info("BYBIT_ORDER: cancel OK link=%s", order_link_id)
    except Exception as e:
        log.exception("BYBIT_ORDER: cancel FAIL link=%s err=%s", order_link_id, e)


# 🔸 DB-helpers

async def _db_update_entry_after_create(*, position_uid: str, exchange: str, order_link_id: str, order_id: Optional[str], ext_status: str):
    # апдейт «шапки» позиции — внешний входной ордер
    await infra.pg_pool.execute(
        """
        UPDATE public.trader_positions
        SET exchange=$2,
            order_link_id=$3,
            order_id=$4,
            ext_status=$5,
            last_ext_event_at=(now() at time zone 'UTC')
        WHERE position_uid=$1
        """,
        position_uid, exchange, order_link_id, order_id, ext_status
    )

async def _db_upsert_child_order(
    *,
    position_uid: str,
    kind: str,
    level: Optional[int],
    exchange: str,
    symbol: str,
    side: Optional[str],
    type_: str,
    tif: str,
    reduce_only: bool,
    price: Optional[Decimal],
    qty: Optional[Decimal],
    order_link_id: str,
    order_id: Optional[str],
    ext_status: str,
):
    await infra.pg_pool.execute(
        """
        INSERT INTO public.trader_position_orders
          (position_uid, kind, level, exchange, symbol, side, type, tif, reduce_only,
           price, qty, order_link_id, order_id, ext_status, created_at, last_ext_event_at)
        VALUES
          ($1,$2,$3,$4,$5,$6,$7,$8,$9,
           $10,$11,$12,$13,$14,(now() at time zone 'UTC'),(now() at time zone 'UTC'))
        ON CONFLICT (order_link_id) DO UPDATE
          SET order_id = EXCLUDED.order_id,
              ext_status = EXCLUDED.ext_status,
              last_ext_event_at = (now() at time zone 'UTC')
        """,
        position_uid, kind, level, exchange, symbol, side, type_, tif, reduce_only,
        str(price) if price is not None else None,
        str(qty) if qty is not None else None,
        order_link_id, order_id, ext_status
    )

async def _db_set_error_for_position(position_uid: str, msg: str):
    await infra.pg_pool.execute(
        """
        UPDATE public.trader_positions
        SET error_last = $2, last_ext_event_at=(now() at time zone 'UTC')
        WHERE position_uid = $1
        """,
        position_uid, msg[:500]
    )

async def _db_update_child_sl_price(position_uid: str, new_sl_price: Decimal):
    await infra.pg_pool.execute(
        """
        UPDATE public.trader_position_orders
        SET price = $2, last_ext_event_at=(now() at time zone 'UTC')
        WHERE position_uid = $1 AND kind='sl'
        """,
        position_uid, str(new_sl_price)
    )

async def _db_update_child_status_by_link(order_link_id: str, new_status: str):
    await infra.pg_pool.execute(
        """
        UPDATE public.trader_position_orders
        SET ext_status = $2, last_ext_event_at=(now() at time zone 'UTC')
        WHERE order_link_id = $1
        """,
        order_link_id, new_status
    )


# 🔸 Утилиты парсинга/округления

def _as_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (bytes, bytearray)):
        try:
            return v.decode()
        except Exception:
            return str(v)
    return str(v)

def _as_int(v: Any) -> Optional[int]:
    try:
        s = _as_str(v)
        return int(s) if s != "" else None
    except Exception:
        return None

def _as_bool(v: Any) -> bool:
    s = _as_str(v).lower()
    return s in ("1", "true", "yes", "y", "on")

def _as_decimal(v: Any) -> Optional[Decimal]:
    try:
        if v is None:
            return None
        if isinstance(v, Decimal):
            return v
        return Decimal(str(v))
    except Exception:
        return None

def _json_list(v: Any) -> List[Dict[str, Any]]:
    try:
        s = _as_str(v)
        if not s:
            return []
        x = json.loads(s)
        return x if isinstance(x, list) else []
    except Exception:
        return []

def _first_price(items: List[Dict[str, Any]]) -> Optional[Decimal]:
    for it in items or []:
        p = _as_decimal(it.get("price"))
        if p is not None:
            return p
    return None

def _ratio_from_env() -> Decimal:
    # BYBIT_SIZE_PCT читается в router; здесь используем для close_market
    from os import getenv
    try:
        pct = Decimal(getenv("BYBIT_SIZE_PCT", "10"))
        if pct <= 0:
            return Decimal("0")
        if pct > 100:
            pct = Decimal("100")
        return pct / Decimal("100")
    except Exception:
        return Decimal("0.10")

def _scale_qty_with_tick(qty_internal: Decimal, R: Decimal, tick_row: Dict[str, Any]) -> Optional[Decimal]:
    from decimal import ROUND_DOWN
    q = qty_internal * R
    prec_q = tick_row.get("precision_qty")
    min_qty = _as_decimal(tick_row.get("min_qty")) or Decimal("0")
    if prec_q is not None:
        q = q.quantize(Decimal(10) ** -int(prec_q), rounding=ROUND_DOWN)
    if q <= 0:
        return None
    if min_qty > 0 and q < min_qty:
        q = min_qty
    return q

def _side_close_from_dir(direction: str) -> str:
    return "Sell" if (direction or "").lower() == "long" else "Buy"

def _find_tp_source(tp_list: List[Dict[str, Any]], level: Optional[int]) -> Optional[Dict[str, Any]]:
    if level is None:
        return None
    for t in tp_list or []:
        try:
            if int(t.get("level")) == int(level):
                return t
        except Exception:
            continue
    return None