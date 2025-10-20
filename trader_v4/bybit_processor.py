# bybit_processor.py — preflight (margin/position/leverage) + план (entry + TP/SL) и запись «плана» в БД
# + submit ордеров на Bybit при TRADER_ORDER_MODE=on (entry → TP → SL) с апдейтом статусов

# 🔸 Импорты
import os
import json
import logging
import asyncio
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import httpx

from trader_infra import infra
from trader_config import config  # leverage из кэша стратегий

# 🔸 Логгер ордеров
log = logging.getLogger("TRADER_ORDERS")

# 🔸 Константы стрима и Consumer Group
ORDER_REQUEST_STREAM = "trader_order_requests"
CG_NAME = "bybit_processor_group"
CONSUMER = "bybit_processor_1"

# 🔸 Режим процессора ордеров (ENV TRADER_ORDER_MODE: off|dry_run|on)
def _normalize_mode(v: Optional[str]) -> str:
    s = (v or "").strip().lower()
    if s in ("off", "false", "0", "no", "disabled"):
        return "off"
    if s in ("dry_run", "dry-run", "dryrun", "test"):
        return "dry_run"
    return "on"

TRADER_ORDER_MODE = _normalize_mode(os.getenv("TRADER_ORDER_MODE"))

# 🔸 Уменьшающий коэффициент размера реального ордера (ENV BYBIT_SIZE_PCT, проценты)
def _get_size_factor() -> Decimal:
    raw = os.getenv("BYBIT_SIZE_PCT", "100").strip()
    try:
        pct = Decimal(raw)
    except Exception:
        pct = Decimal("100")
    if pct < 0:
        pct = Decimal("0")
    if pct > 1000:
        pct = Decimal("1000")
    return (pct / Decimal("100"))

SIZE_FACTOR = _get_size_factor()

# 🔸 Bybit REST (ENV)
API_KEY = os.getenv("BYBIT_API_KEY", "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com")
RECV_WINDOW = os.getenv("BYBIT_RECV_WINDOW", "5000")
CATEGORY = "linear"  # деривативы USDT-perp

# 🔸 Целевые режимы (ENV): маржа и позиционный режим
def _norm_margin_mode(v: Optional[str]) -> str:
    s = (v or "isolated").strip().lower()
    return "isolated" if s == "isolated" else "cross"

def _norm_position_mode(v: Optional[str]) -> str:
    s = (v or "oneway").strip().lower()
    return "hedge" if s == "hedge" else "oneway"

TARGET_MARGIN_MODE = _norm_margin_mode(os.getenv("BYBIT_MARGIN_MODE"))
TARGET_POSITION_MODE = _norm_position_mode(os.getenv("BYBIT_POSITION_MODE"))

# сообщим о режимах в лог
if TRADER_ORDER_MODE == "dry_run":
    log.info(
        "BYBIT processor mode: DRY_RUN (preflight в логах, план в БД; без отправки ордеров). "
        "SIZE_FACTOR=%.4f, margin=%s, position=%s",
        float(SIZE_FACTOR), TARGET_MARGIN_MODE, TARGET_POSITION_MODE
    )
elif TRADER_ORDER_MODE == "off":
    log.info("BYBIT processor mode: OFF (игнорируем заявки).")
else:
    log.info(
        "BYBIT processor mode: ON (preflight + submit ордеров). "
        "SIZE_FACTOR=%.4f, margin=%s, position=%s",
        float(SIZE_FACTOR), TARGET_MARGIN_MODE, TARGET_POSITION_MODE
    )


# 🔸 Основной цикл воркера (последовательная обработка)
async def run_bybit_processor_loop():
    redis = infra.redis_client

    try:
        await redis.xgroup_create(ORDER_REQUEST_STREAM, CG_NAME, id="$", mkstream=True)
        log.debug("📡 Consumer Group создана: %s → %s", ORDER_REQUEST_STREAM, CG_NAME)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug("ℹ️ Consumer Group уже существует: %s", CG_NAME)
        else:
            log.exception("❌ Ошибка создания Consumer Group")
            return

    log.debug("🚦 BYBIT_PROCESSOR запущен (последовательная обработка)")

    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=CG_NAME,
                consumername=CONSUMER,
                streams={ORDER_REQUEST_STREAM: ">"},
                count=1,
                block=1000
            )
            if not entries:
                continue

            for _, records in entries:
                for record_id, data in records:
                    try:
                        await _handle_order_request(record_id, data)
                    except Exception:
                        log.exception("❌ Ошибка обработки заявки (id=%s)", record_id)
                        await redis.xack(ORDER_REQUEST_STREAM, CG_NAME, record_id)
                    else:
                        await redis.xack(ORDER_REQUEST_STREAM, CG_NAME, record_id)

        except Exception:
            log.exception("❌ Ошибка в основном цикле BYBIT_PROCESSOR")
            await asyncio.sleep(2)


# 🔸 Обработка одной заявки из стрима (ожидаем минимум position_uid)
async def _handle_order_request(record_id: str, data: Dict[str, Any]) -> None:
    # режим off: сразу выходим
    if TRADER_ORDER_MODE == "off":
        log.debug("TRADER_ORDER_MODE=off — пропуск заявки id=%s", record_id)
        return

    position_uid = _as_str(data.get("position_uid"))
    sid = _as_int(data.get("strategy_id"))
    if not position_uid:
        log.info("⚠️ Пропуск записи (нет position_uid) id=%s", record_id)
        return

    # тянем позицию из БД
    pos = await _fetch_position(position_uid)
    if not pos:
        log.info("ℹ️ Позиция не найдена в positions_v4, uid=%s", position_uid)
        return

    symbol = _as_str(pos.get("symbol"))
    direction = (_as_str(pos.get("direction")) or "").lower()
    entry_price = _as_decimal(pos.get("entry_price"))
    qty_entry_raw = _as_decimal(pos.get("quantity")) or Decimal("0")
    created_at = pos.get("created_at")

    if not symbol or direction not in ("long", "short") or qty_entry_raw <= 0:
        log.info("⚠️ Недостаточно данных позиции: uid=%s symbol=%s direction=%s qty=%s", position_uid, symbol, direction, qty_entry_raw)
        return

    # правила округлений по тикеру
    rules = await _load_symbol_rules(symbol)
    precision_qty = rules.get("precision_qty")
    min_qty = rules.get("min_qty")
    ticksize = rules.get("ticksize")

    # цели TP/SL
    tp_list, tp_signal_skipped, sl_one = await _fetch_targets_for_plan(position_uid)

    # 🔸 preflight (margin / position-mode / leverage)
    leverage_from_strategy = _get_strategy_leverage(sid)
    preflight_lines = await _preflight_plan_or_apply(
        symbol=symbol,
        leverage=leverage_from_strategy
    )

    # расчёт фактических величин для плана ордеров
    side_word = "BUY" if direction == "long" else "SELL"
    opposite_side = "SELL" if direction == "long" else "BUY"
    qty_entry_real = _round_qty(qty_entry_raw * SIZE_FACTOR, precision_qty)
    entry_link_id = f"{position_uid}-entry"

    # DRY_RUN отчёт по ордерам
    lines: List[str] = []
    lines.extend(preflight_lines)
    lines.append(f"[ORDER_DRY_RUN OPEN] uid={position_uid} symbol={symbol} side={'LONG' if direction=='long' else 'SHORT'}")
    lines.append(f"entry: market {side_word} qty_raw={_fmt(qty_entry_raw)} qty_real={_fmt(qty_entry_real)} linkId={entry_link_id}")

    # запись «плана» entry в БД (если не слишком мало)
    entry_skipped = False
    if min_qty is None or qty_entry_real >= min_qty:
        await _upsert_order(
            position_uid=position_uid,
            kind="entry",
            level=None,
            exchange="BYBIT",
            symbol=symbol,
            side=side_word,
            otype="market",
            tif="GTC",
            reduce_only=False,
            price=None,
            trigger_price=None,
            qty=qty_entry_real,
            order_link_id=entry_link_id,
            ext_status="planned",
            qty_raw=qty_entry_raw,
            price_raw=None,
        )
    else:
        entry_skipped = True
        lines.append("note: entry qty_real < min_qty → SKIP (entry too small)")

    # TP с ценой — лимитные reduce-only
    tp_real_list: List[Tuple[int, Decimal, Decimal, str]] = []  # (level, price_real, qty_tp_real, link_id)
    if tp_list:
        for level, price_raw, qty_tp_raw in tp_list:
            price_real = _round_price(price_raw, ticksize)
            qty_tp_real = _round_qty(qty_tp_raw * SIZE_FACTOR, precision_qty)
            link_id = f"{position_uid}-tp-{level}"
            note = ""
            if min_qty is not None and qty_tp_real < min_qty:
                note = "  # qty_real < min_qty → SKIP"
            lines.append(
                f"tpL{level}: limit reduceOnly price={_fmt(price_real)} "
                f"qty_raw={_fmt(qty_tp_raw)} qty_real={_fmt(qty_tp_real)} linkId={link_id}{note}"
            )
            if not note:
                tp_real_list.append((level, price_real, qty_tp_real, link_id))
                await _upsert_order(
                    position_uid=position_uid,
                    kind="tp",
                    level=level,
                    exchange="BYBIT",
                    symbol=symbol,
                    side=opposite_side,
                    otype="limit",
                    tif="GTC",
                    reduce_only=True,
                    price=price_real,
                    trigger_price=None,
                    qty=qty_tp_real,
                    order_link_id=link_id,
                    ext_status="planned",
                    qty_raw=qty_tp_raw,
                    price_raw=price_raw,
                )
    else:
        lines.append("tp: —  # no percent/atr TP with price")

    # SL — стоп-маркет reduce-only на весь реальный объём
    sl_trigger = None
    sl_link_id = f"{position_uid}-sl"
    if sl_one and sl_one[0] is not None:
        sl_trigger_raw = sl_one[0]
        sl_trigger = _round_price(sl_trigger_raw, ticksize)
        lines.append(
            f"sl: stop-market reduceOnly trigger={_fmt(sl_trigger)} qty={_fmt(qty_entry_real)} linkId={sl_link_id}"
        )
        if not entry_skipped:
            await _upsert_order(
                position_uid=position_uid,
                kind="sl",
                level=None,
                exchange="BYBIT",
                symbol=symbol,
                side=opposite_side,
                otype="stop_market",
                tif="GTC",
                reduce_only=True,
                price=None,
                trigger_price=sl_trigger,
                qty=qty_entry_real,
                order_link_id=sl_link_id,
                ext_status="planned",
                qty_raw=qty_entry_real,   # SL на весь входной объём
                price_raw=None,
            )
    else:
        lines.append("sl: —  # WARN: no SL price")

    # вывод плана в лог
    log.info("\n" + "\n".join(lines))

    # 🔸 Сабмит ордеров в режиме ON
    if TRADER_ORDER_MODE == "on" and not entry_skipped and (API_KEY and API_SECRET):
        # 1) Entry (Market)
        ok_e, oid_e, rc_e, rm_e = await _submit_entry(symbol=symbol, side=side_word, qty=qty_entry_real, link_id=entry_link_id)
        await _mark_order_after_submit(order_link_id=entry_link_id, ok=ok_e, order_id=oid_e, retcode=rc_e, retmsg=rm_e)
        # витрина по позиции
        await _mirror_entry_to_trader_positions(
            position_uid=position_uid,
            order_link_id=entry_link_id,
            order_id=oid_e,
            ext_status=("submitted" if ok_e else "rejected")
        )

        # небольшая пауза, чтобы на бирже появилась позиция перед TP/SL
        await asyncio.sleep(0.25)

        # 2) Все TP (Limit reduce-only) — с лёгким ретраем
        for level, price_real, qty_tp_real, link_id in tp_real_list:
            ok_t, oid_t, rc_t, rm_t = await _submit_tp(symbol=symbol, side=opposite_side, price=price_real, qty=qty_tp_real, link_id=link_id)
            if not ok_t:
                # ещё одна попытка через короткую паузу
                await asyncio.sleep(0.35)
                ok_t, oid_t, rc_t, rm_t = await _submit_tp(symbol=symbol, side=opposite_side, price=price_real, qty=qty_tp_real, link_id=link_id)
            await _mark_order_after_submit(order_link_id=link_id, ok=ok_t, order_id=oid_t, retcode=rc_t, retmsg=rm_t)

        # 3) SL (stop-market reduce-only), если есть
        if sl_trigger is not None:
            ok_s, oid_s, rc_s, rm_s = await _submit_sl(symbol=symbol, side=opposite_side, trigger_price=sl_trigger, qty=qty_entry_real, link_id=sl_link_id)
            if not ok_s:
                await asyncio.sleep(0.35)
                ok_s, oid_s, rc_s, rm_s = await _submit_sl(symbol=symbol, side=opposite_side, trigger_price=sl_trigger, qty=qty_entry_real, link_id=sl_link_id)
            await _mark_order_after_submit(order_link_id=sl_link_id, ok=ok_s, order_id=oid_s, retcode=rc_s, retmsg=rm_s)


# 🔸 Preflight (план/применение)
async def _preflight_plan_or_apply(*, symbol: str, leverage: Optional[Decimal]) -> List[str]:
    lev_str = _lev_to_str(leverage)
    desired_margin = TARGET_MARGIN_MODE   # 'isolated' | 'cross'
    desired_posmode = TARGET_POSITION_MODE  # 'oneway' | 'hedge'

    lines: List[str] = []
    lines.append(f"[PREFLIGHT] symbol={symbol} target: margin={desired_margin}, position={desired_posmode}, leverage={lev_str}")

    # dry_run — только лог
    if TRADER_ORDER_MODE != "on":
        lines.append("[PREFLIGHT] DRY_RUN: no REST calls, just planning")
        return lines

    if not API_KEY or not API_SECRET:
        lines.append("[PREFLIGHT] SKIP: no API keys configured")
        return lines

    try:
        # позиционный режим (0 = oneway, 3 = hedge)
        mode_code = 0 if desired_posmode == "oneway" else 3
        resp_mode = await _bybit_post("/v5/position/switch-mode", {"category": CATEGORY, "symbol": symbol, "mode": mode_code})
        lines.append(f"[PREFLIGHT] switch-mode → retCode={resp_mode.get('retCode')} retMsg={resp_mode.get('retMsg')}")

        # маржинальный режим (1=isolated, 0=cross) + buy/sell leverage
        trade_mode = 1 if desired_margin == "isolated" else 0
        resp_iso = await _bybit_post(
            "/v5/position/switch-isolated",
            {"category": CATEGORY, "symbol": symbol, "tradeMode": trade_mode, "buyLeverage": lev_str, "sellLeverage": lev_str}
        )
        lines.append(f"[PREFLIGHT] switch-isolated → retCode={resp_iso.get('retCode')} retMsg={resp_iso.get('retMsg')}")

        # явная установка leverage (на всякий случай отдельно)
        resp_lev = await _bybit_post(
            "/v5/position/set-leverage",
            {"category": CATEGORY, "symbol": symbol, "buyLeverage": lev_str, "sellLeverage": lev_str}
        )
        lines.append(f"[PREFLIGHT] set-leverage → retCode={resp_lev.get('retCode')} retMsg={resp_lev.get('retMsg')}")
    except Exception as e:
        lines.append(f"[PREFLIGHT] ERROR: {e}")

    return lines


# 🔸 Сабмит: entry / TP / SL
async def _submit_entry(*, symbol: str, side: str, qty: Decimal, link_id: str) -> Tuple[bool, Optional[str], Optional[int], Optional[str]]:
    body = {
        "category": CATEGORY,
        "symbol": symbol,
        "side": side,
        "orderType": "Market",
        "qty": _str_qty(qty),
        "timeInForce": "GTC",
        "reduceOnly": False,
        "orderLinkId": link_id,
    }
    resp = await _bybit_post("/v5/order/create", body)
    rc, rm = resp.get("retCode"), resp.get("retMsg")
    oid = _extract_order_id(resp)
    ok = (rc == 0)
    log.info("submit entry: %s %s qty=%s linkId=%s → rc=%s msg=%s oid=%s", side, symbol, _str_qty(qty), link_id, rc, rm, oid)
    return ok, oid, rc, rm

async def _submit_tp(*, symbol: str, side: str, price: Decimal, qty: Decimal, link_id: str) -> Tuple[bool, Optional[str], Optional[int], Optional[str]]:
    body = {
        "category": CATEGORY,
        "symbol": symbol,
        "side": side,
        "orderType": "Limit",
        "price": _str_price(price),
        "qty": _str_qty(qty),
        "timeInForce": "GTC",
        "reduceOnly": True,
        "orderLinkId": link_id,
    }
    resp = await _bybit_post("/v5/order/create", body)
    rc, rm = resp.get("retCode"), resp.get("retMsg")
    oid = _extract_order_id(resp)
    ok = (rc == 0)
    log.info("submit tp: %s %s price=%s qty=%s linkId=%s → rc=%s msg=%s oid=%s", side, symbol, _str_price(price), _str_qty(qty), link_id, rc, rm, oid)
    return ok, oid, rc, rm

async def _submit_sl(*, symbol: str, side: str, trigger_price: Decimal, qty: Decimal, link_id: str) -> Tuple[bool, Optional[str], Optional[int], Optional[str]]:
    body = {
        "category": CATEGORY,
        "symbol": symbol,
        "side": side,
        "orderType": "Market",
        "qty": _str_qty(qty),
        "reduceOnly": True,
        "triggerPrice": _str_price(trigger_price),
        "timeInForce": "GTC",
        "orderLinkId": link_id,
    }
    resp = await _bybit_post("/v5/order/create", body)
    rc, rm = resp.get("retCode"), resp.get("retMsg")
    oid = _extract_order_id(resp)
    ok = (rc == 0)
    log.info("submit sl: %s %s trigger=%s qty=%s linkId=%s → rc=%s msg=%s oid=%s", side, symbol, _str_price(trigger_price), _str_qty(qty), link_id, rc, rm, oid)
    return ok, oid, rc, rm


# 🔸 Post-submit апдейты в БД
async def _mark_order_after_submit(*, order_link_id: str, ok: bool, order_id: Optional[str], retcode: Optional[int], retmsg: Optional[str]) -> None:
    now = datetime.utcnow()
    status = "submitted" if ok else "rejected"
    await infra.pg_pool.execute(
        """
        UPDATE public.trader_position_orders
        SET
            order_id = COALESCE($2, order_id),
            ext_status = $3,
            last_ext_event_at = $4,
            error_last = CASE WHEN $1 THEN NULL ELSE $5 END
        WHERE order_link_id = $6
        """,
        ok, order_id, status, now, (f"retCode={retcode} retMsg={retmsg}" if not ok else None), order_link_id
    )

async def _mirror_entry_to_trader_positions(*, position_uid: str, order_link_id: str, order_id: Optional[str], ext_status: str) -> None:
    now = datetime.utcnow()
    await infra.pg_pool.execute(
        """
        UPDATE public.trader_positions
        SET
            exchange = COALESCE(exchange, 'BYBIT'),
            order_link_id = COALESCE(order_link_id, $2),
            order_id = COALESCE(order_id, $3),
            ext_status = $4,
            last_ext_event_at = $5
        WHERE position_uid = $1
        """,
        position_uid, order_link_id, order_id, ext_status, now
    )


# 🔸 Вспомогательные функции извлечения и округлений
def _as_str(v: Any) -> str:
    if v is None:
        return ""
    return v.decode() if isinstance(v, (bytes, bytearray)) else str(v)

def _as_int(v: Any) -> Optional[int]:
    try:
        s = _as_str(v)
        return int(s) if s != "" else None
    except Exception:
        return None

def _as_decimal(v: Any) -> Optional[Decimal]:
    try:
        if v is None:
            return None
        if isinstance(v, Decimal):
            return v
        return Decimal(str(v))
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

def _round_qty(qty: Decimal, precision_qty: Optional[int]) -> Decimal:
    if qty is None:
        return Decimal("0")
    if precision_qty is None:
        return qty
    step = Decimal("1").scaleb(-int(precision_qty))
    try:
        return qty.quantize(step, rounding=ROUND_DOWN)
    except Exception:
        return qty

def _round_price(price: Optional[Decimal], ticksize: Optional[Decimal]) -> Optional[Decimal]:
    if price is None or ticksize is None:
        return price
    try:
        quantum = _as_decimal(ticksize) or Decimal("0")
        if quantum <= 0:
            return price
        return price.quantize(quantum, rounding=ROUND_HALF_UP)
    except Exception:
        return price

def _lev_to_str(lev: Optional[Decimal]) -> str:
    try:
        if lev is None:
            return "1"
        return str(int(lev))
    except Exception:
        return "1"

def _str_qty(q: Decimal) -> str:
    return _fmt(q)

def _str_price(p: Decimal) -> str:
    return _fmt(p)

def _get_strategy_leverage(strategy_id: Optional[int]) -> Optional[Decimal]:
    if strategy_id is None:
        return None
    meta = config.strategy_meta.get(strategy_id) or {}
    lev = meta.get("leverage")
    try:
        if lev is None:
            return None
        return lev if isinstance(lev, Decimal) else Decimal(str(lev))
    except Exception:
        return None


# 🔸 Доступ к БД: чтение
async def _fetch_position(position_uid: str) -> Optional[Dict[str, Any]]:
    row = await infra.pg_pool.fetchrow(
        """
        SELECT symbol, direction, entry_price, quantity, notional_value, created_at
        FROM public.positions_v4
        WHERE position_uid = $1
        """,
        position_uid
    )
    return dict(row) if row else None

async def _fetch_targets_for_plan(position_uid: str) -> Tuple[List[Tuple[int, Decimal, Decimal]], int, Optional[Tuple[Optional[Decimal]]]]:
    # tp с ценой (source='price'), живые (не hit и не canceled)
    tp_rows = await infra.pg_pool.fetch(
        """
        SELECT level, price, quantity
        FROM public.position_targets_v4
        WHERE position_uid = $1
          AND type = 'tp'
          AND price IS NOT NULL
          AND (canceled IS NOT TRUE)
          AND (hit IS NOT TRUE)
        ORDER BY level
        """,
        position_uid
    )
    tps: List[Tuple[int, Decimal, Decimal]] = []
    for r in tp_rows:
        lvl = int(r["level"])
        price = _as_decimal(r["price"]) or Decimal("0")
        qty = _as_decimal(r["quantity"]) or Decimal("0")
        tps.append((lvl, price, qty))

    # посчитаем TP-signal для заметки (без цены ИЛИ source='signal')
    tp_sig_cnt_row = await infra.pg_pool.fetchrow(
        """
        SELECT COUNT(*) AS cnt
        FROM public.position_targets_v4
        WHERE position_uid = $1
          AND type = 'tp'
          AND (
                price IS NULL
                OR source = 'signal'
              )
        """,
        position_uid
    )
    tp_signal_skipped = int(tp_sig_cnt_row["cnt"]) if tp_sig_cnt_row and tp_sig_cnt_row["cnt"] is not None else 0

    # sl: возьмём первую «живую» c ценой
    sl_row = await infra.pg_pool.fetchrow(
        """
        SELECT price
        FROM public.position_targets_v4
        WHERE position_uid = $1
          AND type = 'sl'
          AND price IS NOT NULL
          AND (canceled IS NOT TRUE)
          AND (hit IS NOT TRUE)
        ORDER BY level
        LIMIT 1
        """,
        position_uid
    )
    sl_one = ( _as_decimal(sl_row["price"]), ) if sl_row and sl_row["price"] is not None else ( None, )

    return tps, tp_signal_skipped, sl_one

async def _load_symbol_rules(symbol: str) -> Dict[str, Optional[Decimal]]:
    row = await infra.pg_pool.fetchrow(
        """
        SELECT precision_qty, min_qty, ticksize
        FROM public.tickers_bb
        WHERE symbol = $1
        """,
        symbol
    )
    if not row:
        log.info("ℹ️ Не нашли правила тикера в tickers_bb: %s", symbol)
        return {"precision_qty": None, "min_qty": None, "ticksize": None}
    precision_qty = row["precision_qty"]
    min_qty = _as_decimal(row["min_qty"]) if row["min_qty"] is not None else None
    ticksize = _as_decimal(row["ticksize"]) if row["ticksize"] is not None else None
    return {"precision_qty": precision_qty, "min_qty": min_qty, "ticksize": ticksize}


# 🔸 Доступ к БД: запись «плана» ордера (UPSERT по order_link_id)
async def _upsert_order(
    *,
    position_uid: str,
    kind: str,                       # 'entry' | 'tp' | 'sl' | 'close'
    level: Optional[int],
    exchange: str,                   # 'BYBIT'
    symbol: str,
    side: Optional[str],             # 'BUY' | 'SELL' | None
    otype: Optional[str],            # 'market' | 'limit' | 'stop_market' | 'stop_limit' | None
    tif: str,                        # 'GTC'|'IOC'|'FOK'
    reduce_only: bool,
    price: Optional[Decimal],        # для limit
    trigger_price: Optional[Decimal],# для stop-*
    qty: Decimal,                    # НЕ NULL
    order_link_id: str,              # UNIQUE
    ext_status: str,                 # 'planned'
    qty_raw: Optional[Decimal],
    price_raw: Optional[Decimal],
) -> None:
    side_norm = None if side is None else side.upper()
    otype_norm = None if otype is None else otype.lower()

    await infra.pg_pool.execute(
        """
        INSERT INTO public.trader_position_orders (
            position_uid, kind, level, exchange, symbol, side, "type", tif, reduce_only,
            price, trigger_price, qty, order_link_id, ext_status,
            qty_raw, price_raw
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,
                $10,$11,$12,$13,$14,
                $15,$16)
        ON CONFLICT (order_link_id) DO UPDATE SET
            position_uid = EXCLUDED.position_uid,
            kind         = EXCLUDED.kind,
            level        = EXCLUDED.level,
            exchange     = EXCLUDED.exchange,
            symbol       = EXCLUDED.symbol,
            side         = EXCLUDED.side,
            "type"       = EXCLUDED."type",
            tif          = EXCLUDED.tif,
            reduce_only  = EXCLUDED.reduce_only,
            price        = EXCLUDED.price,
            trigger_price= EXCLUDED.trigger_price,
            qty          = EXCLUDED.qty,
            ext_status   = 'planned',
            qty_raw      = EXCLUDED.qty_raw,
            price_raw    = EXCLUDED.price_raw,
            error_last   = NULL
        """,
        position_uid, kind, level, exchange, symbol, side_norm, otype_norm, tif, reduce_only,
        price, trigger_price, qty, order_link_id, ext_status,
        qty_raw, price_raw
    )


# 🔸 Bybit REST: подпись и вызовы
def _rest_sign(ts_ms: int, query_or_body: str) -> str:
    import hmac, hashlib
    payload = f"{ts_ms}{API_KEY}{RECV_WINDOW}{query_or_body}"
    return hmac.new(API_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()

async def _bybit_post(path: str, body: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{BASE_URL}{path}"
    ts = _now_ms()
    body_str = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
    sign = _rest_sign(ts, body_str)
    headers = {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-TIMESTAMP": str(ts),
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
        "X-BAPI-SIGN": sign,
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(url, headers=headers, content=body_str.encode("utf-8"))
        try:
            r.raise_for_status()
        except Exception:
            log.warning("⚠️ Bybit POST %s %s: %s", path, r.status_code, r.text)
        try:
            return r.json()
        except Exception:
            return {"retCode": None, "retMsg": "non-json response", "raw": r.text}

def _extract_order_id(resp: Dict[str, Any]) -> Optional[str]:
    try:
        res = resp.get("result") or {}
        return _as_str(res.get("orderId")) or None
    except Exception:
        return None

def _now_ms() -> int:
    import time
    return int(time.time() * 1000)