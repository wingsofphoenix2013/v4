# bybit_maintainer.py — сопровождение ордеров на бирже под приоритет внутренней системы:
# TP1(force) → SL на entry; SL-replace/protect; close/protect; фон-вотчер «биржа раньше системы»

# 🔸 Импорты
import os
import json
import logging
import asyncio
import contextlib
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import httpx

from trader_infra import infra

# 🔸 Логгер сопровождающего воркера
log = logging.getLogger("BYBIT_MAINTAINER")

# 🔸 Константы потоков и режимов
POS_UPD_STREAM = "positions_update_stream"
CG_NAME = "bybit_maintainer_group"
CONSUMER = "bybit_maintainer_1"

# 🔸 Режим работы (off|dry_run|on) — читаем как у процессора
def _normalize_mode(v: Optional[str]) -> str:
    s = (v or "").strip().lower()
    if s in ("off", "false", "0", "no", "disabled"):
        return "off"
    if s in ("dry_run", "dry-run", "dryrun", "test"):
        return "dry_run"
    return "on"

TRADER_ORDER_MODE = _normalize_mode(os.getenv("TRADER_ORDER_MODE"))

# 🔸 Параметры Bybit API (используем те же ENV, что и в процессоре)
API_KEY = os.getenv("BYBIT_API_KEY", "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com")
RECV_WINDOW = os.getenv("BYBIT_RECV_WINDOW", "5000")
CATEGORY = "linear"  # USDT-perp

# 🔸 Локальная политика (без ENV)
TP1_LEVEL = 1                           # первый TP — особая логика (reverse)
TP1_TOL = Decimal("0.98")               # доля исполнения TP1 на бирже, достаточная для «выполнено»
MAINT_INTERVAL_SEC = 2.0                # период фон-вотчера
LOCK_TTL_SEC = 10                       # TTL локов по позиции, сек

# 🔸 Основной цикл воркера (последовательная обработка) + фон-вотчер
async def run_bybit_maintainer_loop():
    redis = infra.redis_client

    try:
        await redis.xgroup_create(POS_UPD_STREAM, CG_NAME, id="$", mkstream=True)
        log.debug("📡 Consumer Group создана: %s → %s", POS_UPD_STREAM, CG_NAME)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug("ℹ️ Consumer Group уже существует: %s", CG_NAME)
        else:
            log.exception("❌ Ошибка создания Consumer Group")
            return

    log.info("🚦 BYBIT_MAINTAINER запущен. MODE=%s (TP1_TOL=%s, WATCH=%.1fs)", TRADER_ORDER_MODE, TP1_TOL, MAINT_INTERVAL_SEC)

    # запускаем вотчер «биржа раньше системы» параллельно
    watcher_task = asyncio.create_task(_run_exchange_first_watcher())

    try:
        while True:
            try:
                entries = await redis.xreadgroup(
                    groupname=CG_NAME,
                    consumername=CONSUMER,
                    streams={POS_UPD_STREAM: ">"},
                    count=1,
                    block=1000
                )
                if not entries:
                    continue

                for _, records in entries:
                    for record_id, data in records:
                        try:
                            await _handle_pos_update(record_id, data)
                        except Exception:
                            log.exception("❌ Ошибка обработки позиции (id=%s)", record_id)
                            await redis.xack(POS_UPD_STREAM, CG_NAME, record_id)
                        else:
                            await redis.xack(POS_UPD_STREAM, CG_NAME, record_id)

            except Exception:
                log.exception("❌ Ошибка в основном цикле BYBIT_MAINTAINER")
                await asyncio.sleep(2)
    finally:
        watcher_task.cancel()
        with contextlib.suppress(Exception):
            await watcher_task

# 🔸 Обработка события из positions_update_stream
async def _handle_pos_update(record_id: str, raw: Dict[str, Any]) -> None:
    evt = _parse_event(raw)
    if not evt:
        log.debug("⚠️ Пропуск (пустой/непонятный payload) id=%s raw=%s", record_id, raw)
        return

    event_type = (evt.get("event_type") or "").lower()
    position_uid = _as_str(evt.get("position_uid"))
    symbol = _as_str(evt.get("symbol"))
    if not position_uid or not symbol:
        log.debug("⚠️ Пропуск (нет position_uid/symbol). id=%s evt=%s", record_id, evt)
        return

    # позиция должна быть «open» у трейдера
    tracked = await infra.pg_pool.fetchrow("SELECT status FROM public.trader_positions WHERE position_uid=$1", position_uid)
    if not tracked or _as_str(tracked["status"]) != "open":
        log.debug("ℹ️ Позиция не отслеживается/закрыта у трейдера, uid=%s", position_uid)
        return

    direction = await _get_direction(position_uid)  # 'long'|'short'

    if event_type == "tp_hit":
        level = _as_int(evt.get("tp_level")) or TP1_LEVEL
        # интересует reverse-кейс (TP1) — приводим биржу в соответствие «система выполнила TP1»
        if level == TP1_LEVEL:
            await _with_lock(position_uid, _tp1_system_hit, position_uid, symbol, direction, evt)
        else:
            log.debug("↷ TP level=%s не требует действий на бирже (uid=%s)", level, position_uid)

    elif event_type in ("sl_replaced", "protect"):
        # перенос SL (на entry или иной уровень) по внутреннему правилу
        await _with_lock(position_uid, _sl_replace_system, position_uid, symbol, direction, evt)

    elif event_type == "closed":
        # защитное закрытие/реверс — закрыть остаток и отменить TP/SL
        await _with_lock(position_uid, _closed_system, position_uid, symbol, direction, evt)

    else:
        log.debug("↷ BYBIT_MAINTAINER: игнор event_type=%s uid=%s", event_type, position_uid)

# 🔸 TP1: система считает TP1 выполненным → дожимаем биржу и переставляем SL на entry
async def _tp1_system_hit(position_uid: str, symbol: str, direction: Optional[str], evt: Dict[str, Any]) -> None:
    # план
    lines = [f"[MAINTAINER TP1] uid={position_uid} symbol={symbol} → align exchange to SYSTEM"]

    # факт исполнения TP1 на бирже
    tp_link = f"{position_uid}-tp-{TP1_LEVEL}"
    tpo = await _fetch_tpo(tp_link)
    planned = _as_decimal(tpo.get("qty")) if tpo else None
    filled = _as_decimal(tpo.get("filled_qty")) if tpo else None

    # дожать недостающее
    if planned and (filled or Decimal("0")) < planned * TP1_TOL:
        need = (planned - (filled or Decimal("0")))
        if need > 0:
            lines.append(f"force-close: qty={_fmt(need)} link={tp_link}-force")
            log.info("\n" + "\n".join(lines))
            if TRADER_ORDER_MODE == "on" and (API_KEY and API_SECRET):
                ok_f, oid_f, rc_f, rm_f = await _submit_close_market(
                    symbol=symbol,
                    side=_to_title_side(_opposite(direction)),
                    qty=need,
                    link_id=await _next_force_link_id(position_uid, TP1_LEVEL)
                )
                await _mark_order_after_submit(order_link_id=await _last_force_link_id(position_uid), ok=ok_f, order_id=oid_f, retcode=rc_f, retmsg=rm_f)
    else:
        log.info("\n" + "\n".join(lines))

    # отмена текущих SL и постановка нового SL на entry (остаток)
    qty_left = await _calc_real_remainder_qty(position_uid)
    entry_price = await _fetch_entry_price(position_uid)
    if qty_left <= 0 or entry_price is None:
        log.info("[MAINTAINER TP1] nothing to SL-shift (qty_left=%s entry=%s) uid=%s", _fmt(qty_left), _fmt(entry_price), position_uid)
        return

    await _cancel_all_sl(position_uid)
    trig_dir = _calc_trigger_direction(direction)
    new_sl_link = await _next_sl_link_id(position_uid)
    if TRADER_ORDER_MODE == "on" and (API_KEY and API_SECRET):
        ok_s, oid_s, rc_s, rm_s = await _submit_sl(
            symbol=symbol,
            side=_to_title_side(_opposite(direction)),
            trigger_price=entry_price,
            qty=qty_left,
            link_id=new_sl_link,
            trigger_direction=trig_dir,
        )
        await _mark_order_after_submit(order_link_id=new_sl_link, ok=ok_s, order_id=oid_s, retcode=rc_s, retmsg=rm_s)
        if ok_s:
            await _upsert_planned_sl(position_uid, symbol, qty_left, entry_price, new_sl_link)

# 🔸 SL-replace/protect из системы: перенести SL (обычно на entry) на весь остаток
async def _sl_replace_system(position_uid: str, symbol: str, direction: Optional[str], evt: Dict[str, Any]) -> None:
    qty_left = await _calc_real_remainder_qty(position_uid)
    if qty_left <= 0:
        await _cancel_all_sl(position_uid)
        log.info("[MAINTAINER SL_REPLACE] qty_left=0 → canceled all SL (uid=%s)", position_uid)
        return

    # целевой уровень: из события или entry
    new_sl_price = _as_decimal(evt.get("new_sl_price")) or await _fetch_entry_price(position_uid)
    if new_sl_price is None:
        log.info("[MAINTAINER SL_REPLACE] no target price → skip (uid=%s)", position_uid)
        return

    log.info("[MAINTAINER SL_REPLACE] uid=%s cancel SL → submit SL(trigger=%s qty=%s)", position_uid, _fmt(new_sl_price), _fmt(qty_left))

    await _cancel_all_sl(position_uid)
    trig_dir = _calc_trigger_direction(direction)
    new_sl_link = await _next_sl_link_id(position_uid)
    if TRADER_ORDER_MODE == "on" and (API_KEY and API_SECRET):
        ok_s, oid_s, rc_s, rm_s = await _submit_sl(
            symbol=symbol,
            side=_to_title_side(_opposite(direction)),
            trigger_price=new_sl_price,
            qty=qty_left,
            link_id=new_sl_link,
            trigger_direction=trig_dir,
        )
        await _mark_order_after_submit(order_link_id=new_sl_link, ok=ok_s, order_id=oid_s, retcode=rc_s, retmsg=rm_s)
        if ok_s:
            await _upsert_planned_sl(position_uid, symbol, qty_left, new_sl_price, new_sl_link)

# 🔸 Closed из системы (protect/reverse): отменить TP/SL и закрыть остаток market RO
async def _closed_system(position_uid: str, symbol: str, direction: Optional[str], evt: Dict[str, Any]) -> None:
    close_reason = _as_str(evt.get("close_reason")).lower()
    await _cancel_all_tp_sl(position_uid)

    qty_left = await _calc_real_remainder_qty(position_uid)
    do_close = qty_left > 0 and any(k in close_reason for k in ("protect", "reverse", "signal-stop", "stoploss", "sl"))
    if not do_close:
        log.info("[MAINTAINER CLOSED] uid=%s reason=%s → nothing to close on exchange", position_uid, close_reason or "-")
        return

    close_link = f"{position_uid}-close"
    log.info("[MAINTAINER CLOSED] uid=%s → submit CLOSE qty=%s", position_uid, _fmt(qty_left))
    if TRADER_ORDER_MODE == "on" and (API_KEY and API_SECRET):
        ok_c, oid_c, rc_c, rm_c = await _submit_close_market(
            symbol=symbol,
            side=_to_title_side(_opposite(direction)),
            qty=qty_left,
            link_id=close_link
        )
        await _mark_order_after_submit(order_link_id=close_link, ok=ok_c, order_id=oid_c, retcode=rc_c, retmsg=rm_c)
        if ok_c:
            await _upsert_planned_close(position_uid, symbol, qty_left, close_link)

# 🔸 Вотчер «биржа раньше системы»: TP1 достигнут на бирже → SL на entry; SL filled → отменить TP
async def _run_exchange_first_watcher():
    while True:
        try:
            # кандидаты: позиция open, есть tp L1 в tpo
            rows = await infra.pg_pool.fetch(
                """
                SELECT t.position_uid, t.symbol, t.qty, t.filled_qty
                FROM public.trader_position_orders t
                JOIN public.trader_positions p ON p.position_uid = t.position_uid
                WHERE p.status='open' AND t.kind='tp' AND t.level=$1
                """,
                TP1_LEVEL
            )
            for r in rows:
                uid = _as_str(r["position_uid"]); symbol = _as_str(r["symbol"])
                planned = _as_decimal(r["qty"]) or Decimal("0")
                filled = _as_decimal(r["filled_qty"]) or Decimal("0")
                if planned <= 0:
                    continue

                # (A) TP1 на бирже исполнен ≥ TOL → SL должен быть на entry
                if filled >= planned * TP1_TOL:
                    # проверим, что SL не уже на entry (по последнему активному SL)
                    entry_price = await _fetch_entry_price(uid)
                    if entry_price is None:
                        continue
                    last_sl = await infra.pg_pool.fetchrow(
                        """
                        SELECT trigger_price FROM public.trader_position_orders
                        WHERE position_uid=$1 AND kind='sl'
                          AND (ext_status IS NULL OR ext_status NOT IN ('canceled','filled','expired','rejected'))
                        ORDER BY id DESC LIMIT 1
                        """,
                        uid
                    )
                    if last_sl and _as_decimal(last_sl["trigger_price"]) == entry_price:
                        continue  # уже на entry
                    # иначе переставим
                    await _with_lock(uid, _sl_shift_to_entry_exchange_first, uid, symbol)

                # (B) SL filled на бирже → отменить TP (осторожность)
                sl_filled = await infra.pg_pool.fetchrow(
                    """
                    SELECT 1 FROM public.trader_position_orders
                    WHERE position_uid=$1 AND kind='sl' AND ext_status='filled' LIMIT 1
                    """,
                    uid
                )
                if sl_filled:
                    await _cancel_all_tp(uid)

        except Exception:
            log.exception("MAINTAINER watcher: error")

        await asyncio.sleep(MAINT_INTERVAL_SEC)

# условия достаточности
async def _sl_shift_to_entry_exchange_first(uid: str, symbol: str) -> None:
    direction = await _get_direction(uid)
    qty_left = await _calc_real_remainder_qty(uid)
    entry_price = await _fetch_entry_price(uid)
    if qty_left <= 0 or entry_price is None:
        return
    await _cancel_all_sl(uid)
    link = await _next_sl_link_id(uid)
    if TRADER_ORDER_MODE == "on" and (API_KEY and API_SECRET):
        ok, oid, rc, rm = await _submit_sl(
            symbol=symbol,
            side=_to_title_side(_opposite(direction)),
            trigger_price=entry_price,
            qty=qty_left,
            link_id=link,
            trigger_direction=_calc_trigger_direction(direction),
        )
        await _mark_order_after_submit(order_link_id=link, ok=ok, order_id=oid, retcode=rc, retmsg=rm)
        if ok:
            await _upsert_planned_sl(uid, symbol, qty_left, entry_price, link)

# 🔸 Парсинг payload из XREADGROUP
def _parse_event(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    data = raw.get("data")
    if data is None:
        return {k: _as_native(v) for k, v in raw.items()}
    try:
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8", "ignore")
        obj = json.loads(data)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None

# 🔸 Утилиты приведения/округления
def _as_native(v: Any) -> Any:
    if isinstance(v, (bytes, bytearray)):
        try: return v.decode("utf-8", "ignore")
        except Exception: return v
    return v

def _as_str(v: Any) -> str:
    if v is None: return ""
    return v.decode() if isinstance(v, (bytes, bytearray)) else str(v)

def _as_int(v: Any) -> Optional[int]:
    try:
        s = _as_str(v); return int(s) if s != "" else None
    except Exception: return None

def _as_decimal(v: Any) -> Optional[Decimal]:
    try:
        if v is None: return None
        if isinstance(v, Decimal): return v
        return Decimal(str(v))
    except Exception: return None

def _fmt(x: Optional[Decimal], max_prec: int = 8) -> str:
    if x is None: return "—"
    try:
        s = f"{x:.{max_prec}f}".rstrip("0").rstrip("."); return s if s else "0"
    except Exception: return str(x)

def _round_qty(qty: Decimal, precision_qty: Optional[int]) -> Decimal:
    if qty is None: return Decimal("0")
    if precision_qty is None: return qty
    step = Decimal("1").scaleb(-int(precision_qty))
    try: return qty.quantize(step, rounding=ROUND_DOWN)
    except Exception: return qty

def _round_price(price: Optional[Decimal], ticksize: Optional[Decimal]) -> Optional[Decimal]:
    if price is None or ticksize is None: return price
    try:
        quantum = _as_decimal(ticksize) or Decimal("0")
        if quantum <= 0: return price
        return price.quantize(quantum, rounding=ROUND_HALF_UP)
    except Exception: return price

def _opposite(direction: Optional[str]) -> str:
    d = (direction or "").lower()
    return "short" if d == "long" else "long"

def _to_title_side(side: str) -> str:
    s = (side or "").upper()
    return "Buy" if s == "BUY" else "Sell"

def _calc_trigger_direction(position_direction: Optional[str]) -> int:
    d = (position_direction or "").lower()
    return 2 if d == "long" else 1  # long→ждём падение (2), short→ждём рост (1)

# 🔸 Лок на позицию (Redis SET NX EX)
async def _with_lock(position_uid: str, coro, *args, **kwargs):
    key = f"bybit:maint:lock:{position_uid}"
    redis = infra.redis_client
    try:
        ok = await redis.set(key, "1", ex=LOCK_TTL_SEC, nx=True)
        if not ok:
            log.debug("lock busy uid=%s", position_uid); return
        await coro(*args, **kwargs)
    finally:
        try: await redis.delete(key)
        except Exception: pass

# 🔸 Доступ к БД / вспомогательные выборки
async def _get_direction(position_uid: str) -> Optional[str]:
    row = await infra.pg_pool.fetchrow("SELECT direction FROM public.positions_v4 WHERE position_uid=$1", position_uid)
    return (_as_str(row["direction"]).lower() if row and row["direction"] else None)

async def _fetch_entry_price(position_uid: str) -> Optional[Decimal]:
    row = await infra.pg_pool.fetchrow("SELECT entry_price FROM public.positions_v4 WHERE position_uid=$1", position_uid)
    return _as_decimal(row["entry_price"]) if row and row["entry_price"] is not None else None

async def _fetch_tpo(order_link_id: str) -> Dict[str, Any]:
    row = await infra.pg_pool.fetchrow(
        """
        SELECT position_uid, kind, level, qty, filled_qty, order_id, ext_status
        FROM public.trader_position_orders WHERE order_link_id=$1
        """,
        order_link_id
    )
    return dict(row) if row else {}

async def _calc_real_remainder_qty(position_uid: str) -> Decimal:
    q_entry_row = await infra.pg_pool.fetchrow(
        "SELECT qty FROM public.trader_position_orders WHERE position_uid=$1 AND kind='entry' ORDER BY id DESC LIMIT 1",
        position_uid
    )
    entry_qty = _as_decimal(q_entry_row["qty"]) if q_entry_row and q_entry_row["qty"] is not None else Decimal("0")
    sums = await infra.pg_pool.fetchrow(
        """
        SELECT
          COALESCE(SUM(CASE WHEN kind='tp' THEN filled_qty ELSE 0 END),0) AS tp_filled,
          COALESCE(SUM(CASE WHEN kind='close' THEN filled_qty ELSE 0 END),0) AS close_filled
        FROM public.trader_position_orders WHERE position_uid=$1
        """,
        position_uid
    )
    tp_filled = _as_decimal(sums["tp_filled"]) or Decimal("0")
    close_filled = _as_decimal(sums["close_filled"]) or Decimal("0")
    left = entry_qty - tp_filled - close_filled
    return left if left > 0 else Decimal("0")

async def _cancel_all_sl(position_uid: str) -> None:
    rows = await infra.pg_pool.fetch(
        """
        SELECT order_link_id, symbol
        FROM public.trader_position_orders
        WHERE position_uid=$1 AND kind='sl'
          AND (ext_status IS NULL OR ext_status NOT IN ('canceled','filled','expired','rejected'))
        """,
        position_uid
    )
    for r in rows:
        link = _as_str(r["order_link_id"]); symbol = _as_str(r["symbol"])
        if not link or not symbol: continue
        if TRADER_ORDER_MODE == "on" and (API_KEY and API_SECRET):
            resp = await _bybit_post("/v5/order/cancel", {"category": CATEGORY, "symbol": symbol, "orderLinkId": link})
            rc, rm = resp.get("retCode"), resp.get("retMsg")
            log.info("cancel SL: %s → rc=%s msg=%s", link, rc, rm)
        if TRADER_ORDER_MODE == "on":
            await infra.pg_pool.execute(
                "UPDATE public.trader_position_orders SET ext_status='canceled', last_ext_event_at=$2 WHERE order_link_id=$1",
                link, datetime.utcnow()
            )

async def _cancel_all_tp(position_uid: str) -> None:
    rows = await infra.pg_pool.fetch(
        """
        SELECT order_link_id, symbol
        FROM public.trader_position_orders
        WHERE position_uid=$1 AND kind='tp'
          AND (ext_status IS NULL OR ext_status NOT IN ('canceled','filled','expired','rejected'))
        """,
        position_uid
    )
    for r in rows:
        link = _as_str(r["order_link_id"]); symbol = _as_str(r["symbol"])
        if not link or not symbol: continue
        if TRADER_ORDER_MODE == "on" and (API_KEY and API_SECRET):
            resp = await _bybit_post("/v5/order/cancel", {"category": CATEGORY, "symbol": symbol, "orderLinkId": link})
            rc, rm = resp.get("retCode"), resp.get("retMsg")
            log.info("cancel TP: %s → rc=%s msg=%s", link, rc, rm)
        if TRADER_ORDER_MODE == "on":
            await infra.pg_pool.execute(
                "UPDATE public.trader_position_orders SET ext_status='canceled', last_ext_event_at=$2 WHERE order_link_id=$1",
                link, datetime.utcnow()
            )

async def _cancel_all_tp_sl(position_uid: str) -> None:
    rows = await infra.pg_pool.fetch(
        """
        SELECT order_link_id, symbol
        FROM public.trader_position_orders
        WHERE position_uid=$1 AND kind IN ('tp','sl')
          AND (ext_status IS NULL OR ext_status NOT IN ('canceled','filled','expired','rejected'))
        """,
        position_uid
    )
    for r in rows:
        link = _as_str(r["order_link_id"]); symbol = _as_str(r["symbol"])
        if not link or not symbol: continue
        if TRADER_ORDER_MODE == "on" and (API_KEY and API_SECRET):
            resp = await _bybit_post("/v5/order/cancel", {"category": CATEGORY, "symbol": symbol, "orderLinkId": link})
            rc, rm = resp.get("retCode"), resp.get("retMsg")
            log.info("cancel TP/SL: %s → rc=%s msg=%s", link, rc, rm)
        if TRADER_ORDER_MODE == "on":
            await infra.pg_pool.execute(
                "UPDATE public.trader_position_orders SET ext_status='canceled', last_ext_event_at=$2 WHERE order_link_id=$1",
                link, datetime.utcnow()
            )

async def _next_sl_link_id(position_uid: str) -> str:
    rows = await infra.pg_pool.fetch(
        "SELECT order_link_id FROM public.trader_position_orders WHERE position_uid=$1 AND kind='sl'",
        position_uid
    )
    max_ver = 0
    base = f"{position_uid}-sl"
    for r in rows:
        lid = _as_str(r["order_link_id"]) or ""
        if lid == base:
            max_ver = max(max_ver, 1)
        elif "-sl-v" in lid:
            try:
                n = int(lid.split("-sl-v", 1)[1]); max_ver = max(max_ver, n)
            except Exception:
                pass
    return base if max_ver == 0 else f"{position_uid}-sl-v{max_ver + 1}"

async def _next_force_link_id(position_uid: str, level: int) -> str:
    base = f"{position_uid}-tp-{level}-force"
    rows = await infra.pg_pool.fetch(
        "SELECT order_link_id FROM public.trader_position_orders WHERE position_uid=$1 AND order_link_id LIKE $2",
        position_uid, f"{base}%"
    )
    max_ver = 0
    for r in rows:
        lid = _as_str(r["order_link_id"]) or ""
        if lid == base:
            max_ver = max(max_ver, 1)
        elif f"{base}-v" in lid:
            try:
                n = int(lid.split(f"{base}-v", 1)[1]); max_ver = max(max_ver, n)
            except Exception:
                pass
    return base if max_ver == 0 else f"{base}-v{max_ver + 1}"

async def _last_force_link_id(position_uid: str) -> str:
    # берём последний force-link по id
    row = await infra.pg_pool.fetchrow(
        """
        SELECT order_link_id FROM public.trader_position_orders
        WHERE position_uid=$1 AND order_link_id LIKE $2
        ORDER BY id DESC LIMIT 1
        """,
        position_uid, f"{position_uid}-tp-{TP1_LEVEL}-force%"
    )
    return _as_str(row["order_link_id"]) if row else f"{position_uid}-tp-{TP1_LEVEL}-force"

async def _upsert_planned_sl(position_uid: str, symbol: str, qty: Decimal, trigger_price: Decimal, link_id: str) -> None:
    now = datetime.utcnow()
    await infra.pg_pool.execute(
        """
        INSERT INTO public.trader_position_orders (
            position_uid, kind, level, exchange, symbol, side, "type", tif, reduce_only,
            price, trigger_price, qty, order_link_id, ext_status,
            qty_raw, price_raw, created_at
        ) VALUES ($1,'sl',NULL,'BYBIT',$2,NULL,'stop_market','GTC',true,
                  NULL,$3,$4,$5,'submitted',NULL,NULL,$6)
        ON CONFLICT (order_link_id) DO NOTHING
        """,
        position_uid, symbol, trigger_price, qty, link_id, now
    )

async def _upsert_planned_close(position_uid: str, symbol: str, qty: Decimal, link_id: str) -> None:
    now = datetime.utcnow()
    await infra.pg_pool.execute(
        """
        INSERT INTO public.trader_position_orders (
            position_uid, kind, level, exchange, symbol, side, "type", tif, reduce_only,
            price, trigger_price, qty, order_link_id, ext_status,
            qty_raw, price_raw, created_at
        ) VALUES ($1,'close',NULL,'BYBIT',$2,NULL,'market','GTC',true,
                  NULL,NULL,$3,$4,'submitted',NULL,NULL,$5)
        ON CONFLICT (order_link_id) DO NOTHING
        """,
        position_uid, symbol, qty, link_id, now
    )
# 🔸 Post-submit апдейт строки ордера (по order_link_id)
async def _mark_order_after_submit(
    *,
    order_link_id: str,
    ok: bool,
    order_id: Optional[str],
    retcode: Optional[int],
    retmsg: Optional[str],
) -> None:
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
        ok, order_id, status, now,
        (f"retCode={retcode} retMsg={retmsg}" if not ok else None),
        order_link_id,
    )
    
# 🔸 Сабмиты/отмена на бирже
async def _submit_sl(*, symbol: str, side: str, trigger_price: Decimal, qty: Decimal, link_id: str, trigger_direction: int) -> Tuple[bool, Optional[str], Optional[int], Optional[str]]:
    body = {
        "category": CATEGORY,
        "symbol": symbol,
        "side": side,
        "orderType": "Market",
        "qty": _fmt(qty),
        "reduceOnly": True,
        "triggerPrice": _fmt(trigger_price),
        "triggerDirection": trigger_direction,
        "triggerBy": "LastPrice",
        "closeOnTrigger": True,
        "timeInForce": "GTC",
        "orderLinkId": link_id,
    }
    resp = await _bybit_post("/v5/order/create", body)
    rc, rm = resp.get("retCode"), resp.get("retMsg")
    oid = _extract_order_id(resp); ok = (rc == 0)
    log.info("submit SL: %s trigger=%s dir=%s qty=%s link=%s → rc=%s msg=%s oid=%s",
             symbol, _fmt(trigger_price), trigger_direction, _fmt(qty), link_id, rc, rm, oid)
    return ok, oid, rc, rm

async def _submit_close_market(*, symbol: str, side: str, qty: Decimal, link_id: str) -> Tuple[bool, Optional[str], Optional[int], Optional[str]]:
    body = {
        "category": CATEGORY,
        "symbol": symbol,
        "side": side,
        "orderType": "Market",
        "qty": _fmt(qty),
        "reduceOnly": True,
        "timeInForce": "GTC",
        "orderLinkId": link_id,
    }
    resp = await _bybit_post("/v5/order/create", body)
    rc, rm = resp.get("retCode"), resp.get("retMsg")
    oid = _extract_order_id(resp); ok = (rc == 0)
    log.info("submit CLOSE: %s qty=%s link=%s → rc=%s msg=%s oid=%s", symbol, _fmt(qty), link_id, rc, rm, oid)
    return ok, oid, rc, rm

# 🔸 REST-хелперы
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
        oid = res.get("orderId")
        return _as_str(oid) if oid is not None else None
    except Exception:
        return None

def _now_ms() -> int:
    import time
    return int(time.time() * 1000)