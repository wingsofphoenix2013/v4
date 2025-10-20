# bybit_processor.py — dry-run планировщик биржевых ордеров по событиям opened (entry + TP/SL), без отправки

# 🔸 Импорты
import os
import logging
import asyncio
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

from trader_infra import infra

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
    # ограничим здравым диапазоном 0..1000 (на будущее)
    if pct < 0:
        pct = Decimal("0")
    if pct > 1000:
        pct = Decimal("1000")
    return (pct / Decimal("100"))

SIZE_FACTOR = _get_size_factor()

# сообщим о режимах в лог
if TRADER_ORDER_MODE == "dry_run":
    log.info("BYBIT processor mode: DRY_RUN (формируем план ордеров, без отправки). SIZE_FACTOR=%.4f", float(SIZE_FACTOR))
elif TRADER_ORDER_MODE == "off":
    log.info("BYBIT processor mode: OFF (игнорируем заявки).")
else:
    log.info("BYBIT processor mode: ON (реальная отправка ещё не реализована на этом этапе).")

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

# 🔸 Обработка одной заявки из стрима (ожидаем минимум position_uid, strategy_id)
async def _handle_order_request(record_id: str, data: Dict[str, Any]) -> None:
    # режим off: сразу выходим
    if TRADER_ORDER_MODE == "off":
        log.debug("TRADER_ORDER_MODE=off — пропуск заявки id=%s", record_id)
        return

    position_uid = _as_str(data.get("position_uid"))
    if not position_uid:
        log.debug("⚠️ Пропуск записи (нет position_uid) id=%s", record_id)
        return

    # тянем позицию из БД
    pos = await _fetch_position(position_uid)
    if not pos:
        log.info("ℹ️ Позиция не найдена в positions_v4, uid=%s", position_uid)
        return

    symbol = _as_str(pos.get("symbol"))
    direction = (_as_str(pos.get("direction")) or "").lower()
    entry_price = _as_decimal(pos.get("entry_price"))
    qty_raw = _as_decimal(pos.get("quantity")) or Decimal("0")
    created_at = pos.get("created_at")

    if not symbol or direction not in ("long", "short") or qty_raw <= 0:
        log.info("⚠️ Недостаточно данных позиции: uid=%s symbol=%s direction=%s qty=%s", position_uid, symbol, direction, qty_raw)
        return

    # правила округлений по тикеру
    rules = await _load_symbol_rules(symbol)
    precision_qty = rules.get("precision_qty")
    min_qty = rules.get("min_qty")
    ticksize = rules.get("ticksize")

    # цели TP/SL
    tp_list, tp_signal_skipped, sl_one = await _fetch_targets_for_plan(position_uid)

    # расчёт фактических величин
    side_word = "BUY" if direction == "long" else "SELL"
    qty_entry_real = _round_qty(qty_raw * SIZE_FACTOR, precision_qty)

    # округление цены входа (для лога) и целей
    entry_price_disp = _round_price(entry_price, ticksize)

    # собираем строковый отчёт (DRY_RUN)
    lines: List[str] = []
    lines.append(f"[ORDER_DRY_RUN OPEN] uid={position_uid} symbol={symbol} side={'LONG' if direction=='long' else 'SHORT'}")
    lines.append(f"entry: market {side_word} qty_raw={_fmt(qty_raw)} qty_real={_fmt(qty_entry_real)} linkId={position_uid}-entry")

    # TP с ценой (percent/atr) — лимитные reduce-only
    if tp_list:
        for level, price_raw, qty_tp_raw in tp_list:
            price_real = _round_price(price_raw, ticksize)
            qty_tp_real = _round_qty(qty_tp_raw * SIZE_FACTOR, precision_qty)
            note = ""
            if min_qty is not None and qty_tp_real < min_qty:
                note = "  # qty_real < min_qty → SKIP"
            lines.append(
                f"tpL{level}: limit reduceOnly price={_fmt(price_real)} "
                f"qty_raw={_fmt(qty_tp_raw)} qty_real={_fmt(qty_tp_real)} linkId={position_uid}-tp-{level}{note}"
            )
    else:
        lines.append("tp: —  # no percent/atr TP with price")

    # SL (первый «живой», если есть цена) — стоп-маркет reduce-only на весь реальный объём
    if sl_one and sl_one[0] is not None:
        sl_trigger = _round_price(sl_one[0], ticksize)
        lines.append(
            f"sl: stop-market reduceOnly trigger={_fmt(sl_trigger)} qty={_fmt(qty_entry_real)} linkId={position_uid}-sl"
        )
    else:
        lines.append("sl: —  # WARN: no SL price")

    # заметки
    if tp_signal_skipped > 0:
        lines.append(f"note: skipped {tp_signal_skipped} signal-TP (no exchange order)")

    if min_qty is not None and qty_entry_real < min_qty:
        lines.append("note: entry qty_real < min_qty → SKIP (entry too small)")

    if created_at:
        lines.append(f"created_at: {created_at} (UTC naive)  entry_price≈{_fmt(entry_price_disp)}")

    # вывод в лог (DRY_RUN / ON)
    if TRADER_ORDER_MODE == "dry_run":
        log.info("\n" + "\n".join(lines))
        return

    # режим ON ещё не реализован на этом этапе
    log.info("MODE=ON: отправка ордеров не реализована, план:\n" + "\n".join(lines))

# 🔸 Вспомогательные функции извлечения и округлений
def _as_str(v: Any) -> str:
    if v is None:
        return ""
    return v.decode() if isinstance(v, (bytes, bytearray)) else str(v)

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
    # шаг количества = 10^-precision_qty, округляем вниз
    step = Decimal("1").scaleb(-int(precision_qty))
    try:
        return qty.quantize(step, rounding=ROUND_DOWN)
    except Exception:
        return qty

def _round_price(price: Optional[Decimal], ticksize: Optional[Decimal]) -> Optional[Decimal]:
    if price is None or ticksize is None:
        return price
    # округление к ближайшему разрешённому шагу цены
    try:
        quantum = _as_decimal(ticksize) or Decimal("0")
        if quantum <= 0:
            return price
        return price.quantize(quantum, rounding=ROUND_HALF_UP)
    except Exception:
        return price

# 🔸 Доступ к БД
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