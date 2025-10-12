# bb_tickers_precision_updater.py — почасовое обновление точностей из Bybit (linear) в tickers_bb

# 🔸 Импорты и зависимости
import os
import asyncio
import logging
from decimal import Decimal, InvalidOperation

import aiohttp

log = logging.getLogger("BB_PRECISION_UPDATER")

# 🔸 Конфиг
BYBIT_REST_BASE = os.getenv("BYBIT_REST_BASE", "https://api.bybit.com")
CATEGORY = "linear"                  # деривативы USDT-perp
REFRESH_SEC = int(os.getenv("BB_PRECISION_REFRESH_SEC", "86400"))
REQUEST_TIMEOUT = int(os.getenv("BB_HTTP_TIMEOUT_SEC", "15"))
PER_SYMBOL_DELAY = float(os.getenv("BB_PER_SYMBOL_DELAY_SEC", "0.2"))

# 🔸 Вспомогательные утилиты (без эмоджи)
def _decimals_from_step(step_str: str | None) -> int | None:
    if not step_str:
        return None
    try:
        d = Decimal(step_str)
    except InvalidOperation:
        return None
    tup = d.as_tuple()
    # пример: Decimal('0.001') → exponent = -3 → 3 знака
    return max(0, -tup.exponent)

async def _fetch_instrument_info(session: aiohttp.ClientSession, symbol: str) -> dict | None:
    url = f"{BYBIT_REST_BASE}/v5/market/instruments-info"
    params = {"category": CATEGORY, "symbol": symbol}
    try:
        async with session.get(url, params=params, timeout=REQUEST_TIMEOUT) as resp:
            resp.raise_for_status()
            js = await resp.json()
            if js.get("retCode") != 0:
                log.warning(f"[{symbol}] retCode={js.get('retCode')} msg={js.get('retMsg')}")
                return None
            lst = (js.get("result") or {}).get("list") or []
            return lst[0] if lst else None
    except Exception as e:
        log.warning(f"[{symbol}] instruments-info error: {e}")
        return None

async def _update_symbol_row(conn, symbol: str, ticksize: str | None, qty_step: str | None, min_qty: str | None):
    pp = _decimals_from_step(ticksize)
    pq = _decimals_from_step(qty_step)
    # читаем текущие значения
    cur = await conn.execute(
        "SELECT ticksize, min_qty, precision_price, precision_qty FROM tickers_bb WHERE symbol = %s",
        (symbol,)
    )
    row = await cur.fetchone()
    if not row:
        return
    old_tick, old_min, old_pp, old_pq = row

    need = False
    sets = []
    vals = []

    if ticksize is not None and ticksize != old_tick:
        sets.append("ticksize = %s")
        vals.append(ticksize)
        need = True
    if min_qty is not None and min_qty != old_min:
        sets.append("min_qty = %s")
        vals.append(min_qty)
        need = True
    if pp is not None and pp != old_pp:
        sets.append("precision_price = %s")
        vals.append(pp)
        need = True
    if pq is not None and pq != old_pq:
        sets.append("precision_qty = %s")
        vals.append(pq)
        need = True

    if not need:
        return

    vals.append(symbol)
    sql = f"UPDATE tickers_bb SET {', '.join(sets)} WHERE symbol = %s"
    await conn.execute(sql, tuple(vals))

    log.debug(f"[{symbol}] updated: ticksize={ticksize} qtyStep={qty_step} minQty={min_qty} → "
             f"precision_price={pp} precision_qty={pq}")

# 🔸 Основной воркер: раз в час обновляет активные тикеры
async def run_tickers_precision_updater_bb(pg_pool):
    log.debug("BB_PRECISION_UPDATER запущен: обновляю precision из Bybit instruments-info (linear)")

    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT + 5)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # первый прогон сразу
        while True:
            try:
                # берём все символы из tickers_bb (можно ограничить enabled=true, если хочешь)
                async with pg_pool.connection() as conn:
                    cur = await conn.execute("SELECT symbol FROM tickers_bb ORDER BY symbol")
                    rows = await cur.fetchall()
                    symbols = [r[0] for r in rows] if rows else []

                if not symbols:
                    log.debug("BB_PRECISION_UPDATER: символов нет")
                else:
                    log.debug(f"BB_PRECISION_UPDATER: обновляю {len(symbols)} символов")
                    for sym in symbols:
                        info = await _fetch_instrument_info(session, sym)
                        if not info:
                            await asyncio.sleep(PER_SYMBOL_DELAY)
                            continue

                        price_filter = (info.get("priceFilter") or {})
                        lot_filter = (info.get("lotSizeFilter") or {})

                        ticksize = price_filter.get("tickSize")
                        qty_step = lot_filter.get("qtyStep")
                        min_qty = lot_filter.get("minOrderQty")

                        async with pg_pool.connection() as conn:
                            await _update_symbol_row(conn, sym, ticksize, qty_step, min_qty)

                        await asyncio.sleep(PER_SYMBOL_DELAY)

            except Exception as e:
                log.error(f"BB_PRECISION_UPDATER ошибка: {e}", exc_info=True)

            # ждать следующий цикл
            await asyncio.sleep(REFRESH_SEC)