# bybit_processor.py — entry → ожидание стабильного fill → расчёт TP/SL от финального avg fill → сабмит TP/SL (ценовые), виртуальные TP(signal) и шаблоны SL-после-TP → фиксация в БД

# 🔸 Импорты
import os
import json
import logging
import asyncio
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import httpx

from trader_infra import infra
from trader_config import config  # leverage и политика из кэша

# 🔸 Логгер ордеров
log = logging.getLogger("TRADER_ORDERS")

# 🔸 Потоки/группы
ORDER_REQUEST_STREAM = "trader_order_requests"
CG_NAME = "bybit_processor_group"
CONSUMER = "bybit_processor_v2_1"

# 🔸 Режим процессора ордеров (ENV TRADER_ORDER_MODE: off|dry_run|on)
def _normalize_mode(v: Optional[str]) -> str:
    s = (v or "").strip().lower()
    if s in ("off", "false", "0", "no", "disabled"):
        return "off"
    if s in ("dry_run", "dry-run", "dryrun", "test"):
        return "dry_run"
    return "on"

TRADER_ORDER_MODE = _normalize_mode(os.getenv("TRADER_ORDER_MODE"))

# 🔸 Bybit REST (ENV)
API_KEY = os.getenv("BYBIT_API_KEY", "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com")
RECV_WINDOW = os.getenv("BYBIT_RECV_WINDOW", "5000")
CATEGORY = "linear"  # USDT-perp
ACCOUNT_TYPE = os.getenv("BYBIT_ACCOUNT_TYPE", "UNIFIED").upper()  # UNIFIED | CONTRACT | SPOT

# 🔸 Поведение стопов
DEFAULT_TRIGGER_BY = os.getenv("BYBIT_TRIGGER_BY", "LastPrice")  # LastPrice | MarkPrice | IndexPrice

# 🔸 Настройки ожиданий/ограничений (часть — из ENV для окружения)
TP_MIN_QTY_THRESHOLD = Decimal(os.getenv("TP_MIN_QTY_THRESHOLD", "0"))
SIZE_FACTOR = (lambda raw: (Decimal(raw) if raw.replace(".", "", 1).isdigit() else Decimal("100")))(
    os.getenv("BYBIT_SIZE_PCT", "100").strip()
) / Decimal("100")

# 🔸 Единая модель ожидания fill (константы в коде)
ENTRY_FILL_COMPLETENESS = Decimal("0.98")  # доля от планового объёма, при достижении которой считаем fill завершённым
ENTRY_FILL_STABLE_MS    = 2000            # окно стабильности filled_qty (мс)
ENTRY_FILL_MAX_WAIT_SEC = 60              # общий предел ожидания (сек)

# 🔸 Reverse-guard (ожидание «flat» по символу)
REVERSE_WAIT_TIMEOUT_SEC = 10
REVERSE_WAIT_POLL_MS     = 150

# условия достаточности: лог о режиме
if TRADER_ORDER_MODE == "dry_run":
    log.debug("BYBIT processor v2: DRY_RUN (entry/TP/SL в БД; REST без реальной отправки)")
elif TRADER_ORDER_MODE == "off":
    log.debug("BYBIT processor v2: OFF (игнорируем заявки)")
else:
    log.debug(
        "BYBIT processor v2: ON (entry→stable fill→TP/SL от fill); SIZE_FACTOR=%.4f; trigger_by=%s; fill_gate: comp=%.2f, stable=%dms, max_wait=%ds",
        float(SIZE_FACTOR), DEFAULT_TRIGGER_BY, float(ENTRY_FILL_COMPLETENESS), ENTRY_FILL_STABLE_MS, ENTRY_FILL_MAX_WAIT_SEC
    )

# 🔸 Основной цикл воркера
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

    log.debug("🚦 BYBIT_PROCESSOR v2 запущен")

    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=CG_NAME,
                consumername=CONSUMER,
                streams={ORDER_REQUEST_STREAM: ">"},
                count=10,
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
            await asyncio.sleep(0.5)

# 🔸 Обработка одной заявки из стрима (толстый payload)
async def _handle_order_request(record_id: str, data: Dict[str, Any]) -> None:
    if TRADER_ORDER_MODE == "off":
        log.debug("TRADER_ORDER_MODE=off — пропуск заявки id=%s", record_id)
        return

    position_uid = _as_str(data.get("position_uid"))
    sid = _as_int(data.get("strategy_id"))
    symbol = _as_str(data.get("symbol"))
    direction = (_as_str(data.get("direction")) or "").lower()
    created_at = _parse_dt(_as_str(data.get("created_at")))

    if not position_uid or not sid or not symbol or direction not in ("long", "short"):
        log.debug("⚠️ Недостаточные данные заявки: id=%s uid=%s sid=%s symbol=%s dir=%s", record_id, position_uid, sid, symbol, direction)
        return

    # точности
    precision_qty = _as_int(data.get("precision_qty"))
    min_qty = _as_decimal(data.get("min_qty"))
    ticksize = _as_decimal(data.get("ticksize"))
    if precision_qty is None or ticksize is None:
        t = config.tickers.get(symbol) or {}
        precision_qty = t.get("precision_qty") if precision_qty is None else precision_qty
        ticksize = _as_decimal(t.get("ticksize")) if ticksize is None else ticksize
        min_qty = _as_decimal(t.get("min_qty")) if min_qty is None else min_qty

    # политика стратегии
    policy = _parse_policy_json(_as_str(data.get("policy"))) or (config.strategy_policy.get(sid) or {})
    _normalize_policy_inplace(policy)  # приведение уровней к int

    # плечо
    lev = _as_decimal(data.get("leverage"))
    if lev is None:
        meta = config.strategy_meta.get(sid) or {}
        lev = _as_decimal(meta.get("leverage"))

    # сырьё из positions_v4 (quantity / entry_price mark — для DRY_RUN ориентира)
    qty_raw, entry_price_mark = await _try_fetch_initials_from_positions_v4(position_uid)
    if qty_raw is None or qty_raw <= 0:
        log.debug("⚠️ Нет quantity для uid=%s — пропуск", position_uid)
        return

    # масштабирование объёма под BYBIT_SIZE_PCT
    qty_trade = _round_qty(qty_raw * SIZE_FACTOR, precision_qty)
    if min_qty is not None and qty_trade < min_qty:
        log.debug("⚠️ qty_trade < min_qty (uid=%s, qty_trade=%s, min_qty=%s) — пропуск", position_uid, _fmt(qty_trade), _fmt(min_qty))
        return

    # Reverse-guard: дождаться нулевого остатка по этому символу (старая позиция полностью закрыта)
    ok_flat = await _wait_until_symbol_flat(
        symbol=symbol,
        exclude_uid=position_uid,
        timeout_sec=REVERSE_WAIT_TIMEOUT_SEC,
        poll_ms=REVERSE_WAIT_POLL_MS,
    )
    if not ok_flat:
        log.warning(
            "[REVERSE-GUARD] timeout waiting flat for symbol=%s (uid=%s) → skip opening",
            symbol, position_uid
        )
        return

    # planned entry (до сабмита) — фиксируем фактически планируемый объём (qty_trade)
    entry_link_id = f"{position_uid}-entry"
    side_title = _to_title_side("BUY" if direction == "long" else "SELL")
    await _upsert_order(
        position_uid=position_uid,
        kind="entry",
        level=None,
        exchange="BYBIT",
        symbol=symbol,
        side=_side_word(direction),
        otype="market",
        tif="GTC",
        reduce_only=False,
        price=None,
        trigger_price=None,
        qty=qty_trade,
        order_link_id=entry_link_id,
        ext_status="planned" if TRADER_ORDER_MODE == "on" else "virtual",
        qty_raw=qty_trade,
        price_raw=None,
        calc_type=None,
        calc_value=None,
        base_price=None,
        base_kind=None,
        activation_tp_level=None,
        trigger_by=None,
        supersedes_link_id=None,
    )

    # entry (ON) / dry_run (без REST)
    if TRADER_ORDER_MODE == "on" and API_KEY and API_SECRET:
        # preflight: выставляем плечо на бирже (fail-open)
        await _preflight_set_leverage(symbol, lev)

        ok_e, oid_e, rc_e, rm_e = await _submit_entry(
            symbol=symbol,
            side=side_title,
            qty=qty_trade,
            link_id=entry_link_id,
        )
        await _mark_order_after_submit(
            order_link_id=entry_link_id,
            ok=ok_e,
            order_id=oid_e,
            retcode=rc_e,
            retmsg=rm_e,
        )
        await _mirror_entry_to_trader_positions(
            position_uid=position_uid,
            order_link_id=entry_link_id,
            order_id=oid_e,
            ext_status=("submitted" if ok_e else "rejected"),
        )
        if not ok_e:
            log.debug("⚠️ Entry отвергнут (uid=%s) → прекращаем обработку", position_uid)
            return
    else:
        log.debug("[DRY_RUN] entry planned: uid=%s %s qty=%s", position_uid, symbol, _fmt(qty_trade))

    # 🔸 Ждём завершённость/стабильность fill (единый гейт) — возвращает финальные avg_fill и filled_qty
    avg_fill_price, filled_qty = await _wait_entry_fill_or_fallback(
        position_uid=position_uid,
        entry_link_id=entry_link_id,
        symbol=symbol,
        entry_price_mark=entry_price_mark,
        qty_planned=qty_trade,
        precision_qty=precision_qty,
    )
    if avg_fill_price is None or filled_qty is None or filled_qty <= 0:
        log.debug("⚠️ Не получили достаточный fill для uid=%s — прекращаем обработку", position_uid)
        return

    # 🔸 расчёт TP/SL от финального avg_fill и filled_qty (после гейта)
    plan_tp, plan_tp_signal, plan_sl_primary, plan_sls_after_tp = _build_plan_from_policy(
        position_uid=position_uid,
        symbol=symbol,
        direction=direction,
        avg_fill=avg_fill_price,
        filled_qty=filled_qty,
        precision_qty=precision_qty,
        min_qty=min_qty,
        ticksize=ticksize,
        policy=policy,
    )

    # TP ценовые + TP(signal)
    for lvl, price, qty, link_id in plan_tp:
        await _upsert_order(
            position_uid=position_uid,
            kind="tp",
            level=lvl,
            exchange="BYBIT",
            symbol=symbol,
            side=_side_word(_opposite(direction)),
            otype="limit",
            tif="GTC",
            reduce_only=True,
            price=price,
            trigger_price=None,
            qty=qty,
            order_link_id=link_id,
            ext_status=("planned" if TRADER_ORDER_MODE == "on" else "virtual"),
            qty_raw=qty,
            price_raw=price,
            calc_type="percent" if _tp_is_percent(policy, lvl) else ("atr" if _tp_is_atr(policy, lvl) else None),
            calc_value=_tp_value(policy, lvl),
            base_price=avg_fill_price,
            base_kind="fill",
            activation_tp_level=None,
            trigger_by=None,
            supersedes_link_id=None,
        )
    if plan_tp_signal:
        lvl_sig, qty_sig, link_id_sig = plan_tp_signal
        await _upsert_order(
            position_uid=position_uid,
            kind="tp",
            level=lvl_sig,
            exchange="BYBIT",
            symbol=symbol,
            side=None,
            otype=None,
            tif="GTC",
            reduce_only=True,
            price=None,
            trigger_price=None,
            qty=qty_sig,
            order_link_id=link_id_sig,
            ext_status="virtual",
            qty_raw=qty_sig,
            price_raw=None,
            calc_type="signal",
            calc_value=None,
            base_price=avg_fill_price,
            base_kind="fill",
            activation_tp_level=None,
            trigger_by=None,
            supersedes_link_id=None,
        )

    # SL первичный + SL-после-TP (заготовки)
    if plan_sl_primary is not None:
        trig, qty, link_id = plan_sl_primary
        await _upsert_order(
            position_uid=position_uid,
            kind="sl",
            level=None,
            exchange="BYBIT",
            symbol=symbol,
            side=_side_word(_opposite(direction)),
            otype="stop_market",
            tif="GTC",
            reduce_only=True,
            price=None,
            trigger_price=trig,
            qty=qty,
            order_link_id=link_id,
            ext_status=("planned" if TRADER_ORDER_MODE == "on" else "virtual"),
            qty_raw=qty,
            price_raw=None,
            calc_type=policy.get("sl", {}).get("type"),
            calc_value=_as_decimal(policy.get("sl", {}).get("value")),
            base_price=avg_fill_price,
            base_kind="fill",
            activation_tp_level=None,
            trigger_by=DEFAULT_TRIGGER_BY,
            supersedes_link_id=None,
        )
    for lvl, trig, qty, link_id in plan_sls_after_tp:
        mode = _sl_mode(policy, lvl)
        await _upsert_order(
            position_uid=position_uid,
            kind="sl",
            level=None,
            exchange="BYBIT",
            symbol=symbol,
            side=_side_word(_opposite(direction)),
            otype=None,
            tif="GTC",
            reduce_only=True,
            price=None,
            trigger_price=trig,
            qty=qty,
            order_link_id=link_id,
            ext_status="virtual",
            qty_raw=qty,
            price_raw=None,
            calc_type=_calc_type_for_sl_mode(mode),
            calc_value=_sl_value(policy, lvl) if mode in ("percent", "atr") else None,
            base_price=avg_fill_price,
            base_kind="fill",
            activation_tp_level=lvl,
            trigger_by=DEFAULT_TRIGGER_BY,
            supersedes_link_id=None,
        )

    # 🔸 Сабмит реальных ордеров (ON): первичный SL + ценовые TP (после гейта)
    if TRADER_ORDER_MODE == "on" and API_KEY and API_SECRET:
        if plan_sl_primary is not None:
            trig, qty, link_id = plan_sl_primary
            ok_s, oid_s, rc_s, rm_s = await _submit_sl(
                symbol=symbol,
                side=_to_title_side(_side_word(_opposite(direction))),
                trigger_price=trig,
                qty=qty,
                link_id=link_id,
                trigger_direction=_calc_trigger_direction(direction),
            )
            await _mark_order_after_submit(order_link_id=link_id, ok=ok_s, order_id=oid_s, retcode=rc_s, retmsg=rm_s)

        for lvl, price, qty, link_id in plan_tp:
            ok_t, oid_t, rc_t, rm_t = await _submit_tp(
                symbol=symbol,
                side=_to_title_side(_side_word(_opposite(direction))),
                price=price,
                qty=qty,
                link_id=link_id,
            )
            await _mark_order_after_submit(order_link_id=link_id, ok=ok_t, order_id=oid_t, retcode=rc_t, retmsg=rm_t)
    else:
        log.debug("[DRY_RUN] placed: primary SL and priced TPs planned (uid=%s)", position_uid)

# 🔸 Нормализация политики: уровни → int (после JSON)
def _normalize_policy_inplace(policy: Dict[str, Any]) -> None:
    if isinstance(policy.get("tp_levels"), list):
        for t in policy["tp_levels"]:
            try:
                t["level"] = int(t.get("level"))
            except Exception:
                pass
    by_level = policy.get("tp_sl_by_level")
    if isinstance(by_level, dict):
        converted: Dict[int, Any] = {}
        for k, v in by_level.items():
            try:
                converted[int(k)] = v
            except Exception:
                continue
        policy["tp_sl_by_level"] = converted

# 🔸 Маппер calc_type для SL-после-TP
def _calc_type_for_sl_mode(mode: Optional[str]) -> Optional[str]:
    if mode == "percent":
        return "percent"
    if mode == "atr":
        return "atr"
    if mode == "entry":
        return "manual"
    return None

# 🔸 Вспомогательные вычисления плана TP/SL
def _build_plan_from_policy(
    *,
    position_uid: str,
    symbol: str,
    direction: str,
    avg_fill: Decimal,
    filled_qty: Decimal,
    precision_qty: Optional[int],
    min_qty: Optional[Decimal],
    ticksize: Optional[Decimal],
    policy: Dict[str, Any],
) -> Tuple[
    List[Tuple[int, Decimal, Decimal, str]],
    Optional[Tuple[int, Decimal, str]],
    Optional[Tuple[Decimal, Decimal, str]],
    List[Tuple[int, Decimal, Decimal, str]]
]:
    tp_levels = list(policy.get("tp_levels") or [])
    tp_levels.sort(key=lambda x: int(x.get("level", 0)))

    priced_tps = [t for t in tp_levels if (t.get("tp_type") in ("percent", "atr"))]
    signal_tps = [t for t in tp_levels if (t.get("tp_type") == "signal")]
    lvl_signal = int(signal_tps[0]["level"]) if signal_tps else None

    tp_plan: List[Tuple[int, Decimal, Decimal, str]] = []
    sum_priced_qty = Decimal("0")

    for i, t in enumerate(priced_tps):
        lvl = int(t["level"])
        vol_pct = _as_decimal(t.get("volume_percent")) or Decimal("0")
        target_qty = (filled_qty * vol_pct / Decimal("100"))
        target_qty = _round_qty(target_qty, precision_qty)
        if i == len(priced_tps) - 1 and (sum_priced_qty + target_qty) > filled_qty:
            target_qty = filled_qty - sum_priced_qty
            target_qty = _round_qty(target_qty, precision_qty)
        if min_qty is not None and target_qty < min_qty:
            target_qty = Decimal("0")
        if target_qty > 0:
            price = _compute_tp_price_from_policy(avg_fill, direction, t.get("tp_type"), _as_decimal(t.get("tp_value")), ticksize)
            link_id = f"{position_uid}-tp-{lvl}"
            tp_plan.append((lvl, price, target_qty, link_id))
            sum_priced_qty += target_qty

    tp_signal: Optional[Tuple[int, Decimal, str]] = None
    if lvl_signal is not None:
        qty_sig = filled_qty - sum_priced_qty
        qty_sig = _round_qty(qty_sig, precision_qty)
        if qty_sig < 0:
            qty_sig = Decimal("0")
        link_sig = f"{position_uid}-tp-{lvl_signal}-signal"
        tp_signal = (lvl_signal, qty_sig, link_sig)

    sl_base = policy.get("sl") or {}
    sl_primary_price = _compute_sl_from_policy(avg_fill, direction, sl_base.get("type"), _as_decimal(sl_base.get("value")), ticksize)
    sl_primary: Optional[Tuple[Decimal, Decimal, str]] = None
    if sl_primary_price is not None:
        sl_primary = (sl_primary_price, _round_qty(filled_qty, precision_qty), f"{position_uid}-sl")

    sl_after: List[Tuple[int, Decimal, Decimal, str]] = []
    for t in tp_levels:
        lvl = int(t["level"])
        mode = _sl_mode(policy, lvl)
        if not mode or mode == "none":
            continue
        qty_left = _qty_left_after_level_from_plan(filled_qty, tp_plan, lvl, precision_qty)
        if min_qty is not None and qty_left < min_qty:
            continue
        price = _compute_sl_after_tp(avg_fill, direction, mode, _sl_value(policy, lvl), ticksize)
        link = f"{position_uid}-sl-after-tp-{lvl}"
        sl_after.append((lvl, price, qty_left, link))

    return tp_plan, tp_signal, sl_primary, sl_after


def _compute_tp_price_from_policy(
    avg_fill: Decimal,
    direction: str,
    tp_type: Optional[str],
    tp_value: Optional[Decimal],
    ticksize: Optional[Decimal],
) -> Decimal:
    base = avg_fill
    if tp_type == "percent" and tp_value is not None:
        if direction == "long":
            price = base * (Decimal("1") + tp_value / Decimal("100"))
        else:
            price = base * (Decimal("1") - tp_value / Decimal("100"))
    elif tp_type == "atr":
        price = base  # заглушка для atr-цены
    else:
        price = base
    return _round_price(price, ticksize)


def _compute_sl_from_policy(
    avg_fill: Decimal,
    direction: str,
    sl_type: Optional[str],
    sl_value: Optional[Decimal],
    ticksize: Optional[Decimal],
) -> Optional[Decimal]:
    if sl_type is None or sl_value is None:
        return None
    base = avg_fill
    if sl_type == "percent":
        if direction == "long":
            price = base * (Decimal("1") - sl_value / Decimal("100"))
        else:
            price = base * (Decimal("1") + sl_value / Decimal("100"))
    elif sl_type == "atr":
        price = base  # заглушка для atr-цены
    else:
        return None
    return _round_price(price, ticksize)


def _compute_sl_after_tp(
    avg_fill: Decimal,
    direction: str,
    sl_mode: str,
    sl_value: Optional[Decimal],
    ticksize: Optional[Decimal],
) -> Decimal:
    if sl_mode == "entry" or sl_mode == "atr":
        price = avg_fill
    elif sl_mode == "percent" and sl_value is not None:
        if direction == "long":
            price = avg_fill * (Decimal("1") - sl_value / Decimal("100"))
        else:
            price = avg_fill * (Decimal("1") + sl_value / Decimal("100"))
    else:
        price = avg_fill
    return _round_price(price, ticksize)


# условия достаточности: остаток после TP<=level по ФАКТИЧЕСКОМУ плану (учитывая квантование qty)
def _qty_left_after_level_from_plan(
    filled_qty: Decimal,
    tp_plan: List[Tuple[int, Decimal, Decimal, str]],
    level: int,
    precision_qty: Optional[int],
) -> Decimal:
    used = Decimal("0")
    for lvl, _price, qty, _link in tp_plan:
        if int(lvl) <= int(level):
            used += qty
    left = filled_qty - used
    return _round_qty(left if left > 0 else Decimal("0"), precision_qty)


def _tp_is_percent(policy: Dict[str, Any], level: int) -> bool:
    for t in (policy.get("tp_levels") or []):
        if int(t.get("level", -1)) == level:
            return (t.get("tp_type") == "percent")
    return False


def _tp_is_atr(policy: Dict[str, Any], level: int) -> bool:
    for t in (policy.get("tp_levels") or []):
        if int(t.get("level", -1)) == level:
            return (t.get("tp_type") == "atr")
    return False


def _tp_value(policy: Dict[str, Any], level: int) -> Optional[Decimal]:
    for t in (policy.get("tp_levels") or []):
        if int(t.get("level", -1)) == level:
            return _as_decimal(t.get("tp_value"))
    return None


def _sl_mode(policy: Dict[str, Any], level: int) -> Optional[str]:
    by_level = policy.get("tp_sl_by_level") or {}
    v = by_level.get(level)
    return v.get("sl_mode") if isinstance(v, dict) else None


def _sl_value(policy: Dict[str, Any], level: int) -> Optional[Decimal]:
    by_level = policy.get("tp_sl_by_level") or {}
    v = by_level.get(level)
    return _as_decimal(v.get("sl_value")) if isinstance(v, dict) else None

# 🔸 Ожидание стабильного/завершённого fill для entry (единый гейт)
async def _wait_entry_fill_or_fallback(
    position_uid: str,
    entry_link_id: str,
    symbol: str,
    entry_price_mark: Optional[Decimal],
    qty_planned: Decimal,
    precision_qty: Optional[int]
) -> Tuple[Optional[Decimal], Optional[Decimal]]:
    # ON: ждём по правилам гейта; DRY_RUN: возвращаем mark + план
    if TRADER_ORDER_MODE == "on":
        deadline = datetime.utcnow() + timedelta(seconds=ENTRY_FILL_MAX_WAIT_SEC)
        first_seen = False
        last_filled = Decimal("0")
        stable_since: Optional[datetime] = None

        while datetime.utcnow() < deadline:
            row = await infra.pg_pool.fetchrow(
                """
                SELECT filled_qty, avg_fill_price, ext_status
                FROM public.trader_position_orders
                WHERE order_link_id = $1
                """,
                entry_link_id
            )
            if row:
                fq = _as_decimal(row["filled_qty"]) or Decimal("0")
                ap = _as_decimal(row["avg_fill_price"])
                st = (row["ext_status"] or "").strip().lower() if row["ext_status"] is not None else ""

                # признак «первый fill увидели»
                if fq > 0 and not first_seen:
                    first_seen = True
                    stable_since = datetime.utcnow()
                    last_filled = fq

                # обновление «окна стабильности»
                if fq != last_filled:
                    last_filled = fq
                    stable_since = datetime.utcnow()

                # условия завершённости
                if st == "filled":
                    return ap, _round_qty(fq, precision_qty)
                if fq >= (qty_planned * ENTRY_FILL_COMPLETENESS):
                    return ap, _round_qty(fq, precision_qty)
                if first_seen and stable_since and (datetime.utcnow() - stable_since).total_seconds() * 1000 >= ENTRY_FILL_STABLE_MS and fq > 0:
                    return ap, _round_qty(fq, precision_qty)

            await asyncio.sleep(0.25)  # короткий poll

        # дедлайн: если набрали хоть что-то — работаем с тем, что есть
        if row:
            fq = _as_decimal(row["filled_qty"]) or Decimal("0")
            ap = _as_decimal(row["avg_fill_price"])
            if fq > 0 and ap and ap > 0:
                return ap, _round_qty(fq, precision_qty)
        # совсем ничего нет — прерываем
        return None, None

    # DRY_RUN: ориентируемся на mark и плановый объём
    if entry_price_mark is None:
        entry_price_mark = await _fetch_mark_price(symbol)
    return entry_price_mark, _round_qty(qty_planned, precision_qty)

# 🔸 Сабмиты: entry / TP / SL
async def _submit_entry(*, symbol: str, side: str, qty: Decimal, link_id: str) -> Tuple[bool, Optional[str], Optional[int], Optional[str]]:
    body = {
        "category": CATEGORY,
        "symbol": symbol,
        "side": side,  # "Buy" | "Sell"
        "orderType": "Market",
        "qty": _str_qty(qty),
        "timeInForce": "GTC",
        "reduceOnly": False,
        "orderLinkId": link_id,
    }
    resp = await _bybit_post("/v5/order/create", body)
    rc, rm = resp.get("retCode"), resp.get("retMsg")
    oid = _extract_order_id(resp); ok = (rc == 0)
    log.debug("submit entry: %s %s qty=%s linkId=%s → rc=%s msg=%s oid=%s", side, symbol, _str_qty(qty), link_id, rc, rm, oid)
    return ok, oid, rc, rm

async def _submit_tp(*, symbol: str, side: str, price: Decimal, qty: Decimal, link_id: str) -> Tuple[bool, Optional[str], Optional[int], Optional[str]]:
    body = {
        "category": CATEGORY,
        "symbol": symbol,
        "side": side,  # "Buy" | "Sell"
        "orderType": "Limit",
        "price": _str_price(price),
        "qty": _str_qty(qty),
        "timeInForce": "GTC",
        "reduceOnly": True,
        "orderLinkId": link_id,
    }
    resp = await _bybit_post("/v5/order/create", body)
    rc, rm = resp.get("retCode"), resp.get("retMsg")
    oid = _extract_order_id(resp); ok = (rc == 0)
    log.debug("submit tp: %s %s price=%s qty=%s linkId=%s → rc=%s msg=%s oid=%s", side, symbol, _str_price(price), _str_qty(qty), link_id, rc, rm, oid)
    return ok, oid, rc, rm

async def _submit_sl(
    *,
    symbol: str,
    side: str,                     # "Buy" | "Sell"
    trigger_price: Decimal,
    qty: Decimal,
    link_id: str,
    trigger_direction: int,        # 1=rise, 2=fall
) -> Tuple[bool, Optional[str], Optional[int], Optional[str]]:
    body = {
        "category": CATEGORY,
        "symbol": symbol,
        "side": side,
        "orderType": "Market",
        "qty": _str_qty(qty),
        "reduceOnly": True,
        "triggerPrice": _str_price(trigger_price),
        "triggerDirection": trigger_direction,
        "triggerBy": DEFAULT_TRIGGER_BY,
        "closeOnTrigger": True,
        "timeInForce": "GTC",
        "orderLinkId": link_id,
    }
    resp = await _bybit_post("/v5/order/create", body)
    rc, rm = resp.get("retCode"), resp.get("retMsg")
    oid = _extract_order_id(resp); ok = (rc == 0)
    log.debug("submit sl: %s trigger=%s dir=%s qty=%s linkId=%s → rc=%s msg=%s oid=%s",
             symbol, _str_price(trigger_price), trigger_direction, _str_qty(qty), link_id, rc, rm, oid)
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

# 🔸 Вспомогательные: чтение/запись в БД и утилиты
async def _upsert_order(
    *,
    position_uid: str,
    kind: str,
    level: Optional[int],
    exchange: str,
    symbol: str,
    side: Optional[str],
    otype: Optional[str],
    tif: str,
    reduce_only: bool,
    price: Optional[Decimal],
    trigger_price: Optional[Decimal],
    qty: Decimal,
    order_link_id: str,
    ext_status: str,
    qty_raw: Optional[Decimal],
    price_raw: Optional[Decimal],
    calc_type: Optional[str],
    calc_value: Optional[Decimal],
    base_price: Optional[Decimal],
    base_kind: Optional[str],
    activation_tp_level: Optional[int],
    trigger_by: Optional[str],
    supersedes_link_id: Optional[str],
) -> None:
    side_norm = None if side is None else side.upper()
    otype_norm = None if otype is None else otype.lower()

    await infra.pg_pool.execute(
        """
        INSERT INTO public.trader_position_orders (
            position_uid, kind, level, exchange, symbol, side, "type", tif, reduce_only,
            price, trigger_price, qty, order_link_id, ext_status,
            qty_raw, price_raw,
            calc_type, calc_value, base_price, base_kind, activation_tp_level, trigger_by, supersedes_link_id
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,
                $10,$11,$12,$13,$14,
                $15,$16,
                $17,$18,$19,$20,$21,$22,$23)
        ON CONFLICT (order_link_id) DO UPDATE SET
            position_uid        = EXCLUDED.position_uid,
            kind                = EXCLUDED.kind,
            level               = EXCLUDED.level,
            exchange            = EXCLUDED.exchange,
            symbol              = EXCLUDED.symbol,
            side                = EXCLUDED.side,
            "type"              = EXCLUDED."type",
            tif                 = EXCLUDED.tif,
            reduce_only         = EXCLUDED.reduce_only,
            price               = EXCLUDED.price,
            trigger_price       = EXCLUDED.trigger_price,
            qty                 = EXCLUDED.qty,
            ext_status          = EXCLUDED.ext_status,
            qty_raw             = EXCLUDED.qty_raw,
            price_raw           = EXCLUDED.price_raw,
            calc_type           = EXCLUDED.calc_type,
            calc_value          = EXCLUDED.calc_value,
            base_price          = EXCLUDED.base_price,
            base_kind           = EXCLUDED.base_kind,
            activation_tp_level = EXCLUDED.activation_tp_level,
            trigger_by          = EXCLUDED.trigger_by,
            supersedes_link_id  = EXCLUDED.supersedes_link_id,
            error_last          = NULL
        """,
        position_uid, kind, level, exchange, symbol, side_norm, otype_norm, tif, reduce_only,
        price, trigger_price, qty, order_link_id, ext_status,
        qty_raw, price_raw,
        calc_type, calc_value, base_price, base_kind, activation_tp_level, trigger_by, supersedes_link_id
    )

async def _try_fetch_initials_from_positions_v4(position_uid: str) -> Tuple[Optional[Decimal], Optional[Decimal]]:
    row = await infra.pg_pool.fetchrow(
        "SELECT quantity, entry_price FROM public.positions_v4 WHERE position_uid = $1",
        position_uid
    )
    if not row:
        return None, None
    return _as_decimal(row["quantity"]), _as_decimal(row["entry_price"])

async def _fetch_mark_price(symbol: str) -> Optional[Decimal]:
    try:
        v = await infra.redis_client.get(f"bb:price:{symbol}")
        return _as_decimal(v)
    except Exception:
        return None

def _parse_policy_json(s: Optional[str]) -> Dict[str, Any]:
    if not s:
        return {}
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}

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

# 🔸 Preflight: установка плеча на бирже перед entry (ON-режим)
async def _preflight_set_leverage(symbol: str, lev: Optional[Decimal]) -> None:
    if TRADER_ORDER_MODE != "on" or not API_KEY or not API_SECRET:
        return
    try:
        if lev is None:
            return
        lev_int = int(lev) if not isinstance(lev, int) else lev
        if lev_int <= 0:
            return
        body = {
            "category": CATEGORY,
            "symbol": symbol,
            "buyLeverage": str(lev_int),
            "sellLeverage": str(lev_int),
        }
        resp = await _bybit_post("/v5/position/set-leverage", body)
        rc, rm = resp.get("retCode"), resp.get("retMsg")
        log.debug("[PREFLIGHT] set-leverage %s=%s → rc=%s msg=%s", symbol, lev_int, rc, rm)
    except Exception:
        log.exception("[PREFLIGHT] set-leverage failed for %s", symbol)

# 🔸 Reverse-guard helpers
async def _find_open_uid_for_symbol(symbol: str, exclude_uid: str) -> Optional[str]:
    row = await infra.pg_pool.fetchrow(
        """
        SELECT position_uid
        FROM public.trader_positions
        WHERE status='open' AND symbol=$1 AND position_uid <> $2
        ORDER BY id DESC LIMIT 1
        """,
        symbol, exclude_uid
    )
    return _as_str(row["position_uid"]) if row else None

async def _calc_left_qty_for_uid(uid: str) -> Optional[Decimal]:
    row = await infra.pg_pool.fetchrow(
        """
        WITH e AS (
          SELECT COALESCE(MAX(filled_qty),0) AS fq FROM public.trader_position_orders WHERE position_uid=$1 AND kind='entry'
        ),
        t AS (
          SELECT COALESCE(SUM(filled_qty),0) AS fq FROM public.trader_position_orders WHERE position_uid=$1 AND kind='tp'
        ),
        s AS (
          SELECT COALESCE(SUM(filled_qty),0) AS fq FROM public.trader_position_orders WHERE position_uid=$1 AND kind='sl'
        ),
        c AS (
          SELECT COALESCE(SUM(filled_qty),0) AS fq FROM public.trader_position_orders WHERE position_uid=$1 AND kind='close'
        )
        SELECT e.fq - t.fq - s.fq - c.fq AS left_qty FROM e,t,s,c
        """,
        uid
    )
    return _as_decimal(row["left_qty"]) if row else None

async def _wait_until_symbol_flat(symbol: str, exclude_uid: str, timeout_sec: int, poll_ms: int) -> bool:
    deadline = datetime.utcnow() + timedelta(seconds=timeout_sec)
    while datetime.utcnow() < deadline:
        other_uid = await _find_open_uid_for_symbol(symbol, exclude_uid)
        if not other_uid:
            return True  # нет другой открытой позиции по символу
        left = await _calc_left_qty_for_uid(other_uid)
        if left is None or left <= Decimal("0"):
            return True
        await asyncio.sleep(max(poll_ms, 50) / 1000.0)
    return False

# 🔸 Утилиты форматирования/арифметики/направлений
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

def _parse_dt(s: Optional[str]):
    try:
        if not s:
            return None
        return datetime.fromisoformat(s.replace("Z", ""))
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

def _str_qty(q: Decimal) -> str:
    return _fmt(q)

def _str_price(p: Decimal) -> str:
    return _fmt(p)

def _to_title_side(side: str) -> str:
    s = (side or "").upper()
    return "Buy" if s == "BUY" else "Sell"

def _side_word(direction: str) -> str:
    return "BUY" if (direction or "").lower() == "long" else "SELL"

def _opposite(direction: Optional[str]) -> str:
    d = (direction or "").lower()
    return "short" if d == "long" else "long"

def _calc_trigger_direction(position_direction: str) -> int:
    d = (position_direction or "").lower()
    return 2 if d == "long" else 1