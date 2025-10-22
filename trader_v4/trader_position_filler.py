# trader_position_filler.py — быстрый якорь позиции и публикация «толстой» заявки в bybit_processor (чтение из positions_bybit_status)

# 🔸 Импорты
import asyncio
import logging
import json
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, Optional
from datetime import datetime

from trader_infra import infra
from trader_config import config

# 🔸 Логгер воркера
log = logging.getLogger("TRADER_FILLER")

# 🔸 Потоки/группы
POSITIONS_STATUS_STREAM = "positions_bybit_status"   # читаем события жизни позиции (в т.ч. opened)
ORDER_REQUEST_STREAM = "trader_order_requests"       # публикуем «толстую» заявку для bybit_processor
CG_NAME = "trader_filler_status_group"
CONSUMER = "trader_filler_status_1"

# 🔸 Параллелизм чтения из стрима
READ_BLOCK_MS = 1000
READ_COUNT = 10
CONCURRENCY = 8


# 🔸 Основной цикл воркера (параллельная обработка без дополнительных проверок)
async def run_trader_position_filler_loop():
    redis = infra.redis_client

    # создаём Consumer Group (id="$" — только новые записи)
    try:
        await redis.xgroup_create(POSITIONS_STATUS_STREAM, CG_NAME, id="$", mkstream=True)
        log.debug("📡 Consumer Group создана: %s → %s", POSITIONS_STATUS_STREAM, CG_NAME)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug("ℹ️ Consumer Group уже существует: %s", CG_NAME)
        else:
            log.exception("❌ Ошибка создания Consumer Group")
            return

    log.info("🚦 TRADER_FILLER v2 запущен (источник=%s, параллелизм=%d)", POSITIONS_STATUS_STREAM, CONCURRENCY)

    sem = asyncio.Semaphore(CONCURRENCY)

    async def _spawn_task(record_id: str, data: Dict[str, Any]):
        # ограничиваем число одновременных задач
        async with sem:
            try:
                await _handle_status_event(record_id, data)
            except Exception:
                log.exception("❌ Ошибка обработки записи (id=%s)", record_id)
            finally:
                # ack в любом случае (идемпотентность ниже обеспечена ON CONFLICT)
                try:
                    await redis.xack(POSITIONS_STATUS_STREAM, CG_NAME, record_id)
                except Exception:
                    log.exception("⚠️ Не удалось ACK запись (id=%s)", record_id)

    # основной цикл чтения
    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=CG_NAME,
                consumername=CONSUMER,
                streams={POSITIONS_STATUS_STREAM: ">"},
                count=READ_COUNT,
                block=READ_BLOCK_MS
            )
            if not entries:
                continue

            tasks = []
            for _, records in entries:
                for record_id, data in records:
                    tasks.append(asyncio.create_task(_spawn_task(record_id, data)))

            # условия достаточности: дождёмся текущей пачки
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

        except Exception:
            log.exception("❌ Ошибка в основном цикле TRADER_FILLER")
            await asyncio.sleep(0.5)


# 🔸 Обработка одного события из positions_bybit_status (интересует только event='opened')
async def _handle_status_event(record_id: str, data: Dict[str, Any]) -> None:
    # извлекаем минимально нужные поля
    event = (_as_str(data.get("event")) or "").lower()
    if event != "opened":
        return  # слушаем только открытия

    position_uid = _as_str(data.get("position_uid"))
    strategy_id = _as_int(data.get("strategy_id"))
    direction = (_as_str(data.get("direction")) or "").lower()
    created_at = _parse_dt(_as_str(data.get("ts"))) or datetime.utcnow()  # ts из информера — как created_at

    if not position_uid or not strategy_id:
        log.debug("⚠️ Пропуск записи (неполные данные): id=%s sid=%s uid=%s", record_id, strategy_id, position_uid)
        return

    # фильтр: обрабатываем только winner-стратегии
    if strategy_id not in config.trader_winners:
        log.debug("⏭️ Стратегия не в trader_winner (sid=%s), пропуск uid=%s", strategy_id, position_uid)
        return

    # плечо: из события или из кэша
    leverage = _as_decimal(data.get("leverage"))
    if leverage is None:
        leverage = _get_leverage_from_config(strategy_id)
    if leverage is None or leverage <= 0:
        log.debug("⚠️ Некорректное плечо sid=%s (leverage=%s) — пропуск uid=%s", strategy_id, leverage, position_uid)
        return

    # margin_used из события (опционально)
    margin_used = _as_decimal(data.get("margin_used"))

    # символ берём из positions_v4 (в этом стриме symbol не передаётся)
    symbol, entry_price_db, qty_left_db, created_at_db = await _fetch_symbol_and_initials_from_positions_v4(position_uid)

    # если margin_used нет в событии — считаем из БД: (quantity_left * entry_price) / leverage
    if margin_used is None and entry_price_db is not None and qty_left_db is not None:
        try:
            margin_used = (qty_left_db * entry_price_db) / leverage
        except (InvalidOperation, ZeroDivisionError):
            margin_used = None

    # created_at: если в БД есть и мы его не получили из ts — можно схлопнуть на БД
    if created_at is None and created_at_db is not None:
        created_at = created_at_db
    if created_at is None:
        created_at = datetime.utcnow()

    if not symbol:
        log.debug("⚠️ Нет symbol по uid=%s (positions_v4 не успела/нет записи) — пропуск", position_uid)
        return
    if margin_used is None:
        log.debug("⚠️ Не удалось рассчитать margin_used (uid=%s) — пропуск", position_uid)
        return

    # мастер-группа (mirrow) по направлению
    group_master_id = _resolve_group_master_id_from_config(strategy_id, direction)
    if group_master_id is None:
        log.debug("⚠️ Не удалось определить group_master_id sid=%s (direction=%s) — пропуск uid=%s", strategy_id, direction, position_uid)
        return

    # идемпотентная вставка якоря позиции у трейдера (FK нужен для последующих ордеров)
    await _insert_trader_position(
        group_strategy_id=group_master_id,
        strategy_id=strategy_id,
        position_uid=position_uid,
        symbol=symbol,
        margin_used=margin_used,
        created_at=created_at
    )

    # собираем «толстую» заявку для bybit_processor
    payload = _build_thick_order_payload(
        position_uid=position_uid,
        strategy_id=strategy_id,
        symbol=symbol,
        direction=direction,
        created_at=created_at,
    )

    # публикуем заявку
    await _publish_order_request(fields=payload)

    log.debug(
        "✅ FILLER: зафиксирована позиция и отправлена заявка | uid=%s | sid=%s | symbol=%s | group=%s | margin=%s",
        position_uid, strategy_id, symbol, group_master_id, _dec_to_str(margin_used)
    )


# 🔸 Построение «толстой» заявки для bybit_processor
def _build_thick_order_payload(*, position_uid: str, strategy_id: int, symbol: str, direction: str, created_at: datetime) -> Dict[str, str]:
    # политика стратегии (SL/TP) из кэша
    policy = config.strategy_policy.get(strategy_id) or {}
    policy_json = json.dumps(policy, ensure_ascii=False, default=_json_default)

    # метаданные стратегии
    meta = config.strategy_meta.get(strategy_id) or {}
    leverage = meta.get("leverage")

    # точности по тикеру (если есть)
    t = config.tickers.get(symbol) or {}
    precision_qty = t.get("precision_qty")
    min_qty = t.get("min_qty")
    ticksize = t.get("ticksize")

    # все значения — строки (Redis Streams)
    fields = {
        "position_uid": position_uid,
        "strategy_id": str(strategy_id),
        "symbol": symbol,
        "direction": (direction or "").lower(),
        "created_at": _to_iso(created_at),
        "leverage": _dec_to_str(leverage),
        "precision_qty": str(precision_qty) if precision_qty is not None else "",
        "min_qty": _dec_to_str(min_qty),
        "ticksize": _dec_to_str(ticksize),
        "policy": policy_json,   # сериализованный JSON политики
    }
    return fields


# 🔸 Публикация заявки в шину ордеров
async def _publish_order_request(*, fields: Dict[str, str]) -> None:
    redis = infra.redis_client
    try:
        await redis.xadd(ORDER_REQUEST_STREAM, fields)
        log.debug("📤 ORDER_REQ: отправлено в %s для uid=%s", ORDER_REQUEST_STREAM, fields.get("position_uid", ""))
    except Exception:
        log.exception("❌ Не удалось опубликовать заявку uid=%s", fields.get("position_uid", ""))


# 🔸 Вспомогательные функции

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

def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    # условия достаточности: ISO8601 'YYYY-mm-ddTHH:MM:SS[.ffffff]'
    try:
        if not s:
            return None
        return datetime.fromisoformat(s.replace("Z", ""))
    except Exception:
        return None

def _dec_to_str(v: Any) -> str:
    # привести Decimal/число к «красивой» строке
    try:
        d = _as_decimal(v)
        if d is None:
            return ""
        s = f"{d:.12f}".rstrip("0").rstrip(".")
        return s if s else "0"
    except Exception:
        return ""

def _to_iso(v: Any) -> str:
    # если объект с isoformat — использовать, иначе str
    try:
        return (v.isoformat() + "Z") if hasattr(v, "isoformat") else (str(v) if v is not None else "")
    except Exception:
        return str(v) if v is not None else ""

def _json_default(obj):
    # сериализация Decimal и прочих нетривиальных типов в JSON
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def _get_leverage_from_config(strategy_id: int) -> Optional[Decimal]:
    meta = config.strategy_meta.get(strategy_id) or {}
    lev = meta.get("leverage")
    try:
        return lev if isinstance(lev, Decimal) else (Decimal(str(lev)) if lev is not None else None)
    except Exception:
        return None

def _resolve_group_master_id_from_config(strategy_id: int, direction: Optional[str]) -> Optional[int]:
    meta = config.strategy_meta.get(strategy_id) or {}
    mm = meta.get("market_mirrow")
    mm_long = meta.get("market_mirrow_long")
    mm_short = meta.get("market_mirrow_short")

    # ничего не задано → мастер = сама стратегия
    if mm is None and mm_long is None and mm_short is None:
        return strategy_id

    # задан единый мастер
    if mm is not None and mm_long is None and mm_short is None:
        try:
            return int(mm)
        except Exception:
            return None

    # заданы мастера по направлению
    if mm is None and mm_long is not None and mm_short is not None:
        d = (direction or "").lower()
        try:
            if d == "long":
                return int(mm_long)
            if d == "short":
                return int(mm_short)
            return None
        except Exception:
            return None

    # иные комбинации считаем некорректными
    return None

async def _fetch_symbol_and_initials_from_positions_v4(position_uid: str) -> tuple[Optional[str], Optional[Decimal], Optional[Decimal], Optional[datetime]]:
    row = await infra.pg_pool.fetchrow(
        """
        SELECT symbol, entry_price, quantity_left, created_at
        FROM public.positions_v4
        WHERE position_uid = $1
        """,
        position_uid
    )
    if not row:
        return None, None, None, None
    symbol = row["symbol"]
    return (
        str(symbol) if symbol is not None else None,
        _as_decimal(row["entry_price"]),
        _as_decimal(row["quantity_left"]),
        row.get("created_at") if hasattr(row, "get") else row["created_at"],
    )

async def _insert_trader_position(
    group_strategy_id: int,
    strategy_id: int,
    position_uid: str,
    symbol: str,
    margin_used: Decimal,
    created_at: datetime
) -> None:
    # идемпотентная вставка (если запись уже есть — не дублируем)
    await infra.pg_pool.execute(
        """
        INSERT INTO public.trader_positions (
          group_strategy_id, strategy_id, position_uid, symbol,
          margin_used, status, pnl, created_at, closed_at
        ) VALUES ($1, $2, $3, $4, $5, 'open', NULL, $6, NULL)
        ON CONFLICT (position_uid) DO NOTHING
        """,
        group_strategy_id, strategy_id, position_uid, symbol, margin_used, created_at
    )