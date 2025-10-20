# trader_position_filler.py — последовательная фиксация открытых позиций стратегий с флагом trader_winner
# + TG-уведомление об открытии (с TP/SL) и базовые портфельные ограничения

# 🔸 Импорты
import asyncio
import logging
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, Optional, Tuple, List

from trader_infra import infra
from trader_config import config
from trader_tg_notifier import send_open_notification

# 🔸 Логгер воркера
log = logging.getLogger("TRADER_FILLER")

# 🔸 Константы стрима и Consumer Group (жёстко в коде)
SIGNAL_STREAM = "signal_log_queue"
CG_NAME = "trader_filler_group"
CONSUMER = "trader_filler_1"

# 🔸 Новый стрим для заявок на ордера
ORDER_REQUEST_STREAM = "trader_order_requests"

# 🔸 Параметры ретраев поиска позиции (последовательно)
INITIAL_DELAY_SEC = 3.0      # первая пауза после события "opened"
RETRY_DELAY_SEC = 5.0        # задержка между повторными попытками
MAX_ATTEMPTS = 4             # всего попыток: 1 (после INITIAL_DELAY) + 3 ретрая = 4


# 🔸 Основной цикл воркера (строго последовательно, без параллелизма)
async def run_trader_position_filler_loop():
    redis = infra.redis_client

    try:
        await redis.xgroup_create(SIGNAL_STREAM, CG_NAME, id="$", mkstream=True)
        log.info("📡 Consumer Group создана: %s → %s", SIGNAL_STREAM, CG_NAME)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info("ℹ️ Consumer Group уже существует: %s", CG_NAME)
        else:
            log.exception("❌ Ошибка создания Consumer Group")
            return

    log.info("🚦 TRADER_FILLER запущен (последовательная обработка)")

    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=CG_NAME,
                consumername=CONSUMER,
                streams={SIGNAL_STREAM: ">"},
                count=1,
                block=1000
            )
            if not entries:
                continue

            for _, records in entries:
                for record_id, data in records:
                    try:
                        await _handle_signal_opened(record_id, data)
                    except Exception:
                        log.exception("❌ Ошибка обработки записи (id=%s)", record_id)
                        await redis.xack(SIGNAL_STREAM, CG_NAME, record_id)
                    else:
                        await redis.xack(SIGNAL_STREAM, CG_NAME, record_id)

        except Exception:
            log.exception("❌ Ошибка в основном цикле TRADER_FILLER")
            await asyncio.sleep(2)

# 🔸 Обработка одного сообщения из signal_log_queue (интересует только status='opened')
async def _handle_signal_opened(record_id: str, data: Dict[str, Any]) -> None:
    status = _as_str(data.get("status"))
    if status != "opened":
        return  # обрабатываем только открытия

    # поля события
    strategy_id = _as_int(data.get("strategy_id"))
    position_uid = _as_str(data.get("position_uid"))
    symbol_hint = _as_str(data.get("symbol"))  # может быть пустым — не критично

    if not strategy_id or not position_uid:
        log.info("⚠️ Пропуск записи (неполные данные): id=%s sid=%s uid=%s", record_id, strategy_id, position_uid)
        return

    # проверяем, что стратегия помечена как trader_winner (по кэшу конфигурации)
    if strategy_id not in config.trader_winners:
        log.info("⏭️ Стратегия не помечена trader_winner (sid=%s), пропуск opened uid=%s", strategy_id, position_uid)
        return

    # ждём появления записи в positions_v4 и читаем её (для TG/ордера: direction, entry_price, qty и пр.)
    pos = await _fetch_position_with_retry(position_uid)
    if not pos:
        log.info("⏭️ Не нашли позицию в positions_v4 после ретраев: uid=%s (sid=%s)", position_uid, strategy_id)
        return

    # если позиция уже закрыта к моменту фиксации — пропускаем вставку
    status_db = _as_str(pos.get("status"))
    closed_at_db = pos.get("closed_at")
    if status_db == "closed" or closed_at_db is not None:
        log.info("⏭️ Позиция уже закрыта к моменту фиксации, пропуск uid=%s (sid=%s)", position_uid, strategy_id)
        return

    # исходные данные позиции
    symbol = _as_str(pos["symbol"]) or symbol_hint
    notional_value = _as_decimal(pos["notional_value"]) or Decimal("0")
    created_at = pos["created_at"]  # timestamp из БД (UTC)
    direction = _as_str(pos.get("direction")) or None
    entry_price = _as_decimal(pos.get("entry_price"))

    if not symbol or notional_value <= 0:
        log.info("⚠️ Пустой symbol или notional (symbol=%s, notional=%s) — пропуск uid=%s", symbol, notional_value, position_uid)
        return

    # читаем leverage стратегии из кэша (и проверим, что >0)
    leverage = _get_leverage_from_config(strategy_id)
    if leverage is None or leverage <= 0:
        log.info("⚠️ Некорректное плечо для sid=%s (leverage=%s) — пропуск uid=%s", strategy_id, leverage, position_uid)
        return

    # расчёт использованной маржи
    try:
        margin_used = (notional_value / leverage)
    except (InvalidOperation, ZeroDivisionError):
        log.info("⚠️ Ошибка расчёта маржи (N=%s / L=%s) — пропуск uid=%s", notional_value, leverage, position_uid)
        return

    # вычисляем group_master_id согласно правилам market_mirrow / *_long / *_short (из кэша)
    group_master_id = _resolve_group_master_id_from_config(strategy_id, direction)
    if group_master_id is None:
        log.info("⚠️ Не удалось определить group_master_id для sid=%s (direction=%s) — пропуск uid=%s",
                 strategy_id, direction, position_uid)
        return

    # правило 1: по этому symbol не должно быть открытых сделок
    if await _exists_open_for_symbol(symbol):
        log.info("⛔ По символу %s уже есть открытая запись — пропуск uid=%s", symbol, position_uid)
        return

    # правило 2: суммарная маржа открытых сделок ≤ 95% минимального депозита среди текущих trader_winner (из кэша)
    current_open_margin = await _sum_open_margin()
    min_deposit = config.trader_winners_min_deposit
    if min_deposit is None or min_deposit <= 0:
        log.info("⚠️ Не удалось определить min(deposit) среди trader_winner — пропуск uid=%s", position_uid)
        return

    limit = (Decimal("0.95") * min_deposit)
    if (current_open_margin + margin_used) > limit:
        log.info(
            "⛔ Лимит маржи превышен: open=%s + cand=%s > limit=%s (min_dep=%s) — uid=%s",
            current_open_margin, margin_used, limit, min_deposit, position_uid
        )
        return

    # вставка в trader_positions (идемпотентно по position_uid)
    await _insert_trader_position(
        group_strategy_id=group_master_id,
        strategy_id=strategy_id,
        position_uid=position_uid,
        symbol=symbol,
        margin_used=margin_used,
        created_at=created_at
    )

    log.info(
        "✅ TRADER_FILLER: зафиксирована позиция uid=%s | symbol=%s | sid=%s | group=%s | margin=%s",
        position_uid, symbol, strategy_id, group_master_id, margin_used
    )

    # публикация LEAN-заявки на расчёт/план ордеров для bybit_processor
    await _publish_order_request(
        position_uid=position_uid,
        strategy_id=strategy_id,
        symbol=symbol,
        direction=direction,
        created_at=created_at,
    )

    # тянем TP/SL из position_targets_v4 для TG (если есть)
    try:
        tp_targets, sl_targets = await _fetch_targets_for_position(position_uid)
    except Exception:
        tp_targets, sl_targets = [], []
        log.exception("⚠️ Не удалось получить TP/SL для uid=%s", position_uid)

    # имя стратегии для уведомления — из кэша стратегий
    strategy_name = None
    srow = config.strategies.get(strategy_id)
    if srow and srow.get("name"):
        strategy_name = str(srow.get("name"))
    if not strategy_name:
        strategy_name = f"strategy_{strategy_id}"

    # отправка уведомления в Telegram (стрелки направления; без 🟢/🔴 в заголовке; с TP/SL)
    try:
        await send_open_notification(
            symbol=symbol,
            direction=direction,
            entry_price=entry_price,
            strategy_name=strategy_name,
            created_at=created_at,
            tp_targets=tp_targets,
            sl_targets=sl_targets,
        )
    except Exception:
        log.exception("❌ TG: ошибка отправки уведомления об открытии uid=%s", position_uid)

# 🔸 Публикация заявки в шину ордеров (LEAN-пейлоад)
async def _publish_order_request(
    *,
    position_uid: str,
    strategy_id: int,
    symbol: str,
    direction: Optional[str],
    created_at
) -> None:
    redis = infra.redis_client
    try:
        # все значения в строках (Redis decode_responses=True)
        fields = {
            "position_uid": position_uid,
            "strategy_id": str(strategy_id),
            "symbol": symbol or "",
            "direction": (direction or "").lower(),
            "created_at": (created_at.isoformat() + "Z") if hasattr(created_at, "isoformat") else str(created_at or ""),
        }
        await redis.xadd(ORDER_REQUEST_STREAM, fields)
        log.debug("📤 ORDER_REQ: отправлено в %s для uid=%s", ORDER_REQUEST_STREAM, position_uid)
    except Exception:
        log.exception("❌ Не удалось опубликовать заявку ордера uid=%s", position_uid)
        
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

    # заданы оба мастера по направлению (а единый не задан)
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


async def _fetch_position_with_retry(position_uid: str) -> Optional[Dict[str, Any]]:
    # условия достаточности: дождаться строки и убедиться, что она ещё не закрыта
    await asyncio.sleep(INITIAL_DELAY_SEC)
    attempts = 0
    while attempts < MAX_ATTEMPTS:
        row = await infra.pg_pool.fetchrow(
            """
            SELECT symbol, notional_value, created_at, status, closed_at, direction, entry_price
            FROM public.positions_v4
            WHERE position_uid = $1
            """,
            position_uid
        )
        if row:
            return dict(row)

        attempts += 1
        if attempts < MAX_ATTEMPTS:
            await asyncio.sleep(RETRY_DELAY_SEC)
    return None


async def _exists_open_for_symbol(symbol: str) -> bool:
    row = await infra.pg_pool.fetchrow(
        """
        SELECT 1
        FROM public.trader_positions
        WHERE status = 'open' AND symbol = $1
        LIMIT 1
        """,
        symbol
    )
    return row is not None


async def _sum_open_margin() -> Decimal:
    row = await infra.pg_pool.fetchrow(
        "SELECT COALESCE(SUM(margin_used), 0) AS total FROM public.trader_positions WHERE status='open'"
    )
    try:
        return Decimal(str(row["total"])) if row and row["total"] is not None else Decimal("0")
    except Exception:
        return Decimal("0")


async def _insert_trader_position(
    group_strategy_id: int,
    strategy_id: int,
    position_uid: str,
    symbol: str,
    margin_used: Decimal,
    created_at
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


# 🔸 Получение TP/SL целей для позиции (для TG)
async def _fetch_targets_for_position(position_uid: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    rows = await infra.pg_pool.fetch(
        """
        SELECT type, level, price, quantity, hit, canceled
        FROM public.position_targets_v4
        WHERE position_uid = $1
        ORDER BY type, level
        """,
        position_uid
    )
    tp_list: List[Dict[str, Any]] = []
    sl_list: List[Dict[str, Any]] = []

    for r in rows:
        obj = {
            "type": r["type"],
            "level": r["level"],
            "price": r["price"],
            "quantity": r["quantity"],
            "hit": r["hit"],
            "canceled": r["canceled"],
        }
        if r["type"] == "tp":
            tp_list.append(obj)
        elif r["type"] == "sl":
            # берём только первый «живой» SL для уведомления, но возвращаем все — форматтер сам покажет 1-й
            sl_list.append(obj)

    return tp_list, sl_list