# trader_position_filler.py — последовательная фиксация открытых позиций стратегий с флагом trader_winner
# + TG-уведомление об открытии (с TP/SL) и базовые портфельные ограничения

# 🔸 Импорты
import asyncio
import logging
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, Optional, Tuple, List

from trader_infra import infra
from trader_tg_notifier import send_open_notification

# 🔸 Логгер воркера
log = logging.getLogger("TRADER_FILLER")

# 🔸 Константы стрима и Consumer Group (жёстко в коде)
SIGNAL_STREAM = "signal_log_queue"
CG_NAME = "trader_filler_group"
CONSUMER = "trader_filler_1"

# 🔸 Параметры ретраев поиска позиции (последовательно)
INITIAL_DELAY_SEC = 5.0      # первая пауза после события "opened"
RETRY_DELAY_SEC = 5.0        # задержка между повторными попытками
MAX_ATTEMPTS = 4             # всего попыток: 1 (после INITIAL_DELAY) + 3 ретрая = 4


# 🔸 Основной цикл воркера (строго последовательно, без параллелизма)
async def run_trader_position_filler_loop():
    redis = infra.redis_client

    try:
        await redis.xgroup_create(SIGNAL_STREAM, CG_NAME, id="$", mkstream=True)
        log.debug("📡 Consumer Group создана: %s → %s", SIGNAL_STREAM, CG_NAME)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug("ℹ️ Consumer Group уже существует: %s", CG_NAME)
        else:
            log.exception("❌ Ошибка создания Consumer Group")
            return

    log.debug("🚦 TRADER_FILLER запущен (последовательная обработка)")

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
        log.debug("⚠️ Пропуск записи (неполные данные): id=%s sid=%s uid=%s", record_id, strategy_id, position_uid)
        return

    # проверяем, что стратегия помечена как trader_winner (ручная отметка)
    if not await _is_trader_winner(strategy_id):
        log.debug("⏭️ Стратегия не помечена trader_winner (sid=%s), пропуск opened uid=%s", strategy_id, position_uid)
        return

    # ждём появления записи в positions_v4 и читаем её (для TG: direction, entry_price)
    pos = await _fetch_position_with_retry(position_uid)
    if not pos:
        log.debug("⏭️ Не нашли позицию в positions_v4 после ретраев: uid=%s (sid=%s)", position_uid, strategy_id)
        return

    # если позиция уже закрыта к моменту фиксации — пропускаем вставку
    status_db = _as_str(pos.get("status"))
    closed_at_db = pos.get("closed_at")
    if status_db == "closed" or closed_at_db is not None:
        log.debug("⏭️ Позиция уже закрыта к моменту фиксации, пропуск uid=%s (sid=%s)", position_uid, strategy_id)
        return

    # исходные данные позиции
    symbol = _as_str(pos["symbol"]) or symbol_hint
    notional_value = _as_decimal(pos["notional_value"]) or Decimal("0")
    created_at = pos["created_at"]  # timestamp из БД (UTC)
    direction = _as_str(pos.get("direction")) or None
    entry_price = _as_decimal(pos.get("entry_price"))

    if not symbol or notional_value <= 0:
        log.debug("⚠️ Пустой symbol или notional (symbol=%s, notional=%s) — пропуск uid=%s", symbol, notional_value, position_uid)
        return

    # читаем leverage стратегии (и проверим, что >0)
    leverage = await _fetch_leverage(strategy_id)
    if leverage is None or leverage <= 0:
        log.debug("⚠️ Некорректное плечо для sid=%s (leverage=%s) — пропуск uid=%s", strategy_id, leverage, position_uid)
        return

    # расчёт использованной маржи
    try:
        margin_used = (notional_value / leverage)
    except (InvalidOperation, ZeroDivisionError):
        log.debug("⚠️ Ошибка расчёта маржи (N=%s / L=%s) — пропуск uid=%s", notional_value, leverage, position_uid)
        return

    # вычисляем group_master_id согласно правилам market_mirrow / *_long / *_short
    group_master_id = await _resolve_group_master_id(strategy_id, direction)
    if group_master_id is None:
        log.debug("⚠️ Не удалось определить group_master_id для sid=%s (direction=%s) — пропуск uid=%s",
                  strategy_id, direction, position_uid)
        return

    # правило 1: по этому symbol не должно быть открытых сделок
    if await _exists_open_for_symbol(symbol):
        log.debug("⛔ По символу %s уже есть открытая запись — пропуск uid=%s", symbol, position_uid)
        return

    # правило 2: суммарная маржа открытых сделок ≤ 95% минимального депозита среди текущих trader_winner
    current_open_margin = await _sum_open_margin()
    min_deposit = await _min_deposit_among_winners()
    if min_deposit is None or min_deposit <= 0:
        log.debug("⚠️ Не удалось определить min(deposit) среди trader_winner — пропуск uid=%s", position_uid)
        return

    limit = (Decimal("0.95") * min_deposit)
    if (current_open_margin + margin_used) > limit:
        log.debug(
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

    log.debug(
        "✅ TRADER_FILLER: зафиксирована позиция uid=%s | symbol=%s | sid=%s | group=%s | margin=%s",
        position_uid, symbol, strategy_id, group_master_id, margin_used
    )

    # тянем TP/SL из position_targets_v4 для TG (если есть)
    try:
        tp_targets, sl_targets = await _fetch_targets_for_position(position_uid)
    except Exception:
        tp_targets, sl_targets = [], []
        log.exception("⚠️ Не удалось получить TP/SL для uid=%s", position_uid)

    # имя стратегии для уведомления
    strategy_name = await _fetch_strategy_name(strategy_id) or f"strategy_{strategy_id}"

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


async def _is_trader_winner(strategy_id: int) -> bool:
    row = await infra.pg_pool.fetchrow(
        "SELECT trader_winner FROM public.strategies_v4 WHERE id = $1",
        strategy_id
    )
    if not row or row["trader_winner"] is None:
        return False
    return bool(row["trader_winner"])


async def _resolve_group_master_id(strategy_id: int, direction: Optional[str]) -> Optional[int]:
    row = await infra.pg_pool.fetchrow(
        """
        SELECT market_mirrow, market_mirrow_long, market_mirrow_short
        FROM public.strategies_v4
        WHERE id = $1
        """,
        strategy_id
    )
    if not row:
        return None

    mm = row["market_mirrow"]
    mm_long = row["market_mirrow_long"]
    mm_short = row["market_mirrow_short"]

    # Вариант A: ничего не задано → мастер = сама стратегия
    if mm is None and mm_long is None and mm_short is None:
        return strategy_id

    # Вариант B: задан единый мастер
    if mm is not None and mm_long is None and mm_short is None:
        return int(mm)

    # Вариант C: заданы оба мастера по направлению (а единый не задан)
    if mm is None and mm_long is not None and mm_short is not None:
        d = (direction or "").lower()
        if d == "long":
            return int(mm_long)
        if d == "short":
            return int(mm_short)
        # если направление неизвестно — определить нельзя
        return None

    # Иные комбинации считаем некорректными для разрешения мастера
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


async def _fetch_leverage(strategy_id: int) -> Optional[Decimal]:
    row = await infra.pg_pool.fetchrow(
        "SELECT leverage FROM public.strategies_v4 WHERE id=$1",
        strategy_id
    )
    if not row or row["leverage"] is None:
        return None
    try:
        return Decimal(str(row["leverage"]))
    except Exception:
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


async def _min_deposit_among_winners() -> Optional[Decimal]:
    row = await infra.pg_pool.fetchrow(
        """
        SELECT MIN(deposit) AS min_dep
        FROM public.strategies_v4
        WHERE trader_winner = TRUE
          AND deposit IS NOT NULL
          AND deposit > 0
        """
    )
    if not row or row["min_dep"] is None:
        return None
    try:
        return Decimal(str(row["min_dep"]))
    except Exception:
        return None


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


async def _fetch_strategy_name(strategy_id: Optional[int]) -> Optional[str]:
    if strategy_id is None:
        return None
    row = await infra.pg_pool.fetchrow(
        "SELECT name FROM public.strategies_v4 WHERE id = $1",
        strategy_id
    )
    if not row:
        return None
    name = row["name"]
    return str(name) if name is not None else None