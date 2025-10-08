# trader_position_filler.py — последовательная фиксация открытых позиций победителей в trader_positions

# 🔸 Импорты
import asyncio
import logging
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, Optional, Set

from trader_infra import infra
from trader_rating import get_current_group_winners

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
    log_uid = _as_str(data.get("log_uid"))

    if not strategy_id or not position_uid:
        log.info("⚠️ Пропуск записи (неполные данные): id=%s sid=%s uid=%s", record_id, strategy_id, position_uid)
        return

    # актуальный набор победителей — сперва из in-memory, при пустоте одноразовый прогрев из БД
    winners_map = get_current_group_winners() or await _fallback_winners_from_db()
    winners_set: Set[int] = set(winners_map.values())

    if strategy_id not in winners_set:
        log.debug("⏭️ Не победитель (sid=%s), пропуск события opened (uid=%s)", strategy_id, position_uid)
        return

    # находим group_master_id (мастер группы) для этого победителя
    group_master_id = _find_group_master_for_winner(winners_map, strategy_id)
    if group_master_id is None:
        group_master_id = await _fetch_group_master_from_db(strategy_id)
    if group_master_id is None:
        log.info("⚠️ Не удалось определить group_master_id для sid=%s — пропуск uid=%s", strategy_id, position_uid)
        return

    # ждём появления записи в positions_v4 и читаем её (с добавленными полями status/closed_at)
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

    # читаем leverage стратегии (и проверим, что >0)
    leverage = await _fetch_leverage(strategy_id)
    if leverage is None or leverage <= 0:
        log.info("⚠️ Некорректное плечо для sid=%s (leverage=%s) — пропуск uid=%s", strategy_id, leverage, position_uid)
        return

    # исходные данные позиции
    symbol = _as_str(pos["symbol"]) or symbol_hint
    notional_value = _as_decimal(pos["notional_value"]) or Decimal("0")
    created_at = pos["created_at"]  # timestamp из БД (UTC)

    if not symbol or notional_value <= 0:
        log.info("⚠️ Пустой symbol или notional (symbol=%s, notional=%s) — пропуск uid=%s", symbol, notional_value, position_uid)
        return

    # расчёт использованной маржи
    try:
        margin_used = (notional_value / leverage)
    except (InvalidOperation, ZeroDivisionError):
        log.info("⚠️ Ошибка расчёта маржи (N=%s / L=%s) — пропуск uid=%s", notional_value, leverage, position_uid)
        return

    # правило 1: по этому symbol не должно быть открытых сделок
    if await _exists_open_for_symbol(symbol):
        log.info("⛔ По символу %s уже есть открытая запись — пропуск uid=%s", symbol, position_uid)
        return

    # правило 2: суммарная маржа открытых сделок ≤ 95% минимального депозита среди победителей
    current_open_margin = await _sum_open_margin()
    min_deposit = await _min_deposit_among_winners()
    if min_deposit is None or min_deposit <= 0:
        log.info("⚠️ Не удалось определить min(deposit) среди победителей — пропуск uid=%s", position_uid)
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

def _find_group_master_for_winner(winners_map: Dict[int, int], winner_sid: int) -> Optional[int]:
    # winners_map: {group_master_id -> winner_strategy_id}
    for gm, sid in winners_map.items():
        if sid == winner_sid:
            return gm
    return None


async def _fallback_winners_from_db() -> Dict[int, int]:
    rows = await infra.pg_pool.fetch(
        """
        SELECT group_master_id, current_winner_id
        FROM public.trader_rating_active
        WHERE current_winner_id IS NOT NULL
        """
    )
    m = {int(r["group_master_id"]): int(r["current_winner_id"]) for r in rows}
    if m:
        log.info("♻️ Прогрет winners из БД: групп=%d", len(m))
    return m


async def _fetch_group_master_from_db(strategy_id: int) -> Optional[int]:
    row = await infra.pg_pool.fetchrow(
        "SELECT market_mirrow FROM public.strategies_v4 WHERE id=$1",
        strategy_id
    )
    if row and row["market_mirrow"] is not None:
        return int(row["market_mirrow"])
    return None


async def _fetch_position_with_retry(position_uid: str) -> Optional[Dict[str, Any]]:
    # условия достаточности: дождаться строки и убедиться, что она ещё не закрыта
    await asyncio.sleep(INITIAL_DELAY_SEC)
    attempts = 0
    while attempts < MAX_ATTEMPTS:
        row = await infra.pg_pool.fetchrow(
            """
            SELECT symbol, notional_value, created_at, status, closed_at
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
        SELECT MIN(s.deposit) AS min_dep
        FROM public.trader_rating_active tra
        JOIN public.strategies_v4 s ON s.id = tra.current_winner_id
        WHERE tra.current_winner_id IS NOT NULL
          AND s.deposit IS NOT NULL
          AND s.deposit > 0
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