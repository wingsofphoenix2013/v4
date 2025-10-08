# trader_position_closer.py — последовательное закрытие зафиксированных позиций + TG-уведомление о закрытии (с 24h ROI)

# 🔸 Импорты
import asyncio
import logging
from decimal import Decimal
from typing import Any, Optional

from trader_infra import infra
from trader_tg_notifier import send_closed_notification

# 🔸 Логгер воркера
log = logging.getLogger("TRADER_CLOSER")

# 🔸 Константы стрима и Consumer Group (только новые сообщения)
SIGNAL_STREAM = "signal_log_queue"
CG_NAME = "trader_closer_group"
CONSUMER = "trader_closer_1"


# 🔸 Основной цикл воркера (строго последовательно)
async def run_trader_position_closer_loop():
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

    log.info("🚦 TRADER_CLOSER запущен (последовательная обработка)")

    while True:
        try:
            # читаем по одной записи, чтобы исключить гонки
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
                        await _handle_signal_closed(record_id, data)
                    except Exception:
                        log.exception("❌ Ошибка обработки записи (id=%s)", record_id)
                        # ack даже при ошибке, чтобы не зависало в pending
                        await redis.xack(SIGNAL_STREAM, CG_NAME, record_id)
                    else:
                        await redis.xack(SIGNAL_STREAM, CG_NAME, record_id)

        except Exception:
            log.exception("❌ Ошибка в основном цикле TRADER_CLOSER")
            await asyncio.sleep(2)


# 🔸 Обработка одного сообщения (интересует только status='closed')
async def _handle_signal_closed(record_id: str, data: dict) -> None:
    status = _as_str(data.get("status"))
    if status != "closed":
        return  # слушаем только закрытия

    position_uid = _as_str(data.get("position_uid"))
    strategy_id = _as_int(data.get("strategy_id"))
    symbol_hint = _as_str(data.get("symbol"))

    if not position_uid:
        log.info("⚠️ TRADER_CLOSER: пропуск (нет position_uid) id=%s", record_id)
        return

    # проверяем: позиция отслеживается нашим модулем?
    tracked = await infra.pg_pool.fetchrow(
        """
        SELECT id, symbol
        FROM public.trader_positions
        WHERE position_uid = $1
        """,
        position_uid
    )
    if not tracked:
        log.info("ℹ️ TRADER_CLOSER: позиция не отслеживается, пропуск uid=%s", position_uid)
        return

    # берём финальные поля из positions_v4 (к этому моменту они уже записаны core_io)
    row = await infra.pg_pool.fetchrow(
        """
        SELECT symbol, pnl, closed_at, direction, entry_price, exit_price, created_at
        FROM public.positions_v4
        WHERE position_uid = $1
        """,
        position_uid
    )
    if not row:
        log.info("⚠️ TRADER_CLOSER: не нашли позицию в positions_v4, пропуск uid=%s", position_uid)
        return

    symbol = row["symbol"] or tracked["symbol"] or symbol_hint
    pnl = _as_decimal(row["pnl"])
    closed_at = row["closed_at"]          # UTC timestamp (как в БД)
    direction = _as_str(row.get("direction")) or None
    entry_price = _as_decimal(row.get("entry_price"))
    exit_price = _as_decimal(row.get("exit_price"))
    created_at = row.get("created_at")

    # обновляем нашу таблицу
    await infra.pg_pool.execute(
        """
        UPDATE public.trader_positions
        SET status = 'closed',
            pnl = $2,
            closed_at = $3
        WHERE position_uid = $1
        """,
        position_uid, pnl, closed_at
    )

    # считаем скользящий ROI_24h по стратегии (доля, не %)
    roi_24h = None
    if strategy_id is not None:
        agg = await infra.pg_pool.fetchrow(
            """
            SELECT COALESCE(SUM(pnl), 0) AS pnl_sum
            FROM public.positions_v4
            WHERE status = 'closed'
              AND strategy_id = $1
              AND closed_at >= ((now() at time zone 'UTC') - interval '24 hours')
            """,
            strategy_id
        )
        pnl_sum_24 = _as_decimal(agg["pnl_sum"]) if agg else Decimal("0")

        dep_row = await infra.pg_pool.fetchrow(
            "SELECT deposit FROM public.strategies_v4 WHERE id = $1",
            strategy_id
        )
        deposit = _as_decimal(dep_row["deposit"]) if dep_row and dep_row["deposit"] is not None else None

        if deposit and deposit > 0:
            try:
                roi_24h = pnl_sum_24 / deposit
            except Exception:
                roi_24h = None

    log.info(
        "✅ TRADER_CLOSER: закрыта позиция uid=%s | symbol=%s | sid=%s | pnl=%s",
        position_uid, symbol, strategy_id if strategy_id is not None else "-", pnl
    )

    # уведомление в Telegram (🟢/🔴 в заголовке + стрелки направления + 24h ROI)
    try:
        await send_closed_notification(
            symbol=symbol,
            direction=direction,
            entry_price=entry_price,
            exit_price=exit_price,
            pnl=pnl,
            created_at=created_at,
            closed_at=closed_at,
            roi_24h=roi_24h,
        )
    except Exception:
        log.exception("❌ TG: ошибка отправки уведомления о закрытии uid=%s", position_uid)


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