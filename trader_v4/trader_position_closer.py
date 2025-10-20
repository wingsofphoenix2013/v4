# trader_position_closer.py — последовательное закрытие зафиксированных позиций + TG-уведомление
# (портфельные метрики: 24h/TOTAL ROI & Winrate, стратегия по strategies_v4.name)

# 🔸 Импорты
import asyncio
import logging
from decimal import Decimal
from typing import Any, Optional, Tuple

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
        log.debug("📡 Consumer Group создана: %s → %s", SIGNAL_STREAM, CG_NAME)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug("ℹ️ Consumer Group уже существует: %s", CG_NAME)
        else:
            log.exception("❌ Ошибка создания Consumer Group")
            return

    log.debug("🚦 TRADER_CLOSER запущен (последовательная обработка)")

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
        log.debug("⚠️ TRADER_CLOSER: пропуск (нет position_uid) id=%s", record_id)
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
        log.debug("ℹ️ TRADER_CLOSER: позиция не отслеживается, пропуск uid=%s", position_uid)
        return

    # берём финальные поля из positions_v4 (к этому моменту они уже записаны core_io)
    row = await infra.pg_pool.fetchrow(
        """
        SELECT symbol, pnl, closed_at, direction, created_at
        FROM public.positions_v4
        WHERE position_uid = $1
        """,
        position_uid
    )
    if not row:
        log.debug("⚠️ TRADER_CLOSER: не нашли позицию в positions_v4, пропуск uid=%s", position_uid)
        return

    symbol = row["symbol"] or tracked["symbol"] or symbol_hint
    pnl = _as_decimal(row["pnl"])
    closed_at = row["closed_at"]          # UTC timestamp (как в БД)
    direction = _as_str(row.get("direction")) or None
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

    # имя стратегии (ТОЛЬКО strategies_v4.name)
    strategy_name = await _fetch_strategy_name(strategy_id)

    # портфельные метрики для сообщения
    roi_24h, roi_total, wr_24h, wr_total = await _compute_portfolio_metrics()

    log.debug(
        "✅ TRADER_CLOSER: закрыта позиция uid=%s | symbol=%s | sid=%s | pnl=%s",
        position_uid, symbol, strategy_id if strategy_id is not None else "-", pnl
    )

    # уведомление в Telegram (win/loss header + стрелки направления + портфельные метрики + стратегия)
    try:
        await send_closed_notification(
            symbol=symbol,
            direction=direction,
            pnl=pnl,
            strategy_name=strategy_name or f"strategy_{strategy_id}" if strategy_id is not None else "strategy",
            created_at=created_at,
            closed_at=closed_at,
            roi_24h=roi_24h,
            roi_total=roi_total,
            wr_24h=wr_24h,
            wr_total=wr_total,
        )
    except Exception:
        log.exception("❌ TG: ошибка отправки уведомления о закрытии uid=%s", position_uid)


# 🔸 Портфельные метрики: 24h/TOTAL ROI & Winrate (по trader_positions)
async def _compute_portfolio_metrics() -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
    # сумма pnl за 24ч и количество/победы
    r24 = await infra.pg_pool.fetchrow(
        """
        SELECT
          COALESCE(SUM(pnl), 0) AS pnl_sum,
          COUNT(*)               AS cnt,
          SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins
        FROM public.trader_positions
        WHERE status = 'closed'
          AND closed_at >= ((now() at time zone 'UTC') - interval '24 hours')
        """
    )
    pnl_24 = _as_decimal(r24["pnl_sum"]) if r24 else Decimal("0")
    cnt_24 = int(r24["cnt"]) if r24 and r24["cnt"] is not None else 0
    wins_24 = int(r24["wins"]) if r24 and r24["wins"] is not None else 0

    # сумма pnl за всё время и количество/победы
    r_total = await infra.pg_pool.fetchrow(
        """
        SELECT
          COALESCE(SUM(pnl), 0) AS pnl_sum,
          COUNT(*)               AS cnt,
          SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins
        FROM public.trader_positions
        WHERE status = 'closed'
        """
    )
    pnl_total = _as_decimal(r_total["pnl_sum"]) if r_total else Decimal("0")
    cnt_total = int(r_total["cnt"]) if r_total and r_total["cnt"] is not None else 0
    wins_total = int(r_total["wins"]) if r_total and r_total["wins"] is not None else 0

    # средний депозит по стратегиям, присутствующим в trader_positions
    r_dep = await infra.pg_pool.fetchrow(
        """
        SELECT AVG(s.deposit) AS avg_dep
        FROM (
          SELECT DISTINCT strategy_id FROM public.trader_positions
        ) tp
        JOIN public.strategies_v4 s ON s.id = tp.strategy_id
        WHERE s.deposit IS NOT NULL AND s.deposit > 0
        """
    )
    avg_dep = _as_decimal(r_dep["avg_dep"]) if r_dep and r_dep["avg_dep"] is not None else None

    roi_24h = (pnl_24 / avg_dep) if avg_dep and avg_dep > 0 else None
    roi_total = (pnl_total / avg_dep) if avg_dep and avg_dep > 0 else None

    wr24 = (Decimal(wins_24) / Decimal(cnt_24)) if cnt_24 > 0 else None
    wr_total = (Decimal(wins_total) / Decimal(cnt_total)) if cnt_total > 0 else None

    return roi_24h, roi_total, wr24, wr_total


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