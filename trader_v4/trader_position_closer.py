# trader_position_closer.py — обработчик закрытий: ensure_closed → trader_order_requests + апдейт trader_positions_v4 (виртуальные итоги)

# 🔸 Импорты
import asyncio
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from decimal import Decimal

from trader_infra import infra

# 🔸 Логгер воркера
log = logging.getLogger("TRADER_CLOSER")

# 🔸 Потоки/группы
POSITIONS_STATUS_STREAM = "positions_bybit_status"   # источник: informer (closed.*)
ORDER_REQUEST_STREAM    = "trader_order_requests"    # получатель: bybit_processor (cmd=ensure_closed)
CG_NAME   = "trader_closer_status_group"
CONSUMER  = "trader_closer_status_1"

# 🔸 Параметры чтения/параллелизма
READ_BLOCK_MS = 1000
READ_COUNT    = 10
CONCURRENCY   = 8


# 🔸 Основной цикл воркера
async def run_trader_position_closer_loop():
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

    log.info("🚦 TRADER_CLOSER v1 запущен (источник=%s, параллелизм=%d)", POSITIONS_STATUS_STREAM, CONCURRENCY)

    sem = asyncio.Semaphore(CONCURRENCY)

    async def _spawn_task(record_id: str, data: Dict[str, Any]):
        # ack только при успехе публикации команды (и выполненных апдейтах БД по возможности)
        async with sem:
            ack_ok = False
            try:
                ack_ok = await _handle_closed_event(record_id, data)
            except Exception:
                log.exception("❌ Ошибка обработки записи (id=%s)", record_id)
            finally:
                if ack_ok:
                    try:
                        await redis.xack(POSITIONS_STATUS_STREAM, CG_NAME, record_id)
                    except Exception:
                        log.exception("⚠️ Не удалось ACK запись (id=%s)", record_id)

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

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

        except Exception:
            log.exception("❌ Ошибка в основном цикле TRADER_CLOSER")
            await asyncio.sleep(0.5)


# 🔸 Обработка одного события closed.*
async def _handle_closed_event(record_id: str, data: Dict[str, Any]) -> bool:
    # условия достаточности: event начинается с 'closed'
    event = (_as_str(data.get("event")) or "").lower()
    if not event.startswith("closed"):
        log.info("⏭️ CLOSER: пропуск id=%s (event=%s)", record_id, event or "—")
        return True  # это не наш тип — сразу ACK

    position_uid = _as_str(data.get("position_uid"))
    strategy_id  = _as_int(data.get("strategy_id"))
    direction    = (_as_str(data.get("direction")) or "").lower()
    symbol_ev    = _as_str(data.get("symbol"))  # может отсутствовать в редких старых событиях
    ts_ms_str    = _as_str(data.get("ts_ms"))
    ts_iso       = _as_str(data.get("ts"))

    if not position_uid or not strategy_id or direction not in ("long", "short"):
        log.debug("⚠️ closed.*: неполные базовые поля (id=%s, sid=%s, uid=%s, dir=%s)", record_id, strategy_id, position_uid, direction)
        return False

    # маппинг причины
    reason = _map_close_reason(event)

    # узнаем symbol из якоря, если не пришёл в событии
    symbol = symbol_ev or await _fetch_symbol_from_anchor(position_uid)

    # подтянем виртуальные итоги из positions_v4 (не критично, если не найдём)
    virt_pnl, virt_exit_price, virt_closed_at, virt_close_reason = await _fetch_virtual_close_snapshot(position_uid)

    # попробуем обновить нашу агрегатную таблицу (если якорь есть)
    anchor_exists = await _update_trader_position_closing(
        position_uid=position_uid,
        reason=reason,
        virt_pnl=virt_pnl,
        virt_exit_price=virt_exit_price,
        virt_closed_at=virt_closed_at,
        virt_close_reason=virt_close_reason
    )

    # формируем и публикуем команду ensure_closed в шину заявок (идемпотентность по uid+suffix)
    order_fields = {
        "cmd": "ensure_closed",
        "position_uid": position_uid,
        "strategy_id": str(strategy_id),
        "symbol": symbol or "",  # если не знаем — пусть исполнитель возьмёт из БД
        "direction": direction,
        "reason": reason,
        "order_link_suffix": "close",
        "ts": ts_iso or "",
        "ts_ms": ts_ms_str or "",
    }

    try:
        await infra.redis_client.xadd(ORDER_REQUEST_STREAM, order_fields)
    except Exception:
        log.exception("❌ Не удалось опубликовать ensure_closed uid=%s", position_uid)
        return False

    # информационный лог результата
    log.info(
        "✅ CLOSER: ensure_closed → sent | uid=%s | sid=%s | sym=%s | event=%s | reason=%s | anchor=%s",
        position_uid, strategy_id, (symbol or "—"), event, reason, "yes" if anchor_exists else "no"
    )
    return True


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

def _parse_ts(ts_ms_str: Optional[str], ts_iso: Optional[str]) -> Optional[datetime]:
    # ts_ms приоритетнее; ts_iso допускаем без 'Z'
    try:
        if ts_ms_str:
            ms = int(ts_ms_str)
            return datetime.utcfromtimestamp(ms / 1000.0)
    except Exception:
        pass
    try:
        if ts_iso:
            return datetime.fromisoformat(ts_iso.replace("Z", ""))
    except Exception:
        pass
    return None

def _map_close_reason(event: str) -> str:
    # event: 'closed.tp_full_hit' | 'closed.full_sl_hit' | 'closed.sl_tp_hit' | 'closed.reverse_signal_stop' | 'closed.sl_protect_stop'
    if event == "closed.tp_full_hit":
        return "tp_full_hit"
    if event == "closed.full_sl_hit":
        return "full_sl_hit"
    if event == "closed.sl_tp_hit":
        return "sl_tp_hit"
    if event == "closed.reverse_signal_stop":
        return "reverse_signal_stop"
    if event == "closed.sl_protect_stop":
        return "sl_protect_stop"
    # дефолт
    return "other"

async def _fetch_symbol_from_anchor(position_uid: str) -> Optional[str]:
    row = await infra.pg_pool.fetchrow(
        """
        SELECT symbol
          FROM public.trader_positions_v4
         WHERE position_uid = $1
        """,
        position_uid
    )
    return (row["symbol"] if row and row["symbol"] else None)

async def _fetch_virtual_close_snapshot(position_uid: str) -> tuple[Optional[Decimal], Optional[Decimal], Optional[datetime], Optional[str]]:
    # условия достаточности: к моменту closed запись в positions_v4 должна существовать
    row = await infra.pg_pool.fetchrow(
        """
        SELECT pnl, exit_price, closed_at, close_reason
          FROM public.positions_v4
         WHERE position_uid = $1
        """,
        position_uid
    )
    if not row:
        return None, None, None, None
    return (
        _as_decimal(row["pnl"]),
        _as_decimal(row["exit_price"]),
        row["closed_at"],
        (row["close_reason"] if row["close_reason"] else None),
    )

async def _update_trader_position_closing(
    *,
    position_uid: str,
    reason: str,
    virt_pnl: Optional[Decimal],
    virt_exit_price: Optional[Decimal],
    virt_closed_at: Optional[datetime],
    virt_close_reason: Optional[str]
) -> bool:
    # собираем jsonb-патч с виртуальными итогами
    virt_patch_items = []
    if virt_pnl is not None:
        virt_patch_items.append(f'"virt_pnl": "{virt_pnl}"')
    if virt_exit_price is not None:
        virt_patch_items.append(f'"virt_exit_price": "{virt_exit_price}"')
    if virt_closed_at is not None:
        virt_patch_items.append(f'"virt_closed_at": "{virt_closed_at.isoformat()}"')
    if virt_close_reason is not None:
        virt_patch_items.append(f'"virt_close_reason": "{virt_close_reason}"')
    virt_patch = "{%s}" % (", ".join(virt_patch_items)) if virt_patch_items else "{}"

    # апдейт агрегата (не создаём запись, только обновляем, если якорь есть)
    res = await infra.pg_pool.execute(
        """
        UPDATE public.trader_positions_v4
           SET status = CASE WHEN status <> 'closed' THEN 'closing' ELSE status END,
               close_reason = COALESCE(close_reason, $2),
               extras = COALESCE(extras, '{}'::jsonb) || $3::jsonb
         WHERE position_uid = $1
        """,
        position_uid, reason, virt_patch
    )
    # asyncpg.execute возвращает строку вида "UPDATE <n>"
    return res.startswith("UPDATE") and (res.split()[-1] != "0")