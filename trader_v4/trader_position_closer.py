# trader_position_closer.py — обработчик закрытий: ensure_closed → trader_order_requests + апдейт trader_positions_v4 + обновление trader_signals + POS_RUNTIME (config) + поддержка TRADER_ORDER_MODE

# 🔸 Импорты
import os
import asyncio
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from decimal import Decimal

from trader_infra import infra
from trader_config import config

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

# 🔸 Режим исполнения
ORDER_MODE = os.getenv("TRADER_ORDER_MODE", "on").strip().lower()  # on | off | dry_run


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

    log.info("🚦 TRADER_CLOSER v1 запущен (источник=%s, параллелизм=%d, order_mode=%s)", POSITIONS_STATUS_STREAM, CONCURRENCY, ORDER_MODE)

    sem = asyncio.Semaphore(CONCURRENCY)

    async def _spawn_task(record_id: str, data: Dict[str, Any]):
        # ack только при успешной публикации команды (и, по возможности, апдейте БД)
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
    # базовые поля
    event = (_as_str(data.get("event")) or "").lower()
    position_uid = _as_str(data.get("position_uid"))
    strategy_id  = _as_int(data.get("strategy_id"))
    direction    = (_as_str(data.get("direction")) or "").lower()
    symbol_ev    = _as_str(data.get("symbol"))
    ts_ms_str    = _as_str(data.get("ts_ms"))
    ts_iso       = _as_str(data.get("ts"))
    ts_dt        = _parse_ts(ts_ms_str, ts_iso)

    # чужие события — просто лог и ACK, без апдейта trader_signals
    if not event.startswith("closed"):
        log.info("⏭️ CLOSER: пропуск id=%s (event=%s)", record_id, event or "—")
        return True

    # принятый к обработке
    await _update_trader_signal_status(
        stream_id=record_id, position_uid=position_uid, event=event, ts_iso=ts_iso,
        status="accepted_by_closer", note=f"accepted; order_mode={ORDER_MODE}"
    )

    # валидация минимума
    if not position_uid or not strategy_id or direction not in ("long", "short"):
        await _update_trader_signal_status(
            stream_id=record_id, position_uid=position_uid, event=event, ts_iso=ts_iso,
            status="skipped_invalid_payload", note="missing uid/sid/direction"
        )
        log.info("⏭️ CLOSER: skip invalid payload (id=%s uid=%s sid=%s dir=%s)", record_id, position_uid or "—", strategy_id, direction or "—")
        return True

    # маппинг причины
    reason = _map_close_reason(event)

    # символ из якоря (если не пришёл в событии)
    symbol = symbol_ev or await _fetch_symbol_from_anchor(position_uid)

    # подтянем «виртуальные» итоги из positions_v4 (не критично, если не найдём)
    virt_pnl, virt_exit_price, virt_closed_at, virt_close_reason = await _fetch_virtual_close_snapshot(position_uid)

    # режимы:
    # - on      → status='closing', exchange_status='pending_close'
    # - off/dry → status='closed',  closed_at=virt_closed_at|ts|now(), exchange_status='none', exchange_closed_at=closed_at
    try:
        if ORDER_MODE == "on":
            await _update_trader_position_closing_on(
                position_uid=position_uid,
                reason=reason,
                virt_pnl=virt_pnl,
                virt_exit_price=virt_exit_price,
                virt_closed_at=virt_closed_at,
                virt_close_reason=virt_close_reason
            )
        else:
            closed_at_effective = virt_closed_at or ts_dt or datetime.utcnow()
            await _update_trader_position_closing_off(
                position_uid=position_uid,
                reason=reason,
                closed_at=closed_at_effective,
                virt_pnl=virt_pnl,
                virt_exit_price=virt_exit_price,
                virt_closed_at=virt_closed_at,
                virt_close_reason=virt_close_reason
            )
    except Exception:
        await _update_trader_signal_status(
            stream_id=record_id, position_uid=position_uid, event=event, ts_iso=ts_iso,
            status="failed_db_update", note="update trader_positions_v4 failed"
        )
        log.exception("⚠️ CLOSER: db update failed (uid=%s)", position_uid)

    # формируем и публикуем команду ensure_closed (идемпотентно) — только в режиме on
    if ORDER_MODE == "on":
        order_fields = {
            "cmd": "ensure_closed",
            "position_uid": position_uid,
            "strategy_id": str(strategy_id),
            "symbol": symbol or "",
            "direction": direction,
            "reason": reason,
            "order_link_suffix": "close",
            "ts": ts_iso or "",
            "ts_ms": ts_ms_str or "",
        }
        try:
            await infra.redis_client.xadd(ORDER_REQUEST_STREAM, order_fields)
        except Exception as e:
            await _update_trader_signal_status(
                stream_id=record_id, position_uid=position_uid, event=event, ts_iso=ts_iso,
                status="failed_publish_order_request", note=f"redis xadd error: {e.__class__.__name__}"
            )
            log.exception("❌ Не удалось опубликовать ensure_closed uid=%s", position_uid)
            return False  # без ACK → повтор

        note = f"ensure_closed published; reason={reason}; mode=on"
        await _update_trader_signal_status(
            stream_id=record_id, position_uid=position_uid, event=event, ts_iso=ts_iso,
            status="closer_ensure_closed_published", note=note
        )
    else:
        # в off/dry_run публикации нет — фиксируем финализацию
        note = f"closed virtually; reason={reason}; mode={ORDER_MODE}"
        await _update_trader_signal_status(
            stream_id=record_id, position_uid=position_uid, event=event, ts_iso=ts_iso,
            status="closer_ensure_closed_published", note=note
        )

    # POS_RUNTIME: убираем позицию
    try:
        await config.note_closed(position_uid, ts_dt)
    except Exception:
        log.exception("⚠️ POS_RUNTIME: note_closed не удалось (uid=%s)", position_uid)

    log.info(
        "✅ CLOSER: %s | uid=%s | sid=%s | sym=%s | event=%s | reason=%s",
        ("ensure_closed → sent" if ORDER_MODE == "on" else "virtually closed"),
        position_uid, strategy_id, (symbol or "—"), event, reason
    )
    return True


# 🔸 Вспомогательные функции — обновление trader_signals
async def _update_trader_signal_status(
    *,
    stream_id: Optional[str],
    position_uid: Optional[str],
    event: Optional[str],
    ts_iso: Optional[str],
    status: str,
    note: Optional[str] = None
) -> None:
    try:
        if stream_id:
            res = await infra.pg_pool.execute(
                """
                UPDATE public.trader_signals
                   SET processing_status = $1,
                       processing_note   = $2,
                       processed_at      = now()
                 WHERE stream_id = $3
                """,
                status, (note or ""), stream_id
            )
            if res.startswith("UPDATE") and res.split()[-1] != "0":
                return
        if position_uid and event and ts_iso:
            dt = _parse_ts(None, ts_iso)
            if dt is not None:
                t_from = dt - timedelta(seconds=2)
                t_to   = dt + timedelta(seconds=2)
                await infra.pg_pool.execute(
                    """
                    WITH cand AS (
                        SELECT id
                          FROM public.trader_signals
                         WHERE position_uid = $1
                           AND event = $2
                           AND emitted_ts BETWEEN $3 AND $4
                         ORDER BY id DESC
                         LIMIT 1
                    )
                    UPDATE public.trader_signals s
                       SET processing_status = $5,
                           processing_note   = $6,
                           processed_at      = now()
                      FROM cand
                     WHERE s.id = cand.id
                    """,
                    position_uid, event, t_from, t_to, status, (note or "")
                )
    except Exception:
        log.exception("⚠️ trader_signals update failed (status=%s, uid=%s, ev=%s)", status, position_uid or "—", event or "—")


# 🔸 Вспомогательные функции — работа с БД/полями
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
    return "other"

async def _fetch_symbol_from_anchor(position_uid: str) -> Optional[str]:
    row = await infra.pg_pool.fetchrow(
        "SELECT symbol FROM public.trader_positions_v4 WHERE position_uid = $1",
        position_uid
    )
    return (row["symbol"] if row and row["symbol"] else None)

async def _fetch_virtual_close_snapshot(position_uid: str) -> tuple[Optional[Decimal], Optional[Decimal], Optional[datetime], Optional[str]]:
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

# режим on → системный 'closing', биржа 'pending_close' (плюс патч extras)
async def _update_trader_position_closing_on(
    *,
    position_uid: str,
    reason: str,
    virt_pnl: Optional[Decimal],
    virt_exit_price: Optional[Decimal],
    virt_closed_at: Optional[datetime],
    virt_close_reason: Optional[str]
) -> None:
    parts = []
    if virt_pnl is not None:
        parts.append(f'"virt_pnl": "{virt_pnl}"')
    if virt_exit_price is not None:
        parts.append(f'"virt_exit_price": "{virt_exit_price}"')
    if virt_closed_at is not None:
        parts.append(f'"virt_closed_at": "{virt_closed_at.isoformat()}"')
    if virt_close_reason is not None:
        parts.append(f'"virt_close_reason": "{virt_close_reason}"')
    virt_patch = "{%s}" % (", ".join(parts)) if parts else "{}"

    await infra.pg_pool.execute(
        """
        UPDATE public.trader_positions_v4
           SET status = CASE WHEN status <> 'closed' THEN 'closing' ELSE status END,
               exchange_status = 'pending_close',
               close_reason = COALESCE(close_reason, $2),
               extras = COALESCE(extras, '{}'::jsonb) || $3::jsonb
         WHERE position_uid = $1
        """,
        position_uid, reason, virt_patch
    )

# режим off/dry_run → системный 'closed' сразу, биржа 'none'
async def _update_trader_position_closing_off(
    *,
    position_uid: str,
    reason: str,
    closed_at: datetime,
    virt_pnl: Optional[Decimal],
    virt_exit_price: Optional[Decimal],
    virt_closed_at: Optional[datetime],
    virt_close_reason: Optional[str]
) -> None:
    parts = []
    if virt_pnl is not None:
        parts.append(f'"virt_pnl": "{virt_pnl}"')
    if virt_exit_price is not None:
        parts.append(f'"virt_exit_price": "{virt_exit_price}"')
    if virt_closed_at is not None:
        parts.append(f'"virt_closed_at": "{virt_closed_at.isoformat()}"')
    if virt_close_reason is not None:
        parts.append(f'"virt_close_reason": "{virt_close_reason}"')
    virt_patch = "{%s}" % (", ".join(parts)) if parts else "{}"

    await infra.pg_pool.execute(
        """
        UPDATE public.trader_positions_v4
           SET status = 'closed',
               closed_at = $2,
               exchange_status = 'none',
               exchange_closed_at = $2,
               close_reason = COALESCE(close_reason, $3),
               extras = COALESCE(extras, '{}'::jsonb) || $4::jsonb
         WHERE position_uid = $1
        """,
        position_uid, closed_at, reason, virt_patch
    )