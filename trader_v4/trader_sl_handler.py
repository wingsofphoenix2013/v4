# trader_sl_handler.py — синхронизация SL-protect: по sl_replaced (без TP) отправить команду ensure_sl_at_entry + обновление trader_signals (через централизованный POS_RUNTIME в config)

# 🔸 Импорты
import asyncio
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

from trader_infra import infra
from trader_config import config

# 🔸 Логгер воркера
log = logging.getLogger("TRADER_SL")

# 🔸 Потоки/группы
POSITIONS_STATUS_STREAM = "positions_bybit_status"   # источник событий стратегии
ORDER_REQUEST_STREAM    = "trader_order_requests"    # команды для bybit_processor
CG_NAME   = "trader_sl_cg"
CONSUMER  = "trader_sl_1"

# 🔸 Параметры чтения/параллелизма
READ_BLOCK_MS = 1000
READ_COUNT    = 10
CONCURRENCY   = 8

# 🔸 Настройки гейта (debounce)
SL_DEBOUNCE_MS = 300  # короткая задержка, чтобы «догнаться» возможному tp_hit


# 🔸 Основной цикл воркера
async def run_trader_sl_handler_loop():
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

    log.info("🚦 TRADER_SL v1 запущен (источник=%s, параллелизм=%d)", POSITIONS_STATUS_STREAM, CONCURRENCY)

    sem = asyncio.Semaphore(CONCURRENCY)

    async def _spawn_task(record_id: str, data: Dict[str, Any]):
        # ack сразу для opened/tp_hit/closed; для sl_replaced — по результату публикации
        async with sem:
            try:
                ack_ok = await _handle_status_event(record_id, data)
            except Exception:
                log.exception("❌ Ошибка обработки записи (id=%s)", record_id)
                ack_ok = False
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
            log.exception("❌ Ошибка в основном цикле TRADER_SL")
            await asyncio.sleep(0.5)


# 🔸 Обработка события из positions_bybit_status
async def _handle_status_event(record_id: str, data: Dict[str, Any]) -> bool:
    event = (_as_str(data.get("event")) or "").lower()

    # базовые поля для всех типов
    position_uid = _as_str(data.get("position_uid"))
    strategy_id  = _as_int(data.get("strategy_id"))
    symbol_ev    = _as_str(data.get("symbol"))
    direction    = (_as_str(data.get("direction")) or "").lower()
    ts_ms_str    = _as_str(data.get("ts_ms"))
    ts_iso       = _as_str(data.get("ts"))
    ts_dt        = _parse_ts(ts_ms_str, ts_iso)

    # opened v2 → обновляем централизованный POS_RUNTIME
    if event == "opened":
        if position_uid and strategy_id and direction in ("long", "short"):
            await config.note_opened(position_uid, strategy_id, symbol_ev or "", direction, ts_dt)
            log.info("ℹ️ SL_SYNC: opened runtime updated | uid=%s | sym=%s | dir=%s", position_uid or "—", symbol_ev or "—", direction or "—")
        else:
            log.info("⏭️ SL_SYNC: opened skip (invalid base fields) | uid=%s | sid=%s | dir=%s",
                     position_uid or "—", strategy_id, direction or "—")
        return True

    # tp_hit → отмечаем факт TP после open (для гейта)
    if event == "tp_hit":
        if position_uid:
            await config.note_tp_hit(position_uid, ts_dt)
            log.info("ℹ️ SL_SYNC: tp marker set | uid=%s", position_uid or "—")
        return True

    # closed.* → удаляем из POS_RUNTIME
    if event.startswith("closed"):
        if position_uid:
            await config.note_closed(position_uid, ts_dt)
            log.info("ℹ️ SL_SYNC: closed runtime cleared | uid=%s", position_uid or "—")
        return True

    # интересует только sl_replaced
    if event != "sl_replaced":
        log.info("⏭️ SL_SYNC: skip (event=%s)", event or "—")
        return True

    # помечаем принятие sl_replaced (только это событие SL-воркер отражает в trader_signals)
    await _update_trader_signal_status(
        stream_id=record_id, position_uid=position_uid, event="sl_replaced", ts_iso=ts_iso,
        status="accepted_by_sl_handler", note="accepted"
    )

    # debounce: подождём чуть-чуть, вдруг почти одновременно прилетит tp_hit
    if SL_DEBOUNCE_MS > 0:
        await asyncio.sleep(SL_DEBOUNCE_MS / 1000.0)

    # решаем по централизованному состоянию: был ли TP после open
    had_tp = await config.had_tp_since_open(position_uid) if position_uid else False
    if had_tp:
        await _update_trader_signal_status(
            stream_id=record_id, position_uid=position_uid, event="sl_replaced", ts_iso=ts_iso,
            status="skipped_tp_policy", note="had_tp=true"
        )
        log.info("⏭️ SL_SYNC: skip (tp_policy) | uid=%s", position_uid or "—")
        return True

    # это SL-protect → отправляем команду ensure_sl_at_entry (без цен/объёмов; решит шлюз)
    # для символа приоритет: POS_RUNTIME → событие → пусто
    snap = await config.get_position(position_uid) if position_uid else None
    symbol = (snap.symbol if snap else None) or symbol_ev or ""

    order_fields = {
        "cmd": "ensure_sl_at_entry",
        "position_uid": position_uid or "",
        "strategy_id": str(strategy_id) if strategy_id is not None else "",
        "symbol": symbol,
        "direction": direction or "",
        "order_link_suffix": "sl_entry",
        "ts": ts_iso or "",
        "ts_ms": ts_ms_str or "",
    }

    try:
        await infra.redis_client.xadd(ORDER_REQUEST_STREAM, order_fields)
    except Exception as e:
        await _update_trader_signal_status(
            stream_id=record_id, position_uid=position_uid, event="sl_replaced", ts_iso=ts_iso,
            status="failed_publish_order_request", note=f"redis xadd error: {e.__class__.__name__}"
        )
        log.exception("❌ SL_SYNC: публикация ensure_sl_at_entry не удалась | uid=%s", position_uid or "—")
        return False  # не ACK → повтор

    # успех
    await _update_trader_signal_status(
        stream_id=record_id, position_uid=position_uid, event="sl_replaced", ts_iso=ts_iso,
        status="sl_ensure_sl_at_entry_published", note=f"debounce_ms={SL_DEBOUNCE_MS}"
    )
    log.info(
        "✅ SL_SYNC: ensure_sl_at_entry → sent | uid=%s | sid=%s | sym=%s | dir=%s",
        position_uid or "—", (strategy_id if strategy_id is not None else "—"),
        (symbol or "—"), (direction or "—")
    )
    return True


# 🔸 Апдейты public.trader_signals (stream_id → fallback по uid/event/ts)
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
        # попытка 1: по stream_id
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
                return  # обновили успешно

        # попытка 2: по (uid, event, emitted_ts ~ ts_iso ± 2s)
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