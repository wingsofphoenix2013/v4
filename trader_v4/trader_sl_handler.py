# trader_sl_handler.py — синхронизация SL-protect: по sl_replaced (без TP) отправить команду ensure_sl_at_entry в шлюз

# 🔸 Импорты
import asyncio
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

from trader_infra import infra

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

# 🔸 In-memory состояние по позиции
class _SLState:
    def __init__(self):
        self._by_uid: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def upsert_opened(self, uid: str, *, symbol: str, direction: str, ts: Optional[datetime]):
        async with self._lock:
            s = self._by_uid.get(uid) or {}
            s.update({
                "symbol": symbol,
                "direction": direction,
                "had_tp": s.get("had_tp", False),
                "opened_at": ts or datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            })
            self._by_uid[uid] = s

    async def mark_tp(self, uid: str):
        async with self._lock:
            s = self._by_uid.get(uid)
            if s:
                s["had_tp"] = True
                s["updated_at"] = datetime.utcnow()

    async def get_snapshot(self, uid: str) -> Optional[Dict[str, Any]]:
        async with self._lock:
            s = self._by_uid.get(uid)
            return dict(s) if s else None

    async def drop(self, uid: str):
        async with self._lock:
            self._by_uid.pop(uid, None)

    async def gc(self, ttl_hours: int = 24):
        # условия достаточности: чистим старые снепшоты
        cutoff = datetime.utcnow() - timedelta(hours=ttl_hours)
        async with self._lock:
            stale = [k for k, v in self._by_uid.items() if v.get("updated_at") and v["updated_at"] < cutoff]
            for k in stale:
                self._by_uid.pop(k, None)

_sl_state = _SLState()


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
                # фоновый GC состояния (раз в ~READ_BLOCK_MS-пустой тик)
                await _sl_state.gc(ttl_hours=24)
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
    if not event:
        return True  # мусор → ack

    # базовые поля для всех типов
    position_uid = _as_str(data.get("position_uid"))
    strategy_id  = _as_int(data.get("strategy_id"))
    symbol       = _as_str(data.get("symbol"))
    direction    = (_as_str(data.get("direction")) or "").lower()
    ts_ms_str    = _as_str(data.get("ts_ms"))
    ts_iso       = _as_str(data.get("ts"))
    ts_dt        = _parse_ts(ts_ms_str, ts_iso)

    if not position_uid or not strategy_id or direction not in ("long", "short"):
        log.info("⏭️ SL_SYNC: skip (invalid base fields) id=%s ev=%s uid=%s sid=%s dir=%s",
                 record_id, event, position_uid or "—", strategy_id, direction or "—")
        return True

    # opened v2 → создаём снепшот
    if event == "opened":
        await _sl_state.upsert_opened(position_uid, symbol=symbol, direction=direction, ts=ts_dt)
        log.info("ℹ️ SL_SYNC: opened snapshot stored | uid=%s | sym=%s | dir=%s", position_uid, symbol or "—", direction)
        return True

    # tp_hit → помечаем, что после open был TP (значит sl_replaced будет «после-TP», биржу не трогаем)
    if event == "tp_hit":
        await _sl_state.mark_tp(position_uid)
        log.info("ℹ️ SL_SYNC: tp marker set | uid=%s", position_uid)
        return True

    # closed.* → очищаем снепшот (позиция виртуально финализирована)
    if event.startswith("closed"):
        await _sl_state.drop(position_uid)
        log.info("ℹ️ SL_SYNC: closed snapshot dropped | uid=%s | ev=%s", position_uid, event)
        return True

    # интересует только sl_replaced
    if event != "sl_replaced":
        log.info("⏭️ SL_SYNC: skip (event=%s)", event)
        return True

    # debounce: подождём чуть-чуть, вдруг почти одновременно прилетит tp_hit
    if SL_DEBOUNCE_MS > 0:
        await asyncio.sleep(SL_DEBOUNCE_MS / 1000.0)

    snap = await _sl_state.get_snapshot(position_uid)
    if not snap:
        log.info("⏭️ SL_SYNC: skip (no snapshot) | uid=%s", position_uid)
        return True

    # если был TP после open — это SL-после-TP, биржу не трогаем
    if snap.get("had_tp"):
        log.info("⏭️ SL_SYNC: skip (tp_policy) | uid=%s", position_uid)
        return True

    # это SL-protect → отправляем команду ensure_sl_at_entry
    order_fields = {
        "cmd": "ensure_sl_at_entry",
        "position_uid": position_uid,
        "strategy_id": str(strategy_id),
        "symbol": snap.get("symbol") or symbol or "",
        "direction": direction,
        "order_link_suffix": "sl_entry",
        "ts": ts_iso or "",
        "ts_ms": ts_ms_str or "",
    }

    try:
        await infra.redis_client.xadd(ORDER_REQUEST_STREAM, order_fields)
    except Exception:
        log.exception("❌ SL_SYNC: публикация ensure_sl_at_entry не удалась | uid=%s", position_uid)
        return False  # не ACK → повтор

    log.info(
        "✅ SL_SYNC: ensure_sl_at_entry → sent | uid=%s | sid=%s | sym=%s | dir=%s",
        position_uid, strategy_id, order_fields["symbol"] or "—", direction
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