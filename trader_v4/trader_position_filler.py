# trader_position_filler.py — якорение позиции и «толстая» заявка (opened v2) + обновление public.trader_signals

# 🔸 Импорты
import os
import asyncio
import logging
import json
from decimal import Decimal
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

from trader_infra import infra
from trader_config import config

# 🔸 Логгер воркера
log = logging.getLogger("TRADER_FILLER")

# 🔸 Потоки/группы
POSITIONS_STATUS_STREAM = "positions_bybit_status"   # источник: informer v1.2+ (opened, schema="v2")
ORDER_REQUEST_STREAM    = "trader_order_requests"    # получатель: bybit_processor
CG_NAME   = "trader_filler_status_group"
CONSUMER  = "trader_filler_status_1"

# 🔸 Параметры чтения/параллелизма
READ_BLOCK_MS = 1000
READ_COUNT    = 10
CONCURRENCY   = 8

# 🔸 Настройки исполнителя
SIZE_PCT_ENV = "BYBIT_SIZE_PCT"  # % реального размера от виртуального (0 < pct ≤ 100)

# 🔸 Основной цикл воркера
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

    # проверим доступность коэффициента размера
    size_pct = _get_size_pct()
    if size_pct is None:
        log.error("❌ %s не задан или некорректен — воркер не стартует", SIZE_PCT_ENV)
        return

    log.info("🚦 TRADER_FILLER v3 запущен (источник=%s, параллелизм=%d, size_pct=%s)",
             POSITIONS_STATUS_STREAM, CONCURRENCY, _dec_to_str(size_pct))

    sem = asyncio.Semaphore(CONCURRENCY)

    async def _spawn_task(record_id: str, data: Dict[str, Any]):
        # ack только при успехе — at-least-once до ORDER_REQUEST_STREAM
        async with sem:
            ack_ok = False
            try:
                ack_ok = await _handle_opened_v2(record_id, data, size_pct)
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
            log.exception("❌ Ошибка в основном цикле TRADER_FILLER")
            await asyncio.sleep(0.5)

# 🔸 Обработка события opened v2 (schema="v2")
async def _handle_opened_v2(record_id: str, data: Dict[str, Any], size_pct: Decimal) -> bool:
    # условия достаточности: opened + v2
    event = (_as_str(data.get("event")) or "").lower()
    if event != "opened":
        # FILLER обновляет trader_signals только для opened; прочее пропускаем тихо
        log.info("⏭️ FILLER: пропуск id=%s (event=%s)", record_id, event or "—")
        return True

    schema = _as_str(data.get("schema"))
    position_uid = _as_str(data.get("position_uid"))
    strategy_id  = _as_int(data.get("strategy_id"))
    symbol       = _as_str(data.get("symbol"))
    direction    = (_as_str(data.get("direction")) or "").lower()
    ts_ms_str    = _as_str(data.get("ts_ms"))
    ts_iso       = _as_str(data.get("ts"))
    created_at   = _parse_ts(ts_ms_str, ts_iso) or datetime.utcnow()

    # базовая отметка «принято к обработке»
    await _update_trader_signal_status(
        stream_id=record_id, position_uid=position_uid, event="opened", ts_iso=ts_iso,
        status="accepted_by_filler", note="accepted opened v2" if schema == "v2" else "accepted opened (non-v2)"
    )

    if schema != "v2":
        await _update_trader_signal_status(
            stream_id=record_id, position_uid=position_uid, event="opened", ts_iso=ts_iso,
            status="skipped_opened_non_v2", note=f"schema={schema or ''}"
        )
        log.info("⏭️ FILLER: пропуск id=%s (schema=%s, ожидаем 'v2')", record_id, schema or "—")
        return True

    # размеры/плечо из события (обязательны в v1.2+)
    leverage       = _as_decimal(data.get("leverage"))
    virt_qty       = _as_decimal(data.get("quantity"))
    virt_qty_left  = _as_decimal(data.get("quantity_left")) or virt_qty
    virt_margin    = _as_decimal(data.get("margin_used"))

    if not position_uid or not strategy_id or not symbol or direction not in ("long", "short") \
       or leverage is None or leverage <= 0 or virt_qty is None or virt_qty_left is None or virt_margin is None:
        await _update_trader_signal_status(
            stream_id=record_id, position_uid=position_uid, event="opened", ts_iso=ts_iso,
            status="skipped_opened_incomplete", note="missing/invalid fields"
        )
        log.debug("⚠️ opened v2: неполные/некорректные поля (id=%s, sid=%s, uid=%s, sym=%s, dir=%s, lev=%s, qty=%s, ql=%s, mu=%s)",
                  record_id, strategy_id, position_uid, symbol, direction, leverage, virt_qty, virt_qty_left, virt_margin)
        return False  # не ACK → повторная доставка

    # вычисление реальных величин по size_pct
    real_qty   = (virt_qty * size_pct) / Decimal("100")
    real_margin= (virt_margin * size_pct) / Decimal("100")

    # точности тикера и политика стратегии (для заявки)
    tmeta = config.tickers.get(symbol) or {}
    policy = config.strategy_policy.get(strategy_id) or {}
    policy_json = json.dumps(policy, ensure_ascii=False, default=_json_default)

    precision_qty = tmeta.get("precision_qty")
    min_qty = tmeta.get("min_qty")
    ticksize = tmeta.get("ticksize")

    # 1) якорим позицию в trader_positions_v4 (идемпотентно)
    try:
        await infra.pg_pool.execute(
            """
            INSERT INTO public.trader_positions_v4 (
                position_uid, strategy_id, symbol, direction, log_uid,
                leverage, size_pct,
                virt_quantity, virt_quantity_left, virt_margin_used,
                real_quantity, real_margin_used,
                exchange, entry_status, entry_order_link_id, entry_order_id, last_ext_event_at,
                status, created_at, entry_filled_at, closed_at, close_reason,
                pnl_real, exec_fee_total, avg_entry_price, avg_close_price,
                error_last, extras
            ) VALUES (
                $1, $2, $3, $4, NULL,
                $5, $6,
                $7, $8, $9,
                $10, $11,
                'bybit', 'planned', NULL, NULL, NULL,
                'open', $12, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL,
                NULL, NULL
            )
            ON CONFLICT (position_uid) DO NOTHING
            """,
            position_uid, strategy_id, symbol, direction,
            leverage, size_pct,
            virt_qty, virt_qty_left, virt_margin,
            real_qty, real_margin,
            created_at
        )
    except Exception as e:
        await _update_trader_signal_status(
            stream_id=record_id, position_uid=position_uid, event="opened", ts_iso=ts_iso,
            status="failed_db_update", note=f"anchor insert error: {e.__class__.__name__}"
        )
        log.exception("❌ Не удалось вставить якорь позиции (uid=%s)", position_uid)
        return False  # не ACK → повтор

    # 2) собираем «толстую» заявку для bybit_processor
    order_fields = {
        "position_uid": position_uid,
        "strategy_id": str(strategy_id),
        "symbol": symbol,
        "direction": direction,
        "created_at": _to_iso(created_at),
        "ts_ms": ts_ms_str or "",
        "ts": ts_iso or "",

        # политика и точности
        "policy": policy_json,
        "precision_qty": str(precision_qty) if precision_qty is not None else "",
        "min_qty": _dec_to_str(min_qty),
        "ticksize": _dec_to_str(ticksize),

        # плечо и размеры
        "leverage": _dec_to_str(leverage),
        "size_pct": _dec_to_str(size_pct),
        "virt_quantity": _dec_to_str(virt_qty),
        "virt_quantity_left": _dec_to_str(virt_qty_left),
        "virt_margin_used": _dec_to_str(virt_margin),
        "real_quantity": _dec_to_str(real_qty),
        "real_margin_used": _dec_to_str(real_margin),
    }

    # 3) публикация заявки
    try:
        await infra.redis_client.xadd(ORDER_REQUEST_STREAM, order_fields)
    except Exception as e:
        await _update_trader_signal_status(
            stream_id=record_id, position_uid=position_uid, event="opened", ts_iso=ts_iso,
            status="failed_publish_order_request", note=f"redis xadd error: {e.__class__.__name__}"
        )
        log.exception("❌ Не удалось опубликовать заявку uid=%s", position_uid)
        return False

    # финальный статус для opened
    await _update_trader_signal_status(
        stream_id=record_id, position_uid=position_uid, event="opened", ts_iso=ts_iso,
        status="filler_thick_order_published",
        note=f"published thick order; real_qty={_dec_to_str(real_qty)}; size_pct={_dec_to_str(size_pct)}"
    )

    # лог результата (информативный)
    log.info(
        "✅ FILLER: anchored+sent | uid=%s | sid=%s | sym=%s | dir=%s | lev=%s | virt_qty=%s | real_qty=%s | size_pct=%s | margin=%s",
        position_uid, strategy_id, symbol, direction,
        _dec_to_str(leverage), _dec_to_str(virt_qty), _dec_to_str(real_qty), _dec_to_str(size_pct),
        _dec_to_str(virt_margin),
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
                res2 = await infra.pg_pool.execute(
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
                # даже если 0 строк — молча выходим; это не должно ломать бизнес-поток
    except Exception:
        log.exception("⚠️ trader_signals update failed (status=%s, uid=%s, ev=%s)", status, position_uid or "—", event or "—")

# 🔸 Вспомогательные функции

def _get_size_pct() -> Optional[Decimal]:
    # условия достаточности: env задан и 0 < pct ≤ 100
    raw = os.getenv(SIZE_PCT_ENV, "").strip()
    try:
        pct = Decimal(raw)
        if pct <= 0 or pct > 100:
            return None
        return pct
    except Exception:
        return None

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

def _dec_to_str(v: Any) -> str:
    try:
        d = _as_decimal(v)
        if d is None:
            return ""
        s = f"{d:.12f}".rstrip("0").rstrip(".")
        return s if s else "0"
    except Exception:
        return ""

def _to_iso(v: Any) -> str:
    try:
        return (v.isoformat() + "Z") if hasattr(v, "isoformat") else (str(v) if v is not None else "")
    except Exception:
        return str(v) if v is not None else ""

def _json_default(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")