# strategy_705_reverse.py — двунаправленная стратегия с LAB-гейтингом и выбором мастера по направлению (long/short)

# 🔸 Импорты
import logging
import json
import uuid

from infra import lab_sema_acquire, lab_sema_release
from lab_response_router import wait_lab_response

# 🔸 Логгер
log = logging.getLogger("strategy_705_reverse")

# 🔸 Константы LAB-запроса (общие настройки стратегии)
LAB_REQ_STREAM = "laboratory:decision_request"
LAB_TIMEFRAMES = "m5,m15,h1"     		# порядок обязателен
LAB_DECISION_MODE = "mw_and_pack"    	# mw_only | mw_then_pack | mw_and_pack | pack_only
LAB_VERSION = "v5"               		# v1 | v2 | v3 | v4 | v5
LAB_USE_WL = "false"             		# "true" | "false" (строкой)
LAB_USE_BL = "false"             		# "true" | "false" (строкой)
LAB_WAIT_TIMEOUT_SEC = 120        		# таймаут ожидания ответа

# 🔸 Класс стратегии
class Strategy705Reverse:
    # 🔸 Валидация сигнала + запрос в LAB (двунаправленный выбор master по направлению)
    async def validate_signal(self, signal, context):
        # базовая валидация направления (двунаправленно)
        direction = str(signal.get("direction", "")).lower()
        if direction not in ("long", "short"):
            return ("ignore", f"неизвестное направление: {direction}")

        # обязательные зависимости
        redis = context.get("redis")
        if redis is None:
            raise RuntimeError("❌ Redis клиент не передан в context")

        strategy_cfg = context.get("strategy") or {}
        client_sid = strategy_cfg.get("id")

        # выбор мастера по направлению с фолбэком на универсальное поле
        if direction == "long":
            master_sid = strategy_cfg.get("market_mirrow_long") or strategy_cfg.get("market_mirrow")
        else:
            master_sid = strategy_cfg.get("market_mirrow_short") or strategy_cfg.get("market_mirrow")

        # проверка конфигурации мастера
        if not master_sid:
            log.error(
                "❌ invalid_config: отсутствует master для направления [%s] "
                "[client_sid=%s symbol=%s dir=%s log_uid=%s]",
                direction, client_sid, signal.get("symbol"), direction, signal.get("log_uid"),
            )
            return ("ignore", "invalid_config")

        # генерируем уникальный req_uid
        req_uid = str(uuid.uuid4())
        symbol = str(signal.get("symbol", "")).upper()
        log_uid = signal.get("log_uid")

        # формируем payload запроса для LAB
        request = {
            "req_uid": req_uid,
            "log_uid": log_uid,
            "strategy_id": int(master_sid),           # master (WL/BL) — по направлению
            "client_strategy_id": int(client_sid),    # текущая (двунаправленная) стратегия
            "symbol": symbol,
            "direction": direction,                   # long | short
            "timeframes": LAB_TIMEFRAMES,             # "m5,m15,h1"
            "decision_mode": LAB_DECISION_MODE,       # mw_only | mw_then_pack | mw_and_pack | pack_only
            "version": LAB_VERSION,                   # v1 | v2 | v3 | v4 | v5
            "use_wl": LAB_USE_WL,                     # "true" | "false" (строкой)
            "use_bl": LAB_USE_BL,                     # "true" | "false" (строкой)
        }

        # предохранитель нагрузки: распределённый семафор
        holder = f"{req_uid}:{symbol}:{direction}"
        acquired = await lab_sema_acquire(holder)
        if not acquired:
            log.debug(
                "⏳ [LAB_REQ_BUSY] req=%s client_sid=%s master_sid=%s %s %s",
                req_uid, client_sid, master_sid, symbol, direction,
            )
            return ("ignore", "lab_busy")

        try:
            # отправляем запрос в LAB (стрим с обёрткой data)
            await redis.xadd(LAB_REQ_STREAM, {"data": json.dumps(request, separators=(",", ":"))})
            log.debug(
                "📤 [LAB_REQ_SENT] req=%s client_sid=%s master_sid=%s %s %s tfs=%s mode=%s ver=%s bl=%s",
                req_uid, client_sid, master_sid, symbol, direction,
                LAB_TIMEFRAMES, LAB_DECISION_MODE, LAB_VERSION, LAB_USE_BL,
            )

            # ждём ответ от роутера (по req_uid)
            allow, reason = await wait_lab_response(req_uid, timeout_seconds=LAB_WAIT_TIMEOUT_SEC)

            # лог результата (стратегия, помимо роутера)
            log.debug("📥 [LAB_RESP_RECV] req=%s allow=%s reason=%s", req_uid, allow, reason or "")

            if allow:
                return True
            else:
                return ("ignore", reason or "lab_rejected")

        finally:
            # обязательно освобождаем слот семафора
            await lab_sema_release(holder)

    # 🔸 Публикация заявки на открытие позиции
    async def run(self, signal, context):
        # зависимости
        redis = context.get("redis")
        if redis is None:
            raise RuntimeError("❌ Redis клиент не передан в context")

        # собираем payload для opener’а
        payload = {
            "strategy_id": str(signal["strategy_id"]),
            "symbol": signal["symbol"],
            "direction": signal["direction"],
            "log_uid": signal.get("log_uid"),
            "route": "new_entry",
            "received_at": signal.get("received_at"),
        }

        # отправляем в конвейер открытия
        await redis.xadd("strategy_opener_stream", {"data": json.dumps(payload)})
        log.debug(
            "📨 [OPEN_REQ_SENT] log_uid=%s strategy_id=%s %s %s",
            payload["log_uid"], payload["strategy_id"], payload["symbol"], payload["direction"]
        )