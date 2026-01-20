# strategy_105_universal.py — простая транзитная стратегия: без LAB, без фильтра направления

# 🔸 Импорты
import logging
import json

# 🔸 Логгер
log = logging.getLogger("strategy_105_universal")

# 🔸 Класс стратегии
class Strategy105Universal:
    # 🔸 Валидация сигнала (транзитная: принимает любые направления)
    async def validate_signal(self, signal, context):
        # лог результата на уровне info
        log.debug(
            "✅ [TRANSIT_PASS] log_uid=%s strategy_id=%s %s %s",
            signal.get("log_uid"),
            signal.get("strategy_id"),
            signal.get("symbol"),
            str(signal.get("direction")).lower(),
        )
        return True

    # 🔸 Публикация заявки на открытие позиции (без LAB)
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
            "route": "reverse_entry",
            "received_at": signal.get("received_at"),
        }

        # отправляем в конвейер открытия
        await redis.xadd("strategy_opener_stream", {"data": json.dumps(payload, separators=(",", ":"))})
        # лог результата на уровне info
        log.debug(
            "📨 [OPEN_REQ_SENT] log_uid=%s strategy_id=%s %s %s",
            payload["log_uid"], payload["strategy_id"], payload["symbol"], payload["direction"]
        )