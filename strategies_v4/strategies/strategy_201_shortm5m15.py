# strategy_201_shortm5m15.py — зеркальная стратегия (шорт; лаборатория TF: m5,m15)

# 🔸 Импорты
import logging
import json
import asyncio

from infra import infra

# 🔸 Логгер стратегии
log = logging.getLogger("strategy_201_shortm5m15")

# 🔸 Класс стратегии
class Strategy201Shortm5m15:
    # 🔸 Проверка сигнала на допустимость
    async def validate_signal(self, signal, context):
        direction = signal["direction"].lower()

        # направление long сразу отклоняем
        if direction == "long":
            return ("ignore", "long сигналы отключены")

        # допускаем только short
        if direction == "short":
            return True

        # иное направление → отказ
        return ("ignore", f"неизвестное направление: {direction}")

    # 🔸 Основной метод запуска стратегии
    async def run(self, signal, context):
        redis = context.get("redis")
        strategy_cfg = context.get("strategy")
        if redis is None or strategy_cfg is None:
            raise RuntimeError("❌ Нет redis или strategy в context")

        # мастер-стратегия, указанная в поле market_mirrow
        master_sid = strategy_cfg.get("market_mirrow")
        if not master_sid:
            log.warning("⚠️ У стратегии нет market_mirrow → отказ")
            return ("ignore", "отсутствует привязка к мастер-стратегии")

        # формируем запрос в laboratory с комплектом TF m5,m15
        req_payload = {
            "log_uid": signal.get("log_uid"),
            "strategy_id": str(master_sid),   # SID мастера для laboratory
            "direction": "short",
            "symbol": signal["symbol"],
            "timeframes": "m5,m15",
        }

        try:
            # отправляем запрос в laboratory:decision_request
            req_id = await redis.xadd("laboratory:decision_request", req_payload)
            log.debug(f"📤 Отправлен запрос в laboratory: {req_payload}")

            # ждём ответа из laboratory:decision_response
            allow, reason = await self._wait_for_response(redis, req_id)
        except Exception:
            log.exception("❌ Ошибка взаимодействия с laboratory_v4")
            return ("ignore", "ошибка при работе с laboratory_v4")

        # решение лаборатории
        if allow:
            # формируем заявку на открытие позиции (уже от зеркала)
            payload = {
                "strategy_id": str(signal["strategy_id"]),  # SID зеркала (201)
                "symbol": signal["symbol"],
                "direction": "short",
                "log_uid": signal.get("log_uid"),
                "route": "new_entry",
                "received_at": signal.get("received_at"),
            }
            try:
                await redis.xadd("strategy_opener_stream", {"data": json.dumps(payload)})
                log.debug(f"📤 Сигнал отправлен в opener: {payload}")
            except Exception as e:
                log.warning(f"⚠️ Ошибка при отправке в strategy_opener_stream: {e}")
        else:
            # отказ лаборатории → возвращаем ignore с причиной
            return ("ignore", f"отказ лаборатории по причине {reason}")

    # 🔸 Ожидание ответа лаборатории по конкретному req_id
    async def _wait_for_response(self, redis, req_id: str):
        stream = "laboratory:decision_response"

        while True:
            entries = await redis.xread({stream: "0"}, block=1000, count=10)
            if not entries:
                continue

            for _, records in entries:
                for record_id, data in records:
                    # проверяем соответствие запроса
                    if data.get("req_id") == req_id:
                        status = data.get("status")
                        if status != "ok":
                            return False, data.get("reason", "ошибка laboratory")

                        allow = str(data.get("allow", "false")).lower() == "true"
                        reason = data.get("reason", "")
                        return allow, reason