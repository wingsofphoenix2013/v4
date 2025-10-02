# strategy_201_longm5.py — зеркальная стратегия (лонг, m5, с проверкой через laboratory_v4)

import logging
import json
import asyncio

from infra import infra

# 🔸 Логгер стратегии
log = logging.getLogger("strategy_201_longm5")

# 🔸 Класс стратегии
class Strategy201Longm5:
    # 🔸 Проверка сигнала на допустимость
    async def validate_signal(self, signal, context):
        direction = signal["direction"].lower()

        # направление short сразу отклоняем
        if direction == "short":
            return ("ignore", "short сигналы отключены")

        # допускаем только long
        if direction == "long":
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

        # формируем запрос в laboratory
        req_payload = {
            "log_uid": signal.get("log_uid"),
            "strategy_id": str(master_sid),
            "direction": "long",
            "symbol": signal["symbol"],
            "timeframes": "m5",
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
                "strategy_id": str(signal["strategy_id"]),  # SID зеркала
                "symbol": signal["symbol"],
                "direction": "long",
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