# strategy_752_uni.py — 🔸 Универсальная стратегия-транзит: пропускает long и short без фильтров

import logging
import json
from typing import Any, Dict, Tuple, Union

log = logging.getLogger("strategy_752_uni")

IgnoreResult = Tuple[str, str]  # ("ignore", reason)


# 🔸 Класс стратегии
class Strategy752Uni:
    # Проверка и валидация сигнала (транзит без фильтров)
    async def validate_signal(self, signal: Dict[str, Any], context: Dict[str, Any]) -> Union[bool, IgnoreResult]:
        direction = str(signal.get("direction", "")).lower()

        # допускаем только корректные направления
        if direction in ("long", "short"):
            return True
        return ("ignore", f"unsupported direction '{direction}'")

    # Выполнение при допуске (прямой транзит в opener)
    async def run(self, signal: Dict[str, Any], context: Dict[str, Any]) -> None:
        redis = context.get("redis")
        if redis is None:
            raise RuntimeError("No Redis in context")

        # формируем payload без изменений
        payload = {
            "strategy_id": str(signal.get("strategy_id")),
            "symbol": signal.get("symbol"),
            "direction": signal.get("direction"),
            "log_uid": signal.get("log_uid"),
            "route": "new_entry",
            "received_at": signal.get("received_at"),
        }

        try:
            await redis.xadd("strategy_opener_stream", {"data": json.dumps(payload)})
            log.debug(f"📤 Open request queued: {payload}")
        except Exception as e:
            log.warning(f"Failed to enqueue open request: {e}")