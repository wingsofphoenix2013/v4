# strategy_551_long.py — 🔸 Лонговая стратегия-транзит: пропускает только long без фильтров

import logging
import json
from typing import Any, Dict, Tuple, Union

log = logging.getLogger("strategy_551_long")

IgnoreResult = Tuple[str, str]  # ("ignore", reason)


# 🔸 Класс стратегии
class Strategy551Long:
    # Проверка и валидация сигнала (только long)
    async def validate_signal(self, signal: Dict[str, Any], context: Dict[str, Any]) -> Union[bool, IgnoreResult]:
        direction = str(signal.get("direction", "")).lower()

        if direction == "long":
            return True
        if direction == "short":
            return ("ignore", "short signals are disabled")
        return ("ignore", f"unsupported direction '{direction}'")

    # Выполнение при допуске (прямой транзит в opener)
    async def run(self, signal: Dict[str, Any], context: Dict[str, Any]) -> None:
        redis = context.get("redis")
        if redis is None:
            raise RuntimeError("No Redis in context")

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