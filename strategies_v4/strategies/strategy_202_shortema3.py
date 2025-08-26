# strategy_202_shortema3.py — стратегия: разрешение по EMA-паттернам (m5, m15, h1, short)

import logging
import json
import uuid
import asyncio
from datetime import datetime

log = logging.getLogger("strategy_202_shortema3")

REQUEST_STREAM  = "decision_request"
RESPONSE_STREAM = "decision_response"
DECISION_TIMEOUT_MS = 10000  # мс


async def _ask_ema_m5_m15_h1_decision(redis, strategy_id: int, symbol: str, direction: str) -> tuple[str, str]:
    req_id = str(uuid.uuid4())
    payload = {
        "req_id": req_id,
        "strategy_id": str(strategy_id),
        "symbol": symbol,
        "direction": direction.lower(),
        "deadline_ms": str(DECISION_TIMEOUT_MS),
        "mirror": "auto",
        "checks": json.dumps([{"kind": "ema_pattern", "timeframes": ["m5", "m15", "h1"], "source": "on_demand"}]),
        "sent_at": datetime.utcnow().isoformat()
    }
    await redis.xadd(REQUEST_STREAM, payload)

    deadline = asyncio.get_event_loop().time() + (DECISION_TIMEOUT_MS / 1000.0)
    last_id = "0-0"
    while True:
        timeout = max(0, deadline - asyncio.get_event_loop().time())
        block_ms = int(min(500, timeout * 1000)) if timeout > 0 else 0
        if block_ms == 0:
            return ("ignore", "timeout")
        resp = await redis.xread(streams={RESPONSE_STREAM: last_id}, count=50, block=block_ms)
        if not resp:
            continue
        for _, messages in resp:
            for mid, data in messages:
                last_id = mid
                if data.get("req_id") == req_id:
                    decision = (data.get("decision") or "ignore").lower()
                    reason = data.get("reason") or ""
                    return (decision, reason)


class Strategy202Shortema3:
    async def validate_signal(self, signal, context):
        direction = (signal["direction"] or "").lower()
        if direction == "short":
            pass
        elif direction == "long":
            return ("ignore", "long сигналы отключены")
        else:
            return ("ignore", f"неизвестное направление: {direction}")

        redis = context.get("redis")
        if redis is None:
            return ("ignore", "redis недоступен")

        strategy_id = int(signal["strategy_id"])
        symbol = signal["symbol"]

        decision, reason = await _ask_ema_m5_m15_h1_decision(redis, strategy_id, symbol, direction)
        log.debug(f"[DECISION] strat={strategy_id} {symbol} dir={direction} decision={decision} reason={reason}")

        if decision == "allow":
            return True
        elif decision == "deny":
            return ("ignore", "ema_m5_m15_h1_deny")
        else:
            return ("ignore", f"ema_m5_m15_h1_{reason or 'ignore'}")

    async def run(self, signal, context):
        redis = context.get("redis")
        if redis is None:
            raise RuntimeError("❌ Redis клиент не передан в context")
        payload = {
            "strategy_id": str(signal["strategy_id"]),
            "symbol": signal["symbol"],
            "direction": signal["direction"],
            "log_uid": signal.get("log_uid"),
            "route": "new_entry",
            "received_at": signal.get("received_at")
        }
        try:
            await redis.xadd("strategy_opener_stream", {"data": json.dumps(payload)})
            log.debug(f"📤 Сигнал отправлен: {payload}")
        except Exception as e:
            log.warning(f"⚠️ Ошибка при отправке сигнала: {e}")