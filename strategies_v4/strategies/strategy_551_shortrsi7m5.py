# strategy_551_shortrsi7m5.py — стратегия: разрешение по RSI (m5, length=7) через универсальный решатель

import logging
import json
import uuid
from datetime import datetime

log = logging.getLogger("strategy_551_shortrsi7m5")

REQUEST_STREAM  = "decision_request"
RESPONSE_STREAM = "decision_response"


# 🔸 Отправить запрос решателю и дождаться ответа (RSI m5 len=7), без таймаута
async def _ask_rsi_m5_len7_decision(redis, strategy_id: int, symbol: str, direction: str) -> tuple[str, str]:
    req_id = str(uuid.uuid4())

    # фиксируем хвост ответа ДО отправки запроса
    try:
        tail = await redis.xrevrange(RESPONSE_STREAM, count=1)
        last_id = tail[0][0] if tail else "0-0"
    except Exception:
        last_id = "0-0"

    payload = {
        "req_id": req_id,
        "strategy_id": str(strategy_id),
        "symbol": symbol,
        "direction": direction.lower(),
        "mirror": "auto",
        "checks": json.dumps([{
            "kind": "rsi_bucket",
            "timeframes": ["m5"],
            "lengths": {"m5": [7]},
            "bin_step": 5,
            "source": "on_demand"
        }]),
        "sent_at": datetime.utcnow().isoformat()
    }
    await redis.xadd(REQUEST_STREAM, payload)

    # блокирующее ожидание
    while True:
        resp = await redis.xread(streams={RESPONSE_STREAM: last_id}, count=50, block=0)
        if not resp:
            continue
        for _, messages in resp:
            for mid, data in messages:
                last_id = mid
                if data.get("req_id") == req_id:
                    decision = (data.get("decision") or "ignore").lower()
                    reason = data.get("reason") or ""
                    return (decision, reason)


class Strategy551Shortrsi7m5:
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

        decision, reason = await _ask_rsi_m5_len7_decision(redis, strategy_id, symbol, direction)
        log.debug(f"[DECISION] strat={strategy_id} {symbol} dir={direction} decision={decision} reason={reason}")

        if decision == "allow":
            return True
        elif decision == "deny":
            return ("ignore", "rsi_m5_deny")
        else:
            return ("ignore", f"rsi_m5_{reason or 'ignore'}")

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