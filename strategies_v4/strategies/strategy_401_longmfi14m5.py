# strategy_401_longmfi14m5.py — стратегия: разрешение по MFI (m5, length=14) через универсальный решатель

import logging
import json
import uuid
import asyncio
from datetime import datetime

log = logging.getLogger("strategy_401_longmfi14m5")

REQUEST_STREAM  = "decision_request"
RESPONSE_STREAM = "decision_response"


# 🔸 Отправить запрос решателю и дождаться ответа (MFI m5 len=14), без таймаута
async def _ask_mfi_m5_len14_decision(redis, strategy_id: int, symbol: str, direction: str) -> tuple[str, str]:
    req_id = str(uuid.uuid4())

    # зафиксировать хвост decision_response ДО отправки запроса
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
        # один check: mfi_bucket, только m5, длина 14, всегда on_demand
        "checks": json.dumps([{
            "kind": "mfi_bucket",
            "timeframes": ["m5"],
            "lengths": {"m5": [14]},
            "bin_step": 5,
            "source": "on_demand"
        }]),
        "sent_at": datetime.utcnow().isoformat()
    }

    await redis.xadd(REQUEST_STREAM, payload)

    # блокирующее ожидание ответа по req_id
    while True:
        resp = await redis.xread(streams={RESPONSE_STREAM: last_id}, count=50, block=0)  # BLOCK 0 = бесконечно
        if not resp:
            continue
        for _, messages in resp:
            for mid, data in messages:
                last_id = mid
                if data.get("req_id") == req_id:
                    decision = (data.get("decision") or "ignore").lower()
                    reason = data.get("reason") or ""
                    return (decision, reason)


class Strategy401Longmfi14m5:
    async def validate_signal(self, signal, context):
        direction = (signal["direction"] or "").lower()

        if direction == "long":
            pass
        elif direction == "short":
            return ("ignore", "short сигналы отключены")
        else:
            return ("ignore", f"неизвестное направление: {direction}")

        redis = context.get("redis")
        if redis is None:
            return ("ignore", "redis недоступен")

        strategy_id = int(signal["strategy_id"])
        symbol = signal["symbol"]

        decision, reason = await _ask_mfi_m5_len14_decision(redis, strategy_id, symbol, direction)
        log.debug(f"[DECISION] strat={strategy_id} {symbol} dir={direction} decision={decision} reason={reason}")

        if decision == "allow":
            return True
        elif decision == "deny":
            return ("ignore", "mfi_m5_deny")
        else:
            return ("ignore", f"mfi_m5_{reason or 'ignore'}")

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