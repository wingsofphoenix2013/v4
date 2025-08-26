# position_decision_maker.py — этап 1: слушатель decision_request, базовая валидация и заглушка-ответ

import asyncio
import logging
import json
from datetime import datetime

log = logging.getLogger("POSITION_DECISION_MAKER")

REQUEST_STREAM  = "decision_request"
RESPONSE_STREAM = "decision_response"
GROUP           = "decision_maker_group"
CONSUMER        = "decision_maker_1"

# 🔸 Создание consumer group для decision_request
async def _ensure_group(redis):
    try:
        await redis.xgroup_create(REQUEST_STREAM, GROUP, id="$", mkstream=True)
        log.info(f"Создана consumer group {GROUP} для {REQUEST_STREAM}")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug(f"Consumer group {GROUP} уже существует")
        else:
            log.exception("Ошибка создания consumer group")
            raise

# 🔸 Отправка ответа в decision_response
async def _send_response(redis, req_id: str, decision: str, reason: str):
    payload = {
        "req_id": req_id or "",
        "decision": decision,
        "reason": reason,
        "responded_at": datetime.utcnow().isoformat(),
    }
    await redis.xadd(RESPONSE_STREAM, payload)
    log.info(f"[RESP] req_id={req_id} decision={decision} reason={reason}")

# 🔸 Валидация минимально необходимых полей запроса
def _validate_request(data: dict) -> tuple[bool, str]:
    required = ("req_id", "strategy_id", "symbol", "direction", "checks")
    for k in required:
        if data.get(k) in (None, ""):
            return False, f"missing_{k}"
    if data.get("direction", "").lower() not in ("long", "short"):
        return False, "bad_direction"
    if not isinstance(data.get("checks"), (list, tuple)) or len(data["checks"]) == 0:
        return False, "empty_checks"
    return True, "ok"

# 🔸 Основной цикл: читаем decision_request и отвечаем заглушкой
async def run_position_decision_maker(pg, redis):
    await _ensure_group(redis)

    while True:
        try:
            resp = await redis.xreadgroup(
                groupname=GROUP,
                consumername=CONSUMER,
                streams={REQUEST_STREAM: ">"},
                count=50,
                block=2000
            )
            if not resp:
                continue

            to_ack = []
            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)
                    try:
                        # берём req_id и базовые поля
                        req_id      = data.get("req_id")
                        strategy_id = data.get("strategy_id")
                        symbol      = data.get("symbol")
                        direction   = data.get("direction")
                        checks_raw  = data.get("checks")

                        # попытка распарсить checks из строки JSON, если нужно
                        if isinstance(checks_raw, str):
                            try:
                                data["checks"] = json.loads(checks_raw)
                            except Exception:
                                pass

                        ok, reason = _validate_request(data)
                        if not ok:
                            log.warning(f"[REQ_SKIP] req_id={req_id} reason={reason}")
                            await _send_response(redis, req_id, "ignore", reason)
                            continue

                        # лог входящего запроса (укороченно)
                        log.info(f"[REQ] req_id={req_id} strat={strategy_id} {symbol} dir={direction} checks={len(data['checks'])}")

                        # заглушка: пока всегда ignore (дальше добавим реальную логику)
                        await _send_response(redis, req_id, "ignore", "not_implemented")

                    except Exception:
                        log.exception("Ошибка обработки decision_request")
            if to_ack:
                await redis.xack(REQUEST_STREAM, GROUP, *to_ack)

        except Exception as e:
            log.error(f"Ошибка в цикле POSITION_DECISION_MAKER: {e}", exc_info=True)
            await asyncio.sleep(2)