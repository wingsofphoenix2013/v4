# lab_response_router.py — единственный слушатель laboratory:decision_response и ожидание по req_id

# 🔸 Импорты
import asyncio
import logging
from typing import Dict, Tuple, Optional
from datetime import datetime

from infra import infra

# 🔸 Логгер
log = logging.getLogger("LAB_RESP_ROUTER")

# 🔸 Константы
RESP_STREAM = "laboratory:decision_response"
XREAD_BLOCK_MS = 1000
XREAD_COUNT = 100

# 🔸 Реестр ожиданий и ранние результаты
_pending: Dict[str, asyncio.Future] = {}
_early: Dict[str, Tuple[bool, str]] = {}
_last_id = "$"

# 🔸 Ожидание ответа по req_id (API для стратегий)
async def wait_lab_response(req_id: str, timeout_seconds: int = 90) -> Tuple[bool, str]:
    # если ответ уже приходил (router опередил регистрацию ожидания)
    res = _early.pop(req_id, None)
    if res is not None:
        return res

    fut = asyncio.get_running_loop().create_future()
    _pending[req_id] = fut

    try:
        return await asyncio.wait_for(fut, timeout=timeout_seconds)
    except asyncio.TimeoutError:
        # cleanup
        if req_id in _pending:
            _pending.pop(req_id, None)
        return (False, "lab_timeout")

# 🔸 Главный слушатель стрима ответов лаборатории
async def run_lab_response_router():
    global _last_id
    log.debug("🛰️ LAB_RESP_ROUTER запущен (STREAM=%s)", RESP_STREAM)

    while True:
        try:
            entries = await infra.redis_client.xread({RESP_STREAM: _last_id}, count=XREAD_COUNT, block=XREAD_BLOCK_MS)
            if not entries:
                continue

            for _, records in entries:
                for record_id, data in records:
                    _last_id = record_id
                    req_id = data.get("req_id")
                    if not req_id:
                        continue

                    status = data.get("status", "error")
                    if status != "ok":
                        # техническая ошибка от LAB
                        allow = False
                        reason = f"lab_error:{data.get('error','unknown')}"
                    else:
                        allow = str(data.get("allow", "false")).lower() == "true"
                        reason = (data.get("reason") or "")

                    fut = _pending.pop(req_id, None)
                    if fut is not None and not fut.done():
                        fut.set_result((allow, reason))
                    else:
                        # ответ пришёл раньше регистрации ожидания — запомним
                        _early[req_id] = (allow, reason)

        except asyncio.CancelledError:
            log.debug("⏹️ LAB_RESP_ROUTER остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка в LAB_RESP_ROUTER")
            await asyncio.sleep(1.0)