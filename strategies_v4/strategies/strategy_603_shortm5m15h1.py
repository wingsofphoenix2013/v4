# strategy_603_shortm5m15h1.py — зеркальная стратегия (шорт; laboratory_v4 TF: m5,m15,h1; ожидание по last-id, таймаут 90с; INFO-логи; запись ignore в signal_log_queue)

# 🔸 Импорты
import logging
import json
import asyncio
import time
from datetime import datetime

from infra import infra

# 🔸 Логгер стратегии
log = logging.getLogger("strategy_603_shortm5m15h1")

# 🔸 Класс стратегии
class Strategy603Shortm5m15h1:
    # 🔸 Проверка сигнала на допустимость
    async def validate_signal(self, signal, context):
        direction = signal["direction"].lower()

        # запрещаем long
        if direction == "long":
            return ("ignore", "long сигналы отключены")

        # разрешаем только short
        if direction == "short":
            return True

        # неизвестное направление
        return ("ignore", f"неизвестное направление: {direction}")

    # 🔸 Основной метод запуска стратегии
    async def run(self, signal, context):
        redis = context.get("redis")
        strategy_cfg = context.get("strategy")
        if redis is None or strategy_cfg is None:
            note = "нет redis или strategy в context"
            await self._log_ignore_to_queue(redis, signal.get("strategy_id"), signal.get("log_uid"), note)
            return ("ignore", note)

        # мастер-стратегия из market_mirrow
        master_sid = strategy_cfg.get("market_mirrow")
        if not master_sid:
            note = "отсутствует привязка к мастер-стратегии"
            log.debug("⚠️ [IGNORE] log_uid=%s reason=\"no_market_mirrow\"", signal.get("log_uid"))
            await self._log_ignore_to_queue(redis, signal.get("strategy_id"), signal.get("log_uid"), note)
            return ("ignore", note)

        # нормализация тикера
        symbol = str(signal["symbol"]).upper()
        client_sid = str(signal["strategy_id"])
        log_uid = signal.get("log_uid")
        tfs = "m5,m15,h1"

        # режим принятия решения лабораторией: "mw_only" или "mw_then_pack"
        decision_mode = "mw_and_pack"
        
        # получаем last-generated-id ответа ДО отправки запроса
        last_resp_id = await self._get_stream_last_id(redis, "laboratory:decision_response")

        # формируем запрос в laboratory
        req_payload = {
            "log_uid": log_uid,
            "strategy_id": str(master_sid),             # SID мастера
            "client_strategy_id": client_sid,           # SID зеркала
            "direction": "short",
            "symbol": symbol,
            "timeframes": tfs,
            "trace": "true",
            "decision_mode": decision_mode,
        }

        log.debug(
            "[LAB_REQUEST] log_uid=%s master=%s client=%s symbol=%s tf=%s",
            log_uid, master_sid, client_sid, symbol, tfs
        )

        try:
            req_id = await redis.xadd("laboratory:decision_request", req_payload)
            log.debug("[LAB_XADD] req_id=%s", req_id)

            log.debug("[LAB_WAIT] req_id=%s last_id=%s deadline=90s", req_id, last_resp_id)
            allow, reason = await self._wait_for_response(redis, req_id, last_resp_id, timeout_seconds=90)
        except Exception:
            note = "ошибка при работе с laboratory_v4"
            log.exception("❌ Ошибка взаимодействия с laboratory_v4")
            await self._log_ignore_to_queue(redis, client_sid, log_uid, note)
            return ("ignore", note)

        if allow:
            payload = {
                "strategy_id": client_sid,
                "symbol": symbol,
                "direction": "short",
                "log_uid": log_uid,
                "route": "new_entry",
                "received_at": signal.get("received_at"),
            }
            log.debug(
                "[OPEN_REQ] log_uid=%s client_sid=%s symbol=%s direction=%s",
                log_uid, client_sid, symbol, "short"
            )
            try:
                await redis.xadd("strategy_opener_stream", {"data": json.dumps(payload)})
                log.debug("[OPEN_SENT] log_uid=%s position_request_published=true", log_uid)
                return ("ok", "passed_laboratory")
            except Exception as e:
                note = "ошибка отправки в opener"
                log.debug("[OPEN_FAIL] log_uid=%s error=%s", log_uid, str(e))
                await self._log_ignore_to_queue(redis, client_sid, log_uid, note)
                return ("ignore", note)
        else:
            log.debug("[IGNORE] log_uid=%s reason=\"%s\"", log_uid, reason)
            await self._log_ignore_to_queue(redis, client_sid, log_uid, reason)
            return ("ignore", f"отказ лаборатории по причине {reason}")

    # 🔸 Получение last-generated-id
    async def _get_stream_last_id(self, redis, stream_name: str) -> str:
        try:
            info = await redis.xinfo_stream(stream_name)
            return info.get("last-generated-id") or info.get("last_generated_id") or "$"
        except Exception:
            return "$"

    # 🔸 Ожидание ответа
    async def _wait_for_response(self, redis, req_id: str, since_id: str, timeout_seconds: int = 90):
        stream = "laboratory:decision_response"
        deadline = time.monotonic() + timeout_seconds
        read_id = since_id

        while True:
            if time.monotonic() > deadline:
                log.debug("[LAB_TIMEOUT] req_id=%s", req_id)
                return False, "lab_timeout"

            entries = await redis.xread({stream: read_id}, block=1000, count=50)
            if not entries:
                continue

            total = sum(len(records) for _, records in entries)
            if total:
                log.debug("[LAB_READ] req_id=%s batch=%d", req_id, total)

            for _, records in entries:
                for record_id, data in records:
                    read_id = record_id
                    if data.get("req_id") != req_id:
                        continue

                    status = data.get("status", "error")
                    if status == "ok":
                        allow = str(data.get("allow", "false")).lower() == "true"
                        reason = data.get("reason", "") or ""
                        log.debug("[LAB_RESP] req_id=%s status=%s allow=%s reason=\"%s\"",
                                 req_id, status, str(allow).lower(), reason)
                        return allow, reason
                    if status == "error":
                        err_code = data.get("error", "unknown")
                        message = data.get("message", "")
                        log.debug("[LAB_ERROR] req_id=%s error=%s message=\"%s\"", req_id, err_code, message)
                        return False, f"lab_error:{err_code}"

    # 🔸 Логирование ignore
    async def _log_ignore_to_queue(self, redis, strategy_id, log_uid, note: str):
        try:
            if redis is None:
                return
            record = {
                "log_uid": str(log_uid) if log_uid else "",
                "strategy_id": str(strategy_id) if strategy_id else "",
                "status": "ignore",
                "note": note or "",
                "position_uid": "",
                "logged_at": datetime.utcnow().isoformat()
            }
            await redis.xadd("signal_log_queue", record)
            log.debug("[IGNORE_LOGGED] log_uid=%s note=\"%s\"", log_uid, note)
        except Exception as e:
            log.debug("[IGNORE_LOG_FAIL] log_uid=%s error=%s", log_uid, str(e))