# strategy_712_longm5.py — зеркальная стратегия (лонг; laboratory_v4 TF: m5; ожидание по last-id, таймаут 90с; INFO-логи; запись ignore в signal_log_queue)

# 🔸 Импорты
import logging
import json
import asyncio
import time
from datetime import datetime

from infra import infra

# 🔸 Логгер стратегии
log = logging.getLogger("strategy_712_longm5")

# 🔸 Класс стратегии
class Strategy712Longm5:
    # 🔸 Проверка сигнала на допустимость
    async def validate_signal(self, signal, context):
        direction = signal["direction"].lower()

        # запрещаем short
        if direction == "short":
            return ("ignore", "short сигналы отключены")

        # разрешаем только long
        if direction == "long":
            return True

        # неизвестное направление
        return ("ignore", f"неизвестное направление: {direction}")

    # 🔸 Основной метод запуска стратегии
    async def run(self, signal, context):
        redis = context.get("redis")
        strategy_cfg = context.get("strategy")
        if redis is None or strategy_cfg is None:
            note = "нет redis или strategy в context"
            # пишем сразу в очередь логов, чтобы появился след в БД
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
        tfs = "m5"

        # режим принятия решения лабораторией: "mw_only" или "mw_then_pack"
        decision_mode = "mw_only"
        
        # получаем last-generated-id ответа ДО отправки запроса
        last_resp_id = await self._get_stream_last_id(redis, "laboratory:decision_response")

        # формируем запрос в laboratory
        req_payload = {
            "log_uid": log_uid,
            "strategy_id": str(master_sid),            # SID мастера (правила WL/BL)
            "client_strategy_id": client_sid,          # SID зеркала (ворота/anti-dup)
            "direction": "long",
            "symbol": symbol,
            "timeframes": tfs,
            "trace": "true",
            "decision_mode": decision_mode,
            "use_bl": "true",
        }

        # лог запроса
        log.debug(
            "[LAB_REQUEST] log_uid=%s master=%s client=%s symbol=%s tf=%s",
            log_uid, master_sid, client_sid, symbol, tfs
        )

        try:
            # отправляем запрос в laboratory:decision_request
            req_id = await redis.xadd("laboratory:decision_request", req_payload)
            log.debug("[LAB_XADD] req_id=%s", req_id)

            # ждём ответ из laboratory:decision_response (таймаут 90с)
            log.debug("[LAB_WAIT] req_id=%s last_id=%s deadline=90s", req_id, last_resp_id)
            allow, reason = await self._wait_for_response(redis, req_id, last_resp_id, timeout_seconds=90)
        except Exception:
            note = "ошибка при работе с laboratory_v4"
            log.exception("❌ Ошибка взаимодействия с laboratory_v4")
            await self._log_ignore_to_queue(redis, client_sid, log_uid, note)
            return ("ignore", note)

        # решение лаборатории
        if allow:
            # готовим заявку на открытие позиции от имени зеркала
            payload = {
                "strategy_id": client_sid,       # SID зеркала
                "symbol": symbol,
                "direction": "long",
                "log_uid": log_uid,
                "route": "new_entry",
                "received_at": signal.get("received_at"),
            }
            log.debug(
                "[OPEN_REQ] log_uid=%s client_sid=%s symbol=%s direction=%s",
                log_uid, client_sid, symbol, "long"
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
            # отказ лаборатории → формируем ignore с причиной
            log.debug("[IGNORE] log_uid=%s reason=\"%s\"", log_uid, reason)
            await self._log_ignore_to_queue(redis, client_sid, log_uid, reason)
            return ("ignore", f"отказ лаборатории по причине {reason}")

    # 🔸 Получение last-generated-id для стрима (чтение только нового)
    async def _get_stream_last_id(self, redis, stream_name: str) -> str:
        try:
            info = await redis.xinfo_stream(stream_name)
            # если стрим пуст или отсутствует поле — читаем «с конца»
            last_id = info.get("last-generated-id") or info.get("last_generated_id") or "$"
            return last_id
        except Exception:
            # если стрим ещё не создан — используем «с конца»
            return "$"

    # 🔸 Ожидание ответа лаборатории по конкретному req_id с дедлайном
    async def _wait_for_response(self, redis, req_id: str, since_id: str, timeout_seconds: int = 90):
        stream = "laboratory:decision_response"
        deadline = time.monotonic() + timeout_seconds
        read_id = since_id

        while True:
            # страховой выход по таймауту
            if time.monotonic() > deadline:
                log.debug("[LAB_TIMEOUT] req_id=%s", req_id)
                return False, "lab_timeout"

            # читаем только новые записи после read_id
            entries = await redis.xread({stream: read_id}, block=1000, count=50)
            if not entries:
                continue

            # логируем факт получения батча (без избыточного спама)
            total = sum(len(records) for _, records in entries)
            if total:
                log.debug("[LAB_READ] req_id=%s batch=%d", req_id, total)

            for _, records in entries:
                for record_id, data in records:
                    # сдвигаем "курсор"
                    read_id = record_id

                    # ищем наш ответ по req_id
                    if data.get("req_id") != req_id:
                        continue

                    status = data.get("status", "error")

                    # успешная обработка
                    if status == "ok":
                        allow = str(data.get("allow", "false")).lower() == "true"
                        reason = (data.get("reason", "") or "")
                        log.debug("[LAB_RESP] req_id=%s status=%s allow=%s reason=\"%s\"",
                                 req_id, status, str(allow).lower(), reason)
                        return allow, reason

                    # техническая ошибка лаборатории
                    if status == "error":
                        err_code = data.get("error", "unknown")
                        message = data.get("message", "")
                        log.debug("[LAB_ERROR] req_id=%s error=%s message=\"%s\"", req_id, err_code, message)
                        return False, f"lab_error:{err_code}"

            # продолжаем до дедлайна

    # 🔸 Запись ignore-события в очередь логов (для последующей записи в БД)
    async def _log_ignore_to_queue(self, redis, strategy_id, log_uid, note: str):
        try:
            if redis is None:
                return
            record = {
                "log_uid": str(log_uid) if log_uid is not None else "",
                "strategy_id": str(strategy_id) if strategy_id is not None else "",
                "status": "ignore",
                "note": note or "",
                "position_uid": "",
                "logged_at": datetime.utcnow().isoformat()
            }
            await redis.xadd("signal_log_queue", record)
            log.debug("[IGNORE_LOGGED] log_uid=%s note=\"%s\"", log_uid, note)
        except Exception as e:
            log.debug("[IGNORE_LOG_FAIL] log_uid=%s error=%s", log_uid, str(e))