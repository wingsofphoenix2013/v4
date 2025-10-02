# strategy_201_longm5.py — зеркальная стратегия (лонг; laboratory_v4 TF: m5; ожидание по last-id, таймаут 90с; подробные INFO-логи диалога с лабораторией)

# 🔸 Импорты
import logging
import json
import asyncio
import time

from infra import infra

# 🔸 Логгер стратегии
log = logging.getLogger("strategy_201_longm5")

# 🔸 Класс стратегии
class Strategy201Longm5:
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
            return ("ignore", "нет redis или strategy в context")

        # мастер-стратегия из market_mirrow
        master_sid = strategy_cfg.get("market_mirrow")
        if not master_sid:
            log.info("⚠️ [IGNORE] log_uid=%s reason=\"no_market_mirrow\"", signal.get("log_uid"))
            return ("ignore", "отсутствует привязка к мастер-стратегии")

        # нормализация тикера
        symbol = str(signal["symbol"]).upper()
        client_sid = str(signal["strategy_id"])
        log_uid = signal.get("log_uid")
        tfs = "m5"

        # получаем last-generated-id ответа ДО отправки запроса
        last_resp_id = await self._get_stream_last_id(redis, "laboratory:decision_response")

        # формируем запрос в laboratory
        req_payload = {
            "log_uid": log_uid,
            "strategy_id": str(master_sid),           # SID мастера (правила WL/BL)
            "client_strategy_id": client_sid,         # SID зеркала (ворота/anti-dup)
            "direction": "long",
            "symbol": symbol,
            "timeframes": tfs,
            "trace": "true",
        }

        # лог запроса
        log.info(
            "[LAB_REQUEST] log_uid=%s master=%s client=%s symbol=%s tf=%s",
            log_uid, master_sid, client_sid, symbol, tfs
        )

        try:
            # отправляем запрос в laboratory:decision_request
            req_id = await redis.xadd("laboratory:decision_request", req_payload)
            log.info("[LAB_XADD] req_id=%s", req_id)

            # ждём ответ из laboratory:decision_response (таймаут 90с)
            log.info("[LAB_WAIT] req_id=%s last_id=%s deadline=90s", req_id, last_resp_id)
            allow, reason = await self._wait_for_response(redis, req_id, last_resp_id, timeout_seconds=90)
        except Exception:
            log.exception("❌ Ошибка взаимодействия с laboratory_v4")
            return ("ignore", "ошибка при работе с laboratory_v4")

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
            log.info(
                "[OPEN_REQ] log_uid=%s client_sid=%s symbol=%s direction=%s",
                log_uid, client_sid, symbol, "long"
            )
            try:
                await redis.xadd("strategy_opener_stream", {"data": json.dumps(payload)})
                log.info("[OPEN_SENT] log_uid=%s position_request_published=true", log_uid)
                return ("ok", "passed_laboratory")
            except Exception as e:
                log.info("[OPEN_FAIL] log_uid=%s error=%s", log_uid, str(e))
                return ("ignore", "ошибка отправки в opener")
        else:
            # отказ лаборатории → формируем ignore с причиной
            log.info("[IGNORE] log_uid=%s reason=\"%s\"", log_uid, reason)
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
                log.info("[LAB_TIMEOUT] req_id=%s", req_id)
                return False, "lab_timeout"

            # читаем только новые записи после read_id
            entries = await redis.xread({stream: read_id}, block=1000, count=50)
            if not entries:
                continue

            # логируем факт получения батча (без избыточного спама)
            total = sum(len(records) for _, records in entries)
            if total:
                log.info("[LAB_READ] req_id=%s batch=%d", req_id, total)

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
                        log.info("[LAB_RESP] req_id=%s status=%s allow=%s reason=\"%s\"",
                                 req_id, status, str(allow).lower(), reason)
                        return allow, reason

                    # техническая ошибка лаборатории
                    if status == "error":
                        err_code = data.get("error", "unknown")
                        message = data.get("message", "")
                        log.info("[LAB_ERROR] req_id=%s error=%s message=\"%s\"", req_id, err_code, message)
                        return False, f"lab_error:{err_code}"

            # продолжаем до дедлайна