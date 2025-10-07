# strategy_211_longm5m15h1.py — зеркальная стратегия (лонг; laboratory_v4 TF: m5m15h1; таймаут ожидания 90с; INFO-логи; запись ignore в signal_log_queue)

# 🔸 Импорты
import logging
import json
import time
from datetime import datetime

from infra import infra, lab_sema_acquire, lab_sema_release
from lab_response_router import wait_lab_response

# 🔸 Логгер стратегии
log = logging.getLogger("strategy_211_longm5m15h1")

# 🔸 Класс стратегии
class Strategy211Longm5m15h1:
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
        tfs = "m5,m15,h1"

        # режим принятия решения лабораторией: "mw_only XX2", "mw_then_pack XX1", "mw_and_pack XX3"
        decision_mode = "mw_then_pack"

        # формируем запрос в laboratory
        req_payload = {
            "log_uid": log_uid,
            "strategy_id": str(master_sid),            # SID мастера (правила WL/BL)
            "client_strategy_id": client_sid,          # SID зеркала (ворота/anti-dup)
            "direction": "long",
            "symbol": symbol,
            "timeframes": tfs,
            "decision_mode": decision_mode,
            "use_bl": "true",
        }

        # лог запроса
        log.debug(
            "[LAB_REQUEST] log_uid=%s master=%s client=%s symbol=%s tf=%s",
            log_uid, master_sid, client_sid, symbol, tfs
        )

        # резервируем слот на общий запрос в LAB (backpressure)
        holder = f"{log_uid}:{time.monotonic_ns()}"
        ok = await lab_sema_acquire(holder)  # лимиты читаются из ENV: LAB_SEMA_LIMIT/LAB_SEMA_TTL
        if not ok:
            note = "lab_backpressure"
            await self._log_ignore_to_queue(redis, client_sid, log_uid, note)
            return ("ignore", note)

        try:
            # отправляем запрос в laboratory:decision_request
            req_id = await redis.xadd("laboratory:decision_request", req_payload)
            log.debug("[LAB_XADD] req_id=%s", req_id)

            # ждём ответ через общий роутер (таймаут 90с)
            allow, reason = await wait_lab_response(req_id, timeout_seconds=90)

        except Exception:
            note = "ошибка при работе с laboratory_v4"
            log.exception("❌ Ошибка взаимодействия с laboratory_v4")
            await self._log_ignore_to_queue(redis, client_sid, log_uid, note)
            return ("ignore", note)

        finally:
            # освобождаем слот в любом случае
            await lab_sema_release(holder)

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
