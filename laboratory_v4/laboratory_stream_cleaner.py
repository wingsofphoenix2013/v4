# laboratory_stream_cleaner.py — уборщик стримов laboratory_v4

# 🔸 Импорты
import asyncio
import logging
from typing import List, Tuple

# 🔸 Инфраструктура
import laboratory_infra as infra

# 🔸 Логгер
log = logging.getLogger("LAB_STREAM_CLEANER")

# 🔸 Параметры работы клинера
RETENTION_MINUTES = 60          # окно хранения сообщений
START_DELAY_SEC = 90            # стартовая задержка после загрузки сервиса
RUN_EVERY_SEC = 3600            # периодичность прогона, 1 час

# 🔸 Список стримов для очистки (ТОЛЬКО свои)
STREAMS_TO_CLEAN: List[str] = [
    "laboratory:decision_response",  # ответы стратегиям
    "laboratory_decision_filler",    # seed для переносов SLE→LPS
    "indicator_gateway_request",     # запросы к индикаторному gateway
]


# 🔸 Получить время Redis-сервера в миллисекундах
async def _redis_time_ms() -> int:
    # команда TIME возвращает [seconds, microseconds]
    t = await infra.redis_client.time()
    # защита от разных форматов ответов
    if isinstance(t, (list, tuple)) and len(t) >= 2:
        sec, usec = int(t[0]), int(t[1])
        return sec * 1000 + (usec // 1000)
    # fallback — на всякий случай (не должно случаться)
    return 0


# 🔸 Выполнить XTRIM MINID для одного стрима, вернуть текущую длину после трима
async def _trim_stream_minid(stream: str, threshold_id: str) -> int:
    # Пробуем нативный метод redis-py; если нет — используем execute_command
    try:
        # approximate=True → быстрее, допускается хранение части старых батчей чуть дольше
        newlen = await infra.redis_client.xtrim(stream, minid=threshold_id, approximate=True)
    except Exception:
        # универсальный путь
        # XTRIM key MINID ~ threshold_id
        try:
            newlen = await infra.redis_client.execute_command("XTRIM", stream, "MINID", "~", threshold_id)
        except Exception:
            newlen = -1
    return int(newlen) if newlen is not None else -1


# 🔸 Один прогон очистки всех стримов
async def _run_clean_once(retention_minutes: int):
    # считаем порог по времени Redis, чтобы избежать дрейфа локальных часов
    now_ms = await _redis_time_ms()
    if now_ms <= 0:
        log.info("[CLEAN] ⚠️ не удалось получить время Redis — пропуск прогона")
        return

    thr_ms = now_ms - retention_minutes * 60_000
    threshold_id = f"{thr_ms}-0"

    # цикл по стримам
    for stream in STREAMS_TO_CLEAN:
        try:
            newlen = await _trim_stream_minid(stream, threshold_id)
            if newlen >= 0:
                log.info("[CLEAN] ✂️ stream=%s MINID<%s → len=%d", stream, threshold_id, newlen)
            else:
                log.info("[CLEAN] ✂️ stream=%s MINID<%s → len=unknown", stream, threshold_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("[CLEAN] ❌ ошибка трима stream=%s", stream)


# 🔸 Главный воркер очистки стримов
async def run_laboratory_stream_cleaner():
    """
    Удаляет старые сообщения из собственных стримов лаборатории с помощью XTRIM MINID.
    Порог = текущее время Redis минус RETENTION_MINUTES (в миллисекундах).
    Старт через START_DELAY_SEC после запуска, далее — каждые RUN_EVERY_SEC.
    """
    log.debug("🧹 LAB_STREAM_CLEANER стартует: retention=%dmin, start_delay=%ds, period=%ds",
              RETENTION_MINUTES, START_DELAY_SEC, RUN_EVERY_SEC)
    await asyncio.sleep(START_DELAY_SEC)

    while True:
        try:
            await _run_clean_once(RETENTION_MINUTES)
        except asyncio.CancelledError:
            log.debug("⏹️ LAB_STREAM_CLEANER остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ LAB_STREAM_CLEANER ошибка прогона")
        # ожидание до следующего часа
        await asyncio.sleep(RUN_EVERY_SEC)