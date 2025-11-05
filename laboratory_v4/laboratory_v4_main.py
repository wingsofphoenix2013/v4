# 🔸 laboratory_v4_main.py — entrypoint laboratory_v4: инициализация, загрузка конфигов, запуск фоновых воркеров

# 🔸 Импорты
import asyncio
import logging

from laboratory_infra import (
    setup_logging,
    setup_pg,
    setup_redis_client,
)
from laboratory_config import (
    load_initial_config,
    lists_stream_listener,
    config_event_listener,
)

# 🔸 импорт воркера «советчика»
from laboratory_decision_maker import run_laboratory_decision_maker
# 🔸 импорт воркера пост-процессинга
from laboratory_postproc import run_laboratory_postproc
# 🔸 импорт воркера BL-анализатора
from laboratory_bl_analyzer import run_laboratory_bl_analyzer
# 🔸 импорт воркера WL-анализатора
from laboratory_wl_analyzer import run_laboratory_wl_analyzer
# 🔸 импорт воркера PACK-анализатора
from laboratory_pack_analyzer import run_laboratory_pack_analyzer
# 🔸 импорт воркера CLEANER
from laboratory_cleaner import run_laboratory_cleaner

# 🔸 Логгер
log = logging.getLogger("LAB_MAIN")

# 🔸 Параметры запуска воркеров
# задержки перед первым стартом (сек) — можно править при необходимости
INITIAL_DELAY_LISTS = 0
INITIAL_DELAY_CONFIG = 0
INITIAL_DELAY_DECISION = 0
INITIAL_DELAY_POSTPROC = 0
INITIAL_DELAY_BL = 60
INITIAL_DELAY_WL = 60
INITIAL_DELAY_CLEANER = 0
INITIAL_DELAY_PACK = 0

# пример периодичности для потенциальных периодических задач (сек) — не используется сейчас
DEFAULT_INTERVAL_SEC = 6 * 60 * 60


# 🔸 Обёртка с автоперезапуском воркера
async def run_safe_loop(coro, label: str):
    while True:
        try:
            log.info(f"[{label}] 🚀 Запуск задачи")
            await coro()
        except asyncio.CancelledError:
            log.info(f"[{label}] ⏹️ Остановлено по сигналу")
            raise
        except Exception:
            log.exception(f"[{label}] ❌ Упал с ошибкой — перезапуск через 5 секунд")
            await asyncio.sleep(5)


# 🔸 Обёртка для периодического запуска
async def run_periodic(coro_func, interval_sec: int, label: str, initial_delay: int = 0):
    if initial_delay > 0:
        log.info(f"[{label}] ⏳ Ожидание {initial_delay} сек перед первым запуском")
        await asyncio.sleep(initial_delay)
    while True:
        try:
            log.info(f"[{label}] 🔁 Периодический запуск")
            await coro_func()
        except asyncio.CancelledError:
            log.info(f"[{label}] ⏹️ Периодическая задача остановлена")
            raise
        except Exception:
            log.exception(f"[{label}] ❌ Ошибка при периодическом выполнении")
        await asyncio.sleep(int(interval_sec))


# 🔸 Главная точка входа
async def main():
    setup_logging()
    log.info("📦 Запуск сервиса laboratory_v4")

    # подключения к внешним сервисам
    try:
        await setup_pg()
        await setup_redis_client()
        log.info("🔌 Подключения к PostgreSQL и Redis инициализированы")
    except Exception:
        log.exception("❌ Ошибка инициализации внешних сервисов")
        return

    # первичная загрузка конфигурации
    try:
        await load_initial_config()
        log.info("📦 Стартовая конфигурация загружена")
    except Exception:
        log.exception("❌ Ошибка первичной загрузки конфигурации")
        return

    log.info("🚀 Запуск фоновых воркеров laboratory_v4")

    # слушатели: списки (Streams) и конфиги (Pub/Sub)
    await asyncio.gather(
        # слушатель обновлений WL/BL из oracle (Redis Streams)
        run_safe_loop(
            lambda: _start_with_delay(lists_stream_listener, INITIAL_DELAY_LISTS),
            "LAB_LISTS_STREAMS",
        ),
        # слушатель событий конфигов (Pub/Sub тикеров/стратегий)
        run_safe_loop(
            lambda: _start_with_delay(config_event_listener, INITIAL_DELAY_CONFIG),
            "LAB_CONFIG_PUBSUB",
        ),
        # «советчик»: запрос → решение → ответ в стрим → запись в БД
        run_safe_loop(
            lambda: _start_with_delay(run_laboratory_decision_maker, INITIAL_DELAY_DECISION),
            "LAB_DECISION",
        ),
        # пост-процессинг закрытых позиций
        run_safe_loop(
            lambda: _start_with_delay(run_laboratory_postproc, INITIAL_DELAY_POSTPROC),
            "LAB_POSTPROC",
        ),
        # BL-анализатор (полный прогон + подписка на PACK списки)
        run_safe_loop(
            lambda: _start_with_delay(run_laboratory_bl_analyzer, INITIAL_DELAY_BL),
            "LAB_BL_ANALYZER",
        ),
        # WL-анализатор (MW и PACK winrate)
        run_safe_loop(
            lambda: _start_with_delay(run_laboratory_wl_analyzer, INITIAL_DELAY_WL),
            "LAB_WL_ANALYZER",
        ),
        # очистка (по триггерам PACK_LISTS READY)
        run_safe_loop(
            lambda: _start_with_delay(run_laboratory_cleaner, INITIAL_DELAY_CLEANER),
            "LAB_CLEANER",
        ),
        # PACK-анализатор (комбо-статистика 7d, периодический)
        run_safe_loop(
            lambda: _start_with_delay(run_laboratory_pack_analyzer, INITIAL_DELAY_PACK),
            "LAB_PACK_ANALYZER",
        ),
    )


# 🔸 Вспомогательная функция запуска с задержкой
async def _start_with_delay(coro_func, delay_sec: int):
    # условия достаточности
    if delay_sec and delay_sec > 0:
        log.info("⏳ Ожидание %d сек перед стартом задачи", delay_sec)
        await asyncio.sleep(int(delay_sec))
    # запуск целевой корутины
    await coro_func()


# 🔸 Запуск модуля
if __name__ == "__main__":
    asyncio.run(main())