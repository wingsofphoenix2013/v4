# 🔸 auditor_v4_main.py — entrypoint auditor_v4: инициализация, одноразовый аудит закрытых сделок и запуск фоновых воркеров (AUD_CROSS_STRENGTH, AUD_EMA200_SIDE, AUD_ATRREG, AUD_EMA2150_SPREAD, AUD_BEST_SELECTOR)

# 🔸 Импорты
import asyncio
import logging
import datetime as dt

from auditor_infra import (
    setup_logging,
    setup_pg,
    setup_redis_client,
)
from auditor_config import load_active_mw_strategies
import auditor_infra as infra

from auditor_mwstat_worker import run_mwstat_worker

# 🔸 Логгер
log = logging.getLogger("AUD_MAIN")


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


# 🔸 Вспомогательная функция запуска с задержкой
async def _start_with_delay(coro_func, delay_sec: int):
    # условия достаточности
    if delay_sec and delay_sec > 0:
        log.info("⏳ Ожидание %d сек перед стартом задачи", delay_sec)
        await asyncio.sleep(int(delay_sec))
    # запуск целевой корутины
    await coro_func()


# 🔸 Одноразовый аудит: счётчики закрытых сделок по MW-стратегиям (7/14/28 дней и всего)
async def run_one_shot_audit():
    # условия достаточности
    if infra.pg_pool is None:
        log.info("❌ Пропуск аудита: PG не инициализирован")
        return

    # загрузка активных MW-стратегий
    strategies = await load_active_mw_strategies()
    log.info("📦 Найдено активных MW-стратегий: %d", len(strategies))
    if not strategies:
        return

    # расчёт временных границ (UTC, без привязки к началу часа/суток)
    now_utc = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    d7_from = now_utc - dt.timedelta(days=7)
    d14_from = now_utc - dt.timedelta(days=14)
    d28_from = now_utc - dt.timedelta(days=28)
    log.info(
        "🕒 Временные окна: now_utc=%s, d7_from=%s, d14_from=%s, d28_from=%s",
        now_utc, d7_from, d14_from, d28_from
    )

    # список sid для выборки
    sid_list = list(strategies.keys())

    # агрегирующий запрос по всем стратегиям сразу
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                strategy_id,
                direction,
                COUNT(*) FILTER (WHERE closed_at >= $1) AS cnt_7d,
                COUNT(*) FILTER (WHERE closed_at >= $2) AS cnt_14d,
                COUNT(*) FILTER (WHERE closed_at >= $3) AS cnt_28d,
                COUNT(*) AS cnt_total
            FROM positions_v4
            WHERE status = 'closed'
              AND strategy_id = ANY($4)
              AND direction IN ('long','short')
            GROUP BY strategy_id, direction
            """,
            d7_from, d14_from, d28_from, sid_list
        )

    # укладка результатов в {sid: {'long': {...}, 'short': {...}}}
    result = {sid: {"long": {"7d": 0, "14d": 0, "28d": 0, "total": 0},
                    "short": {"7d": 0, "14d": 0, "28d": 0, "total": 0}} for sid in sid_list}

    for r in rows:
        sid = int(r["strategy_id"])
        direction = str(r["direction"])
        if sid not in result:
            continue
        if direction not in ("long", "short"):
            continue
        result[sid][direction]["7d"] = int(r["cnt_7d"] or 0)
        result[sid][direction]["14d"] = int(r["cnt_14d"] or 0)
        result[sid][direction]["28d"] = int(r["cnt_28d"] or 0)
        result[sid][direction]["total"] = int(r["cnt_total"] or 0)

    # суммарные логи по каждой стратегии
    for sid, stats in result.items():
        name = strategies[sid].get("name") or f"sid_{sid}"
        human = strategies[sid].get("human_name") or ""
        # форматирование человека читаемой подписи стратегии
        title = f'{sid} "{name}"' if not human else f'{sid} "{name}" ({human})'
        log.info(
            "📊 %s | long {7d=%d, 14d=%d, 28d=%d, total=%d} | short {7d=%d, 14d=%d, 28d=%d, total=%d}",
            title,
            stats["long"]["7d"], stats["long"]["14d"], stats["long"]["28d"], stats["long"]["total"],
            stats["short"]["7d"], stats["short"]["14d"], stats["short"]["28d"], stats["short"]["total"],
        )

    log.info("✅ Одноразовый аудит закрытых сделок завершён")

# 🔸 Главная точка входа
async def main():
    setup_logging()
    log.info("📦 Запуск сервиса auditor_v4")

    # подключения к внешним сервисам
    try:
        await setup_pg()
        await setup_redis_client()
        log.info("🔌 Подключения к PostgreSQL и Redis инициализированы")
    except Exception:
        log.exception("❌ Ошибка инициализации внешних сервисов")
        return

    log.info("🚀 Запуск задач auditor_v4")
    await asyncio.gather(
        # одноразовый аудит закрытых сделок (выполняется и завершается)
        run_one_shot_audit(),
        
        # одноразовый анализ MW-фильтров m5 (запуск через 60 секунд)
        _start_with_delay(run_mwstat_worker, 60),
        
    )

    log.info("😴 auditor_v4: задачи завершены, уходим в сон на 99 часов, чтобы сервис не перезапускался")

    try:
        # 99 часов сна, чтобы процесс оставался живым
        await asyncio.sleep(99 * 3600)
    except asyncio.CancelledError:
        log.info("⏹️ auditor_v4: сон прерван сигналом, сервис завершается")

# 🔸 Запуск модуля
if __name__ == "__main__":
    asyncio.run(main())