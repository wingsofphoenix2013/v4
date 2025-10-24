# strategies_v4_cleaner.py — фоновый чистильщик: удаление закрытых позиций батчами и финальный дроп deathrow-стратегий (с грейс-паузой)

# 🔸 Импорты
import os
import asyncio
import logging
import json
from typing import List, Tuple

from infra import infra

# 🔸 Константы
SLEEP_START_SEC = 120         # задержка старта — 2 минуты
SLEEP_CYCLE_SEC = 300         # периодичность проверки — 5 минут
BATCH_LIMIT = 500             # размер батча удаления позиций
DELETE_GRACE_SEC = int(os.getenv("CLEANER_DELETE_GRACE_SEC", "5"))  # пауза перед DELETE стратегии (сек)

# 🔸 Логгер
log = logging.getLogger("STRATEGY_CLEANER")


# 🔹 Вспомогательная: получить количество строк из статуса asyncpg ("DELETE 123")
def _rows_affected(status: str) -> int:
    try:
        return int(status.split()[-1])
    except Exception:
        return 0


# 🔹 Стратегии-кандидаты на удаление
async def _fetch_deathrow_strategies() -> List[int]:
    rows = await infra.pg_pool.fetch(
        "SELECT id FROM strategies_v4 WHERE deathrow = TRUE"
    )
    return [r["id"] for r in rows]


# 🔹 Счётчики позиций по стратегии: (n_closed, n_active)
async def _get_position_counts(strategy_id: int) -> Tuple[int, int]:
    row = await infra.pg_pool.fetchrow(
        """
        SELECT
            COALESCE(SUM(CASE WHEN status = 'closed' THEN 1 ELSE 0 END), 0) AS n_closed,
            COALESCE(SUM(CASE WHEN status IN ('open','partial') THEN 1 ELSE 0 END), 0) AS n_active
        FROM positions_v4
        WHERE strategy_id = $1
        """,
        strategy_id,
    )
    return int(row["n_closed"]), int(row["n_active"])


# 🔹 Выбрать батч закрытых позиций для удаления
async def _fetch_closed_position_uids(strategy_id: int, limit: int) -> List[str]:
    rows = await infra.pg_pool.fetch(
        """
        SELECT position_uid
        FROM positions_v4
        WHERE strategy_id = $1 AND status = 'closed'
        ORDER BY id
        LIMIT $2
        """,
        strategy_id,
        limit,
    )
    return [r["position_uid"] for r in rows]


# 🔹 Удалить один батч связанных данных по позициям (в транзакции)
async def _delete_positions_batch(strategy_id: int, uids: List[str]) -> int:
    if not uids:
        return 0

    async with infra.pg_pool.acquire() as conn:
        async with conn.transaction():
            # 1) Цели позиции (TP/SL)
            await conn.execute(
                """
                DELETE FROM public.position_targets_v4
                 WHERE position_uid = ANY ($1::text[])
                """,
                uids,
            )

            # 2) Логи позиции (uuid)
            await conn.execute(
                """
                DELETE FROM public.positions_log_v4
                 WHERE position_uid = ANY (SELECT unnest($1::text[])::uuid)
                """,
                uids,
            )

            # 3) Логи сигналов по позиции
            await conn.execute(
                """
                DELETE FROM public.signal_log_entries_v4
                 WHERE position_uid = ANY ($1::text[])
                """,
                uids,
            )

            # 4) Сами позиции (страховка по статусу)
            pos_status = await conn.execute(
                """
                DELETE FROM public.positions_v4
                 WHERE position_uid = ANY ($1::text[])
                   AND status = 'closed'
                """,
                uids,
            )

    deleted = _rows_affected(pos_status)
    log.debug(
        "🧹 batch(strategy=%s): POS_DELETED=%d (uids=%d)",
        strategy_id, deleted, len(uids)
    )
    return deleted


# 🔹 Очистка зависимых данных стратегии перед финальным удалением
async def _purge_strategy_related_data(conn, strategy_id: int) -> Tuple[int, int, int]:
    # удаляем processed-таблицы (не имеют FK, но содержат следы вычислений)
    cp_pack_status = await conn.execute(
        "DELETE FROM public.oracle_pack_conf_processed WHERE strategy_id = $1",
        strategy_id,
    )
    cp_plain_status = await conn.execute(
        "DELETE FROM public.oracle_conf_processed WHERE strategy_id = $1",
        strategy_id,
    )

    # удаляем отчёты по стратегии — каскадом удалит *mw/*pack aggregated/sense и их whitelists
    reports_status = await conn.execute(
        "DELETE FROM public.oracle_report_stat WHERE strategy_id = $1",
        strategy_id,
    )

    cp_pack = _rows_affected(cp_pack_status)
    cp_plain = _rows_affected(cp_plain_status)
    reports = _rows_affected(reports_status)

    log.info(
        "🗂️ Очистка зависимостей (sid=%s): report_stat=%d, conf_processed(pack)=%d, conf_processed=%d",
        strategy_id, reports, cp_pack, cp_plain
    )
    return reports, cp_pack, cp_plain


# 🔹 Выключить стратегию, оповестить системы и удалить её
async def _disable_and_drop_strategy(strategy_id: int):
    # 1) выключить стратегию в БД
    await infra.pg_pool.execute(
        "UPDATE strategies_v4 SET enabled = FALSE WHERE id = $1",
        strategy_id,
    )

    # 2) Pub/Sub оповещение о выключении (формат как в UI)
    event = {
        "id": strategy_id,
        "type": "enabled",
        "action": "false",
        "source": "cleaner",
    }
    await infra.redis_client.publish("strategies_v4_events", json.dumps(event))
    log.info("📨 [PubSub] Отключение стратегии id=%s", strategy_id)

    # 3) Пауза — даём слушателям (LAB/филлеру) «додренить» in-flight операции
    log.info("⏳ Пауза перед удалением стратегии id=%s: %ss", strategy_id, DELETE_GRACE_SEC)
    await asyncio.sleep(DELETE_GRACE_SEC)

    # 4) Очистка зависимостей и финальный DELETE стратегии — в одной транзакции
    async with infra.pg_pool.acquire() as conn:
        async with conn.transaction():
            # очищаем зависимые данные (reports → каскад на aggregated/sense/whitelists; + processed-таблицы)
            await _purge_strategy_related_data(conn, strategy_id)

            # финально удаляем запись стратегии (FK больше не мешают)
            del_status = await conn.execute(
                "DELETE FROM public.strategies_v4 WHERE id = $1",
                strategy_id,
            )
            deleted = _rows_affected(del_status)

    log.info("🗑️ Стратегия удалена из БД: id=%s (rows=%d)", strategy_id, deleted)


# 🔹 Обработка одной стратегии в deathrow
async def _process_strategy(strategy_id: int) -> Tuple[int, bool]:
    """
    Возвращает: (сколько позиций удалено, была_ли_стратегия_удалена)
    """
    total_deleted = 0

    # 1) Удаляем закрытые позиции батчами
    while True:
        uids = await _fetch_closed_position_uids(strategy_id, BATCH_LIMIT)
        if not uids:
            break

        deleted = await _delete_positions_batch(strategy_id, uids)
        total_deleted += deleted

        log.info(
            "🧹 Стратегия %s: удалён батч закрытых позиций: %d (batch=%d)",
            strategy_id, deleted, len(uids)
        )

        # Если батч меньше лимита — вероятно, закрытых позиций больше нет
        if len(uids) < BATCH_LIMIT:
            break

    # 2) Проверяем оставшиеся позиции
    n_closed, n_active = await _get_position_counts(strategy_id)
    log.debug("ℹ️ Стратегия %s: осталось closed=%d, active=%d", strategy_id, n_closed, n_active)

    # 3) Если вообще ничего не осталось — выключаем и удаляем стратегию
    if n_closed == 0 and n_active == 0:
        log.info("✅ Стратегия %s: позиций не осталось — отключаем и удаляем", strategy_id)
        await _disable_and_drop_strategy(strategy_id)
        return total_deleted, True

    # 4) Иначе — есть активные/partial, ждём следующего цикла
    if n_active > 0 and n_closed == 0:
        log.info("⏸️ Стратегия %s: есть активные позиции (%d), повторная проверка через 5 минут", strategy_id, n_active)
    return total_deleted, False


# 🔸 Публичный воркер
async def run_strategies_v4_cleaner():
    log.info("🕒 Старт воркера через %d секунд…", SLEEP_START_SEC)
    await asyncio.sleep(SLEEP_START_SEC)

    while True:
        try:
            strategy_ids = await _fetch_deathrow_strategies()
            if not strategy_ids:
                log.debug("🔍 Стратегии в deathrow не найдены")
                await asyncio.sleep(SLEEP_CYCLE_SEC)
                continue

            log.info("🔎 Найдено стратегий в deathrow: %d", len(strategy_ids))

            total_positions_deleted = 0
            total_strategies_deleted = 0

            for sid in strategy_ids:
                try:
                    deleted, dropped = await _process_strategy(sid)
                    total_positions_deleted += deleted
                    total_strategies_deleted += 1 if dropped else 0
                except Exception:
                    log.exception("❌ Ошибка обработки стратегии id=%s", sid)

            log.info(
                "📊 Итог прохода: удалено позиций=%d, удалено стратегий=%d",
                total_positions_deleted, total_strategies_deleted
            )

        except Exception:
            log.exception("❌ Критическая ошибка в цикле cleaner")

        # Всегда ждём 5 минут до следующего прохода
        await asyncio.sleep(SLEEP_CYCLE_SEC)