# strategies_v4_cleaner.py

import asyncio
import logging
import json
from typing import List, Tuple

from infra import infra

# 🔸 Константы
SLEEP_START_SEC = 120        # задержка старта — 2 минуты
SLEEP_CYCLE_SEC = 300        # периодичность проверки — 5 минут
BATCH_LIMIT = 500            # размер батча удаления позиций

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
            # 0) Подготовка: подзапрос LogUID для выбранных position_uid
            #    (используем в LPS/SLE очистке ниже)
            #    Прямо в DELETE используем подзапрос, отдельный SELECT не нужен.

            # 1) Очистка LPS (laboratoty_position_stat) по client_strategy_id и position_uid/log_uid
            lps_status = await conn.execute(
                """
                DELETE FROM public.laboratoty_position_stat
                 WHERE client_strategy_id = $2
                   AND (
                        position_uid = ANY ($1::text[])
                        OR log_uid IN (
                            SELECT log_uid FROM public.positions_v4
                             WHERE position_uid = ANY ($1::text[])
                        )
                   )
                """,
                uids, strategy_id,
            )

            # 2) Очистка SLE (signal_laboratory_entries) по client_strategy_id и log_uid
            sle_status = await conn.execute(
                """
                DELETE FROM public.signal_laboratory_entries
                 WHERE client_strategy_id = $2
                   AND log_uid IN (
                       SELECT log_uid FROM public.positions_v4
                        WHERE position_uid = ANY ($1::text[])
                   )
                """,
                uids, strategy_id,
            )

            # 3) Цели позиции (TP/SL)
            await conn.execute(
                """
                DELETE FROM public.position_targets_v4
                 WHERE position_uid = ANY ($1::text[])
                """,
                uids,
            )

            # 4) Логи позиции (uuid)
            await conn.execute(
                """
                DELETE FROM public.positions_log_v4
                 WHERE position_uid = ANY (SELECT unnest($1::text[])::uuid)
                """,
                uids,
            )

            # 5) Логи сигналов по позиции
            await conn.execute(
                """
                DELETE FROM public.signal_log_entries_v4
                 WHERE position_uid = ANY ($1::text[])
                """,
                uids,
            )

            # 6) Сами позиции (страховка по статусу)
            pos_status = await conn.execute(
                """
                DELETE FROM public.positions_v4
                 WHERE position_uid = ANY ($1::text[])
                   AND status = 'closed'
                """,
                uids,
            )

    # возвращаем только число удалённых позиций (как и раньше)
    deleted = _rows_affected(pos_status)
    # При желании можно раскомментировать debug-лог:
    # log.debug("🧹 batch: LPS=%d SLE=%d POS=%d",
    #           _rows_affected(lps_status), _rows_affected(sle_status), deleted)
    return deleted

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
    log.info(f"📨 [PubSub] Отключение стратегии id={strategy_id}")

    # 3) Небольшая пауза — даём слушателям выгрузить стратегию из памяти
    await asyncio.sleep(1.0)

    # 4) удалить саму стратегию (каскады снесут TP/SL/тикеры)
    await infra.pg_pool.execute(
        "DELETE FROM strategies_v4 WHERE id = $1",
        strategy_id,
    )
    log.info(f"🗑️ Стратегия удалена из БД: id={strategy_id}")


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
            f"🧹 Стратегия {strategy_id}: удалён батч закрытых позиций: {deleted} (batch={len(uids)})"
        )

        # Если батч меньше лимита — вероятно, закрытых позиций больше нет
        if len(uids) < BATCH_LIMIT:
            break

    # 2) Проверяем оставшиеся позиции
    n_closed, n_active = await _get_position_counts(strategy_id)
    log.debug(
        f"ℹ️ Стратегия {strategy_id}: осталось closed={n_closed}, active={n_active}"
    )

    # 3) Если вообще ничего не осталось — выключаем и удаляем стратегию
    if n_closed == 0 and n_active == 0:
        log.info(f"✅ Стратегия {strategy_id}: позиций не осталось — отключаем и удаляем")
        await _disable_and_drop_strategy(strategy_id)
        return total_deleted, True

    # 4) Иначе — есть активные/partial, ждём следующего цикла
    if n_active > 0 and n_closed == 0:
        log.info(
            f"⏸️ Стратегия {strategy_id}: есть активные позиции ({n_active}), повторная проверка через 5 минут"
        )
    return total_deleted, False


# 🔸 Публичный воркер
async def run_strategies_v4_cleaner():
    log.info("🕒 Старт воркера через 2 минуты…")
    await asyncio.sleep(SLEEP_START_SEC)

    while True:
        try:
            strategy_ids = await _fetch_deathrow_strategies()
            if not strategy_ids:
                log.debug("🔍 Стратегии в deathrow не найдены")
                await asyncio.sleep(SLEEP_CYCLE_SEC)
                continue

            log.info(f"🔎 Найдено стратегий в deathrow: {len(strategy_ids)}")

            total_positions_deleted = 0
            total_strategies_deleted = 0

            for sid in strategy_ids:
                try:
                    deleted, dropped = await _process_strategy(sid)
                    total_positions_deleted += deleted
                    total_strategies_deleted += 1 if dropped else 0
                except Exception:
                    log.exception(f"❌ Ошибка обработки стратегии id={sid}")

            log.info(
                f"📊 Итог прохода: удалено позиций={total_positions_deleted}, "
                f"удалено стратегий={total_strategies_deleted}"
            )

        except Exception:
            log.exception("❌ Критическая ошибка в цикле cleaner")

        # Всегда ждём 5 минут до следующего прохода
        await asyncio.sleep(SLEEP_CYCLE_SEC)