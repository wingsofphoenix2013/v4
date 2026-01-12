# strategies_v4_cleaner.py — фоновый чистильщик: удаление закрытых позиций батчами и финальный дроп deathrow-стратегий (с грейс-паузой)

# 🔸 Импорты
import os
import json
import uuid
import asyncio
import logging
from typing import List, Tuple

from infra import infra

# 🔸 Константы
SLEEP_START_SEC = 120         # задержка старта — 2 минуты
SLEEP_CYCLE_SEC = 300         # периодичность проверки — 5 минут
BATCH_LIMIT = 500             # размер батча удаления позиций
DELETE_GRACE_SEC = int(os.getenv("CLEANER_DELETE_GRACE_SEC", "5"))  # пауза перед DELETE стратегии (сек)

# 🔸 Логгер
log = logging.getLogger("STRATEGY_CLEANER")


# 🔸 Вспомогательная: получить количество строк из статуса asyncpg ("DELETE 123")
def _rows_affected(status: str) -> int:
    try:
        return int(status.split()[-1])
    except Exception:
        return 0


# 🔸 Вспомогательная: конвертировать список строковых UID в UUID (для uuid-колонок)
def _to_uuid_list(uids: List[str]) -> Tuple[List[uuid.UUID], int]:
    uuids: List[uuid.UUID] = []
    bad = 0

    for s in uids:
        try:
            uuids.append(uuid.UUID(str(s)))
        except Exception:
            bad += 1

    return uuids, bad


# 🔸 Стратегии-кандидаты на обработку (deathrow)
async def _fetch_deathrow_strategies() -> List[int]:
    rows = await infra.pg_pool.fetch("SELECT id FROM strategies_v4 WHERE deathrow = TRUE")
    return [int(r["id"]) for r in rows]


# 🔸 Счётчики позиций по стратегии: (n_closed, n_active)
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


# 🔸 Выбрать батч закрытых позиций для удаления
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
    return [str(r["position_uid"]) for r in rows]


# 🔸 Удалить один батч связанных данных по позициям (в транзакции)
async def _delete_positions_batch(strategy_id: int, uids: List[str]) -> Tuple[int, int, int, int]:
    """
    Возвращает: (deleted_positions, deleted_targets, deleted_pos_logs, deleted_signal_logs)
    """
    if not uids:
        return 0, 0, 0, 0

    pos_deleted = 0
    targets_deleted = 0
    pos_logs_deleted = 0
    signal_logs_deleted = 0

    async with infra.pg_pool.acquire() as conn:
        async with conn.transaction():
            # цели позиции (TP/SL)
            targets_status = await conn.execute(
                """
                DELETE FROM public.position_targets_v4
                 WHERE position_uid = ANY ($1::text[])
                """,
                uids,
            )
            targets_deleted = _rows_affected(targets_status)

            # логи позиции (uuid) — аккуратно конвертируем в UUID, чтобы не падать на касте в SQL
            uids_uuid, bad_uuid = _to_uuid_list(uids)
            if bad_uuid:
                log.warning(
                    "⚠️ batch(strategy=%s): %d position_uid не смогли конвертироваться в UUID для positions_log_v4",
                    strategy_id,
                    bad_uuid,
                )

            if uids_uuid:
                pos_logs_status = await conn.execute(
                    """
                    DELETE FROM public.positions_log_v4
                     WHERE position_uid = ANY ($1::uuid[])
                    """,
                    uids_uuid,
                )
                pos_logs_deleted = _rows_affected(pos_logs_status)

            # логи сигналов по позиции
            signal_logs_status = await conn.execute(
                """
                DELETE FROM public.signal_log_entries_v4
                 WHERE position_uid = ANY ($1::text[])
                """,
                uids,
            )
            signal_logs_deleted = _rows_affected(signal_logs_status)

            # сами позиции (страховка по статусу)
            pos_status = await conn.execute(
                """
                DELETE FROM public.positions_v4
                 WHERE position_uid = ANY ($1::text[])
                   AND status = 'closed'
                """,
                uids,
            )
            pos_deleted = _rows_affected(pos_status)

    log.info(
        "🧹 batch(strategy=%s): positions=%d, targets=%d, pos_logs=%d, signal_logs=%d (uids=%d)",
        strategy_id,
        pos_deleted,
        targets_deleted,
        pos_logs_deleted,
        signal_logs_deleted,
        len(uids),
    )
    return pos_deleted, targets_deleted, pos_logs_deleted, signal_logs_deleted


# 🔸 Выключить стратегию, оповестить системы и удалить её
async def _disable_and_drop_strategy(strategy_id: int) -> int:
    """
    Возвращает: сколько строк стратегии удалено (0/1)
    """
    # выключить стратегию в БД
    await infra.pg_pool.execute(
        "UPDATE strategies_v4 SET enabled = FALSE WHERE id = $1",
        strategy_id,
    )

    # Pub/Sub оповещение о выключении (формат как в UI)
    event = {
        "id": strategy_id,
        "type": "enabled",
        "action": "false",
        "source": "cleaner",
    }
    await infra.redis_client.publish("strategies_v4_events", json.dumps(event))
    log.info("📨 [PubSub] Отключение стратегии id=%s", strategy_id)

    # пауза — даём слушателям (LAB/филлеру) «додренить» in-flight операции
    log.info("⏳ Пауза перед удалением стратегии id=%s: %ss", strategy_id, DELETE_GRACE_SEC)
    await asyncio.sleep(DELETE_GRACE_SEC)

    # финальный DELETE стратегии — в транзакции
    async with infra.pg_pool.acquire() as conn:
        async with conn.transaction():
            # удаляем запись стратегии
            del_status = await conn.execute(
                "DELETE FROM public.strategies_v4 WHERE id = $1",
                strategy_id,
            )
            deleted = _rows_affected(del_status)

    log.info("🗑️ Стратегия удалена из БД: id=%s (rows=%d)", strategy_id, deleted)
    return deleted


# 🔸 Обработка одной стратегии в deathrow
async def _process_strategy(strategy_id: int) -> Tuple[int, int]:
    """
    Возвращает: (сколько позиций удалено, была_ли_стратегия_удалена_строками)
    """
    total_pos_deleted = 0
    total_targets_deleted = 0
    total_pos_logs_deleted = 0
    total_signal_logs_deleted = 0
    batches = 0

    # удаляем закрытые позиции батчами
    while True:
        uids = await _fetch_closed_position_uids(strategy_id, BATCH_LIMIT)
        if not uids:
            break

        # удаление батча
        pos_deleted, targets_deleted, pos_logs_deleted, signal_logs_deleted = await _delete_positions_batch(strategy_id, uids)
        batches += 1

        total_pos_deleted += pos_deleted
        total_targets_deleted += targets_deleted
        total_pos_logs_deleted += pos_logs_deleted
        total_signal_logs_deleted += signal_logs_deleted

        # если батч меньше лимита — вероятно, закрытых позиций больше нет
        if len(uids) < BATCH_LIMIT:
            break

    # проверяем оставшиеся позиции
    n_closed, n_active = await _get_position_counts(strategy_id)
    log.info(
        "ℹ️ Стратегия %s: итог по позициям: batches=%d, deleted_pos=%d (targets=%d, pos_logs=%d, signal_logs=%d), осталось closed=%d, active=%d",
        strategy_id,
        batches,
        total_pos_deleted,
        total_targets_deleted,
        total_pos_logs_deleted,
        total_signal_logs_deleted,
        n_closed,
        n_active,
    )

    # если вообще ничего не осталось — отключаем и удаляем стратегию
    if n_closed == 0 and n_active == 0:
        log.info("✅ Стратегия %s: позиций не осталось — отключаем и удаляем", strategy_id)
        deleted_rows = await _disable_and_drop_strategy(strategy_id)
        return total_pos_deleted, deleted_rows

    # иначе — есть активные/partial, ждём следующего цикла
    if n_active > 0 and n_closed == 0:
        log.info("⏸️ Стратегия %s: есть активные позиции (%d), повторная проверка через %d секунд", strategy_id, n_active, SLEEP_CYCLE_SEC)
    elif n_closed > 0:
        log.info("⏸️ Стратегия %s: остались закрытые позиции (%d), повторная проверка через %d секунд", strategy_id, n_closed, SLEEP_CYCLE_SEC)

    return total_pos_deleted, 0


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
            total_strategies_processed = 0

            for sid in strategy_ids:
                try:
                    total_strategies_processed += 1
                    pos_deleted, strategy_deleted_rows = await _process_strategy(int(sid))
                    total_positions_deleted += pos_deleted
                    if strategy_deleted_rows > 0:
                        total_strategies_deleted += 1
                except Exception:
                    log.exception("❌ Ошибка обработки стратегии id=%s", sid)

            log.info(
                "📊 Итог прохода: processed=%d, strategies_deleted=%d, positions_deleted=%d",
                total_strategies_processed,
                total_strategies_deleted,
                total_positions_deleted,
            )

        except Exception:
            log.exception("❌ Критическая ошибка в цикле cleaner")

        # всегда ждём до следующего прохода
        await asyncio.sleep(SLEEP_CYCLE_SEC)