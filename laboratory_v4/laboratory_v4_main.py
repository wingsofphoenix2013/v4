# 🔸 Точка входа лаборатории: планирование, параллельные раны (по 10), батч-обработка позиций (по 500), финальные сигналы

import os
import asyncio
import logging
from datetime import datetime

import laboratory_v4_infra as infra
import laboratory_v4_loader as loader
import laboratory_v4_results_aggregator as results_agg
import laboratory_v4_adx_worker as adx
import laboratory_v4_seeder as seeder

log = logging.getLogger("LAB_MAIN")

LAB_LOOP_SLEEP_SEC = int(os.getenv("LAB_LOOP_SLEEP_SEC", "21600"))


# 🔸 Обработка одного рана (lab_id × strategy_id)
async def process_run(lab: dict, strategy_id: int):
    lab_id = int(lab["lab_id"])
    lock_key = f"lab:run:lock:{lab_id}:{strategy_id}"

    try:
        try:
            async with infra.redis_lock(lock_key, ttl_sec=infra.LOCK_TTL_SEC):
                # 1) создаём run
                run_id = await infra.create_run(lab_id, strategy_id)
                await infra.mark_run_started(run_id)

                # 2) грузим конфигурацию теста (пороги min_trade_type/value, min_winrate)
                async with infra.pg_pool.acquire() as conn:
                    lab_cfg_row = await conn.fetchrow(
                        """
                        SELECT id AS lab_id, name, min_trade_type, min_trade_value, min_winrate
                        FROM laboratory_instances_v4
                        WHERE id = $1
                        """,
                        lab_id,
                    )

                if not lab_cfg_row:
                    log.error("Нет конфигурации laboratory_instances_v4 для lab_id=%s — ран пропущен", lab_id)
                    await infra.mark_run_finished(run_id)
                    await infra.send_finish_signal(lab_id, strategy_id, run_id)
                    return

                lab_cfg = dict(lab_cfg_row)

                # 🔸 обновляем last_used для этого теста
                async with infra.pg_pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE laboratory_instances_v4 SET last_used = NOW() WHERE id=$1",
                        lab_id,
                    )
                    
                # 3) компоненты теста (для упорядочивания проверки)
                params = await loader.load_lab_parameters(lab_id)
                log.debug(
                    "Старт ранa lab_id=%s strategy_id=%s run_id=%s components=%d",
                    lab_id, strategy_id, run_id, len(params)
                )

                # 4) фиксируем точку отсечения и подготавливаем кэши (ADX агрегаты + тоталы по направлениям)
                cutoff = datetime.now()
                per_tf_cache, comp_cache = await adx.load_adx_aggregates_for_strategy(strategy_id)
                totals_by_dir = await adx.load_total_closed_by_direction(strategy_id, cutoff)

                # 5) счётчики и батч-буфер
                processed = approved = filtered = skipped = 0
                batch_uids: list[str] = []

                # внутренний обработчик пачки (реальная работа через ADX-воркер)
                async def process_batch(uids: list[str]):
                    nonlocal processed, approved, filtered, skipped
                    if not uids:
                        return
                    a, f, s = await adx.process_adx_batch(
                        lab=lab_cfg,
                        strategy_id=strategy_id,
                        run_id=run_id,
                        cutoff=cutoff,
                        lab_params=params,
                        position_uids=uids,
                        per_tf_cache=per_tf_cache,
                        comp_cache=comp_cache,
                        totals_by_dir=totals_by_dir,
                    )
                    processed += len(uids)
                    approved  += a
                    filtered  += f
                    skipped   += s

                    # прогресс после каждой пачки
                    await infra.update_progress_json(run_id, {
                        "cutoff_at": cutoff.isoformat(),
                        "processed": processed,
                        "approved": approved,
                        "filtered": filtered,
                        "skipped_no_data": skipped,
                    })

                # 6) проходим закрытые позиции пачками
                async for uid in loader.iter_closed_positions_uids(strategy_id, cutoff, infra.POSITIONS_BATCH):
                    batch_uids.append(uid)
                    if len(batch_uids) >= infra.POSITIONS_BATCH:
                        await process_batch(batch_uids)
                        batch_uids.clear()

                # хвост
                if batch_uids:
                    await process_batch(batch_uids)
                    batch_uids.clear()

                # 7) финальный прогресс + завершение + сигнал
                await infra.update_progress_json(run_id, {
                    "cutoff_at": cutoff.isoformat(),
                    "processed": processed,
                    "approved": approved,
                    "filtered": filtered,
                    "skipped_no_data": skipped,
                })
                await infra.mark_run_finished(run_id)
                await infra.send_finish_signal(lab_id, strategy_id, run_id)
                log.info(
                    "RUN DONE lab=%s strategy=%s run_id=%s processed=%s approved=%s filtered=%s skipped=%s",
                    lab_id, strategy_id, run_id, processed, approved, filtered, skipped
                )

        except RuntimeError as e:
            # лок занят — это не ошибка логики, просто другая корутина уже обрабатывает пару
            if str(e).startswith("lock_busy:"):
                log.debug("Пропуск: лок занят для lab=%s strategy=%s (%s)", lab_id, strategy_id, e)
                return
            raise

    except asyncio.CancelledError:
        log.info("Остановка ранa lab=%s strategy=%s по сигналу", lab_id, strategy_id)
        raise
    except Exception as e:
        log.exception("Ошибка ранa lab=%s strategy=%s: %s", lab_id, strategy_id, e)

# 🔸 Обёртка для семафора (не более N одновременных ранoв)
async def run_guarded(lab: dict, sid: int):
    await infra.concurrency_sem.acquire()
    try:
        await process_run(lab, sid)
    finally:
        infra.concurrency_sem.release()


# 🔸 Планировщик запусков (периодический рефреш активных тестов/стратегий)
async def scheduler_loop():
    
    await asyncio.sleep(120)
    
    while True:
        try:
            plan = await loader.build_run_plan()
            tasks: list[asyncio.Task] = []
            for lab_id, sid in plan:
                lab_dict = {"lab_id": lab_id}
                tasks.append(asyncio.create_task(run_guarded(lab_dict, sid)))
            if tasks:
                await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            log.info("Планировщик остановлен")
            raise
        except Exception:
            log.exception("Ошибка в цикле планировщика")
        await asyncio.sleep(LAB_LOOP_SLEEP_SEC)


# 🔸 main
async def main():
    infra.setup_logging()
    log.info("Запуск laboratory_v4 Background Worker")

    await infra.setup_pg()
    await infra.setup_redis_client()
    
    await seeder.run_adx_seeder()

    # Запускаем оба воркера под автоперезапуском
    await asyncio.gather(
        infra.run_safe_loop(scheduler_loop, "LAB_SCHEDULER"),
        infra.run_safe_loop(results_agg.run_laboratory_results_aggregator, "LAB_RESULTS_AGG"),
    )


if __name__ == "__main__":
    asyncio.run(main())