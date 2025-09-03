# 🔸 Точка входа лаборатории: планирование, параллельные раны (по 10), батч-обработка позиций (по 500), финальные сигналы

import os
import asyncio
import logging
from datetime import datetime, timezone

import laboratory_v4_infra as infra
import laboratory_v4_loader as loader

log = logging.getLogger("LAB_MAIN")

LAB_LOOP_SLEEP_SEC = int(os.getenv("LAB_LOOP_SLEEP_SEC", "3600"))  # интервал «сон/просыпание»


# 🔸 Обработка одного рана (lab_id × strategy_id)
async def process_run(lab: dict, strategy_id: int):
    lab_id = int(lab["lab_id"])
    run_id = await infra.create_run(lab_id, strategy_id)
    lock_key = f"lab:run:lock:{lab_id}:{strategy_id}"

    try:
        async with infra.redis_lock(lock_key, ttl_sec=infra.LOCK_TTL_SEC):
            await infra.mark_run_started(run_id)

            # параметры теста (для порядка, ADX/BB и т. п. будут читать свои агрегаты в профильных воркерах)
            params = await loader.load_lab_parameters(lab_id)
            log.debug("Старт ранa lab_id=%s strategy_id=%s run_id=%s components=%d", lab_id, strategy_id, run_id, len(params))

            cutoff = datetime.now(timezone.utc)
            processed = approved = filtered = skipped = 0
            batch_uids: list[str] = []

            # внутренний обработчик пачки
            async def process_batch(uids: list[str]):
                nonlocal processed, approved, filtered, skipped
                if not uids:
                    return
                # Здесь в будущем:
                # 1) подтянуть PIS для uids (только нужные поля под профильный фильтр)
                # 2) принять решения (approved/filtered/skipped_no_data) в Python
                # 3) записать batch результатов в laboratory_results_v4
                # Пока — заглушка на прогресс.
                processed += len(uids)

            # итерация по позициям пачками (batch=infra.POSITIONS_BATCH)
            async for uid in loader.iter_closed_positions_uids(strategy_id, cutoff, infra.POSITIONS_BATCH):
                batch_uids.append(uid)
                if len(batch_uids) >= infra.POSITIONS_BATCH:
                    await process_batch(batch_uids)
                    batch_uids.clear()

                # периодически обновляем прогресс
                if processed % 100 == 0:
                    await infra.update_progress_json(run_id, {
                        "cutoff_at": cutoff.isoformat(),
                        "processed": processed,
                        "approved": approved,
                        "filtered": filtered,
                        "skipped_no_data": skipped,
                    })

            # «хвост» неполной пачки
            if batch_uids:
                await process_batch(batch_uids)
                batch_uids.clear()

            await infra.update_progress_json(run_id, {
                "cutoff_at": cutoff.isoformat(),
                "processed": processed,
                "approved": approved,
                "filtered": filtered,
                "skipped_no_data": skipped,
            })

            await infra.mark_run_finished(run_id)
            await infra.send_finish_signal(lab_id, strategy_id, run_id)
            log.info("RUN DONE lab=%s strategy=%s run_id=%s processed=%s", lab_id, strategy_id, run_id, processed)

    except asyncio.CancelledError:
        log.info("Остановка ранa lab=%s strategy=%s run_id=%s по сигналу", lab_id, strategy_id, run_id)
        raise
    except Exception as e:
        log.exception("Ошибка ранa lab=%s strategy=%s run_id=%s: %s", lab_id, strategy_id, run_id, e)
        await infra.mark_run_failed(run_id, reason=str(e))


# 🔸 Обёртка для семафора (не более 10 одновременных ранoв)
async def run_guarded(lab: dict, sid: int):
    await infra.concurrency_sem.acquire()
    try:
        await process_run(lab, sid)
    finally:
        infra.concurrency_sem.release()


# 🔸 Планировщик запусков (периодический рефреш активных тестов/стратегий)
async def scheduler_loop():
    while True:
        try:
            plan = await loader.build_run_plan()
            tasks: list[asyncio.Task] = []

            for lab_id, sid in plan:
                lab_dict = {"lab_id": lab_id}  # компактно передаём только нужное
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

    await scheduler_loop()


if __name__ == "__main__":
    asyncio.run(main())