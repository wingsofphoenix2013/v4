import logging
import infra

log = logging.getLogger("REDIS_IO")

RETENTION_MS = 5184000000  # 60 дней

# 🔸 Обновление RETENTION для всех Redis TS ключей
async def update_retention_for_all_ts_keys():
    try:
        keys = await infra.redis_client.keys("ts:*:*:*")
        log.info(f"🔍 Найдено ключей: {len(keys)}")

        updated = 0
        for key in keys:
            try:
                await infra.redis_client.execute_command("TS.ALTER", key, "RETENTION", RETENTION_MS)
                log.info(f"✅ Обновлён: {key}")
                updated += 1
            except Exception as e:
                log.warning(f"⚠️ Ошибка на {key}: {e}")

        log.info(f"🏁 Завершено: обновлено {updated} из {len(keys)} ключей")

    except Exception:
        log.exception("❌ Общая ошибка при обновлении Redis TS retention")

# 🔸 Обёртка для временного подключения в main
async def redis_task():
    await update_retention_for_all_ts_keys()