# import asyncio
# import logging
# 
# import infra
# 
# log = logging.getLogger("REDIS_IO")
# 
# 
# # 🔸 Обновление RETENTION для всех Redis TS ключей
# async def update_retention_for_all_ts_keys():
#     try:
#         log.info("🔧 Начинаю обновление RETENTION для всех ключей Redis TS...")
# 
#         keys = await infra.redis_client.keys("ts:*:*:*")
#         log.info(f"🔍 Найдено ключей: {len(keys)}")
# 
#         updated = 0
#         for key in keys:
#             try:
#                 await infra.redis_client.execute_command("TS.ALTER", key, "RETENTION", 2592000000)
#                 log.info(f"✅ Обновлён: {key}")
#                 updated += 1
#             except Exception as e:
#                 log.warning(f"⚠️ Ошибка при обновлении {key}: {e}")
# 
#         log.info(f"🏁 Обновление завершено. Успешно обновлено: {updated} ключей из {len(keys)}")
# 
#     except Exception:
#         log.exception("❌ Ошибка при массовом обновлении RETENTION ключей Redis TS")
# 
# 
# # 🔸 Основной воркер (заглушка, если нужно вызвать через run_safe_loop)
# async def redis_task():
#     await update_retention_for_all_ts_keys()