import os
import json
import logging
import asyncio
import asyncpg
import redis.asyncio as aioredis
from decimal import Decimal

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("CRON_TREASURY")

pg_pool = None
redis_client = None


# 🔸 PostgreSQL
async def setup_pg():
    global pg_pool
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("❌ DATABASE_URL не задан")
    pg_pool = await asyncpg.create_pool(db_url)
    await pg_pool.execute("SELECT 1")
    log.info("🛢️ Подключение к PostgreSQL установлено")


# 🔸 Redis
async def setup_redis_client():
    global redis_client
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", 6379))
    password = os.getenv("REDIS_PASSWORD")
    use_tls = os.getenv("REDIS_USE_TLS", "false").lower() == "true"
    protocol = "rediss" if use_tls else "redis"
    redis_url = f"{protocol}://{host}:{port}"

    client = aioredis.from_url(redis_url, password=password, decode_responses=True)
    await client.ping()
    redis_client = client
    log.info("📡 Подключение к Redis установлено")


# 🔸 Основная логика обработки стратегий
async def run():
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT s.id, s.deposit, s.max_risk, s.position_limit,
                   t.strategy_deposit, t.pnl_operational, t.pnl_insurance
            FROM strategies_v4 s
            JOIN strategies_treasury_v4 t ON s.id = t.strategy_id
            WHERE s.enabled = true AND s.auditor_enabled = true
        """)

        total = len(rows)
        if total == 0:
            log.info("ℹ️ Активных и разрешённых стратегий не найдено — ничего не делаем")
            return

        log.info(f"🔍 Найдено {total} стратегий для анализа и обновления")

        for r in rows:
            sid = r["id"]
            deposit = Decimal(r["deposit"])
            max_risk = Decimal(r["max_risk"])
            strategy_deposit = Decimal(r["strategy_deposit"])
            op = Decimal(r["pnl_operational"])
            ins = Decimal(r["pnl_insurance"])

            log.info(f"📄 Стратегия {sid}: deposit={deposit:.2f}, op={op:.2f}, ins={ins:.2f}")

            try:
                async with conn.transaction():
                    threshold = (strategy_deposit * Decimal("0.01")).quantize(Decimal("0.01"))

                    # 🔹 Сценарий 1
                    if op >= threshold:
                        amount = (threshold // Decimal("10")) * Decimal("10")
                        new_deposit = deposit + amount
                        new_limit = int(new_deposit // Decimal("10"))
                        new_op = op - amount

                        await conn.execute("""
                            UPDATE strategies_v4
                            SET deposit = $1, position_limit = $2
                            WHERE id = $3
                        """, new_deposit, new_limit, sid)

                        await conn.execute("""
                            UPDATE strategies_treasury_v4
                            SET pnl_operational = $1
                            WHERE strategy_id = $2
                        """, new_op, sid)

                        await conn.execute("""
                            INSERT INTO strategies_treasury_meta_log_v4 (
                                strategy_id, scenario, comment
                            )
                            VALUES ($1, 'transfer', $2)
                        """, sid,
                            f"Переведено {amount:.2f} из кассы в депозит. "
                            f"Новый депозит: {new_deposit:.2f}, лимит: {new_limit}")

                        log.info(f"✅ Переведено {amount:.2f} из кассы → депозит: {new_deposit:.2f}, лимит: {new_limit}")

                        await redis_client.xadd("strategy_update_stream", {
                            "id": str(sid),
                            "type": "strategy",
                            "action": "update",
                            "source": "treasury_cron"
                        })

                        continue

                    # 🔹 Сценарий 2
                    if op > 0:
                        await conn.execute("""
                            INSERT INTO strategies_treasury_meta_log_v4 (
                                strategy_id, scenario, comment
                            )
                            VALUES ($1, 'noop', $2)
                        """, sid,
                            f"Недостаточно средств в кассе. Требуется ≥ {threshold:.2f}, доступно: {op:.2f}")
                        log.info(f"⏸ Пропуск — в кассе {op:.2f} < порог {threshold:.2f}")
                        continue

                    # 🔹 Сценарий 3
                    if op == 0 and ins < 0:
                        loss = abs(ins)
                        risk_limit = strategy_deposit * (max_risk / Decimal("100"))

                        if loss <= risk_limit:
                            rounded_loss = ((loss + Decimal("9")) // Decimal("10")) * Decimal("10")
                            new_deposit = deposit - rounded_loss
                            new_limit = int(new_deposit // Decimal("10"))

                            await conn.execute("""
                                UPDATE strategies_v4
                                SET deposit = $1, position_limit = $2
                                WHERE id = $3
                            """, new_deposit, new_limit, sid)

                            await conn.execute("""
                                UPDATE strategies_treasury_v4
                                SET pnl_insurance = 0
                                WHERE strategy_id = $1
                            """, sid)

                            await conn.execute("""
                                INSERT INTO strategies_treasury_meta_log_v4 (
                                    strategy_id, scenario, comment
                                )
                                VALUES ($1, 'reduction', $2)
                            """, sid,
                                f"Списано {rounded_loss:.2f} из депозита для покрытия убытка. "
                                f"Новый депозит: {new_deposit:.2f}, лимит: {new_limit}")

                            log.info(f"✅ Списание {rounded_loss:.2f} из депозита → депозит: {new_deposit:.2f}, лимит: {new_limit}")

                            await redis_client.xadd("strategy_update_stream", {
                                "id": str(sid),
                                "type": "strategy",
                                "action": "update",
                                "source": "treasury_cron"
                            })

                            continue

                        # 🔹 Сценарий 4
                        await conn.execute("""
                            UPDATE strategies_v4
                            SET enabled = false
                            WHERE id = $1
                        """, sid)

                        await conn.execute("""
                            INSERT INTO strategies_treasury_meta_log_v4 (
                                strategy_id, scenario, comment
                            )
                            VALUES ($1, 'disabled', $2)
                        """, sid,
                            f"Отключена стратегия: убыток {loss:.2f} > лимит {risk_limit:.2f}")

                        log.info(f"🛑 Отключена стратегия — убыток {loss:.2f} > лимит {risk_limit:.2f}")

                        await redis_client.publish("strategies_v4_events", json.dumps({
                            "id": sid,
                            "type": "enabled",
                            "action": "false",
                            "source": "treasury_cron"
                        }))

            except Exception as e:
                log.exception(f"❌ Ошибка при обработке стратегии {sid}: {e}")
                raise


# 🔸 Запуск
async def main():
    await setup_pg()
    await setup_redis_client()
    await run()


if __name__ == "__main__":
    asyncio.run(main())