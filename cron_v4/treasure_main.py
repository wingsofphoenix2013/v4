import os
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


# 🔸 Диагностика казначейских сценариев
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

        log.info(f"🔍 Найдено {total} стратегий для анализа")

        for r in rows:
            sid = r["id"]
            deposit = Decimal(r["deposit"])
            max_risk = Decimal(r["max_risk"])
            strategy_deposit = Decimal(r["strategy_deposit"])
            op = Decimal(r["pnl_operational"])
            ins = Decimal(r["pnl_insurance"])

            log.info(f"🧾 Стратегия {sid}")
            log.info(f"  ➤ deposit: {deposit}, strategy_deposit: {strategy_deposit}")
            log.info(f"  ➤ pnl_operational: {op}, pnl_insurance: {ins}")
            log.info(f"  ➤ max_risk: {max_risk}%")

            threshold = (strategy_deposit * Decimal("0.01")).quantize(Decimal("0.01"))
            log.info(f"  ➤ 1% от strategy_deposit: {threshold}")

            if op >= threshold:
                amount = (threshold // Decimal("10")) * Decimal("10")
                new_deposit = deposit + amount
                new_limit = int(new_deposit // Decimal("10"))
                log.info(f"  → сценарий 1: перевести {amount} из кассы в депозит → новый депозит: {new_deposit}, лимит: {new_limit}")
                continue

            if op > 0:
                log.info(f"  → сценарий 2: недостаточно средств для перевода (только {op:.2f}, нужно ≥ {threshold:.2f})")
                continue

            if op == 0 and ins < 0:
                loss = abs(ins)
                risk_limit = strategy_deposit * (max_risk / Decimal("100"))
                log.info(f"  ➤ убыток в фонде: {loss:.2f}, допустимый лимит: {risk_limit:.2f}")
                if loss <= risk_limit:
                    rounded_loss = ((loss + Decimal("9")) // Decimal("10")) * Decimal("10")
                    new_deposit = deposit - rounded_loss
                    new_limit = int(new_deposit // Decimal("10"))
                    log.info(f"  → сценарий 3: списание {rounded_loss} из депозита, новый депозит: {new_deposit}, лимит: {new_limit}")
                else:
                    log.info(f"  → сценарий 4: стратегия должна быть отключена (убыток {loss:.2f} > лимит {risk_limit:.2f})")
            else:
                log.info(f"  → никаких действий не требуется")


# 🔸 Запуск
async def main():
    await setup_pg()
    await setup_redis_client()
    await run()


if __name__ == "__main__":
    asyncio.run(main())