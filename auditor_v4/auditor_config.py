# 🔸 auditor_config.py — стартовая загрузка auditor_v4: активные MW-стратегии (market_watcher=true)

# 🔸 Импорты
import logging
from typing import Dict

import auditor_infra as infra

# 🔸 Логгер
log = logging.getLogger("AUD_CONFIG")


# 🔸 Загрузка активных стратегий с признаком market_watcher=true
async def load_active_mw_strategies() -> Dict[int, dict]:
    # условия достаточности
    if infra.pg_pool is None:
        log.info("❌ Пропуск load_active_mw_strategies: PG не инициализирован")
        return {}

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, name, human_name
            FROM strategies_v4
            WHERE enabled = true
              AND (archived IS NOT TRUE)
              AND market_watcher = true
            """
        )

    strategies = {
        int(r["id"]): {
            "id": int(r["id"]),
            "name": str(r["name"] or ""),
            "human_name": str(r["human_name"] or ""),
        }
        for r in rows
    }

    log.info("✅ AUD: загружены активные MW-стратегии (%d)", len(strategies))
    return strategies