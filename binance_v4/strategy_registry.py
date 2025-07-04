# strategy_registry.py

import logging
from infra import infra

log = logging.getLogger("STRATEGY_REGISTRY")

# 🔸 Множество ID стратегий, разрешённых к исполнению на Binance
binance_enabled_strategies: set[int] = set()

async def load_binance_enabled_strategies():
    query = "SELECT id FROM strategies_v4 WHERE binance_enabled = true"
    rows = await infra.pg_pool.fetch(query)
    binance_enabled_strategies.clear()
    binance_enabled_strategies.update(row["id"] for row in rows)
    log.info(f"📊 Загружено {len(binance_enabled_strategies)} стратегий с binance_enabled=true")

def is_strategy_binance_enabled(strategy_id: int) -> bool:
    return strategy_id in binance_enabled_strategies