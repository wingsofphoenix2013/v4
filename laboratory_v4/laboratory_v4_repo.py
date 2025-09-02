# laboratory_v4_repo.py — data access для Laboratory v4

async def load_active_strategies(pg):
    async with pg.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, enabled, COALESCE(market_watcher, false) AS mw
            FROM strategies_v4
            WHERE enabled = true AND COALESCE(market_watcher, false) = true
            ORDER BY id
        """)
    return [{"id": int(r["id"]), "enabled": bool(r["enabled"]), "market_watcher": bool(r["mw"])} for r in rows]

async def load_active_tickers(pg):
    async with pg.acquire() as conn:
        rows = await conn.fetch("""
            SELECT symbol, precision_price
            FROM tickers_v4
            WHERE status = 'enabled' AND tradepermission = 'enabled'
            ORDER BY symbol
        """)
    return [{"symbol": r["symbol"], "precision_price": int(r["precision_price"])} for r in rows]