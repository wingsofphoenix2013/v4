from .signals import router as signals_router
from .trades import router as trades_router
from .strategies import router as strategies_router
from fastapi.templating import Jinja2Templates

routers = [signals_router, trades_router, strategies_router]

# 🔸 Инициализация внешних зависимостей для модулей
def init_dependencies(pg, redis, templates: Jinja2Templates):
    from . import signals, trades, strategies

    # для signals
    signals.pg_pool = pg
    signals.redis_client = redis
    signals.templates = templates

    # для trades
    trades.pg_pool = pg
    trades.templates = templates

    # для strategies
    strategies.pg_pool = pg
    strategies.redis_client = redis
    strategies.templates = templates