from .signals import router as signals_router
from fastapi.templating import Jinja2Templates

routers = [signals_router]

# 🔸 Инициализация внешних зависимостей для модулей
def init_dependencies(pg, redis, templates: Jinja2Templates):
    from . import signals
    signals.pg_pool = pg
    signals.redis_client = redis
    signals.templates = templates