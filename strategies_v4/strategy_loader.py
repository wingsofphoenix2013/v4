import importlib
import pkgutil
import inspect
import logging

# Папка с реализациями стратегий — как Python-пакет
import strategies as strategies_pkg

log = logging.getLogger("STRATEGY_LOADER")

def load_strategies():
    strategies = {}

    for _, modname, _ in pkgutil.iter_modules(strategies_pkg.__path__):
        if not modname.startswith("strategy_"):
            continue

        try:
            module = importlib.import_module(f"strategies_v4.strategies.{modname}")

            expected_class = modname.title().replace("_", "")
            for name, obj in inspect.getmembers(module, inspect.isclass):
                if name.lower() == expected_class.lower():
                    strategies[modname] = obj()
                    log.info(f"✅ Загрузка стратегии: {modname} → {name}")
                    break
            else:
                log.warning(f"⚠️ Стратегия {modname} — класс не найден")

        except Exception:
            log.exception(f"❌ Ошибка при загрузке {modname}")

    return strategies