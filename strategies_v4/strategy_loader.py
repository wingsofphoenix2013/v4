# strategy_loader.py

import pkgutil
import importlib
import inspect
import logging

# 🔸 Логгер загрузчика стратегий
log = logging.getLogger("STRATEGY_LOADER")

# 🔸 Загрузка всех стратегий из папки strategies/
def load_strategies():
    import strategies  # используется как пакет внутри текущей директории

    strategy_registry = {}

    for finder, modname, ispkg in pkgutil.iter_modules(strategies.__path__):
        if not modname.startswith("strategy_"):
            continue

        full_module_name = f"strategies.{modname}"

        try:
            module = importlib.import_module(full_module_name)
            expected_class_name = _derive_class_name(modname)

            strategy_class = None
            for name, obj in inspect.getmembers(module, inspect.isclass):
                log.debug(f"🧪 Модуль: {modname}, класс: {name}, obj.__module__ = {obj.__module__}")
                
                if name == expected_class_name and obj.__module__ == full_module_name:
                    strategy_class = obj
                    break

            if strategy_class is None:
                log.warning(f"⚠️ Класс {expected_class_name} не найден в модуле {modname}")
                continue

            if not all(hasattr(strategy_class, method) for method in ["validate_signal", "run"]):
                log.warning(f"⚠️ Стратегия {modname} не реализует обязательные методы")
                continue

            if modname in strategy_registry:
                log.warning(f"⚠️ Конфликт: стратегия {modname} уже загружена, пропускаем")
                continue

            strategy_registry[modname] = strategy_class()
            log.info(f"✅ Стратегия загружена: {modname} → {expected_class_name}")

        except Exception as e:
            log.exception(f"❌ Ошибка загрузки стратегии {modname}: {e}")

    # 🔸 Финальный лог
    log.info(f"✅ Загружено стратегий: {len(strategy_registry)}")
    return strategy_registry

# 🔸 Преобразование имени модуля в имя класса
def _derive_class_name(modname: str) -> str:
    parts = modname.replace("strategy_", "").split("_")
    return "Strategy" + "".join(p.capitalize() for p in parts)