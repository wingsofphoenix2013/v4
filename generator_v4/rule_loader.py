# rule_loader.py

import importlib
import logging

from infra import SIGNAL_CONFIGS, RULE_DEFINITIONS, ENABLED_TICKERS

log = logging.getLogger("GEN")

# 🔸 Глобальный реестр инстансов правил
# Ключ: (rule_name, symbol, timeframe) → объект SignalRule
RULE_INSTANCES = {}

# 🔸 Автоматическая загрузка классов и создание инстансов
def load_signal_rule_instances():
    global RULE_INSTANCES
    loaded = 0

    for signal in SIGNAL_CONFIGS:
        rule_name = signal["rule"]
        if not rule_name or rule_name not in RULE_DEFINITIONS:
            log.warning(f"[RULE_LOADER] Пропущен сигнал '{signal['name']}' — нет правила '{rule_name}'")
            continue

        rule_def = RULE_DEFINITIONS[rule_name]
        module_name = rule_def["module_name"]
        class_name = rule_def["class_name"]

        try:
            module = importlib.import_module(f"rule_engine.{module_name}")
            rule_class = getattr(module, class_name)
            log.info(f"[RULE_LOADER] Импортирован класс: {module_name}.{class_name}")
        except Exception as e:
            log.exception(f"[RULE_LOADER] ❌ Ошибка импорта {module_name}.{class_name}: {e}")
            continue

        timeframe = signal["timeframe"]
        signal_id = signal["id"]

        for symbol in ENABLED_TICKERS:
            key = (rule_name, symbol, timeframe)
            log.info(f"[RULE_LOADER] Создание: {class_name}(symbol='{symbol}', timeframe='{timeframe}', signal_id={signal_id})")
            try:
                instance = rule_class(symbol=symbol, timeframe=timeframe, signal_id=signal_id)
                RULE_INSTANCES[key] = instance
                loaded += 1
            except Exception as e:
                log.exception(f"[RULE_LOADER] ❌ Ошибка создания инстанса {class_name} для {symbol}/{timeframe}: {e}")

    log.info(f"[RULE_LOADER] Загружено инстансов правил: {loaded}")