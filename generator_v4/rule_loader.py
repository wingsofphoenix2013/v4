import importlib
import logging

from infra import infra

log = logging.getLogger("GEN")

# 🔸 Автоматическая загрузка классов и создание инстансов
def load_signal_rule_instances():
    loaded = 0

    log.info(f"[RULE_LOADER] SIGNAL_CONFIGS: {len(infra.signal_configs)}")

    for signal in infra.signal_configs:
        rule_name = signal.get("rule")
        log.info(f"[RULE_LOADER] Обработка сигнала: {signal.get('name', '<unnamed>')} → rule={rule_name}")

        if not rule_name or rule_name not in infra.rule_definitions:
            log.warning(f"[RULE_LOADER] Пропущен сигнал '{signal.get('name')}' — нет правила '{rule_name}'")
            continue

        rule_def = infra.rule_definitions[rule_name]
        module_name = rule_def["module_name"]
        class_name = rule_def["class_name"]

        try:
            module = importlib.import_module(f"rule_engine.{module_name}")
            rule_class = getattr(module, class_name)
            log.info(f"[RULE_LOADER] Импортирован класс: {module_name}.{class_name}")
        except Exception as e:
            log.exception(f"[RULE_LOADER] ❌ Ошибка импорта {module_name}.{class_name}: {e}")
            continue

        timeframe = signal.get("timeframe")
        signal_id = signal.get("id")

        for symbol in infra.enabled_tickers:
            key = (rule_name, symbol, timeframe)
            log.info(f"[RULE_LOADER] Создание: {class_name}(symbol='{symbol}', timeframe='{timeframe}', signal_id={signal_id})")
            try:
                instance = rule_class(symbol=symbol, timeframe=timeframe, signal_id=signal_id)
                infra.rule_instances[key] = instance
                loaded += 1
            except Exception as e:
                log.exception(f"[RULE_LOADER] ❌ Ошибка создания инстанса {class_name} для {symbol}/{timeframe}: {e}")

    log.info(f"[RULE_LOADER] Загружено инстансов правил: {loaded}")