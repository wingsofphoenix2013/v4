import logging

log = logging.getLogger("ema")

# Получение и логирование входных данных для расчёта EMA
def ema(prices, period):
    """
    Функция-обёртка для расчёта EMA.
    На этом этапе только логирует первые элементы массива цен и период.
    prices — массив close-цен (list of float)
    period — период EMA (int)
    Возвращает заглушку: список с одним элементом 0.0
    """
    log.info(f"Получен вызов ema(): первые 5 цен: {prices[:5]}... всего: {len(prices)}, period={period}")
    # TODO: Реализовать фактический расчёт EMA
    return [0.0]