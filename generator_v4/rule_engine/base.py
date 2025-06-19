# base.py

import abc
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

log = logging.getLogger("GEN")

# 🔸 Результат выполнения правила
@dataclass
class SignalResult:
    signal_id: int              # ID из signals_v4
    direction: str              # 'long' или 'short'
    reason: str                 # человеко-читаемое объяснение
    details: dict               # значения индикаторов, вычисления и т.п.

# 🔸 Базовый интерфейс для правил генерации сигналов
class SignalRule(abc.ABC):
    def __init__(self, symbol: str, timeframe: str, signal_id: int):
        self.symbol = symbol
        self.timeframe = timeframe
        self.signal_id = signal_id

    @abc.abstractmethod
    def required_indicators(self) -> list[str]:
        """
        Возвращает список параметров индикаторов, которые требуются для работы правила.
        Пример: ["ema9", "ema21"]
        """
        pass

    @abc.abstractmethod
    async def update(
        self,
        open_time: datetime,
        indicator_values: dict[str, float]
    ) -> Optional[SignalResult]:
        """
        Основная логика генерации сигнала.
        Возвращает объект SignalResult или None.
        """
        pass