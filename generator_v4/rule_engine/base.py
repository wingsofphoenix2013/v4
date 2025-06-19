# base.py

import abc
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from infra import infra  # для доступа к Redis

log = logging.getLogger("RULE_BASE")


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
    async def update(self, open_time: datetime) -> Optional[SignalResult]:
        """
        Основная логика генерации сигнала.
        Получает только open_time — остальные данные правило загружает само.
        Возвращает объект SignalResult или None.
        """
        pass

    # 🔸 Получение временного ряда значений индикатора из Redis TS
    async def fetch_indicator_series(
        self, param: str, length: int, open_time: datetime
    ) -> list[float]:
        """
        Загружает `length` значений индикатора до и включая `open_time`
        """
        redis = infra.redis_client
        key = f"ts_ind:{self.symbol}:{self.timeframe}:{param}"

        end_ts = int(open_time.timestamp() * 1000)
        start_ts = end_ts - (length - 1) * 60_000  # предположительно 1 точка = 1 минута

        try:
            points = await redis.tsrange(key, start_ts, end_ts)
            return [float(v.decode() if isinstance(v, bytes) else v) for _, v in points]
        except Exception as e:
            log.warning(f"[RULE_BASE] ⚠️ Ошибка при запросе {key}: {e}")
            return []