# emacross_simple.py

from datetime import datetime
from typing import Optional

from rule_engine.base import SignalRule, SignalResult


# 🔸 Правило генерации сигнала: EMA9 пересекает EMA21
class EmaCrossSimple(SignalRule):
    def required_indicators(self) -> list[str]:
        # Требуемые индикаторы для работы правила
        return ["ema9", "ema21"]

    async def update(
        self,
        open_time: datetime,
        indicator_values: dict[str, float]
    ) -> Optional[SignalResult]:
        # Проверка наличия всех нужных значений
        if not all(k in indicator_values for k in self.required_indicators()):
            return None

        ema9 = indicator_values["ema9"]
        ema21 = indicator_values["ema21"]

        # Простая логика: если ema9 выше ema21 — сигнал на long
        if ema9 > ema21:
            return SignalResult(
                signal_id=self.signal_id,
                direction="long",
                reason=f"EMA9 ({ema9}) пересёк EMA21 ({ema21}) вверх",
                details={"ema9": ema9, "ema21": ema21}
            )
        elif ema9 < ema21:
            return SignalResult(
                signal_id=self.signal_id,
                direction="short",
                reason=f"EMA9 ({ema9}) пересёк EMA21 ({ema21}) вниз",
                details={"ema9": ema9, "ema21": ema21}
            )

        # Если они равны — нет сигнала
        return None