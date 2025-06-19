# emacross_simple.py

import logging
from datetime import datetime
from typing import Optional

from rule_engine.base import SignalRule, SignalResult

log = logging.getLogger("EMACROSS_SIMPLE")

# 🔸 Правило генерации сигнала: EMA9 пересекает EMA21
class EmaCrossSimple(SignalRule):
    def required_indicators(self) -> list[str]:
        return ["ema9", "ema21"]

    async def update(
        self,
        open_time: datetime,
        indicator_values: dict[str, float]
    ) -> Optional[SignalResult]:
        if not all(k in indicator_values for k in self.required_indicators()):
            log.warning(f"[EMACROSS_SIMPLE] ❌ Не хватает параметров: {indicator_values}")
            return None

        ema9 = indicator_values["ema9"]
        ema21 = indicator_values["ema21"]

        if ema9 > ema21:
            log.info(f"[EMACROSS_SIMPLE] ✅ Сигнал LONG: EMA9 ({ema9}) > EMA21 ({ema21})")
            return SignalResult(
                signal_id=self.signal_id,
                direction="long",
                reason=f"EMA9 ({ema9}) пересёк EMA21 ({ema21}) вверх",
                details={"ema9": ema9, "ema21": ema21}
            )
        elif ema9 < ema21:
            log.info(f"[EMACROSS_SIMPLE] ✅ Сигнал SHORT: EMA9 ({ema9}) < EMA21 ({ema21})")
            return SignalResult(
                signal_id=self.signal_id,
                direction="short",
                reason=f"EMA9 ({ema9}) пересёк EMA21 ({ema21}) вниз",
                details={"ema9": ema9, "ema21": ema21}
            )

        log.info(f"[EMACROSS_SIMPLE] ℹ️ Пересечения нет: EMA9 = EMA21 = {ema9}")
        return None