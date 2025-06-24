# emacross2150_simple.py

import logging
from datetime import datetime
from typing import Optional

from rule_engine.base import SignalRule, SignalResult

log = logging.getLogger("EMACROSS2150_SIMPLE")


# 🔸 Правило генерации сигнала: пересечение EMA21 и EMA50
class EmaCross2150Simple(SignalRule):
    def required_indicators(self) -> list[str]:
        return ["ema21", "ema50"]

    async def update(self, open_time: datetime) -> Optional[SignalResult]:
        try:
            ema21 = await self.fetch_indicator_series("ema21", 3, open_time)
            ema50 = await self.fetch_indicator_series("ema50", 3, open_time)

            if len(ema21) < 3 or len(ema50) < 3:
                log.info(f"[EMACROSS2150_SIMPLE] ⚠️ Недостаточно данных: {self.symbol}/{self.timeframe}")
                return None

            # используем последние 2 точки для анализа пересечения
            _, prev_ema21, curr_ema21 = ema21
            _, prev_ema50, curr_ema50 = ema50

            if prev_ema21 <= prev_ema50 and curr_ema21 > curr_ema50:
                log.info(f"[EMACROSS2150_SIMPLE] ✅ LONG сигнал для {self.symbol}/{self.timeframe}")
                return SignalResult(
                    signal_id=self.signal_id,
                    direction="long",
                    reason="EMA21 пересек EMA50 снизу вверх",
                    details={
                        "prev_ema21": prev_ema21,
                        "prev_ema50": prev_ema50,
                        "curr_ema21": curr_ema21,
                        "curr_ema50": curr_ema50,
                    },
                )

            if prev_ema21 >= prev_ema50 and curr_ema21 < curr_ema50:
                log.info(f"[EMACROSS2150_SIMPLE] ✅ SHORT сигнал для {self.symbol}/{self.timeframe}")
                return SignalResult(
                    signal_id=self.signal_id,
                    direction="short",
                    reason="EMA21 пересек EMA50 сверху вниз",
                    details={
                        "prev_ema21": prev_ema21,
                        "prev_ema50": prev_ema50,
                        "curr_ema21": curr_ema21,
                        "curr_ema50": curr_ema50,
                    },
                )

            log.debug(f"[EMACROSS2150_SIMPLE] ℹ️ Пересечения нет: {self.symbol}/{self.timeframe}")
            return None

        except Exception as e:
            log.info(f"[EMACROSS2150_SIMPLE] ❌ Ошибка update(): {e}")
            return None