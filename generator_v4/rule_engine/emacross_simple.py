# emacross_simple.py

import logging
from datetime import datetime
from typing import Optional

from rule_engine.base import SignalRule, SignalResult

log = logging.getLogger("EMACROSS_SIMPLE")


# 🔸 Правило генерации сигнала: пересечение EMA9 и EMA21
class EmaCrossSimple(SignalRule):
    def required_indicators(self) -> list[str]:
        return ["ema9", "ema21"]

    # 🔸 Обработка сигнала на заданное open_time
    async def update(self, open_time: datetime) -> Optional[SignalResult]:
        try:
            ema9 = await self.fetch_indicator_series("ema9", 2, open_time)
            ema21 = await self.fetch_indicator_series("ema21", 2, open_time)

            if len(ema9) < 2 or len(ema21) < 2:
                log.info(f"[EMACROSS_SIMPLE] ⚠️ Недостаточно данных для расчёта пересечения: {self.symbol}/{self.timeframe}")
                return None

            prev_ema9, curr_ema9 = ema9
            prev_ema21, curr_ema21 = ema21

            if prev_ema9 < prev_ema21 and curr_ema9 > curr_ema21:
                log.info(f"[EMACROSS_SIMPLE] ✅ LONG сигнал для {self.symbol}/{self.timeframe}: "
                         f"prev_ema9={prev_ema9}, prev_ema21={prev_ema21}, curr_ema9={curr_ema9}, curr_ema21={curr_ema21}")
                return SignalResult(
                    signal_id=self.signal_id,
                    direction="long",
                    reason="EMA9 пересек EMA21 снизу вверх",
                    details={
                        "prev_ema9": prev_ema9,
                        "prev_ema21": prev_ema21,
                        "curr_ema9": curr_ema9,
                        "curr_ema21": curr_ema21
                    }
                )

            if prev_ema9 > prev_ema21 and curr_ema9 < curr_ema21:
                log.info(f"[EMACROSS_SIMPLE] ✅ SHORT сигнал для {self.symbol}/{self.timeframe}: "
                         f"prev_ema9={prev_ema9}, prev_ema21={prev_ema21}, curr_ema9={curr_ema9}, curr_ema21={curr_ema21}")
                return SignalResult(
                    signal_id=self.signal_id,
                    direction="short",
                    reason="EMA9 пересек EMA21 сверху вниз",
                    details={
                        "prev_ema9": prev_ema9,
                        "prev_ema21": prev_ema21,
                        "curr_ema9": curr_ema9,
                        "curr_ema21": curr_ema21
                    }
                )

            log.debug(
                f"[EMACROSS_SIMPLE] ℹ️ Пересечения нет для {self.symbol}/{self.timeframe} — "
                f"prev: 9={prev_ema9} vs 21={prev_ema21}, curr: 9={curr_ema9} vs 21={curr_ema21}"
            )
            return None

        except Exception as e:
            log.info(f"[EMACROSS_SIMPLE] ❌ Ошибка update(): {e}")
            return None