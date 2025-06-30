# kamacross15_ema50_simple.py

import logging
from datetime import datetime
from typing import Optional

from rule_engine.base import SignalRule, SignalResult

log = logging.getLogger("KAMACROSS15_EMA50_SIMPLE")


# 🔸 Сигнал: пересечение KAMA15 и EMA50
class KamaCross15Ema50Simple(SignalRule):
    def required_indicators(self) -> list[str]:
        return ["kama15", "ema50"]

    async def update(self, open_time: datetime) -> Optional[SignalResult]:
        try:
            kama15 = await self.fetch_indicator_series("kama15", 3, open_time)
            ema50 = await self.fetch_indicator_series("ema50", 3, open_time)

            if len(kama15) < 3 or len(ema50) < 3:
                log.info(f"[KAMACROSS15_EMA50_SIMPLE] ⚠️ Недостаточно данных: {self.symbol}/{self.timeframe}")
                return None

            _, prev_kama15, curr_kama15 = kama15
            _, prev_ema50, curr_ema50 = ema50

            if prev_kama15 <= prev_ema50 and curr_kama15 > curr_ema50:
                log.info(f"[KAMACROSS15_EMA50_SIMPLE] ✅ LONG сигнал для {self.symbol}/{self.timeframe}")
                return SignalResult(
                    signal_id=self.signal_id,
                    direction="long",
                    reason="KAMA15 пересек EMA50 снизу вверх",
                    details={
                        "prev_kama15": prev_kama15,
                        "prev_ema50": prev_ema50,
                        "curr_kama15": curr_kama15,
                        "curr_ema50": curr_ema50,
                    },
                )

            if prev_kama15 >= prev_ema50 and curr_kama15 < curr_ema50:
                log.info(f"[KAMACROSS15_EMA50_SIMPLE] ✅ SHORT сигнал для {self.symbol}/{self.timeframe}")
                return SignalResult(
                    signal_id=self.signal_id,
                    direction="short",
                    reason="KAMA15 пересек EMA50 сверху вниз",
                    details={
                        "prev_kama15": prev_kama15,
                        "prev_ema50": prev_ema50,
                        "curr_kama15": curr_kama15,
                        "curr_ema50": curr_ema50,
                    },
                )

            log.debug(f"[KAMACROSS15_EMA50_SIMPLE] ℹ️ Пересечения нет: {self.symbol}/{self.timeframe}")
            return None

        except Exception as e:
            log.info(f"[KAMACROSS15_EMA50_SIMPLE] ❌ Ошибка update(): {e}")
            return None