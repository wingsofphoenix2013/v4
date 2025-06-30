# kamacross21_ema50_simple.py

import logging
from datetime import datetime
from typing import Optional

from rule_engine.base import SignalRule, SignalResult

log = logging.getLogger("KAMACROSS21_EMA50_SIMPLE")


# 🔸 Сигнал: пересечение KAMA21 и EMA50
class KamaCross21Ema50Simple(SignalRule):
    def required_indicators(self) -> list[str]:
        return ["kama21", "ema50"]

    async def update(self, open_time: datetime) -> Optional[SignalResult]:
        try:
            kama21 = await self.fetch_indicator_series("kama21", 3, open_time)
            ema50 = await self.fetch_indicator_series("ema50", 3, open_time)

            if len(kama21) < 3 or len(ema50) < 3:
                log.info(f"[KAMACROSS21_EMA50_SIMPLE] ⚠️ Недостаточно данных: {self.symbol}/{self.timeframe}")
                return None

            _, prev_kama21, curr_kama21 = kama21
            _, prev_ema50, curr_ema50 = ema50

            if prev_kama21 <= prev_ema50 and curr_kama21 > curr_ema50:
                log.info(f"[KAMACROSS21_EMA50_SIMPLE] ✅ LONG сигнал для {self.symbol}/{self.timeframe}")
                return SignalResult(
                    signal_id=self.signal_id,
                    direction="long",
                    reason="KAMA21 пересек EMA50 снизу вверх",
                    details={
                        "prev_kama21": prev_kama21,
                        "prev_ema50": prev_ema50,
                        "curr_kama21": curr_kama21,
                        "curr_ema50": curr_ema50,
                    },
                )

            if prev_kama21 >= prev_ema50 and curr_kama21 < curr_ema50:
                log.info(f"[KAMACROSS21_EMA50_SIMPLE] ✅ SHORT сигнал для {self.symbol}/{self.timeframe}")
                return SignalResult(
                    signal_id=self.signal_id,
                    direction="short",
                    reason="KAMA21 пересек EMA50 сверху вниз",
                    details={
                        "prev_kama21": prev_kama21,
                        "prev_ema50": prev_ema50,
                        "curr_kama21": curr_kama21,
                        "curr_ema50": curr_ema50,
                    },
                )

            log.debug(f"[KAMACROSS21_EMA50_SIMPLE] ℹ️ Пересечения нет: {self.symbol}/{self.timeframe}")
            return None

        except Exception as e:
            log.info(f"[KAMACROSS21_EMA50_SIMPLE] ❌ Ошибка update(): {e}")
            return None