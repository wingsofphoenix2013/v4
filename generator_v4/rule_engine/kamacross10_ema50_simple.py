# kamacross10_ema50_simple.py

import logging
from datetime import datetime
from typing import Optional

from rule_engine.base import SignalRule, SignalResult

log = logging.getLogger("KAMACROSS10_EMA50_SIMPLE")


# 🔸 Сигнал: пересечение KAMA10 и EMA50
class KamaCross10Ema50Simple(SignalRule):
    def required_indicators(self) -> list[str]:
        return ["kama10", "ema50"]

    async def update(self, open_time: datetime) -> Optional[SignalResult]:
        try:
            kama10 = await self.fetch_indicator_series("kama10", 3, open_time)
            ema50 = await self.fetch_indicator_series("ema50", 3, open_time)

            if len(kama10) < 3 or len(ema50) < 3:
                log.info(f"[KAMACROSS10_EMA50_SIMPLE] ⚠️ Недостаточно данных: {self.symbol}/{self.timeframe}")
                return None

            _, prev_kama10, curr_kama10 = kama10
            _, prev_ema50, curr_ema50 = ema50

            if prev_kama10 <= prev_ema50 and curr_kama10 > curr_ema50:
                log.info(f"[KAMACROSS10_EMA50_SIMPLE] ✅ LONG сигнал для {self.symbol}/{self.timeframe}")
                return SignalResult(
                    signal_id=self.signal_id,
                    direction="long",
                    reason="KAMA10 пересек EMA50 снизу вверх",
                    details={
                        "prev_kama10": prev_kama10,
                        "prev_ema50": prev_ema50,
                        "curr_kama10": curr_kama10,
                        "curr_ema50": curr_ema50,
                    },
                )

            if prev_kama10 >= prev_ema50 and curr_kama10 < curr_ema50:
                log.info(f"[KAMACROSS10_EMA50_SIMPLE] ✅ SHORT сигнал для {self.symbol}/{self.timeframe}")
                return SignalResult(
                    signal_id=self.signal_id,
                    direction="short",
                    reason="KAMA10 пересек EMA50 сверху вниз",
                    details={
                        "prev_kama10": prev_kama10,
                        "prev_ema50": prev_ema50,
                        "curr_kama10": curr_kama10,
                        "curr_ema50": curr_ema50,
                    },
                )

            log.debug(f"[KAMACROSS10_EMA50_SIMPLE] ℹ️ Пересечения нет: {self.symbol}/{self.timeframe}")
            return None

        except Exception as e:
            log.info(f"[KAMACROSS10_EMA50_SIMPLE] ❌ Ошибка update(): {e}")
            return None