# emacross950_adx28.py

import logging
from datetime import datetime
from typing import Optional

from rule_engine.base import SignalRule, SignalResult

log = logging.getLogger("EMACROSS950_ADX28")


class EmaCross950Adx28(SignalRule):
    def required_indicators(self) -> list[str]:
        return ["ema9", "ema50", "adx_dmi14_adx"]

    async def update(self, open_time: datetime) -> Optional[SignalResult]:
        try:
            ema9 = await self.fetch_indicator_series("ema9", 3, open_time)
            ema50 = await self.fetch_indicator_series("ema50", 3, open_time)
            adx = await self.fetch_indicator_series("adx_dmi14_adx", 1, open_time)

            if len(ema9) < 3 or len(ema50) < 3 or len(adx) < 1:
                log.info(f"[EMACROSS950_ADX28] ⚠️ Недостаточно данных для {self.symbol}/{self.timeframe}")
                return None

            _, prev_ema9, curr_ema9 = ema9
            _, prev_ema50, curr_ema50 = ema50
            current_adx = adx[0]

            if current_adx <= 28:
                log.debug(f"[EMACROSS950_ADX28] ⏩ ADX={current_adx} < 28 — сигнал не генерируется")
                return None

            if prev_ema9 <= prev_ema50 and curr_ema9 > curr_ema50:
                log.info(f"[EMACROSS950_ADX28] ✅ LONG сигнал для {self.symbol}/{self.timeframe}")
                return SignalResult(
                    signal_id=self.signal_id,
                    direction="long",
                    reason="EMA9 пересек EMA50 снизу вверх + ADX > 28",
                    details={
                        "adx": current_adx,
                        "prev_ema9": prev_ema9,
                        "prev_ema50": prev_ema50,
                        "curr_ema9": curr_ema9,
                        "curr_ema50": curr_ema50,
                    },
                )

            if prev_ema9 >= prev_ema50 and curr_ema9 < curr_ema50:
                log.info(f"[EMACROSS950_ADX28] ✅ SHORT сигнал для {self.symbol}/{self.timeframe}")
                return SignalResult(
                    signal_id=self.signal_id,
                    direction="short",
                    reason="EMA9 пересек EMA50 сверху вниз + ADX > 28",
                    details={
                        "adx": current_adx,
                        "prev_ema9": prev_ema9,
                        "prev_ema50": prev_ema50,
                        "curr_ema9": curr_ema9,
                        "curr_ema50": curr_ema50,
                    },
                )

            log.debug(f"[EMACROSS950_ADX28] ℹ️ Пересечения нет: {self.symbol}/{self.timeframe}")
            return None

        except Exception as e:
            log.exception(f"[EMACROSS950_ADX28] ❌ Ошибка update(): {e}")
            return None