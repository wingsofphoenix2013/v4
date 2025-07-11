# emacross2150_adx24.py

import logging
from datetime import datetime
from typing import Optional

from rule_engine.base import SignalRule, SignalResult

log = logging.getLogger("EMACROSS2150_ADX24")


class EmaCross2150Adx24(SignalRule):
    def required_indicators(self) -> list[str]:
        # Добавляем ADX к стандартным EMA
        return ["ema21", "ema50", "adx_dmi14_adx"]

    async def update(self, open_time: datetime) -> Optional[SignalResult]:
        try:
            # Загружаем серии EMA
            ema21 = await self.fetch_indicator_series("ema21", 3, open_time)
            ema50 = await self.fetch_indicator_series("ema50", 3, open_time)
            adx = await self.fetch_indicator_series("adx_dmi14_adx", 1, open_time)

            if len(ema21) < 3 or len(ema50) < 3 or len(adx) < 1:
                log.info(f"[EMACROSS2150_ADX24] ⚠️ Недостаточно данных для {self.symbol}/{self.timeframe}")
                return None

            # Последние значения
            _, prev_ema21, curr_ema21 = ema21
            _, prev_ema50, curr_ema50 = ema50
            current_adx = adx[0]

            if current_adx <= 24:
                log.debug(f"[EMACROSS2150_ADX24] ⏩ ADX={current_adx} < 24 — сигнал не генерируется")
                return None

            if prev_ema21 <= prev_ema50 and curr_ema21 > curr_ema50:
                log.info(f"[EMACROSS2150_ADX24] ✅ LONG сигнал для {self.symbol}/{self.timeframe}")
                return SignalResult(
                    signal_id=self.signal_id,
                    direction="long",
                    reason="EMA21 пересек EMA50 снизу вверх + ADX > 24",
                    details={
                        "adx": current_adx,
                        "prev_ema21": prev_ema21,
                        "prev_ema50": prev_ema50,
                        "curr_ema21": curr_ema21,
                        "curr_ema50": curr_ema50,
                    },
                )

            if prev_ema21 >= prev_ema50 and curr_ema21 < curr_ema50:
                log.info(f"[EMACROSS2150_ADX24] ✅ SHORT сигнал для {self.symbol}/{self.timeframe}")
                return SignalResult(
                    signal_id=self.signal_id,
                    direction="short",
                    reason="EMA21 пересек EMA50 сверху вниз + ADX > 24",
                    details={
                        "adx": current_adx,
                        "prev_ema21": prev_ema21,
                        "prev_ema50": prev_ema50,
                        "curr_ema21": curr_ema21,
                        "curr_ema50": curr_ema50,
                    },
                )

            log.debug(f"[EMACROSS2150_ADX24] ℹ️ Пересечения нет: {self.symbol}/{self.timeframe}")
            return None

        except Exception as e:
            log.exception(f"[EMACROSS2150_ADX24] ❌ Ошибка update(): {e}")
            return None