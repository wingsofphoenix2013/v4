# emacross_light.py

import logging
from datetime import datetime
from typing import Optional

from rule_engine.base import SignalRule, SignalResult
from infra import ENABLED_TICKERS, infra

log = logging.getLogger("EMACROSS_LIGHT")


# 🔸 Правило: лёгкий фильтр пересечения EMA9/EMA21
class EmaCrossLight(SignalRule):
    def required_indicators(self) -> list[str]:
        return ["ema9", "ema21"]

    # 🔸 Основная логика формирования сигнала
    async def update(self, open_time: datetime) -> Optional[SignalResult]:
        # Защита: последний сигнал должен быть минимум 2 бара назад
        redis = infra.redis_client
        key_last = f"last_signal:{self.symbol}:ema_cross_light"
        last_ts = await redis.get(key_last)
        if last_ts:
            last_dt = datetime.fromisoformat(last_ts)
            if (open_time - last_dt).total_seconds() < 2 * 300:
                log.info(f"[EMACROSS_LIGHT] ⏩ Сигнал пропущен (меньше 2 баров): {self.symbol}")
                return None

        ema9 = await self.fetch_indicator_series("ema9", 2, open_time)
        ema21 = await self.fetch_indicator_series("ema21", 2, open_time)

        if len(ema9) < 2 or len(ema21) < 2:
            log.info(f"[EMACROSS_LIGHT] ⚠️ Недостаточно данных для {self.symbol}/{self.timeframe}")
            return None

        prev_ema9, curr_ema9 = ema9
        prev_ema21, curr_ema21 = ema21

        # Условие 1: пересечение
        if not (prev_ema9 <= prev_ema21 and curr_ema9 > curr_ema21):
            log.info(f"[EMACROSS_LIGHT] ℹ️ Пересечения нет для {self.symbol}/{self.timeframe}")
            return None

        # Условие 2: минимальное расхождение
        min_delta = 0.0002 * curr_ema21
        actual_delta = abs(curr_ema9 - curr_ema21)
        if actual_delta < min_delta:
            log.info(f"[EMACROSS_LIGHT] ℹ️ Сигнал отклонён по дельте: delta={actual_delta:.8f}, min={min_delta:.8f}")
            return None

        # Условие 3: наклон
        if (curr_ema9 - prev_ema9) < (curr_ema21 - prev_ema21):
            log.info(f"[EMACROSS_LIGHT] ℹ️ Наклон ema9 < ema21 — сигнал пропущен")
            return None

        # Всё ок — сохраняем факт генерации
        await redis.set(key_last, open_time.isoformat())

        log.info(f"[EMACROSS_LIGHT] ✅ LONG сигнал для {self.symbol}/{self.timeframe} — delta={actual_delta:.8f}")
        return SignalResult(
            signal_id=self.signal_id,
            direction="long",
            reason="EMA9 пересёк EMA21 с условиями LIGHT",
            details={
                "prev_ema9": prev_ema9,
                "prev_ema21": prev_ema21,
                "curr_ema9": curr_ema9,
                "curr_ema21": curr_ema21,
                "delta": actual_delta,
                "min_delta": min_delta
            }
        )