# emacross_strong.py

import logging
from datetime import datetime
from typing import Optional

from rule_engine.base import SignalRule, SignalResult
from infra import ENABLED_TICKERS, infra

log = logging.getLogger("EMACROSS_STRONG")


# 🔸 Правило: строгое EMA9/EMA21 (STRONG)
class EmaCrossStrong(SignalRule):
    def required_indicators(self) -> list[str]:
        return ["ema9", "ema21"]

    # 🔸 Основная логика сигнала
    async def update(self, open_time: datetime) -> Optional[SignalResult]:
        redis = infra.redis_client
        key_last = f"last_signal:{self.symbol}:ema_cross_strong"
        last_ts = await redis.get(key_last)
        if last_ts:
            last_dt = datetime.fromisoformat(last_ts.decode())
            if (open_time - last_dt).total_seconds() < 6 * 300:
                log.info(f"[EMACROSS_STRONG] ⏩ Пропущено: последний сигнал был < 6 баров назад")
                return None

        ema9 = await self.fetch_indicator_series("ema9", 6, open_time)
        ema21 = await self.fetch_indicator_series("ema21", 6, open_time)

        if len(ema9) < 6 or len(ema21) < 6:
            log.info(f"[EMACROSS_STRONG] ⚠️ Недостаточно данных для {self.symbol}/{self.timeframe}")
            return None

        p9, c9 = ema9[-2], ema9[-1]
        p21, c21 = ema21[-2], ema21[-1]

        # Условие 1: пересечение
        if not (p9 <= p21 and c9 > c21):
            log.info(f"[EMACROSS_STRONG] ℹ️ Пересечения нет для {self.symbol}/{self.timeframe}")
            return None

        # Условие 2: delta >= max(0.0005 * ema21[t], 3 * precision_price)
        precision = ENABLED_TICKERS[self.symbol]["precision_price"]
        min_delta = max(0.0005 * c21, 3 * 10**-precision)
        actual_delta = abs(c9 - c21)
        if actual_delta < min_delta:
            log.info(f"[EMACROSS_STRONG] ℹ️ Дельта слишком мала: {actual_delta:.8f} < {min_delta:.8f}")
            return None

        # Условие 3: наклон ema9 растёт быстрее ema21 (×1.5)
        if (c9 - p9) <= (c21 - p21) * 1.5:
            log.info(f"[EMACROSS_STRONG] ℹ️ Наклон ema9 недостаточен")
            return None

        # Условие 4: delta[i] убывает все 5 баров
        deltas = [abs(ema9[i] - ema21[i]) for i in range(0, 5)]
        if not all(deltas[i] > deltas[i+1] for i in range(4)):
            log.info(f"[EMACROSS_STRONG] ℹ️ Не все дельты убывают: {deltas}")
            return None

        await redis.set(key_last, open_time.isoformat())

        log.info(f"[EMACROSS_STRONG] ✅ LONG сигнал для {self.symbol}/{self.timeframe} — delta={actual_delta:.8f}")
        return SignalResult(
            signal_id=self.signal_id,
            direction="long",
            reason="EMA9 пересёк EMA21 с условиями STRONG",
            details={
                "prev_ema9": p9,
                "prev_ema21": p21,
                "curr_ema9": c9,
                "curr_ema21": c21,
                "delta": actual_delta,
                "min_delta": min_delta,
                "delta_series": deltas
            }
        )