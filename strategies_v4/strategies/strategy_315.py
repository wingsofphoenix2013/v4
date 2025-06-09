# strategies/strategy_315.py
import logging
import json
from datetime import datetime
from infra import load_indicators
from config_loader import config

log = logging.getLogger("STRATEGY_315")

class Strategy315:
    # 🔸 Метод валидации сигнала перед входом
    async def validate_signal(self, signal, context) -> bool | str:
        symbol = signal.get("symbol")
        direction = signal.get("direction")
        strategy_id = int(signal.get("strategy_id"))
        log_uid = signal.get("log_uid")

        log.debug(f"⚙️ [Strategy315] Валидация сигнала: symbol={symbol}, direction={direction}")

        redis = context.get("redis")
        note = None

        try:
            timeframe = "m5"
            indicators = await load_indicators(symbol, [
                "rsi14",
                "mfi14"
            ], timeframe)

            price_raw = await redis.get(f"price:{symbol}")
            if price_raw is None:
                note = "отклонено: отсутствует цена"
            else:
                rsi = indicators.get("rsi14")
                mfi = indicators.get("mfi14")

                if None in [rsi, mfi]:
                    note = "отклонено: недостаточно данных индикаторов"
                else:
                    rsi = float(rsi)
                    mfi = float(mfi)

                    if direction == "long":
                        if not (rsi < 30):
                            note = f"отклонено: RSI14 >= 30 (rsi={rsi})"
                        elif not (mfi < 35):
                            note = f"отклонено: MFI14 >= 35 (mfi={mfi})"

                    elif direction == "short":
                        if not (rsi > 70):
                            note = f"отклонено: RSI14 <= 70 (rsi={rsi})"
                        elif not (mfi > 65):
                            note = f"отклонено: MFI14 <= 65 (mfi={mfi})"

        except Exception as e:
            note = f"ошибка при валидации фильтров: {e}"

        if note:
            log.debug(f"🚫 [Strategy315] {note}")
            if redis:
                log_record = {
                    "log_uid": log_uid,
                    "strategy_id": strategy_id,
                    "status": "ignore",
                    "position_id": None,
                    "note": note,
                    "logged_at": datetime.utcnow().isoformat()
                }

                # 🔸 Удаляем все поля со значением None (например, position_id)
                clean_record = {k: v for k, v in log_record.items() if v is not None}

                try:
                    await redis.xadd("signal_log_queue", clean_record)
                except Exception as e:
                    log.warning(f"⚠️ [Strategy315] Ошибка записи в Redis log_queue: {e}")
            return "logged"

        return True
# 🔸 Основной метод запуска стратегии
    async def run(self, signal, context):
        log.debug(f"🚀 [Strategy315] Запуск стратегии на сигнале: symbol={signal['symbol']}, direction={signal['direction']}")

        redis = context.get("redis")
        if redis:
            payload = {
                "strategy_id": signal["strategy_id"],
                "symbol": signal["symbol"],
                "direction": signal["direction"],
                "log_uid": signal["log_uid"],
                "route": "new_entry"
            }
            try:
                await redis.xadd("strategy_opener_stream", {"data": json.dumps(payload)})
                log.debug(f"📤 [Strategy315] Сигнал отправлен в strategy_opener_stream")
            except Exception as e:
                log.warning(f"⚠️ [Strategy315] Ошибка при отправке в stream: {e}")