# strategy_701_longmw3.py
import logging
import json

log = logging.getLogger("strategy_701_longmw3")

class Strategy701Longmw3:
    async def validate_signal(self, signal, context):
        redis = context.get("redis")
        strategy_cfg = context.get("strategy")
        if redis is None:
            return ("ignore", "нет Redis клиента")
        if not strategy_cfg:
            return ("ignore", "нет конфигурации стратегии")

        direction = signal["direction"].lower()
        if direction != "long":
            return ("ignore", "short сигналы отключены")

        symbol = signal["symbol"]

        # 1. Читаем текущие regime9_code для трёх TF
        try:
            m5 = await redis.get(f"ind:{symbol}:m5:regime9_code")
            m15 = await redis.get(f"ind:{symbol}:m15:regime9_code")
            h1 = await redis.get(f"ind:{symbol}:h1:regime9_code")
        except Exception as e:
            log.warning(f"⚠️ Ошибка чтения regime9_code для {symbol}: {e}")
            return ("ignore", "ошибка Redis при получении regime9_code")

        if not m5 or not m15 or not h1:
            return ("ignore", "не удалось получить все regime9_code")

        marker3_code = f"{m5}-{m15}-{h1}"

        # 2. Определяем, по какому strategy_id смотреть статистику (свой или зеркало)
        base_id = strategy_cfg.get("market_mirrow") or strategy_cfg["id"]

        key = f"oracle:mw:stat:{base_id}:long:{marker3_code}"

        try:
            raw = await redis.get(key)
        except Exception as e:
            log.warning(f"⚠️ Ошибка чтения Redis ключа {key}: {e}")
            return ("ignore", "ошибка Redis при получении статистики")

        if raw is None:
            return ("ignore", "нет статистики по marker3_code")

        try:
            data = json.loads(raw)
            closed_trades = int(data.get("closed_trades", 0))
            winrate = float(data.get("winrate", 0))
        except Exception as e:
            log.warning(f"⚠️ Ошибка парсинга значения ключа {key}: {e}")
            return ("ignore", "некорректные данные статистики")

        if closed_trades <= 2:
            return ("ignore", "недостаточно данных")

        if winrate <= 0.5:
            return ("ignore", f"winrate {winrate:.4f} ниже порога")

        # все условия выполнены → разрешаем
        return True

    async def run(self, signal, context):
        redis = context.get("redis")
        if redis is None:
            raise RuntimeError("❌ Redis клиент не передан в context")

        payload = {
            "strategy_id": str(signal["strategy_id"]),
            "symbol": signal["symbol"],
            "direction": signal["direction"],
            "log_uid": signal.get("log_uid"),
            "route": "new_entry",
            "received_at": signal.get("received_at")
        }

        try:
            await redis.xadd("strategy_opener_stream", {"data": json.dumps(payload)})
            log.debug(f"📤 Сигнал отправлен: {payload}")
        except Exception as e:
            log.warning(f"⚠️ Ошибка при отправке сигнала: {e}")