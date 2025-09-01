import logging
import json

log = logging.getLogger("strategy_641_shortema3")

class Strategy641Shortema3:
    async def validate_signal(self, signal, context):
        redis = context.get("redis")
        strategy_cfg = context.get("strategy")
        if redis is None:
            return ("ignore", "нет Redis клиента")
        if not strategy_cfg:
            return ("ignore", "нет конфигурации стратегии")

        direction = signal["direction"].lower()
        if direction != "short":
            return ("ignore", "long сигналы отключены")

        symbol = signal["symbol"]

        # 1) Текущий EMA-триплет (live) для EMA200 в формате "m5-m15-h1" (например, "1-0-3")
        live_key = f"ind_live:{symbol}:ema200_status_triplet"
        try:
            triplet = await redis.get(live_key)
        except Exception as e:
            log.warning(f"⚠️ Ошибка чтения ключа {live_key}: {e}")
            return ("ignore", "ошибка Redis при получении EMA триплета")

        if not triplet:
            return ("ignore", "нет live EMA триплета")

        # 2) Историческая статистика по этому же триплету из oracle (EMA len = 200) для SHORT
        base_id = strategy_cfg.get("market_mirrow") or strategy_cfg["id"]
        oracle_key = f"oracle:emastat:comp:{base_id}:{direction}:200:{triplet}"

        try:
            raw = await redis.get(oracle_key)
        except Exception as e:
            log.warning(f"⚠️ Ошибка чтения Redis ключа {oracle_key}: {e}")
            return ("ignore", "ошибка Redis при получении статистики")

        if raw is None:
            return ("ignore", "нет статистики по EMA триплету")

        try:
            data = json.loads(raw)
            closed_trades = int(data.get("closed_trades", 0))
            winrate = float(data.get("winrate", 0))
        except Exception as e:
            log.warning(f"⚠️ Ошибка парсинга значения ключа {oracle_key}: {e}")
            return ("ignore", "некорректные данные статистики")

        # 3) Правила допуска
        if closed_trades <= 2:
            return ("ignore", "недостаточно данных")
        if winrate <= 0.5:
            return ("ignore", f"winrate {winrate:.4f} ниже порога")

        # Все условия выполнены → разрешаем
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