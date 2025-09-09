# strategy_401_rsibinshort.py — 🔸 Шортовая стратегия (RSI14 bins): берём posfeat-комбо мастера, сверяем с ORACLE, пороги по направлению

import logging
import json
import math
import asyncio
from typing import Any, Dict, Tuple, Union

log = logging.getLogger("strategy_401_rsibinshort")

IgnoreResult = Tuple[str, str]  # ("ignore", reason)


# 🔸 Класс стратегии
class Strategy401Rsibinshort:
    # Проверка и валидации сигнала (posfeat-подход)
    async def validate_signal(self, signal: Dict[str, Any], context: Dict[str, Any]) -> Union[bool, IgnoreResult]:
        direction = str(signal.get("direction", "")).lower()
        symbol = str(signal.get("symbol", ""))
        log_uid = str(signal.get("log_uid", ""))
        self_sid = int(signal.get("strategy_id"))

        # проверяем направление
        if direction != "short":
            return ("ignore", f"short-only strategy: received '{direction}'")

        # проверяем наличие Redis
        redis = context.get("redis")
        if redis is None:
            return ("ignore", "No Redis in context")

        strategy_cfg = context.get("strategy") or {}

        # определяем master_sid по зеркалам (направляющий/общий/собственный)
        master_sid = self._resolve_mirrored_sid(strategy_cfg, direction, self_sid)

        # читаем posfeat-комбо мастера с таймаутом (старт 5с, затем 2с опрос до 60с)
        posfeat_key = f"posfeat:{master_sid}:{log_uid}:rsi14_bucket_combo"
        triplet = await self._await_posfeat_combo(redis, posfeat_key, initial_sleep_sec=5, poll_interval_sec=2, max_wait_sec=60)
        if not triplet:
            return ("ignore", f"No posfeat RSI14 triplet for {symbol} (master {master_sid}, log_uid={log_uid})")

        # читаем oracle по триплету мастера (short)
        oracle_key = f"oracle:rsi:comp:{master_sid}:short:rsi14:{triplet}"
        oracle_raw = await self._get_str(redis, oracle_key)
        if not oracle_raw:
            return ("ignore", f"No information on RSI14 triplet {triplet} for {symbol}")

        # парсим oracle JSON
        try:
            oracle = json.loads(oracle_raw)
            closed_trades = int(oracle.get("closed_trades", 0))
            winrate = float(oracle.get("winrate", 0.0))
        except Exception:
            return ("ignore", f"Malformed oracle RSI14 entry for {symbol}")

        # читаем направленную базу мастера (closed_short)
        stats_key = f"strategy:stats:{master_sid}"
        closed_short = await self._get_int_hash(redis, stats_key, "closed_short")
        if closed_short is None or closed_short <= 0:
            return ("ignore", f"No directional history: closed_short=0 for master {master_sid}")

        # проверяем объём истории (строго больше 0.2%)
        min_closed_required = math.floor(0.002 * closed_short) + 1
        if closed_trades < min_closed_required:
            return (
                "ignore",
                f"Not enough closed history for RSI14 triplet {triplet}: required={min_closed_required}, got={closed_trades}"
            )

        # проверяем winrate
        if winrate < 0.55:
            return ("ignore", f"Winrate below 0.55 for RSI14 triplet {triplet}: got={winrate:.4f}")

        return True

    # Выполнение при допуске
    async def run(self, signal: Dict[str, Any], context: Dict[str, Any]) -> None:
        redis = context.get("redis")
        if redis is None:
            raise RuntimeError("No Redis in context")

        payload = {
            "strategy_id": str(signal["strategy_id"]),
            "symbol": signal["symbol"],
            "direction": signal["direction"],
            "log_uid": signal.get("log_uid"),
            "route": "new_entry",
            "received_at": signal.get("received_at"),
        }

        try:
            await redis.xadd("strategy_opener_stream", {"data": json.dumps(payload)})
            log.debug(f"📤 Open request queued: {payload}")
        except Exception as e:
            log.warning(f"Failed to enqueue open request: {e}")

    # Определение зеркала мастера (универсально)
    def _resolve_mirrored_sid(self, strategy_cfg: Dict[str, Any], direction: str, fallback_sid: int) -> int:
        mm_long = self._to_int_or_none(strategy_cfg.get("market_mirrow_long"))
        mm_short = self._to_int_or_none(strategy_cfg.get("market_mirrow_short"))
        mm = self._to_int_or_none(strategy_cfg.get("market_mirrow"))

        if mm_long is not None or mm_short is not None:
            if direction == "long" and mm_long is not None:
                return mm_long
            if direction == "short" and mm_short is not None:
                return mm_short
        if mm is not None:
            return mm
        return fallback_sid

    # Ожидание появления posfeat-комбо
    async def _await_posfeat_combo(self, redis, key: str, initial_sleep_sec: int, poll_interval_sec: int, max_wait_sec: int) -> str:
        v = await self._get_str(redis, key)
        if v:
            return v
        try:
            await asyncio.sleep(initial_sleep_sec)
        except Exception:
            pass
        elapsed = initial_sleep_sec
        while elapsed <= max_wait_sec:
            v = await self._get_str(redis, key)
            if v:
                return v
            try:
                await asyncio.sleep(poll_interval_sec)
            except Exception:
                pass
            elapsed += poll_interval_sec
        return ""

    # Утилита: получить строковое значение
    async def _get_str(self, redis, key: str) -> str:
        try:
            raw = await redis.get(key)
            if raw is None:
                return ""
            if isinstance(raw, bytes):
                return raw.decode("utf-8")
            return str(raw)
        except Exception:
            return ""

    # Утилита: получить int из Hash
    async def _get_int_hash(self, redis, key: str, field: str) -> int | None:
        try:
            raw = await redis.hget(key, field)
            if raw is None:
                return None
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            return int(float(raw))
        except Exception:
            return None

    # Утилита: преобразование в int
    @staticmethod
    def _to_int_or_none(v: Any) -> int | None:
        try:
            if v is None:
                return None
            iv = int(v)
            return iv if iv != 0 else None
        except Exception:
            return None