# strategy_453_dmigaprsiuni.py — 🔸 Универсальная стратегия (DMI-gap-bin + RSI14-bin): два posfeat-триплета подряд, ORACLE пороги 0.2% / 0.55

import logging
import json
import math
import asyncio
from typing import Any, Dict, Tuple, Union

log = logging.getLogger("strategy_453_dmigaprsiuni")

IgnoreResult = Tuple[str, str]  # ("ignore", reason)


# 🔸 Класс стратегии
class Strategy453Dmigaprsiuni:
    # 🔸 Проверка и валидация сигнала (DMI-GAP → RSI; оба должны пройти пороги)
    async def validate_signal(self, signal: Dict[str, Any], context: Dict[str, Any]) -> Union[bool, IgnoreResult]:
        # базовые поля сигнала
        direction = str(signal.get("direction", "")).lower()
        symbol = str(signal.get("symbol", ""))
        log_uid = str(signal.get("log_uid", ""))
        self_sid = int(signal.get("strategy_id"))

        # валидация направления
        if direction not in ("long", "short"):
            return ("ignore", f"unsupported direction '{direction}'")

        # проверка наличия Redis
        redis = context.get("redis")
        if redis is None:
            return ("ignore", "No Redis in context")

        # выбор master_sid по зеркалам
        strategy_cfg = context.get("strategy") or {}
        master_sid = self._resolve_mirrored_sid(strategy_cfg, direction, self_sid)

        # направленная база мастера
        stats_key = f"strategy:stats:{master_sid}"
        closed_field = "closed_long" if direction == "long" else "closed_short"
        closed_dir_total = await self._get_int_hash(redis, stats_key, closed_field)
        if closed_dir_total is None or closed_dir_total <= 0:
            return ("ignore", f"No directional history: {closed_field}=0 for master {master_sid}")

        # порог истории (строго > 0.2%)
        min_closed_required = math.floor(0.002 * closed_dir_total) + 1

        # DMI-GAP триплет из posfeat (ожидание 5с + опрос 2с до 60с)
        trip_gap = await self._await_posfeat_combo(
            redis,
            key=f"posfeat:{master_sid}:{log_uid}:dmigapbin_combo",
            initial_sleep_sec=5,
            poll_interval_sec=2,
            max_wait_sec=60,
        )
        if not trip_gap:
            return ("ignore", f"No posfeat DMI-gap triplet for {symbol} (master {master_sid}, log_uid={log_uid})")

        # проверка DMI-GAP по ORACLE
        ok_gap = await self._check_oracle(
            redis=redis,
            oracle_key=f"oracle:dmi_gap:comp:{master_sid}:{direction}:gap:{trip_gap}",
            label=f"DMI-gap triplet {trip_gap}",
            min_closed_required=min_closed_required,
        )
        if ok_gap is not True:
            return ok_gap  # ("ignore", reason)

        # RSI триплет из posfeat (ожидание 5с + опрос 2с до 60с)
        trip_rsi = await self._await_posfeat_combo(
            redis,
            key=f"posfeat:{master_sid}:{log_uid}:rsi14_bucket_combo",
            initial_sleep_sec=5,
            poll_interval_sec=2,
            max_wait_sec=60,
        )
        if not trip_rsi:
            return ("ignore", f"No posfeat RSI14 triplet for {symbol} (master {master_sid}, log_uid={log_uid})")

        # проверка RSI по ORACLE
        ok_rsi = await self._check_oracle(
            redis=redis,
            oracle_key=f"oracle:rsi:comp:{master_sid}:{direction}:rsi14:{trip_rsi}",
            label=f"RSI14 triplet {trip_rsi}",
            min_closed_required=min_closed_required,
        )
        if ok_rsi is not True:
            return ok_rsi  # ("ignore", reason)

        # если оба триплета прошли — допускаем
        return True

    # 🔸 Выполнение при допуске (транзит в opener)
    async def run(self, signal: Dict[str, Any], context: Dict[str, Any]) -> None:
        # формирование payload и отправка в opener
        redis = context.get("redis")
        if redis is None:
            raise RuntimeError("No Redis in context")

        payload = {
            "strategy_id": str(signal.get("strategy_id")),
            "symbol": signal.get("symbol"),
            "direction": signal.get("direction"),
            "log_uid": signal.get("log_uid"),
            "route": "new_entry",
            "received_at": signal.get("received_at"),
        }
        try:
            await redis.xadd("strategy_opener_stream", {"data": json.dumps(payload)})
            log.debug(f"📤 Open request queued: {payload}")
        except Exception as e:
            log.warning(f"Failed to enqueue open request: {e}")

    # проверка одного триплета по ORACLE и порогам
    async def _check_oracle(self, redis, oracle_key: str, label: str, min_closed_required: int) -> Union[bool, IgnoreResult]:
        # чтение ORACLE-ключа
        raw = await self._get_str(redis, oracle_key)
        if not raw:
            return ("ignore", f"No information on {label}")

        # парсинг JSON
        try:
            obj = json.loads(raw)
            closed_trades = int(obj.get("closed_trades", 0))
            winrate = float(obj.get("winrate", 0.0))
        except Exception:
            return ("ignore", f"Malformed oracle entry for {label}")

        # проверка порога истории
        if closed_trades < min_closed_required:
            return ("ignore", f"Not enough closed history for {label}: required={min_closed_required}, got={closed_trades}")

        # проверка порога winrate
        if winrate < 0.55:
            return ("ignore", f"Winrate below 0.55 for {label}: got={winrate:.4f}")

        return True

    # ожидание готовности posfeat-комбо
    async def _await_posfeat_combo(self, redis, key: str, initial_sleep_sec: int, poll_interval_sec: int, max_wait_sec: int) -> str:
        # мгновенная попытка
        v = await self._get_str(redis, key)
        if v:
            return v

        # стартовая пауза
        try:
            await asyncio.sleep(initial_sleep_sec)
        except Exception:
            pass

        # опрос до таймаута
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

    # выбор master_sid по зеркалам
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

    # утилита: безопасное чтение строки из KV
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

    # утилита: чтение int из Hash
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

    # утилита: приведение к int или None
    @staticmethod
    def _to_int_or_none(v: Any) -> int | None:
        try:
            if v is None:
                return None
            iv = int(v)
            return iv if iv != 0 else None
        except Exception:
            return None