# laboratory_decision_postproc.py — постпроцессор закрытий: дописывает в LPS поля позиции после закрытия

# 🔸 Импорты
import asyncio
import json
import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Optional

# 🔸 Инфраструктура
import laboratory_infra as infra

# 🔸 Логгер
log = logging.getLogger("LAB_DECISION_POSTPROC")

# 🔸 Константы Streams/таблиц
SIGNAL_LOG_QUEUE = "signal_log_queue"                    # события по сигналам/позициям (внешний модуль пишет сюда при закрытии)
LPS_TABLE = "public.laboratoty_position_stat"
POS_TABLE = "public.positions_v4"

# 🔸 Параметры чтения Streams
XREAD_BLOCK_MS = 1_000
XREAD_COUNT = 50


# 🔸 Утилита: аккуратный парс чисел из asyncpg status
def _rows_affected(status: str) -> int:
    # формат asyncpg: "UPDATE 3" / "INSERT 0" / "DELETE 1"
    if not status:
        return 0
    parts = status.split()
    try:
        return int(parts[-1])
    except Exception:
        return 0


# 🔸 Обработка одного события закрытия позиции → апдейт LPS по всем TF
async def _process_closed_event(position_uid: str, csid_s: str, log_uid: str):
    # парс client_strategy_id
    try:
        client_strategy_id = int(csid_s)
    except Exception:
        client_strategy_id = None

    if not position_uid or not client_strategy_id or not log_uid:
        # пропускаем некорректные события
        log.info("[POSTPROC] ⚠️ пропуск некорректного события: position_uid=%s csid=%s log_uid=%s",
                 position_uid or "-", csid_s or "-", log_uid or "-")
        return

    # читаем позицию из БД (источник истины для pnl/closed_at/направления/символа)
    async with infra.pg_pool.acquire() as conn:
        pos = await conn.fetchrow(
            f"""
            SELECT position_uid, strategy_id, symbol, direction, pnl, closed_at, status, log_uid
              FROM {POS_TABLE}
             WHERE position_uid = $1
            """,
            position_uid,
        )

        if not pos:
            # позиция отсутствует — это не ошибка для воркера
            log.info("[POSTPROC] ⚠️ позиция не найдена position_uid=%s csid=%s log_uid=%s", position_uid, client_strategy_id, log_uid)
            return

        # извлекаем поля
        symbol: str = pos["symbol"]
        direction: Optional[str] = pos["direction"]
        pnl: Optional[Decimal] = pos["pnl"]
        closed_at: Optional[datetime] = pos["closed_at"]
        status: Optional[str] = pos["status"]

        # проверка статуса на всякий случай
        if status != "closed":
            log.info("[POSTPROC] ⚠️ позиция ещё не закрыта (status=%s) position_uid=%s csid=%s", status or "-", position_uid, client_strategy_id)

        # вычисляем результат: win = pnl > 0, иначе lose (False)
        result_bool: bool = bool(pnl is not None and pnl > 0)

        # апдейт ВСЕХ строк LPS по (log_uid, client_strategy_id, symbol [, optional direction])
        # direction в LPS совпадает с направлением входа; добавим его в where для точности, если он есть
        if direction in ("long", "short"):
            upd_status = await conn.execute(
                f"""
                UPDATE {LPS_TABLE}
                   SET position_uid = $1,
                       pnl = $2,
                       "result" = $3,
                       closed_at = $4,
                       updated_at = NOW()
                 WHERE log_uid = $5
                   AND client_strategy_id = $6
                   AND symbol = $7
                   AND direction = $8
                """,
                position_uid, pnl, result_bool, closed_at, log_uid, client_strategy_id, symbol, direction
            )
        else:
            # без direction
            upd_status = await conn.execute(
                f"""
                UPDATE {LPS_TABLE}
                   SET position_uid = $1,
                       pnl = $2,
                       "result" = $3,
                       closed_at = $4,
                       updated_at = NOW()
                 WHERE log_uid = $5
                   AND client_strategy_id = $6
                   AND symbol = $7
                """,
                position_uid, pnl, result_bool, closed_at, log_uid, client_strategy_id, symbol
            )

        updated_rows = _rows_affected(upd_status)

    # лог результата (не считаем отсутствие строк ошибкой)
    log.info(
        "[POSTPROC] ✅ closed propagated: position_uid=%s csid=%s log_uid=%s %s dir=%s pnl=%s result=%s rows=%d",
        position_uid, client_strategy_id, log_uid, symbol, (direction or "-"),
        (str(pnl) if pnl is not None else "NULL"),
        ("win" if result_bool else "loose"),
        updated_rows
    )


# 🔸 Главный слушатель: обновление LPS после закрытия позиции (по stream signal_log_queue)
async def run_laboratory_decision_postproc():
    """
    Слушает signal_log_queue, обрабатывает только события со status='closed'
    и обновляет laboratoty_position_stat (LPS) по всем TF для пары (log_uid, client_strategy_id).
    Воркэр НЕ мешает другим потребителям стрима: не триммит/не удаляет сообщения, читает только новые.
    """
    log.debug("🛰️ LAB_DECISION_POSTPROC слушатель запущен (BLOCK=%d COUNT=%d)", XREAD_BLOCK_MS, XREAD_COUNT)

    last_id = "$"  # только новые
    redis = infra.redis_client

    while True:
        try:
            resp = await redis.xread(
                streams={SIGNAL_LOG_QUEUE: last_id},
                count=XREAD_COUNT,
                block=XREAD_BLOCK_MS,
            )
            if not resp:
                continue

            for _, messages in resp:
                for msg_id, fields in messages:
                    last_id = msg_id

                    # извлекаем поля
                    status = (fields.get("status") or "").strip().lower()
                    if status != "closed":
                        # игнорируем всё, что не закрытие
                        continue

                    position_uid = (fields.get("position_uid") or "").strip()
                    log_uid = (fields.get("log_uid") or "").strip()
                    csid_s = (fields.get("strategy_id") or "").strip()  # это именно client_strategy_id в нашей модели

                    if not position_uid or not log_uid or not csid_s:
                        # возможен вложенный JSON под key=data
                        data_raw = fields.get("data")
                        if isinstance(data_raw, str):
                            try:
                                data = json.loads(data_raw)
                                status = (data.get("status") or status).strip().lower()
                                position_uid = (data.get("position_uid") or position_uid).strip()
                                log_uid = (data.get("log_uid") or log_uid).strip()
                                csid_s = (data.get("strategy_id") or csid_s).strip()
                            except Exception:
                                pass

                    if status != "closed" or not position_uid or not log_uid or not csid_s:
                        # пропуск неполного/неподходящего события
                        log.info("[POSTPROC] ⚠️ пропуск msg=%s: status=%s position_uid=%s log_uid=%s csid=%s",
                                 msg_id, status or "-", position_uid or "-", log_uid or "-", csid_s or "-")
                        continue

                    # обработка события закрытия
                    try:
                        await _process_closed_event(position_uid=position_uid, csid_s=csid_s, log_uid=log_uid)
                    except Exception:
                        log.exception("[POSTPROC] ❌ ошибка обработки закрытия position_uid=%s csid=%s", position_uid, csid_s)

        except asyncio.CancelledError:
            log.debug("⏹️ LAB_DECISION_POSTPROC остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ LAB_DECISION_POSTPROC ошибка в основном цикле")
            await asyncio.sleep(1.0)