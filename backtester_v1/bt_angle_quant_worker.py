# bt_angle_quant_worker.py — временный воркер квантильного анализа угла LR50 m5 внутри MTF-бинов

import asyncio
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("BT_ANGLE_QUANT")

# 🔸 Настройки Redis stream
ANGLE_STREAM_KEY = "bt:analysis:angle"
ANGLE_CONSUMER_GROUP = "bt_angle_quant"
ANGLE_CONSUMER_NAME = "bt_angle_quant_main"

ANGLE_STREAM_BATCH_SIZE = 10
ANGLE_STREAM_BLOCK_MS = 5000

# 🔸 Настройки квантильного анализа
ANGLE_QUANTILES = 5              # по сколько квантилей бить
MIN_SHARE = Decimal("0.01")      # 1% от общего числа сделок в бине — порог отбора биннов


# 🔸 Публичная точка входа: оркестратор квантилей по углу m5 внутри MTF-бинов
async def run_bt_angle_quant_worker(pg, redis) -> None:
    log.debug("BT_ANGLE_QUANT: воркер квантильного анализа запущен")

    # подготавливаем consumer group
    await _ensure_consumer_group(redis)

    # очищаем временные таблицы перед первым проходом
    await _truncate_tmp_tables(pg)

    # основной цикл чтения стрима bt:analysis:angle
    while True:
        try:
            entries = await _read_from_stream(redis)

            if not entries:
                continue

            total_msgs = 0
            total_pairs = 0
            total_bins = 0

            for stream_key, messages in entries:
                if stream_key != ANGLE_STREAM_KEY:
                    continue

                for entry_id, fields in messages:
                    total_msgs += 1

                    ctx = _parse_angle_message(fields)
                    if not ctx:
                        await redis.xack(ANGLE_STREAM_KEY, ANGLE_CONSUMER_GROUP, entry_id)
                        continue

                    analysis_id = ctx["analysis_id"]
                    scenario_id = ctx["scenario_id"]
                    signal_id = ctx["signal_id"]

                    log.debug(
                        "BT_ANGLE_QUANT: получено событие angle-analysis "
                        "analysis_id=%s, scenario_id=%s, signal_id=%s, finished_at=%s, stream_id=%s",
                        analysis_id,
                        scenario_id,
                        signal_id,
                        ctx["finished_at"],
                        entry_id,
                    )

                    bins_processed = await _process_angle_for_pair(
                        pg=pg,
                        analysis_id=analysis_id,
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                    )
                    if bins_processed > 0:
                        total_pairs += 1
                        total_bins += bins_processed

                    await redis.xack(ANGLE_STREAM_KEY, ANGLE_CONSUMER_GROUP, entry_id)

            log.info(
                "BT_ANGLE_QUANT: пакет сообщений обработан — сообщений=%s, пар=%s, биннов_с_кванилями=%s",
                total_msgs,
                total_pairs,
                total_bins,
            )

        except Exception as e:
            log.error(
                "BT_ANGLE_QUANT: ошибка в основном цикле воркера: %s",
                e,
                exc_info=True,
            )
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима bt:analysis:angle
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=ANGLE_STREAM_KEY,
            groupname=ANGLE_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_ANGLE_QUANT: создана consumer group '%s' для стрима '%s'",
            ANGLE_CONSUMER_GROUP,
            ANGLE_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_ANGLE_QUANT: consumer group '%s' для стрима '%s' уже существует",
                ANGLE_CONSUMER_GROUP,
                ANGLE_STREAM_KEY,
            )
        else:
            log.error(
                "BT_ANGLE_QUANT: ошибка при создании consumer group '%s': %s",
                ANGLE_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:analysis:angle
async def _read_from_stream(redis) -> List[Any]:
    entries = await redis.xreadgroup(
        groupname=ANGLE_CONSUMER_GROUP,
        consumername=ANGLE_CONSUMER_NAME,
        streams={ANGLE_STREAM_KEY: ">"},
        count=ANGLE_STREAM_BATCH_SIZE,
        block=ANGLE_STREAM_BLOCK_MS,
    )

    if not entries:
        return []

    parsed: List[Any] = []
    for stream_key, messages in entries:
        if isinstance(stream_key, bytes):
            stream_key = stream_key.decode("utf-8")

        stream_entries: List[Any] = []
        for msg_id, fields in messages:
            if isinstance(msg_id, bytes):
                msg_id = msg_id.decode("utf-8")

            str_fields: Dict[str, str] = {}
            for k, v in fields.items():
                key_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                val_str = v.decode("utf-8") if isinstance(v, bytes) else str(v)
                str_fields[key_str] = val_str

            stream_entries.append((msg_id, str_fields))

        parsed.append((stream_key, stream_entries))

    return parsed


# 🔸 Парсинг сообщения из bt:analysis:angle
def _parse_angle_message(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        analysis_id_str = fields.get("analysis_id")
        scenario_id_str = fields.get("scenario_id")
        signal_id_str = fields.get("signal_id")
        finished_at_str = fields.get("finished_at")

        if not (analysis_id_str and scenario_id_str and signal_id_str and finished_at_str):
            return None

        return {
            "analysis_id": int(analysis_id_str),
            "scenario_id": int(scenario_id_str),
            "signal_id": int(signal_id_str),
            "finished_at": datetime.fromisoformat(finished_at_str),
        }
    except Exception as e:
        log.error(
            "BT_ANGLE_QUANT: ошибка разбора сообщения стрима bt:analysis:angle: %s, fields=%s",
            e,
            fields,
            exc_info=True,
        )
        return None


# 🔸 Очистка временных таблиц перед проходом
async def _truncate_tmp_tables(pg) -> None:
    async with pg.acquire() as conn:
        await conn.execute("TRUNCATE TABLE bt_tmp_angle_quant_detail")
        await conn.execute("TRUNCATE TABLE bt_tmp_angle_quant_header")
    log.debug("BT_ANGLE_QUANT: bt_tmp_angle_quant_* очищены перед новым проходом")


# 🔸 Обработка одной пары (analysis_id, scenario_id, signal_id)
async def _process_angle_for_pair(
    pg,
    analysis_id: int,
    scenario_id: int,
    signal_id: int,
) -> int:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT bin_name, trades
            FROM bt_analysis_bins_stat
            WHERE analysis_id = $1
              AND scenario_id = $2
              AND signal_id   = $3
            """,
            analysis_id,
            scenario_id,
            signal_id,
        )

    if not rows:
        log.debug(
            "BT_ANGLE_QUANT: нет строк в bt_analysis_bins_stat для analysis_id=%s, scenario_id=%s, signal_id=%s",
            analysis_id,
            scenario_id,
            signal_id,
        )
        return 0

    total_trades = sum(int(r["trades"]) for r in rows)
    if total_trades <= 0:
        log.debug(
            "BT_ANGLE_QUANT: total_trades=0 для analysis_id=%s, scenario_id=%s, signal_id=%s",
            analysis_id,
            scenario_id,
            signal_id,
        )
        return 0

    threshold = (Decimal(total_trades) * MIN_SHARE)
    # чисто > 1%, как ты и просил
    selected_bins = [r for r in rows if Decimal(int(r["trades"])) > threshold]

    if not selected_bins:
        log.info(
            "BT_ANGLE_QUANT: нет биннов с trades > 1%% (total_trades=%s) для analysis_id=%s, scenario_id=%s, signal_id=%s",
            total_trades,
            analysis_id,
            scenario_id,
            signal_id,
        )
        return 0

    log.info(
        "BT_ANGLE_QUANT: analysis_id=%s, scenario_id=%s, signal_id=%s — total_trades=%s, "
        "порог_1%%=%.2f, выбранных_биннов=%s",
        analysis_id,
        scenario_id,
        signal_id,
        total_trades,
        float(threshold),
        len(selected_bins),
    )

    run_at = datetime.utcnow()
    bins_processed = 0

    for r in selected_bins:
        bin_name = r["bin_name"]
        trades_bin = int(r["trades"])

        header_id = await _process_single_bin(
            pg=pg,
            analysis_id=analysis_id,
            scenario_id=scenario_id,
            signal_id=signal_id,
            bin_name=bin_name,
            trades_total=trades_bin,
            run_at=run_at,
        )
        if header_id is not None:
            bins_processed += 1

    return bins_processed


# 🔸 Квантильный анализ по одному bin_name
async def _process_single_bin(
    pg,
    analysis_id: int,
    scenario_id: int,
    signal_id: int,
    bin_name: str,
    trades_total: int,
    run_at: datetime,
) -> Optional[int]:
    async with pg.acquire() as conn:
        # базовая выборка позиций для бинна
        base_rows = await conn.fetch(
            """
            SELECT
                pr.position_uid,
                p.direction,
                p.pnl_abs,
                (p.raw_stat->'tf'->'m5'->'indicators'->'lr'->>'lr50_angle')::numeric AS angle_m5
            FROM bt_analysis_positions_raw pr
            JOIN bt_scenario_positions p
              ON p.position_uid = pr.position_uid
            WHERE pr.analysis_id = $1
              AND pr.scenario_id = $2
              AND pr.signal_id   = $3
              AND pr.bin_name    = $4
            """,
            analysis_id,
            scenario_id,
            signal_id,
            bin_name,
        )

    if not base_rows:
        log.debug(
            "BT_ANGLE_QUANT: bin_name=%s — нет позиций в raw для analysis_id=%s, scenario_id=%s, signal_id=%s",
            bin_name,
            analysis_id,
            scenario_id,
            signal_id,
        )
        return None

    # фильтруем только те, у кого есть угол m5
    filtered = []
    for r in base_rows:
        angle = r["angle_m5"]
        if angle is None:
            continue
        filtered.append(
            {
                "direction": r["direction"],
                "pnl_abs": r["pnl_abs"],
                "angle_m5": angle,
            }
        )

    if not filtered:
        log.debug(
            "BT_ANGLE_QUANT: bin_name=%s — нет позиций с валидным angle_m5",
            bin_name,
        )
        return None

    trades_effective = len(filtered)

    # определяем направление: если все одинаковые, используем его, иначе 'mixed'
    dirs = {str(f["direction"]).lower() for f in filtered if f["direction"] is not None}
    if len(dirs) == 1:
        (direction_val,) = dirs
    elif len(dirs) == 0:
        direction_val = "unknown"
    else:
        direction_val = "mixed"

    # складываем в временную таблицу заголовок
    async with pg.acquire() as conn:
        row_hdr = await conn.fetchrow(
            """
            INSERT INTO bt_tmp_angle_quant_header (
                run_at,
                analysis_id,
                scenario_id,
                signal_id,
                bin_name,
                trades_total,
                quantiles,
                direction
            )
            VALUES (
                $1, $2, $3, $4,
                $5, $6, $7, $8
            )
            RETURNING id
            """,
            run_at,
            analysis_id,
            scenario_id,
            signal_id,
            bin_name,
            trades_effective,
            ANGLE_QUANTILES,
            direction_val,
        )
        header_id = int(row_hdr["id"])

    # считаем квантили по angle_m5 через NTILE в SQL, чтобы не городить свои квантильщики
    async with pg.acquire() as conn:
        quant_rows = await conn.fetch(
            """
            WITH base AS (
                SELECT
                    pr.position_uid,
                    p.direction,
                    p.pnl_abs,
                    (p.raw_stat->'tf'->'m5'->'indicators'->'lr'->>'lr50_angle')::numeric AS angle_m5
                FROM bt_analysis_positions_raw pr
                JOIN bt_scenario_positions p
                  ON p.position_uid = pr.position_uid
                WHERE pr.analysis_id = $1
                  AND pr.scenario_id = $2
                  AND pr.signal_id   = $3
                  AND pr.bin_name    = $4
            ),
            filtered AS (
                SELECT *
                FROM base
                WHERE angle_m5 IS NOT NULL
            ),
            q AS (
                SELECT
                    *,
                    NTILE($5) OVER (
                        ORDER BY
                            CASE
                                WHEN LOWER(direction) = 'short'
                                    THEN -angle_m5
                                ELSE angle_m5
                            END
                    ) AS q_idx
                FROM filtered
            )
            SELECT
                q_idx AS quantile_index,
                MIN(angle_m5) AS angle_lo,
                MAX(angle_m5) AS angle_hi,
                COUNT(*)      AS trades,
                COALESCE(SUM(pnl_abs), 0) AS pnl_abs,
                CASE
                    WHEN COUNT(*) > 0
                        THEN COUNT(*) FILTER (WHERE pnl_abs > 0)::numeric / COUNT(*)::numeric
                    ELSE 0::numeric
                END AS winrate
            FROM q
            GROUP BY q_idx
            ORDER BY q_idx
            """,
            analysis_id,
            scenario_id,
            signal_id,
            bin_name,
            ANGLE_QUANTILES,
        )

    if not quant_rows:
        log.debug(
            "BT_ANGLE_QUANT: bin_name=%s — NTILE не вернул ни одной строки (analysis_id=%s, scenario_id=%s, signal_id=%s)",
            bin_name,
            analysis_id,
            scenario_id,
            signal_id,
        )
        return header_id

    # записываем квантильные строки
    detail_values: List[Tuple[Any, ...]] = []
    for r in quant_rows:
        q_idx = int(r["quantile_index"])
        angle_lo = r["angle_lo"]
        angle_hi = r["angle_hi"]
        trades_q = int(r["trades"])
        pnl_abs_q = r["pnl_abs"]
        winrate_q = r["winrate"]

        detail_values.append(
            (
                header_id,
                q_idx,
                angle_lo,
                angle_hi,
                trades_q,
                pnl_abs_q,
                winrate_q,
            )
        )

    async with pg.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO bt_tmp_angle_quant_detail (
                header_id,
                quantile_index,
                angle_lo,
                angle_hi,
                trades,
                pnl_abs,
                winrate
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7
            )
            """,
            detail_values,
        )

    log.info(
        "BT_ANGLE_QUANT: analysis_id=%s, scenario_id=%s, signal_id=%s, bin_name=%s — "
        "квантилей=%s, trades_effective=%s",
        analysis_id,
        scenario_id,
        signal_id,
        bin_name,
        len(quant_rows),
        trades_effective,
    )
    return header_id


# 🔸 Вспомогательная: безопасное приведение к Decimal (если понадобится)
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")