# bt_complex_main.py — оркестратор комплексных анализаторов (комплексов) backtester_v1

import asyncio
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Callable, Awaitable, Tuple

# 🔸 Тип обработчика комплекса:
#    (complex_cfg, complex_ctx, pg_pool, redis_client) -> {"rows": [...], "summary": {...}}
ComplexHandler = Callable[
    [Dict[str, Any], Dict[str, Any], Any, Any],
    Awaitable[Dict[str, Any]]
]

# 🔸 Воркеры комплексов (из пакета complex/)
from complex.bt_complex_ema_position import run_complex_ema_position

# 🔸 Реестр комплексов: (family_key, key) → handler
COMPLEX_HANDLERS: Dict[Tuple[str, str], ComplexHandler] = {
    ("ema", "ema_position"): run_complex_ema_position,
}

# 🔸 Константы стримов
COMPLEX_INPUT_STREAM_KEY = "bt:analysis:postproc_ready"
COMPLEX_CONSUMER_GROUP = "bt_complex"
COMPLEX_CONSUMER_NAME = "bt_complex_main"

COMPLEX_READY_STREAM_KEY = "bt:complex:ready"

# 🔸 Настройки чтения стрима
COMPLEX_STREAM_BATCH_SIZE = 10
COMPLEX_STREAM_BLOCK_MS = 5000

# 🔸 Ограничение параллелизма комплексов (по парам scenario_id, signal_id)
COMPLEX_MAX_CONCURRENCY = 8

# 🔸 Кеш последних finished_at по (scenario_id, signal_id) для отсечки дублей
_last_postproc_finished_at: Dict[Tuple[int, int], datetime] = {}

log = logging.getLogger("BT_COMPLEX_MAIN")


# 🔸 Публичная точка входа: оркестратор комплексов
async def run_bt_complex_analysis_orchestrator(pg, redis):
    log.debug("BT_COMPLEX_MAIN: оркестратор комплексных анализаторов запущен")

    await _ensure_consumer_group(redis)

    # общий семафор для всех комплексов (ограничение по парам)
    complex_sema = asyncio.Semaphore(COMPLEX_MAX_CONCURRENCY)

    while True:
        try:
            messages = await _read_from_stream(redis)

            if not messages:
                continue

            total_msgs = 0
            total_pairs = 0
            total_complex_planned = 0
            total_complex_ok = 0
            total_complex_failed = 0
            total_rows_inserted = 0
            total_bins_rows = 0

            tasks: List[asyncio.Task] = []

            for stream_key, entries in messages:
                if stream_key != COMPLEX_INPUT_STREAM_KEY:
                    # на всякий случай игнорируем чужие стримы
                    continue

                for entry_id, fields in entries:
                    total_msgs += 1

                    task = asyncio.create_task(
                        _process_message(
                            entry_id=entry_id,
                            fields=fields,
                            pg=pg,
                            redis=redis,
                            sema=complex_sema,
                        ),
                        name=f"BT_COMPLEX_PAIR_{entry_id}",
                    )
                    tasks.append(task)

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for res in results:
                    if isinstance(res, dict):
                        total_pairs += 1
                        total_complex_planned += res.get("complex_total", 0)
                        total_complex_ok += res.get("complex_ok", 0)
                        total_complex_failed += res.get("complex_failed", 0)
                        total_rows_inserted += res.get("rows_inserted", 0)
                        total_bins_rows += res.get("bins_rows", 0)

                log.debug(
                    "BT_COMPLEX_MAIN: пакет сообщений обработан — сообщений=%s, пар=%s, "
                    "комплексов_планировалось=%s, успехов=%s, ошибок=%s, строк_raw=%s, строк_bins=%s",
                    total_msgs,
                    total_pairs,
                    total_complex_planned,
                    total_complex_ok,
                    total_complex_failed,
                    total_rows_inserted,
                    total_bins_rows,
                )
                log.info(
                    "BT_COMPLEX_MAIN: итог по пакету — сообщений=%s, пар=%s, "
                    "комплексов всего=%s, успешно=%s, с ошибками=%s, строк в raw=%s, строк в bins_stat=%s",
                    total_msgs,
                    total_pairs,
                    total_complex_planned,
                    total_complex_ok,
                    total_complex_failed,
                    total_rows_inserted,
                    total_bins_rows,
                )

        except Exception as e:
            log.error(
                "BT_COMPLEX_MAIN: ошибка в основном цикле оркестратора: %s",
                e,
                exc_info=True,
            )
            # небольшая пауза перед повторной попыткой
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима bt:analysis:postproc_ready
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=COMPLEX_INPUT_STREAM_KEY,
            groupname=COMPLEX_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_COMPLEX_MAIN: создана consumer group '%s' для стрима '%s'",
            COMPLEX_CONSUMER_GROUP,
            COMPLEX_INPUT_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_COMPLEX_MAIN: consumer group '%s' для стрима '%s' уже существует",
                COMPLEX_CONSUMER_GROUP,
                COMPLEX_INPUT_STREAM_KEY,
            )
        else:
            log.error(
                "BT_COMPLEX_MAIN: ошибка при создании consumer group '%s': %s",
                COMPLEX_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:analysis:postproc_ready
async def _read_from_stream(redis) -> List[Any]:
    entries = await redis.xreadgroup(
        groupname=COMPLEX_CONSUMER_GROUP,
        consumername=COMPLEX_CONSUMER_NAME,
        streams={COMPLEX_INPUT_STREAM_KEY: ">"},
        count=COMPLEX_STREAM_BATCH_SIZE,
        block=COMPLEX_STREAM_BLOCK_MS,
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


# 🔸 Разбор одного сообщения из стрима bt:analysis:postproc_ready
def _parse_postproc_message(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id_str = fields.get("scenario_id")
        signal_id_str = fields.get("signal_id")
        finished_at_str = fields.get("finished_at")

        if not (scenario_id_str and signal_id_str and finished_at_str):
            return None

        scenario_id = int(scenario_id_str)
        signal_id = int(signal_id_str)
        finished_at = datetime.fromisoformat(finished_at_str)

        positions_total_str = fields.get("positions_total") or "0"
        positions_good_str = fields.get("positions_good") or "0"
        positions_bad_str = fields.get("positions_bad") or "0"

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "finished_at": finished_at,
            "positions_total": int(positions_total_str),
            "positions_good": int(positions_good_str),
            "positions_bad": int(positions_bad_str),
        }
    except Exception as e:
        log.error(
            "BT_COMPLEX_MAIN: ошибка разбора сообщения стрима bt:analysis:postproc_ready: %s, fields=%s",
            e,
            fields,
            exc_info=True,
        )
        return None


# 🔸 Обработка одного сообщения из bt:analysis:postproc_ready с ограничением семафором
async def _process_message(
    entry_id: str,
    fields: Dict[str, str],
    pg,
    redis,
    sema: asyncio.Semaphore,
) -> Dict[str, Any]:
    async with sema:
        ctx = _parse_postproc_message(fields)
        if not ctx:
            # невалидное сообщение — ACK и выходим
            await redis.xack(COMPLEX_INPUT_STREAM_KEY, COMPLEX_CONSUMER_GROUP, entry_id)
            return {
                "complex_total": 0,
                "complex_ok": 0,
                "complex_failed": 0,
                "rows_inserted": 0,
                "bins_rows": 0,
            }

        scenario_id = ctx["scenario_id"]
        signal_id = ctx["signal_id"]
        finished_at = ctx["finished_at"]

        pair_key = (scenario_id, signal_id)
        last_finished = _last_postproc_finished_at.get(pair_key)

        # отсечка дублей по равному finished_at
        if last_finished is not None and last_finished == finished_at:
            log.debug(
                "BT_COMPLEX_MAIN: дубликат сообщения для scenario_id=%s, signal_id=%s, "
                "finished_at=%s, stream_id=%s — комплексы не запускаются",
                scenario_id,
                signal_id,
                finished_at,
                entry_id,
            )
            await redis.xack(COMPLEX_INPUT_STREAM_KEY, COMPLEX_CONSUMER_GROUP, entry_id)
            return {
                "complex_total": 0,
                "complex_ok": 0,
                "complex_failed": 0,
                "rows_inserted": 0,
                "bins_rows": 0,
            }

        _last_postproc_finished_at[pair_key] = finished_at

        log.debug(
            "BT_COMPLEX_MAIN: получено сообщение postproc анализа "
            "scenario_id=%s, signal_id=%s, finished_at=%s, stream_id=%s",
            scenario_id,
            signal_id,
            finished_at,
            entry_id,
        )

        try:
            result = await _process_pair_complex(
                pg=pg,
                redis=redis,
                scenario_id=scenario_id,
                signal_id=signal_id,
            )

            # публикуем событие готовности комплексного анализа
            await _publish_complex_ready(
                redis=redis,
                scenario_id=scenario_id,
                signal_id=signal_id,
                complex_total=result["complex_total"],
                complex_ok=result["complex_ok"],
                complex_failed=result["complex_failed"],
                rows_inserted=result["rows_inserted"],
                bins_rows=result["bins_rows"],
            )

            log.info(
                "BT_COMPLEX_MAIN: комплексный анализ завершён для scenario_id=%s, signal_id=%s — "
                "комплексов всего=%s, успешно=%s, с ошибками=%s, строк_raw=%s, строк_bins=%s",
                scenario_id,
                signal_id,
                result["complex_total"],
                result["complex_ok"],
                result["complex_failed"],
                result["rows_inserted"],
                result["bins_rows"],
            )

            await redis.xack(COMPLEX_INPUT_STREAM_KEY, COMPLEX_CONSUMER_GROUP, entry_id)
            return result

        except Exception as e:
            log.error(
                "BT_COMPLEX_MAIN: ошибка комплексного анализа для scenario_id=%s, signal_id=%s: %s",
                scenario_id,
                signal_id,
                e,
                exc_info=True,
            )
            await redis.xack(COMPLEX_INPUT_STREAM_KEY, COMPLEX_CONSUMER_GROUP, entry_id)
            return {
                "complex_total": 0,
                "complex_ok": 0,
                "complex_failed": 1,
                "rows_inserted": 0,
                "bins_rows": 0,
            }


# 🔸 Основной комплексный анализ для одной пары (scenario_id, signal_id)
async def _process_pair_complex(
    pg,
    redis,
    scenario_id: int,
    signal_id: int,
) -> Dict[str, Any]:
    # получаем все активные связки сценарий ↔ сигнал ↔ комплекс
    complexes = await _load_complex_instances_for_pair(pg, scenario_id, signal_id)
    if not complexes:
        log.debug(
            "BT_COMPLEX_MAIN: для scenario_id=%s, signal_id=%s нет активных комплексов, "
            "комплексный анализ выполняться не будет",
            scenario_id,
            signal_id,
        )
        return {
            "complex_total": 0,
            "complex_ok": 0,
            "complex_failed": 0,
            "rows_inserted": 0,
            "bins_rows": 0,
        }

    complex_total = len(complexes)
    complex_ok = 0
    complex_failed = 0
    rows_inserted_total = 0
    bins_rows_total = 0

    # общий семафор уже учтён на уровне пар, здесь просто запускаем комплексы поочерёдно / в мини-пуле
    tasks: List[asyncio.Task] = []
    for complex_cfg in complexes:
        task = asyncio.create_task(
            _run_complex(
                complex_cfg=complex_cfg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                pg=pg,
                redis=redis,
            ),
            name=f"BT_COMPLEX_{complex_cfg.get('id')}_SC_{scenario_id}_SIG_{signal_id}",
        )
        tasks.append(task)

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for res in results:
        if isinstance(res, Exception):
            complex_failed += 1
            continue

        status = res.get("status")
        inserted = res.get("rows_inserted", 0)
        bins_rows = res.get("bins_rows", 0)

        if status in ("ok", "skipped"):
            complex_ok += 1
        else:
            complex_failed += 1

        rows_inserted_total += inserted
        bins_rows_total += bins_rows

    log.debug(
        "BT_COMPLEX_MAIN: пара scenario_id=%s, signal_id=%s — комплексов всего=%s, "
        "успешно=%s, с ошибками=%s, строк_raw=%s, строк_bins=%s",
        scenario_id,
        signal_id,
        complex_total,
        complex_ok,
        complex_failed,
        rows_inserted_total,
        bins_rows_total,
    )

    return {
        "complex_total": complex_total,
        "complex_ok": complex_ok,
        "complex_failed": complex_failed,
        "rows_inserted": rows_inserted_total,
        "bins_rows": bins_rows_total,
    }


# 🔸 Загрузка активных комплексов для пары (scenario_id, signal_id)
async def _load_complex_instances_for_pair(
    pg,
    scenario_id: int,
    signal_id: int,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                c.complex_id,
                i.family_key,
                i."key",
                i."name",
                i.enabled
            FROM bt_complex_connections c
            JOIN bt_complex_instances i
              ON i.id = c.complex_id
            WHERE c.scenario_id = $1
              AND c.signal_id   = $2
              AND c.enabled     = true
              AND i.enabled     = true
            """,
            scenario_id,
            signal_id,
        )

        if not rows:
            return []

        complex_ids: List[int] = []
        complexes: Dict[int, Dict[str, Any]] = {}

        for r in rows:
            cid = r["complex_id"]
            complex_ids.append(cid)
            complexes[cid] = {
                "id": cid,
                "family_key": r["family_key"],
                "key": r["key"],
                "name": r["name"],
                "enabled": r["enabled"],
                "params": {},  # заполним ниже
            }

        # загружаем параметры для выбранных комплексов
        params_rows = await conn.fetch(
            """
            SELECT
                complex_id,
                param_name,
                param_type,
                param_value
            FROM bt_complex_parameters
            WHERE complex_id = ANY($1::int[])
            """,
            complex_ids,
        )

    for p in params_rows:
        cid = p["complex_id"]
        if cid not in complexes:
            continue
        cfg = complexes[cid]
        params = cfg.setdefault("params", {})
        params[p["param_name"]] = {
            "type": p["param_type"],
            "value": p["param_value"],
        }

    complexes_list = list(complexes.values())

    log.debug(
        "BT_COMPLEX_MAIN: для scenario_id=%s, signal_id=%s загружено комплексов: %s",
        scenario_id,
        signal_id,
        len(complexes_list),
    )
    return complexes_list

# 🔸 Запуск одного комплекса: очистка результатов, запуск воркера, запись raw и пересчёт bin-статистики
async def _run_complex(
    complex_cfg: Dict[str, Any],
    scenario_id: int,
    signal_id: int,
    pg,
    redis,
) -> Dict[str, Any]:
    complex_id = complex_cfg.get("id")
    family_key = str(complex_cfg.get("family_key") or "").strip()
    complex_key = str(complex_cfg.get("key") or "").strip()
    name = complex_cfg.get("name")
    params = complex_cfg.get("params") or {}

    handler = COMPLEX_HANDLERS.get((family_key, complex_key))
    if handler is None:
        log.debug(
            "BT_COMPLEX_MAIN: комплекс id=%s (family=%s, key=%s, name=%s) пока не поддерживается реестром комплексов",
            complex_id,
            family_key,
            complex_key,
            name,
        )
        return {
            "complex_id": complex_id,
            "status": "skipped",
            "rows_inserted": 0,
            "bins_rows": 0,
        }

    # формируем indicator_param из параметров комплекса (tf + ema_length + window_bars)
    tf_cfg = params.get("tf") or {}
    ema_len_cfg = params.get("ema_length") or {}
    window_cfg = params.get("window_bars") or {}

    tf_val = str(tf_cfg.get("value")).strip() if tf_cfg.get("value") is not None else None
    ema_len_val = str(ema_len_cfg.get("value")).strip() if ema_len_cfg.get("value") is not None else None
    window_val = str(window_cfg.get("value")).strip() if window_cfg.get("value") is not None else None

    if tf_val and ema_len_val and window_val:
        indicator_param = f"ema{ema_len_val}_{tf_val}_w{window_val}"
    else:
        indicator_param = None

    log.debug(
        "BT_COMPLEX_MAIN: запуск комплекса id=%s (family=%s, key=%s, name=%s, indicator_param=%s) "
        "для scenario_id=%s, signal_id=%s",
        complex_id,
        family_key,
        complex_key,
        name,
        indicator_param,
        scenario_id,
        signal_id,
    )

    # очистка предыдущих результатов для данного комплекса и пары (сценарий, сигнал)
    async with pg.acquire() as conn:
        await conn.execute(
            """
            DELETE FROM bt_complex_positions_raw
            WHERE complex_id = $1
              AND scenario_id = $2
              AND signal_id   = $3
            """,
            complex_id,
            scenario_id,
            signal_id,
        )
        await conn.execute(
            """
            DELETE FROM bt_complex_bins_stat
            WHERE complex_id = $1
              AND scenario_id = $2
              AND signal_id   = $3
            """,
            complex_id,
            scenario_id,
            signal_id,
        )

    # контекст для комплекса
    complex_ctx: Dict[str, Any] = {
        "scenario_id": scenario_id,
        "signal_id": signal_id,
    }

    try:
        # запускаем бизнес-логику комплекса
        result: Dict[str, Any] = await handler(complex_cfg, complex_ctx, pg, redis)
        rows: List[Dict[str, Any]] = (result or {}).get("rows") or []

        if not rows:
            log.debug(
                "BT_COMPLEX_MAIN: комплекс id=%s (family=%s, key=%s) для scenario_id=%s, signal_id=%s "
                "не вернул строк для вставки (raw)",
                complex_id,
                family_key,
                complex_key,
                scenario_id,
                signal_id,
            )
            return {
                "complex_id": complex_id,
                "status": "ok",
                "rows_inserted": 0,
                "bins_rows": 0,
            }

        # подготовка данных для массовой вставки в bt_complex_positions_raw
        to_insert: List[Tuple[Any, ...]] = []
        for row in rows:
            position_uid = row.get("position_uid")
            timeframe = row.get("timeframe")
            direction = row.get("direction")
            bin_name = row.get("bin_name")
            value = row.get("value")
            pnl_abs = row.get("pnl_abs")

            if not position_uid or timeframe is None or direction is None or bin_name is None:
                # пропускаем некорректные строки
                log.debug(
                    "BT_COMPLEX_MAIN: комплекс id=%s (family=%s, key=%s) вернул некорректную строку, "
                    "она будет пропущена: %s",
                    complex_id,
                    family_key,
                    complex_key,
                    row,
                )
                continue

            to_insert.append(
                (
                    complex_id,
                    position_uid,
                    scenario_id,
                    signal_id,
                    family_key,
                    complex_key,
                    timeframe,
                    direction,
                    bin_name,
                    value,
                    pnl_abs,
                )
            )

        inserted = 0
        bins_rows = 0

        if to_insert:
            async with pg.acquire() as conn:
                await conn.executemany(
                    """
                    INSERT INTO bt_complex_positions_raw (
                        complex_id,
                        position_uid,
                        scenario_id,
                        signal_id,
                        family_key,
                        "key",
                        timeframe,
                        direction,
                        bin_name,
                        value,
                        pnl_abs
                    )
                    VALUES (
                        $1, $2, $3, $4,
                        $5, $6, $7, $8,
                        $9, $10, $11
                    )
                    """,
                    to_insert,
                )
            inserted = len(to_insert)

            # пересчёт статистики по биннам для данного комплекса и пары
            bins_rows, trades_total = await _recalc_complex_bins_stat(
                pg=pg,
                complex_id=complex_id,
                scenario_id=scenario_id,
                signal_id=signal_id,
                indicator_param=indicator_param,
            )

            log.info(
                "BT_COMPLEX_MAIN: комплекс id=%s (family=%s, key=%s, name=%s) "
                "для scenario_id=%s, signal_id=%s записал raw строк=%s и bins строк=%s (trades_total=%s)",
                complex_id,
                family_key,
                complex_key,
                name,
                scenario_id,
                signal_id,
                inserted,
                bins_rows,
                trades_total,
            )
        else:
            log.debug(
                "BT_COMPLEX_MAIN: комплекс id=%s (family=%s, key=%s, name=%s) для scenario_id=%s, signal_id=%s "
                "не сформировал валидных строк raw после фильтрации",
                complex_id,
                family_key,
                complex_key,
                name,
                scenario_id,
                signal_id,
            )

        return {
            "complex_id": complex_id,
            "status": "ok",
            "rows_inserted": inserted,
            "bins_rows": bins_rows,
        }

    except Exception as e:
        log.error(
            "BT_COMPLEX_MAIN: ошибка выполнения комплекса id=%s (family=%s, key=%s) "
            "для scenario_id=%s, signal_id=%s: %s",
            complex_id,
            family_key,
            complex_key,
            scenario_id,
            signal_id,
            e,
            exc_info=True,
        )
        return {
            "complex_id": complex_id,
            "status": "error",
            "rows_inserted": 0,
            "bins_rows": 0,
        }

# 🔸 Пересчёт статистики по биннам для комплексов в bt_complex_bins_stat
async def _recalc_complex_bins_stat(
    pg,
    complex_id: int,
    scenario_id: int,
    signal_id: int,
    indicator_param: Optional[str],
) -> Tuple[int, int]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                timeframe,
                direction,
                bin_name,
                COUNT(*)                                         AS trades,
                COUNT(*) FILTER (WHERE pnl_abs > 0)              AS wins,
                COALESCE(SUM(pnl_abs), 0)                        AS pnl_abs_total
            FROM bt_complex_positions_raw
            WHERE complex_id = $1
              AND scenario_id = $2
              AND signal_id   = $3
            GROUP BY timeframe, direction, bin_name
            """,
            complex_id,
            scenario_id,
            signal_id,
        )

        if not rows:
            log.debug(
                "BT_COMPLEX_MAIN: для комплекса id=%s, scenario_id=%s, signal_id=%s нет строк raw "
                "для пересчёта bins_stat",
                complex_id,
                scenario_id,
                signal_id,
            )
            return 0, 0

        # удаляем старые строки bins_stat для этого комплекса и пары
        await conn.execute(
            """
            DELETE FROM bt_complex_bins_stat
            WHERE complex_id = $1
              AND scenario_id = $2
              AND signal_id   = $3
            """,
            complex_id,
            scenario_id,
            signal_id,
        )

        to_insert: List[Tuple[Any, ...]] = []
        total_trades = 0

        for r in rows:
            timeframe = r["timeframe"]
            direction = r["direction"]
            bin_name = r["bin_name"]
            trades = int(r["trades"])
            wins = int(r["wins"])
            pnl_abs_total = Decimal(str(r["pnl_abs_total"]))

            total_trades += trades

            if trades > 0:
                winrate = Decimal(wins) / Decimal(trades)
            else:
                winrate = Decimal("0")

            # лёгкая квантизация для предсказуемой точности
            pnl_abs_q = pnl_abs_total.quantize(Decimal("0.0001"))
            winrate_q = winrate.quantize(Decimal("0.0001"))

            to_insert.append(
                (
                    complex_id,
                    scenario_id,
                    signal_id,
                    indicator_param,
                    timeframe,
                    direction,
                    bin_name,
                    trades,
                    pnl_abs_q,
                    winrate_q,
                )
            )

        await conn.executemany(
            """
            INSERT INTO bt_complex_bins_stat (
                complex_id,
                scenario_id,
                signal_id,
                indicator_param,
                timeframe,
                direction,
                bin_name,
                trades,
                pnl_abs,
                winrate
            )
            VALUES (
                $1, $2, $3, $4,
                $5, $6, $7,
                $8, $9, $10
            )
            """,
            to_insert,
        )

    bins_count = len(to_insert)

    log.debug(
        "BT_COMPLEX_MAIN: пересчитана статистика bins_stat для complex_id=%s, scenario_id=%s, signal_id=%s — "
        "бинов=%s, trades_total=%s",
        complex_id,
        scenario_id,
        signal_id,
        bins_count,
        total_trades,
    )
    return bins_count, total_trades


# 🔸 Публикация события готовности комплексного анализа в bt:complex:ready
async def _publish_complex_ready(
    redis,
    scenario_id: int,
    signal_id: int,
    complex_total: int,
    complex_ok: int,
    complex_failed: int,
    rows_inserted: int,
    bins_rows: int,
) -> None:
    finished_at = datetime.utcnow()

    try:
        await redis.xadd(
            COMPLEX_READY_STREAM_KEY,
            {
                "scenario_id": str(scenario_id),
                "signal_id": str(signal_id),
                "complex_total": str(complex_total),
                "complex_ok": str(complex_ok),
                "complex_failed": str(complex_failed),
                "rows_raw": str(rows_inserted),
                "rows_bins": str(bins_rows),
                "finished_at": finished_at.isoformat(),
            },
        )
        log.debug(
            "BT_COMPLEX_MAIN: опубликовано событие готовности комплексного анализа в стрим '%s' "
            "для scenario_id=%s, signal_id=%s, complex_total=%s, complex_ok=%s, "
            "complex_failed=%s, rows_raw=%s, rows_bins=%s, finished_at=%s",
            COMPLEX_READY_STREAM_KEY,
            scenario_id,
            signal_id,
            complex_total,
            complex_ok,
            complex_failed,
            rows_inserted,
            bins_rows,
            finished_at,
        )
    except Exception as e:
        log.error(
            "BT_COMPLEX_MAIN: не удалось опубликовать событие в стрим '%s' "
            "для scenario_id=%s, signal_id=%s: %s",
            COMPLEX_READY_STREAM_KEY,
            scenario_id,
            signal_id,
            e,
            exc_info=True,
        )