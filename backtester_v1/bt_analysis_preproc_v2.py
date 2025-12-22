# bt_analysis_preproc_v2.py — упрощённый препроцессинг v2 (полная очистка v2-контура, разметка биннов по знаку pnl_abs и публикация bt:analysis:preproc_ready_v2)

import asyncio
import json
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict, List, Optional, Tuple


# 🔸 Константы стримов и настроек препроцессинга v2
ANALYSIS_READY_STREAM_KEY = "bt:analysis:ready"
PREPROC_READY_STREAM_KEY = "bt:analysis:preproc_ready_v2"

PREPROC_CONSUMER_GROUP = "bt_analysis_preproc_v2"
PREPROC_CONSUMER_NAME = "bt_analysis_preproc_v2_main"

PREPROC_STREAM_BATCH_SIZE = 10
PREPROC_STREAM_BLOCK_MS = 5000

PREPROC_MAX_CONCURRENCY = 6


# 🔸 Константы логики v2 (простая разметка биннов)
V2_BEST_THRESHOLD = Decimal("0")  # техническое поле модели; в v2-подходе порог не используется
V2_META_VERSION = 1
V2_METHOD = "simple_bin_pnl_sign"


# 🔸 Кеш последних source_finished_at по (scenario_id, signal_id) для отсечки дублей
_last_analysis_finished_at: Dict[Tuple[int, int], datetime] = {}

log = logging.getLogger("BT_ANALYSIS_PREPROC_V2")


# 🔸 Публичная точка входа: оркестратор препроцессинга v2
async def run_bt_analysis_preproc_v2_orchestrator(pg, redis):
    log.debug("BT_ANALYSIS_PREPROC_V2: оркестратор запущен")

    await _ensure_consumer_group(redis)

    # общий семафор для ограничения параллелизма по парам (scenario_id, signal_id)
    sema = asyncio.Semaphore(PREPROC_MAX_CONCURRENCY)

    while True:
        try:
            entries = await _read_from_stream(redis)
            if not entries:
                continue

            tasks: List[asyncio.Task] = []
            total_msgs = 0

            for stream_key, messages in entries:
                if stream_key != ANALYSIS_READY_STREAM_KEY:
                    continue

                for entry_id, fields in messages:
                    total_msgs += 1
                    task = asyncio.create_task(
                        _process_message(
                            entry_id=entry_id,
                            fields=fields,
                            pg=pg,
                            redis=redis,
                            sema=sema,
                        ),
                        name=f"BT_ANALYSIS_PREPROC_V2_{entry_id}",
                    )
                    tasks.append(task)

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                errors = sum(1 for r in results if isinstance(r, Exception))
                log.info(
                    "BT_ANALYSIS_PREPROC_V2: обработан пакет сообщений из bt:analysis:ready — сообщений=%s, ошибок=%s",
                    total_msgs,
                    errors,
                )

        except Exception as e:
            log.error(
                "BT_ANALYSIS_PREPROC_V2: ошибка в основном цикле оркестратора: %s",
                e,
                exc_info=True,
            )
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима bt:analysis:ready
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=ANALYSIS_READY_STREAM_KEY,
            groupname=PREPROC_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_ANALYSIS_PREPROC_V2: создана consumer group '%s' для стрима '%s'",
            PREPROC_CONSUMER_GROUP,
            ANALYSIS_READY_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_ANALYSIS_PREPROC_V2: consumer group '%s' для стрима '%s' уже существует",
                PREPROC_CONSUMER_GROUP,
                ANALYSIS_READY_STREAM_KEY,
            )
        else:
            log.error(
                "BT_ANALYSIS_PREPROC_V2: ошибка при создании consumer group '%s': %s",
                PREPROC_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:analysis:ready
async def _read_from_stream(redis) -> List[Any]:
    entries = await redis.xreadgroup(
        groupname=PREPROC_CONSUMER_GROUP,
        consumername=PREPROC_CONSUMER_NAME,
        streams={ANALYSIS_READY_STREAM_KEY: ">"},
        count=PREPROC_STREAM_BATCH_SIZE,
        block=PREPROC_STREAM_BLOCK_MS,
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


# 🔸 Разбор одного сообщения из стрима bt:analysis:ready
def _parse_analysis_ready_message(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id_str = fields.get("scenario_id")
        signal_id_str = fields.get("signal_id")
        finished_at_str = fields.get("finished_at")

        if not (scenario_id_str and signal_id_str and finished_at_str):
            return None

        scenario_id = int(scenario_id_str)
        signal_id = int(signal_id_str)
        source_finished_at = datetime.fromisoformat(finished_at_str)

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "source_finished_at": source_finished_at,
        }
    except Exception as e:
        log.error(
            "BT_ANALYSIS_PREPROC_V2: ошибка разбора сообщения стрима bt:analysis:ready: %s, fields=%s",
            e,
            fields,
            exc_info=True,
        )
        return None


# 🔸 Обработка одного сообщения из bt:analysis:ready с ограничением семафором
async def _process_message(
    entry_id: str,
    fields: Dict[str, str],
    pg,
    redis,
    sema: asyncio.Semaphore,
) -> None:
    async with sema:
        ctx = _parse_analysis_ready_message(fields)
        if not ctx:
            await redis.xack(ANALYSIS_READY_STREAM_KEY, PREPROC_CONSUMER_GROUP, entry_id)
            return

        scenario_id = ctx["scenario_id"]
        signal_id = ctx["signal_id"]
        source_finished_at = ctx["source_finished_at"]

        pair_key = (scenario_id, signal_id)
        last_finished = _last_analysis_finished_at.get(pair_key)

        # отсечка дублей по равному source_finished_at
        if last_finished is not None and last_finished == source_finished_at:
            log.debug(
                "BT_ANALYSIS_PREPROC_V2: дубликат сообщения для scenario_id=%s, signal_id=%s, source_finished_at=%s, stream_id=%s — расчёт не выполняется",
                scenario_id,
                signal_id,
                source_finished_at,
                entry_id,
            )
            await redis.xack(ANALYSIS_READY_STREAM_KEY, PREPROC_CONSUMER_GROUP, entry_id)
            return

        _last_analysis_finished_at[pair_key] = source_finished_at

        started_at = datetime.utcnow()

        try:
            # очистка всего v2-контура по паре (scenario_id, signal_id) перед прогоном "с чистого листа"
            cleanup = await _cleanup_v2_tables_for_pair(pg, scenario_id, signal_id)

            # определяем направления сигнала
            direction_mask = await _load_signal_direction_mask(pg, signal_id)
            directions = _directions_from_mask(direction_mask)

            # считаем модель и разметку отдельно по каждому направлению
            results: Dict[str, Dict[str, Any]] = {}

            for direction in directions:
                res = await _rebuild_model_and_labels_for_direction(
                    pg=pg,
                    scenario_id=scenario_id,
                    signal_id=signal_id,
                    direction=direction,
                    direction_mask=direction_mask,
                    source_finished_at=source_finished_at,
                )
                results[direction] = res

            # публикуем событие готовности препроцессинга v2
            await _publish_preproc_ready_v2(
                redis=redis,
                scenario_id=scenario_id,
                signal_id=signal_id,
                source_finished_at=source_finished_at,
                direction_mask=direction_mask,
            )

            elapsed_ms = int((datetime.utcnow() - started_at).total_seconds() * 1000)

            # суммарный лог
            parts: List[str] = []
            bins_total_all = 0
            bins_good_all = 0
            bins_bad_all = 0

            for d in directions:
                r = results.get(d) or {}
                bins_total_all += int(r.get("bins_total", 0) or 0)
                bins_good_all += int(r.get("bins_good", 0) or 0)
                bins_bad_all += int(r.get("bins_bad", 0) or 0)

                parts.append(
                    f"{d} model_id={r.get('model_id')} bins={r.get('bins_total')} good={r.get('bins_good')} bad={r.get('bins_bad')} analyses={r.get('analysis_ids_cnt')}"
                )

            log.info(
                "BT_ANALYSIS_PREPROC_V2: scenario_id=%s, signal_id=%s — cleanup_total=%s (labels=%s model=%s postproc=%s stat=%s daily=%s), "
                "direction_mask=%s, directions=%s, %s, bins_total=%s good=%s bad=%s, source_finished_at=%s, elapsed_ms=%s",
                scenario_id,
                signal_id,
                cleanup.get("total", 0),
                cleanup.get("labels", 0),
                cleanup.get("model", 0),
                cleanup.get("positions_postproc", 0),
                cleanup.get("scenario_stat", 0),
                cleanup.get("scenario_daily", 0),
                direction_mask,
                directions,
                " | ".join(parts) if parts else "no_results",
                bins_total_all,
                bins_good_all,
                bins_bad_all,
                source_finished_at,
                elapsed_ms,
            )

        except Exception as e:
            log.error(
                "BT_ANALYSIS_PREPROC_V2: ошибка расчёта для scenario_id=%s, signal_id=%s: %s",
                scenario_id,
                signal_id,
                e,
                exc_info=True,
            )
        finally:
            await redis.xack(ANALYSIS_READY_STREAM_KEY, PREPROC_CONSUMER_GROUP, entry_id)


# 🔸 Очистка всех результатов v2-контура по паре (scenario_id, signal_id)
async def _cleanup_v2_tables_for_pair(pg, scenario_id: int, signal_id: int) -> Dict[str, int]:
    deleted_labels = 0
    deleted_model = 0
    deleted_postproc = 0
    deleted_stat = 0
    deleted_daily = 0

    async with pg.acquire() as conn:
        async with conn.transaction():
            # сначала labels (на случай частичных записей)
            res_labels = await conn.execute(
                """
                DELETE FROM bt_analysis_bins_labels_v2
                WHERE scenario_id = $1
                  AND signal_id   = $2
                """,
                scenario_id,
                signal_id,
            )
            deleted_labels = _parse_pg_execute_count(res_labels)

            # затем модели (и всё, что может быть связано каскадом)
            res_model = await conn.execute(
                """
                DELETE FROM bt_analysis_model_opt_v2
                WHERE scenario_id = $1
                  AND signal_id   = $2
                """,
                scenario_id,
                signal_id,
            )
            deleted_model = _parse_pg_execute_count(res_model)

            # контейнер позиций postproc_v2
            res_postproc = await conn.execute(
                """
                DELETE FROM bt_analysis_positions_postproc_v2
                WHERE scenario_id = $1
                  AND signal_id   = $2
                """,
                scenario_id,
                signal_id,
            )
            deleted_postproc = _parse_pg_execute_count(res_postproc)

            # итоговая статистика v2 по сценарию
            res_stat = await conn.execute(
                """
                DELETE FROM bt_analysis_scenario_stat_v2
                WHERE scenario_id = $1
                  AND signal_id   = $2
                """,
                scenario_id,
                signal_id,
            )
            deleted_stat = _parse_pg_execute_count(res_stat)

            # суточная статистика v2
            res_daily = await conn.execute(
                """
                DELETE FROM bt_analysis_scenario_daily_v2
                WHERE scenario_id = $1
                  AND signal_id   = $2
                """,
                scenario_id,
                signal_id,
            )
            deleted_daily = _parse_pg_execute_count(res_daily)

    return {
        "labels": deleted_labels,
        "model": deleted_model,
        "positions_postproc": deleted_postproc,
        "scenario_stat": deleted_stat,
        "scenario_daily": deleted_daily,
        "total": (deleted_labels + deleted_model + deleted_postproc + deleted_stat + deleted_daily),
    }


# 🔸 Пересборка model_opt_v2 + bins_labels_v2 для одного направления по правилу pnl_abs sign
async def _rebuild_model_and_labels_for_direction(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
    direction_mask: Optional[str],
    source_finished_at: datetime,
) -> Dict[str, Any]:
    # загружаем строки bins_stat для пары и направления
    bins_rows = await _load_bins_stat_rows(pg, scenario_id, signal_id, direction)

    # набор анализаторов, которые реально присутствуют в bins_stat
    analysis_ids = sorted({int(r["analysis_id"]) for r in bins_rows}) if bins_rows else []

    # исходные метрики сценария/сигнала по направлению (для заполнения обязательных полей модели)
    orig = await _load_orig_scenario_stat_for_direction(pg, scenario_id, signal_id, direction)

    # meta модели
    meta_obj = {
        "version": V2_META_VERSION,
        "method": V2_METHOD,
        "direction": direction,
        "direction_mask": direction_mask,
        "rules": {
            "good": "pnl_abs > 0",
            "bad": "pnl_abs <= 0",
        },
        "analysis_ids": analysis_ids,
        "bins_total": len(bins_rows),
        "source_finished_at": source_finished_at.isoformat(),
    }

    # upsert model_opt_v2 и получаем model_id
    model_id = await _upsert_model_opt_v2_return_id(
        pg=pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        direction=direction,
        best_threshold=V2_BEST_THRESHOLD,
        selected_analysis_ids=analysis_ids,
        orig=orig,
        meta_obj=meta_obj,
        source_finished_at=source_finished_at,
    )

    # разметка bins_labels_v2 по model_id
    labels_stats = await _rebuild_bins_labels_v2(
        pg=pg,
        model_id=model_id,
        scenario_id=scenario_id,
        signal_id=signal_id,
        direction=direction,
        threshold_used=V2_BEST_THRESHOLD,
        bins_rows=bins_rows,
    )

    return {
        "model_id": model_id,
        "bins_total": int(labels_stats.get("total", 0)),
        "bins_good": int(labels_stats.get("good", 0)),
        "bins_bad": int(labels_stats.get("bad", 0)),
        "analysis_ids_cnt": len(analysis_ids),
    }


# 🔸 Загрузка строк bt_analysis_bins_stat для пары и направления (v2 использует общий bins_stat)
async def _load_bins_stat_rows(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                analysis_id,
                indicator_param,
                timeframe,
                bin_name,
                trades,
                pnl_abs,
                winrate
            FROM bt_analysis_bins_stat
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND direction   = $3
            """,
            scenario_id,
            signal_id,
            direction,
        )

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "analysis_id": int(r["analysis_id"]),
                "indicator_param": r["indicator_param"],
                "timeframe": str(r["timeframe"]),
                "bin_name": str(r["bin_name"]),
                "trades": int(r["trades"]),
                "pnl_abs": _safe_decimal(r["pnl_abs"]),
                "winrate": _safe_decimal(r["winrate"]),
            }
        )
    return out


# 🔸 Загрузка исходной статистики сценария/сигнала по направлению (для заполнения обязательных полей модели)
async def _load_orig_scenario_stat_for_direction(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
) -> Dict[str, Any]:
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                trades,
                pnl_abs,
                winrate,
                roi
            FROM bt_scenario_stat
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND direction   = $3
            LIMIT 1
            """,
            scenario_id,
            signal_id,
            direction,
        )

    if not row:
        return {
            "trades": 0,
            "pnl_abs": Decimal("0"),
            "winrate": Decimal("0"),
            "roi": Decimal("0"),
        }

    return {
        "trades": int(row["trades"]),
        "pnl_abs": _safe_decimal(row["pnl_abs"]),
        "winrate": _safe_decimal(row["winrate"]),
        "roi": _safe_decimal(row["roi"]),
    }


# 🔸 Upsert model_opt_v2 и возврат model_id
async def _upsert_model_opt_v2_return_id(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
    best_threshold: Decimal,
    selected_analysis_ids: List[int],
    orig: Dict[str, Any],
    meta_obj: Dict[str, Any],
    source_finished_at: datetime,
) -> int:
    # v2 на первом этапе хранит filt_* = orig_* (фактическая фильтрация будет рассчитана postproc_v2)
    meta_json = json.dumps(meta_obj, ensure_ascii=False)
    selected_json = json.dumps(selected_analysis_ids or [], ensure_ascii=False)

    orig_trades = int(orig.get("trades", 0) or 0)
    orig_pnl_abs = _q_decimal(_safe_decimal(orig.get("pnl_abs", 0)))
    orig_winrate = _q_decimal(_safe_decimal(orig.get("winrate", 0)))
    orig_roi = _q_decimal(_safe_decimal(orig.get("roi", 0)))

    filt_trades = orig_trades
    filt_pnl_abs = orig_pnl_abs
    filt_winrate = orig_winrate
    filt_roi = orig_roi

    removed_trades = 0
    removed_accuracy = Decimal("0")

    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO bt_analysis_model_opt_v2 (
                scenario_id,
                signal_id,
                direction,
                best_threshold,
                selected_analysis_ids,
                orig_trades,
                orig_pnl_abs,
                orig_winrate,
                orig_roi,
                filt_trades,
                filt_pnl_abs,
                filt_winrate,
                filt_roi,
                removed_trades,
                removed_accuracy,
                meta,
                source_finished_at
            )
            VALUES (
                $1, $2, $3,
                $4,
                $5::jsonb,
                $6, $7, $8, $9,
                $10, $11, $12, $13,
                $14, $15,
                $16::jsonb,
                $17
            )
            ON CONFLICT (scenario_id, signal_id, direction)
            DO UPDATE SET
                best_threshold        = EXCLUDED.best_threshold,
                selected_analysis_ids = EXCLUDED.selected_analysis_ids,
                orig_trades           = EXCLUDED.orig_trades,
                orig_pnl_abs          = EXCLUDED.orig_pnl_abs,
                orig_winrate          = EXCLUDED.orig_winrate,
                orig_roi              = EXCLUDED.orig_roi,
                filt_trades           = EXCLUDED.filt_trades,
                filt_pnl_abs          = EXCLUDED.filt_pnl_abs,
                filt_winrate          = EXCLUDED.filt_winrate,
                filt_roi              = EXCLUDED.filt_roi,
                removed_trades        = EXCLUDED.removed_trades,
                removed_accuracy      = EXCLUDED.removed_accuracy,
                meta                  = EXCLUDED.meta,
                source_finished_at    = EXCLUDED.source_finished_at,
                updated_at            = now()
            RETURNING id
            """,
            scenario_id,
            signal_id,
            direction,
            best_threshold,
            selected_json,
            orig_trades,
            orig_pnl_abs,
            orig_winrate,
            orig_roi,
            filt_trades,
            filt_pnl_abs,
            filt_winrate,
            filt_roi,
            removed_trades,
            _q_decimal(removed_accuracy),
            meta_json,
            source_finished_at,
        )

    return int(row["id"])


# 🔸 Пересборка разметки биннов bt_analysis_bins_labels_v2 для model_id (good/bad)
async def _rebuild_bins_labels_v2(
    pg,
    model_id: int,
    scenario_id: int,
    signal_id: int,
    direction: str,
    threshold_used: Decimal,
    bins_rows: List[Dict[str, Any]],
) -> Dict[str, int]:
    async with pg.acquire() as conn:
        # удаляем старую разметку по model_id (на случай повтора)
        await conn.execute(
            """
            DELETE FROM bt_analysis_bins_labels_v2
            WHERE model_id = $1
            """,
            model_id,
        )

        if not bins_rows:
            return {"total": 0, "good": 0, "bad": 0}

        to_insert: List[Tuple[Any, ...]] = []
        good_cnt = 0
        bad_cnt = 0

        for b in bins_rows:
            pnl_abs = _safe_decimal(b.get("pnl_abs", 0))
            trades = int(b.get("trades", 0) or 0)
            winrate = _safe_decimal(b.get("winrate", 0))

            # простая разметка по знаку pnl_abs
            if pnl_abs > 0:
                state = "good"
                good_cnt += 1
            else:
                state = "bad"
                bad_cnt += 1

            to_insert.append(
                (
                    model_id,
                    scenario_id,
                    signal_id,
                    direction,
                    int(b["analysis_id"]),
                    b.get("indicator_param"),
                    str(b["timeframe"]),
                    str(b["bin_name"]),
                    state,
                    threshold_used,
                    trades,
                    _q_decimal(pnl_abs),
                    _q_decimal(winrate),
                )
            )

        await conn.executemany(
            """
            INSERT INTO bt_analysis_bins_labels_v2 (
                model_id,
                scenario_id,
                signal_id,
                direction,
                analysis_id,
                indicator_param,
                timeframe,
                bin_name,
                state,
                threshold_used,
                trades,
                pnl_abs,
                winrate
            )
            VALUES (
                $1, $2, $3, $4,
                $5, $6, $7, $8,
                $9, $10,
                $11, $12, $13
            )
            """,
            to_insert,
        )

    return {"total": len(to_insert), "good": good_cnt, "bad": bad_cnt}


# 🔸 Публикация события готовности препроцессинга v2 в bt:analysis:preproc_ready_v2
async def _publish_preproc_ready_v2(
    redis,
    scenario_id: int,
    signal_id: int,
    source_finished_at: datetime,
    direction_mask: Optional[str],
) -> None:
    finished_at = datetime.utcnow()

    try:
        await redis.xadd(
            PREPROC_READY_STREAM_KEY,
            {
                "scenario_id": str(scenario_id),
                "signal_id": str(signal_id),
                "finished_at": finished_at.isoformat(),
                "source_finished_at": source_finished_at.isoformat(),
                "direction_mask": str(direction_mask) if direction_mask is not None else "",
            },
        )
        log.debug(
            "BT_ANALYSIS_PREPROC_V2: опубликовано событие preproc_ready_v2 в стрим '%s' для scenario_id=%s, signal_id=%s, finished_at=%s",
            PREPROC_READY_STREAM_KEY,
            scenario_id,
            signal_id,
            finished_at,
        )
    except Exception as e:
        log.error(
            "BT_ANALYSIS_PREPROC_V2: не удалось опубликовать событие в стрим '%s' для scenario_id=%s, signal_id=%s: %s",
            PREPROC_READY_STREAM_KEY,
            scenario_id,
            signal_id,
            e,
            exc_info=True,
        )


# 🔸 Загрузка direction_mask сигнала из bt_signals_parameters (param_name='direction_mask')
async def _load_signal_direction_mask(pg, signal_id: int) -> Optional[str]:
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT param_value
            FROM bt_signals_parameters
            WHERE signal_id  = $1
              AND param_name = 'direction_mask'
            LIMIT 1
            """,
            signal_id,
        )

    if not row:
        return None

    value = row["param_value"]
    if value is None:
        return None

    return str(value).strip().lower() or None


# 🔸 Преобразование direction_mask -> список направлений
def _directions_from_mask(mask: Optional[str]) -> List[str]:
    if not mask:
        return ["long", "short"]

    m = mask.strip().lower()

    if m == "long":
        return ["long"]
    if m == "short":
        return ["short"]

    if m in ("both", "all", "any", "long_short", "short_long", "long+short", "short+long", "long|short", "short|long"):
        return ["long", "short"]

    return ["long", "short"]


# 🔸 Парсинг результата asyncpg conn.execute вида "DELETE 123"
def _parse_pg_execute_count(res: Any) -> int:
    try:
        return int(str(res).split()[-1])
    except Exception:
        return 0


# 🔸 Вспомогательная функция: безопасное приведение к Decimal
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


# 🔸 Вспомогательная функция: квантизация Decimal до 4 знаков (вниз для предсказуемости)
def _q_decimal(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)