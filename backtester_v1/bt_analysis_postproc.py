# bt_analysis_postproc.py — финальная пост-обработка результатов анализаторов по сценариям/сигналам (применение разметки биннов из bt_analysis_bins_labels)

import asyncio
import json
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Константы стримов и настроек постпроцессинга
PREPROC_READY_STREAM_KEY = "bt:analysis:preproc_ready"
POSTPROC_STREAM_KEY = "bt:analysis:postproc_ready"

POSTPROC_CONSUMER_GROUP = "bt_analysis_postproc"
POSTPROC_CONSUMER_NAME = "bt_analysis_postproc_main"

POSTPROC_STREAM_BATCH_SIZE = 10
POSTPROC_STREAM_BLOCK_MS = 5000

POSTPROC_MAX_CONCURRENCY = 8

# 🔸 Fallback порога (если по какой-то причине модель не найдена)
DEFAULT_MIN_WINRATE_THRESHOLD = Decimal("0.3334")

# 🔸 Кеш последних source_finished_at по (scenario_id, signal_id) для отсечки дублей
_last_preproc_source_finished_at: Dict[Tuple[int, int], datetime] = {}

log = logging.getLogger("BT_ANALYSIS_POSTPROC")


# 🔸 Публичная точка входа: оркестратор финального постпроцессинга анализов
async def run_bt_analysis_postproc_orchestrator(pg, redis):
    log.debug("BT_ANALYSIS_POSTPROC: оркестратор финального постпроцессинга запущен")

    await _ensure_consumer_group(redis)

    # общий семафор для ограничения параллелизма по парам (scenario_id, signal_id)
    sema = asyncio.Semaphore(POSTPROC_MAX_CONCURRENCY)

    while True:
        try:
            entries = await _read_from_stream(redis)

            if not entries:
                continue

            tasks: List[asyncio.Task] = []
            total_msgs = 0

            for stream_key, messages in entries:
                if stream_key != PREPROC_READY_STREAM_KEY:
                    # на всякий случай игнорируем чужие стримы
                    continue

                for entry_id, fields in messages:
                    total_msgs += 1

                    # создаём задачу обработки сообщения
                    task = asyncio.create_task(
                        _process_message(
                            entry_id=entry_id,
                            fields=fields,
                            pg=pg,
                            redis=redis,
                            sema=sema,
                        ),
                        name=f"BT_ANALYSIS_POSTPROC_{entry_id}",
                    )
                    tasks.append(task)

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                errors = sum(1 for r in results if isinstance(r, Exception))
                log.debug(
                    "BT_ANALYSIS_POSTPROC: обработан пакет сообщений из bt:analysis:preproc_ready — сообщений=%s, ошибок=%s",
                    total_msgs,
                    errors,
                )

        except Exception as e:
            log.error(
                "BT_ANALYSIS_POSTPROC: ошибка в основном цикле оркестратора: %s",
                e,
                exc_info=True,
            )
            # небольшая пауза, чтобы не крутить CPU при постоянной ошибке
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима bt:analysis:preproc_ready
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=PREPROC_READY_STREAM_KEY,
            groupname=POSTPROC_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_ANALYSIS_POSTPROC: создана consumer group '%s' для стрима '%s'",
            POSTPROC_CONSUMER_GROUP,
            PREPROC_READY_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_ANALYSIS_POSTPROC: consumer group '%s' для стрима '%s' уже существует",
                POSTPROC_CONSUMER_GROUP,
                PREPROC_READY_STREAM_KEY,
            )
        else:
            log.error(
                "BT_ANALYSIS_POSTPROC: ошибка при создании consumer group '%s': %s",
                POSTPROC_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:analysis:preproc_ready
async def _read_from_stream(redis) -> List[Any]:
    entries = await redis.xreadgroup(
        groupname=POSTPROC_CONSUMER_GROUP,
        consumername=POSTPROC_CONSUMER_NAME,
        streams={PREPROC_READY_STREAM_KEY: ">"},
        count=POSTPROC_STREAM_BATCH_SIZE,
        block=POSTPROC_STREAM_BLOCK_MS,
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


# 🔸 Разбор одного сообщения из стрима bt:analysis:preproc_ready
def _parse_preproc_ready_message(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id_str = fields.get("scenario_id")
        signal_id_str = fields.get("signal_id")
        finished_at_str = fields.get("finished_at")
        source_finished_at_str = fields.get("source_finished_at")

        if not (scenario_id_str and signal_id_str and finished_at_str):
            return None

        scenario_id = int(scenario_id_str)
        signal_id = int(signal_id_str)
        finished_at = datetime.fromisoformat(finished_at_str)

        source_finished_at = None
        if source_finished_at_str:
            try:
                source_finished_at = datetime.fromisoformat(source_finished_at_str)
            except Exception:
                source_finished_at = None

        direction_mask = (fields.get("direction_mask") or "").strip().lower() or None

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "finished_at": finished_at,
            "source_finished_at": source_finished_at,
            "direction_mask": direction_mask,
        }
    except Exception as e:
        log.error(
            "BT_ANALYSIS_POSTPROC: ошибка разбора сообщения стрима bt:analysis:preproc_ready: %s, fields=%s",
            e,
            fields,
            exc_info=True,
        )
        return None


# 🔸 Обработка одного сообщения из bt:analysis:preproc_ready с ограничением семафором
async def _process_message(
    entry_id: str,
    fields: Dict[str, str],
    pg,
    redis,
    sema: asyncio.Semaphore,
) -> None:
    async with sema:
        ctx = _parse_preproc_ready_message(fields)
        if not ctx:
            # невалидное сообщение — помечаем как обработанное и выходим
            await redis.xack(PREPROC_READY_STREAM_KEY, POSTPROC_CONSUMER_GROUP, entry_id)
            return

        scenario_id = ctx["scenario_id"]
        signal_id = ctx["signal_id"]
        finished_at = ctx["finished_at"]
        source_finished_at = ctx["source_finished_at"]
        direction_mask_from_msg = ctx.get("direction_mask")

        # если есть source_finished_at — используем его для дедупа; иначе fallback на finished_at
        dedup_ts = source_finished_at or finished_at

        pair_key = (scenario_id, signal_id)
        last_finished = _last_preproc_source_finished_at.get(pair_key)

        # отсечка дублей по равному dedup_ts
        if last_finished is not None and last_finished == dedup_ts:
            log.debug(
                "BT_ANALYSIS_POSTPROC: дубликат сообщения для scenario_id=%s, signal_id=%s, "
                "dedup_ts=%s, stream_id=%s — постпроцессинг не выполняется",
                scenario_id,
                signal_id,
                dedup_ts,
                entry_id,
            )
            await redis.xack(PREPROC_READY_STREAM_KEY, POSTPROC_CONSUMER_GROUP, entry_id)
            return

        _last_preproc_source_finished_at[pair_key] = dedup_ts

        log.debug(
            "BT_ANALYSIS_POSTPROC: получено сообщение preproc_ready "
            "scenario_id=%s, signal_id=%s, finished_at=%s, source_finished_at=%s, stream_id=%s",
            scenario_id,
            signal_id,
            finished_at,
            source_finished_at,
            entry_id,
        )

        try:
            # определяем направления сигнала (из сообщения или из БД)
            direction_mask = direction_mask_from_msg or await _load_signal_direction_mask(pg, signal_id)
            directions = _directions_from_mask(direction_mask)

            # загружаем модели по направлениям
            model_map = await _load_model_opt_map(pg, scenario_id, signal_id, directions)

            # если модели нет — всё равно попробуем отработать с fallback порогом (но без bad bins это бессмысленно)
            if not model_map:
                log.warning(
                    "BT_ANALYSIS_POSTPROC: bt_analysis_model_opt не найден для scenario_id=%s, signal_id=%s — постпроцессинг пропущен",
                    scenario_id,
                    signal_id,
                )
                await redis.xack(PREPROC_READY_STREAM_KEY, POSTPROC_CONSUMER_GROUP, entry_id)
                return

            # применяем разметку биннов из bt_analysis_bins_labels
            result = await _process_pair_postproc(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                directions=directions,
                model_map=model_map,
            )

            # публикуем событие готовности финального постпроцессинга
            await _publish_postproc_ready(
                redis=redis,
                scenario_id=scenario_id,
                signal_id=signal_id,
                result=result,
                model_map=model_map,
                source_finished_at=dedup_ts,
            )

            log.debug(
                "BT_ANALYSIS_POSTPROC: постпроцессинг завершён для scenario_id=%s, signal_id=%s — позиции_всего=%s, хорошие=%s, плохие=%s, models=%s",
                scenario_id,
                signal_id,
                result.get("positions_total", 0),
                result.get("positions_good", 0),
                result.get("positions_bad", 0),
                {d: {"model_id": model_map[d]["model_id"], "thr": str(model_map[d]["best_threshold"])} for d in model_map},
            )

        except Exception as e:
            log.error(
                "BT_ANALYSIS_POSTPROC: ошибка постпроцессинга для scenario_id=%s, signal_id=%s: %s",
                scenario_id,
                signal_id,
                e,
                exc_info=True,
            )
        finally:
            # помечаем сообщение как обработанное в любом случае, чтобы не зациклиться
            await redis.xack(PREPROC_READY_STREAM_KEY, POSTPROC_CONSUMER_GROUP, entry_id)


# 🔸 Основной постпроцессинг для одной пары (scenario_id, signal_id) по разметке биннов
async def _process_pair_postproc(
    pg,
    scenario_id: int,
    signal_id: int,
    directions: List[str],
    model_map: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    log.debug(
        "BT_ANALYSIS_POSTPROC: старт постпроцессинга для scenario_id=%s, signal_id=%s, directions=%s",
        scenario_id,
        signal_id,
        directions,
    )

    # загружаем позиции сценария/сигнала (postproc=true) только по нужным направлениям
    positions = await _load_positions_for_pair(pg, scenario_id, signal_id, directions)
    if not positions:
        log.debug(
            "BT_ANALYSIS_POSTPROC: нет позиций для постпроцессинга scenario_id=%s, signal_id=%s, directions=%s",
            scenario_id,
            signal_id,
            directions,
        )

        # чистим контейнер на всякий случай
        async with pg.acquire() as conn:
            await conn.execute(
                """
                DELETE FROM bt_analysis_positions_postproc
                WHERE scenario_id = $1
                  AND signal_id   = $2
                """,
                scenario_id,
                signal_id,
            )

        # bt_analysis_scenario_stat в этом кейсе не чистим — оставим как есть
        return {
            "positions_total": 0,
            "positions_good": 0,
            "positions_bad": 0,
        }

    # структура: position_uid -> {pnl_abs, direction, good_state, bad_reasons: [...]}
    positions_map: Dict[Any, Dict[str, Any]] = {}
    for p in positions:
        positions_map[p["position_uid"]] = {
            "pnl_abs": p["pnl_abs"],
            "direction": p["direction"],
            "good_state": True,
            "bad_reasons": [],
        }

    positions_total = len(positions_map)

    # агрегат по отбраковкам: direction -> indicator_param -> {total_trades, total_pnl, by_tf{tf->{trades,pnl}}}
    removed_stats: Dict[str, Dict[str, Dict[str, Any]]] = {}
    removed_seen: set = set()

    # threshold_map для записи в bt_analysis_scenario_stat.raw_stat
    threshold_map: Dict[str, Decimal] = {}

    total_bad_bins = 0
    total_bad_hits = 0

    for direction in directions:
        model = model_map.get(direction)
        if not model:
            continue

        model_id = int(model.get("model_id"))
        best_threshold = _safe_decimal(model.get("best_threshold") or DEFAULT_MIN_WINRATE_THRESHOLD)
        threshold_map[direction] = best_threshold

        # загружаем bad bins напрямую из labels (state='bad')
        bad_bins = await _load_bad_bins_from_labels(
            pg=pg,
            model_id=model_id,
            scenario_id=scenario_id,
            signal_id=signal_id,
            direction=direction,
        )
        total_bad_bins += len(bad_bins)

        if not bad_bins:
            continue

        # набор analysis_id для ускорения индекса
        bad_analysis_ids = sorted({b["analysis_id"] for b in bad_bins})

        # строим индекс raw только по нужным analysis_id для этого направления
        raw_index = await _load_positions_raw_index_for_analysis_ids(
            pg=pg,
            scenario_id=scenario_id,
            signal_id=signal_id,
            direction=direction,
            analysis_ids=bad_analysis_ids,
        )

        # применяем bad bins к позициям
        for b in bad_bins:
            analysis_id = b["analysis_id"]
            indicator_param = b["indicator_param"]
            timeframe = b["timeframe"]
            bin_name = b["bin_name"]

            key = (analysis_id, timeframe, bin_name)
            pos_uids = raw_index.get(key, [])

            if not pos_uids:
                continue

            indicator_key = indicator_param if indicator_param is not None else "_none_"

            for uid in pos_uids:
                pos = positions_map.get(uid)
                if pos is None:
                    continue

                pos["bad_reasons"].append(
                    {
                        "analysis_id": analysis_id,
                        "family_key": b["family_key"],
                        "key": b["analysis_key"],
                        "indicator_param": indicator_param,
                        "timeframe": timeframe,
                        "direction": direction,
                        "bin_name": bin_name,
                        "threshold": float(b["threshold_used"]),
                        "winrate": float(b["winrate"]),
                        "trades": int(b["trades"]),
                        "pnl_abs": float(b["pnl_abs"]),
                    }
                )
                pos["good_state"] = False
                total_bad_hits += 1

                # ключ для отсечения дублей в removed_stats
                seen_key = (direction, indicator_key, timeframe, uid)
                if seen_key in removed_seen:
                    continue
                removed_seen.add(seen_key)

                d_stats = removed_stats.setdefault(direction, {})
                i_stats = d_stats.setdefault(
                    indicator_key,
                    {
                        "total_trades": 0,
                        "total_pnl": Decimal("0"),
                        "by_tf": {},
                    },
                )
                i_stats["total_trades"] += 1
                i_stats["total_pnl"] += pos["pnl_abs"]

                tf_stats = i_stats["by_tf"].setdefault(
                    timeframe,
                    {
                        "trades": 0,
                        "pnl_abs": Decimal("0"),
                    },
                )
                tf_stats["trades"] += 1
                tf_stats["pnl_abs"] += pos["pnl_abs"]

    # считаем good/bad позиции
    positions_good = sum(1 for p in positions_map.values() if p["good_state"])
    positions_bad = positions_total - positions_good

    log.debug(
        "BT_ANALYSIS_POSTPROC: итоги фильтрации для scenario_id=%s, signal_id=%s — всего=%s, хорошие=%s, плохие=%s, bad_bins=%s, bad_hits=%s",
        scenario_id,
        signal_id,
        positions_total,
        positions_good,
        positions_bad,
        total_bad_bins,
        total_bad_hits,
    )

    # записываем контейнер в bt_analysis_positions_postproc (сначала чистим старые строки по паре)
    await _store_positions_postproc(pg, scenario_id, signal_id, positions_map)

    # загружаем исходную статистику сценария/сигнала по направлениям
    orig_stats = await _load_orig_scenario_stats(pg, scenario_id, signal_id)

    # загружаем депозит сценария
    deposit = await _load_scenario_deposit(pg, scenario_id)

    # пересчитываем агрегаты до/после по направлениям и пишем в bt_analysis_scenario_stat
    await _update_analysis_scenario_stats(
        pg=pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        positions_map=positions_map,
        removed_stats=removed_stats,
        orig_stats=orig_stats,
        deposit=deposit,
        threshold_map=threshold_map,
    )

    return {
        "positions_total": positions_total,
        "positions_good": positions_good,
        "positions_bad": positions_bad,
    }


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


# 🔸 Загрузка моделей bt_analysis_model_opt по направлениям
async def _load_model_opt_map(
    pg,
    scenario_id: int,
    signal_id: int,
    directions: List[str],
) -> Dict[str, Dict[str, Any]]:
    if not directions:
        return {}

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                id,
                direction,
                best_threshold,
                selected_analysis_ids
            FROM bt_analysis_model_opt
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND direction   = ANY($3::text[])
            """,
            scenario_id,
            signal_id,
            directions,
        )

    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        d = str(r["direction"]).strip().lower()
        selected = r["selected_analysis_ids"]
        if isinstance(selected, str):
            try:
                selected = json.loads(selected)
            except Exception:
                selected = []
        out[d] = {
            "model_id": int(r["id"]),
            "best_threshold": _safe_decimal(r["best_threshold"]),
            "selected_analysis_ids": selected if isinstance(selected, list) else [],
        }
    return out


# 🔸 Загрузка bad bins из bt_analysis_bins_labels (state='bad') + подтягивание family_key/key
async def _load_bad_bins_from_labels(
    pg,
    model_id: int,
    scenario_id: int,
    signal_id: int,
    direction: str,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                l.analysis_id,
                l.indicator_param,
                l.timeframe,
                l.bin_name,
                l.threshold_used,
                l.trades,
                l.pnl_abs,
                l.winrate,
                ai.family_key,
                ai."key" AS analysis_key
            FROM bt_analysis_bins_labels l
            JOIN bt_analysis_instances ai
              ON ai.id = l.analysis_id
            WHERE l.model_id    = $1
              AND l.scenario_id = $2
              AND l.signal_id   = $3
              AND l.direction   = $4
              AND l.state       = 'bad'
            """,
            model_id,
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
                "threshold_used": _safe_decimal(r["threshold_used"]),
                "trades": int(r["trades"]),
                "pnl_abs": _safe_decimal(r["pnl_abs"]),
                "winrate": _safe_decimal(r["winrate"]),
                "family_key": r["family_key"],
                "analysis_key": r["analysis_key"],
            }
        )

    return out


# 🔸 Индекс raw позиций по ключу (analysis_id, timeframe, bin_name) для конкретного направления и списка анализаторов
async def _load_positions_raw_index_for_analysis_ids(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
    analysis_ids: List[int],
) -> Dict[Tuple[int, str, str], List[Any]]:
    if not analysis_ids:
        return {}

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                analysis_id,
                timeframe,
                bin_name,
                position_uid
            FROM bt_analysis_positions_raw
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND direction   = $3
              AND analysis_id = ANY($4::int[])
            """,
            scenario_id,
            signal_id,
            direction,
            analysis_ids,
        )

    index: Dict[Tuple[int, str, str], List[Any]] = {}
    for r in rows:
        key = (int(r["analysis_id"]), str(r["timeframe"]), str(r["bin_name"]))
        index.setdefault(key, []).append(r["position_uid"])
    return index


# 🔸 Загрузка позиций сценария/сигнала (postproc=true) по направлениям
async def _load_positions_for_pair(
    pg,
    scenario_id: int,
    signal_id: int,
    directions: List[str],
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                position_uid,
                direction,
                pnl_abs
            FROM bt_scenario_positions
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND postproc    = true
              AND direction   = ANY($3::text[])
            ORDER BY entry_time
            """,
            scenario_id,
            signal_id,
            directions,
        )

    positions: List[Dict[str, Any]] = []
    for r in rows:
        positions.append(
            {
                "position_uid": r["position_uid"],
                "direction": r["direction"],
                "pnl_abs": _safe_decimal(r["pnl_abs"]),
            }
        )

    log.debug(
        "BT_ANALYSIS_POSTPROC: загружено позиций для postproc scenario_id=%s, signal_id=%s, directions=%s: %s",
        scenario_id,
        signal_id,
        directions,
        len(positions),
    )
    return positions


# 🔸 Запись контейнера позиций в bt_analysis_positions_postproc
async def _store_positions_postproc(
    pg,
    scenario_id: int,
    signal_id: int,
    positions_map: Dict[Any, Dict[str, Any]],
) -> None:
    async with pg.acquire() as conn:
        # сначала удаляем старые строки по паре
        await conn.execute(
            """
            DELETE FROM bt_analysis_positions_postproc
            WHERE scenario_id = $1
              AND signal_id   = $2
            """,
            scenario_id,
            signal_id,
        )

        # подготавливаем строки для вставки
        to_insert: List[Tuple[Any, ...]] = []
        for uid, info in positions_map.items():
            bad_reasons = info.get("bad_reasons") or []

            # формируем JSON для postproc_meta
            if bad_reasons:
                meta_obj = {"bad_reasons": bad_reasons}
                postproc_meta = json.dumps(meta_obj, ensure_ascii=False)
            else:
                postproc_meta = None

            to_insert.append(
                (
                    uid,
                    scenario_id,
                    signal_id,
                    info["pnl_abs"],
                    postproc_meta,
                    info["good_state"],
                )
            )

        if not to_insert:
            log.debug(
                "BT_ANALYSIS_POSTPROC: нет строк для записи в bt_analysis_positions_postproc "
                "для scenario_id=%s, signal_id=%s",
                scenario_id,
                signal_id,
            )
            return

        await conn.executemany(
            """
            INSERT INTO bt_analysis_positions_postproc (
                position_uid,
                scenario_id,
                signal_id,
                pnl_abs,
                postproc_meta,
                good_state
            )
            VALUES (
                $1, $2, $3, $4, $5, $6
            )
            """,
            to_insert,
        )

    log.debug(
        "BT_ANALYSIS_POSTPROC: записано строк в bt_analysis_positions_postproc для scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(to_insert),
    )


# 🔸 Загрузка исходной статистики сценария/сигнала по направлениям из bt_scenario_stat
async def _load_orig_scenario_stats(
    pg,
    scenario_id: int,
    signal_id: int,
) -> Dict[str, Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                direction,
                trades,
                pnl_abs,
                winrate,
                roi
            FROM bt_scenario_stat
            WHERE scenario_id = $1
              AND signal_id   = $2
            """,
            scenario_id,
            signal_id,
        )

    stats: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        direction = r["direction"]
        stats[direction] = {
            "trades": int(r["trades"]),
            "pnl_abs": _safe_decimal(r["pnl_abs"]),
            "winrate": _safe_decimal(r["winrate"]),
            "roi": _safe_decimal(r["roi"]),
        }

    return stats


# 🔸 Загрузка депозита сценария из bt_scenario_parameters (param_name='deposit')
async def _load_scenario_deposit(
    pg,
    scenario_id: int,
) -> Optional[Decimal]:
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT param_value
            FROM bt_scenario_parameters
            WHERE scenario_id = $1
              AND param_name  = 'deposit'
            LIMIT 1
            """,
            scenario_id,
        )

    if not row:
        log.debug(
            "BT_ANALYSIS_POSTPROC: депозит для scenario_id=%s не найден в bt_scenario_parameters",
            scenario_id,
        )
        return None

    dep = _safe_decimal(row["param_value"])
    if dep <= 0:
        return None

    return dep


# 🔸 Пересчёт агрегатов до/после и запись в bt_analysis_scenario_stat
async def _update_analysis_scenario_stats(
    pg,
    scenario_id: int,
    signal_id: int,
    positions_map: Dict[Any, Dict[str, Any]],
    removed_stats: Dict[str, Dict[str, Dict[str, Any]]],
    orig_stats: Dict[str, Dict[str, Any]],
    deposit: Optional[Decimal],
    threshold_map: Dict[str, Decimal],
) -> None:
    # группируем позиции по направлению и good_state
    per_dir_good: Dict[str, List[Dict[str, Any]]] = {}
    per_dir_all: Dict[str, List[Dict[str, Any]]] = {}

    for info in positions_map.values():
        direction = info["direction"]
        per_dir_all.setdefault(direction, []).append(info)
        if info["good_state"]:
            per_dir_good.setdefault(direction, []).append(info)

    async with pg.acquire() as conn:
        for direction in sorted(per_dir_all.keys()):
            orig = orig_stats.get(direction) or {
                "trades": 0,
                "pnl_abs": Decimal("0"),
                "winrate": Decimal("0"),
                "roi": Decimal("0"),
            }

            good_positions = per_dir_good.get(direction, [])
            filt_trades = len(good_positions)
            filt_pnl_abs = sum((p["pnl_abs"] for p in good_positions), Decimal("0"))

            # winrate после фильтрации
            if filt_trades > 0:
                wins = sum(1 for p in good_positions if p["pnl_abs"] > 0)
                filt_winrate = Decimal(wins) / Decimal(filt_trades)
            else:
                filt_winrate = Decimal("0")

            # ROI после фильтрации, если есть депозит
            if deposit and deposit > 0:
                try:
                    filt_roi = filt_pnl_abs / deposit
                except (InvalidOperation, ZeroDivisionError):
                    filt_roi = Decimal("0")
            else:
                filt_roi = Decimal("0")

            # аккуратность среди удалённых сделок
            removed_positions = [p for p in per_dir_all.get(direction, []) if not p["good_state"]]
            removed_trades = len(removed_positions)
            if removed_trades > 0:
                removed_losers = sum(1 for p in removed_positions if p["pnl_abs"] <= 0)
                removed_accuracy = Decimal(removed_losers) / Decimal(removed_trades)
            else:
                removed_accuracy = Decimal("0")

            # raw_stat по отбраковкам
            dir_removed = removed_stats.get(direction) or {}
            threshold = threshold_map.get(direction) or DEFAULT_MIN_WINRATE_THRESHOLD

            raw_stat_obj = _build_raw_stat_json_for_direction(
                dir_removed=dir_removed,
                threshold=threshold,
            )

            raw_stat_json = json.dumps(raw_stat_obj, ensure_ascii=False) if raw_stat_obj is not None else None

            await conn.execute(
                """
                INSERT INTO bt_analysis_scenario_stat (
                    scenario_id,
                    signal_id,
                    direction,
                    orig_trades,
                    orig_pnl_abs,
                    orig_winrate,
                    orig_roi,
                    filt_trades,
                    filt_pnl_abs,
                    filt_winrate,
                    filt_roi,
                    removed_accuracy,
                    raw_stat
                )
                VALUES (
                    $1, $2, $3,
                    $4, $5, $6, $7,
                    $8, $9, $10, $11,
                    $12, $13
                )
                ON CONFLICT (scenario_id, signal_id, direction)
                DO UPDATE SET
                    orig_trades      = EXCLUDED.orig_trades,
                    orig_pnl_abs     = EXCLUDED.orig_pnl_abs,
                    orig_winrate     = EXCLUDED.orig_winrate,
                    orig_roi         = EXCLUDED.orig_roi,
                    filt_trades      = EXCLUDED.filt_trades,
                    filt_pnl_abs     = EXCLUDED.filt_pnl_abs,
                    filt_winrate     = EXCLUDED.filt_winrate,
                    filt_roi         = EXCLUDED.filt_roi,
                    removed_accuracy = EXCLUDED.removed_accuracy,
                    raw_stat         = EXCLUDED.raw_stat,
                    updated_at       = now()
                """,
                scenario_id,
                signal_id,
                direction,
                orig["trades"],
                orig["pnl_abs"],
                orig["winrate"],
                orig["roi"],
                filt_trades,
                filt_pnl_abs,
                filt_winrate,
                filt_roi,
                removed_accuracy,
                raw_stat_json,
            )

            log.debug(
                "BT_ANALYSIS_POSTPROC: обновлена bt_analysis_scenario_stat для scenario_id=%s, signal_id=%s, direction=%s — orig_trades=%s, filt_trades=%s, orig_pnl=%s, filt_pnl=%s, removed_trades=%s, removed_accuracy=%.4f, threshold=%s",
                scenario_id,
                signal_id,
                direction,
                orig["trades"],
                filt_trades,
                orig["pnl_abs"],
                filt_pnl_abs,
                removed_trades,
                float(removed_accuracy),
                threshold,
            )


# 🔸 Формирование raw_stat JSON для одного направления
def _build_raw_stat_json_for_direction(
    dir_removed: Dict[str, Dict[str, Any]],
    threshold: Decimal,
) -> Optional[Dict[str, Any]]:
    if not dir_removed:
        return None

    total_trades = 0
    total_pnl = Decimal("0")

    by_indicator: Dict[str, Any] = {}

    for indicator_key, istats in dir_removed.items():
        it_total_trades = int(istats.get("total_trades", 0))
        it_total_pnl = istats.get("total_pnl", Decimal("0"))
        total_trades += it_total_trades
        total_pnl += it_total_pnl

        by_tf_obj: Dict[str, Any] = {}
        by_tf = istats.get("by_tf") or {}
        for tf, tf_stats in by_tf.items():
            tf_trades = int(tf_stats.get("trades", 0))
            tf_pnl = tf_stats.get("pnl_abs", Decimal("0"))
            by_tf_obj[str(tf)] = {
                "trades": tf_trades,
                "pnl_abs": _decimal_to_json_number(tf_pnl),
            }

        by_indicator[indicator_key] = {
            "total": {
                "trades": it_total_trades,
                "pnl_abs": _decimal_to_json_number(it_total_pnl),
            },
            "by_tf": by_tf_obj,
        }

    raw_obj = {
        "version": 1,
        "min_winrate": float(threshold),
        "removed": {
            "total": {
                "trades": int(total_trades),
                "pnl_abs": _decimal_to_json_number(total_pnl),
            },
            "by_indicator": by_indicator,
        },
    }
    return raw_obj


# 🔸 Публикация события готовности финального постпроцессинга в bt:analysis:postproc_ready
async def _publish_postproc_ready(
    redis,
    scenario_id: int,
    signal_id: int,
    result: Dict[str, Any],
    model_map: Dict[str, Dict[str, Any]],
    source_finished_at: datetime,
) -> None:
    finished_at = datetime.utcnow()

    positions_total = result.get("positions_total", 0)
    positions_good = result.get("positions_good", 0)
    positions_bad = result.get("positions_bad", 0)

    models_json = json.dumps(
        {
            d: {
                "model_id": int(m.get("model_id")),
                "threshold": str(m.get("best_threshold")),
            }
            for d, m in model_map.items()
        },
        ensure_ascii=False,
    )

    try:
        await redis.xadd(
            POSTPROC_STREAM_KEY,
            {
                "scenario_id": str(scenario_id),
                "signal_id": str(signal_id),
                "positions_total": str(positions_total),
                "positions_good": str(positions_good),
                "positions_bad": str(positions_bad),
                "models": models_json,
                "source_finished_at": source_finished_at.isoformat(),
                "finished_at": finished_at.isoformat(),
            },
        )
        log.debug(
            "BT_ANALYSIS_POSTPROC: опубликовано событие финального постпроцессинга в стрим '%s' "
            "для scenario_id=%s, signal_id=%s, positions_total=%s, positions_good=%s, positions_bad=%s, finished_at=%s",
            POSTPROC_STREAM_KEY,
            scenario_id,
            signal_id,
            positions_total,
            positions_good,
            positions_bad,
            finished_at,
        )
    except Exception as e:
        log.error(
            "BT_ANALYSIS_POSTPROC: не удалось опубликовать событие в стрим '%s' "
            "для scenario_id=%s, signal_id=%s: %s",
            POSTPROC_STREAM_KEY,
            scenario_id,
            signal_id,
            e,
            exc_info=True,
        )


# 🔸 Вспомогательная функция: безопасное приведение к Decimal
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


# 🔸 Вспомогательная функция: Decimal -> JSON-совместимое число
def _decimal_to_json_number(value: Decimal) -> float:
    try:
        return float(value)
    except (TypeError, InvalidOperation, ValueError):
        return 0.0