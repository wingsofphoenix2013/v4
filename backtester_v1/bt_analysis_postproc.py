# bt_analysis_postproc.py — финальная пост-обработка результатов анализаторов по сценариям/сигналам

import asyncio
import json
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Константы стримов и настроек постпроцессинга
ANALYSIS_READY_STREAM_KEY = "bt:analysis:ready"
POSTPROC_STREAM_KEY = "bt:analysis:postproc_ready"

POSTPROC_CONSUMER_GROUP = "bt_analysis_postproc"
POSTPROC_CONSUMER_NAME = "bt_analysis_postproc_main"

POSTPROC_STREAM_BATCH_SIZE = 10
POSTPROC_STREAM_BLOCK_MS = 5000

POSTPROC_MAX_CONCURRENCY = 8

# минимальный winrate для биннов; всё, что ниже — считается "плохим бинном"
MIN_WINRATE_THRESHOLD = Decimal("0.5")

# кеш последних finished_at по (scenario_id, signal_id) для отсечки дублей
_last_analysis_finished_at: Dict[Tuple[int, int], datetime] = {}

log = logging.getLogger("BT_ANALYSIS_POSTPROC")


# 🔸 Публичная точка входа: оркестратор постпроцессинга анализов
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
                if stream_key != ANALYSIS_READY_STREAM_KEY:
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
                log.info(
                    "BT_ANALYSIS_POSTPROC: обработан пакет сообщений из bt:analysis:ready — "
                    "сообщений=%s, ошибок=%s",
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


# 🔸 Проверка/создание consumer group для стрима bt:analysis:ready
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=ANALYSIS_READY_STREAM_KEY,
            groupname=POSTPROC_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_ANALYSIS_POSTPROC: создана consumer group '%s' для стрима '%s'",
            POSTPROC_CONSUMER_GROUP,
            ANALYSIS_READY_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_ANALYSIS_POSTPROC: consumer group '%s' для стрима '%s' уже существует",
                POSTPROC_CONSUMER_GROUP,
                ANALYSIS_READY_STREAM_KEY,
            )
        else:
            log.error(
                "BT_ANALYSIS_POSTPROC: ошибка при создании consumer group '%s': %s",
                POSTPROC_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:analysis:ready
async def _read_from_stream(redis) -> List[Any]:
    entries = await redis.xreadgroup(
        groupname=POSTPROC_CONSUMER_GROUP,
        consumername=POSTPROC_CONSUMER_NAME,
        streams={ANALYSIS_READY_STREAM_KEY: ">"},
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
        finished_at = datetime.fromisoformat(finished_at_str)

        analyses_total_str = fields.get("analyses_total") or "0"
        rows_raw_str = fields.get("rows_raw") or "0"
        rows_bins_str = fields.get("rows_bins") or "0"

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "finished_at": finished_at,
            "analyses_total": int(analyses_total_str),
            "rows_raw": int(rows_raw_str),
            "rows_bins": int(rows_bins_str),
        }
    except Exception as e:
        log.error(
            "BT_ANALYSIS_POSTPROC: ошибка разбора сообщения стрима bt:analysis:ready: %s, fields=%s",
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
            # невалидное сообщение — помечаем как обработанное и выходим
            await redis.xack(ANALYSIS_READY_STREAM_KEY, POSTPROC_CONSUMER_GROUP, entry_id)
            return

        scenario_id = ctx["scenario_id"]
        signal_id = ctx["signal_id"]
        finished_at = ctx["finished_at"]

        pair_key = (scenario_id, signal_id)
        last_finished = _last_analysis_finished_at.get(pair_key)

        # отсечка дублей по равному finished_at
        if last_finished is not None and last_finished == finished_at:
            log.debug(
                "BT_ANALYSIS_POSTPROC: дубликат сообщения для scenario_id=%s, signal_id=%s, "
                "finished_at=%s, stream_id=%s — постпроцессинг не выполняется",
                scenario_id,
                signal_id,
                finished_at,
                entry_id,
            )
            await redis.xack(ANALYSIS_READY_STREAM_KEY, POSTPROC_CONSUMER_GROUP, entry_id)
            return

        _last_analysis_finished_at[pair_key] = finished_at

        log.debug(
            "BT_ANALYSIS_POSTPROC: получено сообщение анализа "
            "scenario_id=%s, signal_id=%s, finished_at=%s, stream_id=%s",
            scenario_id,
            signal_id,
            finished_at,
            entry_id,
        )

        try:
            result = await _process_pair_postproc(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
            )

            # публикуем событие готовности финального постпроцессинга
            await _publish_postproc_ready(
                redis=redis,
                scenario_id=scenario_id,
                signal_id=signal_id,
                result=result,
            )

            log.info(
                "BT_ANALYSIS_POSTPROC: постпроцессинг завершён для scenario_id=%s, signal_id=%s — "
                "позиции_всего=%s, хорошие=%s, плохие=%s",
                scenario_id,
                signal_id,
                result.get("positions_total", 0),
                result.get("positions_good", 0),
                result.get("positions_bad", 0),
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
            await redis.xack(ANALYSIS_READY_STREAM_KEY, POSTPROC_CONSUMER_GROUP, entry_id)


# 🔸 Основной постпроцессинг для одной пары (scenario_id, signal_id)
async def _process_pair_postproc(
    pg,
    scenario_id: int,
    signal_id: int,
) -> Dict[str, Any]:
    log.debug(
        "BT_ANALYSIS_POSTPROC: старт постпроцессинга для scenario_id=%s, signal_id=%s",
        scenario_id,
        signal_id,
    )

    # загружаем все позиции сценария/сигнала (postproc = true)
    positions = await _load_positions_for_pair(pg, scenario_id, signal_id)
    if not positions:
        log.info(
            "BT_ANALYSIS_POSTPROC: нет позиций для постпроцессинга scenario_id=%s, signal_id=%s",
            scenario_id,
            signal_id,
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

        # чистить bt_analysis_scenario_stat в этом кейсе не будем — оставим как есть
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

    # загружаем все "плохие бины" из bt_analysis_bins_stat
    bad_bins = await _load_bad_bins_for_pair(pg, scenario_id, signal_id)
    if not bad_bins:
        log.debug(
            "BT_ANALYSIS_POSTPROC: нет плохих биннов для scenario_id=%s, signal_id=%s, "
            "все позиции останутся good_state=true",
            scenario_id,
            signal_id,
        )
    else:
        # загружаем все строки raw для пары (для быстрого поиска позиций по биннам)
        raw_index = await _load_positions_raw_index(pg, scenario_id, signal_id)

        # агрегат по отбраковкам: direction -> indicator_param -> {total_trades, total_pnl, by_tf{tf->{trades,pnl}}}
        removed_stats: Dict[str, Dict[str, Dict[str, Any]]] = {}
        # чтобы не удваивать вклад одной и той же позиции в рамках (dir, indicator_param, tf)
        removed_seen: set = set()

        # применяем отбраковку по каждому плохому бину
        for b in bad_bins:
            analysis_id = b["analysis_id"]
            indicator_param = b["indicator_param"]  # может быть None
            timeframe = b["timeframe"]
            direction = b["direction"]
            bin_name = b["bin_name"]

            key = (analysis_id, timeframe, direction, bin_name)
            pos_uids = raw_index.get(key, [])

            if not pos_uids:
                continue

            # комментарий: обновляем good_state и накапливаем причины/статистику отбраковки
            indicator_key = indicator_param if indicator_param is not None else "_none_"

            for uid in pos_uids:
                pos = positions_map.get(uid)
                if pos is None:
                    continue

                # добавляем причину в bad_reasons
                pos["bad_reasons"].append(
                    {
                        "analysis_id": analysis_id,
                        "family_key": b["family_key"],
                        "key": b["analysis_key"],
                        "indicator_param": indicator_param,
                        "timeframe": timeframe,
                        "direction": direction,
                        "bin_name": bin_name,
                    }
                )
                pos["good_state"] = False

                # ключ для отсечения дублей в статистике по отбраковкам
                seen_key = (direction, indicator_key, timeframe, uid)
                if seen_key in removed_seen:
                    continue
                removed_seen.add(seen_key)

                # накапливаем статистику по отбраковкам
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
        "BT_ANALYSIS_POSTPROC: итоги фильтрации по позициям для scenario_id=%s, signal_id=%s — "
        "всего=%s, хорошие=%s, плохие=%s",
        scenario_id,
        signal_id,
        positions_total,
        positions_good,
        positions_bad,
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
        removed_stats=removed_stats if bad_bins else {},
        orig_stats=orig_stats,
        deposit=deposit,
    )

    return {
        "positions_total": positions_total,
        "positions_good": positions_good,
        "positions_bad": positions_bad,
    }


# 🔸 Загрузка позиций сценария/сигнала (postproc=true)
async def _load_positions_for_pair(
    pg,
    scenario_id: int,
    signal_id: int,
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
            ORDER BY entry_time
            """,
            scenario_id,
            signal_id,
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
        "BT_ANALYSIS_POSTPROC: загружено позиций для postproc scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(positions),
    )
    return positions


# 🔸 Загрузка "плохих биннов" из bt_analysis_bins_stat
async def _load_bad_bins_for_pair(
    pg,
    scenario_id: int,
    signal_id: int,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                analysis_id,
                indicator_param,
                timeframe,
                direction,
                bin_name,
                pnl_abs,
                winrate
            FROM bt_analysis_bins_stat
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND winrate     < $3
            """,
            scenario_id,
            signal_id,
            MIN_WINRATE_THRESHOLD,
        )

    bad_bins: List[Dict[str, Any]] = []
    if not rows:
        log.debug(
            "BT_ANALYSIS_POSTPROC: для scenario_id=%s, signal_id=%s нет биннов с winrate < %s",
            scenario_id,
            signal_id,
            MIN_WINRATE_THRESHOLD,
        )
        return bad_bins

    # для восстановления family_key/key нам понадобится join по analysis_instances
    # комментарий: подтягиваем family_key/key для всех analysis_id из списка
    analysis_ids = sorted({r["analysis_id"] for r in rows})
    family_map = await _load_analysis_family_keys(pg, analysis_ids)

    for r in rows:
        aid = r["analysis_id"]
        fm = family_map.get(aid) or {}
        bad_bins.append(
            {
                "analysis_id": aid,
                "indicator_param": r["indicator_param"],
                "timeframe": r["timeframe"],
                "direction": r["direction"],
                "bin_name": r["bin_name"],
                "pnl_abs": _safe_decimal(r["pnl_abs"]),
                "winrate": _safe_decimal(r["winrate"]),
                "family_key": fm.get("family_key"),
                "analysis_key": fm.get("key"),
            }
        )

    log.debug(
        "BT_ANALYSIS_POSTPROC: для scenario_id=%s, signal_id=%s найдено плохих биннов: %s",
        scenario_id,
        signal_id,
        len(bad_bins),
    )
    return bad_bins


# 🔸 Загрузка family_key / key для множества analysis_id
async def _load_analysis_family_keys(
    pg,
    analysis_ids: List[int],
) -> Dict[int, Dict[str, Any]]:
    if not analysis_ids:
        return {}

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, family_key, "key"
            FROM bt_analysis_instances
            WHERE id = ANY($1::int[])
            """,
            analysis_ids,
        )

    result: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        result[r["id"]] = {
            "family_key": r["family_key"],
            "key": r["key"],
        }
    return result


# 🔸 Построение индекса bt_analysis_positions_raw по (analysis_id, timeframe, direction, bin_name)
async def _load_positions_raw_index(
    pg,
    scenario_id: int,
    signal_id: int,
) -> Dict[Tuple[int, str, str, str], List[Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                analysis_id,
                timeframe,
                direction,
                bin_name,
                position_uid
            FROM bt_analysis_positions_raw
            WHERE scenario_id = $1
              AND signal_id   = $2
            """,
            scenario_id,
            signal_id,
        )

    index: Dict[Tuple[int, str, str, str], List[Any]] = {}
    for r in rows:
        key = (r["analysis_id"], r["timeframe"], r["direction"], r["bin_name"])
        index.setdefault(key, []).append(r["position_uid"])

    log.debug(
        "BT_ANALYSIS_POSTPROC: построен индекс raw позиций для scenario_id=%s, signal_id=%s — "
        "ключей=%s",
        scenario_id,
        signal_id,
        len(index),
    )
    return index


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
                # сериализуем в строку, чтобы PG спокойно принял в jsonb
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

    log.info(
        "BT_ANALYSIS_POSTPROC: записано строк в bt_analysis_positions_postproc "
        "для scenario_id=%s, signal_id=%s: %s",
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

    value = row["param_value"]
    dep = _safe_decimal(value)
    if dep <= 0:
        log.debug(
            "BT_ANALYSIS_POSTPROC: депозит для scenario_id=%s некорректен или неположителен: %s",
            scenario_id,
            dep,
        )
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

            # комментарий: считаем "аккуратность" среди удалённых сделок
            removed_positions = [
                p for p in per_dir_all.get(direction, [])
                if not p["good_state"]
            ]
            removed_trades = len(removed_positions)
            if removed_trades > 0:
                removed_losers = sum(
                    1 for p in removed_positions
                    if p["pnl_abs"] <= 0
                )
                removed_accuracy = (
                    Decimal(removed_losers) / Decimal(removed_trades)
                )
            else:
                removed_accuracy = Decimal("0")

            # формируем raw_stat по отбраковкам для данного направления
            dir_removed = removed_stats.get(direction) or {}
            raw_stat_obj = _build_raw_stat_json_for_direction(
                dir_removed=dir_removed,
                threshold=MIN_WINRATE_THRESHOLD,
            )

            # сериализуем raw_stat в JSON-строку для jsonb; если отбраковок нет — NULL
            if raw_stat_obj is not None:
                raw_stat_json = json.dumps(raw_stat_obj, ensure_ascii=False)
            else:
                raw_stat_json = None

            # upsert в bt_analysis_scenario_stat
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

            log.info(
                "BT_ANALYSIS_POSTPROC: обновлена bt_analysis_scenario_stat для scenario_id=%s, "
                "signal_id=%s, direction=%s — orig_trades=%s, filt_trades=%s, "
                "orig_pnl=%s, filt_pnl=%s, removed_trades=%s, removed_accuracy=%.4f",
                scenario_id,
                signal_id,
                direction,
                orig["trades"],
                filt_trades,
                orig["pnl_abs"],
                filt_pnl_abs,
                removed_trades,
                float(removed_accuracy),
            )
# 🔸 Формирование raw_stat JSON для одного направления
def _build_raw_stat_json_for_direction(
    dir_removed: Dict[str, Dict[str, Any]],
    threshold: Decimal,
) -> Optional[Dict[str, Any]]:
    # если по этому направлению ничего не отбраковали — можно вернуть None
    if not dir_removed:
        return None

    # считаем общий итог по отбраковкам
    total_trades = 0
    total_pnl = Decimal("0")

    by_indicator: Dict[str, Any] = {}

    for indicator_key, istats in dir_removed.items():
        it_total_trades = int(istats.get("total_trades", 0))
        it_total_pnl = istats.get("total_pnl", Decimal("0"))
        total_trades += it_total_trades
        total_pnl += it_total_pnl

        # комментарий: разрез по TF
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
) -> None:
    finished_at = datetime.utcnow()

    positions_total = result.get("positions_total", 0)
    positions_good = result.get("positions_good", 0)
    positions_bad = result.get("positions_bad", 0)

    try:
        await redis.xadd(
            POSTPROC_STREAM_KEY,
            {
                "scenario_id": str(scenario_id),
                "signal_id": str(signal_id),
                "positions_total": str(positions_total),
                "positions_good": str(positions_good),
                "positions_bad": str(positions_bad),
                "min_winrate": str(MIN_WINRATE_THRESHOLD),
                "finished_at": finished_at.isoformat(),
            },
        )
        log.debug(
            "BT_ANALYSIS_POSTPROC: опубликовано событие финального постпроцессинга в стрим '%s' "
            "для scenario_id=%s, signal_id=%s, positions_total=%s, positions_good=%s, "
            "positions_bad=%s, finished_at=%s",
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