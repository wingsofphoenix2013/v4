# bt_analysis_postproc.py — финальная пост-обработка v1 (run-aware; без очисток; good_state=(no bad) AND (has good) строго по окну run)

import asyncio
import json
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Константы стримов и настроек постпроцессинга v1
PREPROC_READY_STREAM_KEY = "bt:analysis:preproc_ready"
POSTPROC_READY_STREAM_KEY = "bt:analysis:postproc_ready"

POSTPROC_CONSUMER_GROUP = "bt_analysis_postproc"
POSTPROC_CONSUMER_NAME = "bt_analysis_postproc_main"

POSTPROC_STREAM_BATCH_SIZE = 10
POSTPROC_STREAM_BLOCK_MS = 5000

POSTPROC_MAX_CONCURRENCY = 16

# 🔸 Кеш последних source_finished_at по (scenario_id, signal_id, run_id) для отсечки дублей
_last_preproc_source_finished_at: Dict[Tuple[int, int, int], datetime] = {}

log = logging.getLogger("BT_ANALYSIS_POSTPROC")


# 🔸 Публичная точка входа: оркестратор финального постпроцессинга v1
async def run_bt_analysis_postproc_orchestrator(pg, redis):
    log.debug("BT_ANALYSIS_POSTPROC: оркестратор запущен")

    await _ensure_consumer_group(redis)

    # общий семафор
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
                        name=f"BT_ANALYSIS_POSTPROC_{entry_id}",
                    )
                    tasks.append(task)

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                errors = sum(1 for r in results if isinstance(r, Exception))
                log.debug(
                    "BT_ANALYSIS_POSTPROC: обработан пакет сообщений — сообщений=%s, ошибок=%s",
                    total_msgs,
                    errors,
                )

        except Exception as e:
            log.error("BT_ANALYSIS_POSTPROC: ошибка в основном цикле: %s", e, exc_info=True)
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


# 🔸 Разбор одного сообщения preproc_ready (run-aware)
def _parse_preproc_ready_message(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id_str = fields.get("scenario_id")
        signal_id_str = fields.get("signal_id")
        run_id_str = fields.get("run_id")

        finished_at_str = fields.get("finished_at")
        source_finished_at_str = fields.get("source_finished_at")

        window_from_str = fields.get("window_from")
        window_to_str = fields.get("window_to")

        if not (scenario_id_str and signal_id_str and run_id_str and finished_at_str and window_from_str and window_to_str):
            return None

        scenario_id = int(scenario_id_str)
        signal_id = int(signal_id_str)
        run_id = int(run_id_str)

        finished_at = datetime.fromisoformat(finished_at_str)

        source_finished_at = None
        if source_finished_at_str:
            try:
                source_finished_at = datetime.fromisoformat(source_finished_at_str)
            except Exception:
                source_finished_at = None

        window_from = datetime.fromisoformat(window_from_str)
        window_to = datetime.fromisoformat(window_to_str)

        direction_mask = (fields.get("direction_mask") or "").strip().lower() or None

        models_raw = fields.get("models")
        models = {}
        if models_raw:
            try:
                models = json.loads(models_raw)
            except Exception:
                models = {}

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "run_id": run_id,
            "finished_at": finished_at,
            "source_finished_at": source_finished_at,
            "window_from": window_from,
            "window_to": window_to,
            "direction_mask": direction_mask,
            "models": models if isinstance(models, dict) else {},
        }
    except Exception as e:
        log.error("BT_ANALYSIS_POSTPROC: ошибка разбора сообщения: %s, fields=%s", e, fields, exc_info=True)
        return None


# 🔸 Обработка одного сообщения
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
            await redis.xack(PREPROC_READY_STREAM_KEY, POSTPROC_CONSUMER_GROUP, entry_id)
            return

        scenario_id = ctx["scenario_id"]
        signal_id = ctx["signal_id"]
        run_id = ctx["run_id"]

        finished_at = ctx["finished_at"]
        source_finished_at = ctx["source_finished_at"]

        window_from = ctx["window_from"]
        window_to = ctx["window_to"]

        direction_mask_from_msg = ctx.get("direction_mask")
        models_from_msg = ctx.get("models") or {}

        dedup_ts = source_finished_at or finished_at
        pair_key = (scenario_id, signal_id, run_id)

        last_finished = _last_preproc_source_finished_at.get(pair_key)
        if last_finished is not None and last_finished == dedup_ts:
            log.debug(
                "BT_ANALYSIS_POSTPROC: дубликат scenario_id=%s signal_id=%s run_id=%s dedup_ts=%s — пропуск",
                scenario_id,
                signal_id,
                run_id,
                dedup_ts,
            )
            await redis.xack(PREPROC_READY_STREAM_KEY, POSTPROC_CONSUMER_GROUP, entry_id)
            return

        _last_preproc_source_finished_at[pair_key] = dedup_ts
        started_at = datetime.utcnow()

        try:
            # направления сигнала
            direction_mask = direction_mask_from_msg or await _load_signal_direction_mask(pg, signal_id)
            directions = _directions_from_mask(direction_mask)

            # загрузка model_id по направлениям (строго для run)
            model_map = await _load_model_map_for_run(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                run_id=run_id,
                directions=directions,
                models_hint=models_from_msg,
            )
            if not model_map:
                log.warning(
                    "BT_ANALYSIS_POSTPROC: model_opt не найден для scenario_id=%s signal_id=%s run_id=%s — пропуск",
                    scenario_id,
                    signal_id,
                    run_id,
                )
                return

            # основной постпроцессинг
            result = await _process_pair_postproc(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                run_id=run_id,
                window_from=window_from,
                window_to=window_to,
                directions=directions,
                model_map=model_map,
            )

            # публикуем событие готовности (run-aware)
            await _publish_postproc_ready(
                redis=redis,
                scenario_id=scenario_id,
                signal_id=signal_id,
                run_id=run_id,
                window_from=window_from,
                window_to=window_to,
                result=result,
                model_map=model_map,
                source_finished_at=dedup_ts,
            )

            elapsed_ms = int((datetime.utcnow() - started_at).total_seconds() * 1000)

            log.info(
                "BT_ANALYSIS_POSTPROC: done scenario_id=%s signal_id=%s run_id=%s window=[%s..%s] — total=%s good=%s bad=%s no_good=%s models=%s elapsed_ms=%s",
                scenario_id,
                signal_id,
                run_id,
                window_from,
                window_to,
                result.get("positions_total", 0),
                result.get("positions_good", 0),
                result.get("positions_bad", 0),
                result.get("positions_no_good", 0),
                {d: {"model_id": model_map[d]["model_id"]} for d in model_map},
                elapsed_ms,
            )

        except Exception as e:
            log.error(
                "BT_ANALYSIS_POSTPROC: ошибка postproc scenario_id=%s signal_id=%s run_id=%s: %s",
                scenario_id,
                signal_id,
                run_id,
                e,
                exc_info=True,
            )
        finally:
            await redis.xack(PREPROC_READY_STREAM_KEY, POSTPROC_CONSUMER_GROUP, entry_id)


# 🔸 Основной постпроцессинг v1 (bad => reject; else require good-hit) строго по окну run
async def _process_pair_postproc(
    pg,
    scenario_id: int,
    signal_id: int,
    run_id: int,
    window_from: datetime,
    window_to: datetime,
    directions: List[str],
    model_map: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    # загружаем позиции (postproc=true, status=closed) по направлениям строго в окне run
    positions = await _load_positions_for_pair(pg, scenario_id, signal_id, directions, window_from, window_to)
    if not positions:
        return {"positions_total": 0, "positions_good": 0, "positions_bad": 0, "positions_no_good": 0}

    # базовая структура по позициям
    positions_map: Dict[Any, Dict[str, Any]] = {}
    for p in positions:
        positions_map[p["position_uid"]] = {
            "pnl_abs": p["pnl_abs"],
            "direction": p["direction"],
            "has_bad": False,
            "has_good": False,
            "bad_reasons": [],
            "good_reasons": [],
        }

    removed_stats_bad: Dict[str, Dict[str, Dict[str, Any]]] = {}
    removed_stats_good_missing: Dict[str, Dict[str, Any]] = {}

    total_bad_hits = 0
    total_good_hits = 0

    for direction in directions:
        model = model_map.get(direction)
        if not model:
            continue
        model_id = int(model["model_id"])

        # грузим bad и good бины из labels (по model_id, run-aware)
        bad_bins = await _load_bins_from_labels(
            pg=pg,
            model_id=model_id,
            run_id=run_id,
            scenario_id=scenario_id,
            signal_id=signal_id,
            direction=direction,
            state="bad",
        )
        good_bins = await _load_bins_from_labels(
            pg=pg,
            model_id=model_id,
            run_id=run_id,
            scenario_id=scenario_id,
            signal_id=signal_id,
            direction=direction,
            state="good",
        )

        # ускорение: грузим raw_index для всех analysis_id, которые встречаются в bad/good (строго по run_id)
        all_analysis_ids = sorted({b["analysis_id"] for b in bad_bins} | {b["analysis_id"] for b in good_bins})
        raw_index = await _load_positions_raw_index_for_analysis_ids(
            pg=pg,
            run_id=run_id,
            scenario_id=scenario_id,
            signal_id=signal_id,
            direction=direction,
            analysis_ids=all_analysis_ids,
        )

        # применяем bad
        for b in bad_bins:
            key = (b["analysis_id"], b["timeframe"], b["bin_name"])
            pos_uids = raw_index.get(key, [])
            if not pos_uids:
                continue

            indicator_key = b["indicator_param"] if b["indicator_param"] is not None else "_none_"
            for uid in pos_uids:
                pos = positions_map.get(uid)
                if pos is None:
                    continue
                pos["has_bad"] = True
                total_bad_hits += 1
                pos["bad_reasons"].append(
                    {
                        "analysis_id": b["analysis_id"],
                        "family_key": b["family_key"],
                        "key": b["analysis_key"],
                        "indicator_param": b["indicator_param"],
                        "timeframe": b["timeframe"],
                        "bin_name": b["bin_name"],
                        "winrate": float(b["winrate"]),
                        "trades": int(b["trades"]),
                        "pnl_abs": float(b["pnl_abs"]),
                    }
                )

                d_stats = removed_stats_bad.setdefault(direction, {})
                i_stats = d_stats.setdefault(
                    indicator_key,
                    {"total_trades": 0, "total_pnl": Decimal("0"), "by_tf": {}},
                )
                i_stats["total_trades"] += 1
                i_stats["total_pnl"] += pos["pnl_abs"]
                tf_stats = i_stats["by_tf"].setdefault(b["timeframe"], {"trades": 0, "pnl_abs": Decimal("0")})
                tf_stats["trades"] += 1
                tf_stats["pnl_abs"] += pos["pnl_abs"]

        # применяем good
        for b in good_bins:
            key = (b["analysis_id"], b["timeframe"], b["bin_name"])
            pos_uids = raw_index.get(key, [])
            if not pos_uids:
                continue

            for uid in pos_uids:
                pos = positions_map.get(uid)
                if pos is None:
                    continue
                pos["has_good"] = True
                total_good_hits += 1
                if len(pos["good_reasons"]) < 5:
                    pos["good_reasons"].append(
                        {
                            "analysis_id": b["analysis_id"],
                            "family_key": b["family_key"],
                            "key": b["analysis_key"],
                            "indicator_param": b["indicator_param"],
                            "timeframe": b["timeframe"],
                            "bin_name": b["bin_name"],
                            "winrate": float(b["winrate"]),
                            "trades": int(b["trades"]),
                            "pnl_abs": float(b["pnl_abs"]),
                        }
                    )

    # финальное бинарное решение:
    # good_state = (no bad hits) AND (has good hit)
    positions_total = len(positions_map)
    positions_good = 0
    positions_bad = 0
    positions_no_good = 0

    for uid, info in positions_map.items():
        if info["has_bad"]:
            info["good_state"] = False
            info["reject_reason"] = "bad_hit"
            positions_bad += 1
        else:
            if info["has_good"]:
                info["good_state"] = True
                info["reject_reason"] = None
                positions_good += 1
            else:
                info["good_state"] = False
                info["reject_reason"] = "no_good_hit"
                positions_no_good += 1

                d = str(info["direction"]).strip().lower()
                st = removed_stats_good_missing.setdefault(d, {"trades": 0, "pnl_abs": Decimal("0")})
                st["trades"] += 1
                st["pnl_abs"] += info["pnl_abs"]

    # пишем контейнер позиций (run-aware, идемпотентно)
    await _store_positions_postproc(pg, run_id, scenario_id, signal_id, positions_map)

    # orig stats берём из bt_scenario_stat(run) по run_id
    orig_stats = await _load_orig_scenario_stats(pg, scenario_id, signal_id, run_id)

    # депозит для ROI
    deposit = await _load_scenario_deposit(pg, scenario_id)

    good_thr_map: Dict[str, Any] = {}
    for d in directions:
        m = model_map.get(d) or {}
        good_thr_map[d] = m.get("good_threshold_selected")

    # обновляем bt_analysis_scenario_stat (run-aware)
    await _upsert_analysis_scenario_stats(
        pg=pg,
        run_id=run_id,
        scenario_id=scenario_id,
        signal_id=signal_id,
        window_from=window_from,
        window_to=window_to,
        positions_map=positions_map,
        removed_stats_bad=removed_stats_bad,
        removed_stats_no_good=removed_stats_good_missing,
        orig_stats=orig_stats,
        deposit=deposit,
        good_threshold_map=good_thr_map,
    )

    return {
        "positions_total": positions_total,
        "positions_good": positions_good,
        "positions_bad": positions_bad,
        "positions_no_good": positions_no_good,
        "bad_hits": total_bad_hits,
        "good_hits": total_good_hits,
    }


# 🔸 Загрузка direction_mask сигнала
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


# 🔸 Преобразование direction_mask -> направления
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


# 🔸 Загрузка моделей bt_analysis_model_opt по направлениям (строго по run)
async def _load_model_map_for_run(
    pg,
    scenario_id: int,
    signal_id: int,
    run_id: int,
    directions: List[str],
    models_hint: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}

    # условия достаточности
    if models_hint:
        for d, mid in models_hint.items():
            try:
                dl = str(d).strip().lower()
                out[dl] = {"model_id": int(mid)}
            except Exception:
                continue

    # подгружаем недостающие из БД
    missing_dirs = [d for d in directions if str(d).strip().lower() not in out]
    if not missing_dirs:
        # подгрузим good_threshold_selected из meta
        return await _enrich_model_map(pg, scenario_id, signal_id, run_id, out)

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, direction, meta
            FROM bt_analysis_model_opt
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND run_id      = $3
              AND direction   = ANY($4::text[])
            """,
            scenario_id,
            signal_id,
            run_id,
            missing_dirs,
        )

    for r in rows:
        d = str(r["direction"]).strip().lower()
        out[d] = {"model_id": int(r["id"])}

    return await _enrich_model_map(pg, scenario_id, signal_id, run_id, out)


# 🔸 Обогащение model_map: вытащить good_threshold_selected из meta
async def _enrich_model_map(
    pg,
    scenario_id: int,
    signal_id: int,
    run_id: int,
    model_map: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    if not model_map:
        return {}

    ids = [int(v.get("model_id")) for v in model_map.values() if v.get("model_id")]
    ids = sorted(set(ids))
    if not ids:
        return model_map

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, direction, meta
            FROM bt_analysis_model_opt
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND run_id      = $3
              AND id = ANY($4::int[])
            """,
            scenario_id,
            signal_id,
            run_id,
            ids,
        )

    meta_by_dir: Dict[str, Any] = {}
    for r in rows:
        d = str(r["direction"]).strip().lower()
        meta = r["meta"]
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = None
        meta_by_dir[d] = meta

    for d, v in model_map.items():
        meta = meta_by_dir.get(d)
        good_thr = None
        if isinstance(meta, dict):
            raw_thr = meta.get("good_threshold_selected")
            if raw_thr is not None:
                try:
                    good_thr = float(raw_thr)
                except Exception:
                    good_thr = None
        v["good_threshold_selected"] = good_thr

    return model_map


# 🔸 Загрузка биннов нужного state из bt_analysis_bins_labels (+ family_key/key)
async def _load_bins_from_labels(
    pg,
    model_id: int,
    run_id: int,
    scenario_id: int,
    signal_id: int,
    direction: str,
    state: str,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                l.analysis_id,
                l.indicator_param,
                l.timeframe,
                l.bin_name,
                l.trades,
                l.pnl_abs,
                l.winrate,
                ai.family_key,
                ai."key" AS analysis_key
            FROM bt_analysis_bins_labels l
            JOIN bt_analysis_instances ai
              ON ai.id = l.analysis_id
            WHERE l.model_id    = $1
              AND l.run_id      = $2
              AND l.scenario_id = $3
              AND l.signal_id   = $4
              AND l.direction   = $5
              AND l.state       = $6
            """,
            model_id,
            run_id,
            scenario_id,
            signal_id,
            direction,
            state,
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
                "family_key": r["family_key"],
                "analysis_key": r["analysis_key"],
            }
        )
    return out


# 🔸 Индекс raw по ключу (analysis_id, timeframe, bin_name) -> list(position_uid) (строго по run_id)
async def _load_positions_raw_index_for_analysis_ids(
    pg,
    run_id: int,
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
            WHERE run_id     = $1
              AND scenario_id = $2
              AND signal_id   = $3
              AND direction   = $4
              AND analysis_id = ANY($5::int[])
            """,
            run_id,
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


# 🔸 Загрузка позиций сценария/сигнала в окне run (status=closed, postproc=true, entry_time in window)
async def _load_positions_for_pair(
    pg,
    scenario_id: int,
    signal_id: int,
    directions: List[str],
    window_from: datetime,
    window_to: datetime,
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
              AND status      = 'closed'
              AND postproc    = true
              AND direction   = ANY($3::text[])
              AND entry_time BETWEEN $4 AND $5
            ORDER BY entry_time
            """,
            scenario_id,
            signal_id,
            directions,
            window_from,
            window_to,
        )

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "position_uid": r["position_uid"],
                "direction": str(r["direction"]).strip().lower(),
                "pnl_abs": _safe_decimal(r["pnl_abs"]),
            }
        )
    return out


# 🔸 Запись контейнера позиций в bt_analysis_positions_postproc (run-aware, идемпотентно)
async def _store_positions_postproc(
    pg,
    run_id: int,
    scenario_id: int,
    signal_id: int,
    positions_map: Dict[Any, Dict[str, Any]],
) -> None:
    async with pg.acquire() as conn:
        to_insert: List[Tuple[Any, ...]] = []

        for uid, info in positions_map.items():
            # meta
            meta_obj = None
            if info.get("reject_reason") == "bad_hit":
                meta_obj = {"reject_reason": "bad_hit", "bad_reasons": info.get("bad_reasons") or []}
            elif info.get("reject_reason") == "no_good_hit":
                meta_obj = {"reject_reason": "no_good_hit"}
            else:
                if info.get("good_reasons"):
                    meta_obj = {"good_reasons": info.get("good_reasons")}

            postproc_meta = json.dumps(meta_obj, ensure_ascii=False) if meta_obj is not None else None

            to_insert.append(
                (
                    run_id,
                    uid,
                    scenario_id,
                    signal_id,
                    info["pnl_abs"],
                    postproc_meta,
                    bool(info.get("good_state")),
                )
            )

        if not to_insert:
            return

        await conn.executemany(
            """
            INSERT INTO bt_analysis_positions_postproc (
                run_id,
                position_uid,
                scenario_id,
                signal_id,
                pnl_abs,
                postproc_meta,
                good_state
            )
            VALUES (
                $1, $2, $3, $4, $5, $6::jsonb, $7
            )
            ON CONFLICT (run_id, scenario_id, signal_id, position_uid) DO NOTHING
            """,
            to_insert,
        )


# 🔸 Загрузка исходной статистики окна из bt_scenario_stat (run)
async def _load_orig_scenario_stats(
    pg,
    scenario_id: int,
    signal_id: int,
    run_id: int,
) -> Dict[str, Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT direction, trades, pnl_abs, winrate, roi
            FROM bt_scenario_stat
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND stat_kind   = 'run'
              AND run_id      = $3
            """,
            scenario_id,
            signal_id,
            run_id,
        )

    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        d = str(r["direction"]).strip().lower()
        out[d] = {
            "trades": int(r["trades"]),
            "pnl_abs": _safe_decimal(r["pnl_abs"]),
            "winrate": _safe_decimal(r["winrate"]),
            "roi": _safe_decimal(r["roi"]),
        }
    return out


# 🔸 Загрузка депозита сценария
async def _load_scenario_deposit(pg, scenario_id: int) -> Optional[Decimal]:
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
        return None

    dep = _safe_decimal(row["param_value"])
    if dep <= 0:
        return None
    return dep


# 🔸 Upsert bt_analysis_scenario_stat (run-aware)
async def _upsert_analysis_scenario_stats(
    pg,
    run_id: int,
    scenario_id: int,
    signal_id: int,
    window_from: datetime,
    window_to: datetime,
    positions_map: Dict[Any, Dict[str, Any]],
    removed_stats_bad: Dict[str, Dict[str, Dict[str, Any]]],
    removed_stats_no_good: Dict[str, Dict[str, Any]],
    orig_stats: Dict[str, Dict[str, Any]],
    deposit: Optional[Decimal],
    good_threshold_map: Dict[str, Any],
) -> None:
    per_dir_all: Dict[str, List[Dict[str, Any]]] = {}
    per_dir_good: Dict[str, List[Dict[str, Any]]] = {}

    for info in positions_map.values():
        d = str(info["direction"]).strip().lower()
        per_dir_all.setdefault(d, []).append(info)
        if info.get("good_state"):
            per_dir_good.setdefault(d, []).append(info)

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

            if filt_trades > 0:
                wins = sum(1 for p in good_positions if p["pnl_abs"] > 0)
                filt_winrate = Decimal(wins) / Decimal(filt_trades)
            else:
                filt_winrate = Decimal("0")

            if deposit and deposit > 0:
                try:
                    filt_roi = filt_pnl_abs / deposit
                except (InvalidOperation, ZeroDivisionError):
                    filt_roi = Decimal("0")
            else:
                filt_roi = Decimal("0")

            removed_positions = [p for p in per_dir_all.get(direction, []) if not p.get("good_state")]
            removed_trades = len(removed_positions)
            if removed_trades > 0:
                removed_losers = sum(1 for p in removed_positions if p["pnl_abs"] <= 0)
                removed_accuracy = Decimal(removed_losers) / Decimal(removed_trades)
            else:
                removed_accuracy = Decimal("0")

            # raw_stat (диагностика)
            raw_obj: Dict[str, Any] = {
                "version": 1,
                "note": "removed stats are diagnostic and may include overlaps across indicators",
                "good_threshold_selected": good_threshold_map.get(direction),
                "removed": {},
            }

            bad_dir = removed_stats_bad.get(direction) or {}
            if bad_dir:
                raw_obj["removed"]["bad_hits"] = _build_removed_block(bad_dir)

            ng = removed_stats_no_good.get(direction)
            if ng:
                raw_obj["removed"]["no_good_hit"] = {
                    "trades": int(ng.get("trades", 0)),
                    "pnl_abs": _decimal_to_json_number(_safe_decimal(ng.get("pnl_abs", 0))),
                }

            raw_json = json.dumps(raw_obj, ensure_ascii=False)

            await conn.execute(
                """
                INSERT INTO bt_analysis_scenario_stat (
                    run_id,
                    scenario_id,
                    signal_id,
                    direction,
                    window_from,
                    window_to,
                    orig_trades,
                    orig_pnl_abs,
                    orig_winrate,
                    orig_roi,
                    filt_trades,
                    filt_pnl_abs,
                    filt_winrate,
                    filt_roi,
                    removed_accuracy,
                    raw_stat,
                    created_at
                )
                VALUES (
                    $1, $2, $3, $4,
                    $5, $6,
                    $7, $8, $9, $10,
                    $11, $12, $13, $14,
                    $15,
                    $16::jsonb,
                    now()
                )
                ON CONFLICT (run_id, scenario_id, signal_id, direction)
                DO UPDATE SET
                    window_from      = EXCLUDED.window_from,
                    window_to        = EXCLUDED.window_to,
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
                run_id,
                scenario_id,
                signal_id,
                direction,
                window_from,
                window_to,
                int(orig["trades"]),
                orig["pnl_abs"],
                orig["winrate"],
                orig["roi"],
                int(filt_trades),
                filt_pnl_abs,
                filt_winrate,
                filt_roi,
                removed_accuracy,
                raw_json,
            )


# условия достаточности: формирование агрегата removed->by_indicator
def _build_removed_block(dir_removed: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
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
            by_tf_obj[str(tf)] = {
                "trades": int(tf_stats.get("trades", 0)),
                "pnl_abs": _decimal_to_json_number(_safe_decimal(tf_stats.get("pnl_abs", 0))),
            }

        by_indicator[indicator_key] = {
            "total": {"trades": it_total_trades, "pnl_abs": _decimal_to_json_number(_safe_decimal(it_total_pnl))},
            "by_tf": by_tf_obj,
        }

    return {"total": {"trades": int(total_trades), "pnl_abs": _decimal_to_json_number(total_pnl)}, "by_indicator": by_indicator}


# 🔸 Публикация события готовности postproc v1 (run-aware)
async def _publish_postproc_ready(
    redis,
    scenario_id: int,
    signal_id: int,
    run_id: int,
    window_from: datetime,
    window_to: datetime,
    result: Dict[str, Any],
    model_map: Dict[str, Dict[str, Any]],
    source_finished_at: datetime,
) -> None:
    finished_at = datetime.utcnow()

    models_json = json.dumps(
        {d: {"model_id": int(m.get("model_id"))} for d, m in model_map.items()},
        ensure_ascii=False,
    )

    try:
        await redis.xadd(
            POSTPROC_READY_STREAM_KEY,
            {
                "scenario_id": str(scenario_id),
                "signal_id": str(signal_id),
                "run_id": str(run_id),
                "window_from": window_from.isoformat(),
                "window_to": window_to.isoformat(),
                "positions_total": str(result.get("positions_total", 0)),
                "positions_good": str(result.get("positions_good", 0)),
                "positions_bad": str(result.get("positions_bad", 0)),
                "positions_no_good": str(result.get("positions_no_good", 0)),
                "models": models_json,
                "source_finished_at": source_finished_at.isoformat(),
                "finished_at": finished_at.isoformat(),
            },
        )
        log.debug(
            "BT_ANALYSIS_POSTPROC: опубликовано событие postproc_ready scenario_id=%s signal_id=%s run_id=%s",
            scenario_id,
            signal_id,
            run_id,
        )
    except Exception as e:
        log.error(
            "BT_ANALYSIS_POSTPROC: не удалось опубликовать событие scenario_id=%s signal_id=%s run_id=%s: %s",
            scenario_id,
            signal_id,
            run_id,
            e,
            exc_info=True,
        )


# 🔸 Вспомогательная функция: безопасное Decimal
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


# 🔸 Вспомогательная функция: Decimal -> JSON number
def _decimal_to_json_number(value: Decimal) -> float:
    try:
        return float(value)
    except (TypeError, InvalidOperation, ValueError):
        return 0.0