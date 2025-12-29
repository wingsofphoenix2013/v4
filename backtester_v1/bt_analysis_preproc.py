# bt_analysis_preproc.py — препроцессинг анализа: поиск оптимального порога winrate по биннам (run-aware) и запись в bt_analysis_preproc_stat

import asyncio
import logging
import json
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("BT_ANALYSIS_PREPROC")

# 🔸 Настройки Decimal
getcontext().prec = 28

# 🔸 Константы стримов
PREPROC_STREAM_KEY = "bt:analysis:ready"
PREPROC_CONSUMER_GROUP = "bt_analysis_preproc"
PREPROC_CONSUMER_NAME = "bt_analysis_preproc_main"

# 🔸 Стрим готовности препроцессинга (для следующего этапа)
PREPROC_READY_STREAM_KEY = "bt:analysis:preproc_ready"

# 🔸 Настройки чтения стрима
PREPROC_STREAM_BATCH_SIZE = 10
PREPROC_STREAM_BLOCK_MS = 5000

# 🔸 Таблица результатов
PREPROC_TABLE = "bt_analysis_preproc_stat"

# 🔸 Квантизация метрик
Q4 = Decimal("0.0001")


# 🔸 Длительность в Decimal с обрезкой до 4 знаков
def _q4(value: Decimal) -> Decimal:
    return value.quantize(Q4, rounding=ROUND_DOWN)


# 🔸 Безопасный Decimal
def _d(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return default


# 🔸 Публичная точка входа: воркер препроцессинга анализа
async def run_bt_analysis_preproc_orchestrator(pg, redis) -> None:
    log.debug("BT_ANALYSIS_PREPROC: оркестратор препроцессинга запущен")

    await _ensure_consumer_group(redis)

    while True:
        try:
            messages = await _read_from_stream(redis)
            if not messages:
                continue

            total_msgs = 0
            total_pairs = 0
            total_groups = 0
            total_upserts = 0
            total_skipped = 0
            total_errors = 0

            for stream_key, entries in messages:
                if stream_key != PREPROC_STREAM_KEY:
                    continue

                for entry_id, fields in entries:
                    total_msgs += 1

                    ctx = _parse_analysis_ready(fields)
                    if not ctx:
                        await redis.xack(PREPROC_STREAM_KEY, PREPROC_CONSUMER_GROUP, entry_id)
                        total_skipped += 1
                        continue

                    scenario_id = ctx["scenario_id"]
                    signal_id = ctx["signal_id"]
                    run_id = ctx["run_id"]
                    finished_at = ctx["finished_at"]

                    total_pairs += 1

                    # загружаем все бины по паре (scenario/signal/run)
                    try:
                        rows = await _load_bins_stat_rows(pg, run_id, scenario_id, signal_id)
                    except Exception as e:
                        total_errors += 1
                        log.error(
                            "BT_ANALYSIS_PREPROC: ошибка загрузки bt_analysis_bins_stat для scenario_id=%s, signal_id=%s, run_id=%s: %s",
                            scenario_id,
                            signal_id,
                            run_id,
                            e,
                            exc_info=True,
                        )
                        await redis.xack(PREPROC_STREAM_KEY, PREPROC_CONSUMER_GROUP, entry_id)
                        continue

                    # группируем и пишем результаты
                    try:
                        groups_processed, upserts_done = await _process_and_store_groups(
                            pg=pg,
                            run_id=run_id,
                            scenario_id=scenario_id,
                            signal_id=signal_id,
                            rows=rows,
                        )
                        total_groups += groups_processed
                        total_upserts += upserts_done
                    except Exception as e:
                        total_errors += 1
                        log.error(
                            "BT_ANALYSIS_PREPROC: ошибка обработки групп для scenario_id=%s, signal_id=%s, run_id=%s: %s",
                            scenario_id,
                            signal_id,
                            run_id,
                            e,
                            exc_info=True,
                        )
                        await redis.xack(PREPROC_STREAM_KEY, PREPROC_CONSUMER_GROUP, entry_id)
                        continue

                    log.info(
                        "BT_ANALYSIS_PREPROC: pair done — scenario_id=%s, signal_id=%s, run_id=%s, finished_at=%s, groups=%s, upserts=%s",
                        scenario_id,
                        signal_id,
                        run_id,
                        finished_at,
                        groups_processed,
                        upserts_done,
                    )

                    # публикуем событие готовности препроцессинга (run-aware)
                    finished_at_preproc = datetime.utcnow()
                    try:
                        await redis.xadd(
                            PREPROC_READY_STREAM_KEY,
                            {
                                "scenario_id": str(scenario_id),
                                "signal_id": str(signal_id),
                                "run_id": str(run_id),
                                "groups": str(groups_processed),
                                "upserts": str(upserts_done),
                                "finished_at": finished_at_preproc.isoformat(),
                            },
                        )
                    except Exception as e:
                        log.error(
                            "BT_ANALYSIS_PREPROC: не удалось опубликовать событие в '%s' scenario_id=%s signal_id=%s run_id=%s: %s",
                            PREPROC_READY_STREAM_KEY,
                            scenario_id,
                            signal_id,
                            run_id,
                            e,
                            exc_info=True,
                        )

                    await redis.xack(PREPROC_STREAM_KEY, PREPROC_CONSUMER_GROUP, entry_id)

            log.info(
                "BT_ANALYSIS_PREPROC: batch summary — msgs=%s, pairs=%s, groups=%s, upserts=%s, skipped=%s, errors=%s",
                total_msgs,
                total_pairs,
                total_groups,
                total_upserts,
                total_skipped,
                total_errors,
            )

        except Exception as e:
            log.error("BT_ANALYSIS_PREPROC: loop error: %s", e, exc_info=True)
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=PREPROC_STREAM_KEY,
            groupname=PREPROC_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_ANALYSIS_PREPROC: создана consumer group '%s' для стрима '%s'",
            PREPROC_CONSUMER_GROUP,
            PREPROC_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_ANALYSIS_PREPROC: consumer group '%s' для стрима '%s' уже существует",
                PREPROC_CONSUMER_GROUP,
                PREPROC_STREAM_KEY,
            )
        else:
            log.error(
                "BT_ANALYSIS_PREPROC: ошибка при создании consumer group '%s': %s",
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
        streams={PREPROC_STREAM_KEY: ">"},
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


# 🔸 Разбор сообщения bt:analysis:ready (run-aware)
def _parse_analysis_ready(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id_str = fields.get("scenario_id")
        signal_id_str = fields.get("signal_id")
        run_id_str = fields.get("run_id")
        finished_at_str = fields.get("finished_at")

        if not (scenario_id_str and signal_id_str and run_id_str and finished_at_str):
            return None

        scenario_id = int(scenario_id_str)
        signal_id = int(signal_id_str)
        run_id = int(run_id_str)
        finished_at = datetime.fromisoformat(finished_at_str)

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "run_id": run_id,
            "finished_at": finished_at,
        }
    except Exception:
        return None


# 🔸 Загрузка бин-статистики по паре run/scenario/signal
async def _load_bins_stat_rows(
    pg,
    run_id: int,
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
                trades,
                pnl_abs,
                winrate
            FROM bt_analysis_bins_stat
            WHERE run_id = $1
              AND scenario_id = $2
              AND signal_id = $3
            ORDER BY analysis_id, indicator_param NULLS FIRST, timeframe, direction, winrate, bin_name
            """,
            int(run_id),
            int(scenario_id),
            int(signal_id),
        )

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "analysis_id": int(r["analysis_id"]),
                "indicator_param": r["indicator_param"],
                "timeframe": str(r["timeframe"]).strip().lower(),
                "direction": str(r["direction"]).strip().lower(),
                "bin_name": str(r["bin_name"]),
                "trades": int(r["trades"] or 0),
                "pnl_abs": _d(r["pnl_abs"]),
                "winrate": _d(r["winrate"]),
            }
        )
    return out


# 🔸 Обработка всех групп и запись результатов в bt_analysis_preproc_stat
async def _process_and_store_groups(
    pg,
    run_id: int,
    scenario_id: int,
    signal_id: int,
    rows: List[Dict[str, Any]],
) -> Tuple[int, int]:
    if not rows:
        return 0, 0

    groups_processed = 0
    upserts_done = 0

    # группируем вручную по (analysis_id, indicator_param_norm, timeframe, direction)
    current_key: Optional[Tuple[int, str, str, str]] = None
    bucket: List[Dict[str, Any]] = []

    def _flush_bucket() -> Tuple[int, int]:
        nonlocal groups_processed, upserts_done, bucket, current_key
        if not bucket or current_key is None:
            bucket = []
            current_key = None
            return 0, 0

        analysis_id, indicator_param_norm, tf, direction = current_key

        # считаем оптимум
        result = _compute_best_threshold(bucket)

        if result is None:
            bucket = []
            current_key = None
            return 0, 0

        (
            orig_trades,
            orig_pnl,
            orig_winrate,
            filt_trades,
            filt_pnl,
            filt_winrate,
            threshold,
            raw_stat,
        ) = result

        # апсерт в таблицу результатов
        async def _upsert() -> None:
            async with pg.acquire() as conn:
                await conn.execute(
                    f"""
                    INSERT INTO {PREPROC_TABLE} (
                        run_id,
                        analysis_id,
                        scenario_id,
                        signal_id,
                        indicator_param,
                        timeframe,
                        direction,
                        orig_trades,
                        orig_pnl_abs,
                        orig_winrate,
                        filt_trades,
                        filt_pnl_abs,
                        filt_winrate,
                        winrate_threshold,
                        raw_stat,
                        created_at
                    )
                    VALUES (
                        $1, $2, $3, $4,
                        $5, $6, $7,
                        $8, $9, $10,
                        $11, $12, $13,
                        $14,
                        $15::jsonb,
                        now()
                    )
                    ON CONFLICT (run_id, analysis_id, scenario_id, signal_id, indicator_param, timeframe, direction)
                    DO UPDATE SET
                        orig_trades        = EXCLUDED.orig_trades,
                        orig_pnl_abs       = EXCLUDED.orig_pnl_abs,
                        orig_winrate       = EXCLUDED.orig_winrate,
                        filt_trades        = EXCLUDED.filt_trades,
                        filt_pnl_abs       = EXCLUDED.filt_pnl_abs,
                        filt_winrate       = EXCLUDED.filt_winrate,
                        winrate_threshold  = EXCLUDED.winrate_threshold,
                        raw_stat           = EXCLUDED.raw_stat,
                        updated_at         = now()
                    """,
                    int(run_id),
                    int(analysis_id),
                    int(scenario_id),
                    int(signal_id),
                    str(indicator_param_norm),
                    str(tf),
                    str(direction),
                    int(orig_trades),
                    str(_q4(orig_pnl)),
                    str(_q4(orig_winrate)),
                    int(filt_trades),
                    str(_q4(filt_pnl)),
                    str(_q4(filt_winrate)),
                    str(_q4(threshold)) if threshold is not None else None,
                    json.dumps(raw_stat, ensure_ascii=False),
                )

        # выполняем upsert (в этом уровне — без параллелизма)
        asyncio.get_event_loop()  # чтобы не ругаться в некоторых рантаймах на отсутствие loop
        loop = asyncio.get_running_loop()
        coro = _upsert()
        loop.create_task(coro)
        # условия достаточности: ждём завершения, чтобы не потерять апсерт
        # (создаём таску только чтобы избежать линтера; тут сразу await)
        # noinspection PyTypeChecker
        # noqa: E999
        # фактически:
        # await _upsert()
        # но без “магии”:
        # (мы уже внутри sync helper, поэтому возвращаем признак и вызываем await выше)
        return 1, 1

    # основной проход
    for r in rows:
        analysis_id = int(r["analysis_id"])
        ind = r.get("indicator_param")
        indicator_param_norm = str(ind).strip() if ind is not None else ""
        tf = str(r["timeframe"]).strip().lower()
        direction = str(r["direction"]).strip().lower()

        key = (analysis_id, indicator_param_norm, tf, direction)

        if current_key is None:
            current_key = key
            bucket = [r]
            continue

        if key == current_key:
            bucket.append(r)
            continue

        # флашим прошлую группу
        # условия достаточности
        groups_processed += 1
        # корректный await на апсерт
        result = _compute_best_threshold(bucket)
        if result is not None:
            (
                orig_trades,
                orig_pnl,
                orig_winrate,
                filt_trades,
                filt_pnl,
                filt_winrate,
                threshold,
                raw_stat,
            ) = result

            async with pg.acquire() as conn:
                await conn.execute(
                    f"""
                    INSERT INTO {PREPROC_TABLE} (
                        run_id,
                        analysis_id,
                        scenario_id,
                        signal_id,
                        indicator_param,
                        timeframe,
                        direction,
                        orig_trades,
                        orig_pnl_abs,
                        orig_winrate,
                        filt_trades,
                        filt_pnl_abs,
                        filt_winrate,
                        winrate_threshold,
                        raw_stat,
                        created_at
                    )
                    VALUES (
                        $1, $2, $3, $4,
                        $5, $6, $7,
                        $8, $9, $10,
                        $11, $12, $13,
                        $14,
                        $15::jsonb,
                        now()
                    )
                    ON CONFLICT (run_id, analysis_id, scenario_id, signal_id, indicator_param, timeframe, direction)
                    DO UPDATE SET
                        orig_trades        = EXCLUDED.orig_trades,
                        orig_pnl_abs       = EXCLUDED.orig_pnl_abs,
                        orig_winrate       = EXCLUDED.orig_winrate,
                        filt_trades        = EXCLUDED.filt_trades,
                        filt_pnl_abs       = EXCLUDED.filt_pnl_abs,
                        filt_winrate       = EXCLUDED.filt_winrate,
                        winrate_threshold  = EXCLUDED.winrate_threshold,
                        raw_stat           = EXCLUDED.raw_stat,
                        updated_at         = now()
                    """,
                    int(run_id),
                    int(current_key[0]),
                    int(scenario_id),
                    int(signal_id),
                    str(current_key[1]),
                    str(current_key[2]),
                    str(current_key[3]),
                    int(orig_trades),
                    str(_q4(orig_pnl)),
                    str(_q4(orig_winrate)),
                    int(filt_trades),
                    str(_q4(filt_pnl)),
                    str(_q4(filt_winrate)),
                    str(_q4(threshold)) if threshold is not None else None,
                    json.dumps(raw_stat, ensure_ascii=False),
                )
            upserts_done += 1

        # начинаем новую
        current_key = key
        bucket = [r]

    # флашим последнюю группу
    if bucket and current_key is not None:
        groups_processed += 1
        result = _compute_best_threshold(bucket)
        if result is not None:
            (
                orig_trades,
                orig_pnl,
                orig_winrate,
                filt_trades,
                filt_pnl,
                filt_winrate,
                threshold,
                raw_stat,
            ) = result

            async with pg.acquire() as conn:
                await conn.execute(
                    f"""
                    INSERT INTO {PREPROC_TABLE} (
                        run_id,
                        analysis_id,
                        scenario_id,
                        signal_id,
                        indicator_param,
                        timeframe,
                        direction,
                        orig_trades,
                        orig_pnl_abs,
                        orig_winrate,
                        filt_trades,
                        filt_pnl_abs,
                        filt_winrate,
                        winrate_threshold,
                        raw_stat,
                        created_at
                    )
                    VALUES (
                        $1, $2, $3, $4,
                        $5, $6, $7,
                        $8, $9, $10,
                        $11, $12, $13,
                        $14,
                        $15::jsonb,
                        now()
                    )
                    ON CONFLICT (run_id, analysis_id, scenario_id, signal_id, indicator_param, timeframe, direction)
                    DO UPDATE SET
                        orig_trades        = EXCLUDED.orig_trades,
                        orig_pnl_abs       = EXCLUDED.orig_pnl_abs,
                        orig_winrate       = EXCLUDED.orig_winrate,
                        filt_trades        = EXCLUDED.filt_trades,
                        filt_pnl_abs       = EXCLUDED.filt_pnl_abs,
                        filt_winrate       = EXCLUDED.filt_winrate,
                        winrate_threshold  = EXCLUDED.winrate_threshold,
                        raw_stat           = EXCLUDED.raw_stat,
                        updated_at         = now()
                    """,
                    int(run_id),
                    int(current_key[0]),
                    int(scenario_id),
                    int(signal_id),
                    str(current_key[1]),
                    str(current_key[2]),
                    str(current_key[3]),
                    int(orig_trades),
                    str(_q4(orig_pnl)),
                    str(_q4(orig_winrate)),
                    int(filt_trades),
                    str(_q4(filt_pnl)),
                    str(_q4(filt_winrate)),
                    str(_q4(threshold)) if threshold is not None else None,
                    json.dumps(raw_stat, ensure_ascii=False),
                )
            upserts_done += 1

    return groups_processed, upserts_done


# 🔸 Вычисление оптимального порога winrate для одной группы биннов
def _compute_best_threshold(
    bins: List[Dict[str, Any]],
) -> Optional[Tuple[int, Decimal, Decimal, int, Decimal, Decimal, Optional[Decimal], Dict[str, Any]]]:
    if not bins:
        return None

    # нормализуем и фильтруем мусор
    normalized: List[Dict[str, Any]] = []
    for b in bins:
        trades = int(b.get("trades") or 0)
        if trades <= 0:
            continue

        winrate = _d(b.get("winrate"))
        pnl = _d(b.get("pnl_abs"))

        normalized.append(
            {
                "bin_name": str(b.get("bin_name") or ""),
                "trades": trades,
                "winrate": winrate,
                "pnl_abs": pnl,
                "wins_est": (Decimal(trades) * winrate),
            }
        )

    if not normalized:
        return None

    # сортировка по winrate (хуже → лучше), затем по pnl (хуже → лучше), затем по имени (детерминизм)
    normalized.sort(key=lambda x: (x["winrate"], x["pnl_abs"], x["bin_name"]))

    n = len(normalized)

    # префиксные суммы по "удаляемым" биннам
    pref_trades: List[int] = [0] * (n + 1)
    pref_pnl: List[Decimal] = [Decimal("0")] * (n + 1)
    pref_wins: List[Decimal] = [Decimal("0")] * (n + 1)

    for i in range(n):
        pref_trades[i + 1] = pref_trades[i] + int(normalized[i]["trades"])
        pref_pnl[i + 1] = pref_pnl[i] + _d(normalized[i]["pnl_abs"])
        pref_wins[i + 1] = pref_wins[i] + _d(normalized[i]["wins_est"])

    orig_trades = pref_trades[n]
    orig_pnl = pref_pnl[n]
    orig_winrate = (pref_wins[n] / Decimal(orig_trades)) if orig_trades > 0 else Decimal("0")

    # перебор k: выкинули первые k худших, оставили [k..n)
    best_k = 0
    best_pnl = orig_pnl
    best_trades = orig_trades
    best_wins = pref_wins[n]
    best_threshold = normalized[0]["winrate"] if n > 0 else None

    for k in range(0, n):
        kept_trades = orig_trades - pref_trades[k]
        if kept_trades <= 0:
            continue

        kept_pnl = orig_pnl - pref_pnl[k]
        kept_wins = pref_wins[n] - pref_wins[k]
        threshold = normalized[k]["winrate"]

        # критерий: максимум pnl по оставшимся
        if kept_pnl > best_pnl:
            best_pnl = kept_pnl
            best_k = k
            best_trades = kept_trades
            best_wins = kept_wins
            best_threshold = threshold

    filt_trades = int(best_trades)
    filt_pnl = best_pnl
    filt_winrate = (best_wins / Decimal(filt_trades)) if filt_trades > 0 else Decimal("0")

    removed_bins = [x["bin_name"] for x in normalized[:best_k]]
    kept_bins = [x["bin_name"] for x in normalized[best_k:]]

    raw_stat = {
        "version": "v1",
        "sort": "winrate_asc",
        "criterion": "max_filt_pnl_abs",
        "bins_total": n,
        "cut_k": int(best_k),
        "winrate_threshold": str(_q4(_d(best_threshold))) if best_threshold is not None else None,
        "kept_bins": kept_bins,
        "removed_bins": removed_bins,
        "orig": {
            "trades": int(orig_trades),
            "pnl_abs": str(_q4(orig_pnl)),
            "winrate": str(_q4(orig_winrate)),
        },
        "filt": {
            "trades": int(filt_trades),
            "pnl_abs": str(_q4(filt_pnl)),
            "winrate": str(_q4(filt_winrate)),
        },
    }

    return (
        int(orig_trades),
        _q4(orig_pnl),
        _q4(orig_winrate),
        int(filt_trades),
        _q4(filt_pnl),
        _q4(filt_winrate),
        _d(best_threshold) if best_threshold is not None else None,
        raw_stat,
    )