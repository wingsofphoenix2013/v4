# bt_analysis_preproc_v2.py — препроцессинг v2: подбор bad-биннов напрямую по ΔROI/ΔPNL (OR-логика) + запись в *_v2 + публикация bt:analysis:preproc_v2_ready

import asyncio
import json
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict, List, Optional, Tuple, Set

# 🔸 Константы стримов и настроек препроцессинга v2
ANALYSIS_READY_STREAM_KEY = "bt:analysis:ready"
PREPROC_V2_READY_STREAM_KEY = "bt:analysis:preproc_v2_ready"

PREPROC_V2_CONSUMER_GROUP = "bt_analysis_preproc_v2"
PREPROC_V2_CONSUMER_NAME = "bt_analysis_preproc_v2_main"

PREPROC_STREAM_BATCH_SIZE = 10
PREPROC_STREAM_BLOCK_MS = 5000

PREPROC_MAX_CONCURRENCY = 3

# 🔸 Ограничения и настройки оптимизации v2
MIN_KEEP_RATIO = Decimal("0.20")     # минимум оставляемых сделок от orig
GREEDY_MAX_STEPS = 2000             # максимальное число шагов greedy по биннам
GREEDY_MIN_IMPROVE_EPS = Decimal("0.00000001")  # технический eps для сравнения objective

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
                        _process_message(entry_id=entry_id, fields=fields, pg=pg, redis=redis, sema=sema),
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
            log.error("BT_ANALYSIS_PREPROC_V2: ошибка в основном цикле оркестратора: %s", e, exc_info=True)
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима bt:analysis:ready
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=ANALYSIS_READY_STREAM_KEY,
            groupname=PREPROC_V2_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_ANALYSIS_PREPROC_V2: создана consumer group '%s' для стрима '%s'",
            PREPROC_V2_CONSUMER_GROUP,
            ANALYSIS_READY_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_ANALYSIS_PREPROC_V2: consumer group '%s' для стрима '%s' уже существует",
                PREPROC_V2_CONSUMER_GROUP,
                ANALYSIS_READY_STREAM_KEY,
            )
        else:
            log.error(
                "BT_ANALYSIS_PREPROC_V2: ошибка при создании consumer group '%s': %s",
                PREPROC_V2_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:analysis:ready
async def _read_from_stream(redis) -> List[Any]:
    entries = await redis.xreadgroup(
        groupname=PREPROC_V2_CONSUMER_GROUP,
        consumername=PREPROC_V2_CONSUMER_NAME,
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
            "BT_ANALYSIS_PREPROC_V2: ошибка разбора сообщения bt:analysis:ready: %s, fields=%s",
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
            await redis.xack(ANALYSIS_READY_STREAM_KEY, PREPROC_V2_CONSUMER_GROUP, entry_id)
            return

        scenario_id = ctx["scenario_id"]
        signal_id = ctx["signal_id"]
        source_finished_at = ctx["source_finished_at"]

        pair_key = (scenario_id, signal_id)
        last_finished = _last_analysis_finished_at.get(pair_key)

        # дедуп по source_finished_at
        if last_finished is not None and last_finished == source_finished_at:
            log.debug(
                "BT_ANALYSIS_PREPROC_V2: дубликат сообщения scenario_id=%s, signal_id=%s, source_finished_at=%s, stream_id=%s — расчёт не выполняется",
                scenario_id,
                signal_id,
                source_finished_at,
                entry_id,
            )
            await redis.xack(ANALYSIS_READY_STREAM_KEY, PREPROC_V2_CONSUMER_GROUP, entry_id)
            return

        _last_analysis_finished_at[pair_key] = source_finished_at

        started_at = datetime.utcnow()

        try:
            # направления: в твоей конфигурации сигнал моно-направленный,
            # но читаем direction_mask для совместимости
            direction_mask = await _load_signal_direction_mask(pg, signal_id)
            directions = _directions_from_mask(direction_mask)

            # депозит сценария (для ROI)
            deposit = await _load_scenario_deposit(pg, scenario_id)

            results: Dict[str, Dict[str, Any]] = {}

            for direction in directions:
                # расчёт модели v2 для направления
                r = await _optimize_bad_bins_for_direction(
                    pg=pg,
                    scenario_id=scenario_id,
                    signal_id=signal_id,
                    direction=direction,
                    deposit=deposit,
                    source_finished_at=source_finished_at,
                    direction_mask=direction_mask,
                )
                results[direction] = r

            # публикуем событие готовности preproc v2
            await _publish_preproc_v2_ready(
                redis=redis,
                scenario_id=scenario_id,
                signal_id=signal_id,
                source_finished_at=source_finished_at,
                direction_mask=direction_mask,
            )

            elapsed_ms = int((datetime.utcnow() - started_at).total_seconds() * 1000)

            parts: List[str] = []
            for d in directions:
                r = results.get(d) or {}
                parts.append(
                    f"{d} steps={r.get('steps_used')} bad_bins={r.get('bad_bins_selected')} "
                    f"orig_trades={r.get('orig_trades')} filt_trades={r.get('filt_trades')} "
                    f"orig_roi={r.get('orig_roi')} filt_roi={r.get('filt_roi')} removed_acc={r.get('removed_accuracy')}"
                )

            log.info(
                "BT_ANALYSIS_PREPROC_V2: scenario_id=%s, signal_id=%s — direction_mask=%s, directions=%s, %s, source_finished_at=%s, elapsed_ms=%s",
                scenario_id,
                signal_id,
                direction_mask,
                directions,
                " | ".join(parts) if parts else "no_results",
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
            await redis.xack(ANALYSIS_READY_STREAM_KEY, PREPROC_V2_CONSUMER_GROUP, entry_id)


# 🔸 Оптимизация v2: прямой подбор bad-биннов по ΔROI/ΔPNL (OR-логика) + запись model_opt_v2 + bins_labels_v2
async def _optimize_bad_bins_for_direction(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
    deposit: Optional[Decimal],
    source_finished_at: datetime,
    direction_mask: Optional[str],
) -> Dict[str, Any]:
    # условия достаточности
    dir_norm = str(direction or "").strip().lower()
    if dir_norm not in ("long", "short"):
        return {
            "status": "skipped",
            "reason": "invalid_direction",
            "steps_used": 0,
            "bad_bins_selected": 0,
        }

    # основной расчёт делаем на одном соединении (tmp tables)
    async with pg.acquire() as conn:
        async with conn.transaction():
            # считаем orig по позициям (postproc=true)
            orig = await _load_orig_stats(conn, scenario_id, signal_id, dir_norm, deposit)

            orig_trades = int(orig["orig_trades"])
            orig_pnl = _safe_decimal(orig["orig_pnl_abs"])
            orig_wins = int(orig["orig_wins"])
            orig_winrate = _safe_decimal(orig["orig_winrate"])
            orig_roi = _safe_decimal(orig["orig_roi"])

            if orig_trades <= 0:
                # нет позиций — чистим записи v2 для направления и выходим
                await _delete_v2_model_for_direction(conn, scenario_id, signal_id, dir_norm)
                return {
                    "status": "ok",
                    "reason": "no_positions",
                    "steps_used": 0,
                    "bad_bins_selected": 0,
                    "orig_trades": 0,
                    "filt_trades": 0,
                    "orig_roi": Decimal("0"),
                    "filt_roi": Decimal("0"),
                    "removed_accuracy": Decimal("0"),
                }

            # минимум оставляемых сделок
            min_keep_trades = int((Decimal(orig_trades) * MIN_KEEP_RATIO).to_integral_value(rounding=ROUND_DOWN))
            if min_keep_trades < 1:
                min_keep_trades = 1

            # tmp tables
            await conn.execute("CREATE TEMP TABLE IF NOT EXISTS tmp_v2_removed_pos (position_uid uuid PRIMARY KEY) ON COMMIT DROP;")
            await conn.execute(
                "CREATE TEMP TABLE IF NOT EXISTS tmp_v2_selected_bins (analysis_id int4, timeframe text, bin_name text, PRIMARY KEY (analysis_id, timeframe, bin_name)) ON COMMIT DROP;"
            )
            await conn.execute(
                "CREATE TEMP TABLE IF NOT EXISTS tmp_v2_bins (analysis_id int4, timeframe text, bin_name text, PRIMARY KEY (analysis_id, timeframe, bin_name)) ON COMMIT DROP;"
            )

            # чистим на всякий случай (в рамках одного conn)
            await conn.execute("TRUNCATE tmp_v2_removed_pos;")
            await conn.execute("TRUNCATE tmp_v2_selected_bins;")
            await conn.execute("TRUNCATE tmp_v2_bins;")

            # загружаем все доступные бины как кандидаты (distinct по ключу OR)
            await conn.execute(
                """
                INSERT INTO tmp_v2_bins (analysis_id, timeframe, bin_name)
                SELECT DISTINCT analysis_id, timeframe, bin_name
                FROM bt_analysis_bins_stat
                WHERE scenario_id = $1
                  AND signal_id   = $2
                  AND direction   = $3
                ON CONFLICT DO NOTHING
                """,
                scenario_id,
                signal_id,
                dir_norm,
            )

            # стартовое состояние: ничего не удалили
            kept_trades = orig_trades
            kept_pnl = orig_pnl
            kept_wins = orig_wins

            # objective: ROI если есть депозит, иначе pnl
            if deposit and deposit > 0:
                kept_obj = _safe_div(kept_pnl, deposit)
            else:
                kept_obj = kept_pnl

            steps_used = 0

            # основной greedy-цикл
            for step in range(int(GREEDY_MAX_STEPS)):
                # выбираем лучший следующий бин (который ещё не выбран) по kept_obj_new
                best = await _find_best_next_bin(
                    conn=conn,
                    scenario_id=scenario_id,
                    signal_id=signal_id,
                    direction=dir_norm,
                    kept_trades=kept_trades,
                    kept_pnl=kept_pnl,
                    kept_wins=kept_wins,
                    deposit=deposit,
                    min_keep_trades=min_keep_trades,
                )

                # нет кандидатов
                if not best:
                    break

                # проверка улучшения
                best_obj_new = _safe_decimal(best["kept_obj_new"])
                if best_obj_new <= kept_obj + GREEDY_MIN_IMPROVE_EPS:
                    break

                analysis_id = int(best["analysis_id"])
                timeframe = str(best["timeframe"])
                bin_name = str(best["bin_name"])

                m_trades = int(best["m_trades"])
                m_pnl = _safe_decimal(best["m_pnl"])
                m_wins = int(best["m_wins"])

                # обновляем состояния
                kept_trades = int(best["kept_trades_new"])
                kept_pnl = _safe_decimal(best["kept_pnl_new"])
                kept_wins = max(0, kept_wins - m_wins)

                kept_obj = best_obj_new
                steps_used = step + 1

                # помечаем бин как выбранный
                await conn.execute(
                    "INSERT INTO tmp_v2_selected_bins (analysis_id, timeframe, bin_name) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                    analysis_id,
                    timeframe,
                    bin_name,
                )

                # добавляем позиции этого бина в removed (только новые)
                await _add_removed_positions_for_bin(
                    conn=conn,
                    scenario_id=scenario_id,
                    signal_id=signal_id,
                    direction=dir_norm,
                    analysis_id=analysis_id,
                    timeframe=timeframe,
                    bin_name=bin_name,
                )

                # достигли нижней границы по сделкам — дальше нельзя
                if kept_trades <= min_keep_trades:
                    break

            # финальные метрики
            filt_trades = kept_trades
            filt_pnl = kept_pnl
            filt_wins = kept_wins

            if filt_trades > 0:
                filt_winrate = Decimal(filt_wins) / Decimal(filt_trades)
            else:
                filt_winrate = Decimal("0")

            if deposit and deposit > 0:
                filt_roi = _safe_div(filt_pnl, deposit)
            else:
                filt_roi = Decimal("0")

            removed_trades = orig_trades - filt_trades
            removed_wins = orig_wins - filt_wins
            removed_losers = removed_trades - removed_wins

            if removed_trades > 0:
                removed_accuracy = Decimal(removed_losers) / Decimal(removed_trades)
            else:
                removed_accuracy = Decimal("0")

            # выбранные bad-бинны
            selected_bins = await _load_selected_bins(conn)
            bad_bins_selected = len(selected_bins)

            selected_analysis_ids = sorted({int(a) for a, _, _ in selected_bins})

            # best_threshold для v2 не используется (логика не пороговая), фиксируем 0
            best_threshold = Decimal("0")

            meta_obj = {
                "version": 1,
                "method": "greedy_bad_bins_by_objective",
                "objective": "roi" if (deposit and deposit > 0) else "pnl_abs",
                "min_keep_ratio": str(MIN_KEEP_RATIO),
                "min_keep_trades": int(min_keep_trades),
                "steps_used": int(steps_used),
                "bad_bins_selected": int(bad_bins_selected),
                "direction": dir_norm,
                "direction_mask": direction_mask,
                "deposit": str(deposit) if deposit is not None else None,
            }

            # upsert model_opt_v2 -> model_id
            model_id = await _upsert_model_opt_v2_return_id(
                conn=conn,
                scenario_id=scenario_id,
                signal_id=signal_id,
                direction=dir_norm,
                best_threshold=best_threshold,
                selected_analysis_ids=selected_analysis_ids,
                orig_trades=orig_trades,
                orig_pnl_abs=orig_pnl,
                orig_winrate=orig_winrate,
                orig_roi=orig_roi,
                filt_trades=filt_trades,
                filt_pnl_abs=filt_pnl,
                filt_winrate=filt_winrate,
                filt_roi=filt_roi,
                removed_trades=removed_trades,
                removed_accuracy=removed_accuracy,
                meta_obj=meta_obj,
                source_finished_at=source_finished_at,
            )

            # rebuild bins_labels_v2 (good/bad) по bins_stat (все строки)
            bins_rows = await _load_bins_stat_rows(conn, scenario_id, signal_id, dir_norm)
            await _rebuild_bins_labels_v2(
                conn=conn,
                model_id=model_id,
                scenario_id=scenario_id,
                signal_id=signal_id,
                direction=dir_norm,
                threshold_used=best_threshold,
                selected_bad_bins=set(selected_bins),
                bins_rows=bins_rows,
            )

            log.info(
                "BT_ANALYSIS_PREPROC_V2: модель v2 сохранена — scenario_id=%s, signal_id=%s, direction=%s, model_id=%s, "
                "steps=%s, bad_bins=%s, orig_trades=%s, filt_trades=%s, orig_roi=%s, filt_roi=%s, removed_acc=%s",
                scenario_id,
                signal_id,
                dir_norm,
                model_id,
                steps_used,
                bad_bins_selected,
                orig_trades,
                filt_trades,
                _q_decimal(orig_roi),
                _q_decimal(filt_roi),
                _q_decimal(removed_accuracy),
            )

            return {
                "status": "ok",
                "steps_used": steps_used,
                "bad_bins_selected": bad_bins_selected,
                "orig_trades": orig_trades,
                "filt_trades": filt_trades,
                "orig_roi": _q_decimal(orig_roi),
                "filt_roi": _q_decimal(filt_roi),
                "removed_accuracy": _q_decimal(removed_accuracy),
                "model_id": model_id,
            }


# 🔸 Поиск лучшего следующего бина по маржинальному эффекту (Δobjective), учитывая tmp_v2_removed_pos и tmp_v2_selected_bins
async def _find_best_next_bin(
    conn,
    scenario_id: int,
    signal_id: int,
    direction: str,
    kept_trades: int,
    kept_pnl: Decimal,
    kept_wins: int,
    deposit: Optional[Decimal],
    min_keep_trades: int,
) -> Optional[Dict[str, Any]]:
    dep = deposit if (deposit and deposit > 0) else Decimal("0")

    row = await conn.fetchrow(
        """
        WITH cand AS (
            SELECT b.analysis_id, b.timeframe, b.bin_name
            FROM tmp_v2_bins b
            LEFT JOIN tmp_v2_selected_bins s
              ON s.analysis_id = b.analysis_id
             AND s.timeframe   = b.timeframe
             AND s.bin_name    = b.bin_name
            WHERE s.analysis_id IS NULL
        ),
        hits AS (
            SELECT
                c.analysis_id,
                c.timeframe,
                c.bin_name,
                p.position_uid,
                p.pnl_abs
            FROM cand c
            JOIN bt_analysis_positions_raw r
              ON r.scenario_id = $1
             AND r.signal_id   = $2
             AND r.direction   = $3
             AND r.analysis_id = c.analysis_id
             AND r.timeframe   = c.timeframe
             AND r.bin_name    = c.bin_name
            JOIN bt_scenario_positions p
              ON p.position_uid = r.position_uid
             AND p.scenario_id  = $1
             AND p.signal_id    = $2
             AND p.direction    = $3
             AND p.postproc     = true
            LEFT JOIN tmp_v2_removed_pos tr
              ON tr.position_uid = p.position_uid
            WHERE tr.position_uid IS NULL
        ),
        agg AS (
            SELECT
                analysis_id,
                timeframe,
                bin_name,
                COUNT(*) AS m_trades,
                COALESCE(SUM(pnl_abs), 0) AS m_pnl,
                COUNT(*) FILTER (WHERE pnl_abs > 0) AS m_wins
            FROM hits
            GROUP BY analysis_id, timeframe, bin_name
        ),
        eval AS (
            SELECT
                a.*,
                ($4::int4 - a.m_trades)::int4 AS kept_trades_new,
                ($5::numeric - a.m_pnl)::numeric AS kept_pnl_new,
                CASE
                    WHEN $6::numeric > 0 THEN (($5::numeric - a.m_pnl) / $6::numeric)
                    ELSE ($5::numeric - a.m_pnl)
                END AS kept_obj_new
            FROM agg a
            WHERE ($4::int4 - a.m_trades) >= $7::int4
        )
        SELECT
            analysis_id,
            timeframe,
            bin_name,
            m_trades,
            m_pnl,
            m_wins,
            kept_trades_new,
            kept_pnl_new,
            kept_obj_new
        FROM eval
        ORDER BY kept_obj_new DESC, kept_trades_new DESC, m_trades DESC
        LIMIT 1
        """,
        scenario_id,
        signal_id,
        direction,
        int(kept_trades),
        kept_pnl,
        dep,
        int(min_keep_trades),
    )

    if not row:
        return None

    return dict(row)


# 🔸 Добавить в tmp_v2_removed_pos все позиции, попавшие в конкретный бин (только новые)
async def _add_removed_positions_for_bin(
    conn,
    scenario_id: int,
    signal_id: int,
    direction: str,
    analysis_id: int,
    timeframe: str,
    bin_name: str,
) -> None:
    await conn.execute(
        """
        INSERT INTO tmp_v2_removed_pos (position_uid)
        SELECT DISTINCT p.position_uid
        FROM bt_analysis_positions_raw r
        JOIN bt_scenario_positions p
          ON p.position_uid = r.position_uid
         AND p.scenario_id  = $1
         AND p.signal_id    = $2
         AND p.direction    = $3
         AND p.postproc     = true
        LEFT JOIN tmp_v2_removed_pos tr
          ON tr.position_uid = p.position_uid
        WHERE r.scenario_id = $1
          AND r.signal_id   = $2
          AND r.direction   = $3
          AND r.analysis_id = $4
          AND r.timeframe   = $5
          AND r.bin_name    = $6
          AND tr.position_uid IS NULL
        ON CONFLICT DO NOTHING
        """,
        scenario_id,
        signal_id,
        direction,
        int(analysis_id),
        str(timeframe),
        str(bin_name),
    )


# 🔸 Загрузить выбранные bad-бинны из tmp_v2_selected_bins
async def _load_selected_bins(conn) -> List[Tuple[int, str, str]]:
    rows = await conn.fetch(
        "SELECT analysis_id, timeframe, bin_name FROM tmp_v2_selected_bins ORDER BY timeframe, analysis_id, bin_name"
    )
    out: List[Tuple[int, str, str]] = []
    for r in rows:
        out.append((int(r["analysis_id"]), str(r["timeframe"]), str(r["bin_name"])))
    return out


# 🔸 Чтение orig-статистики по позициям (postproc=true)
async def _load_orig_stats(
    conn,
    scenario_id: int,
    signal_id: int,
    direction: str,
    deposit: Optional[Decimal],
) -> Dict[str, Any]:
    row = await conn.fetchrow(
        """
        SELECT
            COUNT(*)                                           AS trades,
            COALESCE(SUM(pnl_abs), 0)                          AS pnl_abs,
            COUNT(*) FILTER (WHERE pnl_abs > 0)                AS wins
        FROM bt_scenario_positions
        WHERE scenario_id = $1
          AND signal_id   = $2
          AND direction   = $3
          AND postproc    = true
        """,
        scenario_id,
        signal_id,
        direction,
    )

    trades = int(row["trades"] or 0)
    pnl_abs = _safe_decimal(row["pnl_abs"])
    wins = int(row["wins"] or 0)

    if trades > 0:
        winrate = Decimal(wins) / Decimal(trades)
    else:
        winrate = Decimal("0")

    if deposit and deposit > 0:
        roi = _safe_div(pnl_abs, deposit)
    else:
        roi = Decimal("0")

    return {
        "orig_trades": trades,
        "orig_pnl_abs": pnl_abs,
        "orig_wins": wins,
        "orig_winrate": winrate,
        "orig_roi": roi,
    }


# 🔸 Upsert model_opt_v2 и возврат model_id
async def _upsert_model_opt_v2_return_id(
    conn,
    scenario_id: int,
    signal_id: int,
    direction: str,
    best_threshold: Decimal,
    selected_analysis_ids: List[int],
    orig_trades: int,
    orig_pnl_abs: Decimal,
    orig_winrate: Decimal,
    orig_roi: Decimal,
    filt_trades: int,
    filt_pnl_abs: Decimal,
    filt_winrate: Decimal,
    filt_roi: Decimal,
    removed_trades: int,
    removed_accuracy: Decimal,
    meta_obj: Dict[str, Any],
    source_finished_at: datetime,
) -> int:
    meta_json = json.dumps(meta_obj, ensure_ascii=False)
    selected_json = json.dumps(selected_analysis_ids, ensure_ascii=False)

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
        _q_decimal(best_threshold),
        selected_json,
        int(orig_trades),
        _q_decimal(orig_pnl_abs),
        _q_decimal(orig_winrate),
        _q_decimal(orig_roi),
        int(filt_trades),
        _q_decimal(filt_pnl_abs),
        _q_decimal(filt_winrate),
        _q_decimal(filt_roi),
        int(removed_trades),
        _q_decimal(removed_accuracy),
        meta_json,
        source_finished_at,
    )
    return int(row["id"])


# 🔸 Загрузка строк bins_stat для пары и направления (вся таблица для labels)
async def _load_bins_stat_rows(
    conn,
    scenario_id: int,
    signal_id: int,
    direction: str,
) -> List[Dict[str, Any]]:
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


# 🔸 Пересборка bt_analysis_bins_labels_v2: good/bad (inactive не используем в v2-логике)
async def _rebuild_bins_labels_v2(
    conn,
    model_id: int,
    scenario_id: int,
    signal_id: int,
    direction: str,
    threshold_used: Decimal,
    selected_bad_bins: Set[Tuple[int, str, str]],
    bins_rows: List[Dict[str, Any]],
) -> int:
    # сначала удаляем старую разметку по model_id
    await conn.execute("DELETE FROM bt_analysis_bins_labels_v2 WHERE model_id = $1", int(model_id))

    if not bins_rows:
        return 0

    to_insert: List[Tuple[Any, ...]] = []
    for b in bins_rows:
        aid = int(b["analysis_id"])
        tf = str(b["timeframe"])
        bn = str(b["bin_name"])

        key = (aid, tf, bn)
        state = "bad" if key in selected_bad_bins else "good"

        to_insert.append(
            (
                int(model_id),
                scenario_id,
                signal_id,
                direction,
                aid,
                b["indicator_param"],
                tf,
                bn,
                state,
                _q_decimal(threshold_used),
                int(b["trades"]),
                _q_decimal(b["pnl_abs"]),
                _q_decimal(b["winrate"]),
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

    return len(to_insert)


# 🔸 Удаление v2-модели для направления (и каскадно labels_v2)
async def _delete_v2_model_for_direction(conn, scenario_id: int, signal_id: int, direction: str) -> None:
    await conn.execute(
        """
        DELETE FROM bt_analysis_model_opt_v2
        WHERE scenario_id = $1
          AND signal_id   = $2
          AND direction   = $3
        """,
        scenario_id,
        signal_id,
        direction,
    )


# 🔸 Публикация события готовности preproc v2
async def _publish_preproc_v2_ready(
    redis,
    scenario_id: int,
    signal_id: int,
    source_finished_at: datetime,
    direction_mask: Optional[str],
) -> None:
    finished_at = datetime.utcnow()

    try:
        await redis.xadd(
            PREPROC_V2_READY_STREAM_KEY,
            {
                "scenario_id": str(scenario_id),
                "signal_id": str(signal_id),
                "finished_at": finished_at.isoformat(),
                "source_finished_at": source_finished_at.isoformat(),
                "direction_mask": str(direction_mask) if direction_mask is not None else "",
            },
        )
        log.debug(
            "BT_ANALYSIS_PREPROC_V2: опубликовано событие preproc_v2_ready в '%s' для scenario_id=%s, signal_id=%s, finished_at=%s",
            PREPROC_V2_READY_STREAM_KEY,
            scenario_id,
            signal_id,
            finished_at,
        )
    except Exception as e:
        log.error(
            "BT_ANALYSIS_PREPROC_V2: не удалось опубликовать событие в '%s' для scenario_id=%s, signal_id=%s: %s",
            PREPROC_V2_READY_STREAM_KEY,
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

    if m in (
        "both",
        "all",
        "any",
        "long_short",
        "short_long",
        "long+short",
        "short+long",
        "long|short",
        "short|long",
    ):
        return ["long", "short"]

    return ["long", "short"]


# 🔸 Загрузка депозита сценария из bt_scenario_parameters (param_name='deposit')
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


# 🔸 Вспомогательная функция: безопасное приведение к Decimal
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


# 🔸 Вспомогательная функция: безопасное деление Decimal
def _safe_div(a: Decimal, b: Decimal) -> Decimal:
    try:
        if b == 0:
            return Decimal("0")
        return a / b
    except (InvalidOperation, ZeroDivisionError, TypeError):
        return Decimal("0")


# 🔸 Вспомогательная функция: квантизация Decimal до 4 знаков (вниз для предсказуемости)
def _q_decimal(value: Decimal) -> Decimal:
    return _safe_decimal(value).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)