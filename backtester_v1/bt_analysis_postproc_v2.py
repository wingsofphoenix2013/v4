# bt_analysis_postproc_v2.py — постпроцессинг v2: расчёт компонентов и финального score (0..1) по кандидатам анализаторов (run-aware)

import asyncio
import logging
import json
import math
from datetime import datetime, date
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_POSTPROC_V2")

# 🔸 Настройки Decimal
getcontext().prec = 28

# 🔸 Константы стримов
POSTPROC_V2_STREAM_KEY = "bt:analysis:preproc_ready_v2"
POSTPROC_V2_CONSUMER_GROUP = "bt_analysis_postproc_v2"
POSTPROC_V2_CONSUMER_NAME = "bt_analysis_postproc_v2_main"

# 🔸 Стрим готовности постпроцессинга v2
POSTPROC_V2_READY_STREAM_KEY = "bt:analysis:postproc_ready_v2"

# 🔸 Настройки чтения стрима
POSTPROC_V2_STREAM_BATCH_SIZE = 10
POSTPROC_V2_STREAM_BLOCK_MS = 5000

# 🔸 Таблицы v2
PREPROC_V2_TABLE = "bt_analysis_preproc_stat_v2"
SCORES_V2_TABLE = "bt_analysis_scores_v2"

# 🔸 Версия скоринга (ключ в bt_analysis_scores_v2)
SCORE_VERSION = "v1"

# 🔸 Квантизация
Q4 = Decimal("0.0001")

# 🔸 Веса финального quality_raw (profit доминирует)
W_PROFIT = Decimal("0.70")
W_STABILITY = Decimal("0.20")
W_COVERAGE = Decimal("0.07")
W_CONFIDENCE = Decimal("0.03")

# 🔸 Веса внутри stability (drawdown и recovery важнее)
W_STAB_DD = Decimal("0.40")
W_STAB_REC = Decimal("0.30")
W_STAB_STREAK = Decimal("0.20")
W_STAB_NEGDAYS = Decimal("0.10")

# 🔸 Параметры confidence (плавная насыщаемость)
CONF_TRADES_K = Decimal("500")   # чем больше — тем мягче доверие


# 🔸 Квантизация Decimal до 4 знаков
def _q4(value: Decimal) -> Decimal:
    return value.quantize(Q4, rounding=ROUND_DOWN)


# 🔸 Безопасный Decimal
def _d(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return default


# 🔸 Clamp 0..1
def _clamp01(x: Decimal) -> Decimal:
    if x < Decimal("0"):
        return Decimal("0")
    if x > Decimal("1"):
        return Decimal("1")
    return x


# 🔸 MinMax нормировка (0..1) внутри пары
def _minmax_norm(value: Decimal, vmin: Decimal, vmax: Decimal) -> Decimal:
    if vmax <= vmin:
        return Decimal("0.5")
    return _clamp01((value - vmin) / (vmax - vmin))


# 🔸 Safe json.dumps (Decimal/date)
def _json_dumps_safe(payload: Dict[str, Any]) -> str:
    def _default(o: Any) -> Any:
        if isinstance(o, Decimal):
            return str(_q4(o))
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        return str(o)

    return json.dumps(payload, ensure_ascii=False, default=_default)


# 🔸 Публичная точка входа: воркер постпроцессинга v2 (scoring)
async def run_bt_analysis_postproc_v2_orchestrator(pg, redis) -> None:
    log.debug("BT_ANALYSIS_POSTPROC_V2: оркестратор v2 запущен")

    await _ensure_consumer_group(redis)

    while True:
        try:
            messages = await _read_from_stream(redis)
            if not messages:
                continue

            total_msgs = 0
            total_pairs = 0
            total_candidates = 0
            total_upserts = 0
            total_skipped = 0
            total_errors = 0

            for stream_key, entries in messages:
                if stream_key != POSTPROC_V2_STREAM_KEY:
                    continue

                for entry_id, fields in entries:
                    total_msgs += 1

                    ctx = _parse_preproc_ready(fields)
                    if not ctx:
                        await redis.xack(POSTPROC_V2_STREAM_KEY, POSTPROC_V2_CONSUMER_GROUP, entry_id)
                        total_skipped += 1
                        continue

                    scenario_id = ctx["scenario_id"]
                    signal_id = ctx["signal_id"]
                    run_id = ctx["run_id"]
                    finished_at = ctx["finished_at"]

                    total_pairs += 1

                    # грузим кандидатов из preproc_stat_v2
                    try:
                        candidates = await _load_preproc_v2_candidates(pg, run_id, scenario_id, signal_id)
                    except Exception as e:
                        total_errors += 1
                        log.error(
                            "BT_ANALYSIS_POSTPROC_V2: ошибка загрузки %s для scenario_id=%s signal_id=%s run_id=%s: %s",
                            PREPROC_V2_TABLE,
                            scenario_id,
                            signal_id,
                            run_id,
                            e,
                            exc_info=True,
                        )
                        await redis.xack(POSTPROC_V2_STREAM_KEY, POSTPROC_V2_CONSUMER_GROUP, entry_id)
                        continue

                    if not candidates:
                        log.info(
                            "BT_ANALYSIS_POSTPROC_V2: нет кандидатов в %s — scenario_id=%s signal_id=%s run_id=%s",
                            PREPROC_V2_TABLE,
                            scenario_id,
                            signal_id,
                            run_id,
                        )

                        await _publish_postproc_v2_ready(
                            redis=redis,
                            scenario_id=scenario_id,
                            signal_id=signal_id,
                            run_id=run_id,
                            candidates=0,
                            upserts=0,
                            score_version=SCORE_VERSION,
                        )

                        await redis.xack(POSTPROC_V2_STREAM_KEY, POSTPROC_V2_CONSUMER_GROUP, entry_id)
                        continue

                    # считаем компоненты + финальный score
                    try:
                        scored = _score_candidates_in_pair(candidates)
                    except Exception as e:
                        total_errors += 1
                        log.error(
                            "BT_ANALYSIS_POSTPROC_V2: ошибка скоринга кандидатов scenario_id=%s signal_id=%s run_id=%s: %s",
                            scenario_id,
                            signal_id,
                            run_id,
                            e,
                            exc_info=True,
                        )
                        await redis.xack(POSTPROC_V2_STREAM_KEY, POSTPROC_V2_CONSUMER_GROUP, entry_id)
                        continue

                    # запись результатов
                    try:
                        upserts = await _upsert_scores_v2(pg, scored)
                    except Exception as e:
                        total_errors += 1
                        log.error(
                            "BT_ANALYSIS_POSTPROC_V2: ошибка записи %s scenario_id=%s signal_id=%s run_id=%s: %s",
                            SCORES_V2_TABLE,
                            scenario_id,
                            signal_id,
                            run_id,
                            e,
                            exc_info=True,
                        )
                        await redis.xack(POSTPROC_V2_STREAM_KEY, POSTPROC_V2_CONSUMER_GROUP, entry_id)
                        continue

                    total_candidates += len(scored)
                    total_upserts += upserts

                    # winner для лога
                    best = max(scored, key=lambda x: (_d(x.get("score_01")), _d(x.get("filt_pnl_abs"))))
                    winner_analysis_id = int(best.get("analysis_id") or 0)
                    winner_param = str(best.get("indicator_param") or "")

                    log.info(
                        "BT_ANALYSIS_POSTPROC_V2: pair done — scenario_id=%s signal_id=%s run_id=%s finished_at=%s "
                        "candidates=%s upserts=%s winner_analysis_id=%s winner_param='%s' best_score=%s best_filt_pnl=%s",
                        scenario_id,
                        signal_id,
                        run_id,
                        finished_at,
                        len(scored),
                        upserts,
                        winner_analysis_id,
                        winner_param,
                        str(_q4(_d(best.get("score_01")))),
                        str(_q4(_d(best.get("filt_pnl_abs")))),
                    )

                    await _publish_postproc_v2_ready(
                        redis=redis,
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                        run_id=run_id,
                        candidates=len(scored),
                        upserts=upserts,
                        score_version=SCORE_VERSION,
                    )

                    await redis.xack(POSTPROC_V2_STREAM_KEY, POSTPROC_V2_CONSUMER_GROUP, entry_id)

            log.info(
                "BT_ANALYSIS_POSTPROC_V2: batch summary — msgs=%s pairs=%s candidates=%s upserts=%s skipped=%s errors=%s",
                total_msgs,
                total_pairs,
                total_candidates,
                total_upserts,
                total_skipped,
                total_errors,
            )

        except Exception as e:
            log.error("BT_ANALYSIS_POSTPROC_V2: loop error: %s", e, exc_info=True)
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group (Render-safe: SETID '$')
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=POSTPROC_V2_STREAM_KEY,
            groupname=POSTPROC_V2_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_ANALYSIS_POSTPROC_V2: создана consumer group '%s' для стрима '%s'",
            POSTPROC_V2_CONSUMER_GROUP,
            POSTPROC_V2_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.info(
                "BT_ANALYSIS_POSTPROC_V2: consumer group '%s' уже существует — SETID '$' для игнора истории до старта",
                POSTPROC_V2_CONSUMER_GROUP,
            )
            await redis.execute_command(
                "XGROUP",
                "SETID",
                POSTPROC_V2_STREAM_KEY,
                POSTPROC_V2_CONSUMER_GROUP,
                "$",
            )
            log.debug(
                "BT_ANALYSIS_POSTPROC_V2: consumer group '%s' SETID='$' выполнен для '%s'",
                POSTPROC_V2_CONSUMER_GROUP,
                POSTPROC_V2_STREAM_KEY,
            )
        else:
            log.error(
                "BT_ANALYSIS_POSTPROC_V2: ошибка при создании consumer group '%s': %s",
                POSTPROC_V2_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:analysis:preproc_ready_v2 (NOGROUP recovery)
async def _read_from_stream(redis) -> List[Any]:
    try:
        entries = await redis.xreadgroup(
            groupname=POSTPROC_V2_CONSUMER_GROUP,
            consumername=POSTPROC_V2_CONSUMER_NAME,
            streams={POSTPROC_V2_STREAM_KEY: ">"},
            count=POSTPROC_V2_STREAM_BATCH_SIZE,
            block=POSTPROC_V2_STREAM_BLOCK_MS,
        )
    except Exception as e:
        msg = str(e)
        if "NOGROUP" in msg:
            log.warning("BT_ANALYSIS_POSTPROC_V2: NOGROUP при XREADGROUP — переинициализируем группу и продолжаем")
            await _ensure_consumer_group(redis)
            return []
        raise

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


# 🔸 Разбор сообщения bt:analysis:preproc_ready_v2
def _parse_preproc_ready(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id_str = fields.get("scenario_id")
        signal_id_str = fields.get("signal_id")
        run_id_str = fields.get("run_id")
        finished_at_str = fields.get("finished_at")

        if not (scenario_id_str and signal_id_str and run_id_str and finished_at_str):
            return None

        return {
            "scenario_id": int(scenario_id_str),
            "signal_id": int(signal_id_str),
            "run_id": int(run_id_str),
            "finished_at": datetime.fromisoformat(finished_at_str),
        }
    except Exception:
        return None


# 🔸 Загрузка кандидатов из bt_analysis_preproc_stat_v2 по паре run/scenario/signal
async def _load_preproc_v2_candidates(
    pg,
    run_id: int,
    scenario_id: int,
    signal_id: int,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                run_id,
                scenario_id,
                signal_id,
                analysis_id,
                COALESCE(indicator_param, '') AS indicator_param,
                timeframe,
                direction,

                orig_trades,
                orig_pnl_abs,
                filt_trades,
                filt_pnl_abs,

                days_with_trades,
                neg_days,
                max_neg_streak,
                max_drawdown_abs,
                max_recovery_days
            FROM {PREPROC_V2_TABLE}
            WHERE run_id = $1
              AND scenario_id = $2
              AND signal_id = $3
            ORDER BY analysis_id, COALESCE(indicator_param, ''), timeframe, direction
            """,
            int(run_id),
            int(scenario_id),
            int(signal_id),
        )

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "run_id": int(r["run_id"]),
                "scenario_id": int(r["scenario_id"]),
                "signal_id": int(r["signal_id"]),
                "analysis_id": int(r["analysis_id"]),
                "indicator_param": str(r["indicator_param"] or ""),
                "timeframe": str(r["timeframe"]).strip().lower(),
                "direction": str(r["direction"]).strip().lower(),
                "orig_trades": int(r["orig_trades"] or 0),
                "orig_pnl_abs": _d(r["orig_pnl_abs"]),
                "filt_trades": int(r["filt_trades"] or 0),
                "filt_pnl_abs": _d(r["filt_pnl_abs"]),
                "days_with_trades": int(r["days_with_trades"] or 0),
                "neg_days": int(r["neg_days"] or 0),
                "max_neg_streak": int(r["max_neg_streak"] or 0),
                "max_drawdown_abs": _d(r["max_drawdown_abs"]),
                "max_recovery_days": int(r["max_recovery_days"] or 0),
            }
        )
    return out


# 🔸 Скоринг кандидатов внутри одной пары (scenario_id, signal_id, run_id)
def _score_candidates_in_pair(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not candidates:
        return []

    # profit min/max
    profits = [_d(c.get("filt_pnl_abs")) for c in candidates]
    p_min = min(profits)
    p_max = max(profits)

    # coverage list (0..1)
    coverages: List[Decimal] = []
    for c in candidates:
        orig = Decimal(int(c.get("orig_trades") or 0))
        filt = Decimal(int(c.get("filt_trades") or 0))
        cov = (filt / orig) if orig > 0 else Decimal("0")
        coverages.append(_clamp01(cov))

    cov_min = min(coverages) if coverages else Decimal("0")
    cov_max = max(coverages) if coverages else Decimal("1")

    cov_sqrt_min = Decimal(str(math.sqrt(float(cov_min)))) if cov_min > 0 else Decimal("0")
    cov_sqrt_max = Decimal(str(math.sqrt(float(cov_max)))) if cov_max > 0 else Decimal("0")
    cov_sqrt_min = _clamp01(cov_sqrt_min)
    cov_sqrt_max = _clamp01(cov_sqrt_max)

    # stability mins/maxs (lower is better)
    neg_days_list = [Decimal(int(c.get("neg_days") or 0)) for c in candidates]
    streak_list = [Decimal(int(c.get("max_neg_streak") or 0)) for c in candidates]
    dd_list = [_d(c.get("max_drawdown_abs")) for c in candidates]
    rec_list = [Decimal(int(c.get("max_recovery_days") or 0)) for c in candidates]

    nd_min, nd_max = min(neg_days_list), max(neg_days_list)
    st_min, st_max = min(streak_list), max(streak_list)
    dd_min, dd_max = min(dd_list), max(dd_list)
    rc_min, rc_max = min(rec_list), max(rec_list)

    # confidence (по filt_trades, без порогов)
    # confidence = filt_trades / (filt_trades + K)
    conf_list: List[Decimal] = []
    for c in candidates:
        t = Decimal(int(c.get("filt_trades") or 0))
        conf = (t / (t + CONF_TRADES_K)) if t >= 0 else Decimal("0")
        conf_list.append(_clamp01(conf))

    conf_min = min(conf_list) if conf_list else Decimal("0")
    conf_max = max(conf_list) if conf_list else Decimal("1")

    # считаем компоненты и quality_raw
    scored: List[Dict[str, Any]] = []
    for idx, c in enumerate(candidates):
        profit = _d(c.get("filt_pnl_abs"))
        profit_c = _minmax_norm(profit, p_min, p_max)

        cov = coverages[idx]
        # "средний" штраф: sqrt(coverage), затем min-max внутри пары
        cov_sqrt = Decimal(str(math.sqrt(float(cov)))) if cov > 0 else Decimal("0")
        cov_sqrt = _clamp01(cov_sqrt)
        coverage_c = _minmax_norm(cov_sqrt, cov_sqrt_min, cov_sqrt_max)

        # stability sub-scores (1=лучше, 0=хуже), так как ниже лучше
        neg_days = Decimal(int(c.get("neg_days") or 0))
        streak = Decimal(int(c.get("max_neg_streak") or 0))
        dd = _d(c.get("max_drawdown_abs"))
        rec = Decimal(int(c.get("max_recovery_days") or 0))

        neg_c = Decimal("1") - _minmax_norm(neg_days, nd_min, nd_max)
        streak_c = Decimal("1") - _minmax_norm(streak, st_min, st_max)
        dd_c = Decimal("1") - _minmax_norm(dd, dd_min, dd_max)
        rec_c = Decimal("1") - _minmax_norm(rec, rc_min, rc_max)

        stability_c = _clamp01(
            W_STAB_DD * dd_c +
            W_STAB_REC * rec_c +
            W_STAB_STREAK * streak_c +
            W_STAB_NEGDAYS * neg_c
        )

        conf = conf_list[idx]
        confidence_c = _minmax_norm(conf, conf_min, conf_max)

        quality_raw = _clamp01(
            W_PROFIT * profit_c +
            W_STABILITY * stability_c +
            W_COVERAGE * coverage_c +
            W_CONFIDENCE * confidence_c
        )

        row = dict(c)
        row.update(
            {
                "score_version": SCORE_VERSION,
                "profit_component": _q4(profit_c),
                "coverage_component": _q4(coverage_c),
                "stability_component": _q4(stability_c),
                "confidence_component": _q4(confidence_c),
                "quality_raw": _q4(quality_raw),
            }
        )
        scored.append(row)

    # финальная нормировка quality_raw -> score_01 в рамках пары
    q_vals = [_d(x.get("quality_raw")) for x in scored]
    q_min = min(q_vals)
    q_max = max(q_vals)

    for x in scored:
        q = _d(x.get("quality_raw"))
        score_01 = _minmax_norm(q, q_min, q_max)
        x["score_01"] = _q4(score_01)

    # rank: сортировка по score desc, затем по filt_pnl_abs desc
    scored_sorted = sorted(scored, key=lambda x: (_d(x.get("score_01")), _d(x.get("filt_pnl_abs"))), reverse=True)
    for i, x in enumerate(scored_sorted, start=1):
        x["rank"] = int(i)

    # сводная raw_stat (одинаковая по паре)
    pair_stat = {
        "score_version": SCORE_VERSION,
        "weights": {
            "profit": str(W_PROFIT),
            "stability": str(W_STABILITY),
            "coverage": str(W_COVERAGE),
            "confidence": str(W_CONFIDENCE),
        },
        "stability_weights": {
            "drawdown": str(W_STAB_DD),
            "recovery": str(W_STAB_REC),
            "streak": str(W_STAB_STREAK),
            "neg_days": str(W_STAB_NEGDAYS),
        },
        "ranges": {
            "profit": {"min": str(_q4(p_min)), "max": str(_q4(p_max))},
            "coverage_sqrt": {"min": str(_q4(cov_sqrt_min)), "max": str(_q4(cov_sqrt_max))},
            "neg_days": {"min": str(int(nd_min)), "max": str(int(nd_max))},
            "max_neg_streak": {"min": str(int(st_min)), "max": str(int(st_max))},
            "max_drawdown_abs": {"min": str(_q4(dd_min)), "max": str(_q4(dd_max))},
            "max_recovery_days": {"min": str(int(rc_min)), "max": str(int(rc_max))},
            "confidence": {"min": str(_q4(conf_min)), "max": str(_q4(conf_max))},
            "quality_raw": {"min": str(_q4(q_min)), "max": str(_q4(q_max))},
        },
        "confidence_param": {"trades_k": str(CONF_TRADES_K)},
    }

    for x in scored_sorted:
        x["raw_stat"] = pair_stat

    return scored_sorted


# 🔸 Upsert результатов скоринга в bt_analysis_scores_v2
async def _upsert_scores_v2(pg, scored_rows: List[Dict[str, Any]]) -> int:
    if not scored_rows:
        return 0

    to_insert: List[Tuple[Any, ...]] = []
    for r in scored_rows:
        to_insert.append(
            (
                int(r["run_id"]),
                int(r["scenario_id"]),
                int(r["signal_id"]),
                int(r["analysis_id"]),
                str(r.get("indicator_param") or ""),
                str(r.get("timeframe") or ""),
                str(r.get("direction") or ""),

                int(r.get("orig_trades") or 0),
                str(_q4(_d(r.get("orig_pnl_abs")))),
                int(r.get("filt_trades") or 0),
                str(_q4(_d(r.get("filt_pnl_abs")))),

                str(r.get("score_version") or SCORE_VERSION),
                str(_q4(_d(r.get("profit_component")))) if r.get("profit_component") is not None else None,
                str(_q4(_d(r.get("coverage_component")))) if r.get("coverage_component") is not None else None,
                str(_q4(_d(r.get("stability_component")))) if r.get("stability_component") is not None else None,
                str(_q4(_d(r.get("confidence_component")))) if r.get("confidence_component") is not None else None,
                str(_q4(_d(r.get("quality_raw")))) if r.get("quality_raw") is not None else None,
                str(_q4(_d(r.get("score_01")))) if r.get("score_01") is not None else None,
                int(r.get("rank") or 0),

                _json_dumps_safe(r.get("raw_stat") or {}),
            )
        )

    async with pg.acquire() as conn:
        await conn.executemany(
            f"""
            INSERT INTO {SCORES_V2_TABLE} (
                run_id,
                scenario_id,
                signal_id,
                analysis_id,
                indicator_param,
                timeframe,
                direction,

                orig_trades,
                orig_pnl_abs,
                filt_trades,
                filt_pnl_abs,

                score_version,
                profit_component,
                coverage_component,
                stability_component,
                confidence_component,
                quality_raw,
                score_01,
                "rank",

                raw_stat,
                created_at
            )
            VALUES (
                $1, $2, $3, $4,
                $5, $6, $7,

                $8, $9, $10, $11,

                $12,
                $13, $14, $15, $16,
                $17, $18, $19,

                $20::jsonb,
                now()
            )
            ON CONFLICT (run_id, scenario_id, signal_id, analysis_id, indicator_param, timeframe, direction, score_version)
            DO UPDATE SET
                orig_trades          = EXCLUDED.orig_trades,
                orig_pnl_abs         = EXCLUDED.orig_pnl_abs,
                filt_trades          = EXCLUDED.filt_trades,
                filt_pnl_abs         = EXCLUDED.filt_pnl_abs,

                profit_component     = EXCLUDED.profit_component,
                coverage_component   = EXCLUDED.coverage_component,
                stability_component  = EXCLUDED.stability_component,
                confidence_component = EXCLUDED.confidence_component,
                quality_raw          = EXCLUDED.quality_raw,
                score_01             = EXCLUDED.score_01,
                "rank"               = EXCLUDED."rank",

                raw_stat             = EXCLUDED.raw_stat,
                updated_at           = now()
            """,
            to_insert,
        )

    return len(to_insert)


# 🔸 Публикация события готовности postproc v2
async def _publish_postproc_v2_ready(
    redis,
    scenario_id: int,
    signal_id: int,
    run_id: int,
    candidates: int,
    upserts: int,
    score_version: str,
) -> None:
    finished_at = datetime.utcnow()

    try:
        await redis.xadd(
            POSTPROC_V2_READY_STREAM_KEY,
            {
                "scenario_id": str(int(scenario_id)),
                "signal_id": str(int(signal_id)),
                "run_id": str(int(run_id)),
                "candidates": str(int(candidates)),
                "upserts": str(int(upserts)),
                "score_version": str(score_version),
                "finished_at": finished_at.isoformat(),
            },
        )
        log.debug(
            "BT_ANALYSIS_POSTPROC_V2: published %s scenario_id=%s signal_id=%s run_id=%s candidates=%s upserts=%s score_version=%s",
            POSTPROC_V2_READY_STREAM_KEY,
            scenario_id,
            signal_id,
            run_id,
            candidates,
            upserts,
            score_version,
        )
    except Exception as e:
        log.error(
            "BT_ANALYSIS_POSTPROC_V2: не удалось опубликовать событие в '%s' scenario_id=%s signal_id=%s run_id=%s: %s",
            POSTPROC_V2_READY_STREAM_KEY,
            scenario_id,
            signal_id,
            run_id,
            e,
            exc_info=True,
        )