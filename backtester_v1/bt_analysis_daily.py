# bt_analysis_daily.py — суточная аналитика работы анализаторов (v1/v2) по дням

import asyncio
import logging
from datetime import datetime, date
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Кеши backtester_v1
from backtester_config import get_analysis_instance, get_scenario_instance

# 🔸 Утилиты анализа фич
from bt_analysis_utils import resolve_feature_name

# 🔸 Настройки Decimal
getcontext().prec = 28

log = logging.getLogger("BT_ANALYSIS_DAILY")

# 🔸 Константы стримов
ANALYSIS_POSTPROC_READY_STREAM_KEY = "bt:analysis:postproc:ready"
DAILY_READY_STREAM_KEY = "bt:analysis:daily:ready"
ANALYSIS_DAILY_CONSUMER_GROUP = "bt_analysis_daily"
ANALYSIS_DAILY_CONSUMER_NAME = "bt_analysis_daily_main"

# 🔸 Настройки чтения стрима bt:analysis:postproc:ready
ANALYSIS_DAILY_STREAM_BATCH_SIZE = 10
ANALYSIS_DAILY_STREAM_BLOCK_MS = 5000

# 🔸 Параметры отбора бинов (должны совпадать с bt_analysis_postproc)
MIN_COVERAGE = Decimal("0.30")              # минимальная доля сделок (20% от базовых)
MIN_WINRATE_IMPROVEMENT = Decimal("0.05")   # минимальное улучшение winrate (1%)

# 🔸 Поддерживаемые семейства анализаторов
SUPPORTED_FAMILIES = {"rsi", "adx", "ema", "atr", "supertrend"}

# 🔸 Квантование до 4 знаков
def _q4(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


# 🔸 Безопасное деление
def _safe_div(n: Decimal, d: Decimal) -> Decimal:
    if d == 0:
        return Decimal("0")
    return n / d


# 🔸 Проверка/создание consumer group для стрима bt:analysis:postproc:ready
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=ANALYSIS_POSTPROC_READY_STREAM_KEY,
            groupname=ANALYSIS_DAILY_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_ANALYSIS_DAILY: создана consumer group '%s' для стрима '%s'",
            ANALYSIS_DAILY_CONSUMER_GROUP,
            ANALYSIS_POSTPROC_READY_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.info(
                "BT_ANALYSIS_DAILY: consumer group '%s' для стрима '%s' уже существует",
                ANALYSIS_DAILY_CONSUMER_GROUP,
                ANALYSIS_POSTPROC_READY_STREAM_KEY,
            )
        else:
            log.error(
                "BT_ANALYSIS_DAILY: ошибка при создании consumer group '%s': %s",
                ANALYSIS_DAILY_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:analysis:postproc:ready
async def _read_from_stream(redis) -> List[Any]:
    entries = await redis.xreadgroup(
        groupname=ANALYSIS_DAILY_CONSUMER_GROUP,
        consumername=ANALYSIS_DAILY_CONSUMER_NAME,
        streams={ANALYSIS_POSTPROC_READY_STREAM_KEY: ">"},
        count=ANALYSIS_DAILY_STREAM_BATCH_SIZE,
        block=ANALYSIS_DAILY_STREAM_BLOCK_MS,
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


# 🔸 Разбор одного сообщения из стрима bt:analysis:postproc:ready
def _parse_postproc_message(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id_str = fields.get("scenario_id")
        signal_id_str = fields.get("signal_id")
        family_key = fields.get("family_key")
        analysis_ids_str = fields.get("analysis_ids") or ""
        version = fields.get("version") or "v1"
        finished_at_str = fields.get("finished_at")
        stats_written_str = fields.get("stats_written") or "0"

        if not (scenario_id_str and signal_id_str and family_key and finished_at_str):
            return None

        scenario_id = int(scenario_id_str)
        signal_id = int(signal_id_str)
        finished_at = datetime.fromisoformat(finished_at_str)

        # парсим список id анализаторов из строки через запятую
        raw_ids = [s.strip() for s in analysis_ids_str.split(",") if s.strip()]
        analysis_ids: List[int] = []
        for s in raw_ids:
            try:
                analysis_ids.append(int(s))
            except Exception:
                continue

        try:
            stats_written = int(stats_written_str)
        except Exception:
            stats_written = 0

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "family_key": family_key,
            "analysis_ids": analysis_ids,
            "version": version,
            "stats_written": stats_written,
            "finished_at": finished_at,
        }
    except Exception as e:
        log.error(
            "BT_ANALYSIS_DAILY: ошибка разбора сообщения стрима bt:analysis:postproc:ready: %s, fields=%s",
            e,
            fields,
            exc_info=True,
        )
        return None


# 🔸 Проверка попадания значения feature_value в выбранные интервалы бинов (для v2)
def _value_in_selected_bins(
    value: float,
    bins: List[Tuple[Optional[float], Optional[float]]],
) -> bool:
    for b_from, b_to in bins:
        if b_from is not None and value < b_from:
            continue
        if b_to is not None and value > b_to:
            continue
        return True
    return False

# 🔸 Суточный анализ одного семейства анализаторов для пары scenario_id/signal_id и версии
async def _process_analysis_family_daily(
    pg,
    scenario_id: int,
    signal_id: int,
    family_key: str,
    analysis_ids: List[int],
    version: str,
) -> int:
    rows_written_total = 0

    # загружаем сценарий, чтобы взять deposit
    scenario = get_scenario_instance(scenario_id)
    deposit: Optional[Decimal] = None

    if scenario:
        params = scenario.get("params") or {}
        deposit_cfg = params.get("deposit")
        if deposit_cfg is not None:
            try:
                deposit = Decimal(str(deposit_cfg.get("value")))
            except Exception:
                deposit = None

    if deposit is None or deposit <= 0:
        log.error(
            "BT_ANALYSIS_DAILY: не удалось получить корректный deposit для scenario_id=%s, "
            "ROI_selected будет рассчитан как 0",
            scenario_id,
        )
        deposit = None

    # загружаем суточную базовую статистику по сценарию+сигналу
    async with pg.acquire() as conn:
        daily_rows = await conn.fetch(
            """
            SELECT day,
                   direction,
                   trades,
                   pnl_abs,
                   winrate,
                   roi
            FROM bt_scenario_daily
            WHERE scenario_id = $1
              AND signal_id   = $2
            """,
            scenario_id,
            signal_id,
        )

    if not daily_rows:
        log.debug(
            "BT_ANALYSIS_DAILY: нет записей в bt_scenario_daily для scenario_id=%s, signal_id=%s",
            scenario_id,
            signal_id,
        )
        return 0

    # базовая статистика по дням и направлениям
    base_daily_by_dir_day: Dict[Tuple[str, date], Dict[str, Any]] = {}
    for r in daily_rows:
        direction = r["direction"]
        d = r["day"]
        if direction is None or d is None:
            continue

        base_daily_by_dir_day[(direction, d)] = {
            "trades": int(r["trades"]),
            "pnl_abs": Decimal(str(r["pnl_abs"])),
            "winrate": Decimal(str(r["winrate"])),
            "roi": Decimal(str(r["roi"])),
        }

    if not base_daily_by_dir_day:
        return 0

    # загружаем базовую статистику по сценарию+сигналу для всех направлений
    async with pg.acquire() as conn:
        base_rows_overall = await conn.fetch(
            """
            SELECT direction, trades, winrate
            FROM bt_scenario_stat
            WHERE scenario_id = $1
              AND signal_id   = $2
            """,
            scenario_id,
            signal_id,
        )

    if not base_rows_overall:
        log.debug(
            "BT_ANALYSIS_DAILY: нет базовой статистики в bt_scenario_stat для scenario_id=%s, signal_id=%s",
            scenario_id,
            signal_id,
        )
        return 0

    base_overall_by_dir: Dict[str, Dict[str, Any]] = {}
    for r in base_rows_overall:
        direction = r["direction"]
        if direction is None:
            continue
        base_overall_by_dir[direction] = {
            "trades": int(r["trades"]),
            "winrate": Decimal(str(r["winrate"])),
        }

    if not base_overall_by_dir:
        return 0

    # очищаем старые записи для этой связки/версии и этих анализаторов
    async with pg.acquire() as conn:
        await conn.execute(
            """
            DELETE FROM bt_analysis_daily
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND analysis_id = ANY($3::int[])
              AND version     = $4
            """,
            scenario_id,
            signal_id,
            analysis_ids,
            version,
        )

    # обрабатываем каждый анализатор отдельно
    for aid in analysis_ids:
        inst = get_analysis_instance(aid)
        if not inst:
            log.warning(
                "BT_ANALYSIS_DAILY: analysis_id=%s не найден в кеше, scenario_id=%s, signal_id=%s",
                aid,
                scenario_id,
                signal_id,
            )
            continue

        inst_family = inst.get("family_key")
        key = inst.get("key")
        params = inst.get("params") or {}

        if inst_family != family_key:
            continue

        tf_cfg = params.get("timeframe")
        source_cfg = params.get("source_key")

        timeframe = str(tf_cfg.get("value")).strip() if tf_cfg is not None else "m5"
        source_key = str(source_cfg.get("value")).strip() if source_cfg is not None else "rsi14"

        feature_name = resolve_feature_name(
            family_key=family_key,
            key=key,
            timeframe=timeframe,
            source_key=source_key,
        )

        log.debug(
            "BT_ANALYSIS_DAILY: старт суточного анализа для analysis_id=%s, family=%s, key=%s, "
            "feature_name=%s, timeframe=%s, version=%s, scenario_id=%s, signal_id=%s",
            aid,
            family_key,
            key,
            feature_name,
            timeframe,
            version,
            scenario_id,
            signal_id,
        )

        # загружаем сырые фичи по позициям для этого анализатора (общие по направлениям)
        async with pg.acquire() as conn:
            raw_rows = await conn.fetch(
                """
                SELECT
                    r.position_id,
                    r.direction,
                    r.feature_value,
                    r.bin_label,
                    r.pnl_abs,
                    r.is_win,
                    p.entry_time::date AS day
                FROM bt_position_features_raw r
                JOIN bt_scenario_positions p
                  ON p.id = r.position_id
                WHERE r.scenario_id  = $1
                  AND r.signal_id    = $2
                  AND r.family_key   = $3
                  AND r.key          = $4
                  AND r.feature_name = $5
                  AND r.timeframe    = $6
                """,
                scenario_id,
                signal_id,
                family_key,
                key,
                feature_name,
                timeframe,
            )

        if not raw_rows:
            log.debug(
                "BT_ANALYSIS_DAILY: нет сырых значений фич для analysis_id=%s, feature_name=%s, "
                "scenario_id=%s, signal_id=%s, timeframe=%s, version=%s",
                aid,
                feature_name,
                scenario_id,
                signal_id,
                timeframe,
                version,
            )
            continue

        # по направлениям считаем отдельно
        for direction in ("long", "short"):
            base_overall = base_overall_by_dir.get(direction)
            if not base_overall:
                continue

            base_trades_overall = int(base_overall["trades"])
            base_winrate_overall = base_overall["winrate"]

            if base_trades_overall <= 0:
                continue

            # загружаем все бины для этой фичи/TF/направления/версии
            async with pg.acquire() as conn:
                bin_rows = await conn.fetch(
                    """
                    SELECT bin_from, bin_to, trades, wins, winrate, bin_label
                    FROM bt_scenario_feature_bins
                    WHERE scenario_id  = $1
                      AND signal_id    = $2
                      AND direction    = $3
                      AND timeframe    = $4
                      AND feature_name = $5
                      AND version      = $6
                    """,
                    scenario_id,
                    signal_id,
                    direction,
                    timeframe,
                    feature_name,
                    version,
                )

            if not bin_rows:
                log.debug(
                    "BT_ANALYSIS_DAILY: нет бинов фич для scenario_id=%s, signal_id=%s, analysis_id=%s, "
                    "direction=%s, timeframe=%s, feature_name=%s, version=%s",
                    scenario_id,
                    signal_id,
                    aid,
                    direction,
                    timeframe,
                    feature_name,
                    version,
                )
                continue

            # определяем "хорошие" бины по глобальной базовой линии (для v1 и v2 одинаково)
            selected_ranges: List[Tuple[Optional[float], Optional[float]]] = []
            selected_trades_overall = 0

            for r in bin_rows:
                bin_trades = int(r["trades"])
                bin_wins = int(r["wins"])
                bin_winrate = Decimal(str(r["winrate"]))

                if bin_trades <= 0:
                    continue

                # улучшение winrate как минимум на MIN_WINRATE_IMPROVEMENT
                if bin_winrate < base_winrate_overall + MIN_WINRATE_IMPROVEMENT:
                    continue

                selected_trades_overall += bin_trades

                bin_from_val = r["bin_from"]
                bin_to_val = r["bin_to"]

                try:
                    b_from = float(bin_from_val) if bin_from_val is not None else None
                except Exception:
                    b_from = None

                try:
                    b_to = float(bin_to_val) if bin_to_val is not None else None
                except Exception:
                    b_to = None

                selected_ranges.append((b_from, b_to))

            if selected_trades_overall <= 0 or not selected_ranges:
                continue

            coverage_overall = _safe_div(Decimal(selected_trades_overall), Decimal(base_trades_overall))
            if coverage_overall < MIN_COVERAGE:
                log.debug(
                    "BT_ANALYSIS_DAILY: analysis_id=%s, feature=%s, direction=%s, version=%s — "
                    "глобальное coverage=%.4f < %.4f (selected_trades_overall=%s, base_trades_overall=%s)",
                    aid,
                    feature_name,
                    direction,
                    version,
                    float(coverage_overall),
                    float(MIN_COVERAGE),
                    selected_trades_overall,
                    base_trades_overall,
                )
                continue

            # агрегаты по дням для выбранных позиций
            selected_by_day: Dict[date, Dict[str, Any]] = {}

            for r in raw_rows:
                dir_raw = r["direction"]
                if dir_raw != direction:
                    continue

                day_val: date = r["day"]
                if day_val is None:
                    continue

                base_daily = base_daily_by_dir_day.get((direction, day_val))
                if not base_daily:
                    continue

                fv_raw = r["feature_value"]
                try:
                    fv = float(fv_raw)
                except Exception:
                    continue

                # выбор по числовым интервалам (для v1 и v2 одинаково)
                if not _value_in_selected_bins(fv, selected_ranges):
                    continue

                pnl_abs_raw = r["pnl_abs"]
                try:
                    pnl_abs = Decimal(str(pnl_abs_raw))
                except Exception:
                    continue

                is_win = bool(r["is_win"])

                day_bucket = selected_by_day.setdefault(
                    day_val,
                    {
                        "trades": 0,
                        "wins": 0,
                        "pnl_abs": Decimal("0"),
                    },
                )

                day_bucket["trades"] += 1
                if is_win:
                    day_bucket["wins"] += 1
                day_bucket["pnl_abs"] += pnl_abs

            # готовим строки для вставки в bt_analysis_daily
            rows_to_insert: List[Tuple[Any, ...]] = []

            for (dir_key, d), base_daily in base_daily_by_dir_day.items():
                if dir_key != direction:
                    continue

                base_trades_day = int(base_daily["trades"])
                if base_trades_day <= 0:
                    continue

                base_pnl_abs_day = base_daily["pnl_abs"]
                base_winrate_day = base_daily["winrate"]
                base_roi_day = base_daily["roi"]

                sel_daily = selected_by_day.get(d)
                if sel_daily:
                    selected_trades_day = int(sel_daily["trades"])
                    selected_wins_day = int(sel_daily["wins"])
                    selected_pnl_abs_day = sel_daily["pnl_abs"]
                else:
                    selected_trades_day = 0
                    selected_wins_day = 0
                    selected_pnl_abs_day = Decimal("0")

                if selected_trades_day > 0:
                    selected_winrate_day = _safe_div(
                        Decimal(selected_wins_day),
                        Decimal(selected_trades_day),
                    )
                else:
                    selected_winrate_day = Decimal("0")

                if deposit is not None and deposit != 0:
                    selected_roi_day = _safe_div(selected_pnl_abs_day, deposit)
                else:
                    selected_roi_day = Decimal("0")

                coverage_day = _safe_div(Decimal(selected_trades_day), Decimal(base_trades_day))

                rows_to_insert.append(
                    (
                        scenario_id,             # scenario_id
                        signal_id,               # signal_id
                        aid,                     # analysis_id
                        family_key,              # family_key
                        key,                     # key
                        direction,               # direction
                        timeframe,               # timeframe
                        version,                 # version
                        d,                       # day
                        base_trades_day,         # base_trades
                        _q4(base_winrate_day),   # base_winrate
                        _q4(base_roi_day),       # base_roi
                        _q4(base_pnl_abs_day),   # base_pnl_abs
                        selected_trades_day,     # selected_trades
                        _q4(selected_winrate_day),  # selected_winrate
                        _q4(selected_roi_day),      # selected_roi
                        _q4(selected_pnl_abs_day),  # selected_pnl_abs
                        _q4(coverage_day),       # coverage
                        None,                    # raw_stat
                    )
                )

            if not rows_to_insert:
                log.info(
                    "BT_ANALYSIS_DAILY: для analysis_id=%s, direction=%s, timeframe=%s, version=%s "
                    "нет данных для записи в bt_analysis_daily",
                    aid,
                    direction,
                    timeframe,
                    version,
                )
                continue

            async with pg.acquire() as conn:
                await conn.executemany(
                    """
                    INSERT INTO bt_analysis_daily (
                        scenario_id,
                        signal_id,
                        analysis_id,
                        family_key,
                        key,
                        direction,
                        timeframe,
                        version,
                        day,
                        base_trades,
                        base_winrate,
                        base_roi,
                        base_pnl_abs,
                        selected_trades,
                        selected_winrate,
                        selected_roi,
                        selected_pnl_abs,
                        coverage,
                        raw_stat,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        $1, $2, $3, $4, $5,
                        $6, $7, $8, $9,
                        $10, $11, $12, $13, $14,
                        $15, $16, $17, $18,
                        $19, now(), NULL
                    )
                    """,
                    rows_to_insert,
                )

            rows_written = len(rows_to_insert)
            rows_written_total += rows_written

            log.debug(
                "BT_ANALYSIS_DAILY: записано строк в bt_analysis_daily=%s для "
                "scenario_id=%s, signal_id=%s, analysis_id=%s, direction=%s, timeframe=%s, version=%s",
                rows_written,
                scenario_id,
                signal_id,
                aid,
                direction,
                timeframe,
                version,
            )

    return rows_written_total

# 🔸 Публичная точка входа: воркер суточного анализа анализаторов
async def run_bt_analysis_daily(pg, redis):
    log.info("BT_ANALYSIS_DAILY: воркер суточного анализа запущен")

    # подготавливаем consumer group для стрима bt:analysis:postproc:ready
    await _ensure_consumer_group(redis)

    # основной цикл чтения стрима и обработки
    while True:
        try:
            messages = await _read_from_stream(redis)

            if not messages:
                continue

            total_msgs = 0
            total_pairs = 0
            total_rows_written = 0

            for stream_key, entries in messages:
                if stream_key != ANALYSIS_POSTPROC_READY_STREAM_KEY:
                    continue

                for entry_id, fields in entries:
                    total_msgs += 1

                    ctx = _parse_postproc_message(fields)
                    if not ctx:
                        await redis.xack(ANALYSIS_POSTPROC_READY_STREAM_KEY, ANALYSIS_DAILY_CONSUMER_GROUP, entry_id)
                        continue

                    scenario_id = ctx["scenario_id"]
                    signal_id = ctx["signal_id"]
                    family_key = ctx["family_key"]
                    analysis_ids = ctx["analysis_ids"]
                    version = ctx["version"]

                    log.info(
                        "BT_ANALYSIS_DAILY: получено сообщение о завершении пост-анализа "
                        "scenario_id=%s, signal_id=%s, family=%s, version=%s, analysis_ids=%s, stream_id=%s",
                        scenario_id,
                        signal_id,
                        family_key,
                        version,
                        analysis_ids,
                        entry_id,
                    )

                    # работаем только с поддерживаемыми семействами
                    if family_key not in SUPPORTED_FAMILIES:
                        log.debug(
                            "BT_ANALYSIS_DAILY: family_key=%s пока не поддерживается, "
                            "scenario_id=%s, signal_id=%s",
                            family_key,
                            scenario_id,
                            signal_id,
                        )
                        await redis.xack(ANALYSIS_POSTPROC_READY_STREAM_KEY, ANALYSIS_DAILY_CONSUMER_GROUP, entry_id)
                        continue

                    if not analysis_ids:
                        log.debug(
                            "BT_ANALYSIS_DAILY: для scenario_id=%s, signal_id=%s, family=%s нет analysis_ids",
                            scenario_id,
                            signal_id,
                            family_key,
                        )
                        await redis.xack(ANALYSIS_POSTPROC_READY_STREAM_KEY, ANALYSIS_DAILY_CONSUMER_GROUP, entry_id)
                        continue

                    # 🔸 Ключевое изменение:
                    # daily запускаем ТОЛЬКО на v2-сообщении, и по нему считаем и v1, и v2
                    if version != "v2":
                        log.debug(
                            "BT_ANALYSIS_DAILY: пропускаем сообщение version=%s "
                            "(ожидаем 'v2' для запуска суточного анализа), scenario_id=%s, signal_id=%s",
                            version,
                            scenario_id,
                            signal_id,
                        )
                        await redis.xack(ANALYSIS_POSTPROC_READY_STREAM_KEY, ANALYSIS_DAILY_CONSUMER_GROUP, entry_id)
                        continue

                    # сначала считаем daily для v1
                    rows_v1 = await _process_analysis_family_daily(
                        pg=pg,
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                        family_key=family_key,
                        analysis_ids=analysis_ids,
                        version="v1",
                    )

                    # затем для v2
                    rows_v2 = await _process_analysis_family_daily(
                        pg=pg,
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                        family_key=family_key,
                        analysis_ids=analysis_ids,
                        version="v2",
                    )

                    rows_written = rows_v1 + rows_v2
                    total_pairs += 1
                    total_rows_written += rows_written

                    # 🔸 Публикуем событие о готовности суточной аналитики в bt:analysis:daily:ready
                    finished_at_daily = datetime.utcnow()
                    try:
                        await redis.xadd(
                            DAILY_READY_STREAM_KEY,
                            {
                                "scenario_id": str(scenario_id),
                                "signal_id": str(signal_id),
                                "family_key": str(family_key),
                                "analysis_ids": ",".join(str(a) for a in analysis_ids),
                                "rows_v1": str(rows_v1),
                                "rows_v2": str(rows_v2),
                                "rows_total": str(rows_written),
                                "finished_at": finished_at_daily.isoformat(),
                            },
                        )
                        log.info(
                            "BT_ANALYSIS_DAILY: опубликовано событие готовности daily-аналитики в стрим '%s' "
                            "для scenario_id=%s, signal_id=%s, family=%s, analysis_ids=%s, "
                            "rows_v1=%s, rows_v2=%s, rows_total=%s, finished_at=%s",
                            DAILY_READY_STREAM_KEY,
                            scenario_id,
                            signal_id,
                            family_key,
                            analysis_ids,
                            rows_v1,
                            rows_v2,
                            rows_written,
                            finished_at_daily,
                        )
                    except Exception as e:
                        log.error(
                            "BT_ANALYSIS_DAILY: не удалось опубликовать событие в стрим '%s' "
                            "для scenario_id=%s, signal_id=%s, family=%s: %s",
                            DAILY_READY_STREAM_KEY,
                            scenario_id,
                            signal_id,
                            family_key,
                            e,
                            exc_info=True,
                        )

                    # помечаем сообщение как обработанное
                    await redis.xack(ANALYSIS_POSTPROC_READY_STREAM_KEY, ANALYSIS_DAILY_CONSUMER_GROUP, entry_id)

                    log.info(
                        "BT_ANALYSIS_DAILY: сообщение stream_id=%s для scenario_id=%s, signal_id=%s, version=%s "
                        "обработано, строк_в_bt_analysis_daily=%s (v1=%s, v2=%s)",
                        entry_id,
                        scenario_id,
                        signal_id,
                        version,
                        rows_written,
                        rows_v1,
                        rows_v2,
                    )

            log.info(
                "BT_ANALYSIS_DAILY: пакет сообщений обработан — сообщений=%s, пар_сценарий_сигнал=%s, "
                "строк_в_bt_analysis_daily=%s",
                total_msgs,
                total_pairs,
                total_rows_written,
            )

        except Exception as e:
            log.error(
                "BT_ANALYSIS_DAILY: ошибка в основном цикле воркера: %s",
                e,
                exc_info=True,
            )
            await asyncio.sleep(2)