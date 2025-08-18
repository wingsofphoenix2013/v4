# indicator_auditor.py — аудит целостности indicator_values_v4 и фиксация «дыр» в indicator_gap_v4

import asyncio
import logging
from datetime import datetime, timedelta

from indicators.compute_and_store import get_expected_param_names

log = logging.getLogger("IND_AUDITOR")

# 🔸 Шаги таймфреймов (в минутах)
STEP_MIN = {"m5": 5, "m15": 15, "h1": 60}

# 🔸 Выравнивание времени по шагу
def align_start(ts, step_min: int) -> datetime:
    ts = ts.replace(second=0, microsecond=0)
    rem = ts.minute % step_min
    if rem:
        ts -= timedelta(minutes=rem)
    return ts

# 🔸 Выравнивание времени вперёд (ceil) по шагу таймфрейма
def align_forward(ts: datetime, step_min: int) -> datetime:
    """
    Округляет время вперёд к ближайшей границе шага.
    Если ts уже попадает на границу — возвращает его же.
    """
    ts = ts.replace(second=0, microsecond=0)
    rem = ts.minute % step_min
    if rem == 0:
        return ts
    return ts - timedelta(minutes=rem) + timedelta(minutes=step_min)

# 🔸 Активные инстансы по ТФ с параметрами и enabled_at
async def fetch_enabled_instances_for_tf(pg, timeframe: str):
    async with pg.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, indicator, timeframe, stream_publish, enabled_at
            FROM indicator_instances_v4
            WHERE enabled = true AND timeframe = $1
        """, timeframe)

        result = []
        for r in rows:
            params = await conn.fetch("""
                SELECT param, value
                FROM indicator_parameters_v4
                WHERE instance_id = $1
            """, r["id"])
            param_map = {p["param"]: p["value"] for p in params}
            result.append({
                "id": r["id"],
                "indicator": r["indicator"],
                "timeframe": r["timeframe"],
                "enabled_at": r["enabled_at"],  # timestamp | None
                "params": param_map,
            })
        return result

# 🔸 Уже записанные в БД параметры по окну
async def existing_params_in_db(pg, instance_id: int, symbol: str, start_ts: datetime, end_ts: datetime):
    async with pg.acquire() as conn:
        rows = await conn.fetch("""
            SELECT open_time, param_name
            FROM indicator_values_v4
            WHERE instance_id = $1
              AND symbol = $2
              AND open_time BETWEEN $3 AND $4
        """, instance_id, symbol, start_ts, end_ts)

    by_time = {}
    for r in rows:
        by_time.setdefault(r["open_time"], set()).add(r["param_name"])
    return by_time

# 🔸 Массовая фиксация «дыр» в indicator_gap_v4
async def insert_gaps(pg, gaps):
    """
    gaps: iterable[(instance_id, symbol, open_time, param_name)]
    """
    if not gaps:
        return 0
    async with pg.acquire() as conn:
        await conn.executemany("""
            INSERT INTO indicator_gap_v4 (instance_id, symbol, open_time, param_name, status)
            VALUES ($1, $2, $3, $4, 'found')
            ON CONFLICT (instance_id, symbol, open_time, param_name) DO NOTHING
        """, gaps)
    return len(gaps)

# 🔸 Основной воркер аудитора
async def run_indicator_auditor(pg, redis, window_hours: int = 12):
    log.info("Аудитор индикаторов запущен (iv4_inserted)")

    stream = "iv4_inserted"
    group = "ind_audit_group"
    consumer = "ind_audit_1"

    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create error: {e}")

    while True:
        try:
            resp = await redis.xreadgroup(group, consumer, streams={stream: ">"}, count=50, block=2000)
            if not resp:
                continue

            to_ack = []
            latest = {}  # (symbol, interval) -> max(open_time)

            # 🔸 Сбор последних open_time на пару (symbol, interval)
            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)
                    try:
                        symbol = data.get("symbol")
                        interval = data.get("interval")
                        open_time_iso = data.get("open_time")
                        if not symbol or interval not in STEP_MIN or not open_time_iso:
                            continue
                        end_dt = datetime.fromisoformat(open_time_iso)
                        key = (symbol, interval)
                        if key not in latest or end_dt > latest[key]:
                            latest[key] = end_dt
                    except Exception as e:
                        log.warning(f"parse iv4_inserted error: {e}")

            # 🔸 Обработка агрегированных ключей
            for (symbol, interval), end_dt in latest.items():
                step_min = STEP_MIN[interval]
                step = timedelta(minutes=step_min)

                # выравниваем полученное время и сдвигаем НАЗАД на один бар — исключаем текущий
                end_dt = end_dt.replace(second=0, microsecond=0)
                audit_end = end_dt - step

                # окно: последние window_hours до audit_end
                start_dt = align_start(audit_end - timedelta(hours=window_hours), step_min)

                instances = await fetch_enabled_instances_for_tf(pg, interval)
                if not instances:
                    log.debug(f"[{symbol}] [{interval}] нет активных инстансов для аудита")
                    continue

                total_found = 0

                for inst in instances:
                    iid = inst["id"]
                    indicator = inst["indicator"]
                    params = inst["params"]
                    enabled_at = inst["enabled_at"]

                    # 🔸 Учитываем enabled_at: старт не раньше момента включения и выровнен ВПЕРЁД по сетке TF
                    eff_start = start_dt
                    if enabled_at:
                        cand = max(start_dt, enabled_at.replace(tzinfo=None))
                        eff_start = align_forward(cand, step_min)

                    # если окно выродилось (активация позже audit_end) — пропускаем
                    if eff_start > audit_end:
                        continue

                    # 🔸 Генерация сетки open_time [eff_start .. audit_end]
                    times = []
                    t = eff_start
                    while t <= audit_end:
                        times.append(t)
                        t += step
                    if not times:
                        continue

                    # 🔸 Ожидаемые param_name для индикатора
                    expected = set(get_expected_param_names(indicator, params))

                    # 🔸 Что уже есть в БД
                    have = await existing_params_in_db(pg, iid, symbol, eff_start, audit_end)

                    # 🔸 Вычисление пропусков
                    gaps = []
                    for ot in times:
                        present = have.get(ot, set())
                        missing = expected - present
                        if missing:
                            for pname in missing:
                                gaps.append((iid, symbol, ot, pname))

                    # 🔸 Фиксация пропусков
                    if gaps:
                        inserted = await insert_gaps(pg, gaps)
                        total_found += inserted

                log.debug(f"[AUDIT] {symbol}/{interval} окно {start_dt}..{audit_end} — добавлено пропусков: {total_found}")

            if to_ack:
                await redis.xack(stream, group, *to_ack)

        except Exception as e:
            log.error(f"IND_AUDITOR loop error: {e}", exc_info=True)
            await asyncio.sleep(2)