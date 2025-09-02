# lab_runner_adx.py — авто-раннер ADX: каждые 6 часов создаёт run’ы для активных ADX-инстансов и прогоняет closed позиции

import asyncio
import logging
from decimal import Decimal

from laboratory_v4_config import (
    LAB_BATCH_SIZE,      # используем 1000 по твоей настройке
)

log = logging.getLogger("LAB_RUNNER_ADX")

# 🔸 Константы/параметры цикла
START_DELAY_SEC   = 120          # стартовая задержка
SLEEP_BETWEEN_RUN = 6 * 3600     # 6 часов
BATCH_SIZE        = 1000         # подтверждено
MAX_CONCURRENCY   = 8            # подтверждено

# 🔸 Вспомогалки
def _adx_len(tf: str) -> int:
    return 14 if tf in ("m5", "m15") else 28

def _bin_adx(val: float) -> int | None:
    try:
        v = float(val)
        if v < 0: v = 0.0
        if v > 100.0: v = 100.0
        b = int(v // 5) * 5
        if b == 100: b = 95
        return b
    except Exception:
        return None

# 🔸 Загрузка активных ADX-инстансов и их параметров
async def load_active_adx_instances(pg):
    async with pg.acquire() as conn:
        rows = await conn.fetch("""
            SELECT i.id, i.min_trade_type, i.min_trade_value, i.min_winrate
            FROM laboratory_instances_v4 i
            WHERE i.active = true
              AND EXISTS (
                SELECT 1 FROM laboratory_parameters_v4 p
                WHERE p.lab_id = i.id AND p.test_name = 'adx'
              )
            ORDER BY i.id
        """)
        inst = [{
            "id": int(r["id"]),
            "min_trade_type": r["min_trade_type"],
            "min_trade_value": Decimal(str(r["min_trade_value"])),
            "min_winrate": Decimal(str(r["min_winrate"]))
        } for r in rows]

        # параметры по инстансам
        params_map = {}
        for rinst in inst:
            p_rows = await conn.fetch("""
                SELECT id, test_name, test_type, test_tf, param_spec
                FROM laboratory_parameters_v4
                WHERE lab_id = $1 AND test_name = 'adx'
                ORDER BY id
            """, rinst["id"])
            params_map[rinst["id"]] = [{
                "id": int(pr["id"]),
                "test_type": pr["test_type"],      # 'solo' or 'comp'
                "test_tf": pr["test_tf"],          # 'm5'|'m15'|'h1' or None
                "param_spec": pr["param_spec"],    # jsonb
            } for pr in p_rows]
        return inst, params_map

# 🔸 Создать или взять running run по инстансу
async def ensure_run_for_instance(conn, lab_id: int):
    row = await conn.fetchrow("""
        SELECT id FROM laboratory_runs_v4
        WHERE lab_id=$1 AND status='running'
        ORDER BY started_at LIMIT 1
    """, lab_id)
    if row:
        return int(row["id"]), False
    rr = await conn.fetchrow("""
        INSERT INTO laboratory_runs_v4 (test_id, lab_id, status)
        VALUES ($1, $1, 'running') RETURNING id
    """, lab_id)
    return int(rr["id"]), True

# 🔸 Стратегии MW=true
async def load_active_mw_strategies(pg):
    async with pg.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id FROM strategies_v4
            WHERE enabled = true AND COALESCE(market_watcher, false) = true
            ORDER BY id
        """)
    return [int(r["id"]) for r in rows]

# 🔸 total_closed по strategy_id (все направления)
async def get_total_closed_for_strategy(conn, strategy_id: int) -> int:
    r = await conn.fetchrow("""
        SELECT COUNT(*) AS c FROM positions_v4
        WHERE strategy_id=$1 AND status='closed'
    """, strategy_id)
    return int(r["c"] or 0)

# 🔸 Итератор закрытых позиций по стратегии (батчами)
async def iter_closed_positions(pg, strategy_id: int, batch_size: int):
    offset = 0
    while True:
        async with pg.acquire() as conn:
            rows = await conn.fetch("""
                SELECT position_uid, direction
                FROM positions_v4
                WHERE strategy_id=$1 AND status='closed'
                ORDER BY id
                LIMIT $2 OFFSET $3
            """, strategy_id, batch_size, offset)
        if not rows:
            break
        yield [{"uid": r["position_uid"], "direction": r["direction"]} for r in rows]
        offset += batch_size

# 🔸 PIS: достать ADX значение на входе по TF (14 для m5/m15, 28 для h1)
async def get_pis_adx_for_position(conn, position_uid: str, tf: str) -> float | None:
    adx_len = _adx_len(tf)
    pname = f"adx_dmi{adx_len}_adx"
    r = await conn.fetchrow("""
        SELECT value_num FROM positions_indicators_stat
        WHERE position_uid=$1 AND using_current_bar=true
          AND timeframe=$2 AND param_name=$3
        LIMIT 1
    """, position_uid, tf, pname)
    if not r or r["value_num"] is None:
        return None
    return float(r["value_num"])

# 🔸 Проверка одного параметра ADX (solo TF)
async def check_param_adx_solo(conn, strategy_id: int, direction: str,
                               min_type: str, min_val: Decimal, min_wr: Decimal,
                               tf: str, position_uid: str, total_closed: int) -> tuple[bool, str | None]:
    # PIS -> bin
    val = await get_pis_adx_for_position(conn, position_uid, tf)
    if val is None:
        return False, f"no_pis_value:{tf}"
    bin_code = _bin_adx(val)
    if bin_code is None:
        return False, f"bin_error:{tf}"

    adx_len = _adx_len(tf)
    # агрегатная строка
    s = await conn.fetchrow("""
        SELECT closed_trades, winrate
        FROM positions_adxbins_stat_tf
        WHERE strategy_id=$1 AND direction=$2 AND timeframe=$3
          AND adx_len=$4 AND bin_code=$5
        LIMIT 1
    """, strategy_id, direction, tf, adx_len, bin_code)
    if not s:
        return False, f"no_agg_row:{tf}"

    closed = int(s["closed_trades"] or 0)
    wr = Decimal(str(s["winrate"] or "0"))
    # порог closed
    if min_type == "absolute":
        if closed < int(min_val):
            return False, f"closed_lt_min:{tf}"
    else:
        # percent от total_closed по стратегии
        need = (Decimal(total_closed) * min_val).quantize(Decimal("0.0001"))
        if Decimal(closed) < need:
            return False, f"closed_lt_min_pct:{tf}"
    # порог winrate
    if wr < min_wr:
        return False, f"winrate_lt_min:{tf}"
    return True, None

# 🔸 Проверка композита ADX (триплет)
async def check_param_adx_comp(conn, strategy_id: int, direction: str,
                               min_type: str, min_val: Decimal, min_wr: Decimal,
                               position_uid: str, total_closed: int) -> tuple[bool, str | None]:
    # bins из PIS
    vals = {}
    for tf in ("m5","m15","h1"):
        v = await get_pis_adx_for_position(conn, position_uid, tf)
        if v is None:
            return False, f"no_pis_value:{tf}"
        b = _bin_adx(v)
        if b is None:
            return False, f"bin_error:{tf}"
        vals[tf] = b
    triplet = f"{vals['m5']}-{vals['m15']}-{vals['h1']}"

    s = await conn.fetchrow("""
        SELECT closed_trades, winrate
        FROM positions_adxbins_stat_comp
        WHERE strategy_id=$1 AND direction=$2 AND status_triplet=$3
        LIMIT 1
    """, strategy_id, direction, triplet)
    if not s:
        return False, "no_agg_row:triplet"
    closed = int(s["closed_trades"] or 0)
    wr = Decimal(str(s["winrate"] or "0"))

    if min_type == "absolute":
        if closed < int(min_val):
            return False, "closed_lt_min:triplet"
    else:
        need = (Decimal(total_closed) * min_val).quantize(Decimal("0.0001"))
        if Decimal(closed) < need:
            return False, "closed_lt_min_pct:triplet"

    if wr < min_wr:
        return False, "winrate_lt_min:triplet"
    return True, None

# 🔸 Обработка одной позиции для заданного инстанса
async def process_position_for_instance(pg, run_id: int, lab_id: int, strategy_id: int, direction: str,
                                        min_type: str, min_val: Decimal, min_wr: Decimal,
                                        params: list[dict], position_uid: str, total_closed: int):
    # short-circuit: по порядку параметров
    async with pg.acquire() as conn:
        for p in params:
            if p["test_type"] == "solo":
                ok, reason = await check_param_adx_solo(conn, strategy_id, direction,
                                                        min_type, min_val, min_wr,
                                                        p["test_tf"], position_uid, total_closed)
            else:
                ok, reason = await check_param_adx_comp(conn, strategy_id, direction,
                                                        min_type, min_val, min_wr,
                                                        position_uid, total_closed)
            if not ok:
                await conn.execute("""
                    INSERT INTO laboratory_results_v4
                        (run_id, lab_id, position_uid, strategy_id, test_id, test_result, reason)
                    VALUES ($1,$2,$3,$4,$2,'ignored',$5)
                    ON CONFLICT (run_id, position_uid, test_id) DO NOTHING
                """, run_id, lab_id, position_uid, strategy_id, reason)
                return

        # все параметры прошли
        await conn.execute("""
            INSERT INTO laboratory_results_v4
                (run_id, lab_id, position_uid, strategy_id, test_id, test_result, reason)
            VALUES ($1,$2,$3,$4,$2,'approved',NULL)
            ON CONFLICT (run_id, position_uid, test_id) DO NOTHING
        """, run_id, lab_id, position_uid, strategy_id)

# 🔸 Прогон одного run’а по всем стратегиям
async def run_one_adx(pg, lab, lab_params):
    lab_id = lab["id"]
    min_type = lab["min_trade_type"]
    min_val  = lab["min_trade_value"]
    min_wr   = lab["min_winrate"]

    # создаём/берём run
    async with pg.acquire() as conn:
        run_id, created = await ensure_run_for_instance(conn, lab_id)
        if created:
            log.info("RUN created: lab_id=%d run_id=%d", lab_id, run_id)

    strategies = await load_active_mw_strategies(pg)
    gate = asyncio.Semaphore(MAX_CONCURRENCY)

    for sid in strategies:
        async with pg.acquire() as conn:
            total_closed = await get_total_closed_for_strategy(conn, sid)

        async def worker_batch(batch_rows):
            tasks = []
            for row in batch_rows:
                tasks.append(process_position_for_instance(
                    pg, run_id, lab_id, sid, row["direction"],
                    min_type, min_val, min_wr,
                    lab_params, row["uid"], total_closed
                ))
            async with gate:
                await asyncio.gather(*tasks)

        async for batch in iter_closed_positions(pg, sid, BATCH_SIZE):
            await worker_batch(batch)

    # пометить run завершённым
    async with pg.acquire() as conn:
        await conn.execute("""
            UPDATE laboratory_runs_v4
            SET status='done', finished_at=NOW()
            WHERE id=$1
        """, run_id)
        await conn.execute("""
            UPDATE laboratory_instances_v4
            SET last_used=NOW()
            WHERE id=$1
        """, lab_id)
    log.info("RUN done: lab_id=%d", lab_id)

# 🔸 Основной цикл раннера
async def run_lab_runner_adx(pg):
    # задержка старта
    if START_DELAY_SEC > 0:
        log.info("⏳ ADX runner: задержка старта %d с", START_DELAY_SEC)
        await asyncio.sleep(START_DELAY_SEC)

    while True:
        try:
            # загрузить библиотеку инстансов и их параметров
            instances, params_map = await load_active_adx_instances(pg)
            if not instances:
                log.info("ADX runner: активных инстансов нет")
            else:
                log.info("ADX runner: найдено инстансов=%d", len(instances))
                for lab in instances:
                    lab_params = params_map.get(lab["id"], [])
                    if not lab_params:
                        continue
                    await run_one_adx(pg, lab, lab_params)

        except Exception as e:
            log.error("ADX runner error: %s", e, exc_info=True)

        # спим 6 часов
        log.info("ADX runner: сон на 6 часов")
        await asyncio.sleep(SLEEP_BETWEEN_RUN)