# lab_runner_adx.py — авто-раннер ADX (set-based): каждые 6 часов создаёт run’ы для активных ADX-инстансов и обрабатывает пакетно

import asyncio
import logging
from decimal import Decimal

log = logging.getLogger("LAB_RUNNER_ADX")

# 🔸 Параметры цикла
START_DELAY_SEC   = 120          # задержка перед первым стартом
SLEEP_BETWEEN_RUN = 6 * 3600     # 6 часов
MAX_CONCURRENCY   = 10           # одновременно обрабатываем до 10 инстансов

# 🔸 Определение длины ADX для TF
def _adx_len(tf: str) -> int:
    return 14 if tf in ("m5", "m15") else 28

# 🔸 Загрузка активных ADX-инстансов и их параметров (одним заходом)
async def load_active_adx_instances_and_params(pg):
    async with pg.acquire() as conn:
        inst_rows = await conn.fetch("""
            SELECT i.id, i.min_trade_type, i.min_trade_value, i.min_winrate
            FROM laboratory_instances_v4 i
            WHERE i.active = true
              AND EXISTS (
                SELECT 1 FROM laboratory_parameters_v4 p
                WHERE p.lab_id = i.id AND p.test_name = 'adx'
              )
            ORDER BY i.id
        """)
        lab_ids = [int(r["id"]) for r in inst_rows]
        params_rows = []
        if lab_ids:
            params_rows = await conn.fetch("""
                SELECT lab_id, test_type, test_tf, param_spec
                FROM laboratory_parameters_v4
                WHERE test_name='adx' AND lab_id = ANY($1::int[])
                ORDER BY lab_id, id
            """, lab_ids)

    inst = [{
        "id": int(r["id"]),
        "min_trade_type": r["min_trade_type"],
        "min_trade_value": Decimal(str(r["min_trade_value"])),
        "min_winrate": Decimal(str(r["min_winrate"]))
    } for r in inst_rows]

    params_map = {i["id"]: [] for i in inst}
    for r in params_rows:
        params_map[int(r["lab_id"])].append({
            "test_type": r["test_type"],      # 'solo' or 'comp'
            "test_tf": r["test_tf"],          # 'm5'|'m15'|'h1' or None
            "param_spec": r["param_spec"],    # jsonb
        })
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

# 🔸 Выполнить пакетную обработку одного ADX-инстанса
async def run_one_adx_setbased(pg, lab: dict, lab_params: list[dict]):
    lab_id  = lab["id"]
    min_type= lab["min_trade_type"]
    min_val = lab["min_trade_value"]
    min_wr  = lab["min_winrate"]

    need_comp = any(p["test_type"] == "comp" for p in lab_params)
    need_tf   = sorted({p["test_tf"] for p in lab_params if p["test_type"] == "solo"})

    async with pg.acquire() as conn:
        run_id, created = await ensure_run_for_instance(conn, lab_id)
        if created:
            log.info("RUN created: lab_id=%d run_id=%d", lab_id, run_id)

        # TEMP: closed позиции стратегий MW
        await conn.execute("DROP TABLE IF EXISTS tmp_lab_pos")
        await conn.execute("""
            CREATE TEMP TABLE tmp_lab_pos AS
            SELECT p.strategy_id, p.position_uid, p.direction
            FROM positions_v4 p
            JOIN strategies_v4 s ON s.id = p.strategy_id
            WHERE s.enabled=true AND COALESCE(s.market_watcher,false)=true
              AND p.status='closed'
        """)
        await conn.execute("CREATE INDEX ON tmp_lab_pos(strategy_id)")
        await conn.execute("CREATE INDEX ON tmp_lab_pos(position_uid)")
        await conn.execute("CREATE INDEX ON tmp_lab_pos(strategy_id, direction)")

        # TEMP: totals по стратегии
        await conn.execute("DROP TABLE IF EXISTS tmp_lab_totals")
        await conn.execute("""
            CREATE TEMP TABLE tmp_lab_totals AS
            SELECT strategy_id, COUNT(*)::int AS total_closed
            FROM positions_v4
            WHERE status='closed'
            GROUP BY strategy_id
        """)
        await conn.execute("CREATE INDEX ON tmp_lab_totals(strategy_id)")

        # TEMP: ключи ADX-бинов из PIS
        await conn.execute("DROP TABLE IF EXISTS tmp_lab_adx_keys")
        await conn.execute("""
            CREATE TEMP TABLE tmp_lab_adx_keys AS
            WITH pis AS (
              SELECT position_uid, timeframe,
                     CASE
                       WHEN param_name IN ('adx_dmi14_adx','adx_dmi28_adx') THEN value_num
                       ELSE NULL
                     END AS adx_val
              FROM positions_indicators_stat
              WHERE using_current_bar=true
                AND param_name IN ('adx_dmi14_adx','adx_dmi28_adx')
                AND timeframe IN ('m5','m15','h1')
            ),
            bins AS (
              SELECT position_uid, timeframe,
                     CASE
                       WHEN adx_val IS NULL THEN NULL
                       WHEN adx_val >= 100 THEN 95
                       WHEN adx_val < 0 THEN 0
                       ELSE (floor(adx_val/5))*5
                     END::int AS bin_code
              FROM pis
            )
            SELECT
              p.position_uid,
              max(CASE WHEN timeframe='m5'  THEN bin_code END)::int  AS m5_bin,
              max(CASE WHEN timeframe='m15' THEN bin_code END)::int  AS m15_bin,
              max(CASE WHEN timeframe='h1'  THEN bin_code END)::int  AS h1_bin
            FROM tmp_lab_pos p
            LEFT JOIN bins b USING(position_uid)
            GROUP BY p.position_uid
        """)
        await conn.execute("ALTER TABLE tmp_lab_adx_keys ADD COLUMN triplet text")
        await conn.execute("""
            UPDATE tmp_lab_adx_keys
               SET triplet = (m5_bin::text || '-' || m15_bin::text || '-' || h1_bin::text)
        """)
        await conn.execute("CREATE INDEX ON tmp_lab_adx_keys(position_uid)")
        await conn.execute("CREATE INDEX ON tmp_lab_adx_keys(triplet)")

        # TEMP: проходы по параметрам
        await conn.execute("DROP TABLE IF EXISTS tmp_lab_pass")
        await conn.execute("""
            CREATE TEMP TABLE tmp_lab_pass (
              position_uid text NOT NULL,
              pass_idx     int  NOT NULL
            )
        """)
        pass_idx = 0

        # SOLO TF → один INSERT … SELECT на TF с явными типами
        for tf in need_tf:
            pass_idx += 1
            adx_len = _adx_len(tf)
            if min_type == 'absolute':
                closed_cond = f"t.closed_trades >= {int(min_val)}"
            else:
                closed_cond = "t.closed_trades >= (tot.total_closed * $1::numeric)"  # $1 -> min_val
            wr_cond = "t.winrate >= $2::numeric"  # $2 -> min_wr

            await conn.execute(f"""
                INSERT INTO tmp_lab_pass (position_uid, pass_idx)
                SELECT pos.position_uid, {pass_idx}
                FROM tmp_lab_pos pos
                JOIN tmp_lab_adx_keys k  ON k.position_uid = pos.position_uid
                JOIN positions_adxbins_stat_tf t
                  ON t.strategy_id = pos.strategy_id
                 AND t.direction  = pos.direction
                 AND t.timeframe  = $3::text
                 AND t.adx_len    = $4::int
                 AND t.bin_code   = CASE $5::text
                                        WHEN 'm5' THEN k.m5_bin
                                        WHEN 'm15' THEN k.m15_bin
                                        ELSE k.h1_bin
                                    END
                LEFT JOIN tmp_lab_totals tot ON tot.strategy_id = pos.strategy_id
                WHERE {closed_cond}
                  AND {wr_cond}
            """, (min_val if min_type != 'absolute' else Decimal(0)), min_wr, tf, adx_len, tf)

        # COMP (триплет) → один INSERT … SELECT
        if need_comp:
            pass_idx += 1
            if min_type == 'absolute':
                closed_cond_comp = f"t.closed_trades >= {int(min_val)}"
            else:
                closed_cond_comp = "t.closed_trades >= (tot.total_closed * $1::numeric)"  # $1 -> min_val
            wr_cond_comp = "t.winrate >= $2::numeric"  # $2 -> min_wr

            await conn.execute(f"""
                INSERT INTO tmp_lab_pass (position_uid, pass_idx)
                SELECT pos.position_uid, {pass_idx}
                FROM tmp_lab_pos pos
                JOIN tmp_lab_adx_keys k  ON k.position_uid = pos.position_uid
                JOIN positions_adxbins_stat_comp t
                  ON t.strategy_id = pos.strategy_id
                 AND t.direction  = pos.direction
                 AND t.status_triplet = k.triplet
                LEFT JOIN tmp_lab_totals tot ON tot.strategy_id = pos.strategy_id
                WHERE {closed_cond_comp}
                  AND {wr_cond_comp}
            """, (min_val if min_type != 'absolute' else Decimal(0)), min_wr)

        required_passes = pass_idx

        # INSERT approved
        await conn.execute("""
            INSERT INTO laboratory_results_v4
                (run_id, lab_id, position_uid, strategy_id, test_id, test_result, reason)
            SELECT $1::bigint, $2::int, pos.position_uid, pos.strategy_id, $2::int,
                   'approved', NULL
            FROM tmp_lab_pos pos
            JOIN (
              SELECT position_uid, COUNT(*) AS c
              FROM tmp_lab_pass
              GROUP BY position_uid
            ) p ON p.position_uid = pos.position_uid
            WHERE p.c = $3::int
            ON CONFLICT (run_id, position_uid, test_id) DO NOTHING
        """, run_id, lab_id, required_passes)

        # INSERT ignored (остаток)
        await conn.execute("""
            INSERT INTO laboratory_results_v4
                (run_id, lab_id, position_uid, strategy_id, test_id, test_result, reason)
            SELECT $1::bigint, $2::int, pos.position_uid, pos.strategy_id, $2::int,
                   'ignored', 'failed'
            FROM tmp_lab_pos pos
            LEFT JOIN laboratory_results_v4 r
              ON r.run_id=$1 AND r.test_id=$2 AND r.position_uid=pos.position_uid
            WHERE r.run_id IS NULL
            ON CONFLICT (run_id, position_uid, test_id) DO NOTHING
        """, run_id, lab_id)

        # Закрыть run и отметить last_used
        await conn.execute("UPDATE laboratory_runs_v4 SET status='done', finished_at=NOW() WHERE id=$1", run_id)
        await conn.execute("UPDATE laboratory_instances_v4 SET last_used=NOW() WHERE id=$1", lab_id)

    log.info("RUN done: lab_id=%d", lab_id)

# 🔸 Основной цикл раннера (параллельно до 10 инстансов)
async def run_lab_runner_adx(pg):
    if START_DELAY_SEC > 0:
        log.info("⏳ ADX runner: задержка старта %d с", START_DELAY_SEC)
        await asyncio.sleep(START_DELAY_SEC)

    while True:
        try:
            instances, params_map = await load_active_adx_instances_and_params(pg)
            if not instances:
                log.info("ADX runner: активных инстансов нет")
            else:
                log.info("ADX runner: найдено инстансов=%d", len(instances))
                gate = asyncio.Semaphore(MAX_CONCURRENCY)

                async def worker(lab):
                    lab_params = params_map.get(lab["id"], [])
                    if not lab_params:
                        return
                    async with gate:
                        await run_one_adx_setbased(pg, lab, lab_params)

                tasks = [asyncio.create_task(worker(lab)) for lab in instances]
                await asyncio.gather(*tasks)

        except Exception as e:
            log.error("ADX runner error: %s", e, exc_info=True)

        log.info("ADX runner: сон на 6 часов")
        await asyncio.sleep(SLEEP_BETWEEN_RUN)