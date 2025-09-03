# 🔸 Сидер для лаборатории: генерация тестов ADX и BB (каждый — 24 комплекта × 15 комбинаций), active=false

import logging
from decimal import Decimal
import laboratory_v4_infra as infra

log = logging.getLogger("LAB_SEEDER")

# 🔸 Общие сетки параметров
WINRATE_VARIANTS = [0.50, 0.55, 0.60, 0.65]
MIN_TRADE_VARIANTS = [
    ("absolute", 5),
    ("absolute", 10),
    ("absolute", 15),
    ("percent", 0.05),
    ("percent", 0.10),
    ("percent", 0.15),
]

# 🔸 Компоненты (15 комбинаций без пустого множества)
COMPONENTS = [
    ["m5"],
    ["m15"],
    ["h1"],
    ["comp"],
    ["m5", "m15"],
    ["m5", "h1"],
    ["m5", "comp"],
    ["m15", "h1"],
    ["m15", "comp"],
    ["h1", "comp"],
    ["m5", "m15", "h1"],
    ["m5", "m15", "comp"],
    ["m5", "h1", "comp"],
    ["m15", "h1", "comp"],
    ["m5", "m15", "h1", "comp"],
]


# 🔸 Имя теста ADX
def make_name_adx(components, min_trade_type, min_trade_value, wr):
    comp_str = "+".join(components)
    trade_str = f"abs:{min_trade_value}" if min_trade_type == "absolute" else f"percent:{int(min_trade_value*100)}%"
    return f"ADX | {comp_str} | thresh={trade_str} | wr={wr:.2f}"


# 🔸 Имя теста BB (фиксированные bb20/2.0)
def make_name_bb(components, min_trade_type, min_trade_value, wr):
    comp_str = "+".join(components)
    trade_str = f"abs:{min_trade_value}" if min_trade_type == "absolute" else f"percent:{int(min_trade_value*100)}%"
    return f"BB20/2.0 | {comp_str} | thresh={trade_str} | wr={wr:.2f}"

# 🔸 Имя теста RSI
def make_name_rsi(components, min_trade_type, min_trade_value, wr):
    comp_str = "+".join(components)
    trade_str = f"abs:{min_trade_value}" if min_trade_type == "absolute" else f"percent:{int(min_trade_value*100)}%"
    return f"RSI14 | {comp_str} | thresh={trade_str} | wr={wr:.2f}"
    
# 🔸 Сидер ADX (как было)
async def run_adx_seeder():
    async with infra.pg_pool.acquire() as conn:
        existing = await conn.fetchval("SELECT COUNT(*) FROM laboratory_instances_v4 WHERE name LIKE 'ADX | %'")
        if existing and existing > 0:
            log.info("Сидер: ADX-тесты уже есть (%s шт.), сид не нужен", existing)
            return

    log.info("Сидер: начинаем генерацию ADX-тестов")

    async with infra.pg_pool.acquire() as conn:
        async with conn.transaction():
            for wr in WINRATE_VARIANTS:
                for mt_type, mt_value in MIN_TRADE_VARIANTS:
                    for comps in COMPONENTS:
                        name = make_name_adx(comps, mt_type, mt_value, wr)
                        row = await conn.fetchrow(
                            """
                            INSERT INTO laboratory_instances_v4
                              (name, active, min_trade_type, min_trade_value, min_winrate)
                            VALUES ($1, false, $2, $3, $4)
                            RETURNING id
                            """,
                            name, mt_type, Decimal(str(mt_value)), Decimal(str(wr))
                        )
                        lab_id = row["id"]

                        for c in comps:
                            if c == "m5":
                                await conn.execute(
                                    "INSERT INTO laboratory_parameters_v4 (lab_id, test_name, test_type, test_tf, param_spec) "
                                    "VALUES ($1, 'adx', 'solo', 'm5',  '{\"adx_len\":14}')",
                                    lab_id,
                                )
                            elif c == "m15":
                                await conn.execute(
                                    "INSERT INTO laboratory_parameters_v4 (lab_id, test_name, test_type, test_tf, param_spec) "
                                    "VALUES ($1, 'adx', 'solo', 'm15', '{\"adx_len\":14}')",
                                    lab_id,
                                )
                            elif c == "h1":
                                await conn.execute(
                                    "INSERT INTO laboratory_parameters_v4 (lab_id, test_name, test_type, test_tf, param_spec) "
                                    "VALUES ($1, 'adx', 'solo', 'h1',  '{\"adx_len\":28}')",
                                    lab_id,
                                )
                            elif c == "comp":
                                await conn.execute(
                                    "INSERT INTO laboratory_parameters_v4 (lab_id, test_name, test_type, test_tf, param_spec) "
                                    "VALUES ($1, 'adx', 'comp', NULL,  '{}')",
                                    lab_id,
                                )

    log.info("Сидер: успешно создано %d ADX-тестов", len(WINRATE_VARIANTS) * len(MIN_TRADE_VARIANTS) * len(COMPONENTS))


# 🔸 Сидер BB (новый)
async def run_bb_seeder():
    async with infra.pg_pool.acquire() as conn:
        existing = await conn.fetchval("SELECT COUNT(*) FROM laboratory_instances_v4 WHERE name LIKE 'BB20/2.0 | %'")
        if existing and existing > 0:
            log.info("Сидер: BB-тесты уже есть (%s шт.), сид не нужен", existing)
            return

    log.info("Сидер: начинаем генерацию BB-тестов (bb20/2.0)")

    async with infra.pg_pool.acquire() as conn:
        async with conn.transaction():
            for wr in WINRATE_VARIANTS:
                for mt_type, mt_value in MIN_TRADE_VARIANTS:
                    for comps in COMPONENTS:
                        name = make_name_bb(comps, mt_type, mt_value, wr)
                        row = await conn.fetchrow(
                            """
                            INSERT INTO laboratory_instances_v4
                              (name, active, min_trade_type, min_trade_value, min_winrate)
                            VALUES ($1, false, $2, $3, $4)
                            RETURNING id
                            """,
                            name, mt_type, Decimal(str(mt_value)), Decimal(str(wr))
                        )
                        lab_id = row["id"]

                        for c in comps:
                            if c == "m5":
                                await conn.execute(
                                    "INSERT INTO laboratory_parameters_v4 (lab_id, test_name, test_type, test_tf, param_spec) "
                                    "VALUES ($1, 'bb', 'solo', 'm5',  '{\"bb_len\":20,\"bb_std\":2.0}')",
                                    lab_id,
                                )
                            elif c == "m15":
                                await conn.execute(
                                    "INSERT INTO laboratory_parameters_v4 (lab_id, test_name, test_type, test_tf, param_spec) "
                                    "VALUES ($1, 'bb', 'solo', 'm15', '{\"bb_len\":20,\"bb_std\":2.0}')",
                                    lab_id,
                                )
                            elif c == "h1":
                                await conn.execute(
                                    "INSERT INTO laboratory_parameters_v4 (lab_id, test_name, test_type, test_tf, param_spec) "
                                    "VALUES ($1, 'bb', 'solo', 'h1',  '{\"bb_len\":20,\"bb_std\":2.0}')",
                                    lab_id,
                                )
                            elif c == "comp":
                                await conn.execute(
                                    "INSERT INTO laboratory_parameters_v4 (lab_id, test_name, test_type, test_tf, param_spec) "
                                    "VALUES ($1, 'bb', 'comp', NULL,  '{\"bb_len\":20,\"bb_std\":2.0}')",
                                    lab_id,
                                )

    log.info("Сидер: успешно создано %d BB-тестов", len(WINRATE_VARIANTS) * len(MIN_TRADE_VARIANTS) * len(COMPONENTS))

# 🔸 Сидер RSI (новый)
async def run_rsi_seeder():
    async with infra.pg_pool.acquire() as conn:
        existing = await conn.fetchval("SELECT COUNT(*) FROM laboratory_instances_v4 WHERE name LIKE 'RSI14 | %'")
        if existing and existing > 0:
            log.info("Сидер: RSI-тесты уже есть (%s шт.), сид не нужен", existing)
            return

    log.info("Сидер: начинаем генерацию RSI-тестов (rsi14)")

    async with infra.pg_pool.acquire() as conn:
        async with conn.transaction():
            for wr in WINRATE_VARIANTS:
                for mt_type, mt_value in MIN_TRADE_VARIANTS:
                    for comps in COMPONENTS:
                        name = make_name_rsi(comps, mt_type, mt_value, wr)
                        row = await conn.fetchrow(
                            """
                            INSERT INTO laboratory_instances_v4
                              (name, active, min_trade_type, min_trade_value, min_winrate)
                            VALUES ($1, false, $2, $3, $4)
                            RETURNING id
                            """,
                            name, mt_type, Decimal(str(mt_value)), Decimal(str(wr))
                        )
                        lab_id = row["id"]

                        for c in comps:
                            if c == "m5":
                                await conn.execute(
                                    "INSERT INTO laboratory_parameters_v4 (lab_id, test_name, test_type, test_tf, param_spec) "
                                    "VALUES ($1, 'rsi', 'solo', 'm5',  '{\"rsi_len\":14}')",
                                    lab_id,
                                )
                            elif c == "m15":
                                await conn.execute(
                                    "INSERT INTO laboratory_parameters_v4 (lab_id, test_name, test_type, test_tf, param_spec) "
                                    "VALUES ($1, 'rsi', 'solo', 'm15', '{\"rsi_len\":14}')",
                                    lab_id,
                                )
                            elif c == "h1":
                                await conn.execute(
                                    "INSERT INTO laboratory_parameters_v4 (lab_id, test_name, test_type, test_tf, param_spec) "
                                    "VALUES ($1, 'rsi', 'solo', 'h1',  '{\"rsi_len\":14}')",
                                    lab_id,
                                )
                            elif c == "comp":
                                await conn.execute(
                                    "INSERT INTO laboratory_parameters_v4 (lab_id, test_name, test_type, test_tf, param_spec) "
                                    "VALUES ($1, 'rsi', 'comp', NULL,  '{\"rsi_len\":14}')",
                                    lab_id,
                                )

    log.info("Сидер: успешно создано %d RSI-тестов", len(WINRATE_VARIANTS) * len(MIN_TRADE_VARIANTS) * len(COMPONENTS))