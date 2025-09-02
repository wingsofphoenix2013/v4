# lab_seeder_adx.py — сидер ADX-инстансов (15 вариантов × 8 порогов × 6 winrate = 720), идемпотентно

import json
import logging
from decimal import Decimal

log = logging.getLogger("LAB_SEED_ADX")

# 🔸 Варианты ADX
SOLO_VARIANTS = [
    ("solo:m5",      ["m5"]),
    ("solo:m15",     ["m15"]),
    ("solo:h1",      ["h1"]),
    ("solo:m5+m15",  ["m5","m15"]),
    ("solo:m5+h1",   ["m5","h1"]),
    ("solo:m15+h1",  ["m15","h1"]),
    ("solo:m5+m15+h1", ["m5","m15","h1"]),
]
COMP_VARIANT = ("comp", [])  # триплет
SOLO_COMP_VARIANTS = [
    ("comp+solo:m5",       ["m5"]),
    ("comp+solo:m15",      ["m15"]),
    ("comp+solo:h1",       ["h1"]),
    ("comp+solo:m5+m15",   ["m5","m15"]),
    ("comp+solo:m5+h1",    ["m5","h1"]),
    ("comp+solo:m15+h1",   ["m15","h1"]),
    ("comp+solo:m5+m15+h1",["m5","m15","h1"]),
]

# 🔸 Пороги
ABS_LIST = [2,3,4,5]
PCT_LIST = [Decimal("0.04"), Decimal("0.06"), Decimal("0.08"), Decimal("0.10")]
WR_LIST  = [Decimal("0.50"), Decimal("0.55"), Decimal("0.60"), Decimal("0.65"), Decimal("0.70"), Decimal("0.75")]

# 🔸 Утилиты
def _adx_len(tf: str) -> int:
    return 14 if tf in ("m5","m15") else 28

def _make_name(variant: str, min_type: str, min_value, wr: Decimal) -> str:
    if min_type == "absolute":
        thresh = f"abs:{int(min_value)}"
    else:
        thresh = f"pct:{Decimal(min_value):.2%}".replace("%","")  # 0.04 -> "4.00"
        # компактнее:
        thresh = f"pct:{Decimal(min_value):.2f}"                  # "0.04"
    return f"ADX | {variant} | thresh={thresh} | wr={wr:.2f}"

async def seed(pg):
    log.info("🧩 ADX seeder: старт генерации матрицы инстансов")

    # сформируем полный список вариантов
    variants = SOLO_VARIANTS + [COMP_VARIANT] + SOLO_COMP_VARIANTS

    created, updated = 0, 0

    async with pg.acquire() as conn:
        async with conn.transaction():
            for variant, tfs in variants:
                # пороги absolute
                for N in ABS_LIST:
                    for wr in WR_LIST:
                        name = _make_name(variant, "absolute", N, wr)
                        lab_id, is_new = await upsert_instance(conn, name, "absolute", Decimal(N), wr)
                        await ensure_adx_params(conn, lab_id, variant, tfs)
                        created += int(is_new); updated += int(not is_new)
                # пороги percent
                for p in PCT_LIST:
                    for wr in WR_LIST:
                        name = _make_name(variant, "percent", p, wr)
                        lab_id, is_new = await upsert_instance(conn, name, "percent", p, wr)
                        await ensure_adx_params(conn, lab_id, variant, tfs)
                        created += int(is_new); updated += int(not is_new)

    log.info("🧩 ADX seeder: готово. instances: new=%d, reused=%d (total=%d)", created, updated, created+updated)

async def upsert_instance(conn, name: str, min_type: str, min_value: Decimal, wr: Decimal):
    row = await conn.fetchrow("""
        SELECT id FROM laboratory_instances_v4 WHERE name = $1
    """, name)
    if row:
        await conn.execute("""
            UPDATE laboratory_instances_v4
               SET active=true,
                   min_trade_type=$2, min_trade_value=$3, min_winrate=$4,
                   updated_at=NOW()
             WHERE id=$1
        """, row["id"], min_type, str(min_value), str(wr))
        return row["id"], False
    else:
        row = await conn.fetchrow("""
            INSERT INTO laboratory_instances_v4
                (name, active, min_trade_type, min_trade_value, min_winrate)
            VALUES ($1, true, $2, $3, $4)
            RETURNING id
        """, name, min_type, str(min_value), str(wr))
        return row["id"], True

async def ensure_adx_params(conn, lab_id: int, variant: str, tfs: list[str]):
    # 1) COMP часть?
    need_comp = variant.startswith("comp")  # "comp" или "comp+solo:..."
    if need_comp:
        await upsert_param(conn, lab_id, 'adx', 'comp', None, {})  # param_spec = {}

    # 2) SOLO TF часть (если есть TF в списке)
    for tf in tfs:
        spec = {"adx_len": _adx_len(tf)}
        await upsert_param(conn, lab_id, 'adx', 'solo', tf, spec)

async def upsert_param(conn, lab_id: int, test_name: str, test_type: str, test_tf: str | None, param_spec: dict):
    row = await conn.fetchrow("""
        SELECT id FROM laboratory_parameters_v4
         WHERE lab_id=$1 AND test_name=$2 AND test_type=$3
           AND ((test_tf IS NULL AND $4 IS NULL) OR (test_tf = $4))
           AND param_spec = $5::jsonb
    """, lab_id, test_name, test_type, test_tf, json.dumps(param_spec))
    if row:
        return row["id"], False
    else:
        r = await conn.fetchrow("""
            INSERT INTO laboratory_parameters_v4
                (lab_id, test_name, test_type, test_tf, param_spec)
            VALUES ($1,$2,$3,$4,$5::jsonb)
            RETURNING id
        """, lab_id, test_name, test_type, test_tf, json.dumps(param_spec))
        return r["id"], True