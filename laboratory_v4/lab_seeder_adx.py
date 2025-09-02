# lab_seeder_adx.py — сидер ADX-инстансов (15 вариантов × 8 порогов × 6 winrate = 720), идемпотентно

import json
import logging
from decimal import Decimal

log = logging.getLogger("LAB_SEED_ADX")

# 🔸 Варианты ADX
SOLO_VARIANTS = [
    ("solo:m5",        ["m5"]),
    ("solo:m15",       ["m15"]),
    ("solo:h1",        ["h1"]),
    ("solo:m5+m15",    ["m5", "m15"]),
    ("solo:m5+h1",     ["m5", "h1"]),
    ("solo:m15+h1",    ["m15", "h1"]),
    ("solo:m5+m15+h1", ["m5", "m15", "h1"]),
]
COMP_VARIANT = ("comp", [])  # триплет (m5=14, m15=14, h1=28)
SOLO_COMP_VARIANTS = [
    ("comp+solo:m5",        ["m5"]),
    ("comp+solo:m15",       ["m15"]),
    ("comp+solo:h1",        ["h1"]),
    ("comp+solo:m5+m15",    ["m5", "m15"]),
    ("comp+solo:m5+h1",     ["m5", "h1"]),
    ("comp+solo:m15+h1",    ["m15", "h1"]),
    ("comp+solo:m5+m15+h1", ["m5", "m15", "h1"]),
]

# 🔸 Пороги
ABS_LIST = [2, 3, 4, 5]
PCT_LIST = [Decimal("0.04"), Decimal("0.06"), Decimal("0.08"), Decimal("0.10")]
WR_LIST  = [Decimal("0.50"), Decimal("0.55"), Decimal("0.60"), Decimal("0.65"), Decimal("0.70"), Decimal("0.75")]

# 🔸 Утилиты
def _adx_len(tf: str) -> int:
    return 14 if tf in ("m5", "m15") else 28

def _make_name(variant: str, min_type: str, min_value: Decimal, wr: Decimal) -> str:
    if min_type == "absolute":
        thresh = f"abs:{int(min_value)}"
    else:
        # храним как долю (0.04, 0.06, ...)
        thresh = f"pct:{min_value:.2f}"
    return f"ADX | {variant} | thresh={thresh} | wr={wr:.2f}"

async def seed(pg):
    """
    Генерирует все ADX-инстансы:
      - 15 вариантов (solo/comp/comp+solo)
      - × 4 absolute + 4 percent порога закрытых
      - × 6 winrate порогов
    Идемпотентно: upsert по имени инстанса + проверка наличия параметров.
    """
    log.info("🧩 ADX seeder: старт генерации матрицы инстансов")

    variants = SOLO_VARIANTS + [COMP_VARIANT] + SOLO_COMP_VARIANTS
    total_variants = len(variants)

    created, updated = 0, 0
    param_created, param_reused = 0, 0

    async with pg.acquire() as conn:
        # без общей транзакции: пишем инстансы/параметры сразу
        for vidx, (variant, tfs) in enumerate(variants, 1):
            # absolute пороги
            for N in ABS_LIST:
                for wr in WR_LIST:
                    name = _make_name(variant, "absolute", Decimal(N), wr)
                    lab_id, is_new = await upsert_instance(conn, name, "absolute", Decimal(N), wr)
                    c, r = await ensure_adx_params(conn, lab_id, variant, tfs)
                    created += int(is_new); updated += int(not is_new)
                    param_created += c; param_reused += r

            # percent пороги
            for p in PCT_LIST:
                for wr in WR_LIST:
                    name = _make_name(variant, "percent", p, wr)
                    lab_id, is_new = await upsert_instance(conn, name, "percent", p, wr)
                    c, r = await ensure_adx_params(conn, lab_id, variant, tfs)
                    created += int(is_new); updated += int(not is_new)
                    param_created += c; param_reused += r

            log.info(
                "🧩 ADX seeder: variant %d/%d готов (%s) | inst:new=%d reuse=%d | params:new=%d reuse=%d",
                vidx, total_variants, variant, created, updated, param_created, param_reused
            )

    log.info("🧩 ADX seeder: завершено. instances total=%d (new=%d, reused=%d); params total(new=%d, reused=%d)",
             created+updated, created, updated, param_created, param_reused)

async def upsert_instance(conn, name: str, min_type: str, min_value: Decimal, wr: Decimal):
    row = await conn.fetchrow("SELECT id FROM laboratory_instances_v4 WHERE name = $1", name)
    if row:
        await conn.execute("""
            UPDATE laboratory_instances_v4
               SET active=true,
                   min_trade_type=$2,
                   min_trade_value=$3,
                   min_winrate=$4,
                   updated_at=NOW()
             WHERE id=$1
        """, row["id"], min_type, min_value, wr)
        return row["id"], False
    else:
        row = await conn.fetchrow("""
            INSERT INTO laboratory_instances_v4
                (name, active, min_trade_type, min_trade_value, min_winrate)
            VALUES ($1, true, $2, $3, $4)
            RETURNING id
        """, name, min_type, min_value, wr)
        return row["id"], True

async def ensure_adx_params(conn, lab_id: int, variant: str, tfs: list[str]):
    """
    Создаёт недостающие параметры для заданного инстанса:
      - comp-параметр (если variant начинается с 'comp')
      - solo-параметры по списку tfs (m5/m15 adx_len=14, h1 adx_len=28)
    Возвращает (new_params_count, reused_params_count).
    """
    new_cnt, reused_cnt = 0, 0

    # comp-параметр (триплет)
    if variant.startswith("comp"):
        _, is_new = await upsert_param(conn, lab_id, 'adx', 'comp', None, {})
        new_cnt += int(is_new); reused_cnt += int(not is_new)

    # solo-параметры по TF
    for tf in tfs:
        spec = {"adx_len": _adx_len(tf)}
        _, is_new = await upsert_param(conn, lab_id, 'adx', 'solo', tf, spec)
        new_cnt += int(is_new); reused_cnt += int(not is_new)

    return new_cnt, reused_cnt

async def upsert_param(conn, lab_id: int, test_name: str, test_type: str, test_tf, param_spec: dict):
    """
    Идемпотентная вставка параметра: ключ равен (lab_id, test_name, test_type, test_tf, param_spec).
    test_tf может быть NULL — сравниваем через COALESCE и приводим к text.
    """
    payload = json.dumps(param_spec)
    row = await conn.fetchrow("""
        SELECT id
          FROM laboratory_parameters_v4
         WHERE lab_id    = $1
           AND test_name = $2
           AND test_type = $3
           AND COALESCE(test_tf, '') = COALESCE($4::text, '')
           AND param_spec = $5::jsonb
    """, lab_id, test_name, test_type, test_tf, payload)

    if row:
        return row["id"], False
    else:
        r = await conn.fetchrow("""
            INSERT INTO laboratory_parameters_v4
                (lab_id, test_name, test_type, test_tf, param_spec)
            VALUES ($1, $2, $3, $4::text, $5::jsonb)
            RETURNING id
        """, lab_id, test_name, test_type, test_tf, payload)
        return r["id"], True