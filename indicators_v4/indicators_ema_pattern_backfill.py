# indicators_ema_pattern_backfill.py — бэкофиллер EMA-паттернов: обходит позиции, достраивает emapattern в PIS и проставляет флаг

# 🔸 Импорты
import asyncio
import logging
from datetime import datetime
from typing import Dict, Optional

# 🔸 Логгер
log = logging.getLogger("EMA_PATTERN_BACKFILL")

# 🔸 Конфиг
SLEEP_START_SEC = 120                   # старт через 2 минуты
BATCH_LIMIT = 500                       # размер батча позиций
SLEEP_AFTER_DONE_SEC = 96 * 60 * 60     # «уснуть» на 96 часов после полного прохода
REQUIRED_TFS = ("m5", "m15", "h1")

# 🔸 Фейковые instance_id для emapattern
EMAPATTERN_INSTANCE_ID: Dict[str, int] = {"m5": 1004, "m15": 1005, "h1": 1006}

# 🔸 Имена EMA и соответствие TF
EMA_NAMES = ("ema9", "ema21", "ema50", "ema100", "ema200")

# 🔸 Кэш словаря паттернов (pattern_text -> id)
_EMA_PATTERN_DICT: Dict[str, int] = {}


# 🔸 Разовая загрузка словаря EMA-паттернов
async def _load_emapattern_dict(pg) -> None:
    global _EMA_PATTERN_DICT
    async with pg.acquire() as conn:
        rows = await conn.fetch("SELECT id, pattern_text FROM indicator_emapattern_dict")
    _EMA_PATTERN_DICT = {str(r["pattern_text"]): int(r["id"]) for r in rows}
    log.debug(f"[DICT] loaded={len(_EMA_PATTERN_DICT)}")


# 🔸 Относительное равенство (0.05%)
def _rel_equal(a: float, b: float, eps_rel: float = 0.0005) -> bool:
    m = max(abs(a), abs(b), 1e-12)
    return abs(a - b) <= eps_rel * m


# 🔸 Построение каноничного текста EMA-паттерна из entry_price и 5 EMA
def _build_emapattern_text(entry_price: float, emas: Dict[str, float]) -> str:
    EMA_LEN = {"ema9": 9, "ema21": 21, "ema50": 50, "ema100": 100, "ema200": 200}
    pairs = [("PRICE", float(entry_price))] + [(n.upper(), float(emas[n])) for n in EMA_NAMES]
    pairs.sort(key=lambda kv: kv[1], reverse=True)

    groups = []
    cur = []
    for token, val in pairs:
        if not cur:
            cur = [(token, val)]
            continue
        ref = cur[0][1]
        if _rel_equal(val, ref):
            cur.append((token, val))
        else:
            groups.append([t for t, _ in cur])
            cur = [(token, val)]
    if cur:
        groups.append([t for t, _ in cur])

    canon = []
    for g in groups:
        if "PRICE" in g:
            rest = [t for t in g if t != "PRICE"]
            rest.sort(key=lambda t: EMA_LEN[t.lower()])
            canon.append(["PRICE"] + rest)
        else:
            gg = list(g)
            gg.sort(key=lambda t: EMA_LEN[t.lower()])
            canon.append(gg)

    return " > ".join(" = ".join(g) for g in canon)


# 🔸 Выбрать батч кандидатов (позиции без emapattern_checked у стратегий с market_watcher=true)
async def _fetch_candidate_positions(pg) -> list[str]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT p.position_uid
            FROM positions_v4 p
            JOIN strategies_v4 s ON s.id = p.strategy_id
            WHERE COALESCE(p.emapattern_checked, FALSE) = FALSE
              AND COALESCE(s.market_watcher, FALSE) = TRUE
            ORDER BY p.created_at NULLS LAST
            LIMIT $1
            """,
            BATCH_LIMIT,
        )
    return [r["position_uid"] for r in rows]


# 🔸 Загрузить entry_price и strategy/direction для позиции
async def _load_position_base(pg, uid: str) -> Optional[dict]:
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT strategy_id, direction, entry_price
            FROM positions_v4
            WHERE position_uid = $1
            """,
            uid,
        )
    if not row or row["entry_price"] is None:
        return None
    return {"strategy_id": int(row["strategy_id"]), "direction": row["direction"], "entry_price": float(row["entry_price"])}


# 🔸 Загрузить EMA по TF для позиции из PIS (берём самые свежие bar_open_time на TF)
async def _load_emas_for_tf(pg, uid: str, tf: str) -> Optional[tuple[Dict[str, float], datetime]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT param_name, value_num, value_str, bar_open_time
            FROM positions_indicators_stat
            WHERE position_uid = $1
              AND timeframe = $2
              AND param_name = ANY($3::text[])
            ORDER BY bar_open_time DESC
            """,
            uid,
            tf,
            list(EMA_NAMES),
        )
    if not rows:
        return None

    emas: Dict[str, float] = {}
    latest_ts: Optional[datetime] = None
    seen: set[str] = set()
    for r in rows:
        pn = r["param_name"]
        if pn in seen:
            continue
        seen.add(pn)
        # prefer value_num, fallback to value_str
        v = r["value_num"]
        if v is None and r["value_str"] is not None:
            try:
                v = float(r["value_str"])
            except Exception:
                v = None
        if v is None:
            continue
        emas[pn] = float(v)
        if latest_ts is None or (r["bar_open_time"] and r["bar_open_time"] > latest_ts):
            latest_ts = r["bar_open_time"]

    if not all(n in emas for n in EMA_NAMES):
        return None
    return emas, (latest_ts or datetime.utcnow())


# 🔸 Обработать одну позицию: собрать паттерн по 3 TF и записать в PIS + проставить флаг
async def _process_position(pg, redis, uid: str) -> tuple[int, int]:
    base = await _load_position_base(pg, uid)
    if not base:
        # даже если нет entry_price — считаем позицию «обработанной», чтобы не застревать
        async with pg.acquire() as conn:
            await conn.execute("UPDATE positions_v4 SET emapattern_checked = TRUE WHERE position_uid = $1", uid)
        return 0, 1

    strat_id = base["strategy_id"]
    direction = base["direction"]
    entry_price = base["entry_price"]

    written = 0
    tfs_done = 0
    rows = []

    for tf in REQUIRED_TFS:
        loaded = await _load_emas_for_tf(pg, uid, tf)
        if not loaded:
            continue
        emas, bar_open_dt = loaded
        try:
            pattern_text = _build_emapattern_text(entry_price, emas)
        except Exception:
            continue
        pattern_id = _EMA_PATTERN_DICT.get(pattern_text)

        rows.append((
            uid, strat_id, direction, tf,
            EMAPATTERN_INSTANCE_ID[tf], "emapattern",
            pattern_text,
            (pattern_id if pattern_id is not None else None),
            bar_open_dt,
            None,
            None
        ))
        written += 1
        tfs_done += 1

    async with pg.acquire() as conn:
        async with conn.transaction():
            if rows:
                await conn.executemany(
                    """
                    INSERT INTO positions_indicators_stat
                      (position_uid, strategy_id, direction, timeframe,
                       instance_id, param_name, value_str, value_num,
                       bar_open_time, enabled_at, params_json)
                    VALUES
                      ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                    ON CONFLICT (position_uid, timeframe, instance_id, param_name, bar_open_time)
                    DO NOTHING
                    """,
                    rows
                )
            await conn.execute(
                "UPDATE positions_v4 SET emapattern_checked = TRUE WHERE position_uid = $1",
                uid
            )

    return written, 1


# 🔸 Основной воркер: проход по всем позициям «без флага» — батчами по 500, затем сон 96 часов
async def run_indicators_ema_pattern_backfill(pg, redis):
    log.info("🕒 Бэкофиллер стартует через 2 минуты…")
    await asyncio.sleep(SLEEP_START_SEC)

    # словарь паттернов
    try:
        await _load_emapattern_dict(pg)
    except Exception:
        log.exception("❌ Не удалось загрузить словарь EMA-паттернов")

    while True:
        try:
            total_positions = 0
            total_written = 0

            while True:
                uids = await _fetch_candidate_positions(pg)
                if not uids:
                    break

                log.info(f"🔎 Кандидатов на бэкофилл: {len(uids)} (батч до {BATCH_LIMIT})")

                # последовательно, без конкуренции
                for uid in uids:
                    try:
                        w, marked = await _process_position(pg, redis, uid)
                        total_positions += marked
                        total_written += w
                        log.debug(f"✅ uid={uid} emapattern rows written={w}")
                    except Exception:
                        log.exception(f"❌ Ошибка обработки позиции uid={uid}")

            log.info(f"📊 Бэкофилл завершён: позиций={total_positions}, emapattern-строк записано={total_written}. Сон на 96 часов.")
        except Exception:
            log.exception("❌ Критическая ошибка в бэкофиллере")

        # спим 96 часов и начинаем новый цикл (вдруг появились новые позиции)
        await asyncio.sleep(SLEEP_AFTER_DONE_SEC)