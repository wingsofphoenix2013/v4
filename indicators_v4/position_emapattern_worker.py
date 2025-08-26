# position_emapattern_worker.py — этап 3: считаем EMA-паттерны по закрытиям и логируем

import asyncio
import logging

log = logging.getLogger("IND_EMA_PATTERN_DICT")

STREAM   = "signal_log_queue"
GROUP    = "ema_pattern_aggr_group"
CONSUMER = "ema_aggr_1"

TIMEFRAMES = ("m5", "m15", "h1")
EMA_NAMES  = ("ema9", "ema21", "ema50", "ema100", "ema200")
EPSILON_REL = 0.0005  # 0.05%

EMA_LEN = {"ema9": 9, "ema21": 21, "ema50": 50, "ema100": 100, "ema200": 200}


# 🔸 Инициализация consumer group для стрима
async def _ensure_group(redis):
    try:
        await redis.xgroup_create(STREAM, GROUP, id="$", mkstream=True)
        log.info(f"Создана consumer group {GROUP} для {STREAM}")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug(f"Consumer group {GROUP} уже существует")
        else:
            log.exception("Ошибка создания consumer group")
            raise


# 🔸 Относительное равенство с порогом 0.05%
def _rel_equal(a: float, b: float) -> bool:
    m = max(abs(a), abs(b), 1e-12)
    return abs(a - b) <= EPSILON_REL * m


# 🔸 Построение паттерна из значений PRICE и 5 EMA
def _build_pattern_text(price: float, emas: dict[str, float]) -> str:
    # собираем список токенов с числами
    pairs = [("PRICE", float(price))]
    for ename in EMA_NAMES:
        pairs.append((ename.upper(), float(emas[ename])))

    # сортировка по убыванию значения
    pairs.sort(key=lambda kv: kv[1], reverse=True)

    # нарезаем на группы равенства по соседям
    groups: list[list[str]] = []
    cur_group: list[tuple[str, float]] = []
    for token, val in pairs:
        if not cur_group:
            cur_group = [(token, val)]
            continue
        ref_val = cur_group[0][1]
        if _rel_equal(val, ref_val):
            cur_group.append((token, val))
        else:
            groups.append([t for t, _ in cur_group])
            cur_group = [(token, val)]
    if cur_group:
        groups.append([t for t, _ in cur_group])

    # канонизация порядка внутри группы
    canon_groups: list[list[str]] = []
    for g in groups:
        if "PRICE" in g:
            rest = [t for t in g if t != "PRICE"]
            rest.sort(key=lambda t: EMA_LEN[t.lower()])
            canon_groups.append(["PRICE"] + rest)
        else:
            gg = list(g)
            gg.sort(key=lambda t: EMA_LEN[t.lower()])
            canon_groups.append(gg)

    # склейка в строку
    return " > ".join(" = ".join(g) for g in canon_groups)


# 🔸 Загрузка позиции (entry_price, strategy_id, direction, pnl)
async def _load_position(pg, position_uid: str):
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT strategy_id, direction, entry_price, pnl
            FROM positions_v4
            WHERE position_uid = $1
            """,
            position_uid,
        )
    return row


# 🔸 Загрузка 15 EMA по трём ТФ для позиции (берём последние по snapshot_at на всякий случай)
async def _load_position_emas(pg, position_uid: str) -> dict[str, dict[str, float]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (timeframe, param_name)
                   timeframe, param_name, value_num, value_str
            FROM positions_indicators_stat
            WHERE position_uid = $1
              AND timeframe = ANY($2::text[])
              AND param_name = ANY($3::text[])
            ORDER BY timeframe, param_name, snapshot_at DESC
            """,
            position_uid,
            list(TIMEFRAMES),
            list(EMA_NAMES),
        )

    by_tf: dict[str, dict[str, float]] = {tf: {} for tf in TIMEFRAMES}
    for r in rows:
        tf = r["timeframe"]
        pn = r["param_name"]
        vnum = r["value_num"]
        vstr = r["value_str"]
        try:
            val = float(vnum) if vnum is not None else float(vstr) if vstr is not None else None
        except Exception:
            val = None
        if val is not None and tf in by_tf:
            by_tf[tf][pn] = val

    return by_tf


# 🔸 Точка входа воркера: читаем закрытия, тянем данные, строим паттерны и логируем
async def run_position_emapattern_worker(pg, redis):
    await _ensure_group(redis)

    while True:
        try:
            resp = await redis.xreadgroup(
                groupname=GROUP,
                consumername=CONSUMER,
                streams={STREAM: ">"},
                count=50,
                block=2000
            )
            if not resp:
                continue

            to_ack = []
            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)
                    try:
                        if data.get("status") != "closed":
                            continue

                        position_uid = data.get("position_uid")
                        if not position_uid:
                            continue

                        # грузим позицию
                        pos = await _load_position(pg, position_uid)
                        if not pos:
                            log.warning(f"[SKIP_NO_POS] position_uid={position_uid}")
                            continue

                        strategy_id = pos["strategy_id"]
                        direction   = pos["direction"]
                        entry_price = pos["entry_price"]
                        pnl         = pos["pnl"]

                        if entry_price is None or direction is None:
                            log.warning(f"[SKIP_BAD_POS] position_uid={position_uid} entry_price={entry_price} direction={direction}")
                            continue

                        # грузим EMA
                        emas_by_tf = await _load_position_emas(pg, position_uid)

                        # проверяем полноту: нужны 5 EMA на каждом TF
                        incomplete = [tf for tf in TIMEFRAMES if any(n not in emas_by_tf.get(tf, {}) for n in EMA_NAMES)]
                        if incomplete:
                            miss = {tf: [n for n in EMA_NAMES if n not in emas_by_tf.get(tf, {})] for tf in incomplete}
                            log.info(f"[SKIP_INCOMPLETE] position_uid={position_uid} missing={miss}")
                            continue

                        # считаем паттерны по всем TF
                        for tf in TIMEFRAMES:
                            pattern_text = _build_pattern_text(float(entry_price), emas_by_tf[tf])
                            log.info(
                                f"[PATTERN] position_uid={position_uid} strategy_id={strategy_id} dir={direction} tf={tf} pnl={pnl} pattern={pattern_text}"
                            )

                    except Exception:
                        log.exception("Ошибка обработки события closed")

            if to_ack:
                await redis.xack(STREAM, GROUP, *to_ack)

        except Exception as e:
            log.error(f"Ошибка в цикле IND_EMA_PATTERN_DICT: {e}", exc_info=True)
            await asyncio.sleep(2)