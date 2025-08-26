# position_decision_maker.py — универсальный решатель: этап 2 (EMA-паттерны, on-demand)

import asyncio
import logging
import json
import time
from datetime import datetime

log = logging.getLogger("POSITION_DECISION_MAKER")

REQUEST_STREAM  = "decision_request"
RESPONSE_STREAM = "decision_response"
GROUP           = "decision_maker_group"
CONSUMER        = "decision_maker_1"

EPSILON_REL = 0.0005  # 0.05%
TIMEFRAMES_DEFAULT = ("m5", "m15", "h1")
EMA_NAMES  = ("ema9", "ema21", "ema50", "ema100", "ema200")
EMA_LEN = {"EMA9": 9, "EMA21": 21, "EMA50": 50, "EMA100": 100, "EMA200": 200}

# кэш: pattern_text -> id
_PATTERN_ID = {}
# кэш инстансов: {"m5": {9: iid, 21: iid, ...}, ...}
_EMA_INSTANCES = {}
# указатель чтения indicator_response (для XREAD)
_IND_RESP_LAST_ID = "0-0"


# 🔸 consumer group
async def _ensure_group(redis):
    try:
        await redis.xgroup_create(REQUEST_STREAM, GROUP, id="$", mkstream=True)
        log.debug(f"Создана consumer group {GROUP} для {REQUEST_STREAM}")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug(f"Consumer group {GROUP} уже существует")
        else:
            log.exception("Ошибка создания consumer group")
            raise


# 🔸 ответ
async def _send_response(redis, req_id: str, decision: str, reason: str):
    payload = {
        "req_id": req_id or "",
        "decision": decision,
        "reason": reason,
        "responded_at": datetime.utcnow().isoformat(),
    }
    await redis.xadd(RESPONSE_STREAM, payload)
    log.debug(f"[RESP] req_id={req_id} decision={decision} reason={reason}")


# 🔸 валидация
def _validate_request(data: dict) -> tuple[bool, str]:
    required = ("req_id", "strategy_id", "symbol", "direction", "checks")
    for k in required:
        if data.get(k) in (None, ""):
            return False, f"missing_{k}"
    if data.get("direction", "").lower() not in ("long", "short"):
        return False, "bad_direction"
    if not isinstance(data.get("checks"), (list, tuple)) or len(data["checks"]) == 0:
        return False, "empty_checks"
    return True, "ok"


# 🔸 относительное равенство
def _rel_equal(a: float, b: float) -> bool:
    m = max(abs(a), abs(b), 1e-12)
    return abs(a - b) <= EPSILON_REL * m


# 🔸 построение паттерна
def _build_pattern(price: float, ema_vals: dict[str, float]) -> str:
    pairs = [("PRICE", float(price))]
    for name in EMA_NAMES:
        pairs.append((name.upper(), float(ema_vals[name])))

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

    canon_groups = []
    for g in groups:
        if "PRICE" in g:
            rest = [t for t in g if t != "PRICE"]
            rest.sort(key=lambda t: EMA_LEN[t])
            canon_groups.append(["PRICE"] + rest)
        else:
            gg = list(g)
            gg.sort(key=lambda t: EMA_LEN[t])
            canon_groups.append(gg)

    return " > ".join(" = ".join(g) for g in canon_groups)


# 🔸 загрузка словаря EMA-паттернов в кэш
async def _ensure_pattern_cache(pg):
    if _PATTERN_ID:
        return
    async with pg.acquire() as conn:
        rows = await conn.fetch("SELECT id, pattern_text FROM indicator_emapattern_dict")
    for r in rows:
        _PATTERN_ID[r["pattern_text"]] = int(r["id"])
    log.debug(f"[CACHE_LOADED] patterns={len(_PATTERN_ID)}")


# 🔸 загрузка iid для EMA (по длинам) в кэш
async def _ensure_ema_instances(pg):
    if _EMA_INSTANCES:
        return
    # ищем активные инстансы по EMA и TF {m5,m15,h1}, тянем длины
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT i.id, i.timeframe, p.value AS length
            FROM indicator_instances_v4 i
            JOIN indicator_parameters_v4 p ON p.instance_id = i.id AND p.param='length'
            WHERE i.enabled = true AND i.indicator = 'ema' AND i.timeframe IN ('m5','m15','h1')
            """
        )
    by_tf = {"m5": {}, "m15": {}, "h1": {}}
    for r in rows:
        try:
            tf = r["timeframe"]
            ln = int(r["length"])
            if ln in (9,21,50,100,200):
                by_tf[tf][ln] = int(r["id"])
        except Exception:
            continue
    _EMA_INSTANCES.update(by_tf)
    log.debug(f"[CACHE_LOADED] ema_instances={_EMA_INSTANCES}")

# 🔸 on-demand вызов индикатора: без таймаута, без гонок; ждём ровно свой ответ
async def _ondemand_indicator(redis, symbol: str, timeframe: str, instance_id: int, timeout_ms: int | None = None):
    import time, json

    now_ms = int(time.time() * 1000)

    # зафиксировать последний id ДО отправки запроса (чтобы не пропустить быстрый ответ)
    try:
        last = await redis.xrevrange("indicator_response", count=1)
        last_id = last[0][0] if last else "0-0"
    except Exception:
        last_id = "0-0"

    # отправить запрос
    req_id = await redis.xadd("indicator_request", {
        "symbol": symbol,
        "timeframe": timeframe,
        "instance_id": str(instance_id),
        "timestamp_ms": str(now_ms),
    })

    # блокирующее ожидание: XREAD BLOCK 0 — ждём ответ бесконечно
    while True:
        resp = await redis.xread(streams={"indicator_response": last_id}, count=64, block=0)
        if not resp:
            continue

        _, messages = resp[0]
        for mid, data in messages:
            last_id = mid
            if data.get("req_id") != req_id:
                continue
            status = (data.get("status") or "").lower()
            if status == "ok":
                try:
                    return json.loads(data.get("results") or "{}")
                except Exception:
                    return {}
            else:
                # любой error — мгновенный выход (без ожиданий)
                return {}
    
# 🔸 текущая цена
async def _get_price(redis, symbol: str) -> float | None:
    val = await redis.get(f"price:{symbol}")
    try:
        return float(val) if val is not None else None
    except Exception:
        return None


# 🔸 чтение агрегата из Redis
async def _read_aggr(redis, strategy_id: int, direction: str, tf: str, pattern_id: int):
    key = f"aggr:emapattern:{strategy_id}:{direction}:{tf}:{pattern_id}"
    res = await redis.hgetall(key)
    if not res:
        return None, key
    try:
        ct = int(res.get("count_trades", "0"))
        wr = float(res.get("winrate", "0"))
        return (ct, wr), key
    except Exception:
        return None, key


# 🔸 определение mirror-стратегии
async def _resolve_mirror(pg, strategy_id: int, direction: str, mirror_field: str | int | None):
    # явный mirror id в запросе (число) имеет приоритет
    if isinstance(mirror_field, int):
        return mirror_field
    # auto: читаем из strategies_v4
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT emamirrow, emamirrow_long, emamirrow_short
            FROM strategies_v4 WHERE id = $1
            """, strategy_id
        )
    if not row:
        return None
    if direction == "long" and row["emamirrow_long"]:
        return int(row["emamirrow_long"])
    if direction == "short" and row["emamirrow_short"]:
        return int(row["emamirrow_short"])
    if row["emamirrow"]:
        return int(row["emamirrow"])
    return None

# 🔸 обработка одного check kind=ema_pattern (с параллельным on-demand по 5 EMA внутри каждого TF)
async def _process_ema_check(pg, redis, strategy_id: int, symbol: str, direction: str, check: dict) -> str:
    # timeframes из запроса
    tfs = check.get("timeframes") or list(TIMEFRAMES_DEFAULT)

    # подготовить кэши
    await _ensure_pattern_cache(pg)
    await _ensure_ema_instances(pg)

    # mirror стратегия
    mirror = check.get("mirror")
    if mirror == "auto":
        mirror_id = await _resolve_mirror(pg, strategy_id, direction, "auto")
    elif isinstance(mirror, int):
        mirror_id = mirror
    else:
        mirror_id = await _resolve_mirror(pg, strategy_id, direction, "auto")
    target_strategy = mirror_id if mirror_id else strategy_id

    # текущая цена
    price = await _get_price(redis, symbol)
    if price is None:
        log.debug(f"[EMA] no_price symbol={symbol}")
        return "ignore"

    lengths_needed = (9, 21, 50, 100, 200)

    # по каждому TF: получить 5 EMA on-demand (параллельно)
    for tf in tfs:
        iid_map = _EMA_INSTANCES.get(tf) or {}
        if any(ln not in iid_map for ln in lengths_needed):
            log.debug(f"[EMA] not all EMA instances present tf={tf}")
            return "ignore"

        # параллельные запросы on-demand для всех длин EMA
        tasks = [
            _ondemand_indicator(redis, symbol, tf, iid_map[ln], timeout_ms=2500)
            for ln in lengths_needed
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        ema_vals: dict[str, float] = {}
        for ln, res in zip(lengths_needed, results):
            if isinstance(res, Exception) or not res:
                log.debug(f"[EMA] ondemand timeout/empty tf={tf} len={ln}")
                return "ignore"
            key = f"ema{ln}"
            v = res.get(key)
            if v is None:
                log.debug(f"[EMA] ondemand no key tf={tf} len={ln}")
                return "ignore"
            try:
                ema_vals[key] = float(v)
            except Exception:
                log.debug(f"[EMA] parse_error tf={tf} len={ln} val={v!r}")
                return "ignore"

        # построить паттерн -> id
        pattern_text = _build_pattern(price, ema_vals)
        pid = _PATTERN_ID.get(pattern_text)
        if pid is None:
            log.debug(f"[EMA] pattern_not_found tf={tf} text={pattern_text}")
            return "ignore"

        # прочитать агрегат по зеркальной/текущей стратегии
        aggr, key = await _read_aggr(redis, target_strategy, direction, tf, pid)
        if aggr is None:
            log.debug(f"[EMA] no_agg key={key}")
            return "ignore"

        count_trades, winrate = aggr
        if not (count_trades > 2 and winrate > 0.5):
            log.debug(f"[EMA] below_threshold tf={tf} count={count_trades} winrate={winrate}")
            return "deny"

    # все TF из списка прошли пороги
    return "allow"

# 🔸 главный цикл
async def run_position_decision_maker(pg, redis):
    await _ensure_group(redis)

    while True:
        try:
            resp = await redis.xreadgroup(
                groupname=GROUP,
                consumername=CONSUMER,
                streams={REQUEST_STREAM: ">"},
                count=20,
                block=2000
            )
            if not resp:
                continue

            to_ack = []
            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)
                    try:
                        req_id      = data.get("req_id")
                        strategy_id = int(data.get("strategy_id")) if data.get("strategy_id") else None
                        symbol      = data.get("symbol")
                        direction   = (data.get("direction") or "").lower()
                        checks_raw  = data.get("checks")
                        mirror_in   = data.get("mirror", "auto")

                        # checks может прийти строкой
                        checks = None
                        if isinstance(checks_raw, str):
                            try:
                                checks = json.loads(checks_raw)
                            except Exception:
                                checks = None
                        else:
                            checks = checks_raw
                        data["checks"] = checks

                        ok, reason = _validate_request(data)
                        if not ok:
                            log.warning(f"[REQ_SKIP] req_id={req_id} reason={reason}")
                            await _send_response(redis, req_id, "ignore", reason)
                            continue

                        log.debug(f"[REQ] req_id={req_id} strat={strategy_id} {symbol} dir={direction} checks={len(checks)}")

                        # поддерживаем пока только ema_pattern (следующие kind добавим позже)
                        # правило объединения по одному check — AND по его timeframes
                        decision = "ignore"
                        for check in checks:
                            kind = check.get("kind")
                            if kind != "ema_pattern":
                                decision = "ignore"
                                break
                            # прокинем mirror внутрь check, если пришёл на верхнем уровне
                            if mirror_in is not None and "mirror" not in check:
                                check["mirror"] = mirror_in
                            decision = await _process_ema_check(pg, redis, strategy_id, symbol, direction, check)
                            # один check в текущей версии; если позже будет несколько — комбинировать по AND
                        await _send_response(redis, req_id, decision, "ok" if decision=="allow" else ("below_thresholds" if decision=="deny" else "no_data"))

                    except Exception:
                        log.exception("Ошибка обработки decision_request")

            if to_ack:
                await redis.xack(REQUEST_STREAM, GROUP, *to_ack)

        except Exception as e:
            log.error(f"Ошибка в цикле POSITION_DECISION_MAKER: {e}", exc_info=True)
            await asyncio.sleep(2)