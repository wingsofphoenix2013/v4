# position_decision_maker.py — универсальный решатель (EMA-паттерны + RSI/MFI/ADX buckets), без таймаутов

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

# ---- Общие настройки ----
EPSILON_REL = 0.0005  # 0.05% для сравнения в EMA-паттернах
TIMEFRAMES_DEFAULT = ("m5", "m15", "h1")
EMA_NAMES  = ("ema9", "ema21", "ema50", "ema100", "ema200")
EMA_LEN = {"EMA9": 9, "EMA21": 21, "EMA50": 50, "EMA100": 100, "EMA200": 200}

# ---- Кэши ----
_PATTERN_ID: dict[str, int] = {}             # pattern_text -> id (EMA)
_EMA_INSTANCES: dict[str, dict[int, int]] = {}   # tf -> {length -> instance_id}
_RSI_INSTANCES: dict[str, dict[int, int]] = {}   # tf -> {length -> instance_id}
_MFI_INSTANCES: dict[str, dict[int, int]] = {}   # tf -> {length -> instance_id}
_ADX_INSTANCES: dict[str, dict[int, int]] = {}   # tf -> {length -> instance_id}

# ==========================
# Инфраструктура
# ==========================

# 🔸 consumer group
async def _ensure_group(redis):
    try:
        await redis.xgroup_create(REQUEST_STREAM, GROUP, id="$", mkstream=True)
        log.info(f"Создана consumer group {GROUP} для {REQUEST_STREAM}")
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
    log.info(f"[RESP] req_id={req_id} decision={decision} reason={reason}")

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

# ==========================
# Вспомогательное (EMA)
# ==========================

# 🔸 сравнение с относительным порогом
def _rel_equal(a: float, b: float) -> bool:
    m = max(abs(a), abs(b), 1e-12)
    return abs(a - b) <= EPSILON_REL * m

# 🔸 построение EMA-паттерна
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

    # канонизация внутри групп
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

# 🔸 загрузка словаря EMA-паттернов
async def _ensure_pattern_cache(pg):
    if _PATTERN_ID:
        return
    async with pg.acquire() as conn:
        rows = await conn.fetch("SELECT id, pattern_text FROM indicator_emapattern_dict")
    for r in rows:
        _PATTERN_ID[r["pattern_text"]] = int(r["id"])
    log.info(f"[CACHE_LOADED] patterns={len(_PATTERN_ID)}")

# 🔸 загрузка iid для EMA/RSI/MFI/ADX (по длинам) в кэш
async def _ensure_indicator_instances(pg):
    # EMA
    if not _EMA_INSTANCES:
        async with pg.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT i.id, i.indicator, i.timeframe, p.value AS length
                FROM indicator_instances_v4 i
                JOIN indicator_parameters_v4 p ON p.instance_id = i.id AND p.param='length'
                WHERE i.enabled = true
                  AND i.indicator IN ('ema','rsi','mfi','adx_dmi')
                  AND i.timeframe IN ('m5','m15','h1')
                """
            )
        ema_by_tf, rsi_by_tf, mfi_by_tf, adx_by_tf = {"m5": {}, "m15": {}, "h1": {}}, {"m5": {}, "m15": {}, "h1": {}}, {"m5": {}, "m15": {}, "h1": {}}, {"m5": {}, "m15": {}, "h1": {}}
        for r in rows:
            ind = r["indicator"]
            tf = r["timeframe"]
            try:
                ln = int(r["length"])
            except Exception:
                continue
            if ind == "ema" and ln in (9,21,50,100,200):
                ema_by_tf[tf][ln] = int(r["id"])
            elif ind == "rsi" and ln in (7,14,21):
                rsi_by_tf[tf][ln] = int(r["id"])
            elif ind == "mfi" and ln in (14,):
                mfi_by_tf[tf][ln] = int(r["id"])
            elif ind == "adx_dmi" and ln in (14,28):
                adx_by_tf[tf][ln] = int(r["id"])
        _EMA_INSTANCES.update(ema_by_tf)
        _RSI_INSTANCES.update(rsi_by_tf)
        _MFI_INSTANCES.update(mfi_by_tf)
        _ADX_INSTANCES.update(adx_by_tf)
        log.info(f"[CACHE_LOADED] ema={_EMA_INSTANCES} rsi={_RSI_INSTANCES} mfi={_MFI_INSTANCES} adx={_ADX_INSTANCES}")

# 🔸 текущая цена
async def _get_price(redis, symbol: str) -> float | None:
    val = await redis.get(f"price:{symbol}")
    try:
        return float(val) if val is not None else None
    except Exception:
        return None

# 🔸 чтение EMA-агрегата из Redis (hash)
async def _read_ema_aggr(redis, strategy_id: int, direction: str, tf: str, pattern_id: int):
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

# ==========================
# On-demand запрос индикатора (без таймаутов)
# ==========================

# 🔸 on-demand вызов индикатора: без таймаута; ждём ровно свой ответ
async def _ondemand_indicator(redis, symbol: str, timeframe: str, instance_id: int):
    # зафиксировать текущий «хвост» до запроса
    try:
        last = await redis.xrevrange("indicator_response", count=1)
        last_id = last[0][0] if last else "0-0"
    except Exception:
        last_id = "0-0"

    # отправить запрос
    now_ms = int(time.time() * 1000)
    req_id = await redis.xadd("indicator_request", {
        "symbol": symbol,
        "timeframe": timeframe,
        "instance_id": str(instance_id),
        "timestamp_ms": str(now_ms),
    })

    # блокирующее ожидание
    while True:
        resp = await redis.xread(streams={"indicator_response": last_id}, count=64, block=0)  # BLOCK 0
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
                # любой error → мгновенно возвращаем пустой результат
                return {}

# ==========================
# Зеркало
# ==========================

# 🔸 определение mirror-стратегии
async def _resolve_mirror(pg, strategy_id: int, direction: str, mirror_field):
    if isinstance(mirror_field, int):
        return mirror_field
    # auto
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT emamirrow, emamirrow_long, emamirrow_short FROM strategies_v4 WHERE id = $1",
            strategy_id
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

# ==========================
# EMA-паттерны
# ==========================

# 🔸 обработка одного check kind=ema_pattern (параллельно 5 EMA внутри каждого TF)
async def _process_ema_check(pg, redis, strategy_id: int, symbol: str, direction: str, check: dict) -> str:
    tfs = check.get("timeframes") or list(TIMEFRAMES_DEFAULT)
    await _ensure_pattern_cache(pg)
    await _ensure_indicator_instances(pg)

    mirror = check.get("mirror")
    if mirror == "auto":
        mirror_id = await _resolve_mirror(pg, strategy_id, direction, "auto")
    elif isinstance(mirror, int):
        mirror_id = mirror
    else:
        mirror_id = await _resolve_mirror(pg, strategy_id, direction, "auto")
    target_strategy = mirror_id if mirror_id else strategy_id

    price = await _get_price(redis, symbol)
    if price is None:
        log.debug(f"[EMA] no_price symbol={symbol}")
        return "ignore"

    lengths_needed = (9, 21, 50, 100, 200)
    for tf in tfs:
        iid_map = _EMA_INSTANCES.get(tf) or {}
        if any(ln not in iid_map for ln in lengths_needed):
            log.debug(f"[EMA] not all EMA instances present tf={tf}")
            return "ignore"

        # параллельный on-demand по 5 EMA
        tasks = [_ondemand_indicator(redis, symbol, tf, iid_map[ln]) for ln in lengths_needed]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        ema_vals = {}
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

        pattern_text = _build_pattern(price, ema_vals)
        pid = _PATTERN_ID.get(pattern_text)
        if pid is None:
            log.debug(f"[EMA] pattern_not_found tf={tf} text={pattern_text}")
            return "ignore"

        aggr, key = await _read_ema_aggr(redis, target_strategy, direction, tf, pid)
        if aggr is None:
            log.debug(f"[EMA] no_agg key={key}")
            return "ignore"

        count_trades, winrate = aggr
        if not (count_trades > 2 and winrate > 0.5):
            log.debug(f"[EMA] below_threshold tf={tf} count={count_trades} winrate={winrate}")
            return "deny"

    return "allow"

# ==========================
# Buckets (RSI/MFI/ADX)
# ==========================

# 🔸 квантование 0..100 с шагом step (по умолчанию 5)
def _bin_value_0_100(value: float, step: int = 5) -> int:
    v = max(0.0, min(100.0, float(value)))
    b = int(v // step) * step
    return min(100, b)

# 🔸 чтение bucket-агрегата (строго JSON-строка)
async def _read_bucket_aggr(redis, strategy_id: int, direction: str, tf: str,
                            indicator: str, param_name: str, bucket_key: str, spec: str | int):
    key = f"agg:{strategy_id}:{direction}:{tf}:{indicator}:{param_name}:{bucket_key}:{spec}"
    try:
        s = await redis.get(key)
        if not s:
            return None, key
        obj = json.loads(s)
        ct = int(obj.get("positions_closed") or obj.get("count_trades") or 0)
        wr = float(obj.get("winrate") or 0)
        return (ct, wr), key
    except Exception as e:
        log.warning(f"[BUCKET] error reading key={key}: {e}")
        return None, key

# 🔸 on-demand чтение значения RSI/MFI/ADX (возвращает float или None)
async def _ondemand_param_value(redis, symbol: str, tf: str, instances_map: dict[str, dict[int, int]],
                                length: int, out_key: str) -> float | None:
    iid_map = instances_map.get(tf) or {}
    iid = iid_map.get(length)
    if not iid:
        return None
    res = await _ondemand_indicator(redis, symbol, tf, iid)
    if not res:
        return None
    v = res.get(out_key)
    try:
        return float(v) if v is not None else None
    except Exception:
        return None

# 🔸 обработчик RSI-bucket
async def _process_rsi_bucket_check(pg, redis, strategy_id: int, symbol: str, direction: str, check: dict) -> str:
    await _ensure_indicator_instances(pg)
    tfs = check.get("timeframes") or []
    lengths = check.get("lengths") or {}
    step = int(check.get("bin_step") or 5)

    mirror = check.get("mirror")
    if mirror == "auto":
        mirror_id = await _resolve_mirror(pg, strategy_id, direction, "auto")
    elif isinstance(mirror, int):
        mirror_id = mirror
    else:
        mirror_id = await _resolve_mirror(pg, strategy_id, direction, "auto")
    target_strategy = mirror_id if mirror_id else strategy_id

    for tf in tfs:
        lens = lengths.get(tf) or []
        for ln in lens:
            val = await _ondemand_param_value(redis, symbol, tf, _RSI_INSTANCES, ln, f"rsi{ln}")
            if val is None:
                log.debug(f"[RSI] ondemand no value tf={tf} len={ln}")
                return "ignore"
            spec = _bin_value_0_100(val, step=step)  # bucket_spec
            param_name = f"rsi{ln}"
            aggr, key = await _read_bucket_aggr(redis, target_strategy, direction, tf, "rsi", param_name, "value", spec)
            if aggr is None:
                log.debug(f"[RSI] no_agg key={key}")
                return "ignore"
            count_trades, winrate = aggr
            if not (count_trades > 2 and winrate > 0.5):
                log.debug(f"[RSI] below_threshold tf={tf} len={ln} count={count_trades} winrate={winrate}")
                return "deny"
    return "allow"

# 🔸 обработчик MFI-bucket
async def _process_mfi_bucket_check(pg, redis, strategy_id: int, symbol: str, direction: str, check: dict) -> str:
    await _ensure_indicator_instances(pg)
    tfs = check.get("timeframes") or []
    lengths = check.get("lengths") or {}
    step = int(check.get("bin_step") or 5)

    mirror = check.get("mirror")
    if mirror == "auto":
        mirror_id = await _resolve_mirror(pg, strategy_id, direction, "auto")
    elif isinstance(mirror, int):
        mirror_id = mirror
    else:
        mirror_id = await _resolve_mirror(pg, strategy_id, direction, "auto")
    target_strategy = mirror_id if mirror_id else strategy_id

    for tf in tfs:
        lens = lengths.get(tf) or []
        for ln in lens:
            val = await _ondemand_param_value(redis, symbol, tf, _MFI_INSTANCES, ln, f"mfi{ln}")
            if val is None:
                log.debug(f"[MFI] ondemand no value tf={tf} len={ln}")
                return "ignore"
            spec = _bin_value_0_100(val, step=step)
            param_name = f"mfi{ln}"
            aggr, key = await _read_bucket_aggr(redis, target_strategy, direction, tf, "mfi", param_name, "value", spec)
            if aggr is None:
                log.debug(f"[MFI] no_agg key={key}")
                return "ignore"
            count_trades, winrate = aggr
            if not (count_trades > 2 and winrate > 0.5):
                log.debug(f"[MFI] below_threshold tf={tf} len={ln} count={count_trades} winrate={winrate}")
                return "deny"
    return "allow"

# 🔸 обработчик ADX-bucket (берём _adx из adx_dmi)
async def _process_adx_bucket_check(pg, redis, strategy_id: int, symbol: str, direction: str, check: dict) -> str:
    await _ensure_indicator_instances(pg)
    tfs = check.get("timeframes") or []
    lengths = check.get("lengths") or {}
    step = int(check.get("bin_step") or 5)

    mirror = check.get("mirror")
    if mirror == "auto":
        mirror_id = await _resolve_mirror(pg, strategy_id, direction, "auto")
    elif isinstance(mirror, int):
        mirror_id = mirror
    else:
        mirror_id = await _resolve_mirror(pg, strategy_id, direction, "auto")
    target_strategy = mirror_id if mirror_id else strategy_id

    for tf in tfs:
        lens = lengths.get(tf) or []
        for ln in lens:
            # on-demand возвращает adx_dmi{L}_adx
            iid_map = _ADX_INSTANCES.get(tf) or {}
            iid = iid_map.get(ln)
            if not iid:
                log.debug(f"[ADX] no instance tf={tf} len={ln}")
                return "ignore"
            res = await _ondemand_indicator(redis, symbol, tf, iid)
            if not res:
                log.debug(f"[ADX] ondemand empty tf={tf} len={ln}")
                return "ignore"
            key = f"adx_dmi{ln}_adx"
            try:
                val = float(res.get(key)) if res.get(key) is not None else None
            except Exception:
                val = None
            if val is None:
                log.debug(f"[ADX] ondemand no key tf={tf} len={ln}")
                return "ignore"

            spec = _bin_value_0_100(val, step=step)
            param_name = f"adx_dmi{ln}"
            aggr, key_r = await _read_bucket_aggr(redis, target_strategy, direction, tf, "adx_dmi", param_name, "adx", spec)
            if aggr is None:
                log.debug(f"[ADX] no_agg key={key_r}")
                return "ignore"
            count_trades, winrate = aggr
            if not (count_trades > 2 and winrate > 0.5):
                log.debug(f"[ADX] below_threshold tf={tf} len={ln} count={count_trades} winrate={winrate}")
                return "deny"
    return "allow"

# ==========================
# Главный цикл
# ==========================

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

                        # распакуем checks (могут прийти строкой)
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

                        log.info(f"[REQ] req_id={req_id} strat={strategy_id} {symbol} dir={direction} checks={len(checks)}")

                        decision = "ignore"
                        overall_ok = True
                        for check in checks:
                            kind = (check.get("kind") or "").lower()
                            # прокинем mirror внутрь check при необходимости
                            if mirror_in is not None and "mirror" not in check:
                                check["mirror"] = mirror_in

                            if kind == "ema_pattern":
                                d = await _process_ema_check(pg, redis, strategy_id, symbol, direction, check)
                            elif kind == "rsi_bucket":
                                d = await _process_rsi_bucket_check(pg, redis, strategy_id, symbol, direction, check)
                            elif kind == "mfi_bucket":
                                d = await _process_mfi_bucket_check(pg, redis, strategy_id, symbol, direction, check)
                            elif kind == "adx_bucket":
                                d = await _process_adx_bucket_check(pg, redis, strategy_id, symbol, direction, check)
                            else:
                                d = "ignore"
                                overall_ok = False
                                log.debug(f"[REQ_BAD_KIND] {kind}")

                            if d == "ignore":
                                decision = "ignore"
                                overall_ok = False
                                break
                            if d == "deny":
                                decision = "deny"
                                overall_ok = False
                                break

                        if overall_ok:
                            decision = "allow"

                        await _send_response(
                            redis, req_id, decision,
                            "ok" if decision == "allow" else ("below_thresholds" if decision == "deny" else "no_data")
                        )

                    except Exception:
                        log.exception("Ошибка обработки decision_request")

            if to_ack:
                await redis.xack(REQUEST_STREAM, GROUP, *to_ack)

        except Exception as e:
            log.error(f"Ошибка в цикле POSITION_DECISION_MAKER: {e}", exc_info=True)
            await asyncio.sleep(2)