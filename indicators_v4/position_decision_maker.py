# position_decision_maker.py — универсальный решатель (EMA-паттерны + RSI/MFI/ADX buckets) без on-demand

import asyncio
import logging
import json
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

# ---- Кэш паттернов ----
_PATTERN_ID: dict[str, int] = {}  # pattern_text -> id


# ==========================
# Инфраструктура
# ==========================

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
    log.debug(f"[CACHE_LOADED] patterns={len(_PATTERN_ID)}")

# 🔸 текущая цена (mark-price)
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
# Чтение индикаторов (без on-demand)
# ==========================

async def _get_ind_value(redis, symbol: str, tf: str, param_name: str) -> float | None:
    # KV пишет compute_and_store: ind:{symbol}:{tf}:{param_name}
    val = await redis.get(f"ind:{symbol}:{tf}:{param_name}")
    try:
        return float(val) if val is not None else None
    except Exception:
        return None


# ==========================
# EMA-паттерны
# ==========================

# 🔸 обработка одного check kind=ema_pattern (берём EMA из KV + PRICE из mark-price)
async def _process_ema_check(pg, redis, strategy_id: int, symbol: str, direction: str, check: dict) -> str:
    tfs = check.get("timeframes") or list(TIMEFRAMES_DEFAULT)
    await _ensure_pattern_cache(pg)

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

    for tf in tfs:
        # читаем 5 EMA из KV последнего закрытого бара
        ema_vals = {}
        for ename in EMA_NAMES:
            v = await _get_ind_value(redis, symbol, tf, ename)
            if v is None:
                log.debug(f"[EMA] no_kv tf={tf} param={ename}")
                return "ignore"
            ema_vals[ename] = v

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

# 🔸 обработчик RSI-bucket
async def _process_rsi_bucket_check(pg, redis, strategy_id: int, symbol: str, direction: str, check: dict) -> str:
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
            val = await _get_ind_value(redis, symbol, tf, f"rsi{ln}")
            if val is None:
                log.debug(f"[RSI] no_kv tf={tf} len={ln}")
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
            val = await _get_ind_value(redis, symbol, tf, f"mfi{ln}")
            if val is None:
                log.debug(f"[MFI] no_kv tf={tf} len={ln}")
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

# 🔸 обработчик ADX-bucket (берём _adx из KV)
async def _process_adx_bucket_check(pg, redis, strategy_id: int, symbol: str, direction: str, check: dict) -> str:
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
            val = await _get_ind_value(redis, symbol, tf, f"adx_dmi{ln}_adx")
            if val is None:
                log.debug(f"[ADX] no_kv tf={tf} len={ln}")
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