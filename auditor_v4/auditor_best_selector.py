# 🔸 auditor_best_selector.py — оркестратор витрины «лучшая идея по стратегии/направлению»
#     Принимает результаты от воркеров-идей через Redis Stream и поддерживает таблицу auditor_current_best.
#     Протокол сообщений (Stream: auditor:best:candidates, type="result"):
#       { run_id, strategy_id, direction, idea_key, variant_key, primary_window, eligible: true|false,
#         roi_selected_pct?, roi_all_pct?, wr_selected_pct?, wr_all_pct?, coverage_pct?, decision_confidence?,
#         config_json?, source_table?, source_run_id?, event_uid? }
#     Идея обязана прислать message ВСЕГДА. Если eligible=false — метрики не присылать.
#     Оркестратор ждёт результаты от всех ACTIVE_IDEAS и:
#       — выбирает ЛУЧШЕГО кандидата по delta_roi_pp (далее roi_selected, confidence, coverage) и UPSERT’ит в auditor_current_best;
#       — если ни одной eligible=true → удаляет запись из auditor_current_best (стратегия на паузе).

# 🔸 Импорты
import asyncio
import json
import logging
from typing import Any, Dict, Optional, Tuple, List

import auditor_infra as infra

# 🔸 Логгер
log = logging.getLogger("AUD_BEST")

# 🔸 Константы / параметры (правим здесь, не через ENV)
STREAM_NAME = "auditor:best:candidates"
GROUP_NAME = "AUD_BEST_GROUP"
CONSUMER_NAME = "AUD_BEST_SELECTOR"

# список активных идей, от которых оркестратор ждёт сообщения result (eligible true/false)
ACTIVE_IDEAS = {"emacross_cs", "ema200_side"}

# тайминги и TTL
XREAD_BLOCK_MS = 30_000
XREAD_COUNT = 128
LOCK_TTL_SEC = 15
EPOCH_TTL_SEC = 60 * 60
SEEN_TTL_SEC = 24 * 60 * 60

# ключи Redis
def _k_lock(sid: int, direction: str) -> str:
    return f"aud:best:lock:{sid}:{direction}"

def _k_epoch(sid: int, direction: str) -> str:
    return f"aud:best:epoch:{sid}:{direction}"

def _k_done(sid: int, direction: str, run_id: int) -> str:
    return f"aud:best:done:{sid}:{direction}:{run_id}"

SEEN_SET = "aud:best:seen"

# 🔸 Вспомогательные утилиты
def _as_bool(v: Any) -> bool:
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "t")

def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default

def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default

def _normalize_dir(v: Any) -> str:
    s = str(v or "").strip().lower()
    return "long" if s == "long" else "short" if s == "short" else s

def _rank_better(cur: Optional[Dict[str, Any]], cand: Dict[str, Any]) -> bool:
    """
    Возвращает True, если cand лучше cur по правилу:
    eligible(1/0) → delta_roi_pp ↓ → roi_selected_pct ↓ → decision_confidence ↓ → coverage_pct ↓
    """
    if cur is None:
        return True
    # извлекаем с безопасной типизацией
    cur_key = (
        1.0 if _as_bool(cur.get("eligible", "true")) else 0.0,
        _safe_float(cur.get("delta_roi_pp"), float("-inf")),
        _safe_float(cur.get("roi_selected_pct"), float("-inf")),
        _safe_float(cur.get("decision_confidence"), 0.0),
        _safe_float(cur.get("coverage_pct"), 0.0),
    )
    cand_key = (
        1.0 if _as_bool(cand.get("eligible", "true")) else 0.0,
        _safe_float(cand.get("delta_roi_pp"), float("-inf")),
        _safe_float(cand.get("roi_selected_pct"), float("-inf")),
        _safe_float(cand.get("decision_confidence"), 0.0),
        _safe_float(cand.get("coverage_pct"), 0.0),
    )
    return cand_key > cur_key

# 🔸 Основной воркер
async def run_auditor_best_selector():
    # условия достаточности
    if infra.redis_client is None or infra.pg_pool is None:
        log.info("❌ Пропуск auditor_best_selector: Redis/PG не инициализированы")
        return

    # создать consumer group идемпотентно
    try:
        await infra.redis_client.xgroup_create(name=STREAM_NAME, groupname=GROUP_NAME, id="$", mkstream=True)
        log.info("📡 AUD_BEST: создана consumer group для стрима %s", STREAM_NAME)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            log.exception("❌ AUD_BEST: ошибка создания consumer group")
            return

    log.info("🚀 AUD_BEST: старт воркера (stream=%s, group=%s, consumer=%s)", STREAM_NAME, GROUP_NAME, CONSUMER_NAME)

    # основной цикл чтения
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={STREAM_NAME: ">"},
                count=XREAD_COUNT,
                block=XREAD_BLOCK_MS,
            )
            if not resp:
                continue

            acks: List[str] = []
            for stream_name, msgs in resp:
                for msg_id, fields in msgs:
                    try:
                        await _handle_message(fields)
                        acks.append(msg_id)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        log.exception("❌ AUD_BEST: ошибка обработки сообщения %s", msg_id)

            if acks:
                try:
                    await infra.redis_client.xack(STREAM_NAME, GROUP_NAME, *acks)
                except Exception:
                    log.exception("⚠️ AUD_BEST: ошибка ACK (ids=%s)", acks)

        except asyncio.CancelledError:
            log.info("⏹️ AUD_BEST: остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ AUD_BEST: ошибка цикла — пауза 5 секунд")
            await asyncio.sleep(5)

# 🔸 Обработка одного сообщения type="result"
async def _handle_message(fields: Dict[str, str]):
    # дедупликатор по event_uid (если прислан)
    event_uid = fields.get("event_uid", "")
    if event_uid:
        added = await infra.redis_client.sadd(SEEN_SET, event_uid)
        if added == 0:
            # уже видели
            return
        await infra.redis_client.expire(SEEN_SET, SEEN_TTL_SEC)

    # парсим обязательные поля
    msg_type = str(fields.get("type", "result")).lower()
    if msg_type != "result":
        # игнорируем иные типы
        return

    run_id = _safe_int(fields.get("run_id"))
    sid = _safe_int(fields.get("strategy_id"))
    direction = _normalize_dir(fields.get("direction"))
    idea_key = str(fields.get("idea_key", "")).strip()
    variant_key = str(fields.get("variant_key", "")).strip()
    primary_window = str(fields.get("primary_window", "")).strip()
    eligible = _as_bool(fields.get("eligible", "false"))

    if not run_id or not sid or direction not in ("long", "short") or not idea_key:
        # неполное сообщение — пропускаем
        log.info("ℹ️ AUD_BEST: пропуск некорректного сообщения (run_id=%s sid=%s dir=%s idea=%s)", run_id, sid, direction, idea_key)
        return

    # последовательность по стратегии
    lock_key = _k_lock(sid, direction)
    got_lock = await infra.redis_client.set(lock_key, "1", nx=True, ex=LOCK_TTL_SEC)
    if not got_lock:
        # не получили лок — обработаем позже
        return

    try:
        # epoch и done-ключи
        epoch_key = _k_epoch(sid, direction)
        done_key = _k_done(sid, direction, run_id)

        # если в epoch лежит другой run — обнуляем (начался новый раунд)
        epoch = await infra.redis_client.hgetall(epoch_key)
        if epoch and _safe_int(epoch.get("run_id")) != run_id:
            # сравнение по номеру не критично — просто сброс
            await infra.redis_client.delete(epoch_key)

        # отметить идею как завершившуюся (eligible true/false — неважно)
        await infra.redis_client.hset(done_key, idea_key, "1")
        await infra.redis_client.expire(done_key, EPOCH_TTL_SEC)

        # если кандидат eligible=true — обновить «лучшего» при необходимости
        if eligible:
            roi_sel = _safe_float(fields.get("roi_selected_pct"))
            roi_all = _safe_float(fields.get("roi_all_pct"))
            delta_roi_pp = roi_sel - roi_all
            wr_sel = _safe_float(fields.get("wr_selected_pct"))
            wr_all = _safe_float(fields.get("wr_all_pct"))
            coverage_pct = _safe_float(fields.get("coverage_pct"))
            decision_confidence = _safe_float(fields.get("decision_confidence"))
            source_table = str(fields.get("source_table", "")).strip()
            source_run_id = _safe_int(fields.get("source_run_id"))
            cfg_raw = fields.get("config_json")
            try:
                config_json = json.dumps(json.loads(cfg_raw)) if cfg_raw else "{}"
            except Exception:
                config_json = "{}"

            cand = {
                "eligible": "true",
                "run_id": run_id,
                "idea_key": idea_key,
                "variant_key": variant_key,
                "primary_window": primary_window,
                "roi_selected_pct": roi_sel,
                "roi_all_pct": roi_all,
                "delta_roi_pp": delta_roi_pp,
                "wr_selected_pct": wr_sel,
                "wr_all_pct": wr_all,
                "coverage_pct": coverage_pct,
                "decision_confidence": decision_confidence,
                "config_json": config_json,
                "source_table": source_table,
                "source_run_id": source_run_id,
            }

            cur = None
            epoch = await infra.redis_client.hgetall(epoch_key)
            if epoch:
                cur = {
                    "eligible": epoch.get("eligible", "true"),
                    "run_id": _safe_int(epoch.get("run_id")),
                    "idea_key": epoch.get("idea_key"),
                    "variant_key": epoch.get("variant_key"),
                    "primary_window": epoch.get("primary_window"),
                    "roi_selected_pct": _safe_float(epoch.get("roi_selected_pct")),
                    "roi_all_pct": _safe_float(epoch.get("roi_all_pct")),
                    "delta_roi_pp": _safe_float(epoch.get("delta_roi_pp"), float("-inf")),
                    "wr_selected_pct": _safe_float(epoch.get("wr_selected_pct")),
                    "wr_all_pct": _safe_float(epoch.get("wr_all_pct")),
                    "coverage_pct": _safe_float(epoch.get("coverage_pct")),
                    "decision_confidence": _safe_float(epoch.get("decision_confidence")),
                }

            if _rank_better(cur, cand):
                # перезаписываем лучшего кандидата
                await infra.redis_client.hset(
                    epoch_key,
                    mapping={
                        "eligible": "true",
                        "run_id": str(run_id),
                        "idea_key": idea_key,
                        "variant_key": variant_key,
                        "primary_window": primary_window,
                        "roi_selected_pct": f"{roi_sel}",
                        "roi_all_pct": f"{roi_all}",
                        "delta_roi_pp": f"{delta_roi_pp}",
                        "wr_selected_pct": f"{wr_sel}",
                        "wr_all_pct": f"{wr_all}",
                        "coverage_pct": f"{coverage_pct}",
                        "decision_confidence": f"{decision_confidence}",
                        "config_json": config_json,
                        "source_table": source_table,
                        "source_run_id": str(source_run_id),
                        "updated_at": str(run_id),  # символическая отметка; при желании можно положить ISO-время
                    },
                )
                await infra.redis_client.expire(epoch_key, EPOCH_TTL_SEC)

        # проверка завершения всех активных идей
        done_count = await infra.redis_client.hlen(done_key)
        if done_count >= len(ACTIVE_IDEAS):
            # финализация раунда: апдейт/очистка витрины
            await _finalize_round(sid, direction, run_id, epoch_key)
            # очистка состояния
            await infra.redis_client.delete(epoch_key)
            await infra.redis_client.delete(done_key)

    finally:
        # освобождаем лок
        try:
            await infra.redis_client.delete(lock_key)
        except Exception:
            pass


# 🔸 Финализация: апдейт/удаление строки в auditor_current_best
async def _finalize_round(sid: int, direction: str, run_id: int, epoch_key: str):
    epoch = await infra.redis_client.hgetall(epoch_key)
    # если лучшего нет (или run_id другой) — удаляем витрину
    if not epoch or _safe_int(epoch.get("run_id")) != run_id or not epoch.get("idea_key"):
        log.info("🗑️ AUD_BEST: sid=%s dir=%s — кандидатов нет, витрина очищается", sid, direction)
        await _delete_current_best(sid, direction)
        return

    # апсерт лучшего
    idea_key = epoch.get("idea_key")
    variant_key = epoch.get("variant_key")
    primary_window = epoch.get("primary_window")
    roi_sel = _safe_float(epoch.get("roi_selected_pct"))
    roi_all = _safe_float(epoch.get("roi_all_pct"))
    d_roi = _safe_float(epoch.get("delta_roi_pp"))
    wr_sel = _safe_float(epoch.get("wr_selected_pct"))
    wr_all = _safe_float(epoch.get("wr_all_pct"))
    d_wr = wr_sel - wr_all
    coverage_pct = _safe_float(epoch.get("coverage_pct"))
    decision_confidence = _safe_float(epoch.get("decision_confidence"))
    config_json = epoch.get("config_json") or "{}"
    source_table = epoch.get("source_table") or ""
    source_run_id = _safe_int(epoch.get("source_run_id"))

    # простой класс по признаку положительного ROI; детальный класс оставляем от идеи при желании
    decision_class = "green" if (d_roi >= 5.0 and wr_sel - wr_all >= 3.0) else ("yellow" if d_roi > 0.0 else "red")

    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO auditor_current_best
            (strategy_id, direction, idea_key, variant_key, primary_window,
             coverage_pct, roi_selected_pct, roi_all_pct, delta_roi_pp,
             wr_selected_pct, wr_all_pct, delta_wr_pp,
             decision_class, decision_confidence, config_json, source_table, source_run_id)
            VALUES
            ($1,$2,$3,$4,$5, $6,$7,$8,$9, $10,$11,$12, $13,$14,$15,$16,$17)
            ON CONFLICT (strategy_id, direction) DO UPDATE SET
              idea_key            = EXCLUDED.idea_key,
              variant_key         = EXCLUDED.variant_key,
              primary_window      = EXCLUDED.primary_window,
              coverage_pct        = EXCLUDED.coverage_pct,
              roi_selected_pct    = EXCLUDED.roi_selected_pct,
              roi_all_pct         = EXCLUDED.roi_all_pct,
              delta_roi_pp        = EXCLUDED.delta_roi_pp,
              wr_selected_pct     = EXCLUDED.wr_selected_pct,
              wr_all_pct          = EXCLUDED.wr_all_pct,
              delta_wr_pp         = EXCLUDED.delta_wr_pp,
              decision_class      = EXCLUDED.decision_class,
              decision_confidence = EXCLUDED.decision_confidence,
              config_json         = EXCLUDED.config_json,
              source_table        = EXCLUDED.source_table,
              source_run_id       = EXCLUDED.source_run_id,
              updated_at          = now()
            """,
            sid, direction, idea_key, variant_key, primary_window,
            float(coverage_pct), float(roi_sel), float(roi_all), float(d_roi),
            float(wr_sel), float(wr_all), float(d_wr),
            decision_class, float(decision_confidence), config_json, source_table, int(source_run_id)
        )
    log.info(
        "🏁 AUD_BEST: sid=%s dir=%s → BEST [%s/%s] ΔROI=%.2fpp (ROI_sel=%.2f%%) cov=%.1f%%",
        sid, direction, idea_key, variant_key, d_roi, roi_sel, coverage_pct
    )


# 🔸 Удаление строки витрины (пауза стратегии)
async def _delete_current_best(sid: int, direction: str):
    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM auditor_current_best WHERE strategy_id=$1 AND direction=$2",
            int(sid), str(direction)
        )