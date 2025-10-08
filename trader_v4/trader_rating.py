# trader_rating.py — почасовой расчёт рейтинга стратегий по группам market_mirrow с «входным гейтом»

# 🔸 Импорты
import logging
import json
from decimal import Decimal
from typing import Dict, List, Tuple, Any

from trader_infra import infra

# 🔸 Логгер воркера рейтинга
log = logging.getLogger("TRADER_RATING")

# 🔸 Глобальное состояние победителей (в памяти)
current_group_winners: Dict[int, int] = {}

# 🔸 Пороговые константы «входного гейта» (в долях)
ROI24_FLOOR = Decimal("-0.005")   # -0.5%
MOMENTUM_MIN = Decimal("0.001")   # +0.10%
WR_BASE = Decimal("0.50")         # базовая устойчивость
WR_MOMENTUM = Decimal("0.55")     # чуть выше, если спасаем моментумом

# 🔸 Гиперпараметры для сглаживания winrate (Лаплас)
LAPLACE_A = Decimal("1")
LAPLACE_B = Decimal("1")


# 🔸 Публичный геттер победителей (копия словаря)
def get_current_group_winners() -> Dict[int, int]:
    return dict(current_group_winners)


# 🔸 Основной одноразовый проход расчёта рейтинга (вызывается раз в час из main)
async def run_trader_rating_job():
    # собираем кандидатов
    candidates = await _fetch_candidates()
    if not candidates:
        log.debug("ℹ️ TRADER_RATING: кандидатов нет (enabled & trader_watcher & market_mirrow) — пропуск")
        return

    strategy_ids = [c["id"] for c in candidates]

    # агрегаты за 24ч и 1ч
    metrics = await _fetch_metrics_24h_1h(strategy_ids)

    # победители по группам с учётом «входного гейта»
    group_winners, raw_results, groups_watchers = _compute_group_winners_with_gate(candidates, metrics)

    # применяем флаги в БД и апсерты активных рейтингов
    await _apply_results_to_db(group_winners, groups_watchers, raw_results)

    # обновляем in-memory winners
    _update_inmemory_winners(group_winners)

    # сводка
    total_groups = len(groups_watchers)
    total_winners = sum(1 for v in group_winners.values() if v is not None)
    winners_pretty = ", ".join(f"{gm}->{sid}" for gm, sid in group_winners.items() if sid is not None)
    log.debug("✅ TRADER_RATING: обработано групп=%d, победителей=%d%s",
             total_groups, total_winners, f" | {winners_pretty}" if winners_pretty else "")


# 🔸 Загрузка списка кандидатов (enabled, trader_watcher, есть market_mirrow)
async def _fetch_candidates() -> List[Dict[str, Any]]:
    rows = await infra.pg_pool.fetch(
        """
        SELECT id, deposit, market_mirrow
        FROM public.strategies_v4
        WHERE enabled = TRUE
          AND trader_watcher = TRUE
          AND market_mirrow IS NOT NULL
        """
    )
    out: List[Dict[str, Any]] = []
    for r in rows:
        dep = Decimal(str(r["deposit"])) if r["deposit"] is not None else Decimal("0")
        out.append({"id": int(r["id"]), "deposit": dep, "market_mirrow": int(r["market_mirrow"])})
    return out


# 🔸 Агрегаты по позициям за 24ч и 1ч для набора стратегий
async def _fetch_metrics_24h_1h(strategy_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    if not strategy_ids:
        return {}

    rows_24 = await infra.pg_pool.fetch(
        """
        SELECT
          strategy_id,
          COUNT(*) AS closed_24,
          SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins_24,
          COALESCE(SUM(pnl), 0) AS pnl_24
        FROM public.positions_v4
        WHERE status = 'closed'
          AND closed_at >= ((now() at time zone 'UTC') - interval '24 hours')
          AND strategy_id = ANY($1::int[])
        GROUP BY strategy_id
        """,
        strategy_ids,
    )

    rows_1h = await infra.pg_pool.fetch(
        """
        SELECT
          strategy_id,
          COUNT(*)              AS closed_1h,
          COALESCE(SUM(pnl), 0) AS pnl_1h
        FROM public.positions_v4
        WHERE status = 'closed'
          AND closed_at >= ((now() at time zone 'UTC') - interval '1 hour')
          AND strategy_id = ANY($1::int[])
        GROUP BY strategy_id
        """,
        strategy_ids,
    )

    m: Dict[int, Dict[str, Any]] = {}
    for r in rows_24:
        sid = int(r["strategy_id"])
        m[sid] = {
            "closed_24": int(r["closed_24"]),
            "wins_24": int(r["wins_24"]),
            "pnl_24": Decimal(str(r["pnl_24"])),
            "closed_1h": 0,
            "pnl_1h": Decimal("0"),
        }
    for r in rows_1h:
        sid = int(r["strategy_id"])
        if sid not in m:
            m[sid] = {"closed_24": 0, "wins_24": 0, "pnl_24": Decimal("0"), "closed_1h": 0, "pnl_1h": Decimal("0")}
        m[sid]["closed_1h"] = int(r["closed_1h"])
        m[sid]["pnl_1h"] = Decimal(str(r["pnl_1h"]))
    return m


# 🔸 Расчёт победителей по группам с «входным гейтом»
def _compute_group_winners_with_gate(
    candidates: List[Dict[str, Any]],
    metrics: Dict[int, Dict[str, Any]],
) -> Tuple[Dict[int, int | None], Dict[int, Dict[str, Any]], Dict[int, List[int]]]:

    # группируем кандидатов по market_mirrow
    groups_watchers: Dict[int, List[int]] = {}
    deposits: Dict[int, Decimal] = {}
    for c in candidates:
        sid = c["id"]
        gm = c["market_mirrow"]
        dep = c["deposit"]
        groups_watchers.setdefault(gm, []).append(sid)
        deposits[sid] = dep

    group_winners: Dict[int, int | None] = {}
    raw_results: Dict[int, Dict[str, Any]] = {}

    # функция minmax для нормализации (внутри группы)
    def _minmax(x: float, lo: float, hi: float) -> float:
        if hi == lo:
            return 0.5
        return (x - lo) / (hi - lo)

    # проходим по группам
    for gm, sids in groups_watchers.items():
        per_strategy_results: Dict[str, Any] = {}
        passed: List[Tuple[int, float, float, float, int]] = []  # (sid, roi24, wr_shr, roi1h, n24)

        # подготовка метрик и гейт
        for sid in sids:
            dep: Decimal = deposits.get(sid, Decimal("0"))
            met = metrics.get(sid, {"closed_24": 0, "wins_24": 0, "pnl_24": Decimal("0"), "closed_1h": 0, "pnl_1h": Decimal("0")})
            closed_24 = int(met["closed_24"])
            wins_24 = int(met["wins_24"])
            pnl_24 = Decimal(met["pnl_24"])
            closed_1h = int(met["closed_1h"])
            pnl_1h = Decimal(met["pnl_1h"])

            # базовые значения
            if dep <= 0 or closed_24 <= 0:
                # спящие / некорректные депозиты — не проходят гейт
                per_strategy_results[str(sid)] = {
                    "roi24": 0.0,
                    "wr24": 0.0,
                    "wr_shr": 0.0,
                    "roi1h": 0.0,
                    "n24": closed_24,
                    "n1": closed_1h,
                    "gate": False,
                    "score": 0.0,
                }
                continue

            roi24 = pnl_24 / dep
            roi1h = pnl_1h / dep
            wr24 = Decimal(wins_24) / Decimal(closed_24) if closed_24 > 0 else Decimal("0")
            wr_shr = (Decimal(wins_24) + LAPLACE_A) / (Decimal(closed_24) + LAPLACE_A + LAPLACE_B)

            # условия гейта
            gate_ok = (roi24 > ROI24_FLOOR) and (
                (roi24 >= 0 and wr_shr >= WR_BASE)  # ветка A
                or
                (roi1h > MOMENTUM_MIN and wr_shr >= WR_MOMENTUM)  # ветка B
            )

            per_strategy_results[str(sid)] = {
                "roi24": float(roi24),
                "wr24": float(wr24),
                "wr_shr": float(wr_shr),
                "roi1h": float(roi1h),
                "n24": closed_24,
                "n1": closed_1h,
                "gate": bool(gate_ok),
            }

            if gate_ok:
                passed.append((sid, float(roi24), float(wr_shr), float(roi1h), closed_24))

        # если никто не прошёл гейт — победителя нет
        if not passed:
            group_winners[gm] = None
            raw_results[gm] = {"strategies": per_strategy_results}
            continue

        # нормализация по группе для ROI24 и ROI1h
        roi24_vals = [p[1] for p in passed]
        roi1h_vals = [p[3] for p in passed]
        r24_min, r24_max = min(roi24_vals), max(roi24_vals)
        r1h_min, r1h_max = min(roi1h_vals), max(roi1h_vals)

        scored: List[Tuple[int, float, float, float, int]] = []  # (sid, score, roi24, roi1h, n24)
        for sid, roi24_f, wr_shr_f, roi1h_f, n24 in passed:
            roi24_norm = _minmax(roi24_f, r24_min, r24_max)
            roi1h_norm = _minmax(roi1h_f, r1h_min, r1h_max)
            score = 0.60 * roi24_norm + 0.25 * float(wr_shr_f) + 0.15 * roi1h_norm
            scored.append((sid, score, roi24_f, roi1h_f, n24))
            per_strategy_results[str(sid)]["score"] = score

        # выбираем победителя (тай-брейки: ROI_24h, ROI_1h, N24)
        scored.sort(key=lambda t: (-t[1], -t[2], -t[3], -t[4], t[0]))
        winner_sid = scored[0][0]
        group_winners[gm] = winner_sid
        raw_results[gm] = {"strategies": per_strategy_results}

    return group_winners, raw_results, groups_watchers


# 🔸 Применение результатов в БД: trader_winner-флаги и апсерты в trader_rating_active
async def _apply_results_to_db(
    group_winners: Dict[int, int | None],
    groups_watchers: Dict[int, List[int]],
    raw_results: Dict[int, Dict[str, Any]],
):
    if not groups_watchers:
        return

    group_ids = list(groups_watchers.keys())
    winner_ids = [sid for sid in group_winners.values() if sid is not None]

    async with infra.pg_pool.acquire() as conn:
        async with conn.transaction():
            # сброс флагов в обработанных группах
            await conn.execute(
                """
                UPDATE public.strategies_v4
                SET trader_winner = FALSE
                WHERE trader_watcher = TRUE
                  AND market_mirrow = ANY($1::int[])
                """,
                group_ids,
            )

            # установка флага победителям
            if winner_ids:
                await conn.execute(
                    """
                    UPDATE public.strategies_v4
                    SET trader_winner = TRUE
                    WHERE id = ANY($1::int[])
                    """,
                    winner_ids,
                )

            # upsert в таблицу активных рейтингов
            for gm in group_ids:
                winner = group_winners.get(gm)
                raw_json = raw_results.get(gm, {"strategies": {}})
                raw_json_str = json.dumps(raw_json, ensure_ascii=False, separators=(",", ":"))

                await conn.execute(
                    """
                    INSERT INTO public.trader_rating_active AS tra (
                      group_master_id, current_winner_id, consecutive_wins, raw_results, last_run_at
                    ) VALUES
                      ($1::int4,
                       $2::int4,
                       CASE WHEN $2::int4 IS NULL THEN 0 ELSE 1 END,
                       $3::jsonb,
                       (now() at time zone 'UTC'))
                    ON CONFLICT (group_master_id) DO UPDATE
                    SET
                      current_winner_id = EXCLUDED.current_winner_id,
                      consecutive_wins  = CASE
                                            WHEN EXCLUDED.current_winner_id IS NULL
                                              THEN 0
                                            WHEN tra.current_winner_id = EXCLUDED.current_winner_id
                                              THEN tra.consecutive_wins + 1
                                            ELSE 1
                                          END,
                      raw_results       = EXCLUDED.raw_results,
                      last_run_at       = (now() at time zone 'UTC')
                    """,
                    gm,
                    winner,
                    raw_json_str,
                )


# 🔸 Обновление in-memory списка победителей (глобальная переменная)
def _update_inmemory_winners(group_winners: Dict[int, int | None]) -> None:
    current_group_winners.clear()
    for gm, sid in group_winners.items():
        if sid is not None:
            current_group_winners[gm] = sid