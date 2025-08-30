# 🔸 regime9_core.py — единая логика regime9_code v2 (live + backfill)

from dataclasses import dataclass
from statistics import median
from typing import List, Tuple, Optional, Dict


# 🔸 Константы по умолчанию (можно переопределять извне через RegimeParams)
EPS_Z_DEFAULT = 0.5        # порог для z-score(ΔMACD)
HYST_TREND_BARS = 2        # подтверждение (тренд↔флет), баров подряд
HYST_SUB_BARS = 1          # подтверждение (accel/stable/decel), баров подряд
Z_WINSOR = 5.0             # ограничение z-score по модулю
BB_EPS = 1e-9              # защита деления на почти ноль


# 🔸 Вспомогательные структуры состояния/параметров
@dataclass
class RegimeState:
    core: str = "flat"      # 'flat' | 'trend'
    core_cnt: int = 0       # счётчик подтверждений смены core
    sub: str = "stable"     # 'accel' | 'stable' | 'decel' (имеет смысл только в 'trend')
    sub_cnt: int = 0        # счётчик подтверждений смены sub


@dataclass
class RegimeParams:
    hyst_trend_bars: int = HYST_TREND_BARS
    hyst_sub_bars: int = HYST_SUB_BARS
    eps_z: float = EPS_Z_DEFAULT


# 🔸 Перцентили p30/p70 на списке (без numpy)
def _p30(vals: List[float]) -> float:
    if not vals:
        return float("nan")
    k = max(0, int(0.30 * (len(vals) - 1)))
    return sorted(vals)[k]


def _p70(vals: List[float]) -> float:
    if not vals:
        return float("nan")
    k = max(0, int(0.70 * (len(vals) - 1)))
    return sorted(vals)[k]


# 🔸 MAD и z-score с защитой и winsorize
def _mad(vals: List[float]) -> float:
    if not vals:
        return 0.0
    m = median(vals)
    dev = [abs(x - m) for x in vals]
    s = median(dev)
    return s if s > 0.0 else BB_EPS


def _zscore(x: float, vals: List[float]) -> float:
    m = median(vals) if vals else 0.0
    s = _mad(vals)
    z = (x - m) / s
    # winsorize
    if z > Z_WINSOR:
        z = Z_WINSOR
    elif z < -Z_WINSOR:
        z = -Z_WINSOR
    return z


# 🔸 Нормализация наклона EMA с учётом волатильности
def _normalized_slope(tf: str, ema_t1: float, ema_t: float,
                      atr_t: Optional[float],
                      bb_center_t: Optional[float]) -> float:
    raw = ema_t - ema_t1
    # На m5/m15 нормируем на ATR; на h1 – на центр BB (как суррогат масштаба)
    if tf in ("m5", "m15"):
        denom = atr_t if (atr_t is not None and atr_t > 0.0) else 1.0
    else:
        c = bb_center_t if (bb_center_t is not None and abs(bb_center_t) > BB_EPS) else 1.0
        denom = abs(c)
    return raw / denom if denom != 0.0 else raw


# 🔸 Расчёт ширины полос Боллинджера и её порогов
def _bb_width_and_thresholds(bb_u: List[float], bb_l: List[float], bb_c: List[float]) -> Tuple[float, float, float]:
    # Текущая ширина
    c_t = bb_c[-1] if bb_c else 0.0
    if abs(c_t) <= BB_EPS:
        # если центр почти ноль, делим на 1 чтобы не ронять расчёт — масштаб не важен, важна относительность
        c_t = 1.0
    bbw_series = []
    for u, l, c in zip(bb_u, bb_l, bb_c):
        denom = c if abs(c) > BB_EPS else 1.0
        bbw_series.append((u - l) / denom)
    bbw_t = bbw_series[-1] if bbw_series else 0.0
    bb_low = _p30(bbw_series)
    bb_high = _p70(bbw_series)
    # гарантия порядка порогов
    if bb_low > bb_high:
        bb_low, bb_high = bb_high, bb_low
    return bbw_t, bb_low, bb_high


# 🔸 Пороговая логика ADX с защитой минимального разлёта
def _adx_thresholds(adx_win: List[float]) -> Tuple[float, float, float]:
    adx = adx_win[-1]
    low = max(_p30(adx_win), 15.0)
    high = min(_p70(adx_win), 30.0)
    # минимальный коридор стабильности
    if (high - low) < 4.0:
        low = min(low, 18.0)
        high = max(high, 28.0)
    return adx, low, high


# 🔸 Обновление гистерезиса для core: 'flat' ↔ 'trend'
def _update_core_with_hysteresis(state: RegimeState, adx: float, low: float, high: float, need: int) -> None:
    # Если уже 'trend' — ждём подтверждения ухода ниже low
    if state.core == "trend":
        if adx <= low:
            state.core_cnt += 1
            if state.core_cnt >= need:
                state.core = "flat"
                state.core_cnt = 0
                # при смене core сбрасываем sub
                state.sub = "stable"
                state.sub_cnt = 0
        else:
            state.core_cnt = 0
    # Если уже 'flat' — ждём подтверждения выхода выше high
    else:  # 'flat'
        if adx >= high:
            state.core_cnt += 1
            if state.core_cnt >= need:
                state.core = "trend"
                state.core_cnt = 0
                # при входе в тренд sub оставляем 'stable' (определится далее)
                state.sub = "stable"
                state.sub_cnt = 0
        else:
            state.core_cnt = 0


# 🔸 Обновление гистерезиса для sub (внутри тренда): accel/stable/decel
def _update_sub_with_hysteresis(state: RegimeState, desired: str, need: int) -> None:
    if state.sub == desired:
        state.sub_cnt = 0
        return
    state.sub_cnt += 1
    if state.sub_cnt >= need:
        state.sub = desired
        state.sub_cnt = 0


# 🔸 Основное решение по режиму (0..8) с диагностикой и гистерезисом
def decide_regime_code(
    tf: str,
    features: Dict[str, object],
    state: RegimeState,
    params: RegimeParams
) -> Tuple[int, RegimeState, Dict[str, float]]:
    """
    Ожидаемые поля features:
      ema_t1: float
      ema_t: float
      macd_t1: float
      macd_t: float
      dhist_win: List[float]           # окно последних ΔMACD (не включая текущий, либо включая – не критично для MAD)
      adx_win: List[float]             # окно для ADX (≈200)
      bb_u_win: List[float]
      bb_l_win: List[float]
      bb_c_win: List[float]
      atr_t: Optional[float]           # только для m5/m15; на h1 = None
      atr_win: Optional[List[float]]   # окно ATR для m5/m15; на h1 = None
    Возвращает: (code 0..8, обновлённый state, diag dict)
    """
    ema_t1 = float(features["ema_t1"])
    ema_t = float(features["ema_t"])
    macd_t1 = float(features["macd_t1"])
    macd_t = float(features["macd_t"])
    dhist_win = list(features.get("dhist_win", []))  # тип: List[float]
    adx_win = list(features["adx_win"])
    bb_u = list(features["bb_u_win"])
    bb_l = list(features["bb_l_win"])
    bb_c = list(features["bb_c_win"])
    atr_t = features.get("atr_t", None)
    atr_win = features.get("atr_win", None)

    # ΔMACD и z-score
    d_hist = macd_t - macd_t1
    z = _zscore(d_hist, dhist_win) if dhist_win else 0.0

    # Пороговая логика ADX
    adx, adx_low, adx_high = _adx_thresholds(adx_win)

    # Обновление core (trend/flat) с гистерезисом
    _update_core_with_hysteresis(state, adx, adx_low, adx_high, params.hyst_trend_bars)

    # Ширина BB и пороги
    bb_width, bb_low, bb_high = _bb_width_and_thresholds(bb_u, bb_l, bb_c)

    # Нормированный наклон EMA (направление в 'trend')
    bb_center_t = bb_c[-1] if bb_c else None
    slope = _normalized_slope(tf, ema_t1, ema_t, atr_t, bb_center_t)

    # Ветка FLAT
    if state.core == "flat":
        # ATR-пороги (только для m5/m15), на h1 оставляем None
        atr = float(atr_t) if (atr_t is not None) else None
        atr_low = _p30(atr_win) if (atr_win is not None and len(atr_win) > 0) else None
        atr_high = _p70(atr_win) if (atr_win is not None and len(atr_win) > 0) else None

        if bb_width <= bb_low and (atr is None or (atr_low is not None and atr <= atr_low)):
            code = 0  # F_CONS
        elif bb_width >= bb_high:
            code = 1  # F_EXP
        else:
            code = 2  # F_DRIFT

        diag = {
            "adx": adx, "adx_low": adx_low, "adx_high": adx_high,
            "bb_width": bb_width, "bb_low": bb_low, "bb_high": bb_high,
            "atr": (atr if atr is not None else float("nan")),
            "atr_low": (float(atr_low) if atr_low is not None else float("nan")),
            "atr_high": (float(atr_high) if atr_high is not None else float("nan")),
            "ema_slope": slope,
            "macd_hist": macd_t,
            "d_hist": d_hist,
            "z_d_hist": z
        }
        return code, state, diag

    # Ветка TREND: определяем подрежим по z и направление по знаку slope
    desired_sub = "stable"
    if z > +params.eps_z:
        desired_sub = "accel"
    elif z < -params.eps_z:
        desired_sub = "decel"
    _update_sub_with_hysteresis(state, desired_sub, params.hyst_sub_bars)

    # Направление: по знаку нормированного наклона
    direction_up = slope > 0.0
    # В редком случае slope==0.0 можно ориентироваться на знак MACD_hist
    if slope == 0.0:
        direction_up = (macd_t >= 0.0)

    # Кодировка: Up: 3..5, Down: 6..8
    sub_idx = 0 if state.sub == "accel" else (1 if state.sub == "stable" else 2)
    code = (3 + sub_idx) if direction_up else (6 + sub_idx)

    diag = {
        "adx": adx, "adx_low": adx_low, "adx_high": adx_high,
        "bb_width": bb_width, "bb_low": bb_low, "bb_high": bb_high,
        "atr": (float(atr_t) if atr_t is not None else float("nan")),
        "atr_low": (float(_p30(atr_win)) if atr_win else float("nan")),
        "atr_high": (float(_p70(atr_win)) if atr_win else float("nan")),
        "ema_slope": slope,
        "macd_hist": macd_t,
        "d_hist": d_hist,
        "z_d_hist": z
    }
    return code, state, diag