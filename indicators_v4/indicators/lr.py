# indicators/lr.py
import pandas as pd
import numpy as np


def compute(df: pd.DataFrame, params: dict) -> dict[str, float]:
    """
    Линейная регрессия по close-ценам.
    Возвращает: angle, upper, lower, center (по TradingView)
    """
    length = int(params.get("length", 50))

    if "c" not in df:
        raise ValueError("df должен содержать колонку 'c' (close)")

    closes = df["c"].astype(float)
    if len(closes) < length:
        raise ValueError("Недостаточно данных для расчёта LR")

    closes = closes.tail(length).reset_index(drop=True)
    x = np.arange(length)

    # A. линейная регрессия по ценам
    coeffs_raw = np.polyfit(x, closes, deg=1)
    slope_raw = coeffs_raw[0]
    mid_raw = closes.mean()
    intercept_raw = mid_raw - slope_raw * (length // 2) + ((1 - (length % 2)) / 2) * slope_raw

    reg_line_raw = slope_raw * x + intercept_raw
    std_dev = np.sqrt(np.mean((closes - reg_line_raw) ** 2))

    upper = float(reg_line_raw[-1] + 2 * std_dev)
    lower = float(reg_line_raw[-1] - 2 * std_dev)
    center = float(reg_line_raw[-1])

    # B. угол по нормализованным данным
    base_price = closes.mean()
    norm = (closes - base_price) / base_price
    slope_norm = np.polyfit(x, norm, deg=1)[0]

    import logging
    log = logging.getLogger("CALC")
    log.debug(f"[DEBUG] slope_norm = {slope_norm}")

    angle = float(np.degrees(np.arctan(slope_norm)))

    # 🔸 подавление близких к нулю значений
    if abs(angle) < 1e-5:
        angle = 0.0

    return {
        "angle": angle,
        "upper": upper,
        "lower": lower,
        "center": center
    }