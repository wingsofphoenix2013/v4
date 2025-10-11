# indicators/adx_dmi.py
import pandas as pd

def compute(df: pd.DataFrame, params: dict) -> dict[str, float]:
    """
    Расчёт ADX / DMI по формуле TradingView (ta.rma).
    Возвращает: adx, plus_di, minus_di

    Параметры:
        length: период сглаживания (обычно 14)

    Требуемые колонки в df: 'h', 'l', 'c'
    """
    length = int(params.get("length", 14))
    alpha = 1 / length

    high = df["h"].astype(float)
    low = df["l"].astype(float)
    close = df["c"].astype(float)

    up = high.diff()
    down = -low.diff()

    plus_dm = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)

    tr1 = (high - low).abs()
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    trur = tr.ewm(alpha=alpha, adjust=False).mean()
    plus = 100 * plus_dm.ewm(alpha=alpha, adjust=False).mean() / trur
    minus = 100 * minus_dm.ewm(alpha=alpha, adjust=False).mean() / trur

    sum_ = plus + minus
    sum_ = sum_.replace(0, 1)  # предотвращаем деление на 0
    dx = (plus - minus).abs() / sum_
    adx = 100 * dx.ewm(alpha=alpha, adjust=False).mean()

    result = {
        "adx": round(float(adx.dropna().iloc[-1]), 2),
        "plus_di": round(float(plus.dropna().iloc[-1]), 2),
        "minus_di": round(float(minus.dropna().iloc[-1]), 2)
    }

    return result