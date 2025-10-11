# indicators/atr.py
import pandas as pd

def compute(df: pd.DataFrame, params: dict) -> dict[str, float]:
    """
    Расчёт ATR по формуле TradingView (True Range + Wilder's smoothing).
    df: DataFrame с колонками 'h', 'l', 'c'
    params: {'length': str|int}
    """
    length = int(params.get("length", 14))

    if not all(col in df.columns for col in ("h", "l", "c")):
        raise ValueError("df должен содержать колонки 'h', 'l', 'c'")

    high = df["h"].astype(float)
    low = df["l"].astype(float)
    close = df["c"].astype(float)
    prev_close = close.shift(1)

    tr = pd.concat([
        (high - low).abs(),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)

    atr_series = tr.ewm(alpha=1/length, adjust=False).mean()
    return {"value": float(atr_series.iloc[-1])}