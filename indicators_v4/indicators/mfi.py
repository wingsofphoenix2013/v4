# indicators/mfi.py
import pandas as pd

def compute(df: pd.DataFrame, params: dict) -> dict[str, float]:
    """
    Расчёт Money Flow Index (MFI), совместимый с TradingView.
    Использует high, low, close, volume
    """
    length = int(params.get("length", 14))

    required_cols = ("h", "l", "c", "v")
    if not all(col in df.columns for col in required_cols):
        raise ValueError("df должен содержать 'h', 'l', 'c', 'v'")

    high = df["h"].astype(float)
    low = df["l"].astype(float)
    close = df["c"].astype(float)
    volume = df["v"].astype(float)

    if len(close) < length + 1:
        raise ValueError("Недостаточно данных для расчёта MFI")

    tp = (high + low + close) / 3
    rmf = tp * volume
    delta_tp = tp.diff()

    pos_flow = rmf.where(delta_tp > 0, 0.0)
    neg_flow = rmf.where(delta_tp < 0, 0.0)

    pos_sum = pos_flow.rolling(length).sum()
    neg_sum = neg_flow.rolling(length).sum()

    mfr = pos_sum / neg_sum.replace(0, 1e-6)
    mfi_series = 100 - (100 / (1 + mfr))

    return {"value": float(mfi_series.iloc[-1])}