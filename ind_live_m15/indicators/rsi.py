# indicators/rsi.py
import pandas as pd

def compute(df: pd.DataFrame, params: dict) -> dict[str, float]:
    """
    Расчёт RSI (Relative Strength Index), совместимый с TradingView.
    Использует только колонку 'c' (close)
    Округление до 2 знаков
    """
    length = int(params.get("length", 14))

    if "c" not in df:
        raise ValueError("df должен содержать колонку 'c' (close)")

    closes = df["c"].astype(float)
    if len(closes) < length + 1:
        raise ValueError("Недостаточно данных для расчёта RSI")

    delta = closes.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1/length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/length, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, 1e-6)
    rsi = 100 - (100 / (1 + rs))

    return {"value": round(float(rsi.iloc[-1]), 2)}