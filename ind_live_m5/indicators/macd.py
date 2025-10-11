# indicators/macd.py
import pandas as pd

def compute(df: pd.DataFrame, params: dict) -> dict[str, float]:
    """
    Расчёт MACD (Moving Average Convergence Divergence).
    Возвращает macd, macd_signal, macd_hist — как на TradingView.

    Параметры:
        fast: период быстрой EMA (обычно 12)
        slow: период медленной EMA (обычно 26)
        signal: EMA сигнальной линии (обычно 9)

    Требуемая колонка: 'c' (close)
    """
    fast = int(params.get("fast", 12))
    slow = int(params.get("slow", 26))
    signal = int(params.get("signal", 9))

    close = df["c"].astype(float)

    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    macd_signal = macd_line.ewm(span=signal, adjust=False).mean()
    macd_hist = macd_line - macd_signal

    result = {
        "macd": float(macd_line.dropna().iloc[-1]),
        "macd_signal": float(macd_signal.dropna().iloc[-1]),
        "macd_hist": float(macd_hist.dropna().iloc[-1])
    }

    return result