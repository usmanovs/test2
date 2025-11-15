"""Utility for fetching historical stock prices and saving them to CSV files."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterable, Union

import pandas as pd
import yfinance as yf


def fetch_stock_prices(
    symbols: Union[str, Iterable[str]],
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
    output_file: Union[str, Path],
    *,
    interval: str = "1d",
    auto_adjust: bool = True,
) -> Path:
    """Fetch historical stock price data and write it to a CSV file.

    Parameters
    ----------
    symbols:
        Ticker symbol or iterable of ticker symbols to download. If an iterable
        is provided the prices for all tickers are fetched and written to a
        single CSV file.
    start_date, end_date:
        Date range for the historical data. Either ``datetime`` objects or
        strings parseable by :func:`pandas.to_datetime`.
    output_file:
        Path where the resulting CSV file will be written. Parent directories
        are created automatically if they do not already exist.
    interval:
        Sampling interval for the price data accepted by ``yfinance``.
        Examples include ``"1d"`` (daily) and ``"1h"`` (hourly).
    auto_adjust:
        If ``True`` the data is adjusted for dividends and stock splits.

    Returns
    -------
    pathlib.Path
        The path to the generated CSV file.
    """

    if isinstance(symbols, str):
        tickers = symbols
    else:
        # ``yfinance.download`` accepts comma-delimited string for multiple tickers.
        tickers = ",".join(symbol.strip().upper() for symbol in symbols)

    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)

    data = yf.download(tickers=tickers, start=start, end=end, interval=interval, auto_adjust=auto_adjust)

    if data.empty:
        raise ValueError("No data was returned for the provided parameters.")

    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    data.to_csv(output_path)
    return output_path


__all__ = ["fetch_stock_prices"]
