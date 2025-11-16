"""Utilities for downloading stock price data and saving it to CSV files.

The module exposes :func:`fetch_and_save_stock_prices` which communicates with
Alpha Vantage's stock price API. The function requires an API key provided by
Alpha Vantage (``https://www.alphavantage.co``).

The code is written so it can be unit tested without performing network calls.
Callers can supply a custom ``session`` object that matches the ``requests``
API (anything implementing ``get`` and returning an object with ``raise_for_status``
and ``json`` methods). When no session is provided, :mod:`requests` is imported
lazily.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Mapping, MutableMapping, Optional


ALPHAVANTAGE_URL = "https://www.alphavantage.co/query"


@dataclass
class StockPrice:
    """Represents a single stock price data point."""

    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int

    @classmethod
    def from_alphavantage(cls, symbol: str, timestamp: str, payload: Mapping[str, str]) -> "StockPrice":
        """Create a :class:`StockPrice` from Alpha Vantage API fields.

        Parameters
        ----------
        symbol:
            The stock ticker symbol, e.g. ``"AAPL"``.
        timestamp:
            The ISO-8601 timestamp string returned by Alpha Vantage.
        payload:
            A mapping containing the time series fields such as ``"1. open"``.
        """

        return cls(
            symbol=symbol,
            timestamp=datetime.fromisoformat(timestamp),
            open=float(payload["1. open"]),
            high=float(payload["2. high"]),
            low=float(payload["3. low"]),
            close=float(payload["4. close"]),
            volume=int(float(payload["6. volume"])) if "6. volume" in payload else int(float(payload.get("5. volume", 0))),
        )


def _prepare_rows(data: Iterable[StockPrice]) -> List[List[str]]:
    """Convert an iterable of :class:`StockPrice` objects into CSV rows."""

    rows: List[List[str]] = [[
        "symbol",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]]

    for item in sorted(data, key=lambda p: (p.symbol, p.timestamp)):
        rows.append([
            item.symbol,
            item.timestamp.isoformat(),
            f"{item.open:.4f}",
            f"{item.high:.4f}",
            f"{item.low:.4f}",
            f"{item.close:.4f}",
            str(item.volume),
        ])

    return rows


def fetch_and_save_stock_prices(
    symbols: Iterable[str],
    api_key: str,
    output_csv_path: str,
    *,
    function: str = "TIME_SERIES_DAILY_ADJUSTED",
    session: Optional[object] = None,
) -> None:
    """Fetch stock price data for ``symbols`` and save the results to ``output_csv_path``.

    Parameters
    ----------
    symbols:
        An iterable of stock ticker symbols, e.g. ``["AAPL", "MSFT"]``.
    api_key:
        Your Alpha Vantage API key.
    output_csv_path:
        Path to the CSV file that should be created.
    function:
        Alpha Vantage time series function to use. Defaults to ``TIME_SERIES_DAILY_ADJUSTED``.
    session:
        Optional HTTP session that implements the ``requests`` interface. When not
        provided, :mod:`requests` is imported and used directly.
    """

    if session is None:
        import requests  # type: ignore

        session = requests

    collected: List[StockPrice] = []

    for symbol in symbols:
        params = {
            "function": function,
            "symbol": symbol,
            "apikey": api_key,
        }
        response = session.get(ALPHAVANTAGE_URL, params=params, timeout=30)
        response.raise_for_status()
        payload: Mapping[str, object] = response.json()

        time_series_key = next(
            (
                key
                for key in payload.keys()
                if key.startswith("Time Series") or key.startswith("Monthly Adjusted Time Series")
            ),
            None,
        )
        if not time_series_key:
            raise ValueError(f"Unexpected response format for {symbol}: {payload}")

        time_series_data = payload[time_series_key]
        if not isinstance(time_series_data, MutableMapping):
            raise ValueError(f"Time series data for {symbol} is not a mapping: {type(time_series_data)!r}")

        for timestamp, fields in time_series_data.items():
            if not isinstance(fields, Mapping):
                raise ValueError(f"Time series entry for {symbol} at {timestamp} is not a mapping")
            collected.append(StockPrice.from_alphavantage(symbol, timestamp, fields))

    rows = _prepare_rows(collected)

    import csv

    with open(output_csv_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(rows)


__all__ = ["fetch_and_save_stock_prices", "StockPrice", "ALPHAVANTAGE_URL"]
