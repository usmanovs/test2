"""Utilities for fetching stock prices and writing them to CSV files."""
from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path
from typing import Sequence

import requests


YAHOO_FINANCE_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"


def fetch_stock_prices(symbols: Sequence[str], output_csv: str | Path) -> Path:
    """Fetch stock prices for ``symbols`` and save them to ``output_csv``.

    Parameters
    ----------
    symbols:
        An iterable of stock ticker symbols that should be fetched. Symbols are
        case-insensitive.
    output_csv:
        Path where the CSV data should be written. Parent directories are
        created automatically.

    Returns
    -------
    Path
        The path to the written CSV file.

    Raises
    ------
    ValueError
        If ``symbols`` is empty.
    requests.HTTPError
        If the Yahoo Finance API returns an error status code.
    requests.RequestException
        If the request fails due to connectivity issues or a timeout.
    """
    if not symbols:
        raise ValueError("At least one stock symbol must be provided")

    normalized_symbols = [symbol.strip().upper() for symbol in symbols if symbol.strip()]
    if not normalized_symbols:
        raise ValueError("Symbols must not be empty or whitespace")

    response = requests.get(
        YAHOO_FINANCE_QUOTE_URL,
        params={"symbols": ",".join(normalized_symbols)},
        timeout=10,
    )
    response.raise_for_status()

    payload = response.json()
    quotes = payload.get("quoteResponse", {}).get("result", [])

    rows = []
    quote_lookup = {quote.get("symbol", "").upper(): quote for quote in quotes}
    for symbol in normalized_symbols:
        quote = quote_lookup.get(symbol)
        if quote is None:
            rows.append(
                {
                    "symbol": symbol,
                    "price": "",
                    "currency": "",
                    "timestamp": "",
                }
            )
            continue

        market_time = quote.get("regularMarketTime")
        timestamp = (
            dt.datetime.fromtimestamp(market_time, tz=dt.timezone.utc).isoformat()
            if market_time
            else ""
        )

        rows.append(
            {
                "symbol": symbol,
                "price": quote.get("regularMarketPrice", ""),
                "currency": quote.get("currency", ""),
                "timestamp": timestamp,
            }
        )

    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=["symbol", "price", "currency", "timestamp"])
        writer.writeheader()
        writer.writerows(rows)

    return output_path


__all__ = ["fetch_stock_prices"]
