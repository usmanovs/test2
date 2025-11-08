# test2

## Fetching stock prices

This repository includes a helper function, `fetch_stock_prices`, for downloading
historical stock price data and saving it to a CSV file.

### Requirements

Install the required dependencies:

```bash
pip install yfinance pandas
```

### Example

```python
from datetime import date

from stock_fetcher import fetch_stock_prices

fetch_stock_prices(
    symbols=["AAPL", "MSFT"],
    start_date=date(2023, 1, 1),
    end_date=date(2023, 12, 31),
    output_file="data/prices.csv",
)
```
