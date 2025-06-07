"""Utility functions for fetching data from the Interactive Brokers API."""

from __future__ import annotations

import os
from datetime import datetime
from typing import List

from ib_insync import IB, Stock

from src.data.models import Price

_ib: IB | None = None


def _get_ib() -> IB:
    """Connect to the IBKR API and return the connection instance."""
    global _ib
    if _ib and _ib.isConnected():
        return _ib

    host = os.environ.get("IBKR_HOST", "127.0.0.1")
    port = int(os.environ.get("IBKR_PORT", "7497"))
    client_id = int(os.environ.get("IBKR_CLIENT_ID", "1"))

    ib = IB()
    ib.connect(host, port, clientId=client_id)
    _ib = ib
    return ib


def get_prices(
    ticker: str,
    start_date: str,
    end_date: str,
    bar_size: str = "1 day",
    what_to_show: str = "ADJUSTED_LAST",
) -> List[Price]:
    """Fetch historical prices from Interactive Brokers."""
    ib = _get_ib()
    contract = Stock(ticker, "SMART", "USD")

    start_dt = datetime.fromisoformat(start_date)
    end_dt = datetime.fromisoformat(end_date)
    duration_days = (end_dt - start_dt).days or 1
    duration_str = f"{duration_days} D"

    bars = ib.reqHistoricalData(
        contract,
        endDateTime=end_dt.strftime("%Y%m%d %H:%M:%S"),
        durationStr=duration_str,
        barSizeSetting=bar_size,
        whatToShow=what_to_show,
        useRTH=True,
        formatDate=1,
    )

    prices: List[Price] = []
    for bar in bars:
        prices.append(
            Price(
                open=bar.open,
                close=bar.close,
                high=bar.high,
                low=bar.low,
                volume=bar.volume,
                time=bar.date,
            )
        )

    return prices
