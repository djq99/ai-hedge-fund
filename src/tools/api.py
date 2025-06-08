import datetime
import pandas as pd

from src.tools.ibkr import IBKRClient

_ibkr_client = IBKRClient()

from src.data.cache import get_cache
from src.data.models import (
    CompanyNews,
    FinancialMetrics,
    Price,
    LineItem,
    InsiderTrade,
)

# Global cache instance
_cache = get_cache()


def get_prices(ticker: str, start_date: str, end_date: str) -> list[Price]:
    """Fetch price data from cache or API."""
    # Create a cache key that includes all parameters to ensure exact matches
    cache_key = f"{ticker}_{start_date}_{end_date}"

    # Check cache first - simple exact match
    if cached_data := _cache.get_prices(cache_key):
        return [Price(**price) for price in cached_data]

    # If not in cache, fetch from IBKR
    df = _ibkr_client.get_price_history(ticker, start_date, end_date)
    prices = [
        Price(
            open=row.open,
            close=row.close,
            high=row.high,
            low=row.low,
            volume=int(row.volume),
            time=row.time.strftime("%Y-%m-%d %H:%M:%S"),
        )
        for row in df.itertuples()
    ]

    if not prices:
        return []

    # Cache the results using the comprehensive cache key
    _cache.set_prices(cache_key, [p.model_dump() for p in prices])
    return prices


def get_financial_metrics(
    ticker: str,
    end_date: str,
    period: str = "ttm",
    limit: int = 10,
) -> list[FinancialMetrics]:
    """Fetch financial metrics from cache or API."""
    # Create a cache key that includes all parameters to ensure exact matches
    cache_key = f"{ticker}_{period}_{end_date}_{limit}"

    # Check cache first - simple exact match
    if cached_data := _cache.get_financial_metrics(cache_key):
        return [FinancialMetrics(**metric) for metric in cached_data]

    # If not in cache, fetch fundamentals from IBKR (XML)
    xml = _ibkr_client.get_fundamentals(ticker)
    import xml.etree.ElementTree as ET

    metrics = {field: None for field in FinancialMetrics.__fields__}
    metrics.update({
        "ticker": ticker,
        "report_period": end_date,
        "period": period,
        "currency": "USD",
    })

    if xml:
        root = ET.fromstring(xml)
        val = root.findtext(".//MKTCAP") or root.findtext(".//MarketCap")
        if val:
            try:
                metrics["market_cap"] = float(val)
            except ValueError:
                pass

    financial_metrics = [FinancialMetrics(**metrics)]

    if not financial_metrics:
        return []

    # Cache the results as dicts using the comprehensive cache key
    _cache.set_financial_metrics(cache_key, [m.model_dump() for m in financial_metrics])
    return financial_metrics


def search_line_items(
    ticker: str,
    line_items: list[str],
    end_date: str,
    period: str = "ttm",
    limit: int = 10,
) -> list[LineItem]:
    """IBKR does not provide granular line item search."""
    return []


def get_insider_trades(
    ticker: str,
    end_date: str,
    start_date: str | None = None,
    limit: int = 1000,
) -> list[InsiderTrade]:
    """IBKR does not provide insider trade data via the API."""
    return []


def get_company_news(
    ticker: str,
    end_date: str,
    start_date: str | None = None,
    limit: int = 1000,
) -> list[CompanyNews]:
    """IBKR does not provide a news feed API."""
    return []


def get_market_cap(
    ticker: str,
    end_date: str,
) -> float | None:
    """Fetch market cap from the API."""
    # Check if end_date is today
    if end_date == datetime.datetime.now().strftime("%Y-%m-%d"):
        xml = _ibkr_client.get_fundamentals(ticker, report_type="ReportSnapshot")
        if xml:
            import xml.etree.ElementTree as ET

            root = ET.fromstring(xml)
            val = root.findtext(".//MKTCAP") or root.findtext(".//MarketCap")
            if val:
                try:
                    return float(val)
                except ValueError:
                    pass
        return None

    financial_metrics = get_financial_metrics(ticker, end_date)
    if not financial_metrics:
        return None

    market_cap = financial_metrics[0].market_cap

    if not market_cap:
        return None

    return market_cap


def prices_to_df(prices: list[Price]) -> pd.DataFrame:
    """Convert prices to a DataFrame."""
    df = pd.DataFrame([p.model_dump() for p in prices])
    df["Date"] = pd.to_datetime(df["time"])
    df.set_index("Date", inplace=True)
    numeric_cols = ["open", "close", "high", "low", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.sort_index(inplace=True)
    return df


# Update the get_price_data function to use the new functions
def get_price_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    prices = get_prices(ticker, start_date, end_date)
    return prices_to_df(prices)
