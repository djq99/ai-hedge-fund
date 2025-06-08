import os
import pandas as pd
from datetime import datetime
from ib_insync import IB, Stock, util


class IBKRClient:
    """Simple wrapper around ib_insync to fetch data from IBKR."""

    def __init__(self):
        host = os.getenv("IB_HOST", "127.0.0.1")
        port = int(os.getenv("IB_PORT", "4002"))
        client_id = int(os.getenv("IB_CLIENT_ID", "1"))
        self.ib = IB()
        self.ib.connect(host, port, clientId=client_id)

    def disconnect(self):
        if self.ib.isConnected():
            self.ib.disconnect()

    def get_price_history(
        self, ticker: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """Fetch daily historical prices for `ticker` from IBKR."""
        contract = Stock(ticker, "SMART", "USD")
        end_ts = end_date + " 23:59:59"
        # Compute duration in days
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        duration_days = (end_dt - start_dt).days + 1
        duration_str = f"{duration_days} D"
        bars = self.ib.run(
            self.ib.reqHistoricalDataAsync(
                contract,
                endDateTime=end_ts,
                durationStr=duration_str,
                barSizeSetting="1 day",
                whatToShow="ADJUSTED_LAST",
                useRTH=True,
                formatDate=1,
            )
        )
        df = util.df(bars)
        if not df.empty:
            df.rename(columns={"date": "time"}, inplace=True)
        return df

    def get_fundamentals(self, ticker: str, report_type: str = "ReportsFinStatements") -> str:
        """Request fundamental data report from IBKR (XML string)."""
        contract = Stock(ticker, "SMART", "USD")
        data = self.ib.run(
            self.ib.reqFundamentalDataAsync(contract, reportType=report_type)
        )
        return data or ""
