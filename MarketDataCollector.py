

from typing import Union
import pandas as pd
import requests
import logging
from utils import _handle_res


from dotenv import load_dotenv
load_dotenv()

# Set up logging
log = logging.getLogger(__name__)

# Data Collector for backtesting

class MarketDataCollector:

    def __init__(self, baseUrl, storage_repo = None, verify_ssl = False):
        self.storage_repo = storage_repo
        self.baseUrl = baseUrl
        self.verify_ssl = verify_ssl

        self.__setup_headers()
        self._verify_auth_status()

    def __setup_headers(self):
        self.headers = {
            "Host": "api.ibkr.com",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Connection": "keep-alive",
        }

    def _verify_auth_status(self):
        """
        Verify the authentication status of the API.
        """
        endpoint = "/iserver/auth/status"

        res = requests.get(
            f"{self.baseUrl}{endpoint}",
            headers=self.headers,
            verify=False,
        )
        data = _handle_res(res, "Not authenticated")
        return data["authenticated"]

    def search_conids_by_exchange(self, exchange: str, useDf: bool = False):
        """
        Search for conids by exchange.
        Args:
            exchange (str): The exchange to search for.
            useDf (bool): Whether to return the result as a DataFrame or not.
        Returns:
            list/ Dataframe: A list or Dataframe of conids for the specified exchange.
            
        """
        log.info(f"Searching conids by exchange: {exchange}")

        endpoint = f"/trsrv/all-conids?exchange={exchange}"
        res = requests.get(
            f"{self.baseUrl}{endpoint}",
            headers=self.headers,
            verify=self.verify_ssl,
        )
        data = _handle_res(res, "Error fetching conids by exchange")
        data = data if not useDf else pd.DataFrame(data)
        return data
    
    def search_conids_by_symbol(self, symbols: Union[list[str], str], useDf: bool = False):
        """
        Search for conids by symbol.
        Args:
            symbols (str): The symbol to search for.
            useDf (bool): Whether to return the result as a DataFrame or not.
        
        Returns:
            list: A list of conids for the specified symbol.
        """
        log.info(f"Searching conids by symbol: {symbols}")

        endpoint = f"/trsrv/stocks?symbols={symbols if isinstance(symbols, str) else ','.join(symbols)}"
        res = requests.get(
            f"{self.baseUrl}{endpoint}",
            headers=self.headers,
            verify=self.verify_ssl,
        )
        data = _handle_res(res, "Error fetching conids by symbol")
        return data if not useDf else pd.DataFrame(data)
   
    # hmds
    def get_historical_data_by_conid(self, conid: int, period: str = "1d", bar: str = "1min", useDf: bool = False, **kwargs):
        """
        Get historical data by conid. (Historical Market Data Beta)
        It uses the direct connection to the market data farm.
        Args:
            conid (int): Required. The conid to search for.
            
            period (str): The period of the historical data. Default is 1d.
            Available time period– {1-30}min, {1-8}h, {1-1000}d, {1-792}w, {1-182}m, {1-15}y

            bar (str): The size of the bars. Default is 1min.
            Possible value– 1min, 2min, 3min, 5min, 10min, 15min, 30min, 1h, 2h, 3h, 4h, 8h, 1d, 1w, 1m

            useDf (bool): Whether to return the result as a DataFrame or not.

            **kwargs: Additional arguments to pass to the request.
            outsideRth (bool): Whether to include data outside of regular trading hours. Default is False
            barType (str): The type of bar to return. Default is Last.
        Returns:
            list/ Dataframe: A list or Dataframe of historical data for the specified conid.

        Ref. https://www.interactivebrokers.com/campus/ibkr-api-page/cpapi-v1/#hist-md-beta
        """
        log.info(f"Getting historical data by conid: {conid}")

        outsideRth = kwargs.get("outsideRth", False)
        barType = kwargs.get("barType", "Last")

        endpoint = f"/hmds/history?conid={conid}&period={period}&bar={bar}&outsideRth={str(outsideRth).lower()}&barType={barType}"
        res = requests.get(
            f"{self.baseUrl}{endpoint}",
            headers=self.headers,
            verify=self.verify_ssl,
        )
        data = _handle_res(res, "Error fetching historical data by conid")
        return data if not useDf else pd.DataFrame(data)

    def get_live_snapshot_by_conids(self, conids: list | int, useDf: bool = False, **kwargs):
        """
        Get Live market data by conid. (Live Market Data Snapshot -> Free)
        It uses the direct connection to the market data farm.
        Args:
            conids (list): Required. The conids to search for.
            useDf (bool): Whether to return the result as a DataFrame or not.
            fields (str): The fields to return. Default is 31,84,86.
                
            Remarks: 31: Last Price, 84: Bid Price, 86: Ask Price
        Returns:
            list/ Dataframe: A list or Dataframe of live market data for the specified conids.
        Ref. https://www.interactivebrokers.com/campus/ibkr-api-page/cpapi-v1/#md
        """
        
        log.info(f"Getting live snapshot: {conids}")

        fields = kwargs.get("fields", "31,84,86") # 31: Last Price, 84: Bid Price, 86: Ask Price
        conids = ','.join(map(str, conids)) if isinstance(conids, list) else conids

        endpoint = f"/iserver/marketdata/snapshot?conids={conids}&fields={fields}"
        res = requests.get(
            f"{self.baseUrl}{endpoint}",
            headers=self.headers,
            verify=self.verify_ssl,
        )
        data = _handle_res(res, "Error fetching live data by conids")
        return data if not useDf else pd.DataFrame(data)
