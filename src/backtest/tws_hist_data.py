from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

from datetime import datetime

from ..utils import get_db_connector

# Connect to IB
class TWSHistDataApp(EWrapper, EClient):
    def __init__(self, table_tbl: str, db_type: str = None, db_config: dict = {}, keep_alive = False):
        EClient.__init__(self, self)

        self.table_tbl = table_tbl
        self.result = [] # Bulk insertion
        self.keep_alive = keep_alive
        self.db_config = db_config
        db_connector = get_db_connector(db_type)
        if db_connector:
            self.db_connector = db_connector(**self.db_config)
        else:
            self.db_connector = None
            print(f"Database connector for {db_type} not found. Skipping database operations.")
        self.load_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def exit(self):
        self.disconnect()

    def historicalData(self, reqId: int, contract: Contract):
        data = {
            "reqId": reqId,
            **contract.__dict__,
            "load_datetime": self.load_datetime
        }
        self.result.append(data)

    def historicalDataEnd(self, reqId: int, startTime: str, endTime: str):
        print(reqId, startTime, endTime)
        print("Num of data fetched: ", len(self.result))
        print("Last row: ", self.result[-1] if self.result else "No data fetched")
        if self.db_connector:
            check = self.db_connector.write_data(
                self.result,
                self.table_tbl,
                if_exists = "upsert",
                conflict_columns = ["reqid", "date"],
            )
            if check:
                print("Data written successfully")
            else:
                print("Failed to write data")
        else:
            print("No database connector available. Skipping data write.")

        print("Historical data request End")
        if not self.keep_alive:
            self.exit()
