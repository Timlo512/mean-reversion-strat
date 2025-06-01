from ibapi.contract import Contract
from src.backtest.tws_hist_data import TWSHistDataApp
from src.utils import get_db_connector

import yaml, os, time
import dotenv
dotenv.load_dotenv()

env = os.getenv('ENV'); repo_dir = os.getenv("REPO_DIR")
print(env, repo_dir)

db_type = 'postgres'
db_config = yaml.safe_load(open(os.path.join(repo_dir, 'config/DBConfig.yml')))[env][db_type]
conn = get_db_connector(db_type)(**db_config)

# Setting for TWSHistDataApp
table_name = "market_data.ib_tws_hist_data"
host = "127.0.0.1"
port = 4001
clientId = 1

def collect_bt_hist_data():

    # Get all the configurations from postgres db
    params_sql = """
        select * from market_data.ib_tws_hist_data_req where disabled = false
    """
    params_df = conn.read_sql_by_pd(params_sql)
    print(params_df)

    # Create a list of arguments for the TWSHistDataApp
    for _, row in params_df.iterrows():
        
        # Contract details
        contract =  Contract()
        contract.symbol = row['symbol']
        contract.secType = row['sec_type']
        contract.currency = row['currency']
        contract.exchange = row['exchange']

        reqId = row['id']

        # Additional parameters
        queryTime = row['query_time'] if row['query_time'] else ""
        durationStr = row['duration_str']
        barSizeSetting = row['bar_size_setting']
        whatToShow = row['what_to_show']
        useRTH = int(row['use_rth'])
        formatDate = int(row['format_date'])
        keepUpToDate = row['keep_up_to_date']
        chartOptions = row['chart_options'].split(',') if row['chart_options'] else []

        try:
            # Create a TWSHistDataApp instance for each symbol
            app = TWSHistDataApp(
                table_tbl = table_name,
                db_type = db_type,
                db_config = db_config
            )
            app.connect(host, port, clientId)
            time.sleep(1) # Wait for connection to establish

            app.reqHistoricalData(
                reqId,
                contract,
                queryTime,  # Use current time if empty
                durationStr,
                barSizeSetting,
                whatToShow,
                useRTH,
                formatDate,
                keepUpToDate,
                chartOptions
            )

            app.run()  # Start the event loop to process the request

        except Exception as e:
            print(f"Error connecting to TWS for {row['symbol']}: {e}")
            continue

if __name__ == "__main__":
    collect_bt_hist_data()