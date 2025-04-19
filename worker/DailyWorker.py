from src.MarketDataCollector import MarketDataCollector
from src.utils import get_db_connector

import os
import yaml
import dotenv
dotenv.load_dotenv()

# Get environment variables
env = os.getenv('ENV')

# Get configs
job_config = yaml.safe_load(open(os.path.join(os.getcwd(), 'config/DailyMarketDataCollect.yml')))
db_config = yaml.safe_load(open(os.path.join(os.path.dirname(__file__), '../config/DBConfig.yml')))[env]

class DailyWorker:

    def __init__(self, task_list: list|str, db_type: str = 'postgres'):
        self.task_list = task_list

        print("Create Market Data Collector")
        self.data_collector = MarketDataCollector(
            baseUrl=job_config['BaseUrl'],
            verify_ssl=job_config['VerifySSL']
        )

        print("Create DB Connector")
        self.db_config = db_config[db_type]
        self.db_connector = get_db_connector(db_type)(
            **self.db_config
        )

    # For collect data tasks in market data collector
    def collect_market_data(self, task_config: dict = None):

        func_name = task_config["FunctionName"]
        params = task_config["Params"]
        table_name = task_config["TableName"]

        # # Collect data via IB API
        data = getattr(self.data_collector, func_name)(**params)

        # Save data to database
        self.db_connector.write_data(
            data, 
            table_name
        )

    def run(self):
        for task_config in job_config["Tasks"]:
            if task_config["Name"] in self.task_list:
                if task_config["TaskType"] == "DataCollection":
                    self.collect_market_data(task_config)
        pass