import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.sdk import Variable

repo_dir = Variable.get("repo_dir")
env = Variable.get("env")

with DAG(
    "market_data_job",
    description = "Market Data Collection Daily Job",
    schedule = "@daily",
    start_date = datetime.datetime(2025, 4, 27),
    tags = ["market_data"]
) as dag:

    t1 = BashOperator(
        task_id = "collect_market_data",
        bash_command = f"cd {repo_dir} \
            && ./.venv/bin/python3 main.py DailyWorker -e {env} -t ConidsByExchange HistoricalDataByConids",
    )