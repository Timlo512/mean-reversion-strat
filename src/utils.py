import logging
from src.PostgresConnector import PostgresConnector

# Set up logging
log = logging.getLogger(__name__)

def _handle_res(res, e):
    if res.status_code == 200:
        data = res.json()
        return data
    else:
        log.error(f"Status Code: {res.status_code}")
        raise Exception(f"Error: {res.status_code} - {e}")

def get_db_connector(db_type: str):
    if db_type == 'postgres':
        return PostgresConnector
    else:
        raise ValueError(f"Unsupported database type: {db_type}")