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
    
def expand_list_fr_dict(data_dict: dict, list_key_name: str):
    """
    Expand a list of dictionaries into a single dictionary.
    Args:
        data_dict (dict): The dictionary to expand.
        list_key_name (str): The key name of the list to expand.
    Returns:
        dict: The expanded dictionary.
    """
    if list_key_name in data_dict:
        expanded_dict = []
        tmp_dict = {k: v for k, v in data_dict.items() if k != list_key_name}
        for item in data_dict[list_key_name]:
            item = {**tmp_dict, **item}
            expanded_dict.append(item)
        return expanded_dict
    else:
        return data_dict


def get_db_connector(db_type: str):
    if db_type == 'postgres':
        return PostgresConnector
    else:
        raise ValueError(f"Unsupported database type: {db_type}")