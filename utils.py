import logging

# Set up logging
log = logging.getLogger(__name__)

def _handle_res(res, e):
    if res.status_code == 200:
        data = res.json()
        return data
    else:
        log.error(f"Status Code: {res.status_code}")
        raise Exception(f"Error: {res.status_code} - {e}")
