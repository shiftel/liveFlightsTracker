import os
import requests
from requests.auth import HTTPBasicAuth


def get_token_from_airflow():
    AIRFLOW_USER = os.getenv('AIRFLOW_USER', 'marcel')
    AIRFLOW_PASS = os.getenv('AIRFLOW_PASSWORD', 'projekt2024')
    AIRFLOW_HOST = os.getenv('AIRFLOW_HOST', 'localhost')

    url = f"http://{AIRFLOW_HOST}:8888/api/v1/variables/OPENSKY_CURRENT_TOKEN"

    try:
        response = requests.get(url, auth=HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASS))
        if response.status_code == 200:
            return response.json().get('value')
        else:
            print(f"Błąd: Status {response.status_code}")
            return None
    except Exception as e:
        print(f"Problem z połączeniem z Airflow: {e}")
        return None

if __name__ == "__main__":
    token = get_token_from_airflow()
    print(token)




