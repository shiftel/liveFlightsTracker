import requests
from get_token_from_airflow import get_token_from_airflow

def get_data_from_opensky():
    token = get_token_from_airflow().strip()

    if not token:
        print("Błąd: Nie udało się pobrać tokena z Airflow. Przerywam.")
        return None


    url = "https://opensky-network.org/api/states/all?lamin=49.0&lomin=14.1&lamax=54.8&lomax=24.1"
    headers = {
        'Authorization': token,
    }

    try:
        response = requests.get(url, headers=headers, timeout=20)
        if response.status_code == 200:
            data = response.json()
            return data
        elif response.status_code == 429:
            print("Błąd: Przekroczono limit zapytań OpenSky (Rate Limit)!")
            return None
        else:
            print(f"Błąd OpenSky: Status {response.status_code}")
            print(f"Treść błędu: {response.text}")
            return None
    except Exception as e:
        print(f"Problem z połączeniem z Airflow: {e}")
        return None

if __name__ == "__main__":
    data = get_data_from_opensky()
    print(data)