import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime


def refresh_opensky_token_task():
    cid = Variable.get("CLIENT_ID")
    secret = Variable.get("CLIENT_SECRET")

    url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
    payload = {
        'grant_type': 'client_credentials',
        'client_id': cid,
        'client_secret': secret
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    print(f"Próba pobrania tokena dla Client_ID: {cid}")

    response = requests.post(url, data=payload, headers=headers)

    if response.status_code == 200:
        access_token = response.json().get("access_token")
        Variable.set("OPENSKY_CURRENT_TOKEN", value=access_token, serialize_json=False)
        print("Token został odświeżony i zapisany w Variables.")
    else:
        print(f"Błąd: {response.status_code}")
        print(response.text)
        raise Exception("Nie udało się pobrać tokena.")



with DAG(
        dag_id='opensky_token_manager',
        start_date=datetime(2025, 1, 1),
        schedule='*/30 * * * *',
        catchup=False,
        tags=['opensky', 'kafka']
) as dag:
    run_refresh = PythonOperator(
        task_id='refresh_token_from_api',
        python_callable=refresh_opensky_token_task
    )
