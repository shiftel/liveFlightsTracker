import json
import time

from confluent_kafka import Producer

from get_data_from_opensky import get_data_from_opensky


def send_to_kafka():
    conf = {'bootstrap.servers': "localhost:29092"}
    producer = Producer(conf)


    while True:
        print("Pobieram nowe dane...")
        data = get_data_from_opensky()

        if data:
            producer.produce('opensky_flights', value=json.dumps(data))
            producer.flush()
            print("Dane wysłane do Kafki.")


        print("Czekam 60 sekund...")
        time.sleep(60)


if __name__ == "__main__":
    send_to_kafka()