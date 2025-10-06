import requests
from requests.exceptions import ConnectionError, HTTPError
from quixstreams import Application
import json
import logging
import time


def get_weather_data():
    # Create a counter for retry attempts
    attempt_count = 0
    max_request_attempts = 4

    while attempt_count <= max_request_attempts:
        try:
            # Get free weather forcast data thanks to https://www.buienradar.nl 
            response = requests.get("https://data.buienradar.nl/2.0/feed/json")
            # Catch-all for error responses
            response.raise_for_status()

            return response.json()
        # Adding error handling
        except (ConnectionError, HTTPError) as conn_err:
            attempt_count +=1
            logging.debug(f"An error occured! {conn_err}")

            if attempt_count > max_request_attempts:
                logging.info("Max retry attempts reached. Shutting down!")
                break
            sleep_duration = attempt_count * 300

            # Wait in increments of 5 minutes between attempts
            time.sleep(sleep_duration)
    
    return None

def main():

    app = Application(
        broker_address="localhost:9092",
        loglevel="DEBUG"
        )

    with app.get_producer() as producer:
        while True:
            weather = get_weather_data()
            logging.debug(f"Got weather: {weather.keys()}")
            producer.produce(
                topic = "weather_data",
                key = "Netherlands",
                value = json.dumps(weather)
            )
            logging.info("Produced. Sleeping...")
            time.sleep(300)



if __name__ == "__main__":
    logging.basicConfig(level = 'DEBUG')
    try:
        main()
    
    except KeyboardInterrupt:
        pass
