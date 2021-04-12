import os, requests, json
from kafka import KafkaProducer

'''
Скрипт для отправки данных полётов
с API сервиса aviationstack.com
в очередь Kafka
'''

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    print(excp)

params = {
  'access_key': os.environ['AVIA_TOKEN']
}

api_result = requests.get('http://api.aviationstack.com/v1/flights', params)
api_response = api_result.json()

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda m: json.dumps(m).encode('utf-16'))

for record in api_response['data']:
    departure = record['departure']
    arrival = record['arrival']
    flight = record['flight']
    if departure and arrival and flight:
        flight_number = flight['number']
        departure_time = departure['scheduled']
        departure_airport = departure['airport']
        arrival_airport = arrival['airport']
        if flight_number and departure_time and departure_airport and arrival_airport:
            # produce asynchronously with callbacks
            producer.send('flights', {
                'number': flight_number, # номер рейса
                'time': departure_time, # время вылета
                'departure': departure_airport, # аэропорт вылета
                'arrival': arrival_airport # аэропорт назначения
            }).add_callback(on_send_success).add_errback(on_send_error)

# block until all async messages are sent
producer.flush()

# configure multiple retries
producer = KafkaProducer(retries=5)