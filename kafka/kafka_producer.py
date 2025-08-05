
import finnhub
from confluent_kafka import Producer
import json
import time

key = "YOUR_API_KEY_HERE"
finnhub_client = finnhub.Client(api_key=key)




config = {
    'bootstrap.servers': 'kafka1:19092'
}

producer = Producer(config)

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")



stock = 'AAPL'

try:
    while True:
        try:
            stock_prices = finnhub_client.quote(stock)
            stock_prices['ticker'] = stock
            value = json.dumps(stock_prices)
            producer.produce('stock_prices', key=stock, value=value, callback=delivery_report)
            producer.poll(0)
        except Exception as e:
            print(f"Error: {e}")

        time.sleep(1)
        

except KeyboardInterrupt:
    print("Shut down")

finally:
    producer.flush()
        
