from kafka import KafkaProducer
from json import dumps
import pandas as pd
import time
###############
KAFKA_TOPIC_NAME_CONS = 'orderstopicdemo'
KAFKA_BOOTSTRAP_SERVER_CONS = 'localhost:9092'
###############
if __name__ == '__main__':
    print('Kafka Application producer started....')
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER_CONS,
                             value_serializer=lambda x: dumps(x).encode('utf-8'))

    file_path = "./data/orders.csv"
    # dataframe
    orders_df = pd.read_csv(file_path)

    print(orders_df.head(5))
    orders_list = orders_df.to_dict(orient="records")

    print(orders_list[0])

    for order in orders_list:
        message = order
        print(f"Message to be sent :{message}")
        producer.send(topic=KAFKA_TOPIC_NAME_CONS, value=message)
        time.sleep(1)  # 1 second
