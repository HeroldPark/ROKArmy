import datetime
import time
import pika

sleep_time = 10     # second

# RabbitMQ 연결 설정
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5672
RABBITMQ_USERNAME = 'admin'
RABBITMQ_PASSWORD = 'admin'
RABBITMQ_QUEUE = 'splunk.queue'
RABBITMQ_EXCHANGE = 'splunk.exchange'
RABBITMQ_ROUTING_KEY = 'splunk.ara.#'

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)))
    channel = connection.channel()

    while True:
        current_time = time.time()
        tm = time.localtime(current_time)
        now_tm = time.strftime('%Y-%m-%d %H:%M:%S', tm)

        message = f"["+now_tm+"] Hello, world !!"
        channel.basic_publish(exchange=RABBITMQ_EXCHANGE, routing_key=RABBITMQ_ROUTING_KEY, body=message)
        print(f"Sent message: {message}")
        print(f'{sleep_time}초 waiting...')
        time.sleep(sleep_time)     # 1분(60초) 대기
    
    connection.close()

if __name__ == '__main__':
    main()