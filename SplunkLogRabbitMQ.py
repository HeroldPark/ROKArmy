import csv
import time
import requests
import splunklib.client as client
import io
import pika

# Splunk 설정
SPLUNK_HOST = 'localhost'
SPLUNK_PORT = 8089
SPLUNK_USERNAME = 'shane'
SPLUNK_PASSWORD = 'yrpark12'

# RabbitMQ 설정
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5672
RABBITMQ_USERNAME = 'admin'
RABBITMQ_PASSWORD = 'admin'
RABBITMQ_QUEUE = 'splunk.queue'
RABBITMQ_EXCHANGE = 'splunk.exchange'
RABBITMQ_ROUTING_KEY = 'splunk.ara.#'

sleep_time = 10     # second

# Splunk 연결
service = client.connect(
    host=SPLUNK_HOST,
    port=SPLUNK_PORT,
    username=SPLUNK_USERNAME,
    password=SPLUNK_PASSWORD
) 

# Splunk 로그 가져오기
def get_splunk_logs():
    current_time = time.time()
    minutes_ago = current_time - (1440 * 60)

    search_query = f'search index="_internal" earliest={minutes_ago} latest=now()'
    search_args = {'output_mode': 'csv'}

    tm = time.localtime(minutes_ago)
    print("start search: ", time.strftime('%Y-%m-%d %H:%M:%S', tm))
    results = service.jobs.export(search_query, **search_args)

    mid_time = time.time()
    print("start read: %s" % search_query)
    # # Open the search results as a file-like object using io.BytesIO
    logs = io.BytesIO(results.read())

    fin_time = time.time()
    print(f"search_time: {mid_time-current_time}, read_time: {fin_time-mid_time}")

    return logs

# RabbitMQ 프로듀서
def rabbitmq_producer(logs):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, 
                                                                   credentials=pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)))
    channel = connection.channel()
    print(f'Sent log to RabbitMQ')
    for log in logs:
        # log_message = json.dumps(log)
        message = log
        # channel.basic_publish(exchange=, routing_key=RABBITMQ_QUEUE, body=log_message)
        channel.basic_publish(exchange=RABBITMQ_EXCHANGE, routing_key=RABBITMQ_ROUTING_KEY, body=message)
        # print('Sent log to RabbitMQ:', message)

    connection.close()
    print('Sent complete:')

# RabbitMQ 컨슈머
def rabbitmq_consumer():
    # 메시지 수신 콜백 함수
    def callback(ch, method, properties, body):
        print('Received message from RabbitMQ:', body.decode())

    # RabbitMQ 연결 및 메시지 수신 대기
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT,
                                                                credentials=pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)))
    channel = connection.channel()
    channel.queue_bind(queue=RABBITMQ_QUEUE, exchange=RABBITMQ_EXCHANGE, routing_key=RABBITMQ_ROUTING_KEY)
    channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback, auto_ack=True)

    print('Waiting for messages from RabbitMQ...')

# 로그 저장 및 불러오기
def run():

    while True:
        current_time = time.time()
        tm = time.localtime(current_time)
        now_tm = time.strftime('%Y-%m-%d %H:%M:%S', tm)

        # Splunk 로그 가져오기
        logs = get_splunk_logs()
        # 로그 RabbitMQ 프로듀서로 전송
        rabbitmq_producer(logs)

        # RabbitMQ 컨슈머 시작
        rabbitmq_consumer()

        time.sleep(sleep_time)     # 1분(60초) 대기
        print(f'{sleep_time}초 waiting...')

# 실행
if __name__ == '__main__':
    run()
