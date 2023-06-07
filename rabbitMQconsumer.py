import pika

# RabbitMQ 연결 설정
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5672
RABBITMQ_USERNAME = 'admin'
RABBITMQ_PASSWORD = 'admin'
RABBITMQ_QUEUE = 'splunk.queue'
RABBITMQ_EXCHANGE = 'splunk.exchange'
RABBITMQ_ROUTING_KEY = 'splunk.ara.#'

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
