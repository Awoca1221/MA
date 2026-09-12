import os
import logging
import pika
from message import Notification

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_USER = os.getenv('RABBITMQ_DEFAULT_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_DEFAULT_PASS', 'guest')
QUEUE_NAME = os.getenv('RABBITMQ_QUEUE', 'lab_queue')

credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)


def callback(ch, method, properties, body):
    try:
        message = Notification.from_json(body)
        logging.info(f" [x] Received: {message}")
        if message.type == "Notification":
            logging.info(f"Обработка: {message.text}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logging.error(f"Ошибка обработки: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def start_consumer():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            credentials=credentials,
        )
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=callback,
        auto_ack=False
    )
    logging.info(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    start_consumer()
