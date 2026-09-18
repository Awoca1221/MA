import os
import pika
from pika.exceptions import AMQPConnectionError
from flask import Flask, jsonify
from pika.exchange_type import ExchangeType
from message import Notification
import threading

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_USER = os.getenv('RABBITMQ_DEFAULT_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_DEFAULT_PASS', 'guest')
QUEUE_NAME = "lab_queue"
EXCHANGE_NAME = "direct_message"

credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)

app = Flask(__name__)
app.json.ensure_ascii = False


def publish_message(text: str):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            virtual_host="/",
            credentials=credentials,
        )
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    channel.exchange_declare(
        exchange=EXCHANGE_NAME,
        exchange_type=ExchangeType.direct,
        durable=True
    )
    channel.queue_bind(
        queue=QUEUE_NAME,
        exchange=EXCHANGE_NAME,
        routing_key=QUEUE_NAME,
    )

    message = Notification(text=text)
    channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=QUEUE_NAME,
        body=message.to_json().encode("utf-8"),
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
        ),
    )
    connection.close()
    return message.to_json()


_lock = threading.Lock()
_number = 0


def next_number():
    global _number
    with _lock:
        _number += 1
        return _number


@app.get('/send_message')
def send_message():
    text = f"Новое сообщение: {next_number()}"
    try:
        publish_message(text)
    except AMQPConnectionError as e:
        return jsonify({"status": "error", "detail": str(e)}), 503
    return jsonify({"status": "ok", "sent": text}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
