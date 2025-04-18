import os
import pika
import logging
import time
import message_pb2

logging.basicConfig(level=logging.INFO)

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RETRY_DELAY   = int(os.getenv('RETRY_DELAY', '5'))
MAX_RETRIES   = int(os.getenv('MAX_RETRIES', '10'))
QUEUE_NAME    = os.getenv('QUEUE_NAME')
NODE_ID       = os.getenv('NODE_ID')

if not QUEUE_NAME or not NODE_ID:
    logging.error('Environment variables QUEUE_NAME and NODE_ID must be set')
    exit(1)


def publish_to_queue(queue_name, payload):
    attempts = 0
    while attempts < MAX_RETRIES:
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            ch   = conn.channel()
            ch.queue_declare(queue=queue_name, durable=True, auto_delete=False)
            ch.basic_publish(exchange='', routing_key=queue_name, body=payload)
            logging.info(f"Node {NODE_ID}: published to '{queue_name}'")
            conn.close()
            return
        except Exception as e:
            attempts += 1
            logging.error(f"Node {NODE_ID}: publish error on '{queue_name}' attempt {attempts}: {e}")
            time.sleep(RETRY_DELAY)
    logging.error(f"Node {NODE_ID}: failed to publish after {MAX_RETRIES} attempts")


def callback(ch, method, properties, body):
    try:
        routed = message_pb2.RoutedMessage()
        routed.ParseFromString(body)
        data = routed.client_message.data
        logging.info(f"Node {NODE_ID}: received from {QUEUE_NAME} -> {data}")
    except Exception as e:
        logging.error(f"Node {NODE_ID}: error processing message: {e}")


def main():
    while True:
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            ch   = conn.channel()
            ch.queue_declare(queue=QUEUE_NAME, durable=True, auto_delete=False)
            ch.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)
            logging.info(f"Node {NODE_ID}: listening on '{QUEUE_NAME}'")
            ch.start_consuming()
        except Exception as e:
            logging.error(f"Node {NODE_ID}: connection error: {e}")
            time.sleep(RETRY_DELAY)


if __name__ == '__main__':
    main()