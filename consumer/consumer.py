import pika
import logging
import time

logging.basicConfig(level=logging.INFO)

RABBITMQ_HOST = 'rabbitmq'
RETRY_DELAY = 5

def callback(ch, method, properties, body):
    logging.info("Consumed message: %s", body.decode())

def main():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            channel.queue_declare(queue='client_messages', durable=True, auto_delete=False)
            channel.basic_consume(queue='client_messages', on_message_callback=callback, auto_ack=True)
            logging.info("Consumer started. Waiting for messages.")
            channel.start_consuming()  # Blocks and keeps consuming messages.
        except Exception as e:
            logging.error("Error in consumer: %s", e)
            logging.info("Retrying connection in %d seconds...", RETRY_DELAY)
            time.sleep(RETRY_DELAY)

if __name__ == '__main__':
    main()
