import pika
import logging
import time
import message_pb2

logging.basicConfig(level=logging.INFO)
RABBITMQ_HOST = 'rabbitmq'
RETRY_DELAY = 5
MAX_RETRIES = 10

def publish_to_queue(queue_name, payload):
    attempts = 0
    while attempts < MAX_RETRIES:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            channel.queue_declare(queue=queue_name, durable=True, auto_delete=False)
            channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=payload
            )
            logging.info(f"Published routed message to queue '{queue_name}'")
            connection.close()
            return
        except Exception as e:
            attempts += 1
            logging.error(f"Error publishing to queue '{queue_name}' on attempt {attempts}: {e}")
            time.sleep(RETRY_DELAY)
    logging.error(f"Failed to publish to queue '{queue_name}' after {MAX_RETRIES} attempts.")

def callback(ch, method, properties, body):
    try:
        routed_msg = message_pb2.RoutedMessage()
        routed_msg.ParseFromString(body)
        op = routed_msg.client_message.operation.lower()
        logging.info(f"Routing message for client {routed_msg.client_id} with operation: {op}")
        if op == "read":
            publish_to_queue("read_queue", body)
        elif op == "write":
            publish_to_queue("write_queue", body)
        else:
            logging.error(f"Unexpected operation: {op}")
    except Exception as e:
        logging.error(f"Error processing message: {e}")

def main():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            channel.queue_declare(queue='client_messages', durable=True, auto_delete=False)
            channel.queue_declare(queue='read_queue', durable=True, auto_delete=False)
            channel.queue_declare(queue='write_queue', durable=True, auto_delete=False)
            channel.basic_consume(queue='client_messages', on_message_callback=callback, auto_ack=True)
            logging.info("Routing consumer started. Waiting for messages.")
            channel.start_consuming()
        except Exception as e:
            logging.error(f"Error in routing consumer: {e}")
            logging.info(f"Retrying connection in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)

if __name__ == '__main__':
    main()
