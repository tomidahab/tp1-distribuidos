import socket
import threading
import uuid
import pika
import logging
import time

logging.basicConfig(level=logging.INFO)

RABBITMQ_HOST = 'rabbitmq'
MAX_RETRIES = 10
RETRY_DELAY = 10

def send_to_rabbitmq(client_id, message):
    attempts = 0
    payload = f"{client_id}:{message}"
    while attempts < MAX_RETRIES:
        try:
            logging.info(f"Attempting to connect to RabbitMQ at {RABBITMQ_HOST}:5672 (attempt {attempts+1})")
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            channel.queue_declare(queue='client_messages', durable=True, auto_delete=False)
            channel.basic_publish(exchange='', routing_key='client_messages', body=payload)
            logging.info(f"Sent to RabbitMQ: {payload}")
            connection.close()
            return
        except Exception as e:
            attempts += 1
            logging.error(f"Error connecting to RabbitMQ on attempt {attempts}: {e}")
            time.sleep(RETRY_DELAY)
    raise Exception("Could not connect to RabbitMQ after several attempts.")

def handle_client(client_socket, address):
    client_id = str(uuid.uuid4())
    logging.info(f"Accepted connection from {address}. Assigned client ID {client_id}")
    try:
        data = client_socket.recv(1024)
        if data:
            message = data.decode().strip()
            logging.info(f"Received from client {client_id}: {message}")
            time.sleep(60)
            send_to_rabbitmq(client_id, message)
            client_socket.sendall(f"Received your message, client {client_id}".encode())
    except Exception as e:
        logging.error(f"Error handling client {client_id}: {e}")
    finally:
        client_socket.close()

def main():
    HOST = '0.0.0.0'
    PORT = 5000
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    server_socket.listen(5)
    logging.info("Boundary server listening on port 5000")
    try:
        while True:
            client_socket, address = server_socket.accept()
            thread = threading.Thread(target=handle_client, args=(client_socket, address))
            thread.start()
    except KeyboardInterrupt:
        logging.info("Shutting down server")
    finally:
        server_socket.close()

if __name__ == '__main__':
    main()
