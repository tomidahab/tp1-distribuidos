import socket
import threading
import uuid
import time
import logging
import pika
import message_pb2

logging.basicConfig(level=logging.INFO)
RABBITMQ_HOST = 'rabbitmq'
MAX_RETRIES = 10
RETRY_DELAY = 2

def send_to_rabbitmq(routed_msg_serialized):
    attempts = 0
    while attempts < MAX_RETRIES:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()
            channel.queue_declare(queue='client_messages', durable=True, auto_delete=False)
            channel.basic_publish(
                exchange='',
                routing_key='client_messages',
                body=routed_msg_serialized
            )
            logging.info("Sent routed message to 'client_messages' queue")
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
        data = client_socket.recv(4096)
        if data:
            client_msg = message_pb2.ClientMessage()
            client_msg.ParseFromString(data)
            logging.info(f"Received from client {client_id}: operation={client_msg.operation}, query={client_msg.query}, data={client_msg.data}")
            routed_msg = message_pb2.RoutedMessage()
            routed_msg.client_id = client_id
            routed_msg.client_message.CopyFrom(client_msg)
            serialized_routed = routed_msg.SerializeToString()
            send_to_rabbitmq(serialized_routed)
            client_socket.sendall(f"Message received, client {client_id}".encode())
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
            threading.Thread(target=handle_client, args=(client_socket, address)).start()
    except KeyboardInterrupt:
        logging.info("Shutting down server")
    finally:
        server_socket.close()

if __name__ == '__main__':
    main()
