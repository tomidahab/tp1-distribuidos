import socket
import logging

logging.basicConfig(level=logging.INFO)

def main():
    HOST = 'boundary'
    PORT = 5000
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, PORT))
        message = "hola"
        logging.info(f"Sending message: {message}")
        sock.sendall(message.encode())
        response = sock.recv(1024)
        logging.info(f"Received response: {response.decode().strip()}")
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        sock.close()

if __name__ == '__main__':
    main()
