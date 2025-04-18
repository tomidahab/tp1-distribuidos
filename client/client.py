import socket
import logging
import message_pb2

logging.basicConfig(level=logging.INFO)


def main():
    HOST = 'boundary'
    PORT = 5000

    # Construir el mensaje Protobuf
    msg = message_pb2.ClientMessage()
    msg.operation = "read"           # esto creo que ya se puede sacar
    msg.query = "AAAAAAAA"           # lo mismo

    try:
        with open('example.csv', 'rb') as csv_file:
            file_bytes = csv_file.read()
            msg.data = file_bytes
    except FileNotFoundError:
        logging.error("No se encontró el archivo 'example.csv'.")
        return
    except Exception as e:
        logging.error(f"Error leyendo 'example.csv': {e}")
        return

    serialized = msg.SerializeToString()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((HOST, PORT))
        sock.sendall(serialized)
        response = sock.recv(1024)
        logging.info(f"Received response: {response.decode()}")
    except Exception as e:
        logging.error(f"Error de conexión o envío: {e}")
    finally:
        sock.close()


if __name__ == '__main__':
    main()
