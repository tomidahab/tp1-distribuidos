import socket
import logging
import message_pb2

logging.basicConfig(level=logging.INFO)

def main():
    HOST = 'boundary'
    PORT = 5000
    msg = message_pb2.ClientMessage()
    msg.operation = "read"
    msg.query = "AAAAAAAA"
    msg.data = b"data1,data2" 
    serialized = msg.SerializeToString()
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((HOST, PORT))
        sock.sendall(serialized)
        response = sock.recv(1024)
        logging.info(f"Received response: {response.decode()}")
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        sock.close()

if __name__ == '__main__':
    main()
