import socket
import os
from Protocol import Protocol
import logging
from Config import Config


logging.basicConfig(level=logging.INFO)


class Client:
    def __init__(self, name: str, age: int):
        self.skt = None
        self.name = name
        self.age = age
        self.protocol = Protocol()
        self.config = Config()

    def __str__(self):
        return f"Client(name={self.name}, age={self.age})"
    
    def connect(self, host: str, port: int):
        self.skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.skt.connect((host, port))
        logging.info(f"Connected to {host}:{port}")
        # self.skt.settimeout(5)
        # logging.info(f"Socket timeout set to 5 seconds")

    def shutdown(self):
        if self.skt is None:
            return             
        try:
            self.skt.shutdown(socket.SHUT_RDWR)
            logging.info("Socket shutdown successfully")
        except OSError:
            pass
        finally:
            self.skt.close()
            self.skt = None

    def send_csv(self, file_path: str = None):
        if self.skt is None:
            raise Exception("Socket not connected")
        logging.info(f"\033[94mSending CSV file: {file_path}\033[0m")
        
        for batch in self._read_file_in_batches(file_path, self.config.get_batch_size()):
            self.protocol.send_all(self.skt, batch)
        self.protocol.send_all(self.skt, self.config.get_EOF())
        logging.info(f"\033[94mCSV file sent successfully with EOF: {self.config.get_EOF()}\033[0m")
        
    def _read_file_in_batches(self, file_path: str, batch_size: int):
        # Validate inputs
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        try:
            with open(file_path, 'rb') as f:
                batch = b''
                for line in f:
                    # Check if adding this line would exceed batch_size
                    if len(batch) + len(line) > batch_size and batch:
                        # If yes, yield current batch and start a new one with this line
                        yield batch
                        batch = line
                    else:
                        # Otherwise, add line to current batch
                        batch += line
                # Don't forget to yield the last batch if it has data
                if batch:
                    yield batch
        except IOError as e:
            raise IOError(f"Error reading file {file_path}: {e}")

    def recv_response(self):
        if self.skt is None:
            raise Exception("Socket not connected")
        
        data = self.protocol.recv_response(self.skt)
        return data
