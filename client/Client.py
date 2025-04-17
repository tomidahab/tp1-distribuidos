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

    def send_queries(self, queries: list[str]):
        if self.skt is None:
            raise Exception("Socket not connected")
        
        self.protocol.send_all(self.skt, 'Hello world')
        return

        logging.info(f"Sending a total of {len(queries)} queries")
        csvs_sent = {}
        csvs_sent[self.config.get_movies()] = False
        csvs_sent[self.config.get_ratings()] = False
        csvs_sent[self.config.get_crew()] = False
        
        for query in queries:
            if query == self.config.get_query1():
                self._send_query1_csv(csvs_sent)
                csvs_sent[self.config.get_movies()] = True
            elif query == self.config.get_query2():
                self._send_query2_csv(csvs_sent)
                csvs_sent[self.config.get_movies()] = True
            elif query == self.config.get_query3():
                self._send_query3_csv(csvs_sent)
                csvs_sent[self.config.get_movies()] = True
                csvs_sent[self.config.get_ratings()] = True
            elif query == self.config.get_query4():
                self._send_query4_csv(csvs_sent)
                csvs_sent[self.config.get_movies()] = True
                csvs_sent[self.config.get_crew()] = True
            elif query == self.config.get_query5():
                self._send_query5_csv(csvs_sent)
                csvs_sent[self.config.get_movies()] = True
                csvs_sent[self.config.get_ratings()] = True
            else:
                raise ValueError(f"Unknown query: {query}")
        self._send_queries(queries)


    def _send_query2_csv(self, csvs_sent: dict):
        if csvs_sent[self.config.get_movies()]:
            return
    def _send_query3_csv(self, csvs_sent: dict):
        if csvs_sent[self.config.get_movies()] and csvs_sent[self.config.get_ratings()]:
            return
        
    def _send_query4_csv(self, csvs_sent: dict):
        if csvs_sent[self.config.get_movies()] and csvs_sent[self.config.get_crew()]:
            return
        
    def _send_query5_csv(self, csvs_sent: dict):
        if csvs_sent[self.config.get_movies()] and csvs_sent[self.config.get_ratings()]:
            return
            
    def _send_query1_csv(self, csvs_sent: dict):
        if csvs_sent[self.config.get_movies()]:
            return
        
        for batch in self._read_file_in_batches(self.config.get_movies(), self.config.get_batch_size()):
            self.protocol.send_all(self.skt, batch)
        self.protocol.send_all(self.skt, b'\n')
        logging.info(f"Sent .csv from query 1: {self.config.get_movies()    }")
        
    def _read_file_in_batches(self, file_path: str, batch_size: int):
        # Validate inputs
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        try:
            with open(file_path, 'rb') as f:
                while True:
                    data = f.read(batch_size)
                    if not data:
                        break
                    yield data
        except IOError as e:
            raise IOError(f"Error reading file {file_path}: {e}")

    def _send_queries(self, queries: list[str]):
        if not queries:
            raise ValueError("No queries to send")

        for query in queries:
            self.protocol.send_all(self.skt, query.encode('utf-8'))
        self.protocol.send_all(self.skt, b'\n')
        logging.info(f"Sent queries: {queries}")

    def recv_response(self):
        if self.skt is None:
            raise Exception("Socket not connected")
        
        data = self.protocol.recv_response(self.skt)
        return data
