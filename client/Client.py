import socket
import os
import threading
import json
from datetime import datetime
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
        self.receiver_running = False
        self.receiver_thread = None
        self.output_file = f"output/output_records_client_{self.name}.json"

    def __str__(self):
        return f"Client(name={self.name}, age={self.age})"
    
    def connect(self, host: str, port: int):
        self.skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.skt.connect((host, port))
        logging.info(f"Connected to {host}:{port}")
    
    def shutdown(self):
        # Stop the receiver thread
        self.receiver_running = False
        
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
            
        # Wait for receiver thread to finish if it exists
        # TODO: shutdown the socket so it does not get stuck in the recv when no more data is sent
        if self.receiver_thread and self.receiver_thread.is_alive():
            self.receiver_thread.join()
            if self.receiver_thread.is_alive():
                logging.warning("Receiver thread did not terminate gracefully")

    # Existing methods
    def _send_csv(self, file_path: str = None):
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
                # Skip the header/first line
                f.readline()
                
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

    def _recv_response(self):
        if self.skt is None:
            raise Exception("Socket not connected")
        
        data = self.protocol.recv_response(self.skt)
        return data
    
    # New wrapper methods
    def start_sender_thread(self, file_paths=None):
        """
        Wrapper method to send data files in a separate thread
        file_paths: list of file paths to send, if None uses config movies
        """
        if file_paths is None:
            file_paths = [self.config.get_movies()]
        
        def sender_task():
            try:
                for file_path in file_paths:
                    self._send_csv(file_path)
                logging.info("\033[92mAll files sent successfully\033[0m")
            except Exception as e:
                logging.error(f"Error in sender thread: {e}")
        
        sender_thread = threading.Thread(target=sender_task)
        sender_thread.start()
        return sender_thread
    
    def start_receiver_thread(self):
        """
        Wrapper method to start a receiver thread that continuously listens for messages
        """
        self.receiver_running = True
        self.receiver_thread = threading.Thread(target=self._receive_loop)
        self.receiver_thread.daemon = True
        self.receiver_thread.start()
        logging.info(f"Receiver thread started, logging to {self.output_file}")
        return self.receiver_thread
    
    def _receive_loop(self):
        """Continuously receive messages and log them to a file"""
        try:
            while self.receiver_running and self.skt:
                try:
                    response_data = self.protocol.recv_response(self.skt)
                    try:

                        parsed_data = json.loads(response_data)
                        processed_movies = []
                        
                        for movie in parsed_data:
                            # Format genres as a comma-separated list
                            genres_str = ", ".join(movie.get("Genres", [])) or "No genres"
                            
                            # Create a clean record with just the data we want to log
                            movie_record = {
                                "title": movie.get("Movie", "Unknown"),
                                "genres": genres_str,
                                "year": movie.get("Year", "Unknown"),
                                "rating": movie.get("Rating", "Unknown")
                            }
                            processed_movies.append(movie_record)
                        # Log to file with processed data instead of raw data
                        # Ensure directory exists before writing to file
                        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
                        with open(self.output_file, 'a') as f:
                            for movie in processed_movies:
                                f.write(json.dumps(movie) + "\n")
                    except json.JSONDecodeError as e:
                        logging.error(f"Failed to parse response as JSON: {e}")
                        logging.info(f"Raw response: {response_data[:100]}...")
                except socket.timeout:
                    # Just continue if we get a timeout
                    continue
                except ConnectionError as e:
                    logging.error(f"Connection error in receiver thread: {e}")
                    break
        except Exception as e:
            logging.error(f"Error in receiver thread: {e}")
        
        logging.info("Receiver thread stopping")
