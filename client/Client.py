import socket
import os
import threading
import json
import enum
import pathlib
from datetime import datetime
from Protocol import Protocol
import logging
from Config import Config


logging.basicConfig(level=logging.INFO)

class QueryType(enum.Enum):
    QUERY_1 = os.getenv("QUERY_1", "1")
    QUERY_3 = os.getenv("QUERY_3", "3")
    QUERY_4 = os.getenv("QUERY_4", "4")
    QUERY_5 = os.getenv("QUERY_5", "5")

class Client:
    def __init__(self, name: str):
        self.skt = None
        self.name = name
        self.protocol = Protocol()
        self.config = Config()
        self.receiver_running = False
        self.receiver_thread = None
        
        self.output_dir = pathlib.Path("output")
        
        self.output_dir.mkdir(exist_ok=True)
        
        self.output_files = {
            QueryType.QUERY_1: self.output_dir / f"output_records_client_{self.name}_Q1.json",
            QueryType.QUERY_3: self.output_dir / f"output_records_client_{self.name}_Q3.json",
            QueryType.QUERY_4: self.output_dir / f"output_records_client_{self.name}_Q4.json",
            QueryType.QUERY_5: self.output_dir / f"output_records_client_{self.name}_Q5.json",
        }
    
    def __str__(self):
        return f"Client(name={self.name})"
    
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
        if self.receiver_thread and self.receiver_thread.is_alive():
            self.receiver_thread.join()
            if self.receiver_thread.is_alive():
                logging.warning("Receiver thread did not terminate gracefully")

    def _send_csv(self, file_path: str = None):
        if self.skt is None:
            raise Exception("Socket not connected")
        logging.info(f"\033[94mSending CSV file: {file_path}\033[0m")
        batch_sent = 0
        
        for batch in self._read_file_in_batches(file_path, self.config.get_batch_size()):
            self.protocol.send_all(self.skt, batch)
            batch_sent += 1
            if batch_sent % 50 == 0 and "ratings" in file_path:
                logging.info(f"Sent {batch_sent} batches so far...")
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
        logging.info(f"Receiver thread started, logging to output files")
        return self.receiver_thread
    
    def _receive_loop(self):
        """Continuously receive messages and log them to a file based on query"""
        # Define handlers for each query type using a dictionary dispatch
        query_handlers = {
            QueryType.QUERY_1.value: self._handle_query_1,
            QueryType.QUERY_3.value: self._handle_query_3,
            QueryType.QUERY_4.value: self._handle_query_4,
            QueryType.QUERY_5.value: self._handle_query_5,
        }
        
        try:
            while self.receiver_running and self.skt:
                try:
                    query, response_data = self.protocol.recv_response(self.skt)
                    try:
                        parsed_data = json.loads(response_data)
                        
                        # Use dictionary dispatch to handle the query
                        if query in query_handlers:
                            query_handlers[query](parsed_data)
                            
                    except json.JSONDecodeError as e:
                        logging.error(f"Failed to parse response as JSON: {e}")
                        logging.info(f"Raw response: {response_data[:100]}...")
                except socket.timeout:
                    # Just continue if we get a timeout
                    continue
                except (ConnectionError, socket.error, OSError) as e:
                    # Handle socket errors including when socket is closed
                    if not self.receiver_running:
                        logging.info(f"Socket communication ended due to shutdown: {e}")
                    else:
                        logging.error(f"Connection error in receiver thread: {e}")
                    break
        except Exception as e:
            logging.error(f"Error in receiver thread: {e}")
        
        logging.info("Receiver thread stopping")

    def _handle_query_1(self, data):
        """Handle Query 1 data processing"""
        formatted_data = self._format_data_query_1(data)
        self._write_to_file(self.output_files[QueryType.QUERY_1], formatted_data)

    def _handle_query_3(self, data):
        """Handle Query 3 data processing"""
        # No formatting needed for now
        self._write_to_file(self.output_files[QueryType.QUERY_3], data)
        logging.info(f"\033[94mReceived data for Query {QueryType.QUERY_3.value}\033[0m")

    def _handle_query_4(self, data):
        """Handle Query 4 data processing"""
        formatted_data = self._format_data_query_4(data)
        self._write_to_file(self.output_files[QueryType.QUERY_4], formatted_data)
        logging.info(f"\033[94mReceived data for Query {QueryType.QUERY_4.value}\033[0m")

    def _handle_query_5(self, data):
        """Handle Query 5 data processing"""
        # No formatting needed for now
        self._write_to_file(self.output_files[QueryType.QUERY_5], data)
        logging.info(f"\033[94mReceived data for Query {QueryType.QUERY_5.value}\033[0m")

    def _write_to_file(self, file_path: pathlib.Path, data: list):
        """
        Write processed data to a file
        """
        try:
            with open(file_path, 'a') as f:
                for record in data:
                    f.write(json.dumps(record) + "\n")
        except IOError as e:
            logging.error(f"Error writing to file {file_path}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error writing to file {file_path}: {e}")

    def _format_data_query_4(self, data):
        """
        Format data for Query 4
        """
        # Transform the data into a more user-friendly format
        formatted_data = []
        for actor in data:
            formatted_data.append({actor.get('name', 'Unknown'): actor.get('count', 0) })
        return formatted_data

    def _format_data_query_1(self, data):
        """
        Format data for Query 1
        """
        # Transform the data into a more user-friendly format
        formatted_data = []
        for movie in data:
            # Parse the genres string into a list of genre names
            genres_list = []
            try:
                # The genres are stored as a string representation of a list of dicts
                genres_data = json.loads(movie.get('genres', '[]').replace("'", '"'))
                genres_list = [genre.get('name') for genre in genres_data if genre.get('name')]
            except (json.JSONDecodeError, AttributeError, TypeError):
                # If we can't parse the genres, use an empty list
                pass
            
            # Create a formatted movie entry - without ID
            formatted_movie = {
                "Movie": movie.get('original_title', 'Unknown'),
                "Genres": genres_list
            }
            formatted_data.append(formatted_movie)
        return formatted_data
