import asyncio
import logging
import signal
import socket
import uuid
import os
from Protocol import Protocol
from rabbitmq.Rabbitmq_client import RabbitMQClient
import csv
import json
from io import StringIO
from common.Serializer import Serializer
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Router queue names from environment variables
MOVIES_ROUTER_QUEUE = os.getenv("MOVIES_ROUTER_QUEUE")
CREDITS_ROUTER_QUEUE = os.getenv("CREDITS_ROUTER_QUEUE")
RATINGS_ROUTER_QUEUE = os.getenv("RATINGS_ROUTER_QUEUE")
#TODO move this to a common config file or common env var since worker has this too
BOUNDARY_QUEUE_NAME = "filter_by_year_workers"
RESPONSE_QUEUE = "response_queue"
COUNTRIES_BUDGET_WORKERS = 2
BUDGET_QUEUE = "countries_budget_workers"

COLUMNS_Q1 = {'genres': 3, 'id':5, 'original_title': 8, 'production_countries': 13, 'release_date': 14}
COLUMNS_Q2 = {'budget':2,'genres': 3, 'imdb_id':6, 'original_title': 8, 'production_countries': 13, 'release_date': 14}
COLUMNS_Q3 = {'id': 1, 'rating': 2}
COLUMNS_Q4 = {"cast": 0, "movie_id": 2}
COLUMNS_Q5 = {'budget': 2, 'imdb_id':6, 'original_title': 8, 'overview': 9, 'revenue': 15}
EOF_MARKER = "EOF_MARKER"

RESPONSE_QUEUE = os.getenv("RESPONSE_QUEUE", "response_queue")
MAX_CSVS = 3
MOVIES_CSV = 0
CREDITS_CSV = 1
RATINGS_CSV = 2

SIGTERM = "SIGTERM"
SIGTERM_QUEUE = "sigterm_queue"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

class Boundary:
  def __init__(self, port=5000, listen_backlog=100, movies_router_queue=MOVIES_ROUTER_QUEUE, credits_router_queue=CREDITS_ROUTER_QUEUE, ratings_router_queue=RATINGS_ROUTER_QUEUE, countries_budget_queue= BUDGET_QUEUE):
    self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self._server_socket.bind(("", port))
    self._server_socket.listen(listen_backlog)
    self._server_socket.setblocking(False)
    self._running = True
    self._client_sockets = []
    self.protocol = Protocol
    self.id = uuid.uuid4()
    
    # Create RabbitMQ client instance
    self.rabbitmq = RabbitMQClient()  # Using default parameters
    
    # Router queues for different CSV types
    self.countries_budget_queue = countries_budget_queue
    self.movies_router_queue = movies_router_queue
    self.credits_router_queue = credits_router_queue
    self.ratings_router_queue = ratings_router_queue

    signal.signal(signal.SIGINT, self._handle_shutdown)
    signal.signal(signal.SIGTERM, self._handle_shutdown)

    logging.info(self.green(f"Boundary ID: {self.id} successfully created"))
    logging.info(f"Using router queues: Movies={self.movies_router_queue}, Credits={self.credits_router_queue}, Ratings={self.ratings_router_queue}")

  # TODO: Move to printer class
  def green(self, text): return f"\033[92m{text}\033[0m"

# ------------------------------------------------------------------ #
# main accept‑loop                                                   #
# ------------------------------------------------------------------ #
  async def run(self):
    loop = asyncio.get_running_loop()
    logging.info(f"Listening on {self._server_socket.getsockname()[1]}")
    
    # Set up RabbitMQ
    await self._setup_rabbitmq()

    # Start response queue consumer task
    asyncio.create_task(self._handle_response_queue())

    while self._running:
      try:
        client_sock, addr = await loop.sock_accept(self._server_socket)
        client_id = str(uuid.uuid4())  # Convert UUID to string immediately
        logging.info(self.green(f'client id {client_id}'))
      except asyncio.CancelledError:
        break
      except Exception as exc:
        logging.error(f"Accept failed: {exc}")
        continue

      logging.info(f"New client {addr[0]}:{addr[1]}")
      self._client_sockets.append({client_id: client_sock})
      asyncio.create_task(self._handle_client_connection(client_sock, addr, client_id))


# ------------------------------------------------------------------ #
# response queue consumer logic                                      #
# ------------------------------------------------------------------ #
  async def _handle_response_queue(self):
    """
    Handle messages from the response queue.
    This method blocks waiting for messages without consuming CPU.
    """
    try:
        logging.info(self.green(f"Client_socket: {self._client_sockets}"))
        
        # Set up consumer - this blocks waiting for messages
        await self.rabbitmq.consume(
            queue_name=RESPONSE_QUEUE,
            callback=self._process_response_message,
            no_ack=False
        )
        
        logging.info(self.green(f"Started consuming from {RESPONSE_QUEUE}"))
        
        # Keep this task alive while the service is running
        while self._running:
            await asyncio.sleep(1)
            
    except Exception as e:
        logging.error(f"Error in response queue handler: {e}")

  async def _process_response_message(self, message):
    """Process messages from the response queue"""
    try:
        # Deserialize the message
        deserialized_message = Serializer.deserialize(message.body)

        logging.info(f"RESPONSE RECEIVED: {deserialized_message}")
        
        # Extract client_id and data from the deserialized message
        client_id = deserialized_message.get("client_id")
        data = deserialized_message.get("data")
        query = deserialized_message.get("query")

        if not data:
            logging.warning(f"Response message contains no data")
            await message.ack()
            return
        
        # Convert data to list if it's not already
        if not isinstance(data, list):
            data = [data]
        
        # Find the client socket by client_id
        client_socket = None
        for client_dict in self._client_sockets:
            if client_id in client_dict:
                client_socket = client_dict[client_id]
                break
        
        # Send the data to the client if the socket is found
        if client_socket:
            try:
                # Prepare data for sending
                proto = self.protocol(asyncio.get_running_loop())
                
                serialized_data = json.dumps(data)
                await proto.send_all(client_socket, serialized_data, query)
                
            except Exception as e:
                logging.error(f"Failed to send data to client {client_id}: {e}")
        else:
            logging.warning(f"Client socket not found for client ID: {client_id}")
        
        # Acknowledge message
        await message.ack()
        
    except Exception as e:
        logging.error(f"Error processing response message: {e}")
        # Reject message but don't requeue
        await message.reject(requeue=False)

# ------------------------------------------------------------------ #
# per‑client logic                                                   #
# ------------------------------------------------------------------ #
  async def _handle_client_connection(self, sock, addr, client_id):
    loop = asyncio.get_running_loop()
    proto = self.protocol(loop)
    logging.info(self.green(f"Client ID: {client_id} successfully started"))
    csvs_received = 0
    try:
        data = ''
        while csvs_received < MAX_CSVS:
            try:
                data = await self._receive_csv_batch(sock, proto)
                if data == EOF_MARKER:
                    #TODO CHECK HERE ON QUERY 2 THAT SENDS TO ALL WORKERS
                    await self._send_eof_marker(csvs_received, client_id)
                    csvs_received += 1
                    logging.info(self.green(f"EOF received for CSV #{csvs_received} from client {addr[0]}:{addr[1]}"))
                    continue
                elif data == SIGTERM:
                    logging.info("received SIGTERM")
                    await self._send_sigterm(5, client_id)
                    #Here send to the 5 nodes the sigterm
                    return

                if csvs_received == MOVIES_CSV:
                    filtered_data_q1,filtered_data_q2, filtered_data_q5 = self._project_to_columns(data, [COLUMNS_Q1,COLUMNS_Q2, COLUMNS_Q5])
                    
                    # Send data for Q1 to the movies router
                    prepared_data_q1 = self._addMetaData(client_id, filtered_data_q1)
                    await self._send_data_to_rabbitmq_queue(prepared_data_q1, self.movies_router_queue)

                    prepared_data_q2 = self._addMetaData(client_id, filtered_data_q2)
                    await self._send_data_to_rabbitmq_queue(prepared_data_q2, self.countries_budget_queue)

                    # Send data for Q5 to the reviews router
                    prepared_data_q5 = self._addMetaData(client_id, filtered_data_q5)
                    #TODO change it so instead of sending to the sentiment analysis worker, it sends to the movies router
                    # and the router sends it to the respective worker based on the query in the metadata
                    await self._send_data_to_rabbitmq_queue(prepared_data_q5, "sentiment_analysis_worker")
                
                elif csvs_received == CREDITS_CSV:
                    filtered_data = self._project_to_columns(data, COLUMNS_Q4)
                    filtered_data = self._remove_cast_extra_data(filtered_data)
                    prepared_data = self._addMetaData(client_id, filtered_data)
                    await self._send_data_to_rabbitmq_queue(prepared_data, self.credits_router_queue)
                
                elif csvs_received == RATINGS_CSV:
                    filtered_data = self._project_to_columns(data, COLUMNS_Q3)
                    filtered_data = self._remove_ratings_with_0_rating(filtered_data)
                    prepared_data = self._addMetaData(client_id, filtered_data)
                    await self._send_data_to_rabbitmq_queue(prepared_data, self.ratings_router_queue)
                
            except ConnectionError:
                logging.info(f"Client {addr[0]}:{addr[1]} disconnected")
                break
               
    except Exception as exc:
        logging.error(f"Client {addr[0]}:{addr[1]} error: {exc}")
        logging.exception(exc)

  async def _send_eof_marker(self, csvs_received, client_id):
        prepared_data = self._addMetaData(client_id, None, True)
        if csvs_received == MOVIES_CSV:
           await self._send_data_to_rabbitmq_queue(prepared_data, self.movies_router_queue)
           #TODO: change it so instead of sending to the sentiment analysis worker, it sends to the movies router
           # and the router sends it to the respective worker based on the query in the metadata

           #TODO CHANGE THIS TO ROUTER?
           for i in range(0,COUNTRIES_BUDGET_WORKERS):
                await self._send_data_to_rabbitmq_queue(prepared_data, self.countries_budget_queue)

           await self._send_data_to_rabbitmq_queue(prepared_data, "sentiment_analysis_worker")
        elif csvs_received == CREDITS_CSV:
            await self._send_data_to_rabbitmq_queue(prepared_data, self.credits_router_queue)
        elif csvs_received == RATINGS_CSV:
           await self._send_data_to_rabbitmq_queue(prepared_data, self.ratings_router_queue)

  async def _send_sigterm(self, n, client_id):
        prepared_data = self._addMetaData(client_id, SIGTERM, False)
        for i in range(0,n):
            await self._send_data_to_rabbitmq_queue(prepared_data, SIGTERM_QUEUE)

  def _remove_ratings_with_0_rating(self, data):
    """
    Remove rows with a rating of 0 from the ratings data.
    The rating field is expected to be a string that can be converted to a float.
    """
    result = []
    for row in data:
        if 'rating' in row and row['rating']:
            try:
                # Convert rating to float and check if it's greater than 0
                rating = float(row['rating'])
                if rating > 0:
                    result.append(row)
            except ValueError:
                # Skip rows with unparseable rating data
                logging.warning(f"Could not parse rating data: {row['rating']}")
                pass
    return result

  def _remove_cast_extra_data(self, data):
    """
    Remove extra data from the cast field and filter out rows without cast.
    The cast field is expected to be a list of dictionaries, and we only want the 'name' field.
    Rows without cast data are excluded from the result.
    """
    result = []
    for row in data:
        if 'cast' in row and row['cast']:
            try:
                # Handle the string format properly - it's using Python's repr format
                # First, make it valid JSON by replacing Python-style single quotes
                cast_str = row['cast']
                # Use ast.literal_eval which can safely parse Python literal structures
                import ast
                cast_data = ast.literal_eval(cast_str)
                
                # Extract only the 'name' field from each dictionary
                cast_names = [item.get('name') for item in cast_data if isinstance(item, dict) and item.get('name')]
                
                # Only include rows where we have valid cast names
                if cast_names:
                    row['cast'] = cast_names
                    result.append(row)
            except (SyntaxError, ValueError, TypeError) as e:
                # Skip rows with unparseable cast data
                logging.warning(f"Could not parse cast data: {e}")
                pass
    return result

  def _project_to_columns(self, data, query_columns):
    """
    Extract only the columns defined in query_columns from the CSV data.
    If query_columns is a list, returns multiple result sets, one for each element.
    
    Returns an array of dictionaries (or multiple arrays if query_columns is a list),
    where each dictionary represents a row with column names as keys and the corresponding values.
    
    Special processing is done for Q5 data to ensure budget and revenue values are valid.
    """
    # Use Python's csv module to correctly parse the CSV data
    input_file = StringIO(data)
    csv_reader = csv.reader(input_file)
    
    # Check if query_columns is a list of column mappings or a single mapping
    if isinstance(query_columns, list):
        # Initialize result arrays - one for each element in query_columns list
        results = [[] for _ in range(len(query_columns))]
        
        for row in csv_reader:
            if not row:
                continue
                
            # Process each query column set
            for i, columns in enumerate(query_columns):
                # Skip if row doesn't have enough columns
                if len(row) <= max(columns.values()):
                    continue
                
                # Apply different processing based on which column set we're dealing with
                if 'budget' in columns and 'revenue' in columns:  # This is Q5
                    # Create a dictionary for Q5 with required columns
                    row_dict = {col_name: row[col_idx] for col_name, col_idx in columns.items()}
                    
                    # Check if any field is empty, null or nan
                    if any(not row_dict.get(field, '') for field in columns.keys()):
                        continue  # Skip this row if any field is empty
                    
                    # Check if budget or revenue is 0 or not a valid number
                    try:
                        budget = float(row_dict.get('budget', '0'))
                        revenue = float(row_dict.get('revenue', '0'))
                        
                        # Skip rows where budget or revenue is 0
                        if budget <= 0 or revenue <= 0:
                            continue
                            
                        # Store the numeric values back in the dictionary
                        row_dict['budget'] = budget
                        row_dict['revenue'] = revenue
                        
                        # Only append if all checks pass
                        results[i].append(row_dict)
                    except ValueError:
                        # Skip if budget or revenue is not a valid number
                        continue
                else:  # This is Q1 or any other query
                    # Create a standard dictionary for this row
                    row_dict = {col_name: row[col_idx] for col_name, col_idx in columns.items()}
                    results[i].append(row_dict)
        
        return results
    else:
        # Single query column set - maintain backward compatibility
        result = []
        
        for row in csv_reader:
            if not row or len(row) <= max(query_columns.values()):
                continue
                
            # Create a dictionary for this row with column names as keys
            row_dict = {col_name: row[col_idx] for col_name, col_idx in query_columns.items()}
            result.append(row_dict)
        
        return result
  
  def _addMetaData(self, client_id, data, is_eof_marker=False):
    message = {        
      "client_id": client_id,
      "data": data,
      "EOF_MARKER": is_eof_marker
    }
    return message
  
  # TODO: Move to protocol class
  async def _receive_csv_batch(self, sock, proto):
    """
    Receive a CSV batch from the socket
    First read 4 bytes to get the length, then read the actual data
    """
    length_bytes = await proto.recv_exact(sock, 4)
    msg_length = int.from_bytes(length_bytes, byteorder='big')

    data_bytes = await proto.recv_exact(sock, msg_length)
    data = proto.decode(data_bytes)

    return data

# ----------------------------------------------------------------------- #
# Rabbit-Related-Section                                                  #
# ----------------------------------------------------------------------- #

  async def _setup_rabbitmq(self, retry_count=1):
    connected = await self.rabbitmq.connect()
    if not connected:
        logging.error(f"Failed to connect to RabbitMQ, retrying in {retry_count} seconds...")
        wait_time = min(30, 2 ** retry_count)
        await asyncio.sleep(wait_time)
        return await self._setup_rabbitmq(retry_count + 1)
    
    # Declare all necessary queues
    await self.rabbitmq.declare_queue(self.movies_router_queue, durable=True)
    await self.rabbitmq.declare_queue(self.credits_router_queue, durable=True)
    await self.rabbitmq.declare_queue(self.ratings_router_queue, durable=True)
    await self.rabbitmq.declare_queue(RESPONSE_QUEUE, durable=True)
    await self.rabbitmq.declare_queue(SIGTERM_QUEUE, durable=True)
    
    logging.info("All router queues declared successfully")
  
  async def _send_data_to_rabbitmq_queue(self, data, queue_name):
    """
    Send the data to RabbitMQ queue after serializing it
    
    Args:
        data: The data to send
        queue_name: The queue to send to
    """
    try:
        # Serialize the data to binary
        serialized_data = Serializer.serialize(data)
        success = await self.rabbitmq.publish_to_queue(
            queue_name=queue_name,
            message=serialized_data,
            persistent=True
        )
        
        if not success:
            logging.error(f"Failed to publish data to {queue_name}")
    except Exception as e:
        logging.error(f"Error publishing data to queue '{queue_name}': {e}")
         
# ------------------------------------------------------------------ #
# graceful shutdown handler                                          #
# ------------------------------------------------------------------ #

  def _handle_shutdown(self, *_):
      logging.info(f"Shutting down server")
      self._running = False
      self._server_socket.close()
      
      # Close RabbitMQ connection
      asyncio.create_task(self.rabbitmq.close())
      
      for sock in self._client_sockets:
          try:
              sock.shutdown(socket.SHUT_RDWR)
          except OSError:
              pass
          sock.close()
