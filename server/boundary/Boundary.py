import asyncio
import logging
import signal
import socket
import uuid
import os
from Protocol import Protocol
from rabbitmq.Rabbitmq_client import RabbitMQClient
import csv
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

COLUMNS_Q1 = {'genres': 3, 'id':5, 'original_title': 8, 'production_countries': 13, 'release_date': 14}
COLUMNS_Q4 = {"cast": 0, "movie_id": 2}
EOF_MARKER = "EOF_MARKER"

RESPONSE_QUEUE = os.getenv("RESPONSE_QUEUE", "response_queue")
MAX_CSVS = 3
MOVIES_CSV = 0
CREDITS_CSV = 1
RATINGS_CSV = 2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

class Boundary:
  def __init__(self, port=5000, listen_backlog=100, movies_router_queue=MOVIES_ROUTER_QUEUE, credits_router_queue=CREDITS_ROUTER_QUEUE, ratings_router_queue=RATINGS_ROUTER_QUEUE):
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
  async def _handle_client_connection(self, sock, addr, client_id=None):
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
                    await self._send_eof_marker(csvs_received, client_id)
                    csvs_received += 1
                    logging.info(self.green(f"EOF received for CSV #{csvs_received} from client {addr[0]}:{addr[1]}"))
                    continue

                # Process and route data based on CSV type
                if csvs_received == MOVIES_CSV:
                    # Process and send to movies router
                    filtered_data = self._project_to_columns(data, COLUMNS_Q1)
                    prepared_data = self._addMetaData(client_id, filtered_data)
                    await self._send_data_to_rabbitmq_queue(prepared_data, self.movies_router_queue)
                
                elif csvs_received == CREDITS_CSV:
                    # Process and send to credits router
                    # For now, just forward the raw data - you can add specific processing later
                    filtered_data = self._project_to_columns(data, COLUMNS_Q4)
                    filtered_data = self._removed_cast_extra_data(filtered_data)
                    prepared_data = self._addMetaData(client_id, filtered_data)
                    await self._send_data_to_rabbitmq_queue(prepared_data, self.credits_router_queue)
                
                elif csvs_received == RATINGS_CSV:
                    # Process and send to ratings router
                    # For now, just forward the raw data - you can add specific processing later
                    prepared_data = self._addMetaData(client_id, data)
                    await self._send_data_to_rabbitmq_queue(prepared_data, self.ratings_router_queue)
                
            except ConnectionError:
                logging.info(f"Client {addr[0]}:{addr[1]} disconnected")
                break
               
    except Exception as exc:
        logging.error(f"Client {addr[0]}:{addr[1]} error: {exc}")

  async def _send_eof_marker(self, csvs_received, client_id):
        prepared_data = self._addMetaData(client_id, None, True)
        if csvs_received == MOVIES_CSV:
           await self._send_data_to_rabbitmq_queue(prepared_data, self.movies_router_queue)
        elif csvs_received == CREDITS_CSV:
            await self._send_data_to_rabbitmq_queue(prepared_data, self.credits_router_queue)
        elif csvs_received == RATINGS_CSV:
           await self._send_data_to_rabbitmq_queue(prepared_data, self.ratings_router_queue)

  def _removed_cast_extra_data(self, data):
    """
    Remove extra data from the cast field and filter out rows without cast.
    The cast field is expected to be a list of dictionaries, and we only want the 'name' field.
    Rows without cast data are excluded from the result.
    """
    result = []
    for row in data:
        if 'cast' in row:
            try:
                # Parse the JSON string into a list of dictionaries
                cast_data = json.loads(row['cast'].replace("'", '"'))
                # Extract only the 'name' field from each dictionary
                cast_names = [item.get('name') for item in cast_data if isinstance(item, dict) and item.get('name')]
                
                # Only include rows where we have valid cast names
                if cast_names:
                    row['cast'] = cast_names
                    result.append(row)
            except (json.JSONDecodeError, TypeError):
                # Skip rows with unparseable cast data
                pass
    return result

  def _project_to_columns(self, data, query_columns):
    """
    Extract only the columns defined in COLUMNS from the CSV data.
    Returns an array of dictionaries, where each dictionary represents a row
    with column names as keys and the corresponding values.
    Handles properly quoted fields that may contain commas.
    """
    # Use Python's csv module to correctly parse the CSV data
    input_file = StringIO(data)
    csv_reader = csv.reader(input_file)
    
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
