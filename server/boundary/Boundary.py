import asyncio
import logging
import signal
import socket
import uuid
from Protocol import Protocol
from rabbitmq.Rabbitmq_client import RabbitMQClient
import csv
from io import StringIO
from common.Serializer import Serializer
import json

#TODO move this to a common config file or common env var since worker has this too
BOUNDARY_QUEUE_NAME = "filter_by_year_workers"
BOUNDARY_QUEUE_Q5_NAME = "sentiment_analysis_workers"
COLUMNS = {'genres': 3, 'imdb_id':6, 'original_title': 8, 'production_countries': 13, 'release_date': 14}
COLUMNS_Q5 = {'budget': 2, 'imdb_id':6, 'original_title': 8, 'overview': 9, 'revenue': 15}
EOF_MARKER = "EOF_MARKER"
RESPONSE_QUEUE = "response_queue"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

class Boundary:
  def __init__(self, port=5000, listen_backlog=100):
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
    self._queue_name = BOUNDARY_QUEUE_NAME

    signal.signal(signal.SIGINT, self._handle_shutdown)
    signal.signal(signal.SIGTERM, self._handle_shutdown)

    logging.info(self.green(f"Boundary ID: {self.id} successfully created"))

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
        # Set up consumer - this blocks waiting for messages
        await self.rabbitmq.consume(
            queue_name=RESPONSE_QUEUE,
            callback=self._process_response_message,
            no_ack=False
        )
        
        logging.info(self.green(f"Started consuming from {RESPONSE_QUEUE}"))
        
        # Keep this task alive whileSent 65 registers for Q5 the service is running
        while self._running:
            await asyncio.sleep(1)
            
    except Exception as e:
        logging.error(f"Error in response queue handler: {e}")

# TODO PLEASE RE FACTOR THIS INTO SMALLER FN
  async def _process_response_message(self, message):
      """Process messages from the response queue"""
      try:
          # Deserialize the message
          deserialized_message = Serializer.deserialize(message.body)
          
          # Extract clientId, query and data from the deserialized message
          client_id = deserialized_message.get("clientId")
          query = deserialized_message.get("query", "Q1")  # Default to Q1 if not specified
          data = deserialized_message.get("data")
          
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
                  
                  # Determine query type (1 for Q1, 5 for Q5)
                  query_type = "5" if query == "Q5" else "1"
                  
                  if query == "Q5":
                      # Show a preview of the data (first 3 items or fewer)
                      data_preview = data[:3] if len(data) > 3 else data
                      logging.info(f"SENDING QUERY 5 RESPONSE - Preview of first {len(data_preview)} items: {json.dumps(data_preview)[:200]}...")
                      serialized_data = json.dumps(data)
                  else:
                      # For Q1 and others, format the data as before
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
                          
                          # Create a formatted movie entry - without IMDb ID
                          formatted_movie = {
                              "Movie": movie.get('original_title', 'Unknown'),
                              "Genres": genres_list
                          }
                          formatted_data.append(formatted_movie)
                      
                      serialized_data = json.dumps(formatted_data)
                  
                  # Send with query type
                  await proto.send_all(client_socket, serialized_data, query_type)
                  
                  logging.info(self.green(f"Sent {len(data)} records for query {query} back to client {client_id}"))
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
    try:
        data = ''
        while True:
            try:
                data = await self._receive_csv_batch(sock, proto)
                if data == EOF_MARKER:
                    logging.info(f"EOF received from client {addr[0]}:{addr[1]}")
                    break
                
                filtered_data_Q1, filtered_data_Q5 = self.project_to_columns(data)
                
                prepared_data_Q1 = self._addMetaData(filtered_data_Q1, client_id)
                await self._send_data_to_rabbitmq_queue(prepared_data_Q1)

                if filtered_data_Q5:  # Since im filtering budget and revenue = 0 movies maybe there are no movies if data is broken
                  prepared_data_Q5 = self._addMetaData(filtered_data_Q5, client_id)
                  await self._send_data_to_rabbitmq_queue(prepared_data_Q5, queue_name=BOUNDARY_QUEUE_Q5_NAME)
              
            except ConnectionError:
                logging.info(f"Client {addr[0]}:{addr[1]} disconnected")
                break
                
    except Exception as exc:
        logging.error(f"Client {addr[0]}:{addr[1]} error: {exc}")
        
    # finally:
    #     try:
    #         sock.shutdown(socket.SHUT_RDWR)
    #     except OSError:
    #         pass # Socket might already be closed
    #     sock.close()
    #     if sock in self._client_sockets:
    #         self._client_sockets.remove(sock)
    #     logging.info("Connection gracefully closed %s:%d", *addr)

  def project_to_columns(self, data):
    """
    Extract only the columns defined in COLUMNS from the CSV data.
    Returns an array of dictionaries, where each dictionary represents a row
    with column names as keys and the corresponding values.
    Handles properly quoted fields that may contain commas.
    """
    # Use Python's csv module to correctly parse the CSV data
    input_file = StringIO(data)
    csv_reader = csv.reader(input_file)
    
    result_Q1 = []
    result_Q5 = []
    
    for row in csv_reader:
        # Skip empty rows or rows that don't have enough columns
        if not row:
            continue
            
        # Check if row has enough columns for regular processing
        if len(row) > max(COLUMNS.values()):
            # Create a dictionary for this row with column names as keys
            row_dict = {col_name: row[col_idx] for col_name, col_idx in COLUMNS.items()}
            result_Q1.append(row_dict)
        
        # Check if row has enough columns for Q5 processing
        if len(row) > max(COLUMNS_Q5.values()):
            # Create a dictionary for Q5 with required columns
            row_dict_q5 = {col_name: row[col_idx] for col_name, col_idx in COLUMNS_Q5.items()}
            
            # Check if any field is empty, null or nan
            if any(not row_dict_q5.get(field, '') for field in COLUMNS_Q5.keys()):
                continue  # Skip this row if any field is empty
            
            # Check if budget or revenue is 0 or not a valid number
            try:
                budget = float(row_dict_q5.get('budget', '0'))
                revenue = float(row_dict_q5.get('revenue', '0'))
                
                # Skip rows where budget or revenue is 0
                if budget <= 0 or revenue <= 0:
                    continue
                    
                # Store the numeric values back in the dictionary
                row_dict_q5['budget'] = budget
                row_dict_q5['revenue'] = revenue
                
                # Only append if all checks pass
                result_Q5.append(row_dict_q5)
            except ValueError:
                # Skip if budget or revenue is not a valid number
                continue
    
    return result_Q1, result_Q5
  
  def _addMetaData(self, data, client_id):
    # Yeah this is basically a one line function, but its a function bc if in the future
    # the logic of adding meta data gets more complex is all encapsulated here.
    message = {        
      "clientId": client_id,
      "data": data
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
        await asyncio.sleep(2 ** retry_count)
        return await self._setup_rabbitmq(retry_count + 1)
    
    await self.rabbitmq.declare_queue(self._queue_name, durable=True)
    await self.rabbitmq.declare_queue(BOUNDARY_QUEUE_Q5_NAME, durable=True)
    await self.rabbitmq.declare_queue(RESPONSE_QUEUE, durable=True)
    

  
  async def _send_data_to_rabbitmq_queue(self, data, queue_name=None):
    """
    Send the data to RabbitMQ queue after serializing it
    """
    if queue_name is None:
        queue_name = self._queue_name
        
    try:
        # Serialize the data to binary
        serialized_data = Serializer.serialize(data)
        
        success = await self.rabbitmq.publish_to_queue(
            queue_name=queue_name,
            message=serialized_data,
            persistent=True
        )
        
        if success:
            logging.info(f"Data published to RabbitMQ queue '{queue_name}' ({len(data.get('data', []))} rows)")
        else:
            logging.error(f"Failed to publish data to RabbitMQ queue '{queue_name}'")
    except Exception as e:
        logging.error(f"Error publishing to queue '{queue_name}': {e}")
         
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
