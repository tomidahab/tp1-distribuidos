import asyncio
import logging
import signal
import socket
import uuid
from Protocol import Protocol
from rabbitmq.Rabbitmq_client import RabbitMQClient

#TODO move this to a common config file or common env var since worker has this too
BOUNDARY_QUEUE_NAME = "filter_by_year_workers"

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
    
    while self._running:
      try:
        client_sock, addr = await loop.sock_accept(self._server_socket)
      except asyncio.CancelledError:
        break
      except Exception as exc:
        logging.error(f"Accept failed: {exc}")
        continue

      logging.info(f"New client {addr[0]}:{addr[1]}")
      self._client_sockets.append(client_sock)
      asyncio.create_task(self._handle_client_connection(client_sock, addr))

# ------------------------------------------------------------------ #
# per‑client logic                                                   #
# ------------------------------------------------------------------ #
  async def _handle_client_connection(self, sock, addr):
    loop = asyncio.get_running_loop()
    proto = self.protocol(loop)
    client_id = uuid.uuid4()
    logging.info(self.green(f"Client ID: {client_id} successfully started"))
    try:
        data = ''
        while data != "EOF_MARKER":
            try:
                data = await self._receive_csv_batch(sock, proto)
                await self._send_data_to_rabbitmq_queue(data)
            except ConnectionError:
                logging.info(f"Client {addr[0]}:{addr[1]} disconnected")
                break
            logging.info(f"EOF received from client {addr[0]}:{addr[1]}")
                
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
    # logging.info("CSV chunk received: %s", data[:50])  # show just the beginning

    return data

# ----------------------------------------------------------------------- #
# Rabbit-Related-Section                                                  #
# ----------------------------------------------------------------------- #

  async def _setup_rabbitmq(self, retry_count=1):
    # Connect to RabbitMQ
    connected = await self.rabbitmq.connect()
    if not connected:
        logging.error(f"Failed to connect to RabbitMQ, retrying in {retry_count} seconds...")
        await asyncio.sleep(2 ** retry_count)
        return await self._setup_rabbitmq(retry_count + 1)
    
    # Just declare the queue - no need for exchange or binding
    await self.rabbitmq.declare_queue(self._queue_name, durable=True)

  async def _send_data_to_rabbitmq_queue(self, data):
    """
    Send the data data directly to RabbitMQ queue
    """
    success = await self.rabbitmq.publish_to_queue(
        queue_name=self._queue_name,
        message=data,
        persistent=True
    )
    
    if success:
        logging.info(f"Data published to RabbitMQ queue ({len(data)} bytes)")
    else:
        logging.error(f"Failed to publish data to RabbitMQ")
         
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
