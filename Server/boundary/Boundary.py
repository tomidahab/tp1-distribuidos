import asyncio
import logging
import signal
import socket
import uuid
from Protocol import Protocol

# https://stackoverflow.com/questions/77349095/how-to-communicate-between-a-server-and-two-clients-using-pythons-asynchio

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

    signal.signal(signal.SIGINT, self._handle_shutdown)
    signal.signal(signal.SIGTERM, self._handle_shutdown)

    logging.info(self.green(f"Boundary ID: {self.id} successfully created"))

  def green(self, text): return f"\033[92m{text}\033[0m"

  async def run(self):
    loop = asyncio.get_running_loop()
    logging.info("Listening on *:%d", self._server_socket.getsockname()[1])

    while self._running:
      try:
        client_sock, addr = await loop.sock_accept(self._server_socket)
      except asyncio.CancelledError:
        break
      except Exception as exc:
        logging.error("Accept failed: %s", exc)
        continue

      logging.info("New client %s:%d", *addr)
      self._client_sockets.append(client_sock)
      asyncio.create_task(self._handle_client_connection(client_sock, addr))

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
                await self._send_data(data) # Pushes to Rabbit Q
            except ConnectionError:
                logging.info("Client %s:%d disconnected", *addr)
                break
        logging.info("EOF received from client %s:%d", *addr)
          # Wait for the response of the system
          # response = queue.get() # block until a message is available
          # logging.info("Response from server: %s", response)
          # Send the response back to the client
        await proto.send_all(sock, 'response')
                
    except Exception as exc:
        logging.error("Client %s:%d error: %s", *addr, exc)

    # finally:
    #     try:
    #         sock.shutdown(socket.SHUT_RDWR)
    #     except OSError:
    #         pass # Socket might already be closed
    #     sock.close()
    #     if sock in self._client_sockets:
    #         self._client_sockets.remove(sock)
    #     logging.info("Connection gracefully closed %s:%d", *addr)

  def _handle_shutdown(self, *_):
      logging.info("Shutting down server")
      self._running = False
      self._server_socket.close()
      for sock in self._client_sockets:
          try:
              sock.shutdown(socket.SHUT_RDWR)
          except OSError:
              pass
          sock.close()

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


  async def _send_data(self, batch):
    #  logging.info("Sending data to RabbitMQ: %s", batch[:50])  # show just the beginning
     return
