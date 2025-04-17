class Protocol:
    def __init__(self, loop):
        self.loop = loop

    async def recv_exact(self, sock, num_bytes):
        buffer = bytearray()
        while len(buffer) < num_bytes:
            chunk = await self.loop.sock_recv(sock, num_bytes - len(buffer))
            if not chunk:
                raise ConnectionError("Connection closed while receiving data")
            buffer.extend(chunk)
        return bytes(buffer)

    async def send_all(self, sock, data):
        length_bytes, data_bytes = self._encode_data(data)
        await self._send_all(sock, length_bytes)
        await self._send_all(sock, data_bytes)

    async def _send_all(self, sock, data):
        # sock_sendall already ensures all data is sent and returns None, not bytes sent
        await self.loop.sock_sendall(sock, data)
        return len(data)
        

    def _encode_data(self, data):
        data_bytes = data.encode('utf-8')
        length = len(data_bytes)
        length_bytes = length.to_bytes(4, byteorder='big')
        return length_bytes, data_bytes

    def _decode(self, data_bytes):
        return data_bytes.decode('utf-8')
