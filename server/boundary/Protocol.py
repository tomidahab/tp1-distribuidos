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

    async def send_all(self, sock, data, query_type="1"):
        """
        Send data with query type indicator
        First sends 4 bytes with the length (including query type byte),
        then sends 1 byte for query type, then the actual data
        """
        # Get data bytes
        data_bytes = data.encode('utf-8') if isinstance(data, str) else data
        
        # Get query type byte
        query_byte = query_type.encode('utf-8')[0:1]  # Just take the first byte
        
        # Calculate total length (data + query byte)
        total_length = len(data_bytes) + 1
        length_bytes = total_length.to_bytes(4, byteorder='big')
        
        # Send length bytes
        await self._send_all(sock, length_bytes)
        
        # Send query type byte
        await self._send_all(sock, query_byte)
        
        # Send data bytes
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

    def decode(self, data_bytes):
        return data_bytes.decode('utf-8')
