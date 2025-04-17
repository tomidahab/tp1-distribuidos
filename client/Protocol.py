class Protocol:

    def __init__(self):
        """
        Initialize the Protocol class
        """
        pass

    def recv_exact(self, sock, num_bytes):
        """
        Helper function to receive a specific number of bytes, handling short reads
        """
        buffer = bytearray()
        while len(buffer) < num_bytes:
            packet = sock.recv(num_bytes - len(buffer))
            if not packet:
                raise ConnectionError("Connection closed while receiving data")
            buffer.extend(packet)
        return buffer

    def send_all(self, sock, data: str):
        """
        Helper to send all string, handling short writes
        First sends 4 bytes with the length, then the actual data
        """
        length_bytes, data_bytes = self._encode_data(data)
        
        # Send the length bytes first
        self._send_all(sock, length_bytes)
        
        # Then send the message bytes
        self._send_all(sock, data_bytes)
    
    def _encode_data(self, data: str):
        """
        Encode string data to bytes and prepare length header
        Returns tuple of (length_bytes, data_bytes)
        """
        # Encode the data to bytes
        data_bytes = data.encode('utf-8')
        # Get the length and encode it as 4 bytes
        length = len(data_bytes)
        length_bytes = length.to_bytes(4, byteorder='big')
        return length_bytes, data_bytes

    def _send_all(self, sock, data: bytes):
        """
        Helper to send all bytes, handling short writes
        """
        total_sent = 0
        while total_sent < len(data):
            sent = sock.send(data[total_sent:])
            if sent == 0:
                raise ConnectionError("Socket connection broken during send")
            total_sent += sent

    def recv_response(self, skt):
        """
        Receive a response from the socket
        First read 4 bytes to get the length, then read the actual data
        """
        length_bytes = self.recv_exact(skt, 4)
        length = int.from_bytes(length_bytes, byteorder='big')
        data_bytes = self.recv_exact(skt, length)
        return self._decode(data_bytes)
    
    def _decode(self, data_bytes: bytes):
        """
        Decode bytes to string
        """
        return data_bytes.decode('utf-8')
    
