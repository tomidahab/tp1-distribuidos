import json
import logging
import uuid

class UUIDEncoder(json.JSONEncoder):
    """
    Custom JSON Encoder that handles UUID objects by converting them to strings
    """
    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

class Serializer:
    """
    Serializer for converting between Python data structures and binary data
    for messaging between components.
    """
    @staticmethod
    def serialize(data):
        """
        Convert a Python object (list of dictionaries) to binary data

        Args:
            data: Python object (typically list of dictionaries)

        Returns:
            bytes: Serialized binary data
        """
        try:
            # Convert Python object to JSON string using custom encoder
            json_data = json.dumps(data, cls=UUIDEncoder)
            return json_data.encode('utf-8')
        except Exception as e:
            logging.error(f"Serialization error: {e}")
            raise ValueError(f"Failed to serialize data: {e}")
    @staticmethod
    def deserialize(binary_data):
        """
        Convert binary data back to Python object

        Args:
            binary_data: Serialized binary data

        Returns:
            object: Deserialized Python object (typically list of dictionaries)
        """
        try:
            json_data = binary_data.decode('utf-8')
            # Convert JSON string to Python object
            return json.loads(json_data)
        except Exception as e:
            logging.error(f"Deserialization error: {e}")
            raise ValueError(f"Failed to deserialize data: {e}")
