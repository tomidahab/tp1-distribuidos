import asyncio
import logging
import signal
import os
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from dotenv import load_dotenv
import heapq
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

# Load environment variables
load_dotenv()

# Constants
ROUTER_CONSUME_QUEUE = os.getenv("ROUTER_CONSUME_QUEUE")
RESPONSE_QUEUE = os.getenv("RESPONSE_QUEUE", "response_queue")
EXCHANGE_NAME_PRODUCER = os.getenv("PRODUCER_EXCHANGE", "max_min_movies_exchange")
EXCHANGE_TYPE_PRODUCER = os.getenv("PRODUCER_EXCHANGE_TYPE", "direct")
QUERY_3 = os.getenv("QUERY_3", "3")

class Worker:
    def __init__(self, 
                 consumer_queue_name=ROUTER_CONSUME_QUEUE, 
                 exchange_name_producer=EXCHANGE_NAME_PRODUCER, 
                 exchange_type_producer=EXCHANGE_TYPE_PRODUCER, 
                 producer_queue_name=[RESPONSE_QUEUE]):

        self._running = True
        self.consumer_queue_name = consumer_queue_name
        self.producer_queue_name = producer_queue_name
        self.exchange_name_producer = exchange_name_producer
        self.exchange_type_producer = exchange_type_producer
        self.rabbitmq = RabbitMQClient()
        
        self.client_data = {}
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"max_min Worker initialized for consumer queue '{consumer_queue_name}', producer queues '{producer_queue_name}'")
        logging.info(f"Exchange producer: '{exchange_name_producer}', type: '{exchange_type_producer}'")
    
    async def run(self):
        """Run the worker, connecting to RabbitMQ and consuming messages"""
        # Connect to RabbitMQ
        if not await self._setup_rabbitmq():
            logging.error(f"Failed to set up RabbitMQ connection. Exiting.")
            return False
        
        logging.info(f"Worker running and consuming from queue '{self.consumer_queue_name}'")
        
        # Keep the worker running until shutdown is triggered
        while self._running:
            await asyncio.sleep(1)
            
        return True
    
    async def _setup_rabbitmq(self, retry_count=1):
        """Set up RabbitMQ connection and consumer"""
        # Connect to RabbitMQ
        connected = await self.rabbitmq.connect()
        if not connected:
            logging.error(f"Failed to connect to RabbitMQ, retrying in {retry_count} seconds...")
            wait_time = min(30, 2 ** retry_count)
            await asyncio.sleep(wait_time)
            return await self._setup_rabbitmq(retry_count + 1)
        
        # -------------------- CONSUMER --------------------
        # Declare input queue
        queue = await self.rabbitmq.declare_queue(self.consumer_queue_name, durable=True)
        if not queue:
            logging.error(f"Failed to declare consumer queue '{self.consumer_queue_name}'")
            return False
        # --------------------------------------------------

        # -------------------- PRODUCER --------------------
        # Declare exchange
        exchange = await self.rabbitmq.declare_exchange(
            name=self.exchange_name_producer,
            exchange_type=self.exchange_type_producer,
            durable=True
        )
        if not exchange:
            logging.error(f"Failed to declare exchange '{self.exchange_name_producer}'")
            return False
        
        # Declare output queues
        for queue_name in self.producer_queue_name:
            queue = await self.rabbitmq.declare_queue(queue_name, durable=True)
            if not queue:
                logging.error(f"Failed to declare producer queue '{queue_name}'")
                return False        
            
            # Bind queues to exchange
            success = await self.rabbitmq.bind_queue(
                queue_name=queue_name,
                exchange_name=self.exchange_name_producer,
                routing_key=queue_name
            )
            if not success:
                logging.error(f"Failed to bind queue '{queue_name}' to exchange '{self.exchange_name_producer}'")
                return False
        # --------------------------------------------------
        
        # Set up consumer for the input queue
        success = await self.rabbitmq.consume(
            queue_name=self.consumer_queue_name,
            callback=self._process_message,
            no_ack=False
        )
        
        if not success:
            logging.error(f"Failed to set up consumer for queue '{self.consumer_queue_name}'")
            return False

        return True
    
    async def _process_message(self, message):
        """Process a message and update max/min movies for the client"""
        try:
            # Deserialize the message
            deserialized_message = Serializer.deserialize(message.body)
            
            # Extract client_id, data and EOF marker
            client_id = deserialized_message.get("client_id")
            data = deserialized_message.get("data")
            eof_marker = deserialized_message.get("EOF_MARKER", False)
            
            if eof_marker:
                # If we have data for this client, send it to response queue
                if client_id in self.client_data:
                    max_min = self._get_max_min(client_id)
                    await self.send_response(client_id, max_min, self.producer_queue_name[0], True, QUERY_3)
                    # Clean up client data after sending
                    del self.client_data[client_id]
                    logging.info(f"\033[92mSent max/min ratings for client {client_id} and cleaned up\033[0m")
                    return
                else:
                    logging.warning(f"Received EOF for client {client_id} but no data found")
            if data:
                self._update_max_min(client_id, data)
            else:
                logging.warning(f"Received message with no data for client {client_id}")
            
            # Acknowledge message
            await message.ack()
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            await message.reject(requeue=True)
    
    
    def _update_max_min(self, client_id, data):
        """
        Update the max and min ratings for a client based on new movie data
        
        Args:
            client_id (str): Client identifier
            data (dict or list): Movie ratings data from upstream
        """
        if not data:
            logging.info(f"Received empty data batch for client {client_id}")
            return
        
        # Initialize client data structure if this is the first data for this client
        if client_id not in self.client_data:
            self.client_data[client_id] = {
                'max': None,
                'min': None
            }
                
        # Handle both dictionary format and list format
        if isinstance(data, dict):
            # Process data in dictionary format where keys are movie IDs
            for movie_id, movie_data in data.items():
                avg_rating = movie_data.get('avg')
                
                if avg_rating is None:
                    logging.warning(f"Skipping movie {movie_id} with missing avg rating")
                    continue
                    
                current_max = self.client_data[client_id]['max']
                current_min = self.client_data[client_id]['min']
                
                # Update max if this is the first movie or if this rating is higher
                if current_max is None or avg_rating > current_max['avg']:
                    self.client_data[client_id]['max'] = {
                        'id': movie_id,
                        'avg': avg_rating
                    }
                    
                # Update min if this is the first movie or if this rating is lower
                if current_min is None or avg_rating < current_min['avg']:
                    self.client_data[client_id]['min'] = {
                        'id': movie_id,
                        'avg': avg_rating
                    }
        else:
            # Process data in list format where each item is a movie object
            for movie in data:
                movie_id = movie.get('id')
                avg_rating = movie.get('avg')
                
                if movie_id is None or avg_rating is None:
                    logging.warning(f"Skipping movie with missing id or avg: {movie}")
                    continue
                    
                current_max = self.client_data[client_id]['max']
                current_min = self.client_data[client_id]['min']
                
                # Update max if this is the first movie or if this rating is higher
                if current_max is None or avg_rating > current_max['avg']:
                    self.client_data[client_id]['max'] = {
                        'id': movie_id,
                        'avg': avg_rating
                    }
                    
                # Update min if this is the first movie or if this rating is lower
                if current_min is None or avg_rating < current_min['avg']:
                    self.client_data[client_id]['min'] = {
                        'id': movie_id,
                        'avg': avg_rating
                    }
        

    def _get_max_min(self, client_id):
        """
        Get the movies with max and min ratings for a client
        
        Args:
            client_id (str): Client identifier
            
        Returns:
            dict: Dictionary with max and min movie entries
        """
        if client_id not in self.client_data:
            logging.warning(f"No data found for client {client_id}")
            return {}
        
        result = {
            'max': self.client_data[client_id]['max'],
            'min': self.client_data[client_id]['min']
        }
        
        return result

    
    async def send_response(self, client_id, data, queue_name=None, eof_marker=False, query=None):
        """Send data to the specified response queue"""
        if queue_name is None:
            queue_name = self.producer_queue_name[0]
            
        message = self._add_metadata(client_id, data, eof_marker, query)
        success = await self.rabbitmq.publish(
            exchange_name=self.exchange_name_producer,
            routing_key=queue_name,
            message=Serializer.serialize(message),
            persistent=True
        )
        
        if not success:
            logging.error(f"Failed to send data to {queue_name} for client {client_id}")

    def _add_metadata(self, client_id, data, eof_marker=False, query=None):
        """Prepare the message to be sent to the output queue"""
        message = {        
            "client_id": client_id,
            "data": data,
            "EOF_MARKER": eof_marker,
            "query": query,
        }
        return message
        
    def _handle_shutdown(self, *_):
        """Handle shutdown signals"""
        logging.info(f"Shutting down worker...")
        self._running = False
        
        # Close RabbitMQ connection - note we need to create a task
        # since this is called from a signal handler
        if hasattr(self, 'rabbitmq'):
            asyncio.create_task(self.rabbitmq.close())
