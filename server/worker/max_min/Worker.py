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
        
        self.client_data = defaultdict(dict)

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
        """Process a message and update movies data for the client"""
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
                else:
                    logging.warning(f"Received EOF for client {client_id} but no data found")
            elif data:
                self._update_movie_data(client_id, data)
            else:
                logging.warning(f"Received message with no data for client {client_id}")
            
            await message.ack()
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            await message.reject(requeue=True)

    
    def _update_movie_data(self, client_id, data):
        """
        Update the movie data for a client
        
        Args:
            client_id (str): Client identifier
            data (dict or list): Movie ratings data from upstream
        """
        if not data:
            logging.info(f"Received empty data batch for client {client_id}")
            return
        
        logging.info(f"Updating movie data for client {client_id} with data: {data}")
        for movie in data:
            movie_id = movie.get('id')
            if movie_id is None:
                logging.warning(f"\033[93mSkipping movie with missing id: {movie}\033[0m")
                continue
            if movie.get('count', 0) <= 0 or movie.get('sum', 0) <= 0:
                logging.warning(f"\033[93mSkipping movie with zero count or zero sum or empty values: {movie}\033[0m")
                continue

            # Initialize client data if not present
            if client_id not in self.client_data:
                self.client_data[client_id] = {}

            # Initialize movie data if not present
            if movie_id not in self.client_data[client_id]:
                self.client_data[client_id][movie_id] = {
                    'sum': 0,
                    'count': 0,
                    'name': movie.get('name', '')
                }
            # Update the sum and count
            self.client_data[client_id][movie_id]['sum'] += movie.get('sum')
            self.client_data[client_id][movie_id]['count'] += movie.get('count')
            self.client_data[client_id][movie_id]['avg'] = self.client_data[client_id][movie_id]['sum'] / self.client_data[client_id][movie_id]['count']

    def _get_max_min(self, client_id):
        """
        Calculate and get the movies with max and min ratings for a client
        
        Args:
            client_id (str): Client identifier
            
        Returns:
            dict: Dictionary with max and min movie entries
        """
        if client_id not in self.client_data or not self.client_data[client_id]:
            logging.warning(f"No data found for client {client_id}")
            return {'max': None, 'min': None}
        
        max_movie = None
        min_movie = None
        max_avg = -float('inf')
        min_avg = float('inf')
        
        # Find max and min from all stored movies
        for movie_id, movie_data in self.client_data[client_id].items():
            avg_rating = movie_data.get('avg', 0)
            
            # Update max if this rating is higher
            if avg_rating > max_avg:
                max_avg = avg_rating
                max_movie = {
                    'id': movie_id,
                    'avg': avg_rating,
                    'name': movie_data.get('name', '')
                }
                
            # Update min if this rating is lower
            if avg_rating < min_avg:
                min_avg = avg_rating
                min_movie = {
                    'id': movie_id,
                    'avg': avg_rating,
                    'name': movie_data.get('name', '')
                }
        
        result = {
            'max': max_movie,
            'min': min_movie
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
