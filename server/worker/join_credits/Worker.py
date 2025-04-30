import asyncio
import logging
import signal
import os
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

# Load environment variables
load_dotenv()

# Constants
MOVIES_ROUTER_CONSUME_QUEUE = os.getenv("ROUTER_CONSUME_QUEUE_MOVIES")
CREDITS_ROUTER_CONSUME_QUEUE = os.getenv("ROUTER_CONSUME_QUEUE_CREDITS")
EOF_MARKER = os.getenv("EOF_MARKER", "EOF_MARKER")

# Router configuration
ROUTER_PRODUCER_QUEUE = os.getenv("ROUTER_PRODUCER_QUEUE")
EXCHANGE_NAME_PRODUCER = os.getenv("PRODUCER_EXCHANGE", "filtered_data_exchange")
EXCHANGE_TYPE_PRODUCER = os.getenv("PRODUCER_EXCHANGE_TYPE", "direct")

class Worker:
    def __init__(self, 
                 consumer_queue_names=[MOVIES_ROUTER_CONSUME_QUEUE, CREDITS_ROUTER_CONSUME_QUEUE], 
                 exchange_name_producer=EXCHANGE_NAME_PRODUCER, 
                 exchange_type_producer=EXCHANGE_TYPE_PRODUCER, 
                 producer_queue_name=ROUTER_PRODUCER_QUEUE):

        self._running = True
        self.consumer_queue_names = consumer_queue_names
        self.producer_queue_name = producer_queue_name
        self.exchange_name_producer = exchange_name_producer
        self.exchange_type_producer = exchange_type_producer
        self.rabbitmq = RabbitMQClient()
        
        # State to track which queue we're currently consuming from
        self.current_queue_index = 0
        self.consumers = {}
        self.paused_queues = set()
        
        # Data store for processing
        self.collected_data = {}

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Worker initialized for consumer queues '{consumer_queue_names}', producer queue '{producer_queue_name}' and exchange producer '{exchange_name_producer}'")
    
    async def run(self):
        """Run the worker, connecting to RabbitMQ and consuming messages"""
        # Connect to RabbitMQ
        if not await self._setup_rabbitmq():
            logging.error(f"Failed to set up RabbitMQ connection. Exiting.")
            return False
        
        logging.info(f"Worker running and consuming from queue '{self.consumer_queue_names[self.current_queue_index]}'")
        
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
        # Declare all queues (idempotent operation)
        for queue_name in self.consumer_queue_names:
            queue = await self.rabbitmq.declare_queue(queue_name, durable=True)
            if not queue:
                return False
        # --------------------------------------------------

        # -------------------- PRODUCER --------------------
        # Declare exchange (idempotent operation)
        exchange = await self.rabbitmq.declare_exchange(
            name=self.exchange_name_producer,
            exchange_type=self.exchange_type_producer,
            durable=True
        )
        if not exchange:
            logging.error(f"Failed to declare exchange '{self.exchange_name_producer}'")
            return False
        
        # Declare the producer queue (router input queue)
        queue = await self.rabbitmq.declare_queue(self.producer_queue_name, durable=True)
        if not queue:
            return False        
        
        # Bind queue to exchange
        success = await self.rabbitmq.bind_queue(
            queue_name=self.producer_queue_name,
            exchange_name=self.exchange_name_producer,
            routing_key=self.producer_queue_name
        )
        if not success:
            logging.error(f"Failed to bind queue '{self.producer_queue_name}' to exchange '{self.exchange_name_producer}'")
            return False
        # --------------------------------------------------
        
        # Start consuming from the first queue only
        await self._start_consuming_from_current_queue()

        return True
    
    async def _start_consuming_from_current_queue(self):
        """Start consuming from the current queue index"""
        queue_name = self.consumer_queue_names[self.current_queue_index]
        
        # Only start consuming if not already consuming from this queue
        if queue_name not in self.consumers:
            logging.info(f"Starting to consume from queue: {queue_name}")
            
            success = await self.rabbitmq.consume(
                queue_name=queue_name,
                callback=self._process_message,
                no_ack=False
            )

            if not success:
                logging.error(f"Failed to set up consumer for queue '{queue_name}'")
                return False
                
            self.consumers[queue_name] = True
            
            # Remove from paused queues if it was there
            if queue_name in self.paused_queues:
                self.paused_queues.remove(queue_name)
                
        return True
    
    async def _switch_to_next_queue(self):
        """Switch to the next queue in the list"""
        try:    
            # Pause current queue
            current_queue = self.consumer_queue_names[self.current_queue_index]
            logging.info(f"Pausing consumption from queue: {current_queue}")
            
            # Mark current queue as paused
            self.paused_queues.add(current_queue)
            
            # Cancel the consumer for the current queue to stop consuming
            await self.rabbitmq.cancel_consumer(current_queue)
            
            if current_queue in self.consumers:
                del self.consumers[current_queue]
            
            # Update index to next queue with wrap-around
            self.current_queue_index = (self.current_queue_index + 1) % len(self.consumer_queue_names)
            next_queue = self.consumer_queue_names[self.current_queue_index]
            
            logging.info(f"Switching to queue: {next_queue}")
            
            # Start consuming from the next queue
            await self._start_consuming_from_current_queue()
        except Exception as e:
            logging.error(f"Error switching to next queue: {e}")
            raise e
    
    async def _process_message(self, message):
        """Process a message from the queue"""
        try:
            deserialized_message = Serializer.deserialize(message.body)
            # Extract client_id and data from the deserialized message
            client_id = deserialized_message.get("client_id")
            data = deserialized_message.get("data")
            eof_marker = deserialized_message.get("EOF_MARKER")
            # Check if this is an EOF marker message
            if eof_marker:
                logging.info(f"\033[93mReceived EOF marker for client_id '{client_id}' for current_queue_index {self.current_queue_index}\033[0m")
                if self.current_queue_index == 1  and client_id in self.collected_data:
                    logging.info(f"\033[92mJoined data for client {client_id} with EOF marker\033[0m")
                    await self.send_data(client_id, data, True)
                    del self.collected_data[client_id]

                await message.ack()
                await self._switch_to_next_queue()  # Switch to next queue
                return
            
            if data:
                # If this is the first queue, store data for later join
                if self.current_queue_index == 0:
                    # Save data indexed by client_id (assuming it can process multiple clients)
                    if client_id not in self.collected_data:
                        self.collected_data[client_id] = {}
                    for movie in data:
                        movie_id = movie.get('id')
                        movie_name = movie.get('name')
                        if movie_id and movie_name:
                            self.collected_data[client_id][movie_id] = movie_name
                    
                # If this is the second queue, join with stored data and send result
                elif self.current_queue_index == 1 and client_id in self.collected_data:
                    
                    # Join the data
                    joined_data = self._join_data(
                        self.collected_data[client_id],
                        data
                    )
                    
                    if joined_data:
                        # Send joined data
                        await self.send_data(client_id, joined_data)
            
            # Acknowledge message
            await message.ack()
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            await message.reject(requeue=True)
    
    def _join_data(self, movies_data, credits_data):
        """
        Join the movies and credits data
        This is a simplified implementation - replace with your actual join logic
        """
        try:
            if not movies_data or not credits_data:
                return []
                
            # Create a dict to efficiently look up credits by movie ID
            actors = []
            for credits in credits_data:
                movie_id = credits.get("movie_id")
                if movie_id in movies_data:
                    for actor in credits.get("cast"):
                        actors.append({
                            "name": actor
                        })
            return actors
            
        except Exception as e:
            logging.error(f"Error joining data: {e}")
            return []

    async def send_data(self, client_id, data, eof_marker=False):
        """Send processed data to the output queue"""
        try:    
            message = self._add_metadata(client_id, data, eof_marker)
            success = await self.rabbitmq.publish(
                exchange_name=self.exchange_name_producer,
                routing_key=self.producer_queue_name,
                message=Serializer.serialize(message),
                persistent=True
            )
            if not success:
                logging.error(f"Failed to send joined data to output queue for client {client_id}")
        except Exception as e:
            logging.error(f"Error sending data to output queue: {e}")
            raise e

    def _add_metadata(self, client_id, data, eof_marker, query=None):
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
