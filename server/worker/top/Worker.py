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
EXCHANGE_NAME_PRODUCER = os.getenv("PRODUCER_EXCHANGE", "top_actors_exchange")
EXCHANGE_TYPE_PRODUCER = os.getenv("PRODUCER_EXCHANGE_TYPE", "direct")
TOP_N = int(os.getenv("TOP_N", 10))
QUERY_4 = os.getenv("QUERY_4", "4")

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
        self.top_n = TOP_N
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Top Worker initialized for consumer queue '{consumer_queue_name}', producer queues '{producer_queue_name}'")
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
        """Process a message and update top actors for the client"""
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
                    top_actors = self._get_top_actors(client_id)
                    await self.send_response(client_id, top_actors, self.producer_queue_name[0], True, QUERY_4)
                    # Clean up client data after sending
                    del self.client_data[client_id]
                    logging.info(f"Sent top actors for client {client_id} and cleaned up")
                    return
                else:
                    logging.warning(f"Received EOF for client {client_id} but no data found")
            if data:
                # Update actors counts for this client
                self._update_actors_data(client_id, data)
            else:
                logging.warning(f"Received message with no data for client {client_id}")
            
            # Acknowledge message
            await message.ack()
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            await message.reject(requeue=True)
    
    
    def _update_actors_data(self, client_id, data):
        """Update the actors count data for a specific client"""
        if not isinstance(data, dict):
            logging.warning(f"Expected dict data for client {client_id}, got {type(data)}")
            return        
        
        # Calculate top n from this batch
        batch_top_n = self._calculate_top_n(data)
        
        # If client doesn't exist yet, just store this batch's top n
        if client_id not in self.client_data:
            self.client_data[client_id] = batch_top_n
            return
        
        # Merge existing top n with new batch's top n
        merged_data = {}
        
        # Add existing top n
        for actor, count in self.client_data[client_id].items():
            merged_data[actor] = count
        
        # Add new batch's top n, summing if actor already exists
        for actor, count in batch_top_n.items():
            merged_data[actor] = merged_data.get(actor, 0) + count
        
        self.client_data[client_id] = self._calculate_top_n(merged_data)
        
    def _calculate_top_n(self, data):
        """Calculate top n actors from a dictionary of actor counts"""
        top_actors = heapq.nlargest(self.top_n, data.items(), key=lambda x: x[1])
        return dict(top_actors)

    def _get_top_actors(self, client_id):
        """Get the top actors for a specific client in the required format"""
        if client_id not in self.client_data:
            return []
        
        # The data is already the top n, just convert to the right format
        actors_data = self.client_data[client_id]
        # Sort by count in descending order
        sorted_actors = sorted(actors_data.items(), key=lambda x: x[1], reverse=True)[:self.top_n]
        
        return [{"name": actor, "count": count} for actor, count in sorted_actors]
    
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