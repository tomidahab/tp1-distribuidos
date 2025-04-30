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
ROUTER_CONSUME_QUEUE = os.getenv("ROUTER_CONSUME_QUEUE")
RESPONSE_QUEUE = os.getenv("RESPONSE_QUEUE",)
EXCHANGE_NAME_PRODUCER = os.getenv("PRODUCER_EXCHANGE", "top_actors_exchange")
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
            
            # Extract client_id, data, EOF and SIGTERM marker
            client_id = deserialized_message.get("client_id")
            data = deserialized_message.get("data")
            eof_marker = deserialized_message.get("EOF_MARKER", False)
            sigterm = deserialized_message.get("SIGTERM", False)

            if eof_marker:
                # If we have data for this client, send it to router producer queue
                if client_id in self.client_data:
                    top_actors = self._get_max_min(client_id)
                    await self._send_data(client_id, top_actors, self.producer_queue_name[0], True, QUERY_3)
                    # Clean up client data after sending
                    del self.client_data[client_id]
                    logging.info(f"Sent top 10 actors for client {client_id} and cleaned up")
                else:
                    logging.warning(f"Received EOF for client {client_id} but no data found")
            elif sigterm:
                response_message = self._add_metadata(client_id, "", False, True, QUERY_3)
                logging.info(f"received sigterm from :{client_id}")
                await self.rabbitmq.publish_to_queue(
                    queue_name=self.producer_queue_name[0],
                    message=Serializer.serialize(response_message),
                    persistent=True
                )
                self._handle_shutdown()
            elif data:
                # Update actors counts for this client
                self._update_max_min(client_id, data)
            else:
                logging.warning(f"Received message with no data for client {client_id}")
            
            await message.ack()
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            await message.reject(requeue=True)
    
    def _update_max_min(self, client_id, data):
        """
        Update the max and min movie values for a client
        
        Args:
            client_id: The client identifier
            data: A dictionary containing 'max' and 'min' movie information
        """
        # Get max and min movies from data
        new_max = data.get('max')
        new_min = data.get('min')
        
        if not new_max or not new_min:
            logging.warning(f"Received incomplete max/min data for client {client_id}")
            return
        
        # Initialize client data if it doesn't exist
        if client_id not in self.client_data:
            self.client_data[client_id] = {
                'max': new_max,
                'min': new_min
            }
            return
        
        # Get current max and min
        current_max = self.client_data[client_id]['max']
        current_min = self.client_data[client_id]['min']
        
        # Update max if the new max has a higher avg
        if new_max.get('avg') > current_max.get('avg'):
            self.client_data[client_id]['max'] = new_max
            
        # Update min if the new min has a lower avg
        if new_min.get('avg') < current_min.get('avg'):
            self.client_data[client_id]['min'] = new_min
    
    def _get_max_min(self, client_id):
        """
        Get the max and min movie avgs for a client
        
        Args:
            client_id: The client identifier
        
        Returns:
            A dictionary containing the max and min movie information
        """
        if client_id not in self.client_data:
            return {'max': None, 'min': None}
        
        return self.client_data[client_id]
    
    
    
    async def _send_data(self, client_id, data, queue_name=None, eof_marker=False, query=None):
        """Send data to the specified router producer queue"""
        if queue_name is None:
            queue_name = self.producer_queue_name[0]
            
        message = self._add_metadata(client_id, data, eof_marker, False, query)
        success = await self.rabbitmq.publish(
            exchange_name=self.exchange_name_producer,
            routing_key=queue_name,
            message=Serializer.serialize(message),
            persistent=True
        )
        
        if not success:
            logging.error(f"Failed to send data to {queue_name} for client {client_id}")

    def _add_metadata(self, client_id, data, eof_marker=False, sigterm=False, query=None):
        """Prepare the message to be sent to the output queue - standardized across workers"""
        message = {        
            "client_id": client_id,
            "data": data,
            "EOF_MARKER": eof_marker,
            "query": query,
            "SIGTERM": sigterm
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