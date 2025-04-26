import asyncio
import logging
import signal
import os
import heapq
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

# Constants
ROUTER_CONSUME_QUEUE = os.getenv("ROUTER_CONSUME_QUEUE")
RESPONSE_QUEUE = os.getenv("RESPONSE_QUEUE", "response_queue")
QUERY_2 = os.getenv("QUERY_2", "2") 
TOP_N = 5  # Top 5 countries

class Worker:
    def __init__(self, 
                 consumer_queue_name=ROUTER_CONSUME_QUEUE, 
                 producer_queue_name=RESPONSE_QUEUE):

        self._running = True
        self.consumer_queue_name = consumer_queue_name
        self.producer_queue_name = producer_queue_name
        self.rabbitmq = RabbitMQClient()
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Top 5 Budget Worker initialized for queue '{consumer_queue_name}' → '{producer_queue_name}'")
    
    async def run(self):
        """Run the worker, connecting to RabbitMQ and consuming messages"""
        # Connect to RabbitMQ
        if not await self._setup_rabbitmq():
            logging.error(f"Failed to set up RabbitMQ connection. Exiting.")
            return False
        
        logging.info(f"Top 5 Budget Worker running and consuming from queue '{self.consumer_queue_name}'")
        
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
        
        # Declare consumer queue
        queue = await self.rabbitmq.declare_queue(self.consumer_queue_name, durable=True)
        if not queue:
            return False

        # Declare producer queue
        queue = await self.rabbitmq.declare_queue(self.producer_queue_name, durable=True)
        if not queue:
            return False
        
        # Set up consumer
        success = await self.rabbitmq.consume(
            queue_name=self.consumer_queue_name,
            callback=self._process_message,
            no_ack=False,
            prefetch_count=1
        )
        if not success:
            logging.error(f"Failed to set up consumer for queue '{self.consumer_queue_name}'")
            return False

        return True
    
    async def _process_message(self, message):
        """Process a message from the queue"""
        try:
            # Deserialize the message
            deserialized_message = Serializer.deserialize(message.body)
            
            # Extract client_id and data from the deserialized message
            client_id = deserialized_message.get("clientId", deserialized_message.get("client_id"))
            data = deserialized_message.get("data", [])
            eof_marker = deserialized_message.get("EOF_MARKER", False)

            logging.info(f"HEREEEEEEEEEEEEEEEE")
            
            if eof_marker and data:
                logging.info(f"Received final country budget data for client '{client_id}'")
                # Calculate top 5 countries by budget
                top_countries = self._calculate_top_countries(data)
                # Send to response queue
                await self._send_top_countries(client_id, top_countries)
            elif eof_marker:
                # Send empty response for EOF marker
                logging.info(f"Received EOF marker with no data for client '{client_id}'")
                await self._send_top_countries(client_id, [])
            else:
                logging.warning(f"Received non-EOF message - expected only EOF messages with data")
            
            # Acknowledge message
            await message.ack()
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            await message.reject(requeue=False)

    def _calculate_top_countries(self, country_data):
        """Calculate the top N countries with highest budget"""
        # Ensure we're working with a list
        if not isinstance(country_data, list):
            logging.warning(f"Expected list of country budgets, got {type(country_data)}")
            return []
        
        # Use heap to efficiently get top N countries
        return heapq.nlargest(TOP_N, country_data, key=lambda x: x.get('budget', 0))
    
    async def _send_top_countries(self, client_id, top_countries):
        """Send the top countries to the response queue"""
        # Format the response data
        formatted_countries = [
            {
                "country": country.get("country", "Unknown"),
                "budget": country.get("budget", 0)
            }
            for country in top_countries
        ]
        
        logging.info(f"Sending top {min(TOP_N, len(formatted_countries))} countries by budget to client '{client_id}'")
        for country in formatted_countries:
            logging.info(f"  - {country['country']}: ${country['budget']:,}")
        
        # Create message with metadata
        message = self._add_metadata(client_id, formatted_countries, True)
        
        # Send directly to response queue
        success = await self.rabbitmq.publish_to_queue(
            queue_name=self.producer_queue_name,
            message=Serializer.serialize(message),
            persistent=True
        )
        
        if success:
            logging.info(f"Successfully sent top countries data to response queue '{self.producer_queue_name}'")
        else:
            logging.error(f"Failed to send top countries data to response queue '{self.producer_queue_name}'")
    
    def _add_metadata(self, client_id, data, eof_marker=False):
        """Add metadata to the message"""
        return {
            "client_id": client_id,
            "data": data,
            "EOF_MARKER": eof_marker,
            "query": QUERY_2
        }
    
    def _handle_shutdown(self, *_):
        """Handle shutdown signals"""
        logging.info(f"Shutting down top 5 budget worker...")
        self._running = False
        if hasattr(self, 'rabbitmq'):
            asyncio.create_task(self.rabbitmq.close())