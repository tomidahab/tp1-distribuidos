import asyncio
import logging
import signal
import os
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

# Constants and configuration
ROUTER_CONSUME_QUEUE = os.getenv("ROUTER_CONSUME_QUEUE", "average_sentiment_worker")
RESPONSE_QUEUE = os.getenv("ROUTER_PRODUCER_QUEUE", "response_queue")

class Worker:
    def __init__(self, consumer_queue_name=ROUTER_CONSUME_QUEUE, response_queue_name=RESPONSE_QUEUE):
        self._running = True
        self.consumer_queue_name = consumer_queue_name
        self.response_queue_name = response_queue_name
        self.rabbitmq = RabbitMQClient()
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Average Sentiment Worker initialized for queue '{consumer_queue_name}', response queue '{response_queue_name}'")
    
    async def run(self):
        """Run the worker, connecting to RabbitMQ and consuming messages"""
        # Connect to RabbitMQ
        if not await self._setup_rabbitmq():
            logging.error(f"Failed to set up RabbitMQ connection. Exiting.")
            return False
        
        logging.info(f"Average Sentiment Worker running and consuming from queue '{self.consumer_queue_name}'")
        
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
        
        # Declare queues (idempotent operation)
        queue = await self.rabbitmq.declare_queue(self.consumer_queue_name, durable=True)
        if not queue:
            return False
            
        response_queue = await self.rabbitmq.declare_queue(self.response_queue_name, durable=True)
        if not response_queue:
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
            deserialized_message = Serializer.deserialize(message.body)
            
            # Extract clientId and data from the deserialized message
            client_id = deserialized_message.get("clientId")
            data = deserialized_message.get("data", [])
            eof_marker = deserialized_message.get("EOF_MARKER", False)
            
            if eof_marker:
                logging.info(f"Received EOF marker for clientId '{client_id}'")
                # Pass through EOF marker to response queue
                response_message = {
                    "clientId": client_id,
                    "query": "Q5",
                    "data": [],
                    "EOF_MARKER": True
                }
                
                await self.rabbitmq.publish_to_queue(
                    queue_name=self.response_queue_name,
                    message=Serializer.serialize(response_message),
                    persistent=True
                )
                
                await message.ack()
                return
            
            # Process the sentiment data - for now, just log it
            if data:
                logging.info("==== RECEIVED DATA FROM SENTIMENT ANALYSIS ====")
                logging.info(f"Client ID: {client_id}")
                logging.info(f"Total records: {len(data)}")
                
                # Log the first record to see its structure
                if len(data) > 0:
                    logging.info(f"First record sample: {data[0]}")
                
                # Log a few more records if available
                if len(data) > 1:
                    logging.info(f"Second record sample: {data[1]}")
                
                # Prepare response message - just pass through the data for now
                response_message = {
                    "clientId": client_id,
                    "query": "Q5",
                    "data": data  # Just pass through the data as-is
                }
                
                # Send processed data to response queue
                success = await self.rabbitmq.publish_to_queue(
                    queue_name=self.response_queue_name,
                    message=Serializer.serialize(response_message),
                    persistent=True
                )
                
                if success:
                    logging.info(f"Sent {len(data)} movies directly to response queue for debugging")
                else:
                    logging.error("Failed to send data to response queue")
            else:
                logging.warning(f"Received empty data from client {client_id}")
            
            # Acknowledge message
            await message.ack()
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            await message.reject(requeue=False)
    
    def _handle_shutdown(self, *_):
        logging.info(f"Shutting down average sentiment worker...")
        self._running = False
        if hasattr(self, 'rabbitmq'):
            asyncio.create_task(self.rabbitmq.close())