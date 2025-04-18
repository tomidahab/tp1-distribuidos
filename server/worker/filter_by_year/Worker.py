import asyncio
import logging
import signal
from rabbitmq.Rabbitmq_client import RabbitMQClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

#TODO move this to a common config file or common env var since boundary hasthis too
BOUNDARY_QUEUE_NAME = "filter_by_year_workers"


class Worker:
    def __init__(self, queue_name=BOUNDARY_QUEUE_NAME):
        self._running = True
        self.queue_name = queue_name
        self.rabbitmq = RabbitMQClient()
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Worker initialized for queue '{queue_name}'")
    
    async def run(self):
        """Run the worker, connecting to RabbitMQ and consuming messages"""
        # Connect to RabbitMQ
        if not await self._setup_rabbitmq():
            logging.error(f"Failed to set up RabbitMQ connection. Exiting.")
            return False
        
        logging.info(f"Worker running and consuming from queue '{self.queue_name}'")
        
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
            await asyncio.sleep(2 ** retry_count)
            return await self._setup_rabbitmq(retry_count + 1)
        
        # Declare queue (idempotent operation)
        queue = await self.rabbitmq.declare_queue(self.queue_name, durable=True)
        if not queue:
            return False
        
        # Set up consumer
        success = await self.rabbitmq.consume(
            queue_name=self.queue_name,
            callback=self._process_message,
            no_ack=False  # Require explicit acknowledgment
        )
        
        return success
    
    async def _process_message(self, message):
        """Process a message from the queue"""
        try:
            # Get message body
            body = message.body.decode('utf-8')
            
            # For now, just log the first 100 characters of the message
            preview = body[:100] + "..." if len(body) > 100 else body
            logging.info(f"Received message: {preview}")
            
            # Process the message (for now, just print it)
            # In a real application, you would parse the CSV data and filter by year
            
            # Acknowledge message
            await message.ack()
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            await message.reject(requeue=True)
    
    def _handle_shutdown(self, *_):
        """Handle shutdown signals"""
        logging.info(f"Shutting down worker...")
        self._running = False
        
        # Close RabbitMQ connection - note we need to create a task
        # since this is called from a signal handler
        if hasattr(self, 'rabbitmq'):
            asyncio.create_task(self.rabbitmq.close())
