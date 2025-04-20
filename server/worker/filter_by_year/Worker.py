import asyncio
import logging
import signal
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

#TODO move this to a common config file or common env var since boundary hasthis too
BOUNDARY_QUEUE_NAME = "filter_by_year_workers"
YEAR = 2000
EQ_YEAR_QUEUE_NAME = "eq_year"
EQ_GT_YEAR_QUEUE_NAME = "eq_gt_year"
RELEASE_DATE = "release_date"

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
            # Deserialize the message body from binary to Python object
            data = Serializer.deserialize(message.body)
            
            # Log the number of records received
            logging.info(f"Received {len(data)} movie records to process")
            
            # Process the movie data - preview first item
            if data:
                data_eq_year, data_eq_gt_year = self._filter_data(data)
                if data_eq_year:
                    await self.send_eq_year(data_eq_year)
                if data_eq_gt_year:
                    await self.send_eq_gt_year(data_eq_gt_year)
                logging.info(f"Sent {len(data_eq_year)} records to eq_year queue")
                logging.info(f"Sent {len(data_eq_gt_year)} records to eq_gt_year queue")
            # Acknowledge message
            await message.ack()
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            await message.reject(requeue=True)

    async def send_eq_year(self, data):
        """Send data to the eq_year queue"""
        await self.rabbitmq.publish_to_queue(
            queue_name=EQ_YEAR_QUEUE_NAME,
            message=Serializer.serialize(data),
            persistent=True
        )

    async def send_eq_gt_year(self, data):
        """Send data to the eq_gt_year queue"""
        await self.rabbitmq.publish_to_queue(
            queue_name=EQ_GT_YEAR_QUEUE_NAME,
            message=Serializer.serialize(data),
            persistent=True
        )

    def _filter_data(self, data):
        """Filter data into two lists based on the year"""
        data_eq_year, data_eq_gt_year = [], []
        for record in data:
            logging.info(f"Processing record: {record}")
            year = int(record.get(RELEASE_DATE).split("-")[0])
            logging.info(f"Year extracted: {year}")
            # Filter records based on the year
            if year == YEAR:
                data_eq_year.append(record)
                data_eq_gt_year.append(record)
            elif year > YEAR:
                data_eq_gt_year.append(record)
        
        return data_eq_year, data_eq_gt_year
    
    def _handle_shutdown(self, *_):
        """Handle shutdown signals"""
        logging.info(f"Shutting down worker...")
        self._running = False
        
        # Close RabbitMQ connection - note we need to create a task
        # since this is called from a signal handler
        if hasattr(self, 'rabbitmq'):
            asyncio.create_task(self.rabbitmq.close())
