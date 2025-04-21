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
GT_YEAR_QUEUE_NAME = "gt_year"
RELEASE_DATE = "release_date"
EXCHANGE_NAME_PRODUCER = "filtered_by_year_exchange"
EXCHANGE_TYPE_PRODUCER = "direct"

class Worker:
    def __init__(self, exchange_name_consumer=None, exchange_type_consumer=None, consumer_queue_names=[BOUNDARY_QUEUE_NAME], exchange_name_producer=EXCHANGE_NAME_PRODUCER, exchange_type_producer=EXCHANGE_TYPE_PRODUCER, producer_queue_names=[EQ_YEAR_QUEUE_NAME, GT_YEAR_QUEUE_NAME]):

        self._running = True
        self.consumer_queue_names = consumer_queue_names
        self.producer_queue_names = producer_queue_names
        self.exchange_name_consumer = exchange_name_consumer
        self.exchange_name_producer = exchange_name_producer
        self.exchange_type_consumer = exchange_type_consumer
        self.exchange_type_producer = exchange_type_producer
        self.rabbitmq = RabbitMQClient()
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Worker initialized for consumer queues '{consumer_queue_names}', producer queues '{producer_queue_names}', exchange consumer '{exchange_name_consumer}' and exchange producer '{exchange_name_producer}'")
    
    async def run(self):
        """Run the worker, connecting to RabbitMQ and consuming messages"""
        # Connect to RabbitMQ
        if not await self._setup_rabbitmq():
            logging.error(f"Failed to set up RabbitMQ connection. Exiting.")
            return False
        
        logging.info(f"Worker running and consuming from queue '{self.consumer_queue_names}'")
        
        # Keep the worker running until shutdown is triggered
        # TODO check this later
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
        
        # -------------------- CONSUMER --------------------

        # In this worker there is no exchange consumer
        # Declare queues (idempotent operation)
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
        
        # Declare queues (idempotent operation)
        for queue_name in self.producer_queue_names:
            queue = await self.rabbitmq.declare_queue(queue_name, durable=True)
            if not queue:
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
        

        # Set up consumers
        for queue_name in self.consumer_queue_names:
            success = await self.rabbitmq.consume(
                queue_name=queue_name,
                callback=self._process_message,
                no_ack=False
            )
            if not success:
                logging.error(f"Failed to set up consumer for queue '{queue_name}'")
                return False

        return True
    
    async def _process_message(self, message):
        """Process a message from the queue"""
        try:
            # Deserialize the message body from binary to Python object
            data = Serializer.deserialize(message.body)
            
            # Process the movie data - preview first item
            if data:
                data_eq_year, data_gt_year = self._filter_data(data)
                if data_eq_year:
                    await self.send_eq_year(data_eq_year)
                if data_gt_year:
                    await self.send_gt_year(data_gt_year)
                logging.info(f"Sent {len(data_eq_year)} records to eq_year queue")
                logging.info(f"Processed data_eq_year: {data_eq_year}")
                logging.info(f"Sent {len(data_gt_year)} records to gt_year queue")
                logging.info(f"Processed data_gt_year: {data_gt_year}")
            # Acknowledge message
            await message.ack()
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            await message.reject(requeue=True)

    async def send_eq_year(self, data):
        """Send data to the eq_year queue in our exchange"""
        await self.rabbitmq.publish(exchange_name=self.exchange_name_producer,
            routing_key=EQ_YEAR_QUEUE_NAME,
            message=Serializer.serialize(data),
            persistent=True
        )

    async def send_gt_year(self, data):
        """Send data to the gt_year queue in our exchange"""
        await self.rabbitmq.publish(exchange_name=self.exchange_name_producer,
            routing_key=GT_YEAR_QUEUE_NAME,
            message=Serializer.serialize(data),
            persistent=True
        )

    def _filter_data(self, data):
        """Filter data into two lists based on the year"""
        data_eq_year, data_gt_year = [], []
        for record in data:
            year = int(record.pop(RELEASE_DATE, None).split("-")[0])
            if year == YEAR:
                data_eq_year.append(record)
            elif year > YEAR:
                data_gt_year.append(record)
        
        return data_eq_year, data_gt_year
        
    def _handle_shutdown(self, *_):
        """Handle shutdown signals"""
        logging.info(f"Shutting down worker...")
        self._running = False
        
        # Close RabbitMQ connection - note we need to create a task
        # since this is called from a signal handler
        if hasattr(self, 'rabbitmq'):
            asyncio.create_task(self.rabbitmq.close())
