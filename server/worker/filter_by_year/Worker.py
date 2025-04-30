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
MIN_YEAR = 2000
MAX_YEAR = 2010
RELEASE_DATE = "release_date"

# Router configuration - we now push to a single queue
ROUTER_PRODUCER_QUEUE = os.getenv("ROUTER_PRODUCER_QUEUE")
EXCHANGE_NAME_PRODUCER = os.getenv("PRODUCER_EXCHANGE", "filtered_data_exchange")
EXCHANGE_TYPE_PRODUCER = os.getenv("PRODUCER_EXCHANGE_TYPE", "direct")

# Query types - used as metadata instead of separate queues
QUERY_EQ_YEAR = "eq_year"
QUERY_GT_YEAR = "gt_year"

class Worker:
    def __init__(self, 
                 exchange_name_consumer=None, 
                 exchange_type_consumer=None, 
                 consumer_queue_names=[ROUTER_CONSUME_QUEUE], 
                 exchange_name_producer=EXCHANGE_NAME_PRODUCER, 
                 exchange_type_producer=EXCHANGE_TYPE_PRODUCER, 
                 producer_queue_name=ROUTER_PRODUCER_QUEUE):

        self._running = True
        self.consumer_queue_names = consumer_queue_names
        self.producer_queue_name = producer_queue_name
        self.exchange_name_consumer = exchange_name_consumer
        self.exchange_name_producer = exchange_name_producer
        self.exchange_type_consumer = exchange_type_consumer
        self.exchange_type_producer = exchange_type_producer
        self.rabbitmq = RabbitMQClient()
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Worker initialized for consumer queues '{consumer_queue_names}', producer queue '{producer_queue_name}', exchange consumer '{exchange_name_consumer}' and exchange producer '{exchange_name_producer}'")
    
    async def run(self):
        """Run the worker, connecting to RabbitMQ and consuming messages"""
        # Connect to RabbitMQ
        if not await self._setup_rabbitmq():
            logging.error(f"Failed to set up RabbitMQ connection. Exiting.")
            return False
        
        logging.info(f"Worker running and consuming from queue '{self.consumer_queue_names}'")
        
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
            deserialized_message = Serializer.deserialize(message.body)
            
            # Extract client_id and data from the deserialized message
            client_id = deserialized_message.get("client_id")
            data = deserialized_message.get("data")
            eof_marker = deserialized_message.get("EOF_MARKER")
            sigterm = deserialized_message.get("SIGTERM")

            if eof_marker:
                logging.info(f"\033[95mReceived EOF marker for client_id '{client_id}'\033[0m")
                await self.send_data(client_id, data, QUERY_GT_YEAR, True)
                await message.ack()
                return
            
            if sigterm:
                logging.info(f"\033[95mReceived SIGTERM marker for client_id '{client_id}'\033[0m")
                message = self._add_metadata(client_id, data, False, True)
                await self.rabbitmq.publish(
                    exchange_name=self.exchange_name_producer,
                    routing_key=self.producer_queue_name,
                    message=Serializer.serialize(message),
                    persistent=True
                )
                await message.ack()
                return
            
            # Process the movie data
            if data:
                data_eq_year, data_gt_year = self._filter_data(data)
                if data_eq_year:
                    await self.send_data(client_id, data_eq_year, QUERY_EQ_YEAR)
                if data_gt_year:
                    await self.send_data(client_id, data_gt_year, QUERY_GT_YEAR)
            
            # Acknowledge message
            await message.ack()
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            await message.reject(requeue=True)

    async def send_data(self, client_id, data, query, eof_marker=False):
        """Send data to the router queue with query type in metadata"""
        message = self._add_metadata(client_id, data, eof_marker, False, query)
        success = await self.rabbitmq.publish(
            exchange_name=self.exchange_name_producer,
            routing_key=self.producer_queue_name,
            message=Serializer.serialize(message),
            persistent=True
        )
        if not success:
            logging.error(f"Failed to send data with query type '{query}' to router queue")

    def _add_metadata(self, client_id, data, eof_marker=False, sigterm = False, query=None):
        """Add metadata to the message"""
        message = {        
            "client_id": client_id,
            "EOF_MARKER": eof_marker,
            "SIGTERM":sigterm,
            "data": data,
            "query": query,
        }
        return message

    def _filter_data(self, data):
        """Filter data into two lists based on the year"""
        data_eq_year, data_gt_year = [], []
        
        for record in data:
            try:
                release_date = str(record.get(RELEASE_DATE, ''))
                if not release_date:
                    continue
                year_part = release_date.split("-")[0]
                if not year_part:
                    continue
                    
                year = int(year_part)
                
                del record[RELEASE_DATE]
                
                if self._query1(year):
                    data_eq_year.append(record)
                elif self._query2(year):
                    data_gt_year.append(record)
                    
            except (ValueError, IndexError, AttributeError) as e:
                logging.error(f"Error processing record {record}: {e}")
                continue
            
        return data_eq_year, data_gt_year
    
    def _query1(self, year):
        """Check if the year is equal to the specified year"""
        return MIN_YEAR <= year and year < MAX_YEAR
    
    def _query2(self, year):
        """Check if the year is greater than the specified year"""
        return year > MIN_YEAR
        
    def _handle_shutdown(self, *_):
        """Handle shutdown signals"""
        logging.info(f"Shutting down worker...")
        self._running = False
        
        # Close RabbitMQ connection - note we need to create a task
        # since this is called from a signal handler
        if hasattr(self, 'rabbitmq'):
            asyncio.create_task(self.rabbitmq.close())
