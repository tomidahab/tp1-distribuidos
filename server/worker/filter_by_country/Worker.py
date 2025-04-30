import asyncio
import logging
import signal
import os
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from dotenv import load_dotenv
import ast

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

# Load environment variables
load_dotenv()

# Constants for query types - these match what the previous worker outputs
QUERY_EQ_YEAR = "eq_year"
QUERY_GT_YEAR = "gt_year"
QUERY_1 = os.getenv("QUERY_1", "1")

# Constants for data processing
PRODUCTION_COUNTRIES = "production_countries"
ISO_3166_1 = "iso_3166_1"
ONE_COUNTRY = "AR"
N_COUNTRIES = ["AR", "ES"]

# Output queues and exchange
ROUTER_PRODUCER_QUEUE = os.getenv("ROUTER_PRODUCER_QUEUE")
RESPONSE_QUEUE = os.getenv("RESPONSE_QUEUE", "response_queue")
EXCHANGE_NAME_PRODUCER = os.getenv("PRODUCER_EXCHANGE", "filtered_by_country_exchange")
EXCHANGE_TYPE_PRODUCER = os.getenv("PRODUCER_EXCHANGE_TYPE", "direct")

ROUTER_CONSUME_QUEUE = os.getenv("ROUTER_CONSUME_QUEUE")

class Worker:
    def __init__(self, 
                 consumer_queue_name=ROUTER_CONSUME_QUEUE, 
                 exchange_name_producer=EXCHANGE_NAME_PRODUCER, 
                 exchange_type_producer=EXCHANGE_TYPE_PRODUCER, 
                 producer_queue_names=[ROUTER_PRODUCER_QUEUE, RESPONSE_QUEUE]):

        self._running = True
        self.consumer_queue_name = consumer_queue_name
        self.producer_queue_names = producer_queue_names
        self.exchange_name_producer = exchange_name_producer
        self.exchange_type_producer = exchange_type_producer
        self.rabbitmq = RabbitMQClient()
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Worker initialized for consumer queue '{consumer_queue_name}', producer queues '{producer_queue_names}'")
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
        # Declare input queue (from router)
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
        for queue_name in self.producer_queue_names:
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
        """Process a message based on its query type"""
        try:
            # Deserialize the message
            deserialized_message = Serializer.deserialize(message.body)
            
            # Extract client_id, data, and query from the deserialized message
            client_id = deserialized_message.get("client_id")
            data = deserialized_message.get("data")
            query = deserialized_message.get("query")
            eof_marker = deserialized_message.get("EOF_MARKER")
            sigterm = deserialized_message.get("SIGTERM")
            if eof_marker:
                logging.info(f"\033[95mReceived EOF marker for client_id '{client_id}'\033[0m")
                await self.send_eq_one_country(client_id, data, self.producer_queue_names[0], True)
                await message.ack()
                return
            
            if sigterm:
                logging.info(f"\033[95mReceived SIGTERM marker for client_id '{client_id}'\033[0m")
                message = self._add_metadata(client_id, data, False, True)
                await self.rabbitmq.publish(
                    exchange_name=self.exchange_name_producer,
                    routing_key=self.producer_queue_names[0],
                    message=Serializer.serialize(message),
                    persistent=True
                )
                await message.ack()
                return
            
            if not data:
                logging.warning(f"Received message with no data, client ID: {client_id}")
                await message.ack()
                return
                
            if query == QUERY_EQ_YEAR:
                data_eq_one_country, data_response_queue = self._filter_data(data)
                if data_eq_one_country:
                    projected_data = self._project_to_columns(data_eq_one_country)
                    await self.send_eq_one_country(client_id, projected_data, self.producer_queue_names[0])
                if data_response_queue:
                    await self.send_response_queue(client_id, data_response_queue, self.producer_queue_names[1])
                    
            elif query == QUERY_GT_YEAR:
                data_eq_one_country, _ = self._filter_data(data)
                if data_eq_one_country:
                    projected_data = self._project_to_columns(data_eq_one_country)
                    await self.send_eq_one_country(client_id, projected_data, self.producer_queue_names[0])
                    
            else:
                logging.warning(f"Unknown query type: {query}, client ID: {client_id}")
            
            # Acknowledge message
            await message.ack()
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            await message.reject(requeue=True)

    def _project_to_columns(self, data):
        """Project the data to only include the 'id' column"""
        if not data:
            return []

        projected_data = []
        for record in data:
            if 'id' in record:
                projected_data.append({
                    "id": record.get("id", ""),
                    "name": record.get("original_title", ""),
                })

        return projected_data

    async def send_eq_one_country(self, client_id, data, queue_name=ROUTER_PRODUCER_QUEUE, eof_marker=False):
        """Send data to the eq_one_country queue in our exchange"""
        message = self._add_metadata(client_id, data, eof_marker)
        success = await self.rabbitmq.publish(
            exchange_name=self.exchange_name_producer,
            routing_key=queue_name,
            message=Serializer.serialize(message),
            persistent=True
        )
        if not success:
            logging.error(f"Failed to send data to eq_one_country queue")

    async def send_response_queue(self, client_id, data, queue_name=RESPONSE_QUEUE, query=QUERY_1):
        """Send data to the response queue in our exchange"""
        message = self._add_metadata(client_id, data, query=query)
        success = await self.rabbitmq.publish(
            exchange_name=self.exchange_name_producer,
            routing_key=queue_name,
            message=Serializer.serialize(message),
            persistent=True
        )
        if not success:
            logging.error(f"Failed to send data to response queue")

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
        """Filter data into two lists based on the country"""
        data_eq_one_country, data_response_queue = [], []
        
        for record in data:
            countries = (record.pop(PRODUCTION_COUNTRIES, None))
            if countries is None:
                logging.info(f"Record missing '{PRODUCTION_COUNTRIES}' field: {record}")
                continue
            
            if isinstance(countries, str):
                try:
                    countries = ast.literal_eval(countries)
                except (SyntaxError, ValueError):
                    logging.error(f"Failed to parse countries string: {countries}")
                    continue

            if not isinstance(countries, list):
                logging.error(f"Countries is not a list after processing: {countries}")
                continue
                    
            record_copy = record.copy()
            has_one_country = False
            found_countries = set()
            
            for country_obj in countries:
                if not isinstance(country_obj, dict):
                    logging.warning(f"Country object is not a dictionary: {country_obj}")
                    continue
                    
                if ISO_3166_1 in country_obj:
                    country = country_obj.get(ISO_3166_1)
                    if country == ONE_COUNTRY:
                        has_one_country = True
                    if country in N_COUNTRIES:
                        found_countries.add(country)
            
            if has_one_country:
                data_eq_one_country.append(record_copy)
                
            # Only add records that contain ALL countries from N_COUNTRIES
            if found_countries == set(N_COUNTRIES):
                data_response_queue.append(record_copy)
            
        return data_eq_one_country, data_response_queue
        
    def _handle_shutdown(self, *_):
        """Handle shutdown signals"""
        logging.info(f"Shutting down worker...")
        self._running = False
        
        # Close RabbitMQ connection - note we need to create a task
        # since this is called from a signal handler
        if hasattr(self, 'rabbitmq'):
            asyncio.create_task(self.rabbitmq.close())
