import asyncio
import logging
import signal
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
import ast

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

#TODO move this to a common config file or common env var since boundary hasthis too
EQ_YEAR_QUEUE_NAME = "eq_year"
GT_YEAR_QUEUE_NAME = "gt_year"
PRODUCTION_COUNTRIES = "production_countries"
EQ_ONE_COUNTRY_QUEUE_NAME = "eq_one_country"
RESPONSE_QUEUE = "response_queue"
ONE_COUNTRY = "AR"
N_COUNTRIES = ["AR", "ES"]
ISO_3166_1 = "iso_3166_1"
EXCHANGE_NAME_CONSUMER = "filtered_by_year_exchange"
EXCHANGE_TYPE_CONSUMER = "direct"
EXCHANGE_NAME_PRODUCER = "filtered_by_country_exchange"
EXCHANGE_TYPE_PRODUCER = "direct"

class Worker:
    def __init__(self, exchange_name_consumer=EXCHANGE_NAME_CONSUMER, exchange_type_consumer=EXCHANGE_TYPE_CONSUMER, consumer_queue_names=[EQ_YEAR_QUEUE_NAME, GT_YEAR_QUEUE_NAME], exchange_name_producer=EXCHANGE_NAME_PRODUCER, exchange_type_producer=EXCHANGE_TYPE_PRODUCER, producer_queue_names=[EQ_ONE_COUNTRY_QUEUE_NAME, RESPONSE_QUEUE]):

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

        # Declare exchange (idempotent operation)
        exchange = await self.rabbitmq.declare_exchange(
            name=self.exchange_name_consumer,
            exchange_type=self.exchange_type_consumer,
            durable=True
        )
        if not exchange:
            logging.error(f"Failed to declare exchange '{self.exchange_name_consumer}'")
            return False
        # Declare queues (idempotent operation)
        for queue_name in self.consumer_queue_names:
            queue = await self.rabbitmq.declare_queue(queue_name, durable=True)
            if not queue:
                return False
            # Bind queues to exchange
            success = await self.rabbitmq.bind_queue(
                queue_name=queue_name,
                exchange_name=self.exchange_name_consumer,
                routing_key=queue_name
            )
            if not success:
                logging.error(f"Failed to bind queue '{queue_name}' to exchange '{self.exchange_name_consumer}'")
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
            if queue_name == EQ_YEAR_QUEUE_NAME:
                success = await self.rabbitmq.consume(
                    queue_name=queue_name,
                    callback=self._process_message_for_eq_year,
                    no_ack=False
                )
                if not success:
                    logging.error(f"Failed to set up consumer for queue '{queue_name}'")
                    return False
            elif queue_name == GT_YEAR_QUEUE_NAME:
                success = await self.rabbitmq.consume(
                    queue_name=queue_name,
                    callback=self._process_message_for_gt_year,
                    no_ack=False
                )
                if not success:
                    logging.error(f"Failed to set up consumer for queue '{queue_name}'")
                    return False
        return True
    
    async def _process_message_for_gt_year(self, message):
        try:
            data = Serializer.deserialize(message.body)
            
            if data:
                data_eq_one_country, _ = self._filter_data(data)
                if data_eq_one_country:
                    await self.send_eq_one_country(data_eq_one_country)
            # Acknowledge message
            await message.ack()
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            await message.reject(requeue=True)

    async def _process_message_for_eq_year(self, message):
        try:
            data = Serializer.deserialize(message.body)
            
            if data:
                data_eq_one_country, data_response_queue = self._filter_data(data)
                if data_eq_one_country:
                    await self.send_eq_one_country(data_eq_one_country)
                if data_response_queue:
                    await self.send_response_queue(data_response_queue)
            # Acknowledge message
            await message.ack()
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            await message.reject(requeue=True)

    async def send_eq_one_country(self, data):
        """Send data to the eq_one_country queue in our exchange"""
        await self.rabbitmq.publish(exchange_name=self.exchange_name_producer,
            routing_key=EQ_ONE_COUNTRY_QUEUE_NAME,
            message=Serializer.serialize(data),
            persistent=True
        )

    async def send_response_queue(self, data):
        """Send data to the gt_year queue in our exchange"""
        await self.rabbitmq.publish(exchange_name=self.exchange_name_producer,
            routing_key=RESPONSE_QUEUE,
            message=Serializer.serialize(data),
            persistent=True
        )

    # TODO: Add a optional parameter to the function for one country filtering
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
            # logging.info(f"Found countries in record: {len(found_countries)}")
            # logging.info(f"Found countries in record: {found_countries}")
            if found_countries == set(N_COUNTRIES):
                logging.info(f"Record contains all countries from N_COUNTRIES: {record_copy}")
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
