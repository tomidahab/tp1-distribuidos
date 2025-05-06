import ast
import asyncio
import logging
import signal
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
import os
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

load_dotenv()

#TODO move this to a common config file or common env var since boundary hasthis too
BOUNDARY_QUEUE_NAME = "countries_budget_workers"
#RES_QUEUE = "query2_res"
IMBD_ID = "imdb_id"
ORIGINAL_TITLE = "original_title"
RELEASE_DATE = "release_date"
EXCHANGE_NAME_PRODUCER = "countries_budget_exchange"
EXCHANGE_TYPE_PRODUCER = "direct"
PRODUCTION_COUNTRIES = "production_countries"
GENRES = "genres"
BUDGET = "budget"
ISO_3166_1 = "iso_3166_1"
NAME = "name"
EOF_MARKER = "EOF_MARKER"
ROUTER_PRODUCER_QUEUE = "top_5_budget_queue"
QUERY_2 = "query_2"


ROUTER_CONSUME_QUEUE = os.getenv("ROUTER_CONSUME_QUEUE")
ROUTER_PRODUCER_QUEUE = os.getenv("ROUTER_PRODUCER_QUEUE")

class Worker:
    def __init__(self, exchange_name_consumer=None, exchange_type_consumer=None, consumer_queue_names=[ROUTER_CONSUME_QUEUE], exchange_name_producer=EXCHANGE_NAME_PRODUCER, exchange_type_producer=EXCHANGE_TYPE_PRODUCER, producer_queue_names=[ROUTER_PRODUCER_QUEUE]):

        self._running = True
        self.consumer_queue_names = consumer_queue_names
        self.producer_queue_names = producer_queue_names
        self.exchange_name_consumer = exchange_name_consumer
        self.exchange_name_producer = exchange_name_producer
        self.exchange_type_consumer = exchange_type_consumer
        self.exchange_type_producer = exchange_type_producer
        self.dictionary_countries_budget_by_client = {}
        self.sigterm_clients = []
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
    
    def _add_metadata(self, client_id, data, eof_marker=False, sigterm=False, query=None):
        """Add metadata to the message"""
        message = {        
            "client_id": client_id,
            "EOF_MARKER": eof_marker,
            "data": data,
            "query": query,
            "SIGTERM":sigterm
        }
        return message
  
    
    async def _process_message(self, message):
        """Process a message from the queue"""
        try:
            # Deserialize the message
            deserialized_message = Serializer.deserialize(message.body)
            
            # Extract clientId and data from the deserialized message
            client_id = deserialized_message.get("client_id")
            data = deserialized_message.get("data")
            eof_marker = deserialized_message.get("EOF_MARKER")
            sigterm = deserialized_message.get("SIGTERM")

            if eof_marker:
                logging.info("Received a EOF")
                await self.send_dic(client_id)
                await message.ack()
                return
            
            if sigterm:
                self.sigterm_clients.append(client_id)
                response_message = self._add_metadata(client_id, "", False, True, QUERY_2)
                logging.info(f"received sigterm from :{client_id}")

                await self.rabbitmq.publish_to_queue(
                    queue_name=ROUTER_PRODUCER_QUEUE,
                    message=Serializer.serialize(response_message),
                    persistent=True
                )
                await message.ack()
                return

            if data and client_id not in self.sigterm_clients:
                self._filter_data(data, client_id)
            # Acknowledge message
            await message.ack()
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            await message.reject(requeue=True)

    async def send_dic(self,client_id):
        """Send data to the top5 queue in our exchange"""
        if client_id in self.dictionary_countries_budget_by_client:
            data = self.dictionary_countries_budget_by_client[client_id]
        else:
            data = {}
        message = self._add_metadata(client_id,data)
        await self.rabbitmq.publish(exchange_name=self.exchange_name_producer,
            routing_key=ROUTER_PRODUCER_QUEUE,
            message=Serializer.serialize(message),
            persistent=True
        )
        self.dictionary_countries_budget_by_client[client_id] = {}

        logging.info(f"[Worker1] Published to exchange '{self.exchange_name_producer}' routing_key='{ROUTER_PRODUCER_QUEUE}'")

    def _filter_data(self, data, client_id):
        """Filter data and puts it in its dictionary"""
        for record in data:
            #logging.info(f"record{str(record)}")
            budget = (record.pop(BUDGET, None))
            countries = (record.pop(PRODUCTION_COUNTRIES, None))
            #genres = (record.pop(GENRES, None))
            #imdb_id = (record.pop(IMBD_ID, None))
            #original_title = (record.pop(ORIGINAL_TITLE, None))
            #release_date = (record.pop(RELEASE_DATE, None))

            #if genres is None or imdb_id is None or original_title is None or release_date is None:
            #    logging.error(f"Record missing some field")
            #    continue

            if countries is None:
                logging.error(f"Record missing '{PRODUCTION_COUNTRIES}' field: {record}")
                continue

            #TODO more checks on budget
            if budget is None:
                logging.error(f"Record missing '{BUDGET}' field: {record}")
                continue            

            # Ensure countries is a list of dictionaries
            if isinstance(countries, str):
                # If it's a string that looks like a list, try to evaluate it
                try:
                    countries = ast.literal_eval(countries)
                except (SyntaxError, ValueError):
                    logging.error(f"Failed to parse countries string: {countries}")
                    continue

            # Ensure we have a valid list to work with
            if not isinstance(countries, list):
                logging.error(f"Countries is not a list after processing: {countries}")
                continue
                            
            if len(countries) == 1:
                for country_obj in countries:
                    if not isinstance(country_obj, dict):
                        logging.warning(f"Country object is not a dictionary: {country_obj}")
                        continue
                        
                    if NAME in country_obj:
                        country = country_obj.get(NAME)
                        dic = self.dictionary_countries_budget_by_client.get(client_id, {})
                        if not isinstance(dic, dict):
                            dic = {}
                        dic[country] = dic.get(country, 0) + int(budget)
                        self.dictionary_countries_budget_by_client[client_id] = dic

            #logging.info(f"dic is now:{dic}")

        return
        
    def _handle_shutdown(self, *_):
        """Handle shutdown signals"""
        logging.info(f"Shutting down worker...")
        self._running = False
        
        # Close RabbitMQ connection - note we need to create a task
        # since this is called from a signal handler
        if hasattr(self, 'rabbitmq'):
            asyncio.create_task(self.rabbitmq.close())