import ast
import asyncio
import logging
import os
import signal
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

QUERY_2 = os.getenv("QUERY_2", "2")
CONSUMER_QUEUE_NAME = "top_5_budget_queue"
RES_QUEUE = "query2_res"
RELEASE_DATE = "release_date"
EXCHANGE_NAME_PRODUCER = "top_5_budget_exchange"
EXCHANGE_TYPE_PRODUCER = "direct"
PRODUCTION_COUNTRIES = "production_countries"
BUDGET = "budget"
ISO_3166_1 = "iso_3166_1"
NAME = "name"
EOF_MARKER = "EOF_MARKER"
RESPONSE_QUEUE = "response_queue"
COUNTRIES_BUDGET_WORKERS = 2 #TODO move this into docker-compose

class Worker:
    def __init__(self, exchange_name_consumer=None, exchange_type_consumer=None, consumer_queue_names=[CONSUMER_QUEUE_NAME], exchange_name_producer=EXCHANGE_NAME_PRODUCER, exchange_type_producer=EXCHANGE_TYPE_PRODUCER, producer_queue_names=[RES_QUEUE]):

        self._running = True
        self.consumer_queue_names = consumer_queue_names
        self.producer_queue_names = producer_queue_names
        self.exchange_name_consumer = exchange_name_consumer
        self.exchange_name_producer = exchange_name_producer
        self.exchange_type_consumer = exchange_type_consumer
        self.exchange_type_producer = exchange_type_producer
        self.dictionary_countries_budget_by_client = {}
        self.received_messages = {}
        self.finalized_clients = []
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
            logging.info(f"[Worker2] Declared and consuming from queue '{queue_name}'")
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
            # Deserialize the message
            deserialized_message = Serializer.deserialize(message.body)
            
            # Extract clientId and data from the deserialized message
            client_id = deserialized_message.get("client_id")
            data = deserialized_message.get("data")
            sigterm = deserialized_message.get("SIGTERM")
            
            # Process the movie data - preview first item
            if sigterm:
                logging.info(f"client {client_id} sended SIGTERM")
                self.finalized_clients.append(client_id)
                return

            if client_id in self.finalized_clients:
                logging.info(f"client {client_id} already sended SIGTERM, not processing message")
                return
            
            if data is not None and isinstance(data, dict):
                self._add_dictionary(data, client_id)
            else:
                logging.error(f"data is not valid {data}")

            if self.received_messages[client_id] == COUNTRIES_BUDGET_WORKERS:
                logging.info("Finalizing")
                await self.send_dic(client_id)
                """if data_eq_year:
                    await self.send_eq_year(data_eq_year)
                if data_gt_year:
                    await self.send_gt_year(data_gt_year)"""
                """logging.info(f"Sent {len(data_eq_year)} records to eq_year queue")
                logging.info(f"Processed data_eq_year: {data_eq_year}")
                logging.info(f"Sent {len(data_gt_year)} records to gt_year queue")
                logging.info(f"Processed data_gt_year: {data_gt_year}")"""
            # Acknowledge message
            await message.ack()
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            await message.reject(requeue=True)

    async def send_dic(self, client_id):
        """Send data to the eq_year queue in our exchange"""
        res = self.dictionary_countries_budget_by_client.get(client_id,"THE CLIENT DIC IS EMPTY")
        top_5_countries = dict(sorted(self.dictionary_countries_budget_by_client[client_id].items(), key=lambda kv: kv[1], reverse=True)[:5])
        message = self._add_metadata(client_id, top_5_countries, query=QUERY_2)
        success = await self.rabbitmq.publish_to_queue(
            queue_name=RESPONSE_QUEUE,
            message=Serializer.serialize(message),
            persistent=True
        )
        if not success:
            logging.error(f"Failed to send data to response queue")

    
    def _add_metadata(self, client_id, data, eof_marker=False, query=None):
        """Add metadata to the message"""
        message = {        
            "client_id": client_id,
            "EOF_MARKER": eof_marker,
            "data": data,
            "query": query,
        }
        return message


    def _add_dictionary(self, data, client_id):
        """Combines received dictionary with the one that it has"""

        client_dic = self.dictionary_countries_budget_by_client.get(client_id,{})
        
        self.dictionary_countries_budget_by_client[client_id] = {
            key: client_dic.get(key, 0) + data.get(key, 0)
            for key in set(client_dic) | set(data)
        }
        if client_id in self.received_messages:
            self.received_messages[client_id]+=1
        else:
            self.received_messages[client_id]=1
        return
        
    def _handle_shutdown(self, *_):
        """Handle shutdown signals"""
        logging.info(f"Shutting down worker...")
        self._running = False
        
        # Close RabbitMQ connection - note we need to create a task
        # since this is called from a signal handler
        if hasattr(self, 'rabbitmq'):
            asyncio.create_task(self.rabbitmq.close())