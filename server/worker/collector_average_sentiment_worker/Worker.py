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
RESPONSE_QUEUE = os.getenv("RESPONSE_QUEUE")
EXCHANGE_NAME_PRODUCER = os.getenv("PRODUCER_EXCHANGE", "average_sentiment_exchange")
EXCHANGE_TYPE_PRODUCER = os.getenv("PRODUCER_EXCHANGE_TYPE", "direct")
QUERY_5 = os.getenv("QUERY_5", "5")

class Worker:
    def __init__(self, 
                 consumer_queue_name=ROUTER_CONSUME_QUEUE, 
                 exchange_name_producer=EXCHANGE_NAME_PRODUCER, 
                 exchange_type_producer=EXCHANGE_TYPE_PRODUCER, 
                 producer_queue_name=[RESPONSE_QUEUE]):

        self._running = True
        self.consumer_queue_name = consumer_queue_name
        self.producer_queue_name = producer_queue_name
        self.exchange_name_producer = exchange_name_producer
        self.exchange_type_producer = exchange_type_producer
        self.rabbitmq = RabbitMQClient()
        
        # Store sentiment data from each worker
        self.client_data = {}
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Average Sentiment Collector initialized for consumer queue '{consumer_queue_name}', producer queues '{producer_queue_name}'")
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
        # Declare input queue
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
        for queue_name in self.producer_queue_name:
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
        """Process a message from the various average sentiment workers"""
        try:
            # Deserialize the message
            deserialized_message = Serializer.deserialize(message.body)
            
            # Extract client_id, data and EOF marker
            client_id = deserialized_message.get("client_id")
            data = deserialized_message.get("data")
            eof_marker = deserialized_message.get("EOF_MARKER", False)
            disconnect_marker = deserialized_message.get("DISCONNECT")

            
            if client_id not in self.client_data:
                self.client_data[client_id] = {
                    "POSITIVE": {"sum": 0, "count": 0},
                    "NEGATIVE": {"sum": 0, "count": 0}
                }

            if disconnect_marker:
                removed = self.client_data.pop(client_id, None)
                if removed is not None:
                    logging.info(f"\033[91mDisconnect marker received for client_id '{client_id}'\033[0m")

            
            elif eof_marker:
                # If we have data for this client, send it to response queue
                if client_id in self.client_data:
                    # Calculate final averages
                    await self._calculate_and_send_final_average(client_id)
                    
                    # Clean up client data
                    del self.client_data[client_id]
                    logging.info(f"Sent final average sentiment for client {client_id} and cleaned up data")
                else:
                    logging.warning(f"Received EOF for client {client_id} but no data found")
            
            elif data:
                # Process the data from the sentiment worker
                self._update_sentiment_totals(client_id, data)
                logging.info(f"Updated sentiment totals for client {client_id}")
            
            await message.ack()
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            await message.reject(requeue=True)
    
    def _update_sentiment_totals(self, client_id, data):
        """Update the sentiment totals for a client from new data"""
        for sentiment_data in data:
            sentiment = sentiment_data.get("sentiment")
            sum_value = sentiment_data.get("sum", 0)
            count = sentiment_data.get("count", 0)
            
            # Only process if we have valid sentiment type
            if sentiment in ["POSITIVE", "NEGATIVE"]:
                # Add to the running totals
                self.client_data[client_id][sentiment]["sum"] += sum_value
                self.client_data[client_id][sentiment]["count"] += count
                
                logging.debug(f"Client {client_id}, {sentiment} sum: {self.client_data[client_id][sentiment]['sum']}, count: {self.client_data[client_id][sentiment]['count']}")
    
    async def _calculate_and_send_final_average(self, client_id):
        """Calculate final average and send to response queue"""
        if client_id not in self.client_data:
            logging.warning(f"No data found for client {client_id} when trying to calculate final average")
            return
        
        client_sentiment_totals = self.client_data[client_id]
        
        # Calculate the averages
        positive_avg = 0
        if client_sentiment_totals["POSITIVE"]["count"] > 0:
            positive_avg = client_sentiment_totals["POSITIVE"]["sum"] / client_sentiment_totals["POSITIVE"]["count"]
        
        negative_avg = 0
        if client_sentiment_totals["NEGATIVE"]["count"] > 0:
            negative_avg = client_sentiment_totals["NEGATIVE"]["sum"] / client_sentiment_totals["NEGATIVE"]["count"]
        
        # Prepare detailed results
        positive_count = client_sentiment_totals["POSITIVE"]["count"]
        negative_count = client_sentiment_totals["NEGATIVE"]["count"]
        
        # Create the final response
        result = [{
            "sentiment": "POSITIVE",
            "average_ratio": positive_avg,
            "movie_count": positive_count
        }, {
            "sentiment": "NEGATIVE", 
            "average_ratio": negative_avg,
            "movie_count": negative_count
        }]
        
        # Send the final results
        await self._send_data(client_id, result, True)
    
    async def _send_data(self, client_id, data, eof_marker=False):
        """Send data to the response queue"""
        queue_name = self.producer_queue_name[0]
        
        message = self._add_metadata(client_id, data, eof_marker, QUERY_5)
        success = await self.rabbitmq.publish(
            exchange_name=self.exchange_name_producer,
            routing_key=queue_name,
            message=Serializer.serialize(message),
            persistent=True
        )
        
        if not success:
            logging.error(f"Failed to send final sentiment data for client {client_id}")

    def _add_metadata(self, client_id, data, eof_marker=False, query=None):
        """Prepare the message to be sent to the output queue"""
        message = {        
            "client_id": client_id,
            "data": data,
            "EOF_MARKER": eof_marker,
            "query": query,
        }
        return message
        
    def _handle_shutdown(self, *_):
        """Handle shutdown signals"""
        logging.info(f"Shutting down worker...")
        self._running = False
        
        # Close RabbitMQ connection - note we need to create a task
        # since this is called from a signal handler
        if hasattr(self, 'rabbitmq'):
            asyncio.create_task(self.rabbitmq.close())
