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
COLLECTOR_QUEUE = os.getenv("COLLECTOR_QUEUE", "average_sentiment_collector_router")
QUERY_5 = os.getenv("QUERY_5", "5")

class Worker:
    def __init__(self, consumer_queue_name=ROUTER_CONSUME_QUEUE, producer_queue_name=COLLECTOR_QUEUE):
        self._running = True
        self.consumer_queue_name = consumer_queue_name
        self.producer_queue_name = producer_queue_name
        self.rabbitmq = RabbitMQClient()
        
        # Initialize client data dictionary to track sentiment data per client
        self.client_data = {}
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Average Sentiment Worker initialized for queue '{consumer_queue_name}', producer queue '{self.producer_queue_name}'")
    
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
            
        producer_queue = await self.rabbitmq.declare_queue(self.producer_queue_name, durable=True)
        if not producer_queue:
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
            
            # Extract client_id and data from the deserialized message
            client_id = deserialized_message.get("clientId", deserialized_message.get("client_id"))
            data = deserialized_message.get("data", [])
            eof_marker = deserialized_message.get("EOF_MARKER", False)
            disconnect_marker = deserialized_message.get("DISCONNECT")

            if disconnect_marker:
                await self.send_data(client_id, data, False, disconnect_marker=True)
                self.client_data.pop(client_id, None)
                # TODO: move ack to default behaviour
                await message.ack()
                return

            elif eof_marker:
                logging.info(f"Received EOF marker for client_id '{client_id}'")
                
                if client_id in self.client_data:
                    # Calculate final averages
                    client_sentiment_totals = self.client_data[client_id]
                    
                    # Create the result with sum and count for each sentiment type
                    result = [{
                        "sentiment": "POSITIVE",
                        "sum": client_sentiment_totals["POSITIVE"]["sum"],
                        "count": client_sentiment_totals["POSITIVE"]["count"]
                    }, {
                        "sentiment": "NEGATIVE", 
                        "sum": client_sentiment_totals["NEGATIVE"]["sum"],
                        "count": client_sentiment_totals["NEGATIVE"]["count"]
                    }]
                    
                    # First: Send the data 
                    await self.send_data(client_id, result, False, QUERY_5)
                    
                    # Second: Send message with EOF=True
                    await self.send_data(client_id, [], True, QUERY_5)
                    
                    # Clean up client data after sending
                    del self.client_data[client_id]
                    logging.info(f"Sent sentiment data for client {client_id} and cleaned up client data")
                else:
                    logging.warning(f"Received EOF for client {client_id} but no data found")
                
                await message.ack()
                return
            
            # Process the sentiment data 
            if data:
                # Process each movie in the batch
                for movie in data:
                    # Look for the sentiment field using multiple possible names
                    sentiment = movie.get('sentiment')
                    
                    # Look for the ratio field using multiple possible names
                    ratio = movie.get('ratio', movie.get('Average', 0))
                    
                    logging.debug(f"Processing movie: {movie.get('Movie', 'Unknown')}, sentiment: {sentiment}, ratio: {ratio}")
                    
                    if sentiment:
                        if client_id not in self.client_data:
                            self.client_data[client_id] = {
                                "POSITIVE": {"sum": 0, "count": 0},
                                "NEGATIVE": {"sum": 0, "count": 0}
                            }
                        
                        if sentiment in self.client_data[client_id]:
                            # Add to the running total
                            self.client_data[client_id][sentiment]["sum"] += ratio
                            self.client_data[client_id][sentiment]["count"] += 1
                
                # Log current state
                if client_id in self.client_data:
                    positive_count = self.client_data[client_id]["POSITIVE"]["count"]
                    negative_count = self.client_data[client_id]["NEGATIVE"]["count"]
                    logging.debug(f"Client {client_id} stats - POSITIVE: {positive_count}, NEGATIVE: {negative_count}")
                
                # Acknowledge message
                await message.ack()
                
            else:
                logging.warning(f"Received empty data from client {client_id}")
                await message.ack()
                
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            await message.reject(requeue=False)
    
    async def send_data(self, client_id, data, eof_marker=False, query=None, disconnect_marker=False):
        """Send data to the producer queue with query in metadata"""
        message = self._add_metadata(client_id, data, eof_marker, query, disconnect_marker)
        success = await self.rabbitmq.publish_to_queue(
            queue_name=self.producer_queue_name,
            message=Serializer.serialize(message),
            persistent=True
        )
        if not success:
            logging.error(f"Failed to send data with query '{query}' for client {client_id}")
    
    def _add_metadata(self, client_id, data, eof_marker=False, query=None, disconnect_marker=False):
        """Prepare the message to be sent to the output queue - standardized across workers"""
        message = {        
            "client_id": client_id,
            "data": data,
            "EOF_MARKER": eof_marker,
            "query": query,
            "DISCONNECT": disconnect_marker,
        }
        return message
    
    def _handle_shutdown(self, *_):
        logging.info(f"Shutting down average sentiment worker...")
        self._running = False
        if hasattr(self, 'rabbitmq'):
            asyncio.create_task(self.rabbitmq.close())