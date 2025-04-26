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
RESPONSE_QUEUE = os.getenv("ROUTER_PRODUCER_QUEUE", "response_queue")

class Worker:
    def __init__(self, consumer_queue_name=ROUTER_CONSUME_QUEUE, response_queue_name=RESPONSE_QUEUE):
        self._running = True
        self.consumer_queue_name = consumer_queue_name
        self.response_queue_name = response_queue_name
        self.rabbitmq = RabbitMQClient()
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Average Sentiment Worker initialized for queue '{consumer_queue_name}', response queue '{response_queue_name}'")
    
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
            
        response_queue = await self.rabbitmq.declare_queue(self.response_queue_name, durable=True)
        if not response_queue:
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
            client_id = deserialized_message.get("clientId")
            data = deserialized_message.get("data", [])
            eof_marker = deserialized_message.get("EOF_MARKER", False)
            
            # Initialize or retrieve running sums and counts from class attributes
            if not hasattr(self, 'sentiment_totals'):
                self.sentiment_totals = {
                    "POSITIVE": {"sum": 0, "count": 0},
                    "NEGATIVE": {"sum": 0, "count": 0}
                }
            
            if eof_marker:
                logging.info(f"Received EOF marker for client_id '{client_id}'")
                
                # Calculate final averages
                positive_avg = 0
                if self.sentiment_totals["POSITIVE"]["count"] > 0:
                    positive_avg = self.sentiment_totals["POSITIVE"]["sum"] / self.sentiment_totals["POSITIVE"]["count"]
                    
                negative_avg = 0
                if self.sentiment_totals["NEGATIVE"]["count"] > 0:
                    negative_avg = self.sentiment_totals["NEGATIVE"]["sum"] / self.sentiment_totals["NEGATIVE"]["count"]
                
                # Prepare detailed results
                positive_count = self.sentiment_totals["POSITIVE"]["count"]
                negative_count = self.sentiment_totals["NEGATIVE"]["count"]
                
                # Create the final response with the average results
                result = [{
                    "sentiment": "POSITIVE",
                    "average_ratio": positive_avg,
                    "movie_count": positive_count
                }, {
                    "sentiment": "NEGATIVE", 
                    "average_ratio": negative_avg,
                    "movie_count": negative_count
                }]
                
                logging.info(f"===== FINAL SENTIMENT ANALYSIS RESULTS =====")
                logging.info(f"Positive movies: {positive_count}, Average ratio: {positive_avg:.4f}")
                logging.info(f"Negative movies: {negative_count}, Average ratio: {negative_avg:.4f}")
                
                # Pass the summary to response queue
                response_message = {
                    "clientId": client_id,
                    "query": "Q5",
                    "data": result,
                    "EOF_MARKER": True
                }
                
                await self.rabbitmq.publish_to_queue(
                    queue_name=self.response_queue_name,
                    message=Serializer.serialize(response_message),
                    persistent=True
                )
                
                # Reset the accumulators for the next client
                self.sentiment_totals = {
                    "POSITIVE": {"sum": 0, "count": 0},
                    "NEGATIVE": {"sum": 0, "count": 0}
                }
                
                await message.ack()
                return
            
            # Process the sentiment data 
            if data:
                logging.info(f"Received batch with {len(data)} movies for sentiment analysis from client '{client_id}'")
                
                # Process each movie in the batch
                for movie in data:
                    sentiment = movie.get('sentiment')
                    ratio = movie.get('ratio')
                    
                    if sentiment in self.sentiment_totals:
                        # Add to the running total
                        self.sentiment_totals[sentiment]["sum"] += ratio
                        self.sentiment_totals[sentiment]["count"] += 1
                
                # Log current state
                positive_count = self.sentiment_totals["POSITIVE"]["count"]
                negative_count = self.sentiment_totals["NEGATIVE"]["count"]
                
                if positive_count > 0:
                    positive_current_avg = self.sentiment_totals["POSITIVE"]["sum"] / positive_count
                    logging.info(f"Running totals - Positive: {positive_count} movies, avg ratio: {positive_current_avg:.4f}")
                
                if negative_count > 0:
                    negative_current_avg = self.sentiment_totals["NEGATIVE"]["sum"] / negative_count
                    logging.info(f"Running totals - Negative: {negative_count} movies, avg ratio: {negative_current_avg:.4f}")
                
                # Acknowledge message
                await message.ack()
                
            else:
                logging.warning(f"Received empty data from client {client_id}")
                await message.ack()
                
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            await message.reject(requeue=False)
    
    def _handle_shutdown(self, *_):
        logging.info(f"Shutting down average sentiment worker...")
        self._running = False
        if hasattr(self, 'rabbitmq'):
            asyncio.create_task(self.rabbitmq.close())