import asyncio
import logging
import signal
import time
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from transformers import pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

# Queue names and constants
BOUNDARY_QUEUE_Q5_NAME = "sentiment_analysis_workers"
RESPONSE_QUEUE = "response_queue"

class SentimentWorker:
    def __init__(self, consumer_queue_name=BOUNDARY_QUEUE_Q5_NAME):
        self._running = True
        self.consumer_queue_name = consumer_queue_name
        self.rabbitmq = RabbitMQClient()
        
        logging.info("Loading sentiment analysis model...")
        self.sentiment_pipeline = pipeline("sentiment-analysis")
        logging.info("Sentiment analysis model loaded")
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Sentiment Analysis Worker initialized for queue '{consumer_queue_name}'")
    
    async def run(self):
        """Run the worker, connecting to RabbitMQ and consuming messages"""
        # Connect to RabbitMQ
        if not await self._setup_rabbitmq():
            logging.error(f"Failed to set up RabbitMQ connection. Exiting.")
            return False
        
        logging.info(f"Sentiment Analysis Worker running and consuming from queue '{self.consumer_queue_name}'")
        
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
        
        # Declare queues (idempotent operation)
        queue = await self.rabbitmq.declare_queue(self.consumer_queue_name, durable=True)
        if not queue:
            return False
            
        response_queue = await self.rabbitmq.declare_queue(RESPONSE_QUEUE, durable=True)
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
            start_time = time.time()
            deserialized_message = Serializer.deserialize(message.body)
            
            # Extract clientId and data from the deserialized message
            client_id = deserialized_message.get("clientId")
            data = deserialized_message.get("data")
            
            # Process the movie data for sentiment analysis
            if data:
                logging.info(f"Processing {len(data)} movies for sentiment analysis")
                processed_data = await self._analyze_sentiment_and_calculate_ratios(data)
                
                # Prepare response message
                response_message = {
                    "clientId": client_id,
                    "query": "Q5",
                    "data": processed_data
                }
                
                # Send processed data to response queue
                success = await self.rabbitmq.publish_to_queue(
                    queue_name=RESPONSE_QUEUE,
                    message=Serializer.serialize(response_message),
                    persistent=True
                )
                
                if success:
                    processing_time = time.time() - start_time
                    logging.info(f"Sent {len(processed_data)} processed movies to response queue in {processing_time:.2f} seconds")
                else:
                    logging.error("Failed to send processed data to response queue")
            else:
                logging.warning(f"Received empty data from client {client_id}")
            
            # Acknowledge message
            await message.ack()
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            await message.reject(requeue=False)

    async def _analyze_sentiment_and_calculate_ratios(self, data):
        processed_movies = []
        positive_count = 0
        negative_count = 0
        
        # Process one movie at a time
        batch_size = 1
        total_movies = len(data)
        
        for i in range(0, total_movies, batch_size):
            current_batch = data[i:i+batch_size]
            
            for movie in current_batch:
                try:
                    # Extract required fields
                    original_title = movie.get('original_title', 'Unknown')
                    overview = movie.get('overview', '')
                    budget = float(movie.get('budget', 0))
                    revenue = float(movie.get('revenue', 0))
                    
                    # Calculate ratio
                    ratio = revenue / budget if budget > 0 else 0
                    
                    sentiment = await asyncio.to_thread(self.analyze_sentiment, overview)
                    
                    # Create processed movie record
                    processed_movie = {
                        "Movie": original_title,
                        "feeling": sentiment,
                        "Average": ratio,
                        "confidence": 0.8 
                    }
                    
                    processed_movies.append(processed_movie)
                    
                    # Update counters
                    if sentiment == "positive":
                        positive_count += 1
                    else:
                        negative_count += 1
                    
                except Exception as e:
                    logging.error(f"Error processing movie {movie.get('original_title', 'Unknown')}: {e}")
                    continue
            
            await asyncio.sleep(0.1)
        
        logging.info(f"Processed a total of {len(processed_movies)} movies: {positive_count} positive, {negative_count} negative")
        return processed_movies
    
    def analyze_sentiment(self, text: str) -> str:
        if not text or not text.strip():
            logging.debug("Received empty or whitespace-only text for sentiment analysis.")
            return "neutral"
        
        try:
            result = self.sentiment_pipeline(text, truncation=True)[0]
            label = result["label"].lower()
            
            if label in {"positive", "negative"}:
                logging.debug(f"Sentiment analysis result: {label} for text: {text[:50]}...")
                return label
            else:
                logging.debug(f"Unexpected sentiment label '{label}' for text: {text[:50]}...")
                return "neutral"
                
        except Exception as e:
            logging.error(f"Error during sentiment analysis: {e}")
            return "neutral"
    
    def _handle_shutdown(self, *_):
        logging.info(f"Shutting down sentiment analysis worker...")
        self._running = False
        if hasattr(self, 'rabbitmq'):
            asyncio.create_task(self.rabbitmq.close())