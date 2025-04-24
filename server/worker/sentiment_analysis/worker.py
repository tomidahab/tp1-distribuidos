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
        
        # Initialize sentiment analysis model
        logging.info("Loading sentiment analysis model...")
        self.sentiment_analyzer = pipeline("sentiment-analysis")
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
                    "query": "Q5",  # Add a query identifier
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
            # Reject the message but don't requeue it to avoid endless cycle
            await message.reject(requeue=False)

    async def _analyze_sentiment_and_calculate_ratios(self, data):
        """
        Analyze the sentiment of movie overviews and calculate revenue/budget ratios.
        Returns a list of processed movies with sentiment and ratio information.
        Process movies in smaller batches to prevent memory issues.
        """
        processed_movies = []
        positive_count = 0
        negative_count = 0
        
        # Process in smaller batches of 10 movies at a time
        batch_size = 1
        total_movies = len(data)
        
        for i in range(0, total_movies, batch_size):
            # Get current batch
            end_idx = min(i + batch_size, total_movies)
            current_batch = data[i:end_idx]
            logging.info(f"Processing batch {i//batch_size + 1}/{(total_movies + batch_size - 1)//batch_size} ({len(current_batch)} movies)")
            
            # Process this batch
            for movie in current_batch:
                try:
                    # Extract required fields
                    original_title = movie.get('original_title', 'Unknown')
                    overview = movie.get('overview', '')
                    budget = float(movie.get('budget', 0))
                    revenue = float(movie.get('revenue', 0))
                    
                    # Calculate ratio
                    ratio = revenue / budget if budget > 0 else 0
                    
                    # Truncate very long overviews to avoid excessive memory usage
                    if len(overview) > 500:
                        overview = overview[:500]
                    
                    # Analyze sentiment of overview
                    # We'll do this asynchronously to not block the event loop
                    result = await asyncio.to_thread(self._get_sentiment, overview)
                    sentiment_label = result['label']
                    confidence = result['score']
                    
                    # Increment counters
                    if sentiment_label == 'POSITIVE':
                        positive_count += 1
                    else:
                        negative_count += 1 
                    
                    # Create processed movie record
                    processed_movie = {
                        "Movie": original_title,
                        "feeling": "positive" if sentiment_label == 'POSITIVE' else "negative",
                        "Average": ratio,
                        "confidence": confidence
                    }
                    
                    processed_movies.append(processed_movie)
                    
                except Exception as e:
                    logging.error(f"Error processing movie {movie.get('original_title', 'Unknown')}: {e}")
                    continue
            
            # Add a small delay between batches to prevent memory pressure
            #TODO REMOVE later
            await asyncio.sleep(0.1)
    
        logging.info(f"Processed a total of {len(processed_movies)} movies: {positive_count} positive, {negative_count} negative")
        return processed_movies
    
    def _get_sentiment(self, text):
        """
        Get sentiment of text using the Hugging Face transformers pipeline.
        This is a synchronous operation that will be run in a separate thread.
        With added memory management.
        """
        try:
            # If text is empty or too short, default to neutral sentiment
            if not text or len(text) < 10:
                return {"label": "NEUTRAL", "score": 0.5}
            
            # Still truncate very long texts, but not as aggressively
            if len(text) > 1000:
                text = text[:1000]
            
            # Force garbage collection before analysis
            import gc
            gc.collect()
                    
            # Get sentiment from model
            result = self.sentiment_analyzer(text)[0]
            
            # Force garbage collection after analysis
            gc.collect()
            
            return result
                
        except Exception as e:
            logging.error(f"Error analyzing sentiment: {e}")
            # Return neutral sentiment as fallback
            return {"label": "NEUTRAL", "score": 0.5}
        
    def _handle_shutdown(self, *_):
        """Handle shutdown signals"""
        logging.info(f"Shutting down sentiment analysis worker...")
        self._running = False
        
        # Close RabbitMQ connection - note we need to create a task
        # since this is called from a signal handler
        if hasattr(self, 'rabbitmq'):
            asyncio.create_task(self.rabbitmq.close())