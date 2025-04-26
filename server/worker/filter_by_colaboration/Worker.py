import asyncio
import logging
import signal
import os
import json
import ast
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

# Constants
ROUTER_CONSUME_QUEUE = os.getenv("ROUTER_CONSUME_QUEUE")
ROUTER_PRODUCER_QUEUE = os.getenv("ROUTER_PRODUCER_QUEUE")
QUERY_2 = os.getenv("QUERY_2", "2")

# Field names
PRODUCTION_COUNTRIES = "production_countries"
BUDGET = "budget"
ORIGINAL_TITLE = "original_title"

class Worker:
    def __init__(self, 
                 consumer_queue_name=ROUTER_CONSUME_QUEUE, 
                 producer_queue_name=ROUTER_PRODUCER_QUEUE):

        self._running = True
        self.consumer_queue_name = consumer_queue_name
        self.producer_queue_name = producer_queue_name
        self.rabbitmq = RabbitMQClient()
        
        # Dictionary to accumulate budget by country for single-country productions
        self.countries_budget = {}
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Filter By Collaboration Worker initialized for queue '{consumer_queue_name}' → '{producer_queue_name}'")
    
    async def run(self):
        """Run the worker, connecting to RabbitMQ and consuming messages"""
        # Connect to RabbitMQ
        if not await self._setup_rabbitmq():
            logging.error(f"Failed to set up RabbitMQ connection. Exiting.")
            return False
        
        logging.info(f"Filter By Collaboration Worker running and consuming from queue '{self.consumer_queue_name}'")
        
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
        
        # Declare consumer queue
        queue = await self.rabbitmq.declare_queue(self.consumer_queue_name, durable=True)
        if not queue:
            return False

        # Declare producer queue
        queue = await self.rabbitmq.declare_queue(self.producer_queue_name, durable=True)
        if not queue:
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
            # Deserialize the message
            deserialized_message = Serializer.deserialize(message.body)
            
            # Extract client_id and data from the deserialized message
            client_id = deserialized_message.get("clientId", deserialized_message.get("client_id"))
            data = deserialized_message.get("data", [])
            eof_marker = deserialized_message.get("EOF_MARKER", False)
            
            if eof_marker:
                logging.info(f"Received EOF marker for client '{client_id}'")
                # Send accumulated budgets by country to the next worker
                await self._send_country_budgets(client_id)
                await message.ack()
                return
            
            # Process the movie data
            if data:
                logging.info(f"Processing {len(data)} movies for client '{client_id}'")
                self._filter_single_country_movies(data)
            else:
                logging.warning(f"Received empty data from client {client_id}")
            
            # Acknowledge message
            await message.ack()
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            await message.reject(requeue=False)

    def _filter_single_country_movies(self, data):
        """
        Filter movies with only one production country and 
        accumulate their budget by country
        """
        processed = 0
        for movie in data:
            # Get production countries
            countries = movie.get(PRODUCTION_COUNTRIES)
            budget = movie.get(BUDGET, 0)
            
            # Skip if no countries or budget information
            if not countries or not budget:
                continue
                
            # Parse countries properly
            if isinstance(countries, str):
                try:
                    # Try ast.literal_eval first which is safer for Python literals
                    countries = ast.literal_eval(countries)
                except (SyntaxError, ValueError):
                    try:
                        # Fall back to json.loads with proper string formatting
                        countries = json.loads(countries.replace("'", '"'))
                    except json.JSONDecodeError:
                        logging.warning(f"Failed to parse countries: {countries}")
                        continue
            
            # Process only movies with a single production country
            if isinstance(countries, list) and len(countries) == 1:
                country_obj = countries[0]
                if isinstance(country_obj, dict) and 'name' in country_obj:
                    country_name = country_obj['name']
                    
                    # Try to convert budget to int
                    try:
                        budget_value = int(float(budget))
                        if budget_value > 0:
                            # Add to country budget dictionary
                            self.countries_budget[country_name] = self.countries_budget.get(country_name, 0) + budget_value
                            processed += 1
                    except (ValueError, TypeError):
                        logging.warning(f"Invalid budget value: {budget}")
                        continue
        
        logging.info(f"Filtered {processed} single-country movies. Total countries tracked: {len(self.countries_budget)}")
    
    async def _send_country_budgets(self, client_id):
        """Send the accumulated country budgets to the next worker"""
        # Prepare the data - convert dictionary to list of objects
        country_budget_list = [
            {"country": country, "budget": budget}
            for country, budget in self.countries_budget.items()
        ]
        
        logging.info(f"Sending budget data for {len(country_budget_list)} countries to next worker")
        
        # Create message with metadata
        message = self._add_metadata(client_id, country_budget_list, True)
        
        # Send to next worker directly to queue
        success = await self.rabbitmq.publish_to_queue(
            queue_name=self.producer_queue_name,
            message=Serializer.serialize(message),
            persistent=True
        )
        
        if success:
            logging.info(f"Successfully sent country budget data to '{self.producer_queue_name}'")
            # Reset the dictionary for next client
            self.countries_budget = {}
        else:
            logging.error(f"Failed to send country budget data to '{self.producer_queue_name}'")
    
    def _add_metadata(self, client_id, data, eof_marker=False):
        """Add metadata to the message"""
        return {
            "clientId": client_id,
            "data": data,
            "EOF_MARKER": eof_marker,
            "query": QUERY_2
        }
    
    def _handle_shutdown(self, *_):
        """Handle shutdown signals"""
        logging.info(f"Shutting down filter_by_colaboration worker...")
        self._running = False
        if hasattr(self, 'rabbitmq'):
            asyncio.create_task(self.rabbitmq.close())