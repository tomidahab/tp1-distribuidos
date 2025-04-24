import asyncio
import logging
import signal
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from load_balancer.factory import create_balancer

class RouterWorker:
    def __init__(self, input_queue, output_queues, exchange_name, exchange_type="direct", balancer_type="roundrobin"):
        """Initialize the router worker
        
        Args:
            input_queue (str): Name of queue to consume messages from
            output_queues (list): List of queue names to distribute messages to
            exchange_name (str): Name of the exchange to publish messages to
            exchange_type (str): Type of exchange to use
            balancer_type (str): Type of load balancer to use (e.g., "roundrobin")
        """
        self.input_queue = input_queue
        self.output_queues = output_queues
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.rabbit_client = RabbitMQClient()
        self.balancer = create_balancer(balancer_type, output_queues)
        self.running = False
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Router worker initialized with input: {input_queue}, outputs: {output_queues}, exchange: {exchange_name}")
        
    async def _setup(self, retry_count=1):
        """Setup connections, exchanges, and declare queues with retry mechanism
        
        Args:
            retry_count (int): Current retry attempt number            
        Returns:
            bool: True if setup succeeds, False otherwise
        """
        # Connect to RabbitMQ
        connected = await self.rabbit_client.connect()
        if not connected:
                
            wait_time = min(30, 2 ** retry_count)
            logging.error(f"Failed to connect to RabbitMQ, retrying in {wait_time} seconds...")
            await asyncio.sleep(wait_time)
            return await self._setup(retry_count + 1)
        
        try:
            # Declare input queue
            queue = await self.rabbit_client.declare_queue(self.input_queue, durable=True)
            if not queue:
                logging.error(f"Failed to declare input queue '{self.input_queue}'")
                return False
            
            # Declare exchange
            exchange = await self.rabbit_client.declare_exchange(
                name=self.exchange_name,
                exchange_type=self.exchange_type,
                durable=True
            )
            if not exchange:
                logging.error(f"Failed to declare exchange '{self.exchange_name}'")
                return False
                
            # Declare and bind all output queues
            for queue_name in self.output_queues:
                queue = await self.rabbit_client.declare_queue(queue_name, durable=True)
                if not queue:
                    logging.error(f"Failed to declare output queue '{queue_name}'")
                    return False
                    
                success = await self.rabbit_client.bind_queue(
                    queue_name=queue_name,
                    exchange_name=self.exchange_name,
                    routing_key=queue_name
                )
                if not success:
                    logging.error(f"Failed to bind queue '{queue_name}' to exchange '{self.exchange_name}'")
                    return False
                
            logging.info("Router worker setup complete")
            return True
            
        except Exception as e:
                
            wait_time = min(30, 2 ** retry_count)
            logging.error(f"Error setting up RabbitMQ: {e}. Retrying in {wait_time} seconds...")
            await asyncio.sleep(wait_time)
            
            # Ensure connection is properly closed before retrying
            await self.rabbit_client.close()
            
            return await self._setup(retry_count + 1)
            
    async def _process_message(self, message):
        """Process an incoming message and route it to the next queue"""
        try:
            # Deserialize the message
            deserialized_message = Serializer.deserialize(message.body)
            
            # Extract the necessary information from the message
            client_id = deserialized_message.get("clientId")
            data = deserialized_message.get("data")
            query = deserialized_message.get("query")  # Include query if it exists
            
            if not client_id or not data:
                logging.warning("Received message with missing clientId or data")
                await message.ack()
                return
                
            # Get the target queue using round-robin
            target_queue = self.balancer.next_queue()
            if not target_queue:
                logging.error("No target queues available")
                await message.reject(requeue=True)
                return
                
            # Prepare message to publish - maintain the query field if present
            outgoing_message = {
                "clientId": client_id,
                "data": data
            }
            
            if query:
                outgoing_message["query"] = query
            
            # Publish the message to the selected queue
            success = await self.rabbit_client.publish(
                exchange_name=self.exchange_name,
                routing_key=target_queue,
                message=Serializer.serialize(outgoing_message),
                persistent=True
            )
            
            if success:
                logging.info(f"Message forwarded to queue: {target_queue}")
                await message.ack()
            else:
                logging.error(f"Failed to forward message to queue: {target_queue}")
                await message.reject(requeue=True)
                
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Reject and requeue the message
            await message.reject(requeue=True)
    
    async def run(self):
        """Run the router worker"""
        if not await self._setup():
            logging.error("Failed to set up RabbitMQ connections")
            return
        
        self.running = True
        logging.info(f"Starting to consume from {self.input_queue}")
        
        # Set up consumer
        success = await self.rabbit_client.consume(
            queue_name=self.input_queue,
            callback=self._process_message,
            no_ack=False
        )
        
        if not success:
            logging.error(f"Failed to set up consumer for queue '{self.input_queue}'")
            return
        
        # Keep the worker running
        try:
            while self.running:
                await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"Error in router worker: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop the router worker"""
        self.running = False
        await self.rabbit_client.close()
        logging.info("Router worker stopped")
    
    def _handle_shutdown(self, *_):
        """Handle shutdown signals"""
        logging.info("Shutting down router worker...")
        self.running = False
        
        # Close RabbitMQ connection - create a task since this is called from a signal handler
        if hasattr(self, 'rabbit_client'):
            asyncio.create_task(self.rabbit_client.close())
