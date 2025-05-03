import asyncio
import logging
import signal
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from load_balancer.factory import create_balancer

class RouterWorker:
    def __init__(self, number_of_producer_workers, input_queue, output_queues, exchange_name, exchange_type="direct", balancer_type="shard_by_ascii"):
        """Initialize the router worker
        
        Args:
            number_of_producer_workers (int): Number of producer workers to create
            input_queue (str): Name of queue to consume messages from
            output_queues (list): List of queue names to distribute messages to
            exchange_name (str): Name of the exchange to publish messages to
            exchange_type (str): Type of exchange to use
            balancer_type (str): Type of load balancer to use (e.g., "shard_by_ascii"):
        """
        self.number_of_producer_workers = number_of_producer_workers
        self.input_queue = input_queue
        self.output_queues = output_queues
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.rabbit_client = RabbitMQClient()
        self.balancer = create_balancer(balancer_type, output_queues)
        self.running = False
        self.end_of_file_received = {}
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Router worker initialized with input: {input_queue}, outputs: {output_queues}, exchange: {exchange_name}")
        

    def _get_all_queue_names(self):
        """Get all queue names from the output_queues structure, flattening if needed"""
        if not self.output_queues:
            return []
            
        if isinstance(self.output_queues, str):
            return [self.output_queues]
            
        if not isinstance(self.output_queues[0], list):
            return self.output_queues
            
        # Flatten nested structure
        all_queues = []
        for shard in self.output_queues:
            if isinstance(shard, list):
                all_queues.extend(shard)
            else:
                all_queues.append(shard)
                
        return all_queues

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
            for queue_name in self._get_all_queue_names():
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
            client_id = deserialized_message.get("client_id")
            data = deserialized_message.get("data")
            eof_marker = deserialized_message.get("EOF_MARKER")
            sigterm = deserialized_message.get("SIGTERM")
            query = deserialized_message.get("query")
            

            if not client_id:
                logging.warning("Received message with missing client_id")
                await message.ack()
                return

            # Handle EOF marker specially - we need to count them and possibly send to all queues
            if eof_marker:
                self.end_of_file_received[client_id] = self.end_of_file_received.get(client_id, 0) + 1
                logging.info(f"\033[33mReceived EOF marker for client {client_id} - count: {self.end_of_file_received[client_id]}\033[0m")
                
                # Once we've received all expected EOF markers, send to all output queues
                if self.end_of_file_received[client_id] >= self.number_of_producer_workers:
                    await self._send_marker_to_all_queues(client_id, data, query, True)
                    self.end_of_file_received[client_id] = 0
                await message.ack()
                return
            
            if sigterm:
                logging.info(f"received sigterm {client_id}")
                await self._send_marker_to_all_queues(client_id, data, query,False)
                return

                
            # Prepare message to publish - maintain the query field if present
            outgoing_message = {
                "client_id": client_id,
                "data": data,
                "EOF_MARKER": eof_marker,
                "SIGTERM": sigterm
            }
            if query:
                outgoing_message["query"] = query

            
            # Process message based on exchange type
            if self.exchange_type == "fanout":
                # For fanout exchanges, just publish once with empty routing key
                success = await self.rabbit_client.publish(
                    exchange_name=self.exchange_name,
                    routing_key="",  # Routing key is ignored for fanout exchanges
                    message=Serializer.serialize(outgoing_message),
                    persistent=True
                )
                
                if success:
                    await message.ack()
                else:
                    logging.error(f"Failed to forward message to fanout exchange")
                    await message.reject(requeue=True)
            else:
                # For direct exchanges, use the load balancer
                queue_distribution = self.balancer.select_target_queues(data)
                success = True
                for queue, items in queue_distribution.items():
                    # Publish the message to the selected queue
                    outgoing_message["data"] = items
                    publish_success = await self.rabbit_client.publish(
                        exchange_name=self.exchange_name,
                        routing_key=queue,
                        message=Serializer.serialize(outgoing_message),
                        persistent=True
                    )
                    if not publish_success:
                        success = False
                        logging.error(f"Failed to forward message to queue: {queue}")
                if success:
                    await message.ack()
                else:
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

    async def _send_marker_to_all_queues(self, client_id, data, query=None, isEOF=True):
        """Send EOF/SIGTERM marker to all output queues for a specific client ID"""
        if isEOF:
            string = "EOF"
        else:
            string = "SIGTERM"
        message = {
            "client_id": client_id,
            "data": data,
            "EOF_MARKER": isEOF,
            "SIGTERM": not isEOF
        }
        
        # Include query field if present
        if query:
            message["query"] = query
        
        # For fanout exchanges, we only need to publish once with any routing key
        if self.exchange_type == "fanout":
            # Just publish once to the exchange - it will distribute to all bound queues
            success = await self.rabbit_client.publish(
                exchange_name=self.exchange_name,
                routing_key="",  # Routing key is ignored for fanout exchanges
                message=Serializer.serialize(message),
                persistent=True
            )
            if not success:
                logging.error(f"Failed to send EOF marker to fanout exchange for client {client_id}")
            else:
                logging.info(f"EOF marker sent to fanout exchange for client {client_id}, will be delivered to all bound queues")
            return
        
        # For direct and other exchanges, send to each queue explicitly
        all_queues = self._get_all_queue_names()
        for queue in all_queues:
            success = await self.rabbit_client.publish(
                exchange_name=self.exchange_name,
                routing_key=queue,
                message=Serializer.serialize(message),
                persistent=True
            )
            if not success:
                logging.error(f"Failed to send {string} marker to queue {queue} for client {client_id}")
        

        logging.info(f"{string} markers sent to all {len(all_queues)} output queues for client {client_id}")
