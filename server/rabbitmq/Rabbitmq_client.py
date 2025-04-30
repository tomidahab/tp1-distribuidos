import asyncio
import logging
import aio_pika
from typing import Optional, Any, Dict

class RabbitMQClient:
    def __init__(self, host="rabbitmq", port=5672, username="guest", password="guest"):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.connection_string = f"amqp://{username}:{password}@{host}:{port}/"
        self._connection: Optional[aio_pika.Connection] = None
        self._channel: Optional[aio_pika.Channel] = None
        self._exchanges: Dict[str, aio_pika.Exchange] = {}
        self._queues: Dict[str, aio_pika.Queue] = {}
        self._consumers = {}

        logging.info(f"RabbitMQ client initialized for {host}:{port}")
    
    async def connect(self) -> bool:
        """Establish connection to RabbitMQ server"""
        try:
            if self._connection and not self._connection.is_closed:
                return True
                
            self._connection = await aio_pika.connect_robust(
                self.connection_string,
                retry_delay=1, 
                heartbeat=60
            )
            self._channel = await self._connection.channel()
            logging.info(f"Connected to RabbitMQ at {self.host}:{self.port}")
            return True
        except Exception as e:
            logging.error(f"Failed to connect to RabbitMQ: {e}")
            return False
    
    async def close(self):
        """Close the RabbitMQ connection"""
        try:
            # Clean up consumers first
            for consumer in self._consumers.values():
                try:
                    # Cancel consumer if possible
                    if hasattr(consumer, 'cancel'):
                        await consumer.cancel()
                except Exception as e:
                    logging.warning(f"Error cancelling consumer: {e}")
                    
            self._consumers.clear()
                
            # Then close channel and connection
            if self._channel:
                await self._channel.close()
                self._channel = None
                
            if self._connection and not self._connection.is_closed:
                await self._connection.close()
                self._connection = None
                
            logging.info("RabbitMQ connection closed")
        except Exception as e:
            logging.error(f"Error closing RabbitMQ connection: {e}")
    
    async def declare_exchange(self, name: str, exchange_type=aio_pika.ExchangeType.DIRECT, 
                             durable=True) -> Optional[aio_pika.Exchange]:
        """Declare an exchange"""
        try:
            if not self._channel:
                if not await self.connect():
                    return None
                    
            exchange = await self._channel.declare_exchange(
                name, 
                type=exchange_type,
                durable=durable
            )
            self._exchanges[name] = exchange
            logging.info(f"Exchange '{name}' declared")
            return exchange
        except Exception as e:
            logging.error(f"Failed to declare exchange '{name}': {e}")
            return None
    
    async def declare_queue(self, name: str, durable=True) -> Optional[aio_pika.Queue]:
        """Declare a queue"""
        try:
            if not self._channel:
                if not await self.connect():
                    return None
                    
            queue = await self._channel.declare_queue(
                name,
                durable=durable
            )
            self._queues[name] = queue
            logging.info(f"Queue '{name}' declared")
            return queue
        except Exception as e:
            logging.error(f"Failed to declare queue '{name}': {e}")
            return None
    
    async def bind_queue(self, queue_name: str, exchange_name: str, routing_key: str) -> bool:
        """Bind queue to exchange with routing key"""
        try:
            if queue_name not in self._queues:
                queue = await self.declare_queue(queue_name)
                if not queue:
                    return False
            else:
                queue = self._queues[queue_name]
                
            if exchange_name not in self._exchanges:
                exchange = await self.declare_exchange(exchange_name)
                if not exchange:
                    return False
            else:
                exchange = self._exchanges[exchange_name]
                
            await queue.bind(exchange, routing_key)
            logging.info(f"Queue '{queue_name}' bound to exchange '{exchange_name}' with key '{routing_key}'")
            return True
        except Exception as e:
            logging.error(f"Failed to bind queue '{queue_name}' to exchange '{exchange_name}': {e}")
            return False
    
    async def publish(self, exchange_name: str, routing_key: str, message: str, 
                    persistent=True) -> bool:
        """Publish message to exchange with routing key"""
        try:
            if not self._channel or self._connection.is_closed:
                if not await self.connect():
                    return False
                    
            if exchange_name not in self._exchanges:
                exchange = await self.declare_exchange(exchange_name)
                if not exchange:
                    return False
            else:
                exchange = self._exchanges[exchange_name]
            
            message_body = message.encode('utf-8') if isinstance(message, str) else message
            
            await exchange.publish(
                aio_pika.Message(
                    body=message_body,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT if persistent else aio_pika.DeliveryMode.NOT_PERSISTENT
                ),
                routing_key=routing_key
            )
            
            return True
        except Exception as e:
            logging.error(f"Failed to publish to exchange '{exchange_name}': {e}")
            return False
    
    async def consume(self, queue_name: str, callback, no_ack=False, prefetch_count=None):
        """Set up consumer for a queue"""
        try:
            if not self._channel:
                if not await self.connect():
                    return False
            
            if prefetch_count is not None:
                await self._channel.set_qos(prefetch_count=prefetch_count)
                    
            if queue_name not in self._queues:
                queue = await self.declare_queue(queue_name)
                if not queue:
                    return False
            else:
                queue = self._queues[queue_name]
            
            # Store the consumer tag, but use the queue's consume method
            # which returns a consumer object in aio-pika
            consumer = await queue.consume(
                callback=callback,
                no_ack=no_ack
            )
            
            self._consumers[queue_name] = consumer
            
            logging.info(f"Consumer set up for queue '{queue_name}'")
            return True
        except Exception as e:
            logging.error(f"Failed to set up consumer for queue '{queue_name}': {e}")
            return False

    async def cancel_consumer(self, queue_name: str) -> bool:
        """Cancel a consumer for a specific queue"""
        try:
            if queue_name not in self._consumers:
                logging.warning(f"No active consumer found for queue '{queue_name}'")
                return False
                
            consumer_tag = self._consumers[queue_name]
            
            # In aio_pika, we need to get the queue and cancel the consumer by tag
            if queue_name in self._queues:
                queue = self._queues[queue_name]
                await queue.cancel(consumer_tag)
                
            del self._consumers[queue_name]
            logging.info(f"Consumer for queue '{queue_name}' cancelled")
            return True
        except Exception as e:
            logging.error(f"Failed to cancel consumer for queue '{queue_name}': {e}")
            raise e
        
    async def publish_to_queue(self, queue_name: str, message: str, persistent=True) -> bool:
        """Publish message directly to queue using the default exchange"""
        try:
            if not self._channel or self._connection.is_closed:
                if not await self.connect():
                    return False
                    
            # Ensure queue exists
            if queue_name not in self._queues:
                queue = await self.declare_queue(queue_name)
                if not queue:
                    return False
            # TODO check if this should be done instead by the encode module
            message_body = message.encode('utf-8') if isinstance(message, str) else message
            
            # Use default exchange (empty string) and queue_name as routing key
            await self._channel.default_exchange.publish(
                aio_pika.Message(
                    body=message_body,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT if persistent else aio_pika.DeliveryMode.NOT_PERSISTENT
                ),
                routing_key=queue_name  # In default exchange, routing_key = queue_name
            )
            
            return True
        except Exception as e:
            logging.error(f"Failed to publish to queue '{queue_name}': {e}")
            return False
