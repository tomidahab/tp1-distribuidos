import asyncio
import logging
import os
from dotenv import load_dotenv
from Worker import Worker

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

async def main():
    """Main entry point for the worker service"""
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment variables
    movies_consumer_queue = os.getenv("ROUTER_CONSUME_QUEUE_MOVIES")
    ratings_consumer_queue = os.getenv("ROUTER_CONSUME_QUEUE_CREDITS")
    producer_queue = os.getenv("ROUTER_PRODUCER_QUEUE")
    producer_exchange = os.getenv("PRODUCER_EXCHANGE", "filtered_data_exchange")
    producer_exchange_type = os.getenv("PRODUCER_EXCHANGE_TYPE", "direct")
    
    # Add retry logic for service initialization
    retry_count = 0
    
    while True:
        try:
            # Create worker with the environment configuration
            worker = Worker(
                consumer_queue_names=[movies_consumer_queue, ratings_consumer_queue],
                producer_queue_name=producer_queue,
                exchange_name_producer=producer_exchange,
                exchange_type_producer=producer_exchange_type
            )
            
            success = await worker.run()
            
            if success:
                break  # Worker completed successfully
            else:
                logging.error("Worker failed to run properly")
                retry_count += 1
                
        except Exception as e:
            retry_count += 1
            logging.error(f"Error running worker: {e}. Retry {retry_count}")

        wait_time = min(30, 2 ** retry_count)  # Exponential backoff with a cap
        logging.info(f"Waiting {wait_time} seconds before retrying...")
        await asyncio.sleep(wait_time)


if __name__ == "__main__":
    logging.info("Starting join_credits worker service...")
    asyncio.run(main())