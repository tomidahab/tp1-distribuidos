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
    """Main entry point for the filter_by_colaboration worker service"""
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment variables
    consumer_queue = os.getenv("ROUTER_CONSUME_QUEUE")
    producer_queue = os.getenv("ROUTER_PRODUCER_QUEUE")
    
    # Add retry logic for service initialization
    retry_count = 0
    
    while True:
        try:
            # Create worker with the environment configuration - simplified to use just queues
            worker = Worker(
                consumer_queue_name=consumer_queue,
                producer_queue_name=producer_queue
            )
            
            success = await worker.run()
            
            if success:
                break  # Worker completed successfully
            else:
                logging.error("Filter by collaboration worker failed to run properly")
                retry_count += 1
                
        except Exception as e:
            retry_count += 1
            logging.error(f"Error running filter by collaboration worker: {e}. Retry {retry_count}")

        wait_time = min(30, 2 ** retry_count)  # Exponential backoff with a cap
        logging.info(f"Waiting {wait_time} seconds before retrying...")
        await asyncio.sleep(wait_time)


if __name__ == "__main__":
    logging.info("Starting filter_by_colaboration worker service...")
    asyncio.run(main())