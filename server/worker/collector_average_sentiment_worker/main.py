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
    consumer_queue = os.getenv("ROUTER_CONSUME_QUEUE")
    response_queue = os.getenv("RESPONSE_QUEUE")
    
    if not consumer_queue or not response_queue:
        logging.error("Environment variables for queues are not set properly.")
        return

    # Add retry logic for service initialization
    retry_count = 0
    
    while True:
        try:
            # Create worker with the environment configuration
            worker = Worker(
                consumer_queue_name=consumer_queue,
                producer_queue_name=[response_queue],
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
    logging.info("Starting collector average sentiment worker service...")
    asyncio.run(main())
