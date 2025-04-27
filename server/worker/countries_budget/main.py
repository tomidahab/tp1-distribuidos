import asyncio
import logging
import time
from Worker import Worker

async def main():
    """Main entry point for the worker service"""
    # Add retry logic for service initialization
    retry_count = 0
    
    while True:
        try:
            worker = Worker()
            success = await worker.run()
            
            if success:
                break  # Worker completed successfully
            else:
                logging.error("Worker failed to run properly")
                retry_count += 1
                
        except Exception as e:
            retry_count += 1
            logging.error(f"Error running worker: {e}. Retry {retry_count}")

        wait_time = 2 ** retry_count  # Exponential backoff
        logging.info(f"Waiting {wait_time} seconds before retrying...")
        await asyncio.sleep(wait_time)


if __name__ == "__main__":
    logging.info("Starting worker service...")
    asyncio.run(main())